package io.netsecml.platform.bootstrap.archive;

import io.netsecml.platform.adapter.clickhouse.writer.ClickHouseBatchSink;
import io.netsecml.platform.adapter.clickhouse.writer.ClickHouseConfig;
import io.netsecml.platform.adapter.flink.source.RawBytesDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import java.time.Duration;
import java.util.List;

// The independent Kafka-to-ClickHouse archive job.
//
// One source-to-sink chain per registered log type, in one job. The Flink job
// graph IS the routing, which is why there is no ArchiveRouter class: a router
// on top of a topology that already routes would be an abstraction with no
// behaviour.
//
// All chains share checkpoint fate deliberately. They all write to the same
// ClickHouse, so if it is unreachable every chain should stall and let Kafka lag
// grow rather than one quietly racing ahead.
//
// This job must never be able to stop online feature production. That is why it
// is a separate deployment with its own restart strategy and its own lag.
public final class ArchiveJob {

    // One consumer group for every topic: this is one logical archiver.
    public static final String CONSUMER_GROUP = "conn-archive-job";

    // Non-instantiable: every member here is static.
    private ArchiveJob() {
    }

    // One archive chain: a Kafka topic, the map function that turns its raw bytes
    // into a ClickHouse row, the target table, and the three operator uids.
    //
    // Public so a caller assembling the parameterised build() below can register
    // its own log type without reaching into this class's internals -- Spec §6.3
    // requires build() to take the set of log types as a parameter, and adding a
    // log type must be one entry in a list, not a new String parameter.
    //
    // The uids are stored explicitly rather than derived from the chain name. They
    // are checkpoint state identity, and the two pre-existing chains named their
    // operators inconsistently -- "feature-vector-source" but "invalid-event-row"
    // under a "dlq" source, and two pluralised sinks. Any rule that generated
    // those six names would be more intricate than the names themselves, so they
    // are data. The six existing uid strings below are unchanged from before this
    // type was made public -- changing any of them would make Flink silently
    // discard that operator's checkpoint state on restore.
    public record LogTypeChain<T>(String topic, RichMapFunction<byte[], T> rowMapper, String table,
                                  String sourceUid, String mapUid, String sinkUid) {
    }

    // Builds one chain per registered log type on the given environment. Called
    // from main() below, and directly by tests so a test can inject a short
    // checkpoint interval and a Testcontainers-backed bootstrapServers/clickHouse
    // before the environment is executed.
    //
    // This is the parameterised form Spec §6.3 requires: the set of log types is
    // the chains list, so a sixth log type is a sixth list entry, never a
    // thirteenth String parameter.
    public static void build(StreamExecutionEnvironment env, String bootstrapServers,
                             List<LogTypeChain<?>> chains, ClickHouseConfig clickHouse) {
        for (LogTypeChain<?> chain : chains) {
            wire(env, bootstrapServers, clickHouse, chain);
        }
    }

    // The original two-topic overload, kept so main() and the existing topology
    // and E2E tests keep compiling and passing unchanged. Delegates into the
    // parameterised form above with the same two chains and the same six uids
    // this job has always used.
    public static void build(StreamExecutionEnvironment env, String bootstrapServers,
                             String featureVectorTopic, String dlqTopic, ClickHouseConfig clickHouse) {
        build(env, bootstrapServers, List.of(
            // Chain 1 -- feature vectors. The required Day 6 path: a versioned
            // vector observable in Kafka must become queryable in ClickHouse.
            new LogTypeChain<>(featureVectorTopic,
                new FeatureVectorRowMapFunction(featureVectorTopic), "feature_vectors",
                "feature-vector-source", "feature-vector-row", "feature-vectors-clickhouse-sink"),
            // Chain 2 -- rejected records. Low volume, and duplicates after a
            // replay are expected rather than prevented.
            new LogTypeChain<>(dlqTopic,
                new InvalidEventRowMapFunction(dlqTopic), "invalid_events",
                "dlq-source", "invalid-event-row", "invalid-events-clickhouse-sink")),
            clickHouse);
    }

    // Wires one chain. Generic so the row type flows from the map function to the
    // sink without a cast.
    //
    // Every operator gets its explicit, stable uid. Without one Flink derives the
    // operator ID from the topology hash, so ANY future edit to this graph
    // silently discards state on restore-from-checkpoint instead of failing
    // loudly -- and a generated topology can collide uids in a way a hand-written
    // one cannot, which ArchiveJobTopologyTest guards.
    // The one-time cost of assigning uids was judged acceptable when they were
    // introduced: this job carries no keyed state -- only the source's committed
    // offset position and the sink's in-flight batch, both of which replay safely
    // from Kafka -- so a checkpoint that fails to restore across the change loses
    // nothing that Kafka cannot re-deliver.
    private static <T> void wire(StreamExecutionEnvironment env, String bootstrapServers,
                                 ClickHouseConfig clickHouse, LogTypeChain<T> chain) {
        env.fromSource(source(bootstrapServers, chain.topic()),
                WatermarkStrategy.noWatermarks(), chain.sourceUid())
            .uid(chain.sourceUid())
            .map(chain.rowMapper())
            .name(chain.mapUid())
            .uid(chain.mapUid())
            .sinkTo(new ClickHouseBatchSink<T>(chain.table(), clickHouse))
            .name(chain.sinkUid())
            .uid(chain.sinkUid());
    }

    // No watermarks: nothing downstream is event-time windowed. The archive job
    // batches and inserts; it never reasons about time.
    private static KafkaSource<byte[]> source(String bootstrapServers, String topic) {
        return KafkaSource.<byte[]>builder()
            .setBootstrapServers(bootstrapServers)
            .setTopics(topic)
            .setGroupId(CONSUMER_GROUP)
            // Resume from committed offsets so a redeploy continues where it left
            // off, falling back to the earliest offset on a first run.
            .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
            .setValueOnlyDeserializer(new RawBytesDeserializationSchema())
            .build();
    }

    // Production entry point: reads every setting from the environment (all
    // documented in .env.example) and runs the job until cancelled.
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Flink 2.x removed StreamExecutionEnvironment.setRestartStrategy, so the restart
        // strategy is configuration-only now. Applied before enableCheckpointing below so
        // the explicit checkpoint settings are the last word regardless of what configure()
        // reads out of this Configuration. failure-rate keeps a transient ClickHouse outage
        // recoverable while refusing to hot-loop a genuinely broken deployment.
        Configuration restartConfig = new Configuration();
        restartConfig.set(RestartStrategyOptions.RESTART_STRATEGY,
            RestartStrategyOptions.RestartStrategyType.FAILURE_RATE.getMainValue());
        restartConfig.set(RestartStrategyOptions.RESTART_STRATEGY_FAILURE_RATE_MAX_FAILURES_PER_INTERVAL, 3);
        restartConfig.set(RestartStrategyOptions.RESTART_STRATEGY_FAILURE_RATE_FAILURE_RATE_INTERVAL, Duration.ofMinutes(10));
        restartConfig.set(RestartStrategyOptions.RESTART_STRATEGY_FAILURE_RATE_DELAY, Duration.ofSeconds(10));
        env.configure(restartConfig);

        // Checkpointing is what makes this job's delivery contract real, so it is not
        // optional tuning. A failed ClickHouse insert throws out of the sink writer's
        // flush(), which fails the checkpoint, which leaves the Kafka offsets uncommitted
        // so the batch replays on recovery and ReplacingMergeTree absorbs the duplicates.
        // With checkpointing off the source never commits offsets at all, and the sink's
        // bounded retry would be guarding a contract that never engages.
        // Values are FINAL_ARCHITECTURE.md's initial settings -- benchmark, do not canonize.
        env.enableCheckpointing(30_000L);
        CheckpointConfig checkpoints = env.getCheckpointConfig();
        checkpoints.setMinPauseBetweenCheckpoints(10_000L);
        checkpoints.setCheckpointTimeout(120_000L);
        checkpoints.setMaxConcurrentCheckpoints(1);

        // Same variable names the online job reads, all documented in .env.example.
        String bootstrapServers = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
        String featureTopic = System.getenv().getOrDefault("FEATURE_VECTOR_TOPIC", "netsec.conn.feature-vector.v1");
        String dlqTopic = System.getenv().getOrDefault("DLQ_TOPIC", "netsec.conn.dlq.v1");

        ClickHouseConfig clickHouse = ClickHouseConfig.of(
            System.getenv().getOrDefault("CLICKHOUSE_HOST", "localhost"),
            Integer.parseInt(System.getenv().getOrDefault("CLICKHOUSE_PORT", "8123")),
            System.getenv().getOrDefault("CLICKHOUSE_DATABASE", "netsec_ml"),
            System.getenv().getOrDefault("CLICKHOUSE_USER", "default"),
            System.getenv().getOrDefault("CLICKHOUSE_PASSWORD", ""));

        build(env, bootstrapServers, featureTopic, dlqTopic, clickHouse);
        env.execute("conn-archive-job");
    }
}
