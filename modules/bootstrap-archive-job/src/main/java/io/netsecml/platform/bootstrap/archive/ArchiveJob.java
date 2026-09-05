package io.netsecml.platform.bootstrap.archive;

import io.netsecml.platform.adapter.clickhouse.row.FeatureVectorRow;
import io.netsecml.platform.adapter.clickhouse.row.InvalidEventRow;
import io.netsecml.platform.adapter.clickhouse.writer.ClickHouseBatchSink;
import io.netsecml.platform.adapter.clickhouse.writer.ClickHouseConfig;
import io.netsecml.platform.adapter.flink.source.RawBytesDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import java.time.Duration;

// The independent Kafka-to-ClickHouse archive job.
//
// Two source-to-sink chains in one job. The Flink job graph IS the routing, which
// is why there is no ArchiveRouter class: a router on top of a topology that
// already routes would be an abstraction with no behaviour.
//
// The two chains share checkpoint fate deliberately. Both write to the same
// ClickHouse, so if it is unreachable both should stall and let Kafka lag grow
// rather than one quietly racing ahead.
//
// This job must never be able to stop online feature production. That is why it
// is a separate deployment with its own restart strategy and its own lag.
public final class ArchiveJob {

    // One consumer group for both topics: this is one logical archiver.
    public static final String CONSUMER_GROUP = "conn-archive-job";

    // Non-instantiable: every member here is static.
    private ArchiveJob() {
    }

    // Builds both chains on the given environment. Package-visible from main()
    // below and called directly by tests so a test can inject a short checkpoint
    // interval and a Testcontainers-backed bootstrapServers/clickHouse before the
    // environment is executed.
    public static void build(StreamExecutionEnvironment env, String bootstrapServers,
                             String featureVectorTopic, String dlqTopic, ClickHouseConfig clickHouse) {

        // Chain 1 — feature vectors. The required Day 6 path: a versioned vector
        // observable in Kafka must become queryable in ClickHouse.
        env.fromSource(source(bootstrapServers, featureVectorTopic),
                WatermarkStrategy.noWatermarks(), "feature-vector-source")
            .map(new FeatureVectorRowMapFunction(featureVectorTopic))
            .name("feature-vector-row")
            .sinkTo(new ClickHouseBatchSink<FeatureVectorRow>("feature_vectors", clickHouse))
            .name("feature-vectors-clickhouse-sink");

        // Chain 2 — rejected records. Low volume, and duplicates after a replay
        // are expected rather than prevented.
        env.fromSource(source(bootstrapServers, dlqTopic),
                WatermarkStrategy.noWatermarks(), "dlq-source")
            .map(new InvalidEventRowMapFunction(dlqTopic))
            .name("invalid-event-row")
            .sinkTo(new ClickHouseBatchSink<InvalidEventRow>("invalid_events", clickHouse))
            .name("invalid-events-clickhouse-sink");
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
