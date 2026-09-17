package io.netsecml.platform.bootstrap.archive;

import io.netsecml.platform.adapter.clickhouse.row.FeatureVectorRow;
import io.netsecml.platform.adapter.clickhouse.row.InvalidEventRow;
import io.netsecml.platform.adapter.clickhouse.writer.ClickHouseBatchSink;
import io.netsecml.platform.adapter.clickhouse.writer.ClickHouseConfig;
import io.netsecml.platform.adapter.flink.source.RawBytesDeserializationSchema;
import io.netsecml.platform.domain.event.LogType;
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
    // those six names together would be more intricate than the names
    // themselves, so they are data here, on this record. That is not a blanket
    // argument against generating uids anywhere: dlqChain and featureVectorChain
    // below ARE generation rules, one per family -- DLQ and feature-vector --
    // each covering that family's six per-protocol variants under its OWN
    // regular pattern, with CONN folded in as the unprefixed case rather than a
    // special one (see either method's own comment). The two families' patterns
    // are independent of each other precisely because the mismatched pair of
    // conn uids they each start from never matched. The existing uid strings
    // are unchanged from before this type was made public, wherever they are now
    // assembled -- changing any of them would make Flink silently discard that
    // operator's checkpoint state on restore.
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
            // Built through featureVectorChain so there is exactly one place in
            // this class that knows the conn uids -- the same reason chain 2
            // below is built through dlqChain rather than written out here.
            featureVectorChain(LogType.CONN, featureVectorTopic),
            // Chain 2 -- rejected records. Low volume, and duplicates after a
            // replay are expected rather than prevented. Built through dlqChain so
            // there is exactly one place in this class that knows the conn uids.
            dlqChain(LogType.CONN, dlqTopic)),
            clickHouse);
    }

    // One feature-vector chain for a log type. Mirrors dlqChain immediately
    // below -- a factory rather than three literal strings at each call site,
    // because the uids are checkpoint state identity and hand-writing them per
    // protocol is how a typo silently orphans state.
    //
    // CONN keeps the uids it has always had -- feature-vector-source,
    // feature-vector-row and feature-vectors-clickhouse-sink -- predating any
    // per-protocol naming pattern and not regularised: a running job restores
    // state by looking them up verbatim. Every other log type gets the same
    // prefix pattern dlqChain uses.
    public static LogTypeChain<FeatureVectorRow> featureVectorChain(LogType logType, String topic) {
        // prefix is the whole of the CONN/non-CONN distinction, exactly as in
        // dlqChain below: empty for conn, "<wirename>-" for everything else.
        // Conn's three uids fall out of the same concatenation as every other
        // log type's rather than being special-cased -- and they land on
        // EXACTLY feature-vector-source, feature-vector-row and
        // feature-vectors-clickhouse-sink, the three literal uids this class
        // has always used for its first chain.
        String prefix = logType == LogType.CONN ? "" : logType.wireName() + "-";
        String sourceUid = prefix + "feature-vector-source";
        String mapUid = prefix + "feature-vector-row";
        // Plural ("...-vectors-...") where every other uid in this class is
        // singular. That mismatch predates this method -- Task 12 mirrors it
        // verbatim rather than correcting it, because correcting it would
        // rename a running job's checkpoint state for a purely cosmetic reason.
        String sinkUid = prefix + "feature-vectors-clickhouse-sink";

        // FeatureVectorRowMapFunction is log-type agnostic already (it reads
        // logType back out of the deserialized FeatureVector itself, the same
        // way FeatureVectorSerializer writes it on the online-job side) -- it
        // takes only the topic, unlike InvalidEventRowMapFunction below, which
        // needs logType passed in because RejectedRecord carries no such field.
        return new LogTypeChain<>(topic, new FeatureVectorRowMapFunction(topic), "feature_vectors",
            sourceUid, mapUid, sinkUid);
    }

    // One DLQ chain for a log type. A factory rather than six literal strings at
    // each call site, because the uids are checkpoint state identity and hand-
    // writing them per protocol is how a typo silently orphans state.
    //
    // CONN keeps the uids it has always had. They predate any per-protocol naming
    // pattern and cannot be regularised: a running job restores state by looking
    // them up verbatim. Every other log type gets the pattern.
    public static LogTypeChain<InvalidEventRow> dlqChain(LogType logType, String topic) {
        // prefix is the whole of the CONN/non-CONN distinction: empty for conn,
        // "<wirename>-" for everything else. Conn's three uids therefore fall out
        // of the same concatenation as every other log type's -- they are the
        // unprefixed case, not a special case.
        String prefix = logType == LogType.CONN ? "" : logType.wireName() + "-";
        String sourceUid = prefix + "dlq-source";
        String mapUid = prefix + "invalid-event-row";
        String sinkUid = prefix + "invalid-events-clickhouse-sink";

        return new LogTypeChain<>(topic, new InvalidEventRowMapFunction(topic, logType),
            "invalid_events", sourceUid, mapUid, sinkUid);
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
        // FEATURE_VECTOR_TOPIC and DLQ_TOPIC keep their pre-DNS names and
        // meanings (ruling 12f) -- they were never protocol-qualified, so
        // renaming them now would break an existing deployment for no benefit.
        String bootstrapServers = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
        String featureTopic = System.getenv().getOrDefault("FEATURE_VECTOR_TOPIC", "netsec.conn.feature-vector.v1");
        String dlqTopic = System.getenv().getOrDefault("DLQ_TOPIC", "netsec.conn.dlq.v1");
        String dnsFeatureTopic = System.getenv().getOrDefault("DNS_FEATURE_VECTOR_TOPIC",
            "netsec.dns.feature-vector.v1");
        String dnsDlqTopic = System.getenv().getOrDefault("DNS_DLQ_TOPIC", "netsec.dns.dlq.v1");

        ClickHouseConfig clickHouse = ClickHouseConfig.of(
            System.getenv().getOrDefault("CLICKHOUSE_HOST", "localhost"),
            Integer.parseInt(System.getenv().getOrDefault("CLICKHOUSE_PORT", "8123")),
            System.getenv().getOrDefault("CLICKHOUSE_DATABASE", "netsec_ml"),
            System.getenv().getOrDefault("CLICKHOUSE_USER", "default"),
            System.getenv().getOrDefault("CLICKHOUSE_PASSWORD", ""));

        // Four chains through the parameterised, list-form build(): conn's two
        // (unchanged uids) plus dns's two (the prefix pattern). This is Spec
        // §6.3's "adding a log type is a one-line registration" made real --
        // the pre-DNS 5-argument overload above stays available for the tests
        // that call it directly, but production now registers both protocols.
        build(env, bootstrapServers, List.of(
            featureVectorChain(LogType.CONN, featureTopic),
            dlqChain(LogType.CONN, dlqTopic),
            featureVectorChain(LogType.DNS, dnsFeatureTopic),
            dlqChain(LogType.DNS, dnsDlqTopic)),
            clickHouse);
        env.execute("conn-archive-job");
    }
}
