package io.netsecml.platform.bootstrap.online;

import io.netsecml.platform.adapter.flink.process.ConnFeatureProcessFunction;
import io.netsecml.platform.adapter.flink.process.ConnParseMapValidateFunction;
import io.netsecml.platform.adapter.flink.process.ConnSnapshotExtractFunction;
import io.netsecml.platform.adapter.flink.process.ConnSnapshotJoinFunction;
import io.netsecml.platform.adapter.flink.process.DnsFeatureProcessFunction;
import io.netsecml.platform.adapter.flink.process.DnsParseMapValidateFunction;
import io.netsecml.platform.adapter.flink.process.EventUidKeySelector;
import io.netsecml.platform.adapter.flink.process.ParseMapValidateFunction;
import io.netsecml.platform.adapter.flink.process.RejectedRecord;
import io.netsecml.platform.adapter.flink.process.SnapshotUidKeySelector;
import io.netsecml.platform.adapter.flink.process.SourceKeySelector;
import io.netsecml.platform.adapter.flink.source.RawBytesDeserializationSchema;
import io.netsecml.platform.adapter.kafka.sink.FeatureVectorSerializer;
import io.netsecml.platform.adapter.kafka.sink.RejectedRecordPayload;
import io.netsecml.platform.adapter.kafka.sink.RejectedRecordSerializer;
import io.netsecml.platform.domain.event.NetworkEvent;
import io.netsecml.platform.domain.event.SensorId;
import io.netsecml.platform.domain.feature.ConnSnapshot;
import io.netsecml.platform.domain.feature.FeatureVector;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import java.time.Duration;

public final class OnlineFeatureJob {

    // The conn-only topology, unchanged since before this task. Kept building
    // EXACTLY as it always has -- same uids, same group id, same delivery
    // guarantee -- because OnlineFeatureJobE2ETest and ClickHouseOutageTest (in
    // the separate bootstrap-archive-job module) both call this exact five-
    // argument overload directly and must keep compiling and passing unchanged
    // (Task 12 binding ruling 12a). The two-protocol overload below is
    // additive, not a replacement.
    public static void build(StreamExecutionEnvironment env, String bootstrapServers, String inputTopic,
                              String featureVectorTopic, String dlqTopic, SensorId sensor) {
        // Every operator and sink below gets an explicit, stable .uid(). Without one
        // Flink derives the operator ID from the topology hash, so ANY future edit to
        // this graph silently discards state on restore-from-checkpoint instead of
        // failing loudly -- and THIS job is the one where that risk is real:
        // ConnFeatureProcessFunction holds keyed rolling-window state per
        // (sensor, sourceIp). Assigning uids now is a one-time cost -- it changes
        // operator identity, so an existing checkpoint/savepoint will not restore
        // across this change -- and strictly cheaper to pay now than after more
        // state has accumulated.
        // KNOWN GAP: there is still no TTL on either feature process function's
        // keyed rolling-window state. grep StateTtlConfig / enableTimeToLive
        // across modules/ no longer comes back empty -- ConnSnapshotJoinFunction's
        // "conn-enrichment" state (see that class's open()) added this codebase's
        // first TTL -- but that TTL belongs to a DIFFERENT operator's DIFFERENT
        // state, the conn.log enrichment join, not to the windows below.
        // ConnFeatureProcessFunction's "rolling-counters" and
        // DnsFeatureProcessFunction's "dns-window-state" (wired in the
        // two-protocol overload below) are each still untouched: every state
        // VALUE is bounded, at five one-minute buckets per key, but the KEY SET
        // is not -- every distinct (sensor, sourceIp) ever seen, for either
        // protocol, keeps its own state forever. That is in tension with
        // CLAUDE.md's "Bounded per-(sensor, sourceIp) state only" invariant,
        // which this satisfies per-value but not in aggregate. Adding a TTL to
        // these two is a separate design decision with its own trade-offs and is
        // deliberately not made here.
        DataStream<byte[]> rawStream = rawSource(env, bootstrapServers, inputTopic, "conn-online-job",
            "conn-raw-source");

        SingleOutputStreamOperator<NetworkEvent> parsed = rawStream
            .process(new ConnParseMapValidateFunction(sensor))
            .name("parse-map-validate")
            .uid("parse-map-validate");

        DataStream<FeatureVector> featureVectors = parsed
            .keyBy(new SourceKeySelector())
            .process(new ConnFeatureProcessFunction())
            .name("conn-feature-extraction")
            .uid("conn-feature-extraction");
        sinkFeatureVectors(featureVectors, bootstrapServers, featureVectorTopic, "feature-vector-sink");

        DataStream<RejectedRecord> rejected = parsed.getSideOutput(ParseMapValidateFunction.REJECTED_TAG);
        sinkRejected(rejected, bootstrapServers, dlqTopic, "dlq-sink");
    }

    // One protocol's three Kafka topic names, grouped so the two-protocol
    // build() below takes one argument per protocol instead of six loose
    // Strings that a caller could transpose without the compiler noticing.
    // Nested here (rather than top-level) because it has no meaning outside
    // this job's own wiring.
    public record ProtocolTopics(String input, String featureVector, String dlq) {
    }

    // The two-protocol topology (this unit's task-12 brief pipeline diagram):
    // conn and dns each get their own source -> parse -> ... -> sink chain, the
    // two joined only by the conn.log enrichment snapshot stream that flows
    // from the conn chain into the dns chain's join stage. A second overload
    // rather than a replacement of the one above -- the same pattern
    // ArchiveJob.build already uses for its own two overloads (see that
    // class's comment on why): the conn-only overload must keep building
    // exactly today's topology for its two existing callers, so a genuinely
    // different topology (two sources, a cross-protocol join) gets its own
    // entry point instead of a growing parameter list on the old one.
    //
    // Per ruling 12c, only the three shapes conn and dns genuinely share --
    // the Kafka source, the feature-vector sink, and the DLQ sink -- are
    // factored into the private helpers below. Each protocol's middle stages
    // (parse, feature extraction, and dns's join) stay written out here:
    // dns has a join stage with no conn counterpart, and forcing both into one
    // generic chain method would hide that difference rather than express it.
    public static void build(StreamExecutionEnvironment env, String bootstrapServers,
                              ProtocolTopics conn, ProtocolTopics dns, SensorId sensor) {
        // ---- conn chain: source -> parse -> feature extraction -> sink, plus
        // the DLQ side output and the conn.log snapshot extraction that feeds
        // the dns chain's join below. Identical in shape to the conn-only
        // overload above -- same uids -- because it IS the same chain; the only
        // addition is the snapshot branch dns needs.
        DataStream<byte[]> connRaw = rawSource(env, bootstrapServers, conn.input(), "conn-online-job",
            "conn-raw-source");

        SingleOutputStreamOperator<NetworkEvent> connParsed = connRaw
            .process(new ConnParseMapValidateFunction(sensor))
            .name("parse-map-validate")
            .uid("parse-map-validate");

        DataStream<FeatureVector> connFeatureVectors = connParsed
            .keyBy(new SourceKeySelector())
            .process(new ConnFeatureProcessFunction())
            .name("conn-feature-extraction")
            .uid("conn-feature-extraction");
        sinkFeatureVectors(connFeatureVectors, bootstrapServers, conn.featureVector(), "feature-vector-sink");

        DataStream<RejectedRecord> connRejected = connParsed.getSideOutput(ParseMapValidateFunction.REJECTED_TAG);
        sinkRejected(connRejected, bootstrapServers, conn.dlq(), "dlq-sink");

        // The conn.log enrichment producer
        // (docs/superpowers/specs/2026-09-10-per-protocol-feature-schemas-design.md
        // section 6.2): reads the SAME parsed conn stream the feature chain
        // above reads, so one parsed conn record feeds both the feature sink
        // and this extraction independently -- neither branch feeds the other.
        DataStream<ConnSnapshot> connSnapshots = connParsed
            .flatMap(new ConnSnapshotExtractFunction())
            .name("conn-snapshot-extract")
            .uid("conn-snapshot-extract");

        // ---- dns chain: its own source and parse stage, then the enrichment
        // join against the conn snapshots above, then feature extraction and
        // sink -- the shape the pipeline diagram in this unit's task-12 brief
        // draws as two sources converging on one join.
        DataStream<byte[]> dnsRaw = rawSource(env, bootstrapServers, dns.input(), "dns-online-job",
            "dns-raw-source");

        SingleOutputStreamOperator<NetworkEvent> dnsParsed = dnsRaw
            .process(new DnsParseMapValidateFunction(sensor))
            .name("dns-parse-map-validate")
            .uid("dns-parse-map-validate");

        // The enrichment join itself (spec section 6.2 above): dns is input 1,
        // the conn snapshot stream is input 2, both keyed on connection uid so
        // ConnSnapshotJoinFunction's keyed state partitions the two inputs onto
        // the same slot per connection. NEVER blocks on conn.log arriving --
        // see that class's own javadoc for why blocking would be a correctness
        // failure, not merely a latency cost.
        SingleOutputStreamOperator<NetworkEvent> dnsEnriched = dnsParsed
            .connect(connSnapshots)
            .keyBy(new EventUidKeySelector(), new SnapshotUidKeySelector())
            .process(new ConnSnapshotJoinFunction())
            .name("dns-conn-enrichment")
            .uid("dns-conn-enrichment");

        DataStream<FeatureVector> dnsFeatureVectors = dnsEnriched
            .keyBy(new SourceKeySelector())
            .process(new DnsFeatureProcessFunction())
            .name("dns-feature-extraction")
            .uid("dns-feature-extraction");
        sinkFeatureVectors(dnsFeatureVectors, bootstrapServers, dns.featureVector(), "dns-feature-vector-sink");

        // dns's own rejects, from the dns parse stage's side output. This reads
        // the SAME static REJECTED_TAG the conn chain read above, but a side
        // output is scoped to the OPERATOR INSTANCE it is read from, not
        // globally by tag, so this is dns's rejects only -- never a mix of both
        // chains' -- exactly as the pipeline diagram shows two separate DLQ
        // arrows, one per parse stage.
        DataStream<RejectedRecord> dnsRejected = dnsParsed.getSideOutput(ParseMapValidateFunction.REJECTED_TAG);
        sinkRejected(dnsRejected, bootstrapServers, dns.dlq(), "dns-dlq-sink");
    }

    // Shared shape 1 of 3 (ruling 12c): a byte[] Kafka source differing only in
    // topic, consumer group and uid. Both protocols read raw Zeek JSON lines
    // off an external topic identically -- there is no protocol-specific
    // behaviour here to hide by extracting it, unlike the parse/feature/join
    // stages above, which stay written out per protocol.
    private static DataStream<byte[]> rawSource(StreamExecutionEnvironment env, String bootstrapServers,
                                                  String topic, String groupId, String uid) {
        KafkaSource<byte[]> source = KafkaSource.<byte[]>builder()
            .setBootstrapServers(bootstrapServers)
            .setTopics(topic)
            .setGroupId(groupId)
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(new RawBytesDeserializationSchema())
            .build();
        return env.fromSource(source, WatermarkStrategy.noWatermarks(), uid).uid(uid);
    }

    // Shared shape 2 of 3: publish a FeatureVector to its protocol's
    // feature-vector topic.
    //
    // The serialization schema stays an explicit anonymous class, never a
    // lambda: Flink extracts FeatureVector as this interface's generic
    // parameter by reflecting over the anonymous class, and a lambda erases
    // that type parameter -- InvalidTypesException at job submission, not at
    // compile time. This is the same defect class that made main() unable to
    // run at all before fix/flink-job-serializability; keeping this an
    // explicit class is what stops it recurring here.
    private static void sinkFeatureVectors(DataStream<FeatureVector> vectors, String bootstrapServers,
                                            String topic, String uid) {
        FeatureVectorSerializer featureSerializer = new FeatureVectorSerializer();
        KafkaSink<FeatureVector> sink = KafkaSink.<FeatureVector>builder()
            .setBootstrapServers(bootstrapServers)
            .setRecordSerializer(KafkaRecordSerializationSchema.<FeatureVector>builder()
                .setTopic(topic)
                .setValueSerializationSchema(new SerializationSchema<FeatureVector>() {
                    @Override
                    public byte[] serialize(FeatureVector vector) {
                        return featureSerializer.serialize(topic, vector);
                    }
                })
                .build())
            .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
            .build();
        vectors.sinkTo(sink).name(uid).uid(uid);
    }

    // Shared shape 3 of 3: publish a RejectedRecord to its protocol's DLQ
    // topic. Explicit anonymous SerializationSchema for the same reason as
    // sinkFeatureVectors above -- kept with this helper rather than only
    // stated once, because the next editor to touch either helper in
    // isolation still needs the warning in front of them.
    private static void sinkRejected(DataStream<RejectedRecord> rejected, String bootstrapServers,
                                       String topic, String uid) {
        RejectedRecordSerializer rejectedSerializer = new RejectedRecordSerializer();
        KafkaSink<RejectedRecord> sink = KafkaSink.<RejectedRecord>builder()
            .setBootstrapServers(bootstrapServers)
            .setRecordSerializer(KafkaRecordSerializationSchema.<RejectedRecord>builder()
                .setTopic(topic)
                .setValueSerializationSchema(new SerializationSchema<RejectedRecord>() {
                    @Override
                    public byte[] serialize(RejectedRecord r) {
                        // stage comes from the domain's ReasonCode, so the archive
                        // adapter never has to re-derive it from the reason name.
                        return rejectedSerializer.serialize(topic,
                            new RejectedRecordPayload(r.rawPayload(), r.eventId(), r.reason().stage().name(),
                                r.reason().name(), r.detail(), r.receivedAt()));
                    }
                })
                .build())
            .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
            .build();
        rejected.sinkTo(sink).name(uid).uid(uid);
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Flink 2.x removed StreamExecutionEnvironment.setRestartStrategy, so the restart
        // strategy is configuration-only now. Applied before enableCheckpointing below so
        // the explicit checkpoint settings are the last word regardless of what configure()
        // reads out of this Configuration. Without this the default with checkpointing on
        // would restart forever; failure-rate stops hot-looping a broken deployment.
        Configuration restartConfig = new Configuration();
        restartConfig.set(RestartStrategyOptions.RESTART_STRATEGY,
            RestartStrategyOptions.RestartStrategyType.FAILURE_RATE.getMainValue());
        restartConfig.set(RestartStrategyOptions.RESTART_STRATEGY_FAILURE_RATE_MAX_FAILURES_PER_INTERVAL, 3);
        restartConfig.set(RestartStrategyOptions.RESTART_STRATEGY_FAILURE_RATE_FAILURE_RATE_INTERVAL, Duration.ofMinutes(10));
        restartConfig.set(RestartStrategyOptions.RESTART_STRATEGY_FAILURE_RATE_DELAY, Duration.ofSeconds(10));
        env.configure(restartConfig);

        // This job is stateful -- the keyed rolling windows in
        // ConnFeatureProcessFunction and DnsFeatureProcessFunction, and the
        // enrichment join's conn-snapshot state, only survive a failure if
        // there are checkpoints to restore them from. With checkpointing off
        // the default strategy is no-restart, so any transient error kills
        // the job outright and loses that state; with it on, an in-run failure
        // restores state and source offsets together. The at-least-once Kafka sinks also
        // flush on the checkpoint barrier, so their durability is tied to this too.
        // Values are FINAL_ARCHITECTURE.md's initial settings -- benchmark, do not canonize.
        env.enableCheckpointing(30_000L);
        CheckpointConfig checkpoints = env.getCheckpointConfig();
        checkpoints.setMinPauseBetweenCheckpoints(10_000L);
        checkpoints.setCheckpointTimeout(120_000L);
        checkpoints.setMaxConcurrentCheckpoints(1);

        String bootstrapServers = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
        String sensorId = System.getenv().getOrDefault("SENSOR_ID", "sensor-default");

        // CONN_INPUT_TOPIC, FEATURE_VECTOR_TOPIC and DLQ_TOPIC keep their
        // pre-DNS names and meanings (ruling 12f) -- renaming them would break
        // any deployment's existing configuration for no benefit, since they
        // were never protocol-qualified to begin with.
        ProtocolTopics conn = new ProtocolTopics(
            System.getenv().getOrDefault("CONN_INPUT_TOPIC", "conn"),
            System.getenv().getOrDefault("FEATURE_VECTOR_TOPIC", "netsec.conn.feature-vector.v1"),
            System.getenv().getOrDefault("DLQ_TOPIC", "netsec.conn.dlq.v1"));
        ProtocolTopics dns = new ProtocolTopics(
            System.getenv().getOrDefault("DNS_INPUT_TOPIC", "dns"),
            System.getenv().getOrDefault("DNS_FEATURE_VECTOR_TOPIC", "netsec.dns.feature-vector.v1"),
            System.getenv().getOrDefault("DNS_DLQ_TOPIC", "netsec.dns.dlq.v1"));

        build(env, bootstrapServers, conn, dns, new SensorId(sensorId));
        env.execute("conn-online-feature-job");
    }
}
