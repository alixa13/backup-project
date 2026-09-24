package io.netsecml.platform.bootstrap.online;

import io.netsecml.platform.adapter.flink.process.ConnFeatureProcessFunction;
import io.netsecml.platform.adapter.flink.process.ConnParseMapValidateFunction;
import io.netsecml.platform.adapter.flink.process.ConnSnapshotExtractFunction;
import io.netsecml.platform.adapter.flink.process.ConnSnapshotJoinFunction;
import io.netsecml.platform.adapter.flink.process.DnsFeatureProcessFunction;
import io.netsecml.platform.adapter.flink.process.DnsParseMapValidateFunction;
import io.netsecml.platform.adapter.flink.process.EventUidKeySelector;
import io.netsecml.platform.adapter.flink.process.ModbusEntityKeySelector;
import io.netsecml.platform.adapter.flink.process.ModbusFeatureProcessFunction;
import io.netsecml.platform.adapter.flink.process.ModbusParseMapValidateFunction;
import io.netsecml.platform.adapter.flink.process.ParseMapValidateFunction;
import io.netsecml.platform.adapter.flink.process.RejectedRecord;
import io.netsecml.platform.adapter.flink.process.SnapshotUidKeySelector;
import io.netsecml.platform.adapter.flink.process.SourceKeySelector;
import io.netsecml.platform.adapter.flink.source.RawBytesDeserializationSchema;
import io.netsecml.platform.adapter.kafka.sink.FeatureVectorSerializer;
import io.netsecml.platform.adapter.kafka.sink.RejectedRecordPayload;
import io.netsecml.platform.adapter.kafka.sink.RejectedRecordSerializer;
import io.netsecml.platform.domain.event.ConnEvent;
import io.netsecml.platform.domain.event.DnsEvent;
import io.netsecml.platform.domain.event.ModbusEvent;
import io.netsecml.platform.domain.event.S7commEvent;
import io.netsecml.platform.domain.event.NetworkEvent;
import io.netsecml.platform.domain.event.SensorId;
import io.netsecml.platform.domain.feature.ConnSnapshot;
import io.netsecml.platform.domain.feature.FeatureVector;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
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

    // The conn-only topology: delegates entirely into connChain below, so
    // conn's wiring is defined in exactly one place -- the same method the
    // two-protocol overload calls for its own conn half, below. Kept as its
    // own entry point rather than folded away because OnlineFeatureJobE2ETest
    // and ClickHouseOutageTest (in the separate bootstrap-archive-job module)
    // both call this exact five-argument signature directly and must keep
    // compiling and passing against exactly today's conn-only topology. The
    // returned parsed-conn stream is discarded here; only the two-protocol
    // overload needs it, to feed dns's join.
    public static void build(StreamExecutionEnvironment env, String bootstrapServers, String inputTopic,
                              String featureVectorTopic, String dlqTopic, SensorId sensor) {
        connChain(env, bootstrapServers, new ProtocolTopics(inputTopic, featureVectorTopic, dlqTopic), sensor);
    }

    // One protocol's three Kafka topic names, grouped so the two-protocol
    // build() below takes one argument per protocol instead of six loose
    // Strings that a caller could transpose without the compiler noticing.
    // Nested here (rather than top-level) because it has no meaning outside
    // this job's own wiring.
    public record ProtocolTopics(String input, String featureVector, String dlq) {
    }

    // The two-protocol topology: conn and dns each get their own source ->
    // parse -> ... -> sink chain, the two joined only by the conn.log
    // enrichment snapshot stream that flows from the conn chain into the dns
    // chain's join stage. A second overload rather than a replacement of the
    // one above, because the conn-only overload's two existing callers
    // (OnlineFeatureJobE2ETest, and ClickHouseOutageTest in the separate
    // bootstrap-archive-job module) must keep building exactly today's
    // conn-only topology. Both overloads assemble conn's chain through the
    // SAME connChain(...) helper below, so the two topologies cannot drift
    // apart from each other -- there is exactly one place that wires conn's
    // source, parse, feature extraction and sinks.
    //
    // Only the three shapes conn and dns genuinely share -- the Kafka source,
    // the feature-vector sink, and the DLQ sink -- are factored into private
    // helpers. Each protocol's middle stages (parse, feature extraction, and
    // dns's join) stay written out: dns has a join stage with no conn
    // counterpart, and folding both into one generic chain method would hide
    // that difference rather than express it.
    //
    // KNOWN GAP: neither ConnFeatureProcessFunction's "rolling-counters" state
    // nor DnsFeatureProcessFunction's "dns-window-state" carries a TTL -- the
    // two rolling-window stages this overload puts on the graph, one via
    // connChain below and one wired directly further down. grep
    // StateTtlConfig / enableTimeToLive across modules/ no longer comes back
    // empty -- ConnSnapshotJoinFunction's "conn-enrichment" state (see that
    // class's open()) added this codebase's first TTL -- but that TTL belongs
    // to a DIFFERENT operator's DIFFERENT state, the conn.log enrichment
    // join, not to either window here. Each window's state VALUE is bounded,
    // at five one-minute buckets per key, but the KEY SET is not -- every
    // distinct (sensor, sourceIp) ever seen, for either protocol, keeps its
    // own state forever. That is in tension with CLAUDE.md's "Bounded
    // per-(sensor, sourceIp) state only" invariant, which this satisfies
    // per-value but not in aggregate. Adding a TTL to these two is a separate
    // design decision with its own trade-offs and is deliberately not made
    // here.
    //
    // KNOWN SEAM: this method's own signature -- build(conn, dns, sensor) --
    // is shaped and named for exactly two protocols, not for the general N.
    // A third protocol is a new parameter and a new copy of dns's wiring
    // pattern here, not a loop over a list the way ArchiveJob.build(chains)
    // already handles an arbitrary log type set on the archive side. The next
    // protocol's unit should budget for that asymmetry, not assume this method
    // merely grows another argument for free.
    public static void build(StreamExecutionEnvironment env, String bootstrapServers,
                              ProtocolTopics conn, ProtocolTopics dns, SensorId sensor) {
        SingleOutputStreamOperator<NetworkEvent> connParsed = connChain(env, bootstrapServers, conn, sensor);

        // The conn.log enrichment producer
        // (docs/superpowers/specs/2026-09-10-per-protocol-feature-schemas-design.md
        // section 6.2): reads the SAME parsed conn stream connChain returns,
        // so one parsed conn record feeds both the feature sink inside
        // connChain and this extraction independently -- neither branch feeds
        // the other.
        DataStream<ConnSnapshot> connSnapshots = connParsed
            .flatMap(new ConnSnapshotExtractFunction())
            .name("conn-snapshot-extract")
            .uid("conn-snapshot-extract");

        // dns's own source and parse stage, then the enrichment join against
        // the conn snapshots above, then feature extraction and sink -- two
        // sources converging on one join; conn's own chain has no join stage
        // at all.
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

        // dns's own rejects, from the dns parse stage's side output. This
        // reads the SAME static REJECTED_TAG connChain's conn parse stage
        // reads from too, but a side output is scoped to the OPERATOR
        // INSTANCE it is read from, not globally by tag, so this is dns's
        // rejects only -- never a mix of both chains'.
        DataStream<RejectedRecord> dnsRejected = dnsParsed.getSideOutput(ParseMapValidateFunction.REJECTED_TAG);
        sinkRejected(dnsRejected, bootstrapServers, dns.dlq(), "dns-dlq-sink");
    }

    // The three-protocol topology: everything the two-protocol build() above
    // wires, plus modbus's own chain bolted on afterward. A third overload
    // rather than a third loose ProtocolTopics parameter grafted onto the
    // two-protocol signature above, because that signature's own two existing
    // callers (OnlineFeatureJobTopologyTest's two-protocol cases, and this
    // job's own conn-only overload's callers, transitively) must keep
    // compiling unchanged -- exactly the same reasoning the two-protocol
    // overload's own comment gives for not replacing the conn-only one.
    //
    // Delegates into the two-protocol build() for conn+dns rather than
    // duplicating their wiring here: conn and dns's chains are unaffected by
    // modbus's presence (modbus has no enrichment join, so it reads nothing
    // the conn/dns chains produce, and nothing it produces feeds them), so
    // there is exactly one place -- the two-protocol build() -- that wires
    // conn and dns, and exactly one place -- modbusChain below -- that wires
    // modbus.
    //
    // KNOWN SEAM (inherited from the two-protocol build() above, now a
    // fourth protocol's problem too): a fourth protocol is a new overload and
    // a new copy of modbus's wiring pattern, not a signature this method
    // merely grows another argument on.
    public static void build(StreamExecutionEnvironment env, String bootstrapServers,
                              ProtocolTopics conn, ProtocolTopics dns, ProtocolTopics modbus, SensorId sensor) {
        build(env, bootstrapServers, conn, dns, sensor);
        modbusChain(env, bootstrapServers, modbus, sensor);
    }

    // Modbus's own chain: source -> parse -> narrow -> feature extraction ->
    // sink, plus modbus's own DLQ side output and sink. Unlike dns, modbus has
    // no conn.log enrichment join, so this chain's shape mirrors connChain's
    // own five-stage shape rather than the two-protocol build()'s dns half --
    // but it cannot BE connChain, because modbus's KeySelector and
    // KeyedProcessFunction (ModbusEntityKeySelector, ModbusFeatureProcessFunction)
    // are typed and keyed directly on ModbusEvent, never on NetworkEvent the
    // way SourceKeySelector and Conn/DnsFeatureProcessFunction are -- see
    // those two classes' own comments for why. That typing difference is what
    // the narrow stage below exists to bridge.
    private static void modbusChain(StreamExecutionEnvironment env, String bootstrapServers,
                                     ProtocolTopics modbus, SensorId sensor) {
        DataStream<byte[]> modbusRaw = rawSource(env, bootstrapServers, modbus.input(), "modbus-online-job",
            "modbus-source");

        SingleOutputStreamOperator<NetworkEvent> modbusParsed = modbusRaw
            .process(new ModbusParseMapValidateFunction(sensor))
            .name("modbus-parse")
            .uid("modbus-parse");

        // Bridges ParseMapValidateFunction's fixed NetworkEvent output (shared
        // by every subclass, modbus's included -- see that base class's own
        // comment) down to the ModbusEvent type ModbusEntityKeySelector and
        // ModbusFeatureProcessFunction require. This is a structural
        // consequence of modbus's operators being typed on ModbusEvent rather
        // than NetworkEvent -- a sixth, stateless uid alongside the five
        // stateful/checkpointed modbus stages: it carries no keyed state of
        // its own, so a uid rename here (unlike those five) costs nothing on
        // restore, but it still gets an explicit one per this job's own
        // "every operator gets a stable uid" rule.
        DataStream<ModbusEvent> modbusEvents = modbusParsed
            .map(new NarrowToModbusEvent())
            .name("modbus-event-narrow")
            .uid("modbus-event-narrow");

        DataStream<FeatureVector> modbusFeatureVectors = modbusEvents
            .keyBy(new ModbusEntityKeySelector())
            .process(new ModbusFeatureProcessFunction())
            .name("modbus-features")
            .uid("modbus-features");
        sinkFeatureVectors(modbusFeatureVectors, bootstrapServers, modbus.featureVector(), "modbus-sink");

        DataStream<RejectedRecord> modbusRejected =
            modbusParsed.getSideOutput(ParseMapValidateFunction.REJECTED_TAG);
        sinkRejected(modbusRejected, bootstrapServers, modbus.dlq(), "modbus-dlq-sink");
    }

    // The narrowing bridge modbusChain's own comment above describes. A named
    // static class rather than a lambda, matching every other KeySelector/
    // MapFunction in this job and its process package (SourceKeySelector,
    // ConnSnapshotExtractFunction, and so on) -- none of them are lambdas.
    // Exhaustive over NetworkEvent's sealed permits with no default arm, per
    // this project's rule (see SourceKeySelector's and
    // ConnSnapshotExtractFunction's own ModbusEvent/DnsEvent arms): a
    // ConnEvent or DnsEvent reaching here is a wiring error, not a runtime
    // condition -- modbus-parse above, built from ModbusParseMapValidateFunction,
    // can only ever have produced a ModbusEvent.
    //
    // WHERE THE NARROWING HAPPENS differs between the protocols, so the next
    // protocol's author should choose between the two shapes deliberately.
    // ParseMapValidateFunction emits NetworkEvent for every subclass, so every
    // chain has to get from NetworkEvent to its own event type somewhere:
    //   - conn and dns narrow INSIDE each operator that consumes their parsed
    //     stream: SourceKeySelector (which the two share),
    //     ConnFeatureProcessFunction, DnsFeatureProcessFunction, and on dns's
    //     enrichment path ConnSnapshotExtractFunction and
    //     ConnSnapshotJoinFunction. Each is typed on NetworkEvent and switches
    //     over its sealed permits, with a throw arm for every event type its
    //     chain never receives -- so each one needed a new ModbusEvent arm when
    //     ModbusEvent joined the permits. No extra operator sits on the graph,
    //     and one selector can serve both protocols.
    //   - modbus narrows ONCE, here, at the chain's boundary, so
    //     ModbusEntityKeySelector and ModbusFeatureProcessFunction are typed on
    //     ModbusEvent and carry no throw arms of their own; the throw arms for
    //     the other event types live in this one switch instead. This adds one
    //     stateless operator (and its uid) to the graph, and those two
    //     operators accept only a ModbusEvent stream, so they cannot be shared
    //     with another protocol the way SourceKeySelector is.
    private static final class NarrowToModbusEvent implements MapFunction<NetworkEvent, ModbusEvent> {
        @Override
        public ModbusEvent map(NetworkEvent event) {
            return switch (event) {
                case ModbusEvent modbusEvent -> modbusEvent;
                case S7commEvent ignored -> throw new IllegalStateException(
                    "modbus chain received an S7commEvent; modbus-parse can only ever produce a ModbusEvent "
                    + "and this is a wiring error, not a runtime condition");
                case ConnEvent ignored -> throw new IllegalStateException(
                    "modbus chain received a ConnEvent; modbus-parse can only ever produce a ModbusEvent "
                    + "and this is a wiring error, not a runtime condition");
                case DnsEvent ignored -> throw new IllegalStateException(
                    "modbus chain received a DnsEvent; modbus-parse can only ever produce a ModbusEvent "
                    + "and this is a wiring error, not a runtime condition");
            };
        }
    }

    // Builds conn's entire chain -- source, parse, feature extraction, the
    // feature-vector sink, and the DLQ side output plus sink -- and returns
    // the parsed stream so a caller that needs to branch off it (the
    // two-protocol overload above, for dns's join) can. The conn-only
    // overload above calls this too and discards the return value. One
    // method assembling conn's chain for both overloads is what keeps them
    // from drifting apart from each other.
    //
    // Every operator and sink assigned a uid here gets an explicit, stable
    // one. Without one, Flink derives the operator id from the topology hash,
    // so ANY future edit to this graph silently discards state on restore-
    // from-checkpoint instead of failing loudly. That risk is concrete in
    // THIS job -- ConnFeatureProcessFunction holds keyed rolling-window state
    // per (sensor, sourceIp), and so do DnsFeatureProcessFunction's
    // "dns-window-state" and ConnSnapshotJoinFunction's "conn-enrichment" on
    // the dns side of the same job (see the KNOWN GAP comment on the
    // two-protocol build() above) -- unlike the separate archive job
    // (ArchiveJob), whose own uid comment notes it carries no keyed state at
    // all, only a source's committed offset and a sink's in-flight batch,
    // both of which replay safely from Kafka.
    private static SingleOutputStreamOperator<NetworkEvent> connChain(StreamExecutionEnvironment env,
            String bootstrapServers, ProtocolTopics conn, SensorId sensor) {
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

        return connParsed;
    }

    // Shared shape 1 of 3: a byte[] Kafka source differing only in topic,
    // consumer group and uid. Both protocols read raw Zeek JSON lines off an
    // external topic identically -- there is no protocol-specific behaviour
    // here to hide by extracting it, unlike the parse/feature/join stages
    // above, which stay written out per protocol.
    private static DataStream<byte[]> rawSource(StreamExecutionEnvironment env, String bootstrapServers,
                                                  String topic, String groupId, String uid) {
        // KNOWN LIMIT: earliest(), unconditionally -- unlike ArchiveJob's source,
        // which resumes from committedOffsets(EARLIEST). An online restart with
        // no usable checkpoint therefore replays this topic's entire retention
        // window rather than picking up from wherever this consumer group last
        // committed, which compounds the enrichment join's processing-time TTL
        // gap (see ConnSnapshotJoinFunction's own TTL comment): more replayed
        // history means more distinct connection uids accumulating in that
        // join's keyed state before the TTL can fire on any of them.
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
        // pre-DNS names and meanings -- renaming them would break any
        // deployment's existing configuration for no benefit, since they were
        // never protocol-qualified to begin with.
        ProtocolTopics conn = new ProtocolTopics(
            System.getenv().getOrDefault("CONN_INPUT_TOPIC", "conn"),
            System.getenv().getOrDefault("FEATURE_VECTOR_TOPIC", "netsec.conn.feature-vector.v1"),
            System.getenv().getOrDefault("DLQ_TOPIC", "netsec.conn.dlq.v1"));
        ProtocolTopics dns = new ProtocolTopics(
            System.getenv().getOrDefault("DNS_INPUT_TOPIC", "dns"),
            System.getenv().getOrDefault("DNS_FEATURE_VECTOR_TOPIC", "netsec.dns.feature-vector.v1"),
            System.getenv().getOrDefault("DNS_DLQ_TOPIC", "netsec.dns.dlq.v1"));
        // MODBUS_RAW_TOPIC (not *_INPUT_TOPIC): modbus's input topic was never
        // an existing deployment's bare wire name the way conn's/dns's are --
        // it is the newer netsec.modbus.raw.v1 naming, so its env var follows
        // that naming from the start rather than inheriting conn/dns's older
        // pre-protocol-qualified convention.
        ProtocolTopics modbus = new ProtocolTopics(
            System.getenv().getOrDefault("MODBUS_RAW_TOPIC", "netsec.modbus.raw.v1"),
            System.getenv().getOrDefault("MODBUS_FEATURE_VECTOR_TOPIC", "netsec.modbus.feature-vector.v1"),
            System.getenv().getOrDefault("MODBUS_DLQ_TOPIC", "netsec.modbus.dlq.v1"));

        build(env, bootstrapServers, conn, dns, modbus, new SensorId(sensorId));
        env.execute("online-feature-job");
    }
}
