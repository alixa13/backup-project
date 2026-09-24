package io.netsecml.platform.bootstrap.online;

import io.netsecml.platform.adapter.kafka.sink.FeatureVectorDeserializer;
import io.netsecml.platform.domain.event.LogType;
import io.netsecml.platform.domain.event.SensorId;
import io.netsecml.platform.domain.feature.DnsFeatureSchemaV1;
import io.netsecml.platform.domain.feature.FeatureVector;
import io.netsecml.platform.domain.feature.ModbusFeatureSchemaV1;
import io.netsecml.platform.domain.feature.QualityFlags;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers(disabledWithoutDocker = true)
class OnlineFeatureJobE2ETest {
    @Container
    private final ConfluentKafkaContainer kafka =
        new ConfluentKafkaContainer("confluentinc/cp-kafka:7.6.1")
            // Testcontainers waits 60 s by default for the broker to log its
            // transition to RUNNING. On a slow machine this image needs longer
            // than that just to reach SharedServer startup, so the container is
            // killed mid-boot and the test fails for no reason of its own.
            .withStartupTimeout(Duration.ofMinutes(3));

    @Test
    void connFixtureFlowsToFeatureVectorTopic() throws Exception {
        kafka.start();
        String bootstrapServers = kafka.getBootstrapServers();
        String inputTopic = "netsec.conn.raw.v1";
        String featureTopic = "netsec.conn.feature-vector.v1";
        String dlqTopic = "netsec.conn.dlq.v1";

        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(producerProps)) {
            byte[] payload = Files.readAllBytes(Paths.get("..", "..", "tests", "fixtures", "zeek_conn", "valid-tcp-ssl.json"));
            producer.send(new ProducerRecord<>(inputTopic, payload)).get();
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        OnlineFeatureJob.build(env, bootstrapServers, inputTopic, featureTopic, dlqTopic, new SensorId("sensor-eu-1"));

        // Submitted on the test thread itself, mirroring ArchiveJobE2ETest: executeAsync returns as
        // soon as the job is handed to the (embedded) cluster, so a helper thread buys nothing here but
        // a JobClient this method could never reach in order to cancel it below.
        JobClient job = env.executeAsync("conn-foundation-e2e-test");

        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "e2e-test-reader");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // cancelJob is declared before consumer so try-with-resources closes it LAST (resources close in
        // the reverse of their declaration order): the consumer stops reading before the job it reads
        // from is cancelled. An AutoCloseable rather than a plain try/finally so a cancellation failure
        // is recorded as a suppressed exception instead of masking an assertion failure from the body.
        try (AutoCloseable cancelJob = () -> job.cancel().get(60, TimeUnit.SECONDS);
             Consumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
            consumer.subscribe(List.of(featureTopic));
            long deadline = System.currentTimeMillis() + 60_000;
            List<String> collected = new ArrayList<>();
            while (collected.isEmpty() && System.currentTimeMillis() < deadline) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(2));
                records.forEach(r -> collected.add(r.value()));
            }
            assertTrue(collected.size() >= 1, "expected at least one feature vector published within 60s");
            assertTrue(collected.get(0).contains("\"schemaId\":\"conn-feature-v1\""));
            assertTrue(collected.get(0).contains("sensor-eu-1:Cabc123XYZ"));

            // The two envelope fields the archive job depends on. Without these
            // assertions the wire contract could lose them and this test would
            // still pass, since every check above is a substring match.
            assertTrue(collected.get(0).contains("\"sensor\":\"sensor-eu-1\""),
                "the published record must carry the sensor as its own field");
            assertTrue(collected.get(0).contains("\"producedAt\":"),
                "the published record must carry an emission timestamp for row_version");
        }
    }

    // Drives OnlineFeatureJob's two-protocol build() overload -- conn and dns
    // running side by side, joined by the conn.log enrichment snapshot stream --
    // against a real broker. connFixtureFlowsToFeatureVectorTopic above only ever
    // exercises the conn-only overload; nothing before this test has run this
    // topology's dns half, its join, or its dns dlq sink against Kafka at all.
    @Test
    void twoProtocolJobEnrichesJoinedDnsRecordFlagsOrphanAndRoutesMalformedDnsToDlq() throws Exception {
        kafka.start();
        String bootstrapServers = kafka.getBootstrapServers();

        String connInputTopic = "netsec.conn.raw.v1";
        String connFeatureTopic = "netsec.conn.feature-vector.v1";
        String connDlqTopic = "netsec.conn.dlq.v1";
        String dnsInputTopic = "netsec.dns.raw.v1";
        String dnsFeatureTopic = "netsec.dns.feature-vector.v1";
        String dnsDlqTopic = "netsec.dns.dlq.v1";

        // Both input topics must exist before the job below is submitted: a
        // KafkaSource subscribed to a not-yet-created topic fails its enumerator,
        // and conn and dns share one StreamExecutionEnvironment here, so a dns
        // topic missing at submission time would fail the conn chain too. The
        // malformed dns record is published now, alongside conn's, for the same
        // reason -- there is no later dns-only submission step to defer it to.
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(producerProps)) {
            producer.send(new ProducerRecord<>(connInputTopic, fixture("zeek_conn", "valid-udp-dns.json"))).get();
            producer.send(new ProducerRecord<>(dnsInputTopic, fixture("zeek_dns", "malformed.json"))).get();
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        OnlineFeatureJob.build(env, bootstrapServers,
            new OnlineFeatureJob.ProtocolTopics(connInputTopic, connFeatureTopic, connDlqTopic),
            new OnlineFeatureJob.ProtocolTopics(dnsInputTopic, dnsFeatureTopic, dnsDlqTopic),
            new SensorId("sensor-eu-1"));

        // Submitted on the test thread itself -- see the comment on the same call
        // in connFixtureFlowsToFeatureVectorTopic above.
        JobClient job = env.executeAsync("two-protocol-online-job-e2e-test");

        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "two-protocol-e2e-test-reader");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // byte[] values, not String: FeatureVectorDeserializer -- the same wire-contract
        // reader the archive job uses -- takes the raw bytes, not a String, so decoding
        // here would just be undone a moment later.
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());

        FeatureVectorDeserializer featureVectorDeserializer = new FeatureVectorDeserializer();
        Map<String, FeatureVector> vectorsByEventId = new HashMap<>();
        List<String> dnsDlqPayloads = new ArrayList<>();

        // cancelJob is declared before consumer so try-with-resources closes it LAST (resources close in
        // the reverse of their declaration order): the consumer stops reading before the job it reads
        // from is cancelled. An AutoCloseable rather than a plain try/finally so a cancellation failure
        // is recorded as a suppressed exception instead of masking an assertion failure from the body.
        try (AutoCloseable cancelJob = () -> job.cancel().get(60, TimeUnit.SECONDS);
             Consumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerProps)) {
            consumer.subscribe(List.of(connFeatureTopic, dnsFeatureTopic, dnsDlqTopic));

            // Wait for the conn feature vector specifically, not just any record on
            // this topic, before publishing either dns record below.
            // ConnParseMapValidateFunction's parsed output feeds both this feature
            // branch and, independently, the snapshot branch ConnSnapshotExtractFunction
            // reads (OnlineFeatureJob wires both off that same parsed stream). Seeing
            // the vector therefore proves the matching snapshot has also left the
            // parse stage, while the dns records below still have to travel
            // test -> broker -> dns source -> parse -> join -- publishing them only
            // now is what keeps the join from racing its own enrichment input.
            String connEventId = "sensor-eu-1:Cdef456UVW";
            long connDeadline = System.currentTimeMillis() + 60_000;
            while (!vectorsByEventId.containsKey(connEventId) && System.currentTimeMillis() < connDeadline) {
                collectRecords(consumer.poll(Duration.ofSeconds(2)), dnsDlqTopic, featureVectorDeserializer,
                    vectorsByEventId, dnsDlqPayloads);
            }
            assertTrue(vectorsByEventId.containsKey(connEventId),
                "expected the conn feature vector " + connEventId + " to arrive within 60s");

            // In production this order is backwards: Zeek writes a connection's
            // conn.log line only when the connection ends (for UDP, after an
            // inactivity timeout), so a dns.log record for that connection normally
            // reaches this pipeline BEFORE conn.log does, and ConnSnapshotJoinFunction
            // usually finds no snapshot yet -- see that class's own "AN HONEST LIMIT"
            // comment. Publishing conn first and waiting for its vector above is
            // deliberate and different: it is what lets this test prove the
            // enrichment join itself works end to end against a real broker, not an
            // attempt to model Zeek's actual write order. The two dns records are
            // published in event-time order (the shared fixture's ts, then
            // valid-zeek-shaped.json's, which is later) because both carry
            // id_orig_h 10.0.0.5 and so share one rolling-window key -- publishing
            // them any other way would leave this test quietly depending on
            // out-of-order handling it never intended to cover.
            try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(producerProps)) {
                producer.send(new ProducerRecord<>(dnsInputTopic,
                    fixture("zeek_dns", "valid-shares-uid-with-conn-udp-dns.json"))).get();
                producer.send(new ProducerRecord<>(dnsInputTopic,
                    fixture("zeek_dns", "valid-zeek-shaped.json"))).get();
            }

            // trans_id 9001 is valid-shares-uid-with-conn-udp-dns.json's own; 4242
            // and Cdns005ZEK are valid-zeek-shaped.json's, which shares no uid with
            // any conn record published in this test and so must come back an
            // orphan.
            String joinedDnsEventId = "sensor-eu-1:Cdef456UVW:9001";
            String orphanDnsEventId = "sensor-eu-1:Cdns005ZEK:4242";
            long dnsDeadline = System.currentTimeMillis() + 60_000;
            while ((!vectorsByEventId.containsKey(joinedDnsEventId)
                        || !vectorsByEventId.containsKey(orphanDnsEventId)
                        || dnsDlqPayloads.isEmpty())
                    && System.currentTimeMillis() < dnsDeadline) {
                collectRecords(consumer.poll(Duration.ofSeconds(2)), dnsDlqTopic, featureVectorDeserializer,
                    vectorsByEventId, dnsDlqPayloads);
            }

            // Conn vector: the frozen conn-feature-v1 contract, proven by a
            // deserialized wire record rather than a substring match. 20 is
            // deliberately a literal, not a read-back of the schema this test is
            // checking.
            FeatureVector connVector = vectorsByEventId.get(connEventId);
            assertEquals("conn-feature-v1", connVector.schemaId(), "conn vector schemaId must be conn-feature-v1");
            assertEquals(LogType.CONN, connVector.logType(), "conn vector logType must be CONN");
            assertEquals(20, connVector.values().length, "conn-feature-v1 is frozen at 20 values");

            // Joined dns vector: enriched via the conn.log join, because its own ts
            // (valid-shares-uid-with-conn-udp-dns.json) is strictly after the conn
            // snapshot's observedAt.
            assertTrue(vectorsByEventId.containsKey(joinedDnsEventId),
                "expected the joined dns feature vector " + joinedDnsEventId + " to arrive within 60s");
            FeatureVector joinedDns = vectorsByEventId.get(joinedDnsEventId);
            assertEquals("dns-feature-v1", joinedDns.schemaId(), "joined dns vector schemaId must be dns-feature-v1");
            assertEquals(DnsFeatureSchemaV1.CONTENT_HASH, joinedDns.schemaHash(),
                "joined dns vector schemaHash must match the frozen dns-feature-v1 content hash");
            assertEquals(LogType.DNS, joinedDns.logType(), "joined dns vector logType must be DNS");
            assertEquals(24, joinedDns.values().length, "dns-feature-v1 is frozen at 24 values");
            assertEquals("Cdef456UVW", joinedDns.connectionUid(),
                "joined dns vector must carry conn's uid as its correlation key");
            // Exact equality, not a bit test: a use case that enriches but also
            // sets some unrelated bit must still fail this.
            assertEquals(QualityFlags.NONE, joinedDns.qualityFlags(),
                "the join must enrich this record: qualityFlags must be exactly NONE");

            // Orphan dns vector: same schema/width, but no snapshot exists for
            // Cdns005ZEK, so CONN_ENRICHMENT_ABSENT must be set. This is not an
            // optional extra check -- NONE on the joined record above would also
            // pass a use case that never sets CONN_ENRICHMENT_ABSENT at all; only
            // seeing it set HERE, on a record with no snapshot, proves the bit is
            // actually wired rather than merely declared.
            assertTrue(vectorsByEventId.containsKey(orphanDnsEventId),
                "expected the orphan dns feature vector " + orphanDnsEventId + " to arrive within 60s");
            FeatureVector orphanDns = vectorsByEventId.get(orphanDnsEventId);
            assertEquals("dns-feature-v1", orphanDns.schemaId(), "orphan dns vector schemaId must be dns-feature-v1");
            assertEquals(DnsFeatureSchemaV1.CONTENT_HASH, orphanDns.schemaHash(),
                "orphan dns vector schemaHash must match the frozen dns-feature-v1 content hash");
            assertEquals(LogType.DNS, orphanDns.logType(), "orphan dns vector logType must be DNS");
            assertEquals(24, orphanDns.values().length, "dns-feature-v1 is frozen at 24 values");
            assertEquals(QualityFlags.CONN_ENRICHMENT_ABSENT, orphanDns.qualityFlags(),
                "no conn.log snapshot exists for Cdns005ZEK: qualityFlags must be exactly CONN_ENRICHMENT_ABSENT");

            // dns dlq: catches a DLQ sink wired to conn's own dlq topic instead of
            // dns's -- a mistake the topology test cannot see, since it only checks
            // operator uids, never the topic names those operators are configured
            // to publish to.
            assertTrue(dnsDlqPayloads.stream().anyMatch(payload -> payload.contains("\"reasonCode\":\"MALFORMED_JSON\"")),
                "expected a dns dlq record with reasonCode MALFORMED_JSON, got: " + dnsDlqPayloads);
        }
    }

    // Drives OnlineFeatureJob's three-protocol build() overload -- the one
    // main() calls -- against a real broker. Neither method above submits it:
    // one builds the conn-only overload, the other the two-protocol one. So
    // nothing before this test had run modbus's chain, its keyed entity state,
    // or its dlq sink against Kafka.
    //
    // This is also the end-to-end proof that a modbus response finds its own
    // request. Zeek's id_orig_h/id_resp_h describe the CONNECTION, not the
    // packet, so the request and the response below carry the SAME pair, as
    // the sensor emits them. Only request_response tells the two apart.
    // ModbusEventMapper has to orient that pair by direction before
    // ModbusEntityKey keys it (see the mapper's resolvePerPacketEndpoints). If
    // it did not, the response would be keyed as client 10.0.0.9, server
    // 10.0.0.5 -- a different state bucket from its request's -- and it would
    // arrive with rtt_valid 0 and response_without_request 1. A fixture that
    // swapped the IPs on the response would hide exactly that failure, which
    // is why neither record here swaps them.
    @Test
    void threeProtocolJobMatchesAModbusResponseToItsRequestAndRoutesMalformedModbusToItsOwnDlq() throws Exception {
        kafka.start();
        String bootstrapServers = kafka.getBootstrapServers();

        String connInputTopic = "netsec.conn.raw.v1";
        String connFeatureTopic = "netsec.conn.feature-vector.v1";
        String connDlqTopic = "netsec.conn.dlq.v1";
        String dnsInputTopic = "netsec.dns.raw.v1";
        String dnsFeatureTopic = "netsec.dns.feature-vector.v1";
        String dnsDlqTopic = "netsec.dns.dlq.v1";
        String modbusInputTopic = "netsec.modbus.raw.v1";
        String modbusFeatureTopic = "netsec.modbus.feature-vector.v1";
        String modbusDlqTopic = "netsec.modbus.dlq.v1";

        // Every input topic must exist before the job is submitted. A
        // KafkaSource subscribed to a topic that does not exist yet fails its
        // enumerator (see the two-protocol method above), and all three
        // protocols share this one environment. Nothing is published to conn's
        // or dns's input, so their topics are created here directly rather
        // than by publishing a record to them. Keeping those two inputs empty
        // is what lets the dlq check below expect conn's and dns's dlq to stay
        // EMPTY, not merely free of one particular record. Those two dlq
        // topics are created now as well, so the consumer below is assigned
        // them from its first poll. modbus's input gets exactly one partition,
        // so the request is delivered before its response. That is the per-key
        // order a production sensor has to provide by partitioning on
        // (client, server).
        try (Admin admin = Admin.create(Map.<String, Object>of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers))) {
            admin.createTopics(List.of(
                new NewTopic(connInputTopic, 1, (short) 1),
                new NewTopic(dnsInputTopic, 1, (short) 1),
                new NewTopic(modbusInputTopic, 1, (short) 1),
                new NewTopic(connDlqTopic, 1, (short) 1),
                new NewTopic(dnsDlqTopic, 1, (short) 1))).all().get(60, TimeUnit.SECONDS);
        }

        // One Modbus/TCP transaction, shaped as ICSNPP's modbus_detailed.log
        // emits it. Both records carry the same uid, tid and unit, and the SAME
        // connection-level endpoints, with the client (10.0.0.5) as id_orig_h
        // on both. The endpoints are one shared string, so the two records
        // cannot drift apart on them. Direction comes only from
        // request_response: neither record carries is_orig. The response is
        // 0.25 s after the request. Both timestamps and their difference are
        // exact in binary floating point, so rtt_s can be checked exactly.
        String connectionEndpoints =
            "\"id_orig_h\":\"10.0.0.5\",\"id_orig_p\":50001,\"id_resp_h\":\"10.0.0.9\",\"id_resp_p\":502,";
        String requestJson = "{\"ts\":1789977600.5,\"uid\":\"CmbE2E0001\"," + connectionEndpoints
            + "\"request_response\":\"REQUEST\",\"tid\":17,\"unit\":1,\"func\":\"READ_HOLDING_REGISTERS\","
            + "\"address\":40001,\"quantity\":2,\"request_values\":[],\"response_values\":[]}";
        String responseJson = "{\"ts\":1789977600.75,\"uid\":\"CmbE2E0001\"," + connectionEndpoints
            + "\"request_response\":\"RESPONSE\",\"tid\":17,\"unit\":1,\"func\":\"READ_HOLDING_REGISTERS\","
            + "\"address\":40001,\"quantity\":2,\"matched\":true,\"request_values\":[],\"response_values\":[7,9]}";
        float publishedGapSeconds = 0.25f;

        // Cut off mid-object, so it fails at the parse stage as MALFORMED_JSON.
        // The dlq carries only a SHA-256 of the raw bytes (never the bytes),
        // so the hash is how the check below tells this record apart.
        byte[] malformedModbus = "{ \"uid\": \"CmbBroken\", \"ts\": 1789977601.0, \"id_orig_h\": \"10.0.0.5\""
            .getBytes(StandardCharsets.UTF_8);
        String malformedModbusHash =
            HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(malformedModbus));

        // Published in this order before the job starts, so the source reads
        // all three from the beginning of the log: the request, then its
        // response, then the malformed record.
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(producerProps)) {
            producer.send(new ProducerRecord<>(modbusInputTopic, requestJson.getBytes(StandardCharsets.UTF_8))).get();
            producer.send(new ProducerRecord<>(modbusInputTopic, responseJson.getBytes(StandardCharsets.UTF_8))).get();
            producer.send(new ProducerRecord<>(modbusInputTopic, malformedModbus)).get();
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        OnlineFeatureJob.build(env, bootstrapServers,
            new OnlineFeatureJob.ProtocolTopics(connInputTopic, connFeatureTopic, connDlqTopic),
            new OnlineFeatureJob.ProtocolTopics(dnsInputTopic, dnsFeatureTopic, dnsDlqTopic),
            new OnlineFeatureJob.ProtocolTopics(modbusInputTopic, modbusFeatureTopic, modbusDlqTopic),
            new SensorId("sensor-eu-1"));

        // Submitted on the test thread itself -- see the comment on the same call
        // in connFixtureFlowsToFeatureVectorTopic above.
        JobClient job = env.executeAsync("three-protocol-online-job-e2e-test");

        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "three-protocol-e2e-test-reader");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // byte[] values, for the same reason as the two-protocol method above:
        // FeatureVectorDeserializer takes the raw bytes.
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());

        FeatureVectorDeserializer featureVectorDeserializer = new FeatureVectorDeserializer();
        Map<String, FeatureVector> modbusVectorsByEventId = new HashMap<>();
        Map<String, List<String>> dlqPayloadsByTopic = new HashMap<>();

        // modbus event identity is sensor:uid:tid:direction:ts_millis (see
        // ModbusEventMapper), so the request and its response get distinct ids
        // even though they share uid and tid.
        String requestEventId = "sensor-eu-1:CmbE2E0001:17:REQUEST:1789977600500";
        String responseEventId = "sensor-eu-1:CmbE2E0001:17:RESPONSE:1789977600750";

        // cancelJob is declared before consumer so try-with-resources closes it LAST (resources close in
        // the reverse of their declaration order): the consumer stops reading before the job it reads
        // from is cancelled. An AutoCloseable rather than a plain try/finally so a cancellation failure
        // is recorded as a suppressed exception instead of masking an assertion failure from the body.
        try (AutoCloseable cancelJob = () -> job.cancel().get(60, TimeUnit.SECONDS);
             Consumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerProps)) {
            consumer.subscribe(List.of(modbusFeatureTopic, modbusDlqTopic, connDlqTopic, dnsDlqTopic));

            // Wait for both modbus vectors and the modbus dlq record. 90 s
            // rather than the 60 s the methods above allow: this is the largest
            // topology this class submits (three sources, three chains), and
            // the wait includes the job's own start-up.
            long deadline = System.currentTimeMillis() + 90_000;
            while ((!modbusVectorsByEventId.containsKey(requestEventId)
                        || !modbusVectorsByEventId.containsKey(responseEventId)
                        || dlqPayloadsByTopic.getOrDefault(modbusDlqTopic, List.of()).isEmpty())
                    && System.currentTimeMillis() < deadline) {
                collectModbusRecords(consumer.poll(Duration.ofSeconds(2)), modbusFeatureTopic,
                    featureVectorDeserializer, modbusVectorsByEventId, dlqPayloadsByTopic);
            }

            // Keep reading for a short grace period after that. The checks for
            // conn's and dns's dlq below are checks for ABSENCE, and a
            // misrouted copy of the modbus rejection would be written by a
            // different sink running alongside modbus's own, so it could land a
            // moment after modbus's copy. This bounds the absence check in
            // time; it cannot prove nothing arrives later.
            long graceDeadline = System.currentTimeMillis() + 5_000;
            while (System.currentTimeMillis() < graceDeadline) {
                collectModbusRecords(consumer.poll(Duration.ofSeconds(1)), modbusFeatureTopic,
                    featureVectorDeserializer, modbusVectorsByEventId, dlqPayloadsByTopic);
            }

            // Exactly the two published records produced vectors. The
            // malformed record produced none.
            assertEquals(Set.of(requestEventId, responseEventId), modbusVectorsByEventId.keySet(),
                "expected exactly the request's and the response's modbus feature vectors within 90s");

            // Request vector: the frozen modbus-feature-v1 contract, read back
            // from the wire. 42 is a literal on purpose, not read back from the
            // schema under test. qualityFlags NONE: the key's first event is
            // never flagged out of order.
            FeatureVector request = modbusVectorsByEventId.get(requestEventId);
            assertEquals("modbus-feature-v1", request.schemaId(), "request vector schemaId must be modbus-feature-v1");
            assertEquals(ModbusFeatureSchemaV1.CONTENT_HASH, request.schemaHash(),
                "request vector schemaHash must match the frozen modbus-feature-v1 content hash");
            assertEquals(LogType.MODBUS, request.logType(), "request vector logType must be MODBUS");
            assertEquals(42, request.values().length, "modbus-feature-v1 is frozen at 42 values");
            assertEquals("CmbE2E0001", request.connectionUid(),
                "request vector must carry the Zeek uid as its correlation key");
            assertEquals(QualityFlags.NONE, request.qualityFlags(), "request vector qualityFlags must be exactly NONE");
            assertEquals(0f, modbusFeature(request, 0, "is_response"), "the request must be scored as a request");

            // Response vector: same schema and width. qualityFlags NONE
            // because it arrived after its request, in timestamp order.
            FeatureVector response = modbusVectorsByEventId.get(responseEventId);
            assertEquals("modbus-feature-v1", response.schemaId(), "response vector schemaId must be modbus-feature-v1");
            assertEquals(ModbusFeatureSchemaV1.CONTENT_HASH, response.schemaHash(),
                "response vector schemaHash must match the frozen modbus-feature-v1 content hash");
            assertEquals(LogType.MODBUS, response.logType(), "response vector logType must be MODBUS");
            assertEquals(42, response.values().length, "modbus-feature-v1 is frozen at 42 values");
            assertEquals("CmbE2E0001", response.connectionUid(),
                "response vector must carry the Zeek uid as its correlation key");
            assertEquals(QualityFlags.NONE, response.qualityFlags(),
                "the response arrived after its request: qualityFlags must be exactly NONE");
            assertEquals(1f, modbusFeature(response, 0, "is_response"),
                "direction must come from request_response alone");

            // The response found its request's state. Each of these fails if
            // the response was keyed into a different bucket from its request:
            // that bucket would be empty, so it would show no outstanding
            // request, count the response as unmatched, and have no RTT.
            assertEquals(1f, modbusFeature(response, 30, "outstanding_requests_before_event"),
                "the request must still be pending, in the SAME entity state, when its response arrives");
            assertEquals(0f, modbusFeature(response, 31, "response_without_request"),
                "the response must find its pending request: response_without_request must be 0");
            assertEquals(1f, modbusFeature(response, 33, "rtt_valid"),
                "the response must match its request causally: rtt_valid must be 1");
            assertEquals(publishedGapSeconds, modbusFeature(response, 34, "rtt_s"),
                "rtt_s must equal the published gap between the request's and the response's ts");

            // modbus dlq: exactly one record, and it is the malformed one. The
            // hash ties it to those exact bytes.
            List<String> modbusDlq = dlqPayloadsByTopic.getOrDefault(modbusDlqTopic, List.of());
            assertEquals(1, modbusDlq.size(),
                "exactly one modbus dlq record expected (the request and response are valid), got: " + modbusDlq);
            assertTrue(modbusDlq.get(0).contains("\"reasonCode\":\"MALFORMED_JSON\""),
                "expected the modbus dlq record to carry reasonCode MALFORMED_JSON, got: " + modbusDlq.get(0));
            assertTrue(modbusDlq.get(0).contains("\"rawPayloadHash\":\"" + malformedModbusHash + "\""),
                "expected the modbus dlq record to carry the malformed record's own payload hash, got: "
                    + modbusDlq.get(0));

            // conn's and dns's dlq: nothing was published to either protocol's
            // input, so anything on their dlq topics could only be a modbus
            // rejection sent to the wrong topic. The topology test cannot see
            // that, because it checks operator uids, never topic names.
            assertEquals(List.of(), dlqPayloadsByTopic.getOrDefault(connDlqTopic, List.of()),
                "a modbus rejection must never reach conn's dlq");
            assertEquals(List.of(), dlqPayloadsByTopic.getOrDefault(dnsDlqTopic, List.of()),
                "a modbus rejection must never reach dns's dlq");
        }
    }

    // Reads one modbus-feature-v1 value by a literal index, and checks that
    // literal against the name the frozen contract gives it. The index stays a
    // literal on purpose: the upstream contract fixes each position (its
    // 1-based index minus one). Looking the index up by name would follow a
    // reordered schema instead of failing on it, and the name check turns a
    // typo in the literal into a failure instead of a read of the wrong value.
    private static float modbusFeature(FeatureVector vector, int index, String name) {
        assertEquals(name, ModbusFeatureSchemaV1.SCHEMA.definitions().get(index).name(),
            "modbus-feature-v1 index " + index + " must be " + name);
        return vector.values()[index];
    }

    // Reads one committed fixture's raw bytes for the two-protocol method above to
    // publish -- it needs four, where connFixtureFlowsToFeatureVectorTopic above
    // needs only its own one inline.
    private static byte[] fixture(String protocol, String name) throws IOException {
        return Files.readAllBytes(Paths.get("..", "..", "tests", "fixtures", protocol, name));
    }

    // Classifies one poll's records into the two shared collections the caller
    // accumulates across both consume phases: a feature vector topic's records
    // deserialize into vectorsByEventId keyed by eventId (never by arrival
    // order), while dnsDlqTopic's own raw bytes are decoded to text and kept for
    // a substring check, since the dlq wire form has no equivalent typed reader.
    // Every topic this test subscribes to besides dnsDlqTopic is a feature-vector
    // topic (conn's or dns's), so no third branch is needed here.
    private static void collectRecords(ConsumerRecords<byte[], byte[]> records, String dnsDlqTopic,
            FeatureVectorDeserializer featureVectorDeserializer, Map<String, FeatureVector> vectorsByEventId,
            List<String> dnsDlqPayloads) {
        records.forEach(record -> {
            if (record.topic().equals(dnsDlqTopic)) {
                dnsDlqPayloads.add(new String(record.value(), StandardCharsets.UTF_8));
            } else {
                FeatureVector vector = featureVectorDeserializer.deserialize(record.topic(), record.value());
                vectorsByEventId.put(vector.eventId(), vector);
            }
        });
    }

    // The three-protocol method's own classifier. collectRecords above does
    // not fit it: that helper treats every topic except its one dlq topic as a
    // feature-vector topic, while the three-protocol method subscribes to
    // THREE dlq topics (modbus's, plus conn's and dns's to check they stay
    // empty). Here, records from modbusFeatureTopic are deserialized into
    // vectorsByEventId, keyed by eventId. Every other subscribed topic is a
    // dlq topic, so its payloads are decoded to text and kept under that
    // topic's own name.
    private static void collectModbusRecords(ConsumerRecords<byte[], byte[]> records, String modbusFeatureTopic,
            FeatureVectorDeserializer featureVectorDeserializer, Map<String, FeatureVector> vectorsByEventId,
            Map<String, List<String>> dlqPayloadsByTopic) {
        records.forEach(record -> {
            if (record.topic().equals(modbusFeatureTopic)) {
                FeatureVector vector = featureVectorDeserializer.deserialize(record.topic(), record.value());
                vectorsByEventId.put(vector.eventId(), vector);
            } else {
                dlqPayloadsByTopic.computeIfAbsent(record.topic(), topic -> new ArrayList<>())
                    .add(new String(record.value(), StandardCharsets.UTF_8));
            }
        });
    }
}
