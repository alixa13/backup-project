package io.netsecml.platform.bootstrap.archive;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.query.GenericRecord;
import io.netsecml.platform.adapter.clickhouse.ClickHouseTestSupport;
import io.netsecml.platform.adapter.clickhouse.writer.ClickHouseConfig;
import io.netsecml.platform.adapter.kafka.sink.FeatureVectorSerializer;
import io.netsecml.platform.adapter.kafka.sink.RejectedRecordPayload;
import io.netsecml.platform.adapter.kafka.sink.RejectedRecordSerializer;
import io.netsecml.platform.domain.event.LogType;
import io.netsecml.platform.domain.event.SensorId;
import io.netsecml.platform.domain.feature.ConnFeatureSchemaV1;
import io.netsecml.platform.domain.feature.DnsFeatureSchemaV1;
import io.netsecml.platform.domain.feature.FeatureVector;
import io.netsecml.platform.domain.feature.ModbusFeatureSchemaV1;
import io.netsecml.platform.domain.feature.QualityFlags;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Properties;
import static org.junit.jupiter.api.Assertions.*;
import java.time.Duration;

// Roadmap Day 6, test 1: a feature vector published to Kafka becomes a queryable
// ClickHouse row, and a rejected record becomes a queryable invalid_events row.
@Testcontainers(disabledWithoutDocker = true)
class ArchiveJobE2ETest {
    private static final String FEATURE_TOPIC = "netsec.conn.feature-vector.v1";
    private static final String DLQ_TOPIC = "netsec.conn.dlq.v1";
    private static final String DATABASE = "archive_e2e";

    // A second, fully isolated set of topics and database for the four-chain
    // method below. KAFKA and CLICKHOUSE are static @Container fields, shared by
    // every method in this class, and freshDatabase() never drops a database --
    // so two methods reusing a topic or database name would read each other's rows.
    private static final String FOUR_CHAIN_CONN_FEATURE_TOPIC = "four-chain.netsec.conn.feature-vector.v1";
    private static final String FOUR_CHAIN_CONN_DLQ_TOPIC = "four-chain.netsec.conn.dlq.v1";
    private static final String FOUR_CHAIN_DNS_FEATURE_TOPIC = "four-chain.netsec.dns.feature-vector.v1";
    private static final String FOUR_CHAIN_DNS_DLQ_TOPIC = "four-chain.netsec.dns.dlq.v1";
    private static final String FOUR_CHAIN_DATABASE = "archive_e2e_four_chain";

    // A third isolated set of topics and database, for the six-chain method
    // below, for the same reason as the four-chain set above.
    private static final String SIX_CHAIN_CONN_FEATURE_TOPIC = "six-chain.netsec.conn.feature-vector.v1";
    private static final String SIX_CHAIN_CONN_DLQ_TOPIC = "six-chain.netsec.conn.dlq.v1";
    private static final String SIX_CHAIN_DNS_FEATURE_TOPIC = "six-chain.netsec.dns.feature-vector.v1";
    private static final String SIX_CHAIN_DNS_DLQ_TOPIC = "six-chain.netsec.dns.dlq.v1";
    private static final String SIX_CHAIN_MODBUS_FEATURE_TOPIC = "six-chain.netsec.modbus.feature-vector.v1";
    private static final String SIX_CHAIN_MODBUS_DLQ_TOPIC = "six-chain.netsec.modbus.dlq.v1";
    private static final String SIX_CHAIN_DATABASE = "archive_e2e_six_chain";

    @Container
    private static final ConfluentKafkaContainer KAFKA =
        new ConfluentKafkaContainer("confluentinc/cp-kafka:7.6.1")
            // Testcontainers waits 60 s by default for the broker to log its
            // transition to RUNNING. On a slow machine this image needs longer
            // than that just to reach SharedServer startup, so the container is
            // killed mid-boot and the test fails for no reason of its own.
            .withStartupTimeout(Duration.ofMinutes(3));

    @Container
    private static final GenericContainer<?> CLICKHOUSE = ClickHouseTestSupport.newContainer();

    // 20 values so the row matches what a real vector carries; index 0 is
    // distinctive so the assertion proves the payload, not just the row count.
    private FeatureVector vector() {
        float[] values = new float[20];
        values[0] = 42.5f;
        values[19] = 7f;
        return new FeatureVector("sensor-eu-1:Cabc123XYZ", Instant.parse("2026-08-27T10:03:11.250Z"),
            new SensorId("sensor-eu-1"), LogType.CONN, "Cabc123XYZ",
            ConnFeatureSchemaV1.SCHEMA.id(), ConnFeatureSchemaV1.CONTENT_HASH,
            values, 0, Instant.parse("2026-08-27T10:03:11.402Z"));
    }

    // 24 values -- the common tier plus dns's own twelve -- so the row matches
    // what a real dns vector carries; index 0 and the last index are both
    // distinctive so the assertion proves the whole array survived, not just
    // its first element. quality_flags is non-zero here (unlike vector()'s
    // above) so the round trip is proven for that column too.
    private FeatureVector dnsVector() {
        float[] values = new float[24];
        values[0] = 13.25f;
        values[23] = 99f;
        return new FeatureVector("sensor-eu-1:Cdns005ZEK:4242", Instant.parse("2026-08-27T10:05:00.500Z"),
            new SensorId("sensor-eu-1"), LogType.DNS, "Cdns005ZEK",
            DnsFeatureSchemaV1.SCHEMA.id(), DnsFeatureSchemaV1.CONTENT_HASH,
            values, QualityFlags.CONN_ENRICHMENT_ABSENT, Instant.parse("2026-08-27T10:05:00.650Z"));
    }

    // 42 values -- modbus-feature-v1's frozen width, which carries no common
    // tier -- so the row matches what a real modbus vector carries. The first
    // and last values are both distinctive, so the assertions prove the whole
    // array survived. quality_flags is MODBUS_OUT_OF_ORDER, modbus's own bit
    // (value 4, above both earlier bits), so a round trip that dropped or
    // masked it would fail. The event id has modbus's
    // sensor:uid:tid:direction:ts_millis shape.
    private FeatureVector modbusVector() {
        float[] values = new float[42];
        values[0] = 1f;
        values[41] = 0.75f;
        return new FeatureVector("sensor-eu-1:CmbE2E0001:17:RESPONSE:1789977600750",
            Instant.parse("2026-09-21T08:00:00.750Z"), new SensorId("sensor-eu-1"), LogType.MODBUS, "CmbE2E0001",
            ModbusFeatureSchemaV1.SCHEMA.id(), ModbusFeatureSchemaV1.CONTENT_HASH,
            values, QualityFlags.MODBUS_OUT_OF_ORDER, Instant.parse("2026-09-21T08:00:00.900Z"));
    }

    // Publishes one already-serialized message to the given topic and blocks
    // until the broker acknowledges it, so the record is guaranteed visible to
    // the archive job's source before the job starts consuming.
    private void produce(String topic, byte[] payload) throws Exception {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(props)) {
            producer.send(new ProducerRecord<>(topic, payload)).get();
        }
    }

    // Polls until the query returns at least one row or the deadline passes. The
    // archive path is asynchronous by design, so a fixed sleep would be either
    // slow or flaky.
    private List<GenericRecord> awaitRows(Client client, String sql) throws Exception {
        long deadline = System.currentTimeMillis() + 90_000;
        List<GenericRecord> rows = List.of();
        while (rows.isEmpty() && System.currentTimeMillis() < deadline) {
            rows = client.queryAll(sql);
            if (rows.isEmpty()) {
                Thread.sleep(500);
            }
        }
        return rows;
    }

    @Test
    void featureVectorAndRejectedRecordBothReachClickHouse() throws Exception {
        // Publish one record to each internal topic before the job starts, so the
        // source reads them from the beginning of the log.
        produce(FEATURE_TOPIC, new FeatureVectorSerializer().serialize(FEATURE_TOPIC, vector()));
        produce(DLQ_TOPIC, new RejectedRecordSerializer().serialize(DLQ_TOPIC,
            new RejectedRecordPayload("{ broken".getBytes(StandardCharsets.UTF_8), "",
                "PARSE", "MALFORMED_JSON", "unexpected end of input",
                // "now", not the fixed 2026-08-27 date vector() above still uses:
                // invalid_events carries a 30-day TTL on received_at, so a fixed
                // date this old eventually lets a background TTL merge delete the
                // row before this test ever reads it -- a failure with nothing to
                // do with this job's code. feature_vectors has no TTL, so vector()'s
                // eventTime/producedAt stay on the fixed date the row_version
                // assertion below still depends on.
                Instant.now().truncatedTo(ChronoUnit.MILLIS))));

        try (Client query = ClickHouseTestSupport.freshDatabase(CLICKHOUSE, DATABASE)) {
            ClickHouseConfig config = ClickHouseConfig.of(CLICKHOUSE.getHost(),
                CLICKHOUSE.getMappedPort(ClickHouseTestSupport.HTTP_PORT), DATABASE, "default", ClickHouseTestSupport.PASSWORD);

            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(1);
            // A short checkpoint interval keeps the test's flush latency low;
            // production uses 30 s.
            env.enableCheckpointing(1_000L);
            ArchiveJob.build(env, KAFKA.getBootstrapServers(), FEATURE_TOPIC, DLQ_TOPIC, config);

            // executeAsync returns a JobClient immediately — no helper thread needed.
            JobClient job = env.executeAsync("archive-job-e2e-test");
            try {
                List<GenericRecord> features = awaitRows(query,
                    "SELECT event_id, sensor, log_type, connection_uid, schema_hash, "
                        + "length(`values`) AS n, `values`[1] AS first, "
                        + "toUnixTimestamp64Milli(row_version) AS version FROM feature_vectors");

                assertEquals(1, features.size(), "one feature vector must reach feature_vectors");
                assertEquals("sensor-eu-1:Cabc123XYZ", features.get(0).getString("event_id"));
                assertEquals("sensor-eu-1", features.get(0).getString("sensor"));
                assertEquals(ConnFeatureSchemaV1.CONTENT_HASH, features.get(0).getString("schema_hash"));
                assertEquals(20, features.get(0).getInteger("n"), "all 20 values must survive the round trip");
                // The multi-protocol envelope columns. Without these the round trip
                // would pass while silently dropping both, and every archived row
                // would be unjoinable across log types.
                assertEquals("conn", features.get(0).getString("log_type"));
                assertEquals("Cabc123XYZ", features.get(0).getString("connection_uid"));
                assertEquals(42.5f, features.get(0).getFloat("first"), 0.0001f);
                assertEquals(Instant.parse("2026-08-27T10:03:11.402Z").toEpochMilli(),
                    features.get(0).getLong("version"), "row_version is the producer's producedAt");

                List<GenericRecord> invalid = awaitRows(query,
                    "SELECT event_id, stage, reason_code, source_version, log_type FROM invalid_events");

                assertEquals(1, invalid.size(), "one rejected record must reach invalid_events");
                assertEquals("PARSE", invalid.get(0).getString("stage"));
                assertEquals("MALFORMED_JSON", invalid.get(0).getString("reason_code"));
                assertEquals("zeek-conn-source-v1", invalid.get(0).getString("source_version"));
                assertEquals("", invalid.get(0).getString("event_id"), "a parse-stage rejection has no identity");
                // Proves the production binding end to end: build() -> dlqChain(CONN,
                // dlqTopic) -> InvalidEventRowMapFunction(topic, CONN) -> open() ->
                // mapper actually lands "conn" in the row. Nothing else on this branch
                // runs a real map function against the wired log type, so a later
                // refactor binding the wrong constant would otherwise pass every test.
                assertEquals("conn", invalid.get(0).getString("log_type"),
                    "the log type must arrive from the topic binding, through the real job graph");
            } finally {
                job.cancel().get();
            }
        }
    }

    // Roadmap Day 6, test 2: the two-protocol four-chain list -- conn's feature
    // vector and DLQ chains plus dns's -- built through connAndDnsChains()
    // rather than hand-copied, so this test fails if that method ever binds
    // one of these four topics to the wrong log type. main() called
    // connAndDnsChains() until modbus was wired; it now calls
    // connDnsAndModbusChains(), which the six-chain method below builds
    // through. Isolated from the method above by its own database and topic
    // names, since KAFKA and CLICKHOUSE are static @Container fields shared by
    // every method in this class.
    @Test
    void connAndDnsChainsWriteFeatureVectorsAndRejectionsUnderTheirOwnLogType() throws Exception {
        // One timestamp for both rejections below. invalid_events has a 30-day
        // TTL on received_at, so "now" keeps this test from expiring the way a
        // fixed past date eventually would.
        Instant now = Instant.now().truncatedTo(ChronoUnit.MILLIS);

        // Publish all four records before the job starts, so every source reads
        // its record from the beginning of the log.
        produce(FOUR_CHAIN_CONN_FEATURE_TOPIC,
            new FeatureVectorSerializer().serialize(FOUR_CHAIN_CONN_FEATURE_TOPIC, vector()));
        produce(FOUR_CHAIN_DNS_FEATURE_TOPIC,
            new FeatureVectorSerializer().serialize(FOUR_CHAIN_DNS_FEATURE_TOPIC, dnsVector()));
        produce(FOUR_CHAIN_CONN_DLQ_TOPIC, new RejectedRecordSerializer().serialize(FOUR_CHAIN_CONN_DLQ_TOPIC,
            new RejectedRecordPayload("{ conn broken".getBytes(StandardCharsets.UTF_8), "",
                "PARSE", "MALFORMED_JSON", "conn: unexpected end of input", now)));
        produce(FOUR_CHAIN_DNS_DLQ_TOPIC, new RejectedRecordSerializer().serialize(FOUR_CHAIN_DNS_DLQ_TOPIC,
            new RejectedRecordPayload("{ dns broken".getBytes(StandardCharsets.UTF_8), "",
                "PARSE", "MALFORMED_JSON", "dns: unexpected end of input", now)));

        try (Client query = ClickHouseTestSupport.freshDatabase(CLICKHOUSE, FOUR_CHAIN_DATABASE)) {
            ClickHouseConfig config = ClickHouseConfig.of(CLICKHOUSE.getHost(),
                CLICKHOUSE.getMappedPort(ClickHouseTestSupport.HTTP_PORT), FOUR_CHAIN_DATABASE, "default",
                ClickHouseTestSupport.PASSWORD);

            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(1);
            // A short checkpoint interval keeps the test's flush latency low;
            // production uses 30 s.
            env.enableCheckpointing(1_000L);
            // Built through connAndDnsChains() rather than a hand-assembled
            // list, so a chain that method binds to the wrong log type would
            // fail this test too.
            ArchiveJob.build(env, KAFKA.getBootstrapServers(),
                ArchiveJob.connAndDnsChains(FOUR_CHAIN_CONN_FEATURE_TOPIC, FOUR_CHAIN_CONN_DLQ_TOPIC,
                    FOUR_CHAIN_DNS_FEATURE_TOPIC, FOUR_CHAIN_DNS_DLQ_TOPIC),
                config);

            // executeAsync returns a JobClient immediately — no helper thread needed.
            JobClient job = env.executeAsync("archive-job-e2e-four-chain-test");
            try {
                // Every query below filters on log_type. awaitRows returns on the
                // first non-empty result, so an unfiltered query against a table
                // that ends up holding both conn's and dns's row could return
                // before the other log type's row has landed.
                List<GenericRecord> dnsFeatures = awaitRows(query,
                    "SELECT event_id, connection_uid, schema_id, schema_hash, length(`values`) AS n, "
                        + "`values`[1] AS first, `values`[24] AS last, quality_flags FROM feature_vectors "
                        + "WHERE log_type = 'dns'");

                assertEquals(1, dnsFeatures.size(), "exactly one dns feature vector must reach feature_vectors");
                assertEquals("sensor-eu-1:Cdns005ZEK:4242", dnsFeatures.get(0).getString("event_id"),
                    "the dns vector's event_id must survive the round trip");
                assertEquals("Cdns005ZEK", dnsFeatures.get(0).getString("connection_uid"),
                    "connection_uid must carry the dns record's own uid, not conn's");
                assertEquals(DnsFeatureSchemaV1.SCHEMA.id(), dnsFeatures.get(0).getString("schema_id"),
                    "schema_id must identify the dns schema, not conn's");
                assertEquals(DnsFeatureSchemaV1.CONTENT_HASH, dnsFeatures.get(0).getString("schema_hash"),
                    "schema_hash must match the frozen dns-feature-v1 hash");
                assertEquals(24, dnsFeatures.get(0).getInteger("n"), "all 24 dns values must survive the round trip");
                assertEquals(13.25f, dnsFeatures.get(0).getFloat("first"), 0.0001f,
                    "the first of the 24 values must survive the round trip");
                // The 24th value, not just the 1st: a vector truncated back down to
                // conn's 20 values would still pass the "first" assertion above.
                assertEquals(99f, dnsFeatures.get(0).getFloat("last"), 0.0001f,
                    "the last of the 24 values must survive, not just the first");
                assertEquals(QualityFlags.CONN_ENRICHMENT_ABSENT, dnsFeatures.get(0).getInteger("quality_flags"),
                    "quality_flags must survive the round trip alongside the values array");

                List<GenericRecord> connFeatures = awaitRows(query,
                    "SELECT event_id, schema_hash, length(`values`) AS n FROM feature_vectors "
                        + "WHERE log_type = 'conn'");

                assertEquals(1, connFeatures.size(), "exactly one conn feature vector must reach feature_vectors");
                assertEquals("sensor-eu-1:Cabc123XYZ", connFeatures.get(0).getString("event_id"),
                    "conn's event_id must be unaffected by the dns chain sharing this job");
                assertEquals(ConnFeatureSchemaV1.CONTENT_HASH, connFeatures.get(0).getString("schema_hash"),
                    "conn's schema_hash must be unaffected by the dns chain sharing this job");
                assertEquals(20, connFeatures.get(0).getInteger("n"),
                    "conn's 20 values must be unaffected by the dns chain sharing this job");

                List<GenericRecord> dnsInvalid = awaitRows(query,
                    "SELECT detail, source_version, stage, reason_code FROM invalid_events WHERE log_type = 'dns'");

                assertEquals(1, dnsInvalid.size(), "exactly one dns rejection must reach invalid_events");
                // detail is the only column that names which topic a rejection came
                // from -- if a chain bound the dns DLQ topic to LogType.CONN instead,
                // this row would carry log_type "conn" and this query would return
                // nothing.
                assertEquals("dns: unexpected end of input", dnsInvalid.get(0).getString("detail"),
                    "detail must name the dns topic's own broken payload, not conn's");
                assertEquals("zeek-dns-source-v1", dnsInvalid.get(0).getString("source_version"),
                    "source_version must name the dns source contract, not conn's");
                assertEquals("PARSE", dnsInvalid.get(0).getString("stage"),
                    "stage must survive the round trip");
                assertEquals("MALFORMED_JSON", dnsInvalid.get(0).getString("reason_code"),
                    "reason_code must survive the round trip");

                List<GenericRecord> connInvalid = awaitRows(query,
                    "SELECT detail, source_version FROM invalid_events WHERE log_type = 'conn'");

                assertEquals(1, connInvalid.size(), "exactly one conn rejection must reach invalid_events");
                assertEquals("conn: unexpected end of input", connInvalid.get(0).getString("detail"),
                    "detail must name the conn topic's own broken payload, not dns's");
                assertEquals("zeek-conn-source-v1", connInvalid.get(0).getString("source_version"),
                    "source_version must name the conn source contract, not dns's");
            } finally {
                job.cancel().get();
            }
        }
    }

    // Roadmap Day 6, test 3: the six-chain list main() wires now -- conn's,
    // dns's and modbus's feature-vector and DLQ chains -- built through
    // connDnsAndModbusChains(), the method ArchiveJob.main() calls, so this
    // test fails if main()'s own list binds a modbus topic to the wrong log
    // type. A new method rather than a change to the four-chain one above,
    // which keeps proving connAndDnsChains() unchanged. Isolated from both
    // methods above by its own database and topic names.
    @Test
    void sixChainsFromMainWriteAModbusFeatureVectorAndRejectionUnderTheModbusLogType() throws Exception {
        // One timestamp for all three rejections below. invalid_events has a
        // 30-day TTL on received_at, so "now" keeps this test from expiring
        // the way a fixed past date eventually would.
        Instant now = Instant.now().truncatedTo(ChronoUnit.MILLIS);

        // modbus's two records, which is what this method is about. The
        // rejection is a MAP-stage one, as the online job's modbus parse stage
        // writes it: its event_id is sensor:uid:tid only, a correlation key
        // rather than the sensor:uid:tid:direction:ts_millis id a successfully
        // mapped modbus record gets -- a map-stage rejection may be about
        // exactly the direction or the timestamp.
        produce(SIX_CHAIN_MODBUS_FEATURE_TOPIC,
            new FeatureVectorSerializer().serialize(SIX_CHAIN_MODBUS_FEATURE_TOPIC, modbusVector()));
        produce(SIX_CHAIN_MODBUS_DLQ_TOPIC, new RejectedRecordSerializer().serialize(SIX_CHAIN_MODBUS_DLQ_TOPIC,
            new RejectedRecordPayload("{\"uid\":\"CmbE2E0001\",\"tid\":18}".getBytes(StandardCharsets.UTF_8),
                "sensor-eu-1:CmbE2E0001:18", "MAP", "MISSING_REQUIRED_FIELD",
                "modbus: direction is required: neither request_response nor is_orig resolved", now)));

        // conn's and dns's four records. All six sources need their topic to
        // exist when the job starts -- a KafkaSource subscribed to a missing
        // topic fails its enumerator -- and publishing to a topic is how the
        // four-chain method above makes its topics exist too. Publishing
        // them also lets this method check that conn and dns each still get
        // exactly their own rows with modbus's chains in the same job.
        produce(SIX_CHAIN_CONN_FEATURE_TOPIC,
            new FeatureVectorSerializer().serialize(SIX_CHAIN_CONN_FEATURE_TOPIC, vector()));
        produce(SIX_CHAIN_DNS_FEATURE_TOPIC,
            new FeatureVectorSerializer().serialize(SIX_CHAIN_DNS_FEATURE_TOPIC, dnsVector()));
        produce(SIX_CHAIN_CONN_DLQ_TOPIC, new RejectedRecordSerializer().serialize(SIX_CHAIN_CONN_DLQ_TOPIC,
            new RejectedRecordPayload("{ conn broken".getBytes(StandardCharsets.UTF_8), "",
                "PARSE", "MALFORMED_JSON", "conn: unexpected end of input", now)));
        produce(SIX_CHAIN_DNS_DLQ_TOPIC, new RejectedRecordSerializer().serialize(SIX_CHAIN_DNS_DLQ_TOPIC,
            new RejectedRecordPayload("{ dns broken".getBytes(StandardCharsets.UTF_8), "",
                "PARSE", "MALFORMED_JSON", "dns: unexpected end of input", now)));

        try (Client query = ClickHouseTestSupport.freshDatabase(CLICKHOUSE, SIX_CHAIN_DATABASE)) {
            ClickHouseConfig config = ClickHouseConfig.of(CLICKHOUSE.getHost(),
                CLICKHOUSE.getMappedPort(ClickHouseTestSupport.HTTP_PORT), SIX_CHAIN_DATABASE, "default",
                ClickHouseTestSupport.PASSWORD);

            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(1);
            // A short checkpoint interval keeps the test's flush latency low;
            // production uses 30 s.
            env.enableCheckpointing(1_000L);
            // Built through connDnsAndModbusChains() -- the exact method main()
            // calls -- rather than a hand-assembled list, so a chain bound to
            // the wrong log type inside main() itself would fail this test too.
            ArchiveJob.build(env, KAFKA.getBootstrapServers(),
                ArchiveJob.connDnsAndModbusChains(SIX_CHAIN_CONN_FEATURE_TOPIC, SIX_CHAIN_CONN_DLQ_TOPIC,
                    SIX_CHAIN_DNS_FEATURE_TOPIC, SIX_CHAIN_DNS_DLQ_TOPIC,
                    SIX_CHAIN_MODBUS_FEATURE_TOPIC, SIX_CHAIN_MODBUS_DLQ_TOPIC),
                config);

            // executeAsync returns a JobClient immediately — no helper thread needed.
            JobClient job = env.executeAsync("archive-job-e2e-six-chain-test");
            try {
                // Every query below filters on log_type, for the same reason as
                // in the four-chain method above: awaitRows returns on the
                // first non-empty result.
                List<GenericRecord> modbusFeatures = awaitRows(query,
                    "SELECT event_id, connection_uid, schema_id, schema_hash, length(`values`) AS n, "
                        + "`values`[1] AS first, `values`[42] AS last, quality_flags FROM feature_vectors "
                        + "WHERE log_type = 'modbus'");

                assertEquals(1, modbusFeatures.size(), "exactly one modbus feature vector must reach feature_vectors");
                assertEquals("sensor-eu-1:CmbE2E0001:17:RESPONSE:1789977600750",
                    modbusFeatures.get(0).getString("event_id"),
                    "the modbus vector's event_id must survive the round trip");
                assertEquals("CmbE2E0001", modbusFeatures.get(0).getString("connection_uid"),
                    "connection_uid must carry the modbus record's own Zeek uid");
                assertEquals(ModbusFeatureSchemaV1.SCHEMA.id(), modbusFeatures.get(0).getString("schema_id"),
                    "schema_id must identify the modbus schema");
                assertEquals(ModbusFeatureSchemaV1.CONTENT_HASH, modbusFeatures.get(0).getString("schema_hash"),
                    "schema_hash must match the frozen modbus-feature-v1 hash");
                assertEquals(42, modbusFeatures.get(0).getInteger("n"), "all 42 modbus values must survive the round trip");
                assertEquals(1f, modbusFeatures.get(0).getFloat("first"), 0.0001f,
                    "the first of the 42 values must survive the round trip");
                // The 42nd value, not just the 1st: a vector cut down to dns's
                // 24 or conn's 20 values would still pass the "first" check.
                assertEquals(0.75f, modbusFeatures.get(0).getFloat("last"), 0.0001f,
                    "the last of the 42 values must survive, not just the first");
                assertEquals(QualityFlags.MODBUS_OUT_OF_ORDER, modbusFeatures.get(0).getInteger("quality_flags"),
                    "modbus's own quality flag bit must survive the round trip");

                List<GenericRecord> modbusInvalid = awaitRows(query,
                    "SELECT event_id, detail, source_version, stage, reason_code FROM invalid_events "
                        + "WHERE log_type = 'modbus'");

                // invalid_events.log_type comes from the topic binding, not from
                // the record (dlq-v1 carries no protocol field), so this row
                // exists under 'modbus' only if main()'s list binds the modbus
                // DLQ topic to LogType.MODBUS.
                assertEquals(1, modbusInvalid.size(), "exactly one modbus rejection must reach invalid_events");
                assertEquals("sensor-eu-1:CmbE2E0001:18", modbusInvalid.get(0).getString("event_id"),
                    "a modbus map-stage rejection's sensor:uid:tid event_id must survive the round trip");
                assertEquals("modbus: direction is required: neither request_response nor is_orig resolved",
                    modbusInvalid.get(0).getString("detail"),
                    "detail must name the modbus topic's own rejection");
                assertEquals("zeek-modbus-source-v1", modbusInvalid.get(0).getString("source_version"),
                    "source_version must name the modbus source contract");
                assertEquals("MAP", modbusInvalid.get(0).getString("stage"),
                    "stage must be derived from MISSING_REQUIRED_FIELD as MAP");
                assertEquals("MISSING_REQUIRED_FIELD", modbusInvalid.get(0).getString("reason_code"),
                    "reason_code must survive the round trip");

                // conn and dns still land exactly one row each, per table, under
                // their own log type -- so neither of modbus's chains wrote
                // into theirs.
                List<GenericRecord> connFeatures = awaitRows(query,
                    "SELECT event_id FROM feature_vectors WHERE log_type = 'conn'");
                assertEquals(1, connFeatures.size(), "exactly one conn feature vector must reach feature_vectors");
                assertEquals("sensor-eu-1:Cabc123XYZ", connFeatures.get(0).getString("event_id"),
                    "conn's event_id must be unaffected by the modbus chains sharing this job");

                List<GenericRecord> dnsFeatures = awaitRows(query,
                    "SELECT event_id FROM feature_vectors WHERE log_type = 'dns'");
                assertEquals(1, dnsFeatures.size(), "exactly one dns feature vector must reach feature_vectors");
                assertEquals("sensor-eu-1:Cdns005ZEK:4242", dnsFeatures.get(0).getString("event_id"),
                    "dns's event_id must be unaffected by the modbus chains sharing this job");

                List<GenericRecord> connInvalid = awaitRows(query,
                    "SELECT detail FROM invalid_events WHERE log_type = 'conn'");
                assertEquals(1, connInvalid.size(), "exactly one conn rejection must reach invalid_events");
                assertEquals("conn: unexpected end of input", connInvalid.get(0).getString("detail"),
                    "detail must name the conn topic's own broken payload, not modbus's");

                List<GenericRecord> dnsInvalid = awaitRows(query,
                    "SELECT detail FROM invalid_events WHERE log_type = 'dns'");
                assertEquals(1, dnsInvalid.size(), "exactly one dns rejection must reach invalid_events");
                assertEquals("dns: unexpected end of input", dnsInvalid.get(0).getString("detail"),
                    "detail must name the dns topic's own broken payload, not modbus's");
            } finally {
                job.cancel().get();
            }
        }
    }
}
