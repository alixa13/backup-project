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
import io.netsecml.platform.domain.feature.FeatureVector;
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
                Instant.parse("2026-08-27T10:03:11.250Z"))));

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
}
