package io.netsecml.platform.bootstrap.archive;

import com.clickhouse.client.api.Client;
import io.netsecml.platform.adapter.clickhouse.ClickHouseTestSupport;
import io.netsecml.platform.adapter.clickhouse.writer.ClickHouseConfig;
import io.netsecml.platform.bootstrap.online.OnlineFeatureJob;
import io.netsecml.platform.domain.event.SensorId;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import java.nio.file.Files;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import static org.junit.jupiter.api.Assertions.*;

// Roadmap Day 6, test 3, and the Definition of Done's teeth: ClickHouse failure
// must not stop feature production.
//
// Assertions here are deliberately coarse — records still arriving on the feature
// topic, online job still RUNNING. Asserting on lag values, restart counts or
// timings would buy flakiness in exchange for no information.
@Testcontainers(disabledWithoutDocker = true)
class ClickHouseOutageTest {
    private static final String INPUT_TOPIC = "netsec.conn.raw.v1";
    private static final String FEATURE_TOPIC = "netsec.conn.feature-vector.v1";
    private static final String DLQ_TOPIC = "netsec.conn.dlq.v1";
    private static final String DATABASE = "outage_test";

    @Container
    private static final ConfluentKafkaContainer KAFKA =
        new ConfluentKafkaContainer("confluentinc/cp-kafka:7.6.1");

    @Container
    private static final GenericContainer<?> CLICKHOUSE = ClickHouseTestSupport.newContainer();

    // The same fixture the online job's own E2E test uses.
    private byte[] fixture() throws Exception {
        return Files.readAllBytes(
            ClickHouseTestSupport.repoPath("tests", "fixtures", "zeek_conn", "valid-tcp-ssl.json"));
    }

    // Publishes `count` copies of the fixture to the raw input topic, blocking on
    // each send so every record is confirmed on the broker before the caller
    // moves on to draining the feature topic.
    private void produce(int count) throws Exception {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(props)) {
            for (int i = 0; i < count; i++) {
                producer.send(new ProducerRecord<>(INPUT_TOPIC, fixture())).get();
            }
        }
    }

    // Builds a consumer subscribed to the feature-vector topic from the earliest
    // offset, so it independently observes everything the online job publishes
    // across both the healthy and post-outage phases of the test.
    private Consumer<String, String> featureTopicConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "outage-test-reader");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        Consumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(List.of(FEATURE_TOPIC));
        return consumer;
    }

    // Collects up to `wanted` records, or gives up at the deadline. Returns what
    // it got either way, so the caller decides what a shortfall means.
    private List<String> drain(Consumer<String, String> consumer, int wanted, Duration timeout) {
        List<String> collected = new ArrayList<>();
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        while (collected.size() < wanted && System.currentTimeMillis() < deadline) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(2));
            records.forEach(record -> collected.add(record.value()));
        }
        return collected;
    }

    @Test
    void onlineJobKeepsPublishingAfterClickHouseDies() throws Exception {
        Client query = ClickHouseTestSupport.freshDatabase(CLICKHOUSE, DATABASE);
        ClickHouseConfig config = ClickHouseConfig.of(CLICKHOUSE.getHost(),
            CLICKHOUSE.getMappedPort(ClickHouseTestSupport.HTTP_PORT), DATABASE, "default", ClickHouseTestSupport.PASSWORD);

        // The online job deliberately gets no checkpointing and no restart
        // strategy here. That is asymmetric with the archive job below, and on
        // purpose: with no restart strategy configured, any failure that reaches
        // this job kills it outright instead of silently restarting it, which is
        // what makes the final assertEquals(JobStatus.RUNNING, ...) below mean
        // something. Enabling checkpointing here would let the online job
        // survive a fault the same way the archive job does, and the test would
        // no longer be able to tell "unaffected" from "recovered."
        StreamExecutionEnvironment onlineEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        onlineEnv.setParallelism(1);
        OnlineFeatureJob.build(onlineEnv, KAFKA.getBootstrapServers(), INPUT_TOPIC,
            FEATURE_TOPIC, DLQ_TOPIC, new SensorId("sensor-eu-1"));

        // The archive job, in contrast, needs checkpointing on: its sink flushes
        // on the checkpoint barrier and a failed insert fails the checkpoint
        // (see ArchiveJob.main()'s comment), so a short interval is what turns a
        // ClickHouse outage into the checkpoint failures and restarts this test
        // expects, rather than a job that just quietly stops flushing.
        StreamExecutionEnvironment archiveEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        archiveEnv.setParallelism(1);
        archiveEnv.enableCheckpointing(1_000L);
        ArchiveJob.build(archiveEnv, KAFKA.getBootstrapServers(), FEATURE_TOPIC, DLQ_TOPIC, config);

        // Both jobs run as independent embedded Flink executions in this one
        // test JVM. executeAsync hands back a JobClient for each without
        // blocking, so both are started before anything is produced.
        JobClient onlineJob = onlineEnv.executeAsync("outage-test-online");
        JobClient archiveJob = archiveEnv.executeAsync("outage-test-archive");

        try (Consumer<String, String> consumer = featureTopicConsumer()) {
            // Phase 1 — everything healthy. Prove the whole chain works before
            // breaking anything, otherwise a later "no rows" result is ambiguous.
            produce(5);
            assertEquals(5, drain(consumer, 5, Duration.ofSeconds(90)).size(),
                "the online job must publish five vectors while ClickHouse is up");

            // Poll ClickHouse until the archive job has actually written a row,
            // rather than assuming the async archive path has caught up by the
            // time the online job's own output is visible on the feature topic.
            long deadline = System.currentTimeMillis() + 90_000;
            long archived = 0;
            while (archived == 0 && System.currentTimeMillis() < deadline) {
                archived = query.queryAll("SELECT count() AS c FROM feature_vectors").get(0).getLong("c");
                if (archived == 0) {
                    Thread.sleep(500);
                }
            }
            assertTrue(archived > 0, "the archive job must reach ClickHouse before the outage");
            query.close();

            // Phase 2 — kill ClickHouse. The archive job will now fail its
            // checkpoints and restart on a loop; that is the designed behaviour,
            // and Kafka lag is where the failure surfaces.
            CLICKHOUSE.stop();

            // Phase 3 — the online job must not have noticed.
            produce(5);
            assertEquals(5, drain(consumer, 5, Duration.ofSeconds(120)).size(),
                "feature production must continue after ClickHouse dies");
            assertEquals(JobStatus.RUNNING, onlineJob.getJobStatus().get(),
                "an archive-side failure must never reach the online job");
        } finally {
            onlineJob.cancel().get();
            // The archive job may already be in a restart loop; cancelling it can
            // race with that, and its final state is not what this test is about.
            try {
                archiveJob.cancel().get();
            } catch (Exception ignored) {
                // Cancelling a restarting job is best-effort.
            }
        }
    }
}
