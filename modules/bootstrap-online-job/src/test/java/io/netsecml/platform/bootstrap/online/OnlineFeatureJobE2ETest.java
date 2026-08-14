package io.netsecml.platform.bootstrap.online;

import io.netsecml.platform.domain.event.SensorId;
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
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers(disabledWithoutDocker = true)
class OnlineFeatureJobE2ETest {
    @Container
    private final ConfluentKafkaContainer kafka =
        new ConfluentKafkaContainer("confluentinc/cp-kafka:7.6.1");

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

        Thread jobThread = new Thread(() -> {
            try {
                env.executeAsync("conn-foundation-e2e-test");
            } catch (Exception ignored) {
            }
        });
        jobThread.setDaemon(true);
        jobThread.start();

        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "e2e-test-reader");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        try (Consumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
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
        }
    }
}
