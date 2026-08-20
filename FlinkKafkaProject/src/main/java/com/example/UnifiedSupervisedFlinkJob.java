package com.example;

import com.example.util.LogDeserializationSchema;
import com.example.util.SessionAssemblerProcessFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * Unified Supervised Flink Job: One job instead of 4.
 * - Conn is the anchor; sessions are assembled by UID.
 * - Reads zeek-* topics to buffer logs, malicious-* as triggers.
 * - When LSTM-AE flags a log, we collect all logs for that connection (UID)
 *   and run session-level UNSW-style classification.
 */
public class UnifiedSupervisedFlinkJob {

    private static final Logger LOG = Logger.getLogger(UnifiedSupervisedFlinkJob.class.getName());

    private static String extractUid(JsonNode node) {
        if (node == null) return "";
        if (node.has("uid")) return node.get("uid").asText("");
        if (node.has("zeek-conn") && node.get("zeek-conn").has("uid"))
            return node.get("zeek-conn").get("uid").asText("");
        if (node.has("zeek-dns") && node.get("zeek-dns").has("uid"))
            return node.get("zeek-dns").get("uid").asText("");
        if (node.has("zeek-http") && node.get("zeek-http").has("uid"))
            return node.get("zeek-http").get("uid").asText("");
        if (node.has("zeek-ssl") && node.get("zeek-ssl").has("uid"))
            return node.get("zeek-ssl").get("uid").asText("");
        return "";
    }

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(8);

        Properties consumerProps = new Properties();
        consumerProps.setProperty("bootstrap.servers", APIConfig.getKafkaBootstrapServers());
        consumerProps.setProperty("group.id", "unified-supervised-session-consumer-group");
        consumerProps.setProperty("auto.offset.reset", "earliest");
        consumerProps.setProperty("enable.auto.commit", "false");
        consumerProps.setProperty("max.poll.records", "1000");
        consumerProps.setProperty("fetch.max.bytes", "104857600");
        consumerProps.setProperty("max.partition.fetch.bytes", "20971520");
        consumerProps.setProperty("max.poll.interval.ms", "300000");
        consumerProps.setProperty("session.timeout.ms", "60000");
        consumerProps.setProperty("heartbeat.interval.ms", "15000");

        List<String> zeekTopics = Arrays.asList(
            APIConfig.getKafkaTopic("conn"),
            APIConfig.getKafkaTopic("dns"),
            APIConfig.getKafkaTopic("http"),
            APIConfig.getKafkaTopic("ssl")
        );

        List<String> maliciousTopics = Arrays.asList(
            APIConfig.getKafkaMaliciousTopic("conn"),
            APIConfig.getKafkaMaliciousTopic("dns"),
            APIConfig.getKafkaMaliciousTopic("http"),
            APIConfig.getKafkaMaliciousTopic("ssl")
        );

        KafkaSource<JsonNode> zeekSource = KafkaSource.<JsonNode>builder()
            .setBootstrapServers(APIConfig.getKafkaBootstrapServers())
            .setTopics(zeekTopics)
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(new LogDeserializationSchema())
            .setProperties(consumerProps)
            .build();

        KafkaSource<JsonNode> maliciousSource = KafkaSource.<JsonNode>builder()
            .setBootstrapServers(APIConfig.getKafkaBootstrapServers())
            .setTopics(maliciousTopics)
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(new LogDeserializationSchema())
            .setProperties(consumerProps)
            .build();

        DataStream<JsonNode> zeekStream = env.fromSource(zeekSource, WatermarkStrategy.noWatermarks(), "Zeek Source")
            .filter(node -> node != null && !extractUid(node).isEmpty());

        DataStream<JsonNode> maliciousStream = env.fromSource(maliciousSource, WatermarkStrategy.noWatermarks(), "Malicious Source")
            .filter(node -> node != null && !extractUid(node).isEmpty());

        DataStream<String> supervisedOutput = zeekStream
            .connect(maliciousStream)
            .keyBy(UnifiedSupervisedFlinkJob::extractUid, UnifiedSupervisedFlinkJob::extractUid)
            .process(new SessionAssemblerProcessFunction());

        String supervisedTopic = APIConfig.getProperty("kafka.topic.supervised.unsw42", "supervised-unsw42");
        KafkaSink<String> sink = KafkaSink.<String>builder()
            .setBootstrapServers(APIConfig.getKafkaBootstrapServers())
            .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                .setTopic(supervisedTopic)
                .setValueSerializationSchema(new SimpleStringSchema())
                .build())
            .build();

        supervisedOutput.sinkTo(sink);

        LOG.info("Unified Supervised Job: zeek-* + malicious-* -> session assembly (UNSW42 from conn+http+dns+ssl) -> /predict_unsw42 -> supervised-unsw42");
        env.execute("Unified Supervised Flink Job (Session-Based)");
    }
}
