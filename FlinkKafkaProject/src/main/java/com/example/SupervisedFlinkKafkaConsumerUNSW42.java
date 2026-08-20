package com.example;

import com.example.util.LogDeserializationSchema;
import com.example.util.UNSW42FeatureEncoder;
import com.example.util.TrustedIps;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Supervised Flink Job for CONN logs using UNSW-NB15 42-feature model.
 * Reads from malicious-conn, preprocesses with UNSW42FeatureEncoder, calls /predict_unsw42.
 */
public class SupervisedFlinkKafkaConsumerUNSW42 {
    private static final Logger LOG = Logger.getLogger(SupervisedFlinkKafkaConsumerUNSW42.class.getName());
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(16);

        Properties consumerProps = new Properties();
        consumerProps.setProperty("bootstrap.servers", APIConfig.getKafkaBootstrapServers());
        consumerProps.setProperty("group.id", "supervised-unsw42-consumer-group");
        consumerProps.setProperty("auto.offset.reset", "earliest");
        consumerProps.setProperty("enable.auto.commit", "false");
        consumerProps.setProperty("max.poll.records", "1000");
        consumerProps.setProperty("fetch.max.bytes", "104857600");
        consumerProps.setProperty("max.partition.fetch.bytes", "20971520");
        consumerProps.setProperty("max.poll.interval.ms", "300000");
        consumerProps.setProperty("session.timeout.ms", "60000");
        consumerProps.setProperty("heartbeat.interval.ms", "15000");

        String inputTopic = APIConfig.getKafkaMaliciousTopic("conn");

        KafkaSource<JsonNode> source = KafkaSource.<JsonNode>builder()
                .setBootstrapServers(APIConfig.getKafkaBootstrapServers())
                .setTopics(inputTopic)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new LogDeserializationSchema())
                .setProperties(consumerProps)
                .build();

        DataStream<String> output = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source conn")
                .filter(node -> node != null)
                .keyBy(node -> {
                    JsonNode d = node.has("zeek-conn") ? node.get("zeek-conn") : node;
                    return d.has("id.orig_h") ? d.get("id.orig_h").asText("") : "";
                })
                .process(new KeyedProcessFunction<String, JsonNode, String>() {
                    private ValueState<UNSW42FeatureEncoder.ConnState> stateState;

                    @Override
                    public void open(Configuration parameters) {
                        stateState = getRuntimeContext().getState(
                                new ValueStateDescriptor<>("connState", UNSW42FeatureEncoder.ConnState.class));
                    }

                    @Override
                    public void processElement(JsonNode logEntry, Context ctx, Collector<String> out) throws Exception {
                        try {
                            JsonNode connData = logEntry.has("zeek-conn") ? logEntry.get("zeek-conn") : logEntry;
                            String srcIp = connData.has("id.orig_h") ? connData.get("id.orig_h").asText("") : "";
                            if (TrustedIps.isTrusted(srcIp)) return;

                            UNSW42FeatureEncoder.ConnState state = stateState.value();
                            if (state == null) {
                                state = new UNSW42FeatureEncoder.ConnState();
                            }
                            double[] features = UNSW42FeatureEncoder.encodeConn(connData, state);
                            stateState.update(state);

                            if (features == null) return;

                            String jsonData = MAPPER.writeValueAsString(features);
                            String prediction = APIClient.getPrediction("unsw42", jsonData);

                            if (prediction != null && !prediction.equals("normal") && !prediction.equals("unknown")) {
                                String src = connData.has("id.orig_h") ? connData.get("id.orig_h").asText() : "?";
                                String dst = connData.has("id.resp_h") ? connData.get("id.resp_h").asText() : "?";
                                LOG.warning("Attack detected (UNSW42): " + prediction + " | " + src + " -> " + dst);
                                out.collect(logEntry.toString());
                            }
                        } catch (Exception e) {
                            LOG.log(Level.WARNING, "Error processing CONN log: " + e.getMessage(), e);
                        }
                    }
                })
                .name("Supervised UNSW42 (malicious-conn -> 42 features -> /predict_unsw42)");

        String outTopic = APIConfig.getProperty("kafka.topic.supervised.unsw42", "supervised-unsw42");
        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers(APIConfig.getKafkaBootstrapServers())
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(outTopic)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .build();

        output.sinkTo(sink);

        LOG.info("Supervised UNSW42: malicious-conn -> UNSW 42 features -> /predict_unsw42 -> supervised-unsw42");
        env.execute("Supervised Flink Kafka Consumer for CONN (UNSW42)");
    }
}
