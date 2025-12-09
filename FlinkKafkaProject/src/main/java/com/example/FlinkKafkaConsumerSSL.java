package com.example;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import com.example.model.ZeekLogEvent;

import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;

/**
 * Consumer for ssl.log records, processes all fields from Zeek SSL logs,
 * sends for classification and anomaly detection.
 */
public class FlinkKafkaConsumerSSL {
    private static final Logger LOGGER = Logger.getLogger(FlinkKafkaConsumerSSL.class.getName());
    private static final String LOG_TYPE = "ssl";
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Load API configuration
        LOGGER.info("Loading API configuration...");
        try {
            APIConfig.loadConfig();
            LOGGER.info("Using supervised API endpoint: " + APIConfig.getSupervisedEndpoint(LOG_TYPE));
            LOGGER.info("Using supervised Kafka topic: " + APIConfig.getKafkaSupervisedTopic(LOG_TYPE));
        } catch (Exception e) {
            LOGGER.warning("Failed to load API configuration, using defaults: " + e.getMessage());
        }

        // Get Kafka properties from configuration
        Properties kafkaProps = APIConfig.getKafkaProperties(LOG_TYPE);
        LOGGER.info("Using Kafka bootstrap servers: " + APIConfig.getKafkaBootstrapServers());
        LOGGER.info("Using Kafka topic: " + APIConfig.getKafkaTopic(LOG_TYPE));

        // Create Kafka consumer
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
            APIConfig.getKafkaTopic(LOG_TYPE),
            new SimpleStringSchema(),
            kafkaProps
        );

        // Add source
        DataStream<String> kafkaStream = env.addSource(kafkaConsumer);

        // Process stream
        DataStream<ZeekLogEvent> processedStream = kafkaStream
            .map(new MapFunction<String, ZeekLogEvent>() {
                @Override
                public ZeekLogEvent map(String value) throws Exception {
                    try {
                        // Parse the JSON
                        JsonNode root = MAPPER.readTree(value);
                        
                        // Create a new ZeekLogEvent
                        ZeekLogEvent event = new ZeekLogEvent(LOG_TYPE);
                        
                        // Set common fields with timestamp parsing
                        event.setTs(parseTimestamp(safeGetString(root, "ts")));
                        event.setUid(safeGetString(root, "uid"));
                        event.setSourceIp(safeGetString(root, "id.orig_h"));
                        event.setSourcePort(safeGetInt(root, "id.orig_p", 0));
                        event.setDestIp(safeGetString(root, "id.resp_h"));
                        event.setDestPort(safeGetInt(root, "id.resp_p", 0));
                        
                        // SSL specific fields
                        event.addField("version", safeGetString(root, "version"));
                        event.addField("cipher", safeGetString(root, "cipher"));
                        event.addField("server_name", safeGetString(root, "server_name"));
                        event.addField("subject", safeGetString(root, "subject"));
                        event.addField("issuer", safeGetString(root, "issuer"));
                        
                        // Process the event through ML APIs
                        enrichWithMlServices(event, value);
                        
                        return event;
                    } catch (Exception e) {
                        LOGGER.log(Level.WARNING, "Failed to process message: " + e.getMessage(), e);
                        
                        // Return a simple event instead of throwing to keep the pipeline running
                        ZeekLogEvent fallbackEvent = new ZeekLogEvent(LOG_TYPE);
                        fallbackEvent.setPrediction("unknown_error");
                        return fallbackEvent;
                    }
                }
            })
            .filter(event -> event != null);
            
        // Convert ZeekLogEvent back to JSON and write to supervised topic
        DataStream<String> supervisedStream = processedStream
            .map(new MapFunction<ZeekLogEvent, String>() {
                @Override
                public String map(ZeekLogEvent event) throws Exception {
                    ObjectNode jsonNode = MAPPER.createObjectNode();
                    
                    // Add common fields
                    jsonNode.put("ts", event.getTs());
                    jsonNode.put("uid", event.getUid());
                    jsonNode.put("source_ip", event.getSourceIp());
                    jsonNode.put("source_port", event.getSourcePort());
                    jsonNode.put("dest_ip", event.getDestIp());
                    jsonNode.put("dest_port", event.getDestPort());
                    jsonNode.put("proto", event.getProto());
                    
                    // Add ML results
                    jsonNode.put("prediction", event.getPrediction());
                    jsonNode.put("anomaly_score", event.getAnomalyScore());
                    
                    // Add additional fields
                    event.getAdditionalFields().forEach((key, value) -> {
                        if (value instanceof String) {
                            jsonNode.put(key, (String) value);
                        } else if (value instanceof Integer) {
                            jsonNode.put(key, (Integer) value);
                        } else if (value instanceof Long) {
                            jsonNode.put(key, (Long) value);
                        } else if (value instanceof Double) {
                            jsonNode.put(key, (Double) value);
                        } else if (value instanceof Float) {
                            jsonNode.put(key, (Float) value);
                        } else if (value instanceof Boolean) {
                            jsonNode.put(key, (Boolean) value);
                        } else if (value != null) {
                            jsonNode.put(key, value.toString());
                        }
                    });
                    
                    return MAPPER.writeValueAsString(jsonNode);
                }
            });

        // Create new Kafka sink for supervised topic (replaces deprecated FlinkKafkaProducer)
        KafkaSink<String> supervisedSink = KafkaSink.<String>builder()
                .setBootstrapServers(APIConfig.getKafkaBootstrapServers())
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(APIConfig.getKafkaSupervisedTopic(LOG_TYPE))
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .build();

        // Add sink to write to supervised topic
        supervisedStream.sinkTo(supervisedSink);

        // Execute the Flink job
        env.execute("Flink Zeek SSL Log Processor");
    }
    
    /**
     * Enrich the event with prediction and anomaly score from ML services
     */
    private static void enrichWithMlServices(ZeekLogEvent event, String jsonData) {
        try {
            // Get prediction from supervised API
            String prediction = APIClient.getPrediction(LOG_TYPE, jsonData);
            event.setPrediction(prediction);
            
            // Get anomaly score from external unsupervised ML system
            double anomalyScore = APIClient.getUnsupervisedAnomalyScore(LOG_TYPE, jsonData);
            event.setAnomalyScore(anomalyScore);
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Error enriching event with ML services", e);
            event.setPrediction("unknown");
            event.setAnomalyScore(0.0);
        }
    }
    
    /**
     * Parse timestamp string to handle potential format issues
     */
    private static String parseTimestamp(String timestamp) {
        if (timestamp == null || timestamp.isEmpty()) {
            return null;
        }
        
        try {
            // If timestamp is already a number (Unix timestamp), return as is
            if (timestamp.matches("^\\d+(\\.\\d+)?$")) {
                return timestamp;
            }
            
            // If it's a string representation, try to extract the numeric part
            String numericPart = timestamp.replaceAll("[^\\d.]", "");
            if (!numericPart.isEmpty()) {
                return numericPart;
            }
            
            return timestamp;
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Failed to parse timestamp: " + timestamp, e);
            return timestamp;
        }
    }
    
    // Helper methods for safely extracting values
    
    private static String safeGetString(JsonNode root, String field) {
        JsonNode node = root.get(field);
        return (node == null || node.isNull()) ? null : node.asText();
    }
    
    private static int safeGetInt(JsonNode root, String field, int defaultValue) {
        JsonNode node = root.get(field);
        return (node == null || node.isNull()) ? defaultValue : node.asInt(defaultValue);
    }
}

