package com.example;

import com.example.APIClient;
import com.example.APIConfig;
import com.example.model.ZeekLogEvent;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

public class FlinkKafkaConsumerConn {
    private static final Logger LOGGER = Logger.getLogger(FlinkKafkaConsumerConn.class.getName());
    private static final String LOG_TYPE = "conn";
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Load API configuration
        LOGGER.info("Loading API configuration...");
        try {
            APIConfig.loadConfig();
            LOGGER.info("Using supervised API endpoint: " + APIConfig.getSupervisedEndpoint(LOG_TYPE));
            LOGGER.info("Using supervised Kafka topic: " + APIConfig.getKafkaSupervisedTopic(LOG_TYPE));
            LOGGER.info("Note: The unsupervised ML system connects directly to Kafka (not through Flink)");
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

        // Process stream to create ZeekLogEvents
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
                            event.setProto(safeGetString(root, "proto"));
                            
                            // Set conn-specific fields
                            event.setService(safeGetString(root, "service"));
                            event.setDuration(safeGetDouble(root, "duration", 0.0));
                            event.setOrigBytes(safeGetLong(root, "orig_bytes", 0L));
                            event.setRespBytes(safeGetLong(root, "resp_bytes", 0L));
                            event.setConnState(safeGetString(root, "conn_state"));
                            
                            // Add additional fields
                            event.addField("local_orig", safeGetBoolean(root, "local_orig", false));
                            event.addField("missed_bytes", safeGetLong(root, "missed_bytes", 0L));
                            event.addField("history", safeGetString(root, "history"));
                            event.addField("orig_pkts", safeGetLong(root, "orig_pkts", 0L));
                            event.addField("orig_ip_bytes", safeGetLong(root, "orig_ip_bytes", 0L));
                            event.addField("resp_pkts", safeGetLong(root, "resp_pkts", 0L));
                            event.addField("resp_ip_bytes", safeGetLong(root, "resp_ip_bytes", 0L));
                            
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
                    
                    // Add optional fields if present
                    if (event.getService() != null) jsonNode.put("service", event.getService());
                    if (event.getDuration() > 0) jsonNode.put("duration", event.getDuration());
                    if (event.getOrigBytes() > 0) jsonNode.put("orig_bytes", event.getOrigBytes());
                    if (event.getRespBytes() > 0) jsonNode.put("resp_bytes", event.getRespBytes());
                    if (event.getConnState() != null) jsonNode.put("conn_state", event.getConnState());
                    
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
        env.execute("Flink Zeek Conn Log Processor");
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
    private static String parseTimestamp(Object timestamp) {
        if (timestamp == null) {
            return String.valueOf(System.currentTimeMillis() / 1000.0);
        }
        
        if (timestamp instanceof String) {
            String ts = (String) timestamp;
            try {
                // Try to parse as double in case it's in scientific notation
                double value = Double.parseDouble(ts);
                return String.valueOf(value);
            } catch (NumberFormatException e) {
                // If it's not a number, return as-is
                return ts;
            }
        } else if (timestamp instanceof Number) {
            return String.valueOf(((Number) timestamp).doubleValue());
        } else {
            return timestamp.toString();
        }
    }
    
    /**
     * Safely get a string value from JsonNode
     */
    private static String safeGetString(JsonNode node, String fieldName) {
        JsonNode field = node.get(fieldName);
        return field != null && !field.isNull() ? field.asText() : null;
    }
    
    /**
     * Safely get an integer value from JsonNode
     */
    private static int safeGetInt(JsonNode node, String fieldName, int defaultValue) {
        JsonNode field = node.get(fieldName);
        return field != null && !field.isNull() ? field.asInt(defaultValue) : defaultValue;
    }
    
    /**
     * Safely get a long value from JsonNode
     */
    private static long safeGetLong(JsonNode node, String fieldName, long defaultValue) {
        JsonNode field = node.get(fieldName);
        return field != null && !field.isNull() ? field.asLong(defaultValue) : defaultValue;
    }
    
    /**
     * Safely get a double value from JsonNode
     */
    private static double safeGetDouble(JsonNode node, String fieldName, double defaultValue) {
        JsonNode field = node.get(fieldName);
        return field != null && !field.isNull() ? field.asDouble(defaultValue) : defaultValue;
    }
    
    /**
     * Safely get a boolean value from JsonNode
     */
    private static boolean safeGetBoolean(JsonNode node, String fieldName, boolean defaultValue) {
        JsonNode field = node.get(fieldName);
        return field != null && !field.isNull() ? field.asBoolean(defaultValue) : defaultValue;
    }
}

