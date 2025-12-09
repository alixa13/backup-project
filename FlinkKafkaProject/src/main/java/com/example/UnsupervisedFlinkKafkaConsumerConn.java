package com.example;

import com.example.model.ZeekConnLog;
import com.example.util.LogDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

public class UnsupervisedFlinkKafkaConsumerConn {
    private static final Logger LOGGER = Logger.getLogger(UnsupervisedFlinkKafkaConsumerConn.class.getName());
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String LOG_TYPE = "conn";
    private static final int MAX_BUFFER_SIZE = 2000; // handle burst traffic up to 40K logs/sec
    private static final long MAX_BUFFER_AGE_MS = 3_000L; // flush every 3s for real-time detection
    private static final List<String> LEARNING_BUFFER = new ArrayList<>();
    private static long lastBufferFlushTime = System.currentTimeMillis();
    
    static {
        // Configure ObjectMapper to ignore unknown fields
        MAPPER.configure(org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    /**
     * Parse timestamp string to double value
     * Handles scientific notation and regular decimal format
     */
    private static double parseTimestamp(Object timestamp) {
        try {
            if (timestamp == null) {
                return 0.0;
            }
            
            String timestampStr;
            if (timestamp instanceof String) {
                timestampStr = (String) timestamp;
            } else if (timestamp instanceof Number) {
                timestampStr = timestamp.toString();
            } else {
                timestampStr = timestamp.toString();
            }
            
            if (timestampStr.trim().isEmpty()) {
                return 0.0;
            }
            
            // Handle scientific notation and regular decimal format
            return Double.parseDouble(timestampStr.trim());
        } catch (NumberFormatException e) {
            LOGGER.warning("Failed to parse timestamp: " + timestamp + ", using 0.0");
            return 0.0;
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Unexpected error parsing timestamp: " + timestamp + ", using 0.0", e);
            return 0.0;
        }
    }

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // Increase operator parallelism to consume faster
        env.setParallelism(16);

        // Configure Kafka source with separate consumer configuration
        Properties consumerProps = new Properties();
        consumerProps.setProperty("bootstrap.servers", APIConfig.getKafkaBootstrapServers());
        consumerProps.setProperty("group.id", "unsupervised-conn-consumer-group");
        consumerProps.setProperty("auto.offset.reset", "earliest");
        consumerProps.setProperty("enable.auto.commit", "false");
        consumerProps.setProperty("max.poll.records", "500");
        consumerProps.setProperty("fetch.max.bytes", "52428800"); // 50MB
        consumerProps.setProperty("max.partition.fetch.bytes", "10485760"); // 10MB
        consumerProps.setProperty("max.poll.interval.ms", "300000");
        consumerProps.setProperty("session.timeout.ms", "60000");
        consumerProps.setProperty("heartbeat.interval.ms", "15000");
        
        String inputTopic = APIConfig.getKafkaTopic(LOG_TYPE);

        KafkaSource<JsonNode> source = KafkaSource.<JsonNode>builder()
                .setBootstrapServers(APIConfig.getKafkaBootstrapServers())
                .setTopics(inputTopic)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new LogDeserializationSchema())
                .setProperties(consumerProps)
                .build();

        // Process data and filter for anomalies
        DataStream<String> anomalousData = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source " + LOG_TYPE)
                .process(new ProcessFunction<JsonNode, String>() {
                    @Override
                    public void processElement(JsonNode logEntry, Context ctx, Collector<String> out) throws Exception {
                        try {
                            // Handle nested structure: {"zeek-conn": {...}}
                            JsonNode connData = logEntry;
                            if (logEntry.has("zeek-conn")) {
                                connData = logEntry.get("zeek-conn");
                            }
                            
                            // Extract features for LSTM API (all available features)
                            ZeekConnLog connLog = MAPPER.treeToValue(connData, ZeekConnLog.class);
                        
                        // Convert all numeric fields to features array according to features.txt
                        double[] features = new double[] {
                            // 1. ts (timestamp) - convert to double
                            connData.has("ts") ? parseTimestamp(connData.get("ts")) : 0.0,
                            // 2. uid - convert to numeric (hash code)
                            connLog.getUid() != null ? (double) connLog.getUid().hashCode() : 0.0,
                            // 3. id.orig_h - convert to numeric (hash code)
                            connLog.getIdOrigH() != null ? (double) connLog.getIdOrigH().hashCode() : 0.0,
                            // 4. id.orig_p (port)
                            connLog.getIdOrigP() != null ? connLog.getIdOrigP() : 0.0,
                            // 5. id.resp_h - convert to numeric (hash code)
                            connLog.getIdRespH() != null ? (double) connLog.getIdRespH().hashCode() : 0.0,
                            // 6. id.resp_p (port)
                            connLog.getIdRespP() != null ? connLog.getIdRespP() : 0.0,
                            // 7. proto - convert to numeric (hash code)
                            connLog.getProto() != null ? (double) connLog.getProto().hashCode() : 0.0,
                            // 8. service - convert to numeric (hash code)
                            connLog.getService() != null ? (double) connLog.getService().hashCode() : 0.0,
                            // 9. duration
                            connLog.getDuration() != null ? connLog.getDuration() : 0.0,
                            // 10. orig_bytes
                            connLog.getOrigBytes() != null ? connLog.getOrigBytes() : 0.0,
                            // 11. resp_bytes
                            connLog.getRespBytes() != null ? connLog.getRespBytes() : 0.0,
                            // 12. conn_state - convert to numeric (hash code)
                            connLog.getConnState() != null ? (double) connLog.getConnState().hashCode() : 0.0,
                            // 13. local_orig - convert to numeric (1 for true, 0 for false)
                            connLog.getLocalOrig() != null ? (connLog.getLocalOrig() ? 1.0 : 0.0) : 0.0,
                            // 14. missed_bytes
                            connLog.getMissedBytes() != null ? connLog.getMissedBytes() : 0.0,
                            // 15. history - convert to numeric (hash code)
                            connLog.getHistory() != null ? (double) connLog.getHistory().hashCode() : 0.0,
                            // 16. orig_pkts
                            connLog.getOrigPkts() != null ? connLog.getOrigPkts() : 0.0,
                            // 17. orig_ip_bytes
                            connLog.getOrigIpBytes() != null ? connLog.getOrigIpBytes() : 0.0,
                            // 18. resp_pkts
                            connLog.getRespPkts() != null ? connLog.getRespPkts() : 0.0,
                            // 19. resp_ip_bytes
                            connLog.getRespIpBytes() != null ? connLog.getRespIpBytes() : 0.0,
                            // 20. tunnel_parents - convert to numeric (hash code)
                            connLog.getTunnelParents() != null && connLog.getTunnelParents().length > 0 ? 
                                (double) connLog.getTunnelParents()[0].hashCode() : 0.0
                        };
                        
                        String jsonData = MAPPER.writeValueAsString(features);

                        // Check if learning is enabled (cached, ~10s resolution)
                        boolean isLearningEnabled = APIClient.getUnsupervisedLearningStatusCached(LOG_TYPE);

                        // Buffer while learning is on, then flush once it turns off or buffer gets big/old
                        if (isLearningEnabled) {
                            addToLearningBuffer(jsonData);
                            APIClient.updateBufferSize(LOG_TYPE, LEARNING_BUFFER.size());
                            return; // skip scoring while collecting
                        }

                        // Flush any buffered data if we just left learning mode
                        flushLearningBufferIfNeeded(true);

                            // Learning mode disabled, make prediction and conditionally send to malicious topic
                            double anomalyScore = APIClient.getUnsupervisedAnomalyScore(LOG_TYPE, jsonData);

                            if (anomalyScore >= 0.5) { // Anomaly threshold
                                LOGGER.warning("Anomaly detected for " + LOG_TYPE + " log (Score: " + anomalyScore + "): " + logEntry.toString());
                                // Output anomalous data to be sent to malicious topic
                                out.collect(logEntry.toString());
                            } else if (anomalyScore != -1.0) {
                                LOGGER.info("Normal " + LOG_TYPE + " log (Score: " + anomalyScore + "): " + logEntry.toString().substring(0, Math.min(logEntry.toString().length(), 200)) + "...");
                            } else {
                                LOGGER.warning("Failed to get anomaly score for " + LOG_TYPE + " log, skipping: " + logEntry.toString());
                        }
                    } catch (Exception e) {
                        LOGGER.warning("Error processing CONN log entry: " + logEntry.toString() + " - " + e.getMessage());
                        // Don't throw the exception, just log it and continue processing
                    }
                    }
                })
                .name("Unsupervised " + LOG_TYPE.toUpperCase() + " Log Processing");

        // Create Kafka sink for malicious topic
        KafkaSink<String> maliciousSink = KafkaSink.<String>builder()
                .setBootstrapServers(APIConfig.getKafkaBootstrapServers())
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(APIConfig.getKafkaMaliciousTopic(LOG_TYPE))
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .build();

        // Send anomalous data to malicious topic
        anomalousData.sinkTo(maliciousSink);

        LOGGER.info("Starting Flink Job for Unsupervised " + LOG_TYPE.toUpperCase() + " Logs");
        env.execute("Unsupervised Flink Kafka Consumer for " + LOG_TYPE.toUpperCase() + " Logs");
    }

    /**
     * Add a record to the in-memory learning buffer with simple caps to avoid
     * unbounded growth while learning mode is enabled.
     */
    private static void addToLearningBuffer(String jsonData) {
        synchronized (LEARNING_BUFFER) {
            LEARNING_BUFFER.add(jsonData);
            boolean sizeLimitReached = LEARNING_BUFFER.size() >= MAX_BUFFER_SIZE;
            boolean ageLimitReached = System.currentTimeMillis() - lastBufferFlushTime >= MAX_BUFFER_AGE_MS;

            if (sizeLimitReached || ageLimitReached) {
                flushLearningBufferIfNeeded(false);
            }
        }
    }

    /**
     * Flush buffered learning data to the unsupervised API when either
     * learning mode turns off or the buffer hits size/time limits.
     *
     * @param forceFlush whether to flush regardless of size/age thresholds
     */
    private static void flushLearningBufferIfNeeded(boolean forceFlush) {
        synchronized (LEARNING_BUFFER) {
            if (LEARNING_BUFFER.isEmpty()) {
                return;
            }

            boolean sizeLimitReached = LEARNING_BUFFER.size() >= MAX_BUFFER_SIZE;
            boolean ageLimitReached = System.currentTimeMillis() - lastBufferFlushTime >= MAX_BUFFER_AGE_MS;

            if (!(forceFlush || sizeLimitReached || ageLimitReached)) {
                return;
            }

            List<String> batch = new ArrayList<>(LEARNING_BUFFER);
            LEARNING_BUFFER.clear();
            lastBufferFlushTime = System.currentTimeMillis();

            boolean success = APIClient.sendUnsupervisedBatch(LOG_TYPE, batch);
            if (!success) {
                // If sending fails, re-queue data for a later attempt
                LEARNING_BUFFER.addAll(batch);
                LOGGER.warning("Failed to flush learning buffer; data re-queued (" + LEARNING_BUFFER.size() + " records)");
                APIClient.updateBufferSize(LOG_TYPE, LEARNING_BUFFER.size());
            } else {
                LOGGER.info("Flushed learning buffer to API (" + batch.size() + " records)");
                APIClient.updateBufferSize(LOG_TYPE, 0);
            }
        }
    }
} 