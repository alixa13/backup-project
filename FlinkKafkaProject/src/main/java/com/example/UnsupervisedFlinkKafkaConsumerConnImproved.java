package com.example;

import com.example.model.ZeekConnLog;
import com.example.util.FeatureEncoder;
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

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Improved Unsupervised Flink Kafka Consumer for Connection Logs
 * with proper feature engineering (replaces hashCode() approach).
 *
 * Feature Count: 41 features (expanded from 20)
 * - 5 source IP features
 * - 5 destination IP features
 * - 2 port features
 * - 1 protocol feature
 * - 9 service features (one-hot)
 * - 1 connection state feature
 * - 4 temporal features
 * - 1 duration feature
 * - 2 byte count features
 * - 2 packet count features
 * - 7 derived statistical features
 * - 2 boolean flags
 *
 * Total: 41 features
 */
public class UnsupervisedFlinkKafkaConsumerConnImproved {
    private static final Logger LOGGER = Logger.getLogger(UnsupervisedFlinkKafkaConsumerConnImproved.class.getName());
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String LOG_TYPE = "conn";
    private static final int MAX_BUFFER_SIZE = 2000;
    private static final long MAX_BUFFER_AGE_MS = 3_000L;
    private static final List<String> LEARNING_BUFFER = new ArrayList<>();
    private static long lastBufferFlushTime = System.currentTimeMillis();

    // Feature count for this log type
    public static final int FEATURE_COUNT = 41;

    static {
        MAPPER.configure(
            org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES,
            false
        );
    }

    /**
     * Parse timestamp string to double value
     */
    private static double parseTimestamp(Object timestamp) {
        try {
            if (timestamp == null) return 0.0;

            String timestampStr;
            if (timestamp instanceof String) {
                timestampStr = (String) timestamp;
            } else if (timestamp instanceof Number) {
                timestampStr = timestamp.toString();
            } else {
                timestampStr = timestamp.toString();
            }

            if (timestampStr.trim().isEmpty()) return 0.0;

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
        env.setParallelism(16);

        Properties consumerProps = new Properties();
        consumerProps.setProperty("bootstrap.servers", APIConfig.getKafkaBootstrapServers());
        consumerProps.setProperty("group.id", "unsupervised-conn-improved-consumer-group");
        consumerProps.setProperty("auto.offset.reset", "earliest");
        consumerProps.setProperty("enable.auto.commit", "false");
        consumerProps.setProperty("max.poll.records", "500");
        consumerProps.setProperty("fetch.max.bytes", "52428800");
        consumerProps.setProperty("max.partition.fetch.bytes", "10485760");
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

                            ZeekConnLog connLog = MAPPER.treeToValue(connData, ZeekConnLog.class);

                            // Extract timestamp
                            double timestamp = connData.has("ts") ? parseTimestamp(connData.get("ts")) : 0.0;

                            // ==================== IMPROVED FEATURE EXTRACTION ====================
                            // Build feature vector with proper encoding (41 features total)

                            List<Double> featureList = new ArrayList<>();

                            // 1-5: Source IP Address (5 features: 4 octets + private flag)
                            double[] srcIP = FeatureEncoder.encodeIPAddress(connLog.getIdOrigH());
                            for (double f : srcIP) featureList.add(f);

                            // 6-10: Destination IP Address (5 features: 4 octets + private flag)
                            double[] dstIP = FeatureEncoder.encodeIPAddress(connLog.getIdRespH());
                            for (double f : dstIP) featureList.add(f);

                            // 11: Source Port (normalized)
                            featureList.add(FeatureEncoder.normalizePort(connLog.getIdOrigP()));

                            // 12: Destination Port (normalized)
                            featureList.add(FeatureEncoder.normalizePort(connLog.getIdRespP()));

                            // 13: Protocol (label encoded, normalized)
                            featureList.add(FeatureEncoder.encodeProtocol(connLog.getProto()));

                            // 14-22: Service (one-hot encoded, 9 features)
                            double[] service = FeatureEncoder.encodeService(connLog.getService());
                            for (double f : service) featureList.add(f);

                            // 23: Connection State (label encoded, normalized)
                            featureList.add(FeatureEncoder.encodeConnState(connLog.getConnState()));

                            // 24-27: Temporal Features (4 features)
                            double[] temporal = FeatureEncoder.encodeTimestamp(timestamp);
                            for (double f : temporal) featureList.add(f);

                            // 28: Duration (log-scaled, normalized)
                            featureList.add(FeatureEncoder.normalizeDuration(connLog.getDuration()));

                            // 29: Originator Bytes (log-scaled, normalized)
                            featureList.add(FeatureEncoder.normalizeBytes(connLog.getOrigBytes()));

                            // 30: Responder Bytes (log-scaled, normalized)
                            featureList.add(FeatureEncoder.normalizeBytes(connLog.getRespBytes()));

                            // 31: Originator Packets (log-scaled, normalized)
                            featureList.add(FeatureEncoder.normalizePackets(connLog.getOrigPkts()));

                            // 32: Responder Packets (log-scaled, normalized)
                            featureList.add(FeatureEncoder.normalizePackets(connLog.getRespPkts()));

                            // 33-39: Derived Statistical Features (7 features)
                            // [byte_ratio, packet_ratio, throughput, packet_rate, avg_orig_pkt_size, avg_resp_pkt_size, symmetry]
                            double[] derived = FeatureEncoder.derivedConnectionFeatures(
                                connLog.getOrigBytes(),
                                connLog.getRespBytes(),
                                connLog.getOrigPkts(),
                                connLog.getRespPkts(),
                                connLog.getDuration()
                            );
                            for (double f : derived) featureList.add(f);

                            // 40: Local Originator Flag
                            featureList.add(FeatureEncoder.booleanToDouble(connLog.getLocalOrig()));

                            // 41: Missed Bytes Flag (binary: has missed bytes or not)
                            featureList.add((connLog.getMissedBytes() != null && connLog.getMissedBytes() > 0) ? 1.0 : 0.0);

                            // Convert to array
                            double[] features = featureList.stream().mapToDouble(Double::doubleValue).toArray();

                            // Validate feature count
                            if (features.length != FEATURE_COUNT) {
                                LOGGER.warning(String.format(
                                    "Feature count mismatch! Expected %d, got %d",
                                    FEATURE_COUNT, features.length
                                ));
                            }

                            String jsonData = MAPPER.writeValueAsString(features);

                            // Check if learning is enabled (cached, ~10s resolution)
                            boolean isLearningEnabled = APIClient.getUnsupervisedLearningStatusCached(LOG_TYPE);

                            if (isLearningEnabled) {
                                addToLearningBuffer(jsonData);
                                APIClient.updateBufferSize(LOG_TYPE, LEARNING_BUFFER.size());
                                return;
                            }

                            // Flush any buffered data if we just left learning mode
                            flushLearningBufferIfNeeded(true);

                            // Learning mode disabled, make prediction
                            double anomalyScore = APIClient.getUnsupervisedAnomalyScore(LOG_TYPE, jsonData);

                            if (anomalyScore >= 0.5) {
                                LOGGER.warning("Anomaly detected for " + LOG_TYPE + " (Score: " + anomalyScore + "): " +
                                    connLog.getIdOrigH() + ":" + connLog.getIdOrigP() + " -> " +
                                    connLog.getIdRespH() + ":" + connLog.getIdRespP());
                                out.collect(logEntry.toString());
                            } else if (anomalyScore != -1.0) {
                                LOGGER.fine("Normal " + LOG_TYPE + " (Score: " + anomalyScore + ")");
                            } else {
                                LOGGER.warning("Failed to get anomaly score for " + LOG_TYPE + " log");
                            }

                        } catch (Exception e) {
                            LOGGER.log(Level.WARNING, "Error processing CONN log entry: " + e.getMessage(), e);
                        }
                    }
                })
                .name("Unsupervised " + LOG_TYPE.toUpperCase() + " Log Processing (Improved)");

        // Create Kafka sink for malicious topic
        KafkaSink<String> maliciousSink = KafkaSink.<String>builder()
                .setBootstrapServers(APIConfig.getKafkaBootstrapServers())
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(APIConfig.getKafkaMaliciousTopic(LOG_TYPE))
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .build();

        anomalousData.sinkTo(maliciousSink);

        LOGGER.info("Starting Improved Flink Job for Unsupervised " + LOG_TYPE.toUpperCase() + " Logs");
        LOGGER.info("Feature count: " + FEATURE_COUNT);
        env.execute("Unsupervised Flink Kafka Consumer for " + LOG_TYPE.toUpperCase() + " Logs (Improved)");
    }

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

    private static void flushLearningBufferIfNeeded(boolean forceFlush) {
        synchronized (LEARNING_BUFFER) {
            if (LEARNING_BUFFER.isEmpty()) return;

            boolean sizeLimitReached = LEARNING_BUFFER.size() >= MAX_BUFFER_SIZE;
            boolean ageLimitReached = System.currentTimeMillis() - lastBufferFlushTime >= MAX_BUFFER_AGE_MS;

            if (!(forceFlush || sizeLimitReached || ageLimitReached)) return;

            List<String> batch = new ArrayList<>(LEARNING_BUFFER);
            LEARNING_BUFFER.clear();
            lastBufferFlushTime = System.currentTimeMillis();

            boolean success = APIClient.sendUnsupervisedBatch(LOG_TYPE, batch);
            if (!success) {
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
