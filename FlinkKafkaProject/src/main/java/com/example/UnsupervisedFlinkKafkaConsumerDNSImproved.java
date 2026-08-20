package com.example;

import com.example.util.LogDeserializationSchema;
import com.example.util.NetworkAnomalyPreprocessor;
import com.example.util.TrustedIps;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;

import java.util.logging.Logger;

import java.util.HashMap;
import java.util.Map;
import java.io.FileWriter;
import java.io.IOException;
/**
 * Unsupervised Flink Kafka Consumer for DNS logs
 * Uses NetworkAnomalyPreprocessor (ND4J-based) for unified 33-feature extraction
 * 
 * All log types (HTTP, DNS, SSL, CONN) now use the SAME 33 features:
 * - 4 temporal (sin/cos encoding)
 * - 8 IP octets (source + dest)
 * - 2 port embeddings
 * - 3 interaction features (stateful)
 * - 11 flow dynamics (log-scaled)
 * - 5 TCP flags
 * Total: 33 features
 */
public class UnsupervisedFlinkKafkaConsumerDNSImproved {
    private static final Logger LOGGER = Logger.getLogger(UnsupervisedFlinkKafkaConsumerDNSImproved.class.getName());
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String LOG_TYPE = "dns";
    private static final double DEFAULT_ANOMALY_THRESHOLD = 0.5;
    private static final int MAX_BUFFER_SIZE = 10000;
    private static final long MAX_BUFFER_AGE_MS = 5_000L;
    private static final List<String> LEARNING_BUFFER = new ArrayList<>();
    private static long lastBufferFlushTime = System.currentTimeMillis();
    
    // BATCH PREDICTION BUFFER (for detection mode)
    private static final int PREDICTION_BATCH_SIZE = 50;
    private static final long PREDICTION_BATCH_TIMEOUT_MS = 200L;
    private static final List<String> PREDICTION_BUFFER = new ArrayList<>();
    private static final List<String> PREDICTION_BUFFER_LOGS = new ArrayList<>();
    private static long lastPredictionBatchTime = System.currentTimeMillis();
    
    // Dynamic threshold fetched once at startup (or after training)
    private static volatile double currentThreshold = DEFAULT_ANOMALY_THRESHOLD;
    private static volatile boolean thresholdInitialized = false;

    // NEW: Unified preprocessor for all log types (33 features)
    public static final int FEATURE_COUNT = 33;
    private static final NetworkAnomalyPreprocessor PREPROCESSOR = new NetworkAnomalyPreprocessor(
        10,     // window_size
        5000,   // max_active_sessions
        Arrays.asList(21, 22, 53, 80, 443, 8080, 3306, 445)  // known_ports
    );

    static {
        MAPPER.configure(org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    // parseTimestamp removed - NetworkAnomalyPreprocessor handles timestamp parsing

    /**
     * Fetch and update threshold from API (called when transitioning from learning to detection mode)
     */
    private static void updateThreshold() {
        synchronized (UnsupervisedFlinkKafkaConsumerDNSImproved.class) {
            LOGGER.info("Fetching dynamic threshold for " + LOG_TYPE + "...");
            Double threshold = APIClient.getUnsupervisedThreshold(LOG_TYPE);
            if (threshold != null) {
                currentThreshold = threshold;
                thresholdInitialized = true;
                LOGGER.info("Updated dynamic threshold for " + LOG_TYPE + ": " + currentThreshold);
            } else {
                LOGGER.info("No dynamic threshold available for " + LOG_TYPE + ", using default: " + DEFAULT_ANOMALY_THRESHOLD);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(16);

        LOGGER.info("Loading API configuration...");
        try {
            APIConfig.loadConfig();
        } catch (Exception e) {
            LOGGER.warning("Failed to load API configuration, using defaults: " + e.getMessage());
        }

        Properties consumerProps = new Properties();
        consumerProps.setProperty("bootstrap.servers", APIConfig.getKafkaBootstrapServers());
        consumerProps.setProperty("group.id", "unsupervised-dns-improved-consumer-group");
        consumerProps.setProperty("auto.offset.reset", "earliest");
        consumerProps.setProperty("enable.auto.commit", "false");
        consumerProps.setProperty("max.poll.records", "1000");
        consumerProps.setProperty("fetch.max.bytes", "104857600");
        consumerProps.setProperty("max.partition.fetch.bytes", "20971520");
        consumerProps.setProperty("max.poll.interval.ms", "300000");
        consumerProps.setProperty("session.timeout.ms", "60000");
        consumerProps.setProperty("heartbeat.interval.ms", "15000");
        consumerProps.setProperty("request.timeout.ms", "300000");
        consumerProps.setProperty("default.api.timeout.ms", "300000");
        
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
                        if (logEntry == null || logEntry.isNull()) return;
                        try {
                            JsonNode dnsData = logEntry.has("zeek-dns") ? logEntry.get("zeek-dns") : logEntry;
                            if (dnsData == null || dnsData.isNull()) return;

                            JsonNode orig = dnsData.get("id.orig_h");
                            String srcIp = (orig != null && !orig.isNull()) ? orig.asText("") : "";
                            if (TrustedIps.isTrusted(srcIp)) return;

                            // ==================== UNIFIED PREPROCESSING (33 FEATURES) ====================
                            // Uses NetworkAnomalyPreprocessor with ND4J library
                            
                            double[] features = PREPROCESSOR.buildRawVector(dnsData);

                            // Validate feature count
                            if (features.length != FEATURE_COUNT) {
                                LOGGER.warning(String.format(
                                    "Feature count mismatch! Expected %d, got %d",
                                    FEATURE_COUNT, features.length
                                ));
                                return;
                            }

                            String jsonData = MAPPER.writeValueAsString(features);

                            boolean isLearningEnabled = APIClient.getUnsupervisedLearningStatusCached(LOG_TYPE);

                            if (isLearningEnabled) {
                                addToLearningBuffer(jsonData);
                                APIClient.updateBufferSize(LOG_TYPE, LEARNING_BUFFER.size());
                                return;
                            }

                            // Just transitioned from learning to detection mode - fetch threshold
                            if (!thresholdInitialized) {
                                updateThreshold();
                            }

                            flushLearningBufferIfNeeded(true);

                            // OPTIMIZED: Use batch prediction
                            addToPredictionBuffer(jsonData, logEntry.toString(), out);
                        } catch (Exception e) {
                            String msg = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
                            String entryStr = (logEntry != null && !logEntry.isNull()) ? logEntry.toString() : "null";
                            LOGGER.warning("Error processing DNS log entry: " + entryStr + " - " + msg);
                            if (LOGGER.isLoggable(Level.FINE) && e.getCause() != null) {
                                LOGGER.fine("Caused by: " + e.getCause().getClass().getSimpleName() + ": " + e.getCause().getMessage());
                            }
                        }
                    }
                })
                .name("Unsupervised " + LOG_TYPE.toUpperCase() + " Log Processing (Improved)");

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

    private static void addToPredictionBuffer(String jsonData, String logJson, Collector<String> out) {
        synchronized (PREDICTION_BUFFER) {
            PREDICTION_BUFFER.add(jsonData);
            PREDICTION_BUFFER_LOGS.add(logJson);
            
            boolean sizeLimitReached = PREDICTION_BUFFER.size() >= PREDICTION_BATCH_SIZE;
            boolean ageLimitReached = System.currentTimeMillis() - lastPredictionBatchTime >= PREDICTION_BATCH_TIMEOUT_MS;
            
            if (sizeLimitReached || ageLimitReached) {
                flushPredictionBuffer(out);
            }
        }
    }

    private static void flushPredictionBuffer(Collector<String> out) {
        synchronized (PREDICTION_BUFFER) {
            if (PREDICTION_BUFFER.isEmpty()) return;

            List<String> batch = new ArrayList<>(PREDICTION_BUFFER);
            List<String> logs = new ArrayList<>(PREDICTION_BUFFER_LOGS);
            PREDICTION_BUFFER.clear();
            PREDICTION_BUFFER_LOGS.clear();
            lastPredictionBatchTime = System.currentTimeMillis();

            List<Double> scores = APIClient.getBatchAnomalyScores(LOG_TYPE, batch);
            
            if (scores != null && scores.size() == batch.size()) {
                for (int i = 0; i < scores.size(); i++) {
                    double anomalyScore = scores.get(i);
                    if (anomalyScore >= currentThreshold) {
                        LOGGER.warning("Anomaly detected for " + LOG_TYPE + " (Score: " + anomalyScore + 
                            ", Threshold: " + currentThreshold + ")");
                        out.collect(logs.get(i));
                    }
                }
                LOGGER.fine("Batch prediction processed: " + batch.size() + " records");
            } else {
                LOGGER.warning("Batch prediction failed for " + LOG_TYPE + " (API returned null or size mismatch; ensure LSTM model is loaded for " + LOG_TYPE + ", or API is healthy)");
            }
        }
    }
}
