package com.example;

import com.example.util.BufferedAnomalyProcessFunction;
import com.example.util.LogDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * Unsupervised Flink Kafka consumer for HTTP logs.
 *
 * <p>Extracts the unified 33-feature vector with NetworkAnomalyPreprocessor, scores
 * batches against the LSTM autoencoder API, and republishes anomalous records to the
 * malicious topic.
 *
 * <p>The stream is keyed on the source IP before processing. That is required for the
 * processing-time timer that flushes a partly filled buffer when traffic goes quiet, and
 * it keeps each host's preprocessor state on a single thread - the preprocessor mutates
 * per-IP interaction state in place and used to be a static instance shared by every
 * subtask. All buffering now lives in BufferedAnomalyProcessFunction, one instance per
 * subtask, instead of static fields shared across the JVM.
 */
public class UnsupervisedFlinkKafkaConsumerHTTPImproved {
    private static final Logger LOGGER =
            Logger.getLogger(UnsupervisedFlinkKafkaConsumerHTTPImproved.class.getName());

    private static final String LOG_TYPE = "http";
    private static final String NESTED_KEY = "zeek-http";

    /** Unified feature count across all log types. */
    public static final int FEATURE_COUNT = 33;

    /** Fallback when the API has no trained threshold yet. */
    private static final double DEFAULT_ANOMALY_THRESHOLD = 0.5;

    private static final int PREDICTION_BATCH_SIZE = 50;
    private static final long PREDICTION_BATCH_TIMEOUT_MS = 200L;
    private static final int LEARNING_BATCH_SIZE = 10_000;
    private static final long LEARNING_BATCH_TIMEOUT_MS = 5_000L;

    /** Re-fetch the threshold periodically so a retrained model is picked up. */
    private static final long THRESHOLD_REFRESH_MS = 300_000L;

    /** Preprocessor settings; window size matches the model's timesteps. */
    private static final int WINDOW_SIZE = 10;
    private static final int MAX_ACTIVE_SESSIONS = 5000;

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(16);

        Properties consumerProps = new Properties();
        consumerProps.setProperty("bootstrap.servers", APIConfig.getKafkaBootstrapServers());
        consumerProps.setProperty("group.id", "unsupervised-http-improved-consumer-group");
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

        DataStream<String> anomalousData = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source " + LOG_TYPE)
                .keyBy(new BufferedAnomalyProcessFunction.SourceIpKeySelector(NESTED_KEY))
                .process(new BufferedAnomalyProcessFunction(
                        LOG_TYPE,
                        NESTED_KEY,
                        FEATURE_COUNT,
                        PREDICTION_BATCH_SIZE,
                        PREDICTION_BATCH_TIMEOUT_MS,
                        LEARNING_BATCH_SIZE,
                        LEARNING_BATCH_TIMEOUT_MS,
                        DEFAULT_ANOMALY_THRESHOLD,
                        THRESHOLD_REFRESH_MS,
                        WINDOW_SIZE,
                        MAX_ACTIVE_SESSIONS,
                        Arrays.asList(21, 22, 53, 80, 443, 8080, 3306, 445)))
                .name("Unsupervised " + LOG_TYPE.toUpperCase() + " Log Processing (Improved)");

        KafkaSink<String> maliciousSink = KafkaSink.<String>builder()
                .setBootstrapServers(APIConfig.getKafkaBootstrapServers())
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(APIConfig.getKafkaMaliciousTopic(LOG_TYPE))
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .build();

        anomalousData.sinkTo(maliciousSink);

        LOGGER.info("Starting Improved Flink Job for Unsupervised " + LOG_TYPE.toUpperCase() + " Logs");
        LOGGER.info("Feature count: " + FEATURE_COUNT);
        env.execute("Unsupervised Flink Kafka Consumer for " + LOG_TYPE.toUpperCase() + " Logs (Improved)");
    }
}
