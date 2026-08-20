package com.example;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Utility class to load and access API configuration from properties file.
 * This replaces hardcoded URLs in the Java classes.
 */
public class APIConfig {
    private static final Logger LOGGER = Logger.getLogger(APIConfig.class.getName());
    private static final String DEFAULT_CONFIG_FILE_PATH = "/opt/flink/config/api-config.properties";
    private static Properties properties = new Properties();
    private static boolean initialized = false;

    /**
     * Defensive cleanup for topic strings loaded from properties.
     * Trailing whitespace can cause Kafka InvalidTopicException.
     */
    private static String sanitizeTopic(String topic) {
        return topic == null ? null : topic.trim();
    }

    // Initialize configuration on first access
    static {
        loadConfig();
    }

    /**
     * Load configuration from properties file
     */
    /**
     * Resolve the properties file: system property "api.config.file", env
     * API_CONFIG_FILE, else the packaged default. The path used to be a hardcoded
     * constant, which is why the copy in src/main/resources was never read and why the
     * class could not be exercised outside a container.
     */
    public static String getConfigFilePath() {
        String path = System.getProperty("api.config.file");
        if (path != null && !path.isEmpty()) return path;
        path = System.getenv("API_CONFIG_FILE");
        if (path != null && !path.isEmpty()) return path;
        return DEFAULT_CONFIG_FILE_PATH;
    }

    public static void loadConfig() {
        String configPath = getConfigFilePath();
        try (InputStream input = new FileInputStream(configPath)) {
            properties.load(input);
            initialized = true;
            LOGGER.info("Successfully loaded API configuration from: " + configPath);
        } catch (IOException ex) {
            // Mark initialized regardless: getProperty() re-invoked loadConfig() on every
            // call while this stayed false, so a missing file meant re-opening it and
            // logging a full stack trace for every single property read.
            initialized = true;
            LOGGER.log(Level.SEVERE,
                    "Failed to load API configuration from " + configPath + "; using defaults", ex);
        }
    }

    /**
     * Reload configuration from file (if configuration changes at runtime)
     */
    public static void reloadConfig() {
        properties.clear();
        loadConfig();
    }

    /**
     * Get property value by key
     * 
     * @param key Property key
     * @return Property value or null if key not found
     */
    public static String getProperty(String key) {
        if (!initialized) {
            loadConfig();
        }
        return properties.getProperty(key);
    }

    /**
     * Get property with default value
     * 
     * @param key Property key
     * @param defaultValue Default value if key not found
     * @return Property value or default value if key not found
     */
    public static String getProperty(String key, String defaultValue) {
        if (!initialized) {
            loadConfig();
        }
        return properties.getProperty(key, defaultValue);
    }

    /**
     * Get integer property
     * 
     * @param key Property key
     * @param defaultValue Default value if key not found or not an integer
     * @return Property value as integer or default if not found/not an integer
     */
    public static int getIntProperty(String key, int defaultValue) {
        String value = getProperty(key);
        if (value == null) {
            return defaultValue;
        }
        
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            LOGGER.warning("Invalid integer value for key: " + key + ", using default");
            return defaultValue;
        }
    }

    /**
     * Get supervised API base URL
     * @return The base URL for supervised API
     */
    public static String getSupervisedBaseUrl() {
        return getProperty("supervised.api.base.url", "http://supervised:8000");
    }

    /**
     * Get supervised API endpoint for a specific log type
     * 
     * @param logType The log type (conn, http, dns, ssl)
     * @return The full URL for the specified log type endpoint
     */
    public static String getSupervisedEndpoint(String logType) {
        String baseUrl = getSupervisedBaseUrl();
        String endpoint = getProperty("supervised.api." + logType + ".endpoint", "/predict_" + logType);
        return baseUrl + endpoint;
    }

    /**
     * Get supervised API endpoint for UNSW42 (unified session model).
     * @return The full URL for /predict_unsw42
     */
    public static String getSupervisedUnsw42Endpoint() {
        String baseUrl = getSupervisedBaseUrl();
        String endpoint = getProperty("supervised.api.unsw42.endpoint", "/predict_unsw42");
        return baseUrl + endpoint;
    }

    /**
     * Get unsupervised API base URL
     * @return The base URL for unsupervised API
     */
    public static String getUnsupervisedBaseUrl() {
        return getProperty("unsupervised.api.base.url", "http://lstm-autoencoder:5000");
    }

    /**
     * Get unsupervised API endpoint for a specific log type and action
     * 
     * @param logType The log type (conn, http, dns, ssl)
     * @param action The action (learning.status, learning.enable, learning.disable, predict)
     * @return The full URL for the specified endpoint
     */
    public static String getUnsupervisedEndpoint(String logType, String action) {
        String baseUrl = getUnsupervisedBaseUrl();
        String endpoint = getProperty("unsupervised.api." + logType + "." + action);
        if (endpoint == null || endpoint.trim().isEmpty()) {
            LOGGER.warning("No endpoint configured for unsupervised.api." + logType + "." + action + ", using fallback");
            endpoint = "/" + action.replace(".", "/") + "/" + logType;
        }
        return baseUrl + endpoint;
    }

    /**
     * Get unsupervised API learning status endpoint for a specific log type
     * 
     * @param logType The log type (conn, http, dns, ssl)
     * @return The full URL for the learning status endpoint
     */
    public static String getUnsupervisedLearningStatusEndpoint(String logType) {
        return getUnsupervisedEndpoint(logType, "learning.status");
    }

    /**
     * Get unsupervised API predict endpoint for a specific log type
     * 
     * @param logType The log type (conn, http, dns, ssl)
     * @return The full URL for the predict endpoint
     */
    public static String getUnsupervisedPredictEndpoint(String logType) {
        return getUnsupervisedEndpoint(logType, "predict");
    }

    /**
     * Get unsupervised API buffer update endpoint for a specific log type
     * 
     * @param logType The log type (conn, http, dns, ssl)
     * @return The full URL for the buffer update endpoint
     */
    public static String getUnsupervisedBufferUpdateEndpoint(String logType) {
        return getUnsupervisedEndpoint(logType, "buffer.update");
    }

    /**
     * Get unsupervised API threshold endpoint for a specific log type
     * 
     * @param logType The log type (conn, http, dns, ssl)
     * @return The full URL for the threshold endpoint
     */
    public static String getUnsupervisedThresholdEndpoint(String logType) {
        return getUnsupervisedEndpoint(logType, "threshold");
    }

    /**
     * Get connection timeout in milliseconds
     * @return Connection timeout
     */
    public static int getConnectionTimeout() {
        return getIntProperty("api.connection.timeout", 5000);
    }

    /**
     * Get read timeout in milliseconds
     * @return Read timeout
     */
    public static int getReadTimeout() {
        return getIntProperty("api.read.timeout", 30000);
    }

    /**
     * Get retry count for API calls
     * @return Retry count
     */
    public static int getRetryCount() {
        return getIntProperty("api.retry.count", 3);
    }

    /**
     * Base delay for the retry backoff, in ms. The delay doubles per attempt and is
     * capped by {@link #getRetryBackoffMaxMs()}.
     * @return Base retry delay in ms
     */
    public static int getRetryDelayMs() {
        return getIntProperty("api.retry.delay", 500);
    }

    /**
     * Upper bound on a single retry sleep, in ms. This sleep happens on a Flink task
     * thread, so it must stay small enough that a stalled API cannot block the pipeline.
     * @return Max retry backoff in ms
     */
    public static int getRetryBackoffMaxMs() {
        return getIntProperty("api.retry.backoff.max.ms", 4000);
    }

    /**
     * Test connectivity to a specific API endpoint
     * 
     * @param apiUrl The URL to test
     * @return true if connection successful, false otherwise
     */
    public static boolean testApiConnectivity(String apiUrl) {
        try {
            URL url = new URL(apiUrl);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("HEAD");
            connection.setConnectTimeout(getConnectionTimeout());
            connection.connect();
            
            int responseCode = connection.getResponseCode();
            connection.disconnect();
            
            boolean success = (responseCode >= 200 && responseCode < 400);
            if (success) {
                LOGGER.info("Successfully connected to API endpoint: " + apiUrl);
            } else {
                LOGGER.warning("Failed to connect to API endpoint: " + apiUrl + " (Response code: " + responseCode + ")");
            }
            return success;
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Error connecting to API endpoint: " + apiUrl, e);
            return false;
        }
    }

    /**
     * Test connectivity to all configured API endpoints
     * 
     * @return true if all endpoints are reachable, false if any fail
     */
    public static boolean testAllApiEndpoints() {
        boolean allSuccessful = true;
        
        // Test supervised endpoint (UNSW42 only)
        if (!testApiConnectivity(getSupervisedEndpoint("unsw42"))) {
            allSuccessful = false;
        }
        
        // Test unsupervised endpoints
        if (!testApiConnectivity(getUnsupervisedLearningStatusEndpoint("dns"))) {
            allSuccessful = false;
        }
        
        return allSuccessful;
    }

    /**
     * Get Kafka bootstrap servers
     * @return The Kafka bootstrap servers
     */
    public static String getKafkaBootstrapServers() {
        return getProperty("kafka.bootstrap.servers", "kafka:9092");
    }
    
    /**
     * Get Kafka consumer group ID for a specific log type
     * 
     * @param logType The log type (conn, http, dns, ssl)
     * @return The consumer group ID for the specified log type
     */
    public static String getKafkaConsumerGroup(String logType) {
        return getProperty("kafka.consumer.group." + logType, "flink-consumer-group-" + logType);
    }
    
    /**
     * Get Kafka topic name for a specific log type
     * 
     * @param logType The log type (conn, http, dns, ssl)
     * @return The topic name for the specified log type
     */
    public static String getKafkaTopic(String logType) {
        return sanitizeTopic(getProperty("kafka.topic." + logType, "zeek-" + logType));
    }

    /**
     * Get Kafka auto offset reset configuration
     * @return The auto offset reset setting
     */
    public static String getKafkaAutoOffsetReset() {
        return getProperty("kafka.auto.offset.reset", "latest");
    }
    
    /**
     * Get Kafka auto commit setting
     * @return The auto commit setting
     */
    public static boolean getKafkaEnableAutoCommit() {
        return "true".equalsIgnoreCase(getProperty("kafka.enable.auto.commit", "true"));
    }

    /**
     * Get Kafka malicious topic name for a specific log type
     * 
     * @param logType The log type (conn, http, dns, ssl)
     * @return The malicious topic name for the specified log type
     */
    public static String getKafkaMaliciousTopic(String logType) {
        return sanitizeTopic(getProperty("kafka.topic.malicious." + logType, "malicious-" + logType));
    }
    
    /**
     * Get comprehensive Kafka properties for a consumer
     * 
     * @param logType The log type (conn, http, dns, ssl)
     * @return Properties object with full Kafka consumer configuration
     */
    public static Properties getKafkaProperties(String logType) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", getKafkaBootstrapServers());
        props.setProperty("group.id", getKafkaConsumerGroup(logType));
        props.setProperty("auto.offset.reset", getKafkaAutoOffsetReset());
        props.setProperty("enable.auto.commit", String.valueOf(getKafkaEnableAutoCommit()));
        
        return props;
    }

    /**
     * Get Kafka supervised topic name for a specific log type
     * 
     * @param logType The log type (conn, http, dns, ssl)
     * @return The supervised topic name for the specified log type
     */
    public static String getKafkaSupervisedTopic(String logType) {
        return sanitizeTopic(getProperty("kafka.topic.supervised." + logType, "supervised-" + logType));
    }

    /**
     * Get comprehensive Kafka properties for a producer
     * 
     * @return Properties object with full Kafka producer configuration
     */
    public static Properties getKafkaProducerProperties() {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", getKafkaBootstrapServers());
        props.setProperty("acks", getProperty("kafka.producer.acks", "all"));
        props.setProperty("retries", getProperty("kafka.producer.retries", "3"));
        props.setProperty("batch.size", getProperty("kafka.producer.batch.size", "16384"));
        props.setProperty("linger.ms", getProperty("kafka.producer.linger.ms", "1"));
        props.setProperty("buffer.memory", getProperty("kafka.producer.buffer.memory", "33554432"));
        props.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return props;
    }
} 