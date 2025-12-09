package com.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.List;
import java.util.stream.Collectors;

/**
 * API Client for interacting with the supervised and unsupervised ML APIs.
 * Includes circuit breaker and health check logic.
 */
public class APIClient {
    private static final Logger LOG = LoggerFactory.getLogger(APIClient.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    
    // Circuit breaker state
    private static boolean supervisedApiHealthy = true;
    private static boolean unsupervisedApiHealthy = true;
    private static int consecutiveFailures = 0;
    private static boolean circuitOpen = false;
    private static long circuitOpenTime = 0;
    private static final int FAILURE_THRESHOLD = 5;
    private static final long CIRCUIT_RESET_TIMEOUT_MS = 60000; // 1 minute
    private static long lastHealthCheckTime = 0;
    private static final long HEALTH_CHECK_INTERVAL_MS = 60000; // 1 minute
    
    // Learning status cache (per log_type), to avoid calling the API on every packet
    private static final long LEARNING_STATUS_CACHE_MS = 10_000L; // 10 seconds
    private static final Map<String, Boolean> learningStatusCache = new ConcurrentHashMap<>();
    private static final Map<String, Long> learningStatusLastCheck = new ConcurrentHashMap<>();
    
    // Buffer size reporting throttle (per log_type)
    private static final Map<String, Integer> lastReportedBufferSize = new ConcurrentHashMap<>();
    private static final Map<String, Long> lastBufferReportTime = new ConcurrentHashMap<>();
    private static final long BUFFER_REPORT_INTERVAL_MS = 5_000L;
    
    // Static initialization block to log important information
    static {
        LOG.info("Initializing API Client");
        LOG.info("NOTE: The unsupervised ML system (LSTM Autoencoder) is completely separate from Flink");
        LOG.info("      and connects directly to Kafka for data streams at {}", APIConfig.getKafkaBootstrapServers());
        LOG.info("      Flink only requests anomaly scores via API at {}", APIConfig.getUnsupervisedBaseUrl());
    }

    /**
     * Send a request to an API endpoint with retry logic.
     * 
     * @param endpoint The API endpoint to call
     * @param jsonData The JSON data to send
     * @param retries  The number of times to retry on failure
     * @return The JSON response from the API, or null if the request fails after all retries.
     */
    public static JsonNode sendRequest(String endpoint, String jsonData, int retries) {
        final String FUNCTION_NAME = "sendRequest";
        
        // Check if circuit is open
        if (isCircuitOpen()) {
            LOG.warn("[{}] Circuit is open, skipping request to: {}", FUNCTION_NAME, endpoint);
            return null;
        }
        
        int attempts = 0;
        while (attempts < retries) {
            attempts++;
            LOG.debug("[{}] Attempt {} of {} - Making HTTP request to: {}", FUNCTION_NAME, attempts, retries, endpoint);
            
            try {
                URL url = new URL(endpoint);
                HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("POST");
                conn.setRequestProperty("Content-Type", "application/json");
                conn.setRequestProperty("User-Agent", "Java/11");
                conn.setDoOutput(true);
                conn.setConnectTimeout(APIConfig.getConnectionTimeout());
                conn.setReadTimeout(APIConfig.getReadTimeout());
                
                try (var os = conn.getOutputStream()) {
                    byte[] input = jsonData.getBytes(StandardCharsets.UTF_8);
                    os.write(input, 0, input.length);
                }
                
                int responseCode = conn.getResponseCode();
                String responseMessage = conn.getResponseMessage();

                if (responseCode >= 200 && responseCode < 300) {
                    try (InputStream is = conn.getInputStream()) {
                        String responseText = new String(is.readAllBytes(), StandardCharsets.UTF_8);
                        JsonNode response = MAPPER.readTree(responseText);
                        
                        LOG.debug("[{}] SUCCESS (Attempt {}) - Response: {}", FUNCTION_NAME, attempts, responseText);
                        
                        // Reset failure counter on success
                        resetCircuitBreaker();
                        return response;
                    }
                } else {
                    // Handle error responses
                    LOG.warn("[{}] ERROR (Attempt {}) - HTTP {}: {}", FUNCTION_NAME, attempts, responseCode, responseMessage);
                    
                    if (responseCode >= 400 && responseCode < 500) {
                        try (InputStream es = conn.getErrorStream()) {
                            if (es != null) {
                                String errorBody = new String(es.readAllBytes(), StandardCharsets.UTF_8);
                                LOG.debug("[{}] Error Response Body: {}", FUNCTION_NAME, errorBody);
                            }
                        }
                    }
                    
                    recordFailure();
                }
            } catch (Exception e) {
                LOG.warn("[{}] EXCEPTION (Attempt {}) - Error calling API: {} - Exception: {}", 
                        FUNCTION_NAME, attempts, endpoint, e.getMessage());
                LOG.debug("[{}] Full exception details:", FUNCTION_NAME, e);
                recordFailure();
            }
            
            if (attempts < retries) {
                try {
                    // Exponential backoff
                    long backoffTime = (long) Math.pow(2, attempts) * 1000;
                    LOG.debug("[{}] Retrying request to {} in {}ms (attempt {} of {})", 
                            FUNCTION_NAME, endpoint, backoffTime, attempts, retries);
                    Thread.sleep(backoffTime);
                } catch (InterruptedException ie) {
                    LOG.warn("[{}] Request interrupted during retry delay", FUNCTION_NAME);
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
        
        // Log failure after all retries
        LOG.warn("[{}] FAILED - Failed to get a successful response from API after {} attempts. Endpoint: {}", 
                FUNCTION_NAME, attempts, endpoint);
        return null;
    }
    
    /**
     * Check if the circuit breaker is open.
     * If the reset timeout has passed, it will move to half-open state.
     *
     * @return true if the circuit is open, false otherwise.
     */
    private static synchronized boolean isCircuitOpen() {
        if (circuitOpen) {
            long currentTime = System.currentTimeMillis();
            if (currentTime - circuitOpenTime > CIRCUIT_RESET_TIMEOUT_MS) {
                LOG.info("Circuit timeout elapsed, entering half-open state");
                circuitOpen = false;
                return false;
            }
            return true;
        }
        return false;
    }
    
    /**
     * Record a failure. If the failure threshold is reached, open the circuit.
     */
    private static synchronized void recordFailure() {
        consecutiveFailures++;
        if (consecutiveFailures >= FAILURE_THRESHOLD) {
            circuitOpen = true;
            circuitOpenTime = System.currentTimeMillis();
            LOG.warn("Circuit breaker opened after {} consecutive failures", consecutiveFailures);
        }
    }
    
    /**
     * Reset the circuit breaker after a successful call.
     */
    private static synchronized void resetCircuitBreaker() {
        if (consecutiveFailures > 0) {
            LOG.info("Resetting circuit breaker after successful API call");
            consecutiveFailures = 0;
            circuitOpen = false;
        }
    }
    
    /**
     * Check the health of supervised and unsupervised APIs.
     */
    private static synchronized void checkApiHealth() {
        long currentTime = System.currentTimeMillis();
        if (currentTime - lastHealthCheckTime < HEALTH_CHECK_INTERVAL_MS) {
            return; // Don't check too frequently
        }
        
        lastHealthCheckTime = currentTime;
        LOG.info("Performing API health check");
        
        // Check supervised API health
        boolean wasSupervisedHealthy = supervisedApiHealthy;
        try {
            URL url = new URL(APIConfig.getSupervisedBaseUrl() + "/health");
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            conn.setConnectTimeout(5000);
            int statusCode = conn.getResponseCode();
            supervisedApiHealthy = (statusCode == 200);
            
            if (wasSupervisedHealthy != supervisedApiHealthy) {
                if (supervisedApiHealthy) {
                    LOG.info("Supervised API is now available");
                } else {
                    LOG.warn("Supervised API is unavailable (HTTP {})", statusCode);
                }
            }
        } catch (Exception e) {
            if (supervisedApiHealthy) {
                supervisedApiHealthy = false;
                LOG.warn("Supervised API is unavailable: {}", e.getMessage());
            }
        }
        
        // Check unsupervised API health
        boolean wasUnsupervisedHealthy = unsupervisedApiHealthy;
        try {
            URL url = new URL(APIConfig.getUnsupervisedBaseUrl() + "/health");
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            conn.setConnectTimeout(5000);
            int statusCode = conn.getResponseCode();
            unsupervisedApiHealthy = (statusCode == 200);
            
            if (wasUnsupervisedHealthy != unsupervisedApiHealthy) {
                if (unsupervisedApiHealthy) {
                    LOG.info("External unsupervised ML system is now available");
                } else {
                    LOG.warn("External unsupervised ML system is unavailable (HTTP {})", statusCode);
                }
            }
        } catch (Exception e) {
            if (unsupervisedApiHealthy) {
                unsupervisedApiHealthy = false;
                LOG.warn("External unsupervised ML system is unavailable: {}", e.getMessage());
            }
        }
    }
    
    /**
     * Get prediction from supervised ML API.
     *
     * @param logType  The log type (conn, http, dns, ssl)
     * @param jsonData The JSON data to send for prediction.
     * @return The prediction result, or "unknown" if the request fails.
     */
    public static String getPrediction(String logType, String jsonData) {
        final String FUNCTION_NAME = "getPrediction";
        
        // Proactively check API health to update status
        checkApiHealth();
        
        // If API is known to be unhealthy, skip the request
        if (!supervisedApiHealthy) {
            LOG.debug("[{}] Skipping request to supervised API (known to be unhealthy)", FUNCTION_NAME);
            return "unknown";
        }
        
        String endpoint = APIConfig.getSupervisedEndpoint(logType);
        LOG.info("[{}] Starting API call - Function: {}, LogType: {}, Endpoint: {}", 
                FUNCTION_NAME, FUNCTION_NAME, logType, endpoint);
        
        try {
            // Parse the features array from the input
            JsonNode featuresArray = MAPPER.readTree(jsonData);
            if (!featuresArray.isArray()) {
                LOG.warn("[{}] Input data is not a JSON array: {}", FUNCTION_NAME, jsonData);
                return "unknown";
            }

            // Create the format that the supervised API expects: {"features": [...]}
            String requestJson = "{\"features\": " + featuresArray.toString() + "}";

            LOG.info("[{}] REQUEST DETAILS:", FUNCTION_NAME);
            LOG.info("[{}]   LogType: {}", FUNCTION_NAME, logType);
            LOG.info("[{}]   Endpoint: {}", FUNCTION_NAME, endpoint);
            LOG.info("[{}]   Request JSON: {}", FUNCTION_NAME, requestJson);
            
            // Send the formatted request
            JsonNode response = sendRequest(endpoint, requestJson, APIConfig.getRetryCount());
            if (response != null && response.has("prediction")) {
                String prediction = response.get("prediction").asText();
                LOG.info("[{}] SUCCESS - Received prediction: {} for logType: {}", 
                        FUNCTION_NAME, prediction, logType);
                return prediction;
            } else {
                LOG.warn("[{}] ERROR - Response missing prediction field: {}", FUNCTION_NAME, response);
            }
        } catch (Exception e) {
            LOG.warn("[{}] EXCEPTION - Error formatting data for supervised API: {} - Exception: {}", 
                    FUNCTION_NAME, e.getMessage(), e.getMessage());
            LOG.debug("[{}] Full exception details:", FUNCTION_NAME, e);
        }

        LOG.info("[{}] Returning 'unknown' (default) for logType: {}", FUNCTION_NAME, logType);
        return "unknown";
    }

    /**
     * Get learning status from unsupervised ML API.
     *
     * @param logType The log type (conn, http, dns, ssl)
     * @return true if learning is enabled, false otherwise.
     */
    public static boolean getUnsupervisedLearningStatus(String logType) {
        final String FUNCTION_NAME = "getUnsupervisedLearningStatus";
        
        // Validate logType parameter
        if (logType == null || logType.trim().isEmpty()) {
            LOG.error("[{}] Invalid logType parameter: {}", FUNCTION_NAME, logType);
            return false;
        }
        
        String endpoint = APIConfig.getUnsupervisedLearningStatusEndpoint(logType);
        LOG.info("[{}] Starting API call - Function: {}, LogType: {}, Endpoint: {}", 
                FUNCTION_NAME, FUNCTION_NAME, logType, endpoint);
        
        try {
            URL url = new URL(endpoint);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            conn.setRequestProperty("Accept", "application/json");
            conn.setRequestProperty("User-Agent", "Java/11");
            conn.setConnectTimeout(5000);
            conn.setReadTimeout(10000);

            int responseCode = conn.getResponseCode();

            if (responseCode == 200) {
                try (InputStream is = conn.getInputStream()) {
                    String responseText = new String(is.readAllBytes(), StandardCharsets.UTF_8);
                    LOG.debug("[{}] Response Body: {}", FUNCTION_NAME, responseText);
                    
                    JsonNode response = MAPPER.readTree(responseText);
                    boolean learningEnabled = response.path("learning_enabled").asBoolean(false);
                    
                    LOG.info("[{}] SUCCESS - Learning enabled: {} for logType: {}", 
                            FUNCTION_NAME, learningEnabled, logType);
                    
                    return learningEnabled;
                }
            } else {
                // Log error response body
                try (InputStream es = conn.getErrorStream()) {
                    if (es != null) {
                        String errorBody = new String(es.readAllBytes(), StandardCharsets.UTF_8);
                        LOG.warn("[{}] Error Response Body: {}", FUNCTION_NAME, errorBody);
                    }
                }
                
                LOG.warn("[{}] ERROR - Unsupervised API learning status returned error: {} (Response code: {})", 
                        FUNCTION_NAME, endpoint, responseCode);
            }
        } catch (Exception e) {
            LOG.warn("[{}] EXCEPTION - Error calling unsupervised API for learning status: {} - Exception: {}", 
                    FUNCTION_NAME, endpoint, e.getMessage());
            LOG.debug("[{}] Full exception details:", FUNCTION_NAME, e);
        }
        
        LOG.info("[{}] Returning false (default) for logType: {}", FUNCTION_NAME, logType);
        return false;
    }
    
    /**
     * Get unsupervised learning status with a simple 10s cache per log type.
     * This avoids hitting the LSTM API for every single packet.
     *
     * @param logType The log type (conn, http, dns, ssl)
     * @return true if learning is enabled, false otherwise.
     */
    public static boolean getUnsupervisedLearningStatusCached(String logType) {
        long now = System.currentTimeMillis();
        
        Long lastCheck = learningStatusLastCheck.get(logType);
        Boolean cached = learningStatusCache.get(logType);
        
        if (lastCheck == null || now - lastCheck > LEARNING_STATUS_CACHE_MS || cached == null) {
            boolean currentStatus = getUnsupervisedLearningStatus(logType);
            learningStatusCache.put(logType, currentStatus);
            learningStatusLastCheck.put(logType, now);
            return currentStatus;
        }
        
        return cached;
    }
    
    /**
     * Send data to unsupervised ML API for prediction and get anomaly score with specific log type.
     *
     * @param logType The log type (conn, http, dns, ssl)
     * @param jsonData The JSON data to send for prediction (should be a features array).
     * @return The anomaly score or -1.0 if the request failed.
     */
    public static double getUnsupervisedAnomalyScore(String logType, String jsonData) {
        final String FUNCTION_NAME = "getUnsupervisedAnomalyScore";
        
        // Proactively check API health to update status
        checkApiHealth();

        if (!unsupervisedApiHealthy) {
            LOG.debug("[{}] Skipping request to unsupervised API (known to be unhealthy)", FUNCTION_NAME);
            return -1.0; // Indicate failure
        }

        String endpoint = APIConfig.getUnsupervisedPredictEndpoint(logType);
        LOG.info("[{}] Starting API call - Function: {}, LogType: {}, Endpoint: {}", 
                FUNCTION_NAME, FUNCTION_NAME, logType, endpoint);
        
        try {
            // Parse the input data to ensure it's a valid JSON array
            JsonNode dataArray = MAPPER.readTree(jsonData);
            if (!dataArray.isArray()) {
                LOG.warn("[{}] Input data is not a JSON array: {}", FUNCTION_NAME, jsonData);
                return -1.0;
            }

            // Format the data as {"data": [...]}
            String requestJson = String.format("{\"data\": %s}", jsonData);

            LOG.debug("[{}] Request JSON: {}", FUNCTION_NAME, requestJson);

            JsonNode response = sendRequest(endpoint, requestJson, APIConfig.getRetryCount());

            if (response != null && response.has("anomaly_scores") && response.get("anomaly_scores").has("mse")) {
                double score = response.get("anomaly_scores").get("mse").asDouble();
                LOG.info("[{}] SUCCESS - Received anomaly score: {} for logType: {}", 
                        FUNCTION_NAME, score, logType);
                return score;
            } else if (response != null && response.has("learning_enabled") && response.get("learning_enabled").asBoolean()) {
                // If in learning mode, API returns a success message instead of a score.
                // Return a specific value to indicate data was collected for learning.
                LOG.info("[{}] LEARNING MODE - Data collected for learning, logType: {}", FUNCTION_NAME, logType);
                return -2.0;
            } else {
                LOG.warn("[{}] ERROR - Unexpected response format: {}", FUNCTION_NAME, response);
            }
        } catch (Exception e) {
            LOG.warn("[{}] EXCEPTION - Error formatting data for LSTM API: {} - Exception: {}", 
                    FUNCTION_NAME, e.getMessage(), e.getMessage());
            LOG.debug("[{}] Full exception details:", FUNCTION_NAME, e);
        }
        
        LOG.info("[{}] Returning -1.0 (failure) for logType: {}", FUNCTION_NAME, logType);
        return -1.0; // Indicate failure
    }

    /**
     * Send a batch of feature arrays to the unsupervised API in a single call.
     * Used to avoid per-packet HTTP calls during learning mode; the payload is
     * formatted as {"data": [[...], [...]]}.
     *
     * @param logType The log type (conn, http, dns, ssl)
     * @param featureJsonList List of feature arrays serialized as JSON strings
     * @return true if the request succeeded, false otherwise
     */
    public static boolean sendUnsupervisedBatch(String logType, List<String> featureJsonList) {
        final String FUNCTION_NAME = "sendUnsupervisedBatch";

        if (featureJsonList == null || featureJsonList.isEmpty()) {
            LOG.debug("[{}] No data to batch-send for logType: {}", FUNCTION_NAME, logType);
            return true;
        }

        checkApiHealth();
        if (!unsupervisedApiHealthy) {
            LOG.debug("[{}] Skipping batch send (unsupervised API unhealthy)", FUNCTION_NAME);
            return false;
        }

        String endpoint = APIConfig.getUnsupervisedPredictEndpoint(logType);

        // Build a single JSON array of feature arrays
        String dataArray = featureJsonList.stream()
                .map(s -> s == null ? "[]" : s)
                .collect(Collectors.joining(",", "[", "]"));
        String requestJson = String.format("{\"data\": %s}", dataArray);

        LOG.info("[{}] Sending batch of {} records to {}", FUNCTION_NAME, featureJsonList.size(), endpoint);
        JsonNode response = sendRequest(endpoint, requestJson, APIConfig.getRetryCount());

        boolean success = response != null;
        if (success) {
            LOG.info("[{}] Batch send succeeded for logType: {} ({} records)", FUNCTION_NAME, logType, featureJsonList.size());
            resetCircuitBreaker();
        } else {
            LOG.warn("[{}] Batch send failed for logType: {} ({} records)", FUNCTION_NAME, logType, featureJsonList.size());
        }
        return success;
    }

    /**
     * Report current in-memory buffer size to the unsupervised API so external
     * tools (e.g., lstm-control.sh) can display it. Throttled to avoid chatter.
     */
    public static void updateBufferSize(String logType, int size) {
        final String FUNCTION_NAME = "updateBufferSize";

        long now = System.currentTimeMillis();
        Integer lastSize = lastReportedBufferSize.get(logType);
        Long lastTime = lastBufferReportTime.get(logType);

        boolean sizeChanged = lastSize == null || lastSize != size;
        boolean intervalElapsed = lastTime == null || (now - lastTime) >= BUFFER_REPORT_INTERVAL_MS;

        if (!sizeChanged && !intervalElapsed) {
            return; // skip redundant update
        }

        String endpoint = APIConfig.getUnsupervisedBufferUpdateEndpoint(logType);
        String payload = String.format("{\"buffer_size\": %d}", size);

        JsonNode response = sendRequest(endpoint, payload, APIConfig.getRetryCount());
        if (response != null) {
            lastReportedBufferSize.put(logType, size);
            lastBufferReportTime.put(logType, now);
            LOG.debug("[{}] Reported buffer size {} for {}", FUNCTION_NAME, size, logType);
        } else {
            LOG.warn("[{}] Failed to report buffer size {} for {}", FUNCTION_NAME, size, logType);
        }
    }
} 