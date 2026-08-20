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
import java.util.concurrent.atomic.AtomicLong;
import java.util.List;
import java.util.ArrayList;
import java.util.stream.Collectors;

/**
 * API Client for interacting with the supervised and unsupervised ML APIs.
 * Includes circuit breaker and health check logic.
 */
public class APIClient {
    private static final Logger LOG = LoggerFactory.getLogger(APIClient.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    
    // Circuit breaker state, one breaker per target host:port. Previously a single
    // global breaker was shared by both APIs, so failures talking to the supervised
    // API also stopped all unsupervised scoring.
    private static final class Breaker {
        final String name;
        int consecutiveFailures = 0;
        boolean open = false;
        long openTime = 0;
        Breaker(String name) { this.name = name; }
    }
    private static final Map<String, Breaker> BREAKERS = new ConcurrentHashMap<>();
    private static volatile boolean supervisedApiHealthy = true;
    private static volatile boolean unsupervisedApiHealthy = true;
    private static final int FAILURE_THRESHOLD = 5;
    private static final long CIRCUIT_RESET_TIMEOUT_MS = 60000; // 1 minute
    private static final AtomicLong lastHealthCheckTime = new AtomicLong(0);
    private static final long HEALTH_CHECK_INTERVAL_MS = 60000; // 1 minute
    
    // Learning status cache (per log_type), to avoid calling the API on every packet
    private static final long LEARNING_STATUS_CACHE_MS = 30_000L; // 30 seconds (reduced API calls)
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
        final Breaker breaker = breakerFor(endpoint);
        
        // Check if circuit is open
        if (isCircuitOpen(breaker)) {
            LOG.warn("[{}] Circuit is open for {}, skipping request to: {}",
                    FUNCTION_NAME, breaker.name, endpoint);
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
                        resetCircuitBreaker(breaker);
                        return response;
                    }
                } else {
                    // Handle error responses
                    LOG.warn("[{}] ERROR (Attempt {}) - HTTP {}: {}", FUNCTION_NAME, attempts, responseCode, responseMessage);
                    
                    // Always drain the error stream, otherwise the socket cannot be
                    // reused by HTTP keep-alive.
                    try (InputStream es = conn.getErrorStream()) {
                        if (es != null) {
                            String errorBody = new String(es.readAllBytes(), StandardCharsets.UTF_8);
                            LOG.debug("[{}] Error Response Body: {}", FUNCTION_NAME, errorBody);
                        }
                    }
                    
                    // A 4xx means we sent something the API cannot accept (e.g. the wrong
                    // feature count). Retrying cannot help, and counting it as a failure
                    // let a handful of malformed records open the circuit and blind the
                    // detector. 408 and 429 are transient and stay retryable.
                    if (responseCode >= 400 && responseCode < 500
                            && responseCode != 408 && responseCode != 429) {
                        LOG.warn("[{}] Non-retryable HTTP {} from {} - giving up without tripping the circuit breaker",
                                FUNCTION_NAME, responseCode, endpoint);
                        return null;
                    }
                    
                    recordFailure(breaker);
                }
            } catch (Exception e) {
                LOG.warn("[{}] EXCEPTION (Attempt {}) - Error calling API: {} - Exception: {}", 
                        FUNCTION_NAME, attempts, endpoint, e.getMessage());
                LOG.debug("[{}] Full exception details:", FUNCTION_NAME, e);
                recordFailure(breaker);
            }
            
            if (attempts < retries) {
                try {
                    // Exponential backoff, capped. This sleep blocks a Flink task thread,
                    // so the total stall has to stay well under the checkpoint timeout.
                    // The old 2^attempts * 1000 with api.retry.count=10 slept for up to
                    // 1022s on a single record.
                    int shift = Math.min(attempts - 1, 20);
                    long backoffTime = Math.min(
                            (long) APIConfig.getRetryDelayMs() << shift,
                            APIConfig.getRetryBackoffMaxMs());
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
    private static Breaker breakerFor(String endpoint) {
        String key;
        try {
            key = new URL(endpoint).getAuthority();
        } catch (Exception e) {
            key = null;
        }
        return BREAKERS.computeIfAbsent(key == null ? "unknown" : key, Breaker::new);
    }

    private static boolean isCircuitOpen(Breaker breaker) {
        synchronized (breaker) {
            if (!breaker.open) {
                return false;
            }
            if (System.currentTimeMillis() - breaker.openTime > CIRCUIT_RESET_TIMEOUT_MS) {
                // Half-open. Clear the counter as well: leaving it at the threshold meant
                // the first failure after the timeout re-opened the circuit immediately.
                LOG.info("Circuit timeout elapsed for {}, entering half-open state", breaker.name);
                breaker.open = false;
                breaker.consecutiveFailures = 0;
                return false;
            }
            return true;
        }
    }
    
    /**
     * Record a failure. If the failure threshold is reached, open the circuit.
     */
    private static void recordFailure(Breaker breaker) {
        synchronized (breaker) {
            breaker.consecutiveFailures++;
            if (breaker.consecutiveFailures >= FAILURE_THRESHOLD && !breaker.open) {
                breaker.open = true;
                breaker.openTime = System.currentTimeMillis();
                LOG.warn("Circuit breaker opened for {} after {} consecutive failures",
                        breaker.name, breaker.consecutiveFailures);
            }
        }
    }
    
    /**
     * Reset the circuit breaker after a successful call.
     */
    private static void resetCircuitBreaker(Breaker breaker) {
        synchronized (breaker) {
            if (breaker.consecutiveFailures > 0 || breaker.open) {
                LOG.info("Resetting circuit breaker for {} after successful API call", breaker.name);
                breaker.consecutiveFailures = 0;
                breaker.open = false;
            }
        }
    }
    
    /**
     * Check the health of supervised and unsupervised APIs.
     */
    private static void checkApiHealth() {
        // This runs on every record from every task thread. It used to be a
        // synchronized static method, so all subtasks serialised on one monitor just
        // to discover the interval had not elapsed. Read first, and let a single
        // thread win the CAS to actually perform the round.
        long currentTime = System.currentTimeMillis();
        long last = lastHealthCheckTime.get();
        if (currentTime - last < HEALTH_CHECK_INTERVAL_MS) {
            return; // Don't check too frequently
        }
        if (!lastHealthCheckTime.compareAndSet(last, currentTime)) {
            return; // another thread is already running this round
        }
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
        LOG.debug("[{}] Starting API call - Function: {}, LogType: {}, Endpoint: {}", 
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

            LOG.debug("[{}] REQUEST DETAILS:", FUNCTION_NAME);
            LOG.debug("[{}]   LogType: {}", FUNCTION_NAME, logType);
            LOG.debug("[{}]   Endpoint: {}", FUNCTION_NAME, endpoint);
            LOG.debug("[{}]   Request JSON: {}", FUNCTION_NAME, requestJson);
            
            // Send the formatted request
            JsonNode response = sendRequest(endpoint, requestJson, APIConfig.getRetryCount());
            if (response != null && response.has("prediction")) {
                String prediction = response.get("prediction").asText();
                LOG.debug("[{}] SUCCESS - Received prediction: {} for logType: {}", 
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

        LOG.debug("[{}] Returning 'unknown' (default) for logType: {}", FUNCTION_NAME, logType);
        return "unknown";
    }

    /**
     * Get prediction from supervised ML API for UNSW42 (unified session) model.
     * Uses /predict_unsw42 endpoint with 42 UNSW features.
     *
     * @param jsonFeatures JSON array of 42 UNSW features
     * @return The prediction result, or "unknown" if the request fails.
     */
    public static String getSessionPrediction(String jsonFeatures) {
        return getPrediction("unsw42", jsonFeatures);
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
        LOG.debug("[{}] Starting API call - Function: {}, LogType: {}, Endpoint: {}", 
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
                    
                    LOG.debug("[{}] SUCCESS - Learning enabled: {} for logType: {}", 
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
        
        LOG.debug("[{}] Returning false (default) for logType: {}", FUNCTION_NAME, logType);
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
        LOG.debug("[{}] Starting API call - Function: {}, LogType: {}, Endpoint: {}", 
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

            if (response != null && response.has("anomaly_scores") && response.get("anomaly_scores").has("mae")) {
                double score = response.get("anomaly_scores").get("mae").asDouble();
                LOG.debug("[{}] SUCCESS - Received anomaly score: {} for logType: {}", 
                        FUNCTION_NAME, score, logType);
                return score;
            } else if (response != null && response.has("learning_enabled") && response.get("learning_enabled").asBoolean()) {
                // If in learning mode, API returns a success message instead of a score.
                // Return a specific value to indicate data was collected for learning.
                LOG.debug("[{}] LEARNING MODE - Data collected for learning, logType: {}", FUNCTION_NAME, logType);
                return -2.0;
            } else {
                LOG.warn("[{}] ERROR - Unexpected response format: {}", FUNCTION_NAME, response);
            }
        } catch (Exception e) {
            LOG.warn("[{}] EXCEPTION - Error formatting data for LSTM API: {} - Exception: {}", 
                    FUNCTION_NAME, e.getMessage(), e.getMessage());
            LOG.debug("[{}] Full exception details:", FUNCTION_NAME, e);
        }
        
        LOG.debug("[{}] Returning -1.0 (failure) for logType: {}", FUNCTION_NAME, logType);
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

        LOG.debug("[{}] Sending batch of {} records to {}", FUNCTION_NAME, featureJsonList.size(), endpoint);
        JsonNode response = sendRequest(endpoint, requestJson, APIConfig.getRetryCount());

        boolean success = response != null;
        if (success) {
            LOG.debug("[{}] Batch send succeeded for logType: {} ({} records)", FUNCTION_NAME, logType, featureJsonList.size());
        } else {
            LOG.warn("[{}] Batch send failed for logType: {} ({} records)", FUNCTION_NAME, logType, featureJsonList.size());
        }
        return success;
    }

    /**
     * BATCH PREDICTION: Send multiple feature arrays and get anomaly scores in one API call.
     * 10-100x more efficient than individual calls during detection mode.
     *
     * @param logType The log type (conn, http, dns, ssl)
     * @param featureJsonList List of feature arrays serialized as JSON strings
     * @return List of anomaly scores (same order as input), or null if failed
     */
    public static List<Double> getBatchAnomalyScores(String logType, List<String> featureJsonList) {
        final String FUNCTION_NAME = "getBatchAnomalyScores";

        if (featureJsonList == null || featureJsonList.isEmpty()) {
            return new ArrayList<>();
        }

        checkApiHealth();
        if (!unsupervisedApiHealthy) {
            LOG.debug("[{}] Skipping batch prediction (API unhealthy)", FUNCTION_NAME);
            return null;
        }

        String endpoint = APIConfig.getUnsupervisedPredictEndpoint(logType);

        // Build JSON array
        String dataArray = featureJsonList.stream()
                .map(s -> s == null ? "[]" : s)
                .collect(Collectors.joining(",", "[", "]"));
        String requestJson = String.format("{\"data\": %s}", dataArray);

        LOG.debug("[{}] Batch predict: {} records to {}", FUNCTION_NAME, featureJsonList.size(), endpoint);
        JsonNode response = sendRequest(endpoint, requestJson, APIConfig.getRetryCount());

        if (response == null) {
            LOG.warn("[{}] Batch prediction failed for {}", FUNCTION_NAME, logType);
            return null;
        }

        try {
            // Handle batch response: {"status": "success", "results": [{"mae": 0.5, "is_anomaly": false}, ...]}
            if (response.has("results") && response.get("results").isArray()) {
                List<Double> scores = new ArrayList<>();
                for (JsonNode result : response.get("results")) {
                    double mae = result.get("mae").asDouble(0.0);
                    scores.add(mae);
                }
                LOG.debug("[{}] Batch prediction success: {} scores for {}", FUNCTION_NAME, scores.size(), logType);
                return scores;
            }
            // Handle single response (backward compatible)
            else if (response.has("anomaly_scores") && response.get("anomaly_scores").has("mae")) {
                double score = response.get("anomaly_scores").get("mae").asDouble();
                return List.of(score);
            } else {
                LOG.warn("[{}] Unexpected response format: {}", FUNCTION_NAME, response);
                return null;
            }
        } catch (Exception e) {
            LOG.warn("[{}] Error parsing batch response: {}", FUNCTION_NAME, e.getMessage());
            return null;
        }
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

    /**
     * Get the dynamic anomaly threshold for a specific log type from the unsupervised API.
     * This threshold is calculated as the 95th percentile of reconstruction errors during training.
     *
     * @param logType The log type (conn, http, dns, ssl)
     * @return The anomaly threshold, or null if not available
     */
    public static Double getUnsupervisedThreshold(String logType) {
        final String FUNCTION_NAME = "getUnsupervisedThreshold";
        
        // Validate logType parameter
        if (logType == null || logType.trim().isEmpty()) {
            LOG.error("[{}] Invalid logType parameter: {}", FUNCTION_NAME, logType);
            return null;
        }
        
        String endpoint = APIConfig.getUnsupervisedThresholdEndpoint(logType);
        LOG.debug("[{}] Starting API call - Function: {}, LogType: {}, Endpoint: {}", 
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
                    
                    // Check if threshold is available
                    if (response.has("has_threshold") && response.get("has_threshold").asBoolean()) {
                        double threshold = response.get("threshold").asDouble();
                        LOG.debug("[{}] SUCCESS - Retrieved threshold: {} for logType: {}", 
                                FUNCTION_NAME, threshold, logType);
                        return threshold;
                    } else {
                        LOG.warn("[{}] No threshold available for logType: {} (model may not be trained yet)", 
                                FUNCTION_NAME, logType);
                        return null;
                    }
                }
            } else {
                // Log error response body
                try (InputStream es = conn.getErrorStream()) {
                    if (es != null) {
                        String errorBody = new String(es.readAllBytes(), StandardCharsets.UTF_8);
                        LOG.warn("[{}] Error Response Body: {}", FUNCTION_NAME, errorBody);
                    }
                }
                
                LOG.warn("[{}] ERROR - Threshold endpoint returned error: {} (Response code: {})", 
                        FUNCTION_NAME, endpoint, responseCode);
            }
        } catch (Exception e) {
            LOG.warn("[{}] EXCEPTION - Error calling threshold endpoint: {} - Exception: {}", 
                    FUNCTION_NAME, endpoint, e.getMessage());
            LOG.debug("[{}] Full exception details:", FUNCTION_NAME, e);
        }
        
        LOG.debug("[{}] Returning null (threshold not available) for logType: {}", FUNCTION_NAME, logType);
        return null;
    }
    
} 