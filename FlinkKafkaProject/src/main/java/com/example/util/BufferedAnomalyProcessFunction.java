package com.example.util;

import com.example.APIClient;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Buffers Zeek records per subtask and scores them against the unsupervised API in
 * batches, emitting the records whose anomaly score crosses the current threshold.
 *
 * <p>Replaces the copy-pasted buffering logic that lived in each of the four
 * Unsupervised*Improved job classes. That version had three problems this class fixes:
 *
 * <ul>
 *   <li><b>Static buffers.</b> The buffers were {@code private static final}, so all
 *       parallel subtasks in a TaskManager JVM shared one list behind one monitor.
 *       Parallelism bought nothing, and a flush triggered by subtask B emitted records
 *       belonging to subtask A through A's {@code Collector}. Buffers are now instance
 *       fields: one per subtask, mutated only by that subtask's task thread, so there
 *       is no lock at all.</li>
 *   <li><b>HTTP under a lock.</b> The API call happened inside
 *       {@code synchronized (PREDICTION_BUFFER)}, blocking every other subtask for the
 *       duration of the request. There is no shared lock left to hold.</li>
 *   <li><b>Records that were never scored.</b> The old code only checked the size and
 *       age limits when a new record arrived, so whatever sat in the buffer when traffic
 *       went quiet stayed there indefinitely. A processing-time timer now flushes it.</li>
 * </ul>
 *
 * <p>Timers require a keyed stream, so jobs must {@code keyBy} before {@code process}.
 * Keying on the source IP is also what makes {@link NetworkAnomalyPreprocessor} safe:
 * its per-IP interaction state is mutated in place and is not thread safe, and it was
 * previously a static instance shared across every subtask thread.
 */
public class BufferedAnomalyProcessFunction extends KeyedProcessFunction<String, JsonNode, String> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = Logger.getLogger(BufferedAnomalyProcessFunction.class.getName());

    /** Hard cap on the learning buffer so failed flushes cannot grow it without bound. */
    private static final int LEARNING_BUFFER_HARD_CAP_MULTIPLIER = 3;

    // ---- configuration, serialized with the function ----
    private final String logType;
    private final String nestedKey;
    private final int featureCount;
    private final int predictionBatchSize;
    private final long predictionBatchTimeoutMs;
    private final int learningBatchSize;
    private final long learningBatchTimeoutMs;
    private final double defaultThreshold;
    private final long thresholdRefreshMs;
    private final int windowSize;
    private final int maxActiveSessions;
    private final List<Integer> knownPorts;

    // ---- per-subtask runtime state; single threaded, so no synchronization ----
    private transient ObjectMapper mapper;
    private transient NetworkAnomalyPreprocessor preprocessor;
    private transient List<String> predictionFeatures;
    private transient List<String> predictionLogs;
    private transient List<String> learningFeatures;
    private transient long lastLearningFlushMs;
    private transient long pendingTimerMs;
    private transient double currentThreshold;
    private transient long thresholdFetchedAtMs;

    public BufferedAnomalyProcessFunction(
            String logType,
            String nestedKey,
            int featureCount,
            int predictionBatchSize,
            long predictionBatchTimeoutMs,
            int learningBatchSize,
            long learningBatchTimeoutMs,
            double defaultThreshold,
            long thresholdRefreshMs,
            int windowSize,
            int maxActiveSessions,
            List<Integer> knownPorts) {
        this.logType = logType;
        this.nestedKey = nestedKey;
        this.featureCount = featureCount;
        this.predictionBatchSize = predictionBatchSize;
        this.predictionBatchTimeoutMs = predictionBatchTimeoutMs;
        this.learningBatchSize = learningBatchSize;
        this.learningBatchTimeoutMs = learningBatchTimeoutMs;
        this.defaultThreshold = defaultThreshold;
        this.thresholdRefreshMs = thresholdRefreshMs;
        this.windowSize = windowSize;
        this.maxActiveSessions = maxActiveSessions;
        this.knownPorts = knownPorts;
    }

    @Override
    public void open(Configuration parameters) {
        mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        preprocessor = new NetworkAnomalyPreprocessor(windowSize, maxActiveSessions, knownPorts);
        predictionFeatures = new ArrayList<>(predictionBatchSize);
        predictionLogs = new ArrayList<>(predictionBatchSize);
        learningFeatures = new ArrayList<>();
        lastLearningFlushMs = System.currentTimeMillis();
        pendingTimerMs = -1L;
        currentThreshold = defaultThreshold;
        thresholdFetchedAtMs = 0L;
    }

    @Override
    public void processElement(JsonNode logEntry, Context ctx, Collector<String> out) {
        if (logEntry == null || logEntry.isNull()) {
            return;
        }
        try {
            JsonNode data = logEntry.has(nestedKey) ? logEntry.get(nestedKey) : logEntry;
            if (data == null || data.isNull()) {
                return;
            }

            JsonNode orig = data.get("id.orig_h");
            String srcIp = (orig != null && !orig.isNull()) ? orig.asText("") : "";
            if (TrustedIps.isTrusted(srcIp)) {
                return;
            }

            double[] features = preprocessor.buildRawVector(data);
            if (features.length != featureCount) {
                LOG.warning(String.format("Feature count mismatch for %s: expected %d, got %d",
                        logType, featureCount, features.length));
                return;
            }
            String featureJson = mapper.writeValueAsString(features);

            if (APIClient.getUnsupervisedLearningStatusCached(logType)) {
                learningFeatures.add(featureJson);
                APIClient.updateBufferSize(logType, learningFeatures.size());
                if (learningFeatures.size() >= learningBatchSize) {
                    flushLearning();
                }
                scheduleFlush(ctx);
                return;
            }

            // Just left learning mode: hand over whatever was collected for training.
            if (!learningFeatures.isEmpty()) {
                flushLearning();
            }

            refreshThresholdIfDue();

            predictionFeatures.add(featureJson);
            predictionLogs.add(logEntry.toString());
            if (predictionFeatures.size() >= predictionBatchSize) {
                flushPredictions(out);
            } else {
                scheduleFlush(ctx);
            }
        } catch (Exception e) {
            String msg = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
            LOG.log(Level.WARNING, "Error processing " + logType + " log entry: " + msg, e);
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) {
        pendingTimerMs = -1L;
        flushPredictions(out);
        if (!learningFeatures.isEmpty()
                && System.currentTimeMillis() - lastLearningFlushMs >= learningBatchTimeoutMs) {
            flushLearning();
        }
    }

    @Override
    public void close() {
        // Flink gives close() no Collector, so buffered predictions cannot be emitted on
        // shutdown; the timer bounds that loss to predictionBatchTimeoutMs of records.
        // The learning buffer is a plain HTTP send and can still be drained.
        if (learningFeatures != null && !learningFeatures.isEmpty()) {
            flushLearning();
        }
    }

    /**
     * Ensure something will wake this subtask up so a partly filled buffer is not left
     * unscored when traffic stops. Only one timer is kept outstanding at a time; it is
     * registered against whichever key is current, which is fine because the flush
     * drains the whole subtask buffer rather than one key's records.
     */
    private void scheduleFlush(Context ctx) {
        if (pendingTimerMs >= 0) {
            return;
        }
        long fireAt = ctx.timerService().currentProcessingTime() + predictionBatchTimeoutMs;
        ctx.timerService().registerProcessingTimeTimer(fireAt);
        pendingTimerMs = fireAt;
    }

    private void flushPredictions(Collector<String> out) {
        if (predictionFeatures.isEmpty()) {
            return;
        }
        List<String> batch = new ArrayList<>(predictionFeatures);
        List<String> logs = new ArrayList<>(predictionLogs);
        predictionFeatures.clear();
        predictionLogs.clear();

        List<Double> scores = APIClient.getBatchAnomalyScores(logType, batch);
        if (scores == null || scores.size() != batch.size()) {
            LOG.warning("Batch prediction failed for " + logType + " (expected " + batch.size()
                    + " scores, got " + (scores == null ? "null" : scores.size())
                    + "); " + batch.size() + " records dropped");
            return;
        }
        int emitted = 0;
        for (int i = 0; i < scores.size(); i++) {
            if (scores.get(i) >= currentThreshold) {
                out.collect(logs.get(i));
                emitted++;
            }
        }
        LOG.fine("Scored " + batch.size() + " " + logType + " records, emitted " + emitted);
    }

    private void flushLearning() {
        if (learningFeatures.isEmpty()) {
            return;
        }
        List<String> batch = new ArrayList<>(learningFeatures);
        learningFeatures.clear();
        lastLearningFlushMs = System.currentTimeMillis();

        if (APIClient.sendUnsupervisedBatch(logType, batch)) {
            LOG.info("Flushed " + batch.size() + " " + logType + " records to the learning API");
            APIClient.updateBufferSize(logType, 0);
            return;
        }

        // Training data is worth keeping, so re-queue - but bounded. The previous version
        // re-queued without a cap, so a persistently unreachable API grew the buffer until
        // the TaskManager ran out of heap.
        int cap = learningBatchSize * LEARNING_BUFFER_HARD_CAP_MULTIPLIER;
        learningFeatures.addAll(0, batch);
        if (learningFeatures.size() > cap) {
            int drop = learningFeatures.size() - cap;
            learningFeatures.subList(0, drop).clear();
            LOG.warning("Learning buffer for " + logType + " exceeded " + cap
                    + " records; dropped the oldest " + drop);
        }
        LOG.warning("Failed to flush learning buffer for " + logType + "; holding "
                + learningFeatures.size() + " records");
        APIClient.updateBufferSize(logType, learningFeatures.size());
    }

    /**
     * Refresh the anomaly threshold periodically. The previous code fetched it once and
     * set a {@code thresholdInitialized} flag, so a threshold produced by a later
     * retraining was never picked up until the job was resubmitted.
     */
    private void refreshThresholdIfDue() {
        long now = System.currentTimeMillis();
        if (thresholdFetchedAtMs != 0L && now - thresholdFetchedAtMs < thresholdRefreshMs) {
            return;
        }
        thresholdFetchedAtMs = now;
        Double fetched = APIClient.getUnsupervisedThreshold(logType);
        if (fetched == null) {
            LOG.fine("No dynamic threshold for " + logType + ", keeping " + currentThreshold);
            return;
        }
        if (fetched != currentThreshold) {
            LOG.info("Threshold for " + logType + " updated: " + currentThreshold + " -> " + fetched);
            currentThreshold = fetched;
        }
    }

    // ---- test seams ----------------------------------------------------------

    /** Number of records waiting to be scored in this subtask. */
    int pendingPredictionCount() {
        return predictionFeatures == null ? 0 : predictionFeatures.size();
    }

    /** Number of records waiting to be sent to the learning API in this subtask. */
    int pendingLearningCount() {
        return learningFeatures == null ? 0 : learningFeatures.size();
    }

    /**
     * Extracts the source IP so records for one host always reach the same subtask.
     * Required for timers, and required for {@link NetworkAnomalyPreprocessor}'s per-IP
     * state to be touched by one thread at a time.
     */
    public static final class SourceIpKeySelector implements KeySelector<JsonNode, String> {
        private static final long serialVersionUID = 1L;
        private final String nestedKey;

        public SourceIpKeySelector(String nestedKey) {
            this.nestedKey = nestedKey;
        }

        @Override
        public String getKey(JsonNode logEntry) {
            if (logEntry == null || logEntry.isNull()) {
                return "unknown";
            }
            JsonNode data = logEntry.has(nestedKey) ? logEntry.get(nestedKey) : logEntry;
            if (data == null || data.isNull()) {
                return "unknown";
            }
            JsonNode orig = data.get("id.orig_h");
            String ip = (orig != null && !orig.isNull()) ? orig.asText("") : "";
            return ip.isEmpty() ? "unknown" : ip;
        }
    }
}
