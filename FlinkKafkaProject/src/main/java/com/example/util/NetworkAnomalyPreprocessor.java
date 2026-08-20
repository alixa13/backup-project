package com.example.util;

import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.factory.Nd4j;
import org.nd4j.linalg.ops.transforms.Transforms;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.ZoneOffset;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

/**
 * Network Anomaly Preprocessor using Java ML Libraries
 * 
 * Equivalent to Python's anomaly_preprocessor_01.py but using:
 * - ND4J (NumPy for Java) for array operations
 * - Apache Commons Math for statistics
 * - Standard Java collections for state management
 * 
 * Produces 33 standardized features for all log types (conn, dns, http, ssl)
 */
public class NetworkAnomalyPreprocessor {
    private static final Logger LOGGER = Logger.getLogger(NetworkAnomalyPreprocessor.class.getName());
    
    // Configuration
    private final int windowSize;
    private final int maxActiveSessions;
    private final Set<Integer> knownPorts;
    private final Map<Integer, Integer> portMap;
    private final int unknownPortIdx;
    
    // Scaler state (min/max values learned from training data)
    private INDArray scaleMin;
    private INDArray scaleMax;
    private boolean isFitted = false;
    
    // Runtime state (LRU cache for session tracking)
    private final Map<String, Deque<INDArray>> sessionBuffer;
    private final Map<String, InteractionState> interactionState;
    
    // Thread-safe state management
    private final Object scalerLock = new Object();
    
    /**
     * Interaction state for each source IP (CICFlowMeter-style tracking)
     */
    private static class InteractionState {
        int count = 0;
        Set<String> destIps = new HashSet<>();
        Set<Integer> ports = new HashSet<>();
        
        // For IAT (Inter-Arrival Time) calculation
        long lastFlowTimestamp = 0;
        List<Long> iatValues = new ArrayList<>();  // IAT between flows
        
        double[] getFeatures() {
            int conn = count;
            int srv = (ports != null) ? ports.size() : 0;
            double rate = srv / (double) Math.max(conn, 1);
            return new double[]{conn, srv, rate};
        }
        
        double[] getIATStats() {
            // Null check - iatValues might be null due to serialization or reset issues
            if (iatValues == null || iatValues.isEmpty()) {
                return new double[]{0.0, 0.0};  // mean, std
            }
            
            // Copy to avoid ConcurrentModificationException when shared across Flink parallel tasks
            List<Long> snapshot = new ArrayList<>(iatValues);
            if (snapshot.isEmpty()) {
                return new double[]{0.0, 0.0};  // mean, std
            }
            
            // Calculate mean
            double sum = 0;
            for (long iat : snapshot) {
                sum += iat;
            }
            double mean = sum / snapshot.size();
            
            // Calculate std deviation
            double variance = 0;
            for (long iat : snapshot) {
                variance += Math.pow(iat - mean, 2);
            }
            double std = Math.sqrt(variance / snapshot.size());
            
            return new double[]{mean / 1_000_000.0, std / 1_000_000.0};  // Convert to seconds
        }
    }
    
    /**
     * Constructor
     */
    public NetworkAnomalyPreprocessor(int windowSize, int maxActiveSessions, List<Integer> knownPorts) {
        this.windowSize = windowSize;
        this.maxActiveSessions = maxActiveSessions;
        
        // Port embedding setup
        this.knownPorts = knownPorts != null ? new HashSet<>(knownPorts) : 
                         new HashSet<>(Arrays.asList(21, 22, 53, 80, 443, 8080, 3306, 445));
        this.portMap = new HashMap<>();
        int idx = 1;
        for (Integer port : this.knownPorts) {
            portMap.put(port, idx++);
        }
        this.unknownPortIdx = idx;
        
        // Initialize state (using LinkedHashMap for LRU)
        this.sessionBuffer = Collections.synchronizedMap(new LinkedHashMap<String, Deque<INDArray>>(maxActiveSessions, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, Deque<INDArray>> eldest) {
                return size() > maxActiveSessions;
            }
        });
        
        this.interactionState = new ConcurrentHashMap<>();
        
        LOGGER.info("NetworkAnomalyPreprocessor initialized (ND4J-based) - Window: " + windowSize + ", Features: 33");
    }
    
    /**
     * Extract time features using sine/cosine encoding (cyclical)
     * Returns: [hour_sin, hour_cos, min_sin, min_cos]
     */
    private double[] extractTimeFeatures(double timestamp) {
        try {
            Instant instant = Instant.ofEpochSecond((long) timestamp);
            ZonedDateTime dt = ZonedDateTime.ofInstant(instant, ZoneOffset.UTC);
            
            int hour = dt.getHour();
            int minute = dt.getMinute();
            
            // Cyclical encoding using Math library
            return new double[]{
                Math.sin(2 * Math.PI * hour / 24.0),
                Math.cos(2 * Math.PI * hour / 24.0),
                Math.sin(2 * Math.PI * minute / 60.0),
                Math.cos(2 * Math.PI * minute / 60.0)
            };
        } catch (Exception e) {
            return new double[]{0, 0, 0, 0};
        }
    }
    
    /**
     * Extract IP features (4 octets normalized)
     * Returns: [octet1/255, octet2/255, octet3/255, octet4/255]
     */
    private double[] extractIpFeatures(String ipStr) {
        if (ipStr == null || ipStr.isEmpty()) {
            return new double[]{0, 0, 0, 0};
        }
        
        String[] parts = ipStr.split("\\.");
        if (parts.length != 4) {
            return new double[]{0, 0, 0, 0};
        }
        
        try {
            double[] octets = new double[4];
            for (int i = 0; i < 4; i++) {
                octets[i] = Integer.parseInt(parts[i]) / 255.0;
            }
            return octets;
        } catch (NumberFormatException e) {
            return new double[]{0, 0, 0, 0};
        }
    }
    
    /**
     * Map port to categorical index
     */
    private int mapPort(int port) {
        return portMap.getOrDefault(port, unknownPortIdx);
    }
    
    /**
     * Safe log transform using Apache Commons Math
     */
    private double safeLog(double value) {
        if (Double.isInfinite(value) || Double.isNaN(value)) {
            return 0.0;
        }
        return Math.log1p(Math.max(0, value));
    }
    
    /**
     * Update interaction state for a source IP (CICFlowMeter-style)
     */
    private double[] updateInteractionState(String srcIp, String dstIp, int dstPort, long flowTimestamp) {
        // Handle null or empty IPs - use a default key
        if (srcIp == null || srcIp.isEmpty()) {
            srcIp = "unknown";
        }
        if (dstIp == null || dstIp.isEmpty()) {
            dstIp = "unknown";
        }
        
        InteractionState state = interactionState.computeIfAbsent(srcIp, k -> new InteractionState());
        
        // Calculate IAT (Inter-Arrival Time between flows)
        if (state.lastFlowTimestamp > 0) {
            long iat = flowTimestamp - state.lastFlowTimestamp;
            if (iat > 0 && iat < 3600_000_000L) {  // Sanity check: < 1 hour
                state.iatValues.add(iat);
                // Keep only last 100 IAT values to prevent memory issues
                if (state.iatValues.size() > 100) {
                    state.iatValues.remove(0);
                }
            }
        }
        state.lastFlowTimestamp = flowTimestamp;
        
        state.count++;
        state.destIps.add(dstIp);
        state.ports.add(dstPort);
        
        // Memory cleanup (reset after 2000 connections)
        if (state.count > 2000) {
            state = new InteractionState();
            state.count = 1;
            state.destIps.add(dstIp);
            state.ports.add(dstPort);
            state.lastFlowTimestamp = flowTimestamp;
            interactionState.put(srcIp, state);
        }
        
        return state.getFeatures();
    }
    
    /**
     * Parse TCP flags from Zeek history field
     * History format: "ShAdDaFf" where S=SYN, A=ACK, D=Data, F=FIN, R=RST, etc.
     * Returns: [SYN_count, FIN_count, RST_count, PSH_count, ACK_count]
     */
    private double[] parseTCPFlags(String history) {
        if (history == null || history.isEmpty()) {
            return new double[]{0, 0, 0, 0, 0};
        }
        
        int synCount = 0;
        int finCount = 0;
        int rstCount = 0;
        int pshCount = 0;
        int ackCount = 0;
        
        for (char c : history.toCharArray()) {
            switch (c) {
                case 'S': synCount++; break;
                case 'F': finCount++; break;
                case 'R': rstCount++; break;
                case 'P': pshCount++; break;
                case 'A': ackCount++; break;
            }
        }
        
        return new double[]{synCount, finCount, rstCount, pshCount, ackCount};
    }
    
    /**
     * Build raw 33-feature vector from Zeek log (CICFlowMeter-style calculations)
     * Works for conn, dns, http, ssl. DNS logs lack duration/orig_bytes/etc; use rtt and zeros.
     * SSL logs also lack flow metrics - use available fields or defaults.
     */
    public double[] buildRawVector(JsonNode record) {
        if (record == null || record.isNull()) {
            return new double[33];
        }
        List<Double> features = new ArrayList<>(33);
        
        // Null-safe extraction (record.get() can return null for explicit JSON null or missing)
        double timestamp = safeAsDouble(record.get("ts"), 0.0);
        long timestampMicros = (long) (timestamp * 1_000_000);
        
        // Handle null/missing IP fields - use empty string as default
        JsonNode origHost = record.get("id.orig_h");
        JsonNode respHost = record.get("id.resp_h");
        String srcIp = (origHost != null && !origHost.isNull()) ? origHost.asText("") : "";
        String dstIp = (respHost != null && !respHost.isNull()) ? respHost.asText("") : "";
        
        // Fallback to empty if asText returns null
        if (srcIp == null) srcIp = "";
        if (dstIp == null) dstIp = "";
        
        int srcPort = safeAsInt(record.get("id.orig_p"), 0);
        int dstPort = safeAsInt(record.get("id.resp_p"), 0);
        
        // Flow metrics: conn/http/ssl have duration, orig_bytes, etc.; DNS has rtt only
        // SSL logs may have duration if they track handshake time, but often missing
        double duration = getDoubleField(record, "duration");
        if (duration <= 0.0 && record.has("rtt")) {
            double rtt = getDoubleField(record, "rtt");
            if (rtt > 0) {
                duration = rtt;  // DNS/SSL: use rtt (seconds) as duration
            }
        }
        // SSL logs often don't have duration - use a small default to avoid division by zero
        if (duration <= 0.0) {
            duration = 0.001;  // Default small duration for SSL logs without flow metrics
        }
        
        double origBytes = getDoubleField(record, "orig_bytes");
        double respBytes = getDoubleField(record, "resp_bytes");
        double origPkts = getDoubleField(record, "orig_pkts");
        double respPkts = getDoubleField(record, "resp_pkts");
        double totalBytes = origBytes + respBytes;
        double totalPkts = origPkts + respPkts;
        
        // 1-4: Temporal Features (cyclical encoding)
        double[] timeFeats = extractTimeFeatures(timestamp);
        for (double f : timeFeats) features.add(f);
        
        // 5-8: Source IP (4 octets)
        double[] srcIpFeats = extractIpFeatures(srcIp);
        for (double f : srcIpFeats) features.add(f);
        
        // 9-12: Dest IP (4 octets)
        double[] dstIpFeats = extractIpFeatures(dstIp);
        for (double f : dstIpFeats) features.add(f);
        
        // 13-14: Port Embeddings (categorical indices)
        features.add((double) mapPort(srcPort));
        features.add((double) mapPort(dstPort));
        
        // 15-17: Interaction Features (stateful)
        double[] interactionFeats = updateInteractionState(srcIp, dstIp, dstPort, timestampMicros);
        for (double f : interactionFeats) features.add(f);
        
        // 18-28: Flow Dynamics & Volume (11 features, log-scaled) - CICFlowMeter style
        
        // Get IAT stats for this source IP
        InteractionState state = interactionState.get(srcIp);
        double[] iatStats = state != null ? state.getIATStats() : new double[]{0.0, 0.0};
        
        // Feature 18: Flow Duration (log-scaled)
        features.add(safeLog(duration));
        
        // Feature 19: Flow IAT Mean (calculated from flow-level timing)
        features.add(safeLog(iatStats[0]));
        
        // Feature 20: Flow IAT Std (calculated from flow-level timing)
        features.add(safeLog(iatStats[1]));
        
        // Feature 21: Total Fwd Packets (log-scaled)
        features.add(safeLog(origPkts));
        
        // Feature 22: Total Bwd Packets (log-scaled)
        features.add(safeLog(respPkts));
        
        // Feature 23: Total Length of Fwd Packets (log-scaled)
        features.add(safeLog(origBytes));
        
        // Feature 24: Total Length of Bwd Packets (log-scaled)
        features.add(safeLog(respBytes));
        
        // Feature 25: Flow Bytes/s (calculated)
        features.add(safeLog(totalBytes / duration));
        
        // Feature 26: Flow Packets/s (calculated)
        features.add(safeLog(totalPkts / duration));
        
        // Feature 27: Packet Length Mean (calculated)
        double pktLengthMean = totalPkts > 0 ? totalBytes / totalPkts : 0.0;
        features.add(safeLog(pktLengthMean));
        
        // Feature 28: Packet Length Std (estimated - Zeek doesn't provide per-packet data)
        // Use coefficient of variation as proxy: if fwd/bwd are similar, std is low
        double fwdAvg = origPkts > 0 ? origBytes / origPkts : 0.0;
        double bwdAvg = respPkts > 0 ? respBytes / respPkts : 0.0;
        double pktLengthStd = Math.abs(fwdAvg - bwdAvg) / 2.0;  // Rough estimate
        features.add(safeLog(pktLengthStd));
        
        // 29-33: TCP Flags (5 features) - Parse from Zeek history field (conn/ssl; DNS has no history)
        // SSL logs use "ssl_history" field, conn logs use "history" field
        String history = safeAsText(record.get("history"), "");
        if ((history == null || history.isEmpty()) && record.has("ssl_history")) {
            history = safeAsText(record.get("ssl_history"), "");
        }
        double[] flagCounts = parseTCPFlags(history);
        for (double f : flagCounts) features.add(f);
        
        // Validate feature count
        if (features.size() != 33) {
            LOGGER.warning("Feature count mismatch! Expected 33, got " + features.size());
            // Pad or truncate to 33
            while (features.size() < 33) features.add(0.0);
            if (features.size() > 33) features = features.subList(0, 33);
        }
        
        // Convert to primitive array
        double[] result = new double[33];
        for (int i = 0; i < 33; i++) {
            result[i] = features.get(i);
        }
        
        return result;
    }
    
    /**
     * Helper to safely get double field from JSON (handles null node from record.get())
     */
    private double getDoubleField(JsonNode record, String field) {
        if (record == null) return 0.0;
        JsonNode node = record.get(field);
        if (node == null || node.isNull()) return 0.0;
        try {
            return safeAsDouble(node, 0.0);
        } catch (Exception e) {
            return 0.0;
        }
    }

    private static double safeAsDouble(JsonNode node, double def) {
        if (node == null || node.isNull()) return def;
        try {
            return node.asDouble(def);
        } catch (Exception e) {
            return def;
        }
    }

    private static String safeAsText(JsonNode node, String def) {
        if (node == null || node.isNull()) return def;
        try {
            return node.asText(def);
        } catch (Exception e) {
            return def;
        }
    }

    private static int safeAsInt(JsonNode node, int def) {
        if (node == null || node.isNull()) return def;
        try {
            return node.asInt(def);
        } catch (Exception e) {
            return def;
        }
    }
    
    /**
     * Fit the scaler on training data using ND4J
     * This learns the min/max values for normalization
     */
    public void fit(List<double[]> trainingData) {
        if (trainingData == null || trainingData.isEmpty()) {
            throw new IllegalArgumentException("Training data cannot be empty");
        }
        
        synchronized (scalerLock) {
            // Convert training data to ND4J matrix
            int numSamples = trainingData.size();
            INDArray dataMatrix = Nd4j.create(numSamples, 33);
            
            for (int i = 0; i < numSamples; i++) {
                dataMatrix.putRow(i, Nd4j.create(trainingData.get(i)));
            }
            
            // Calculate min and max for each feature using ND4J operations
            scaleMin = dataMatrix.min(0); // Min along axis 0 (columns)
            scaleMax = dataMatrix.max(0); // Max along axis 0 (columns)
            
            isFitted = true;
            LOGGER.info("Scaler fitted on " + numSamples + " training samples using ND4J");
        }
    }
    
    /**
     * Transform (normalize) features using fitted scaler - ND4J vectorized operations
     */
    public double[] transform(double[] rawFeatures) {
        if (!isFitted) {
            throw new IllegalStateException("Scaler not fitted! Call fit() first or load scaler.");
        }
        
        synchronized (scalerLock) {
            // Convert to ND4J array for vectorized operations
            INDArray features = Nd4j.create(rawFeatures);
            
            // MinMax normalization: (x - min) / (max - min)
            // Using ND4J vectorized operations (like NumPy)
            INDArray range = scaleMax.sub(scaleMin);
            range = Transforms.max(range, 1e-8); // Prevent division by zero
            
            INDArray normalized = features.sub(scaleMin).div(range);
            
            // Clip to [0, 1] range
            normalized = Transforms.max(normalized, 0.0);
            normalized = Transforms.min(normalized, 1.0);
            
            return normalized.toDoubleVector();
        }
    }
    
    /**
     * Process live record and return sequence for LSTM
     * Returns: (1, window_size, 33) shaped array or null if not ready
     */
    public INDArray processLiveRecord(JsonNode record) {
        if (!isFitted) {
            LOGGER.warning("Scaler not fitted! Cannot process live record.");
            return null;
        }
        
        try {
            // 1. Extract raw features
            double[] rawVec = buildRawVector(record);
            
            // 2. Normalize
            double[] scaledVec = transform(rawVec);
            INDArray scaledArray = Nd4j.create(scaledVec);
            
            // 3. Update session buffer (LRU)
            String srcIp = record.has("id.orig_h") ? record.get("id.orig_h").asText("unknown") : "unknown";
            
            Deque<INDArray> history = sessionBuffer.computeIfAbsent(srcIp, k -> new LinkedList<>());
            history.addLast(scaledArray);
            
            // Keep only last 'windowSize' entries
            while (history.size() > windowSize) {
                history.removeFirst();
            }
            
            // 4. Build sequence (cold start padding if needed)
            int seqLen = history.size();
            INDArray sequence = Nd4j.create(1, windowSize, 33);
            
            if (seqLen == windowSize) {
                // Full window available
                int idx = 0;
                for (INDArray vec : history) {
                    sequence.put(new int[]{0, idx, 0}, vec);
                    idx++;
                }
            } else {
                // Pad with zeros at the beginning
                int padNeeded = windowSize - seqLen;
                int idx = padNeeded;
                for (INDArray vec : history) {
                    sequence.put(new int[]{0, idx, 0}, vec);
                    idx++;
                }
            }
            
            return sequence;
            
        } catch (Exception e) {
            LOGGER.severe("Error processing record: " + e.getMessage());
            return null;
        }
    }
    
    /**
     * Get current state info
     */
    public Map<String, Object> getStateInfo() {
        Map<String, Object> info = new HashMap<>();
        info.put("window_size", windowSize);
        info.put("max_sessions", maxActiveSessions);
        info.put("active_sessions", sessionBuffer.size());
        info.put("tracked_ips", interactionState.size());
        info.put("is_fitted", isFitted);
        return info;
    }
    
    /**
     * Clear runtime state (reset buffers)
     */
    public void clearState() {
        sessionBuffer.clear();
        interactionState.clear();
        LOGGER.info("Runtime state cleared");
    }
}
