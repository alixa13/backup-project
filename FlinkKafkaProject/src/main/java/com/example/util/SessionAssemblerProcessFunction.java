package com.example.util;

import com.example.APIClient;
import com.example.model.ZeekSession;
import com.example.util.TrustedIps;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * KeyedCoProcessFunction that assembles sessions by UID.
 * Stream 1 (Zeek): buffers conn, dns, http, ssl by UID.
 * Stream 2 (Malicious): trigger - when any malicious log arrives, assemble session and output.
 * Conn is the anchor. Uses SessionUNSW42FeatureEncoder to extract UNSW 42 features from all logs.
 */
public class SessionAssemblerProcessFunction extends KeyedCoProcessFunction<String, JsonNode, JsonNode, String> {

    private static final Logger LOG = Logger.getLogger(SessionAssemblerProcessFunction.class.getName());
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private ValueState<ZeekSession> sessionState;
    private ValueState<SessionUNSW42FeatureEncoder.SessionState> encoderState;

    @Override
    public void open(Configuration parameters) {
        sessionState = getRuntimeContext().getState(
            new ValueStateDescriptor<>("session", ZeekSession.class));
        encoderState = getRuntimeContext().getState(
            new ValueStateDescriptor<>("encoderState", SessionUNSW42FeatureEncoder.SessionState.class));
    }

    @Override
    public void processElement1(JsonNode zeekLog, Context ctx, Collector<String> out) throws Exception {
        String uid = extractUid(zeekLog);
        if (uid == null || uid.isEmpty()) return;

        String logType = inferLogType(zeekLog);
        if (logType == null) return;

        ZeekSession session = sessionState.value();
        if (session == null) {
            session = new ZeekSession(uid);
        }

        addLogToSession(session, logType, zeekLog);
        sessionState.update(session);
    }

    @Override
    public void processElement2(JsonNode maliciousLog, Context ctx, Collector<String> out) throws Exception {
        String uid = extractUid(maliciousLog);
        if (uid == null || uid.isEmpty()) return;

        String logType = inferLogType(maliciousLog);
        if (logType == null) return;

        ZeekSession session = sessionState.value();
        if (session == null) {
            session = new ZeekSession(uid);
        }

        // Add the malicious log (it's the trigger - could be conn, dns, http, or ssl)
        addLogToSession(session, logType, maliciousLog);
        sessionState.update(session);

        // Only output if we have conn (anchor)
        if (!session.hasConn()) {
            LOG.warning("LSTM anomaly for UID " + uid + " but no conn yet, skipping supervised classification");
            return;
        }

        JsonNode conn = session.getConn();
        String src = conn.has("id.orig_h") ? conn.get("id.orig_h").asText() : "?";
        if (TrustedIps.isTrusted(src)) {
            return;  // Skip anomaly check for trusted IPs
        }

        // Encode session: UNSW 42 features from conn + http + dns + ssl
        try {
            SessionUNSW42FeatureEncoder.SessionState encState = encoderState.value();
            if (encState == null) encState = new SessionUNSW42FeatureEncoder.SessionState();
            double[] features = SessionUNSW42FeatureEncoder.encode(session, encState);
            encoderState.update(encState);
            if (features == null || features.length != 42) {
                LOG.warning("LSTM anomaly UID " + uid + ": SessionUNSW42 encode failed (features=" +
                    (features == null ? "null" : features.length) + "), skipping supervised");
                return;
            }

            String jsonFeatures = MAPPER.writeValueAsString(features);
            String prediction = APIClient.getPrediction("unsw42", jsonFeatures);

            String dst = conn.has("id.resp_h") ? conn.get("id.resp_h").asText() : "?";

            // Always log supervised output for LSTM-flagged anomalies (pipeline visibility)
            if (prediction != null && !prediction.equals("normal") && !prediction.equals("unknown")) {
                LOG.warning("Attack detected (session UNSW42): " + prediction + " | " + src + " -> " + dst);
                out.collect(sessionToJson(session));
            } else if (prediction != null && prediction.equals("normal")) {
                LOG.info("LSTM anomaly -> Supervised: Normal (no known attack type) | " + src + " -> " + dst);
            } else {
                LOG.warning("LSTM anomaly -> Supervised: unknown (API error or no prediction) | " + src + " -> " + dst);
            }
        } catch (Exception e) {
            LOG.log(Level.WARNING, "Error encoding/predicting session for UID " + uid + ": " + e.getMessage(), e);
        }
    }


    private void addLogToSession(ZeekSession session, String logType, JsonNode log) {
        JsonNode inner = unwrap(log, logType);
        switch (logType) {
            case "conn":
                session.setConn(inner);
                break;
            case "dns":
                session.addDns(inner);
                break;
            case "http":
                session.addHttp(inner);
                break;
            case "ssl":
                session.addSsl(inner);
                break;
            default:
                break;
        }
    }

    private static JsonNode unwrap(JsonNode node, String logType) {
        if (node == null) return null;
        String key = "zeek-" + logType;
        return node.has(key) ? node.get(key) : node;
    }

    private static String extractUid(JsonNode node) {
        if (node == null) return null;
        if (node.has("uid")) return node.get("uid").asText(null);
        if (node.has("zeek-conn") && node.get("zeek-conn").has("uid"))
            return node.get("zeek-conn").get("uid").asText(null);
        if (node.has("zeek-dns") && node.get("zeek-dns").has("uid"))
            return node.get("zeek-dns").get("uid").asText(null);
        if (node.has("zeek-http") && node.get("zeek-http").has("uid"))
            return node.get("zeek-http").get("uid").asText(null);
        if (node.has("zeek-ssl") && node.get("zeek-ssl").has("uid"))
            return node.get("zeek-ssl").get("uid").asText(null);
        return null;
    }

    private static String inferLogType(JsonNode node) {
        if (node == null) return null;
        JsonNode n = node.has("zeek-conn") ? node.get("zeek-conn") :
                     node.has("zeek-dns") ? node.get("zeek-dns") :
                     node.has("zeek-http") ? node.get("zeek-http") :
                     node.has("zeek-ssl") ? node.get("zeek-ssl") : node;

        if (n.has("conn_state") && n.has("duration")) return "conn";
        if (n.has("query") && n.has("rcode_name")) return "dns";
        if (n.has("method") && n.has("host")) return "http";
        if (n.has("server_name") && n.has("cipher")) return "ssl";
        return null;
    }

    private static String sessionToJson(ZeekSession session) {
        try {
            return MAPPER.writeValueAsString(new Object[]{
                session.getUid(),
                session.getConn() != null ? session.getConn().toString() : null,
                session.getDns().size(),
                session.getHttp().size(),
                session.getSsl().size()
            });
        } catch (Exception e) {
            return "{}";
        }
    }
}
