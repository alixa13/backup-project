package com.example.util;

import com.example.model.ZeekConnLog;
import com.example.model.ZeekDNSLog;
import com.example.model.ZeekHTTPLog;
import com.example.model.ZeekSSLLog;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.List;

/**
 * Unified Feature Engineering for Supervised Attack Type Detection.
 * <p>
 * Single module for all 4 log types (conn, ssl, dns, http).
 * Each supervised job reads from its malicious topic, calls encode() with its log type,
 * and sends preprocessed features to the model for attack type classification.
 * <p>
 * Feature counts: conn=41, ssl=33, dns=38, http=52
 */
public class SupervisedFeatureEncoder {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    static {
        MAPPER.configure(
            org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES,
            false
        );
    }

    public static final int FEATURE_COUNT_CONN = 41;
    public static final int FEATURE_COUNT_SSL = 33;
    public static final int FEATURE_COUNT_DNS = 38;
    public static final int FEATURE_COUNT_HTTP = 52;

    public static int getFeatureCount(String logType) {
        switch (logType.toLowerCase()) {
            case "conn": return FEATURE_COUNT_CONN;
            case "ssl": return FEATURE_COUNT_SSL;
            case "dns": return FEATURE_COUNT_DNS;
            case "http": return FEATURE_COUNT_HTTP;
            default: throw new IllegalArgumentException("Unknown log type: " + logType);
        }
    }

    /**
     * Parse timestamp from JSON to double.
     */
    public static double parseTimestamp(Object timestamp) {
        try {
            if (timestamp == null) return 0.0;
            String s = timestamp.toString().trim();
            if (s.isEmpty()) return 0.0;
            return Double.parseDouble(s);
        } catch (Exception e) {
            return 0.0;
        }
    }

    /**
     * Main entry point: encode Zeek log JSON to feature vector for supervised model.
     *
     * @param logType One of: conn, ssl, dns, http
     * @param logEntry Full JSON (may contain zeek-conn, zeek-dns, etc.)
     * @return Feature array for the given log type, or null on error
     */
    public static double[] encode(String logType, JsonNode logEntry) {
        if (logEntry == null) return null;
        JsonNode data = logEntry;
        String key = "zeek-" + logType.toLowerCase();
        if (logEntry.has(key)) {
            data = logEntry.get(key);
        }
        if (data == null) return null;

        switch (logType.toLowerCase()) {
            case "conn": return encodeConn(data);
            case "ssl": return encodeSsl(data);
            case "dns": return encodeDns(data);
            case "http": return encodeHttp(data);
            default: return null;
        }
    }

    /**
     * Encode conn log (41 features).
     */
    public static double[] encodeConn(JsonNode connData) {
        try {
            ZeekConnLog log = MAPPER.treeToValue(connData, ZeekConnLog.class);
            double timestamp = connData.has("ts") ? parseTimestamp(connData.get("ts")) : 0.0;

            List<Double> features = new ArrayList<>();
            features.addAll(list(FeatureEncoder.encodeIPAddress(log.getIdOrigH())));
            features.addAll(list(FeatureEncoder.encodeIPAddress(log.getIdRespH())));
            features.add(FeatureEncoder.normalizePort(log.getIdOrigP()));
            features.add(FeatureEncoder.normalizePort(log.getIdRespP()));
            features.add(FeatureEncoder.encodeProtocol(log.getProto()));
            features.addAll(list(FeatureEncoder.encodeService(log.getService())));
            features.add(FeatureEncoder.encodeConnState(log.getConnState()));
            features.addAll(list(FeatureEncoder.encodeTimestamp(timestamp)));
            features.add(FeatureEncoder.normalizeDuration(log.getDuration()));
            features.add(FeatureEncoder.normalizeBytes(log.getOrigBytes()));
            features.add(FeatureEncoder.normalizeBytes(log.getRespBytes()));
            features.add(FeatureEncoder.normalizePackets(log.getOrigPkts()));
            features.add(FeatureEncoder.normalizePackets(log.getRespPkts()));
            features.addAll(list(FeatureEncoder.derivedConnectionFeatures(
                log.getOrigBytes(), log.getRespBytes(), log.getOrigPkts(), log.getRespPkts(), log.getDuration())));
            features.add(FeatureEncoder.booleanToDouble(log.getLocalOrig()));
            features.add((log.getMissedBytes() != null && log.getMissedBytes() > 0) ? 1.0 : 0.0);

            return toArray(features);
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Encode ssl log (33 features).
     */
    public static double[] encodeSsl(JsonNode sslData) {
        try {
            ZeekSSLLog log = MAPPER.treeToValue(sslData, ZeekSSLLog.class);
            double timestamp = sslData.has("ts") ? parseTimestamp(sslData.get("ts")) : 0.0;
            String version = log.getVersion();
            String subject = log.getSubject();
            String issuer = log.getIssuer();
            String serverName = log.getServerName();

            List<Double> features = new ArrayList<>();
            features.addAll(list(FeatureEncoder.encodeIPAddress(log.getIdOrigH())));
            features.addAll(list(FeatureEncoder.encodeIPAddress(log.getIdRespH())));
            features.add(FeatureEncoder.normalizePort(log.getIdOrigP()));
            features.add(FeatureEncoder.normalizePort(log.getIdRespP()));
            features.addAll(list(FeatureEncoder.encodeTimestamp(timestamp)));
            features.add(FeatureEncoder.encodeSSLVersion(version));
            features.addAll(list(FeatureEncoder.encodeSSLCertificate(subject, issuer)));
            features.add(serverName != null ? Math.min(serverName.length(), 255) / 255.0 : 0.0);
            features.add(serverName != null ? FeatureEncoder.calculateEntropy(serverName) : 0.0);
            features.add(FeatureEncoder.booleanToDouble(log.getEstablished()));
            features.add(log.getCipher() != null ? Math.min(log.getCipher().length(), 100) / 100.0 : 0.0);
            features.add(log.getCurve() != null && !log.getCurve().isEmpty() ? 1.0 : 0.0);
            features.add(FeatureEncoder.booleanToDouble(log.getResumed()));
            features.add(log.getResumed() != null && log.getResumed() ? 0.5 : 0.0);
            features.add(0.0); // last_alert placeholder

            double versionScore = 0.0;
            if (version != null) {
                if (version.contains("TLSv1.3")) versionScore = 1.0;
                else if (version.contains("TLSv1.2")) versionScore = 0.8;
                else if (version.contains("TLSv1.1")) versionScore = 0.5;
                else if (version.contains("TLSv1.0")) versionScore = 0.3;
                else if (version.contains("SSLv3")) versionScore = 0.1;
            }
            features.add(versionScore);
            features.add((log.getCertChainFuids() != null && log.getCertChainFuids().length > 0) ? 1.0 : 0.0);
            features.add((log.getClientCertChainFuids() != null && log.getClientCertChainFuids().length > 0) ? 1.0 : 0.0);
            features.add(log.getValidationStatus() != null && !log.getValidationStatus().isEmpty() ? 1.0 : 0.0);
            double quality = 0.0;
            if (Boolean.TRUE.equals(log.getEstablished())) quality += 0.5;
            if (subject != null && issuer != null && !subject.equals(issuer)) quality += 0.3;
            if (versionScore > 0.5) quality += 0.2;
            features.add(quality);

            return toArray(features);
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Encode dns log (38 features).
     */
    public static double[] encodeDns(JsonNode dnsData) {
        try {
            ZeekDNSLog log = MAPPER.treeToValue(dnsData, ZeekDNSLog.class);
            double timestamp = dnsData.has("ts") ? parseTimestamp(dnsData.get("ts")) : 0.0;
            String query = dnsData.has("query") ? dnsData.get("query").asText("") : "";
            String rcodeName = dnsData.has("rcode_name") ? dnsData.get("rcode_name").asText("") : "";
            int answerCount = dnsData.has("answers") && dnsData.get("answers").isArray() ? dnsData.get("answers").size() : 0;

            List<Double> features = new ArrayList<>();
            features.addAll(list(FeatureEncoder.encodeIPAddress(log.getIdOrigH())));
            features.addAll(list(FeatureEncoder.encodeIPAddress(log.getIdRespH())));
            features.add(FeatureEncoder.normalizePort(log.getIdOrigP()));
            features.add(FeatureEncoder.normalizePort(log.getIdRespP()));
            features.add(FeatureEncoder.encodeProtocol(log.getProto()));
            features.addAll(list(FeatureEncoder.encodeTimestamp(timestamp)));
            features.addAll(list(FeatureEncoder.encodeDNSQuery(query)));
            features.add(Math.min(dnsData.has("qtype") ? dnsData.get("qtype").asDouble(0) : 0, 100) / 100.0);
            features.add(FeatureEncoder.encodeDNSRcode(rcodeName));
            features.add(Math.log1p(log.getRtt()) / Math.log1p(10.0));
            features.add(dnsData.has("AA") && dnsData.get("AA").asBoolean(false) ? 1.0 : 0.0);
            features.add(dnsData.has("TC") && dnsData.get("TC").asBoolean(false) ? 1.0 : 0.0);
            features.add(dnsData.has("RD") && dnsData.get("RD").asBoolean(false) ? 1.0 : 0.0);
            features.add(dnsData.has("RA") && dnsData.get("RA").asBoolean(false) ? 1.0 : 0.0);
            features.add(dnsData.has("Z") && dnsData.get("Z").asDouble(0) > 0 ? 1.0 : 0.0);
            features.add(Math.min(answerCount, 20) / 20.0);
            features.add(answerCount > 0 ? 0.5 : 0.0);
            features.add(answerCount > 0 ? 0.5 : 0.0);
            features.add(answerCount > 0 ? 0.5 : 0.0);
            features.add(dnsData.has("rejected") && dnsData.get("rejected").asBoolean(false) ? 1.0 : 0.0);
            features.add(Math.min(log.getTransId() != null ? log.getTransId() : 0, 65535) / 65535.0);
            features.add(query.length() > 0 ? answerCount / (double) Math.max(query.length(), 1) : 0.0);
            features.add("NXDOMAIN".equalsIgnoreCase(rcodeName) ? 1.0 : 0.0);

            return toArray(features);
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Encode http log (52 features).
     */
    public static double[] encodeHttp(JsonNode httpData) {
        try {
            ZeekHTTPLog log = MAPPER.treeToValue(httpData, ZeekHTTPLog.class);
            double timestamp = httpData.has("ts") ? parseTimestamp(httpData.get("ts")) : 0.0;
            String uri = log.getUri() != null ? log.getUri() : "";
            String userAgent = log.getUserAgent() != null ? log.getUserAgent() : "";
            String referrer = log.getReferrer() != null ? log.getReferrer() : "";
            String host = log.getHost() != null ? log.getHost() : "";
            Long reqBodyLen = log.getRequestBodyLen();
            Long respBodyLen = log.getResponseBodyLen();
            long reqLen = reqBodyLen != null ? reqBodyLen : 0;
            long resLen = respBodyLen != null ? respBodyLen : 0;
            long totalLen = reqLen + resLen;

            List<Double> features = new ArrayList<>();
            features.addAll(list(FeatureEncoder.encodeIPAddress(log.getIdOrigH())));
            features.addAll(list(FeatureEncoder.encodeIPAddress(log.getIdRespH())));
            features.add(FeatureEncoder.normalizePort(log.getIdOrigP()));
            features.add(FeatureEncoder.normalizePort(log.getIdRespP()));
            features.addAll(list(FeatureEncoder.encodeTimestamp(timestamp)));
            features.addAll(list(FeatureEncoder.encodeHTTPMethod(log.getMethod())));
            features.addAll(list(FeatureEncoder.encodeURIFeatures(uri)));
            features.addAll(list(FeatureEncoder.encodeStatusCode(log.getStatusCode())));
            features.add(reqBodyLen != null ? Math.log1p(reqBodyLen) / Math.log1p(1e9) : 0.0);
            features.add(respBodyLen != null ? Math.log1p(respBodyLen) / Math.log1p(1e9) : 0.0);
            features.add(log.getTransDepth() != null ? Math.min(log.getTransDepth(), 20) / 20.0 : 0.0);
            features.add(Math.min(userAgent.length(), 500) / 500.0);
            features.add(FeatureEncoder.calculateEntropy(userAgent));
            features.add(userAgent.toLowerCase().contains("bot") ? 1.0 : 0.0);
            features.add(referrer.isEmpty() ? 0.0 : 1.0);
            features.add(FeatureEncoder.calculateEntropy(referrer));
            features.add(FeatureEncoder.calculateEntropy(host));
            features.add(totalLen > 0 ? reqLen / (double) totalLen : 0.5);
            features.add((log.getUsername() != null || log.getPassword() != null) ? 1.0 : 0.0);
            features.add((log.getOrigFuids() != null && log.getOrigFuids().length > 0) ? 1.0 : 0.0);
            features.add((log.getRespFuids() != null && log.getRespFuids().length > 0) ? 1.0 : 0.0);
            features.add((log.getProxied() != null && log.getProxied().length > 0) ? 1.0 : 0.0);
            features.add((log.getTags() != null && log.getTags().length > 0) ? 1.0 : 0.0);

            return toArray(features);
        } catch (Exception e) {
            return null;
        }
    }

    private static List<Double> list(double[] arr) {
        List<Double> list = new ArrayList<>(arr.length);
        for (double v : arr) list.add(v);
        return list;
    }

    private static double[] toArray(List<Double> list) {
        double[] arr = new double[list.size()];
        for (int i = 0; i < list.size(); i++) arr[i] = list.get(i);
        return arr;
    }
}
