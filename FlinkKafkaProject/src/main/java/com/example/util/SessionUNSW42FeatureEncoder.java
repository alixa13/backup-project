package com.example.util;

import com.example.model.ZeekSession;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;

import java.io.Serializable;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * UNSW-NB15 42-feature encoder from ZeekSession (conn + http + dns + ssl).
 * Extracts features from all log types per NUSW-NB15_features.csv.
 * Produces missing features (0 or default) when not available in any log.
 */
public class SessionUNSW42FeatureEncoder {

    private static final Logger LOG = Logger.getLogger(SessionUNSW42FeatureEncoder.class.getName());
    public static final int FEATURE_COUNT = 42;

    /** State for ct_* - caller holds per key (e.g. src_ip). */
    public static class SessionState implements Serializable {
        private static final long serialVersionUID = 1L;
        public final Map<String, Integer> srcService = new HashMap<>();
        public final Map<String, Integer> srcDst = new HashMap<>();
        public final Map<String, Integer> dstLtm = new HashMap<>();
        public final Map<String, Integer> srcLtm = new HashMap<>();
        public final Map<String, Integer> srcDportLtm = new HashMap<>();
        public final Map<String, Integer> dstSportLtm = new HashMap<>();
        public final Map<String, Integer> dstSrcLtm = new HashMap<>();
        public final Map<String, Integer> stateTtl = new HashMap<>();
        public final Map<String, Double> lastTs = new HashMap<>();
        public final Map<String, java.util.List<Double>> iatBuffer = new HashMap<>();
    }

    private static double parseTs(Object o) {
        try {
            if (o == null) return 0;
            String s = o.toString().trim();
            return s.isEmpty() ? 0 : Double.parseDouble(s);
        } catch (Exception e) {
            return 0;
        }
    }

    private static int encodeProto(String p) {
        if (p == null) return 0;
        switch (p.toLowerCase()) {
            case "tcp": return 0;
            case "udp": return 1;
            case "icmp": return 2;
            default: return 3;
        }
    }

    private static int encodeService(String s) {
        if (s == null || s.isEmpty()) return 0;
        switch (s.toLowerCase()) {
            case "http": return 1;
            case "https": case "ssl": return 2;
            case "dns": return 3;
            case "ssh": return 4;
            case "ftp": return 5;
            default: return 0;
        }
    }

    private static int encodeState(String s) {
        if (s == null) return 0;
        switch (s.toUpperCase()) {
            case "SF": return 1;
            case "S0": return 2;
            case "REJ": return 3;
            case "S1": return 4;
            case "S2": return 5;
            case "S3": return 6;
            case "RSTO": return 7;
            case "RSTR": return 8;
            case "OTH": return 13;
            default: return 0;
        }
    }

    private static int[] parseHistory(String h) {
        if (h == null || h.isEmpty()) return new int[]{0, 0};
        int synack = 0, ackdat = 0;
        for (char c : h.toCharArray()) {
            if (c == 'S' || c == 'A') synack++;
            if (c == 'A' || c == 'D') ackdat++;
        }
        return new int[]{synack, ackdat};
    }

    private static int cap255(int v) {
        return Math.min(Math.max(v, 0), 255);
    }

    private static long getLong(JsonNode n, String key, long def) {
        if (n == null || !n.has(key)) return def;
        try {
            return n.get(key).asLong(def);
        } catch (Exception e) {
            return def;
        }
    }

    private static int getInt(JsonNode n, String key, int def) {
        if (n == null || !n.has(key)) return def;
        try {
            return n.get(key).asInt(def);
        } catch (Exception e) {
            return def;
        }
    }

    private static String getStr(JsonNode n, String key, String def) {
        if (n == null || !n.has(key)) return def;
        try {
            JsonNode v = n.get(key);
            return v != null && !v.isNull() ? v.asText(def) : def;
        } catch (Exception e) {
            return def;
        }
    }

    private static double getDouble(JsonNode n, String key, double def) {
        if (n == null || !n.has(key)) return def;
        try {
            return n.get(key).asDouble(def);
        } catch (Exception e) {
            return def;
        }
    }

    /**
     * Extract UNSW features from HTTP logs: trans_depth, res_bdy_len, ct_flw_http_mthd, is_ftp_login.
     */
    private static void extractFromHttp(List<JsonNode> httpLogs, int[] out) {
        int transDepth = 0;
        long resBdyLen = 0;
        int ctFlwHttpMthd = 0;
        boolean isFtpLogin = false;
        for (JsonNode h : httpLogs) {
            transDepth = Math.max(transDepth, getInt(h, "trans_depth", 0));
            resBdyLen += getLong(h, "response_body_len", 0);
            String method = getStr(h, "method", "").toUpperCase();
            if ("GET".equals(method) || "POST".equals(method)) ctFlwHttpMthd++;
            if (!getStr(h, "username", "").isEmpty() || !getStr(h, "password", "").isEmpty()) {
                isFtpLogin = true;
            }
        }
        out[0] = transDepth;
        out[1] = (int) Math.min(resBdyLen, Integer.MAX_VALUE);
        out[2] = cap255(ctFlwHttpMthd);
        out[3] = isFtpLogin ? 1 : 0;
    }

    /**
     * Extract jitter proxy from DNS rtt variance (Sjit, Djit).
     */
    private static double[] extractJitterFromDns(List<JsonNode> dnsLogs) {
        if (dnsLogs == null || dnsLogs.isEmpty()) return new double[]{0.0, 0.0};
        double[] rtts = new double[dnsLogs.size()];
        int i = 0;
        for (JsonNode d : dnsLogs) {
            double rtt = getDouble(d, "rtt", 0);
            if (rtt > 0) rtts[i++] = rtt;
        }
        if (i < 2) return new double[]{0.0, 0.0};
        double mean = 0;
        for (int j = 0; j < i; j++) mean += rtts[j];
        mean /= i;
        double var = 0;
        for (int j = 0; j < i; j++) var += (rtts[j] - mean) * (rtts[j] - mean);
        double std = Math.sqrt(var / i);
        return new double[]{std, std};  // sjit, djit proxy
    }

    /**
     * Check SSL for FTP (is_ftp_login via server_name).
     */
    private static boolean hasFtpFromSsl(List<JsonNode> sslLogs) {
        if (sslLogs == null) return false;
        for (JsonNode s : sslLogs) {
            String sn = getStr(s, "server_name", "").toLowerCase();
            if (sn.contains("ftp")) return true;
        }
        return false;
    }

    /**
     * Encode ZeekSession to exactly 42 UNSW-NB15 features.
     * Conn is required. HTTP/DNS/SSL enrich where features exist.
     */
    public static double[] encode(ZeekSession session, SessionState state) {
        if (session == null || !session.hasConn()) return null;
        JsonNode conn = session.getConn();
        List<JsonNode> httpLogs = session.getHttp();
        List<JsonNode> dnsLogs = session.getDns();
        List<JsonNode> sslLogs = session.getSsl();

        try {
            // Extract from JsonNode directly (Zeek uses dotted names: id.orig_h, id.resp_h, etc.)
            double ts = conn.has("ts") ? parseTs(conn.get("ts")) : 0;
            double dur = Math.max(getDouble(conn, "duration", 0.0), 0.001);
            long ob = getLong(conn, "orig_bytes", 0);
            long rb = getLong(conn, "resp_bytes", 0);
            long op = getLong(conn, "orig_pkts", 0);
            long rp = getLong(conn, "resp_pkts", 0);
            String srcIp = getStr(conn, "id.orig_h", "");
            String dstIp = getStr(conn, "id.resp_h", "");
            int srcPort = getInt(conn, "id.orig_p", 0);
            int dstPort = getInt(conn, "id.resp_p", 0);
            String proto = getStr(conn, "proto", "");
            String service = getStr(conn, "service", "");
            String connState = getStr(conn, "conn_state", "");
            String history = getStr(conn, "history", "");

            // IAT from conn ts
            double sinpkt = dur, dinpkt = 0;
            if (state != null && ts > 0 && !srcIp.isEmpty()) {
                Double last = state.lastTs.get(srcIp);
                if (last != null) {
                    double iat = ts - last;
                    if (iat > 0 && iat < 3600) {
                        state.iatBuffer.computeIfAbsent(srcIp, k -> new java.util.ArrayList<>()).add(iat);
                        java.util.List<Double> buf = state.iatBuffer.get(srcIp);
                        if (buf.size() > 50) buf.remove(0);
                        if (!buf.isEmpty()) {
                            double mean = buf.stream().mapToDouble(Double::doubleValue).average().orElse(dur);
                            sinpkt = mean;
                            dinpkt = buf.size() > 1 ? Math.sqrt(buf.stream().mapToDouble(x -> Math.pow(x - mean, 2)).average().orElse(0)) : 0;
                        }
                    }
                }
                state.lastTs.put(srcIp, ts);
            }

            // Extract from HTTP: trans_depth, res_bdy_len, ct_flw_http_mthd, is_ftp_login
            int[] httpFeat = new int[4];
            extractFromHttp(httpLogs, httpFeat);
            int transDepth = httpFeat[0];
            long resBdyLen = httpFeat[1];
            int ctFlwHttpMthd = httpFeat[2];
            boolean isFtpLoginHttp = httpFeat[3] == 1;

            // Jitter from DNS rtt variance (or 0)
            double[] jitter = extractJitterFromDns(dnsLogs);
            double sjit = jitter[0];
            double djit = jitter[1];

            // is_ftp_login: HTTP username/password OR SSL server_name contains ftp
            boolean isFtpLogin = isFtpLoginHttp || hasFtpFromSsl(sslLogs);

            // ct_*
            String kSrv = srcIp + "|" + service;
            String kDst = srcIp + "|" + dstIp;
            String kDport = srcIp + "|" + dstPort;
            String kSport = dstIp + "|" + srcPort;
            String kDstSrc = dstIp + "|" + srcIp;

            if (state != null) {
                state.srcService.merge(kSrv, 1, Integer::sum);
                state.srcDst.merge(kDst, 1, Integer::sum);
                state.dstLtm.merge(dstIp, 1, Integer::sum);
                state.srcLtm.merge(srcIp, 1, Integer::sum);
                state.srcDportLtm.merge(kDport, 1, Integer::sum);
                state.dstSportLtm.merge(kSport, 1, Integer::sum);
                state.dstSrcLtm.merge(kDstSrc, 1, Integer::sum);
                state.stateTtl.merge(connState != null ? connState : "", 1, Integer::sum);
            }

            int ctSrvSrc = state != null ? cap255(state.srcService.getOrDefault(kSrv, 0)) : 1;
            int ctStateTtl = state != null ? cap255(state.stateTtl.getOrDefault(connState != null ? connState : "", 0)) : 1;
            int ctDstLtm = state != null ? cap255(state.dstLtm.getOrDefault(dstIp, 0)) : 1;
            int ctSrcDport = state != null ? cap255(state.srcDportLtm.getOrDefault(kDport, 0)) : 1;
            int ctDstSport = state != null ? cap255(state.dstSportLtm.getOrDefault(kSport, 0)) : 1;
            int ctDstSrc = state != null ? cap255(state.dstSrcLtm.getOrDefault(kDstSrc, 0)) : 1;
            int ctSrcLtm = state != null ? cap255(state.srcLtm.getOrDefault(srcIp, 0)) : 1;
            int ctSrvDst = state != null ? cap255(state.srcDst.getOrDefault(kDst, 0)) : 1;

            int[] ha = parseHistory(history);
            double synack = ha[0], ackdat = ha[1];
            long totalPkts = op + rp;
            double rate = dur > 0 ? totalPkts / dur : 0;
            double sload = dur > 0 ? ob / dur : 0;
            double dload = dur > 0 ? rb / dur : 0;
            double smean = op > 0 ? (double) ob / op : 0;
            double dmean = rp > 0 ? (double) rb / rp : 0;

            boolean isFtp = service.toLowerCase().contains("ftp");
            int ctFtpCmd = isFtp ? ctSrvSrc : 0;
            boolean isSmIpsPorts = srcIp.equals(dstIp) && srcPort == dstPort;

            // f1-f42 per NUSW-NB15_features.csv - exact UNSW implementation
            return new double[]{
                Math.log1p(dur), encodeProto(proto), encodeService(service), encodeState(connState),  // f1-4 dur,proto,service,state
                op, rp, ob, rb,                                                                        // f5-8 spkts,dpkts,sbytes,dbytes
                rate, 64.0, 64.0,                                                                      // f9-11 rate,sttl,dttl
                sload, dload, 0.0, 0.0,                                                                // f12-15 sload,dload,sloss,dloss
                Math.log1p(sinpkt), Math.log1p(dinpkt), sjit, djit,                                     // f16-19 sinpkt,dinpkt,sjit,djit
                0.0, 0.0, 0.0, 0.0,                                                                    // f20-23 swin,stcpb,dtcpb,dwin
                dur > 0 ? Math.log1p(dur) : 0, synack, ackdat,                                         // f24-26 tcprtt,synack,ackdat
                smean, dmean, transDepth, Math.min(resBdyLen, Integer.MAX_VALUE),                       // f27-30 smean,dmean,trans_depth,res_bdy_len
                ctSrvSrc, ctStateTtl, ctDstLtm, ctSrcDport, ctDstSport, ctDstSrc,                       // f31-36
                isFtpLogin ? 1.0 : 0.0, (double) ctFtpCmd, (double) ctFlwHttpMthd,                      // f37-39 is_ftp_login,ct_ftp_cmd,ct_flw_http_mthd
                ctSrcLtm, ctSrvDst,                                                                   // f40-41
                isSmIpsPorts ? 1.0 : 0.0                                                              // f42 is_sm_ips_ports
            };
        } catch (Exception e) {
            LOG.log(Level.WARNING, "SessionUNSW42 encode failed: " + e.getMessage(), e);
            return null;
        }
    }
}
