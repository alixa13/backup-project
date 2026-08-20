package com.example.util;

import com.example.model.ZeekConnLog;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * UNSW-NB15 Standard 42-Feature Encoder for Flink.
 * Matches official f1-f42 from NUSW-NB15_features.csv.
 * See: supervised/UNSW_NB15_STANDARD.md
 *
 * Stateful: ct_* features require counters. Caller must pass and update state.
 */
public class UNSW42FeatureEncoder {

    public static final int FEATURE_COUNT = 42;
    private static final ObjectMapper MAPPER = new ObjectMapper();

    /** State for ct_* computation - caller holds this per key (e.g. src_ip). */
    public static class ConnState implements Serializable {
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

    /**
     * Encode Zeek conn.log to UNSW 42 features.
     * Updates state for ct_*; caller must reuse same state for same logical stream.
     */
    public static double[] encodeConn(JsonNode connData, ConnState state) {
        try {
            ZeekConnLog log = MAPPER.treeToValue(connData, ZeekConnLog.class);
            double ts = connData.has("ts") ? parseTs(connData.get("ts")) : 0;
            double dur = Math.max(log.getDuration() != null ? log.getDuration() : 0, 0.001);
            long ob = log.getOrigBytes() != null ? log.getOrigBytes() : 0;
            long rb = log.getRespBytes() != null ? log.getRespBytes() : 0;
            long op = log.getOrigPkts() != null ? log.getOrigPkts() : 0;
            long rp = log.getRespPkts() != null ? log.getRespPkts() : 0;
            String srcIp = log.getIdOrigH() != null ? log.getIdOrigH() : "";
            String dstIp = log.getIdRespH() != null ? log.getIdRespH() : "";
            int srcPort = log.getIdOrigP() != null ? log.getIdOrigP() : 0;
            int dstPort = log.getIdRespP() != null ? log.getIdRespP() : 0;
            String proto = log.getProto();
            String service = log.getService() != null ? log.getService() : "";
            String connState = log.getConnState();
            String history = log.getHistory();

            // IAT (sinpkt, dinpkt)
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
            boolean isSmIpsPorts = srcIp.equals(dstIp) && srcPort == dstPort;

            // f1-f42 per UNSW-NB15 standard
            return new double[]{
                Math.log1p(dur), encodeProto(proto), encodeService(service), encodeState(connState),  // f1-4
                op, rp, ob, rb,                                                                        // f5-8
                rate, 64.0, 64.0,                                                                      // f9-11 sttl,dttl
                sload, dload, 0.0, 0.0,                                                                // f12-15 sloss,dloss
                Math.log1p(sinpkt), Math.log1p(dinpkt), 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,                  // f16-23
                dur > 0 ? Math.log1p(dur) : 0, synack, ackdat,                                         // f24-26 tcprtt,synack,ackdat
                smean, dmean, 0.0, 0.0,                                                                // f27-30 trans_depth,response_body_len
                ctSrvSrc, ctStateTtl, ctDstLtm, ctSrcDport, ctDstSport, ctDstSrc,                       // f31-36
                isFtp ? 1.0 : 0.0, isFtp ? ctSrvSrc : 0.0, 0.0,                                        // f37-39
                ctSrcLtm, ctSrvDst,                                                                   // f40-41
                isSmIpsPorts ? 1.0 : 0.0                                                              // f42
            };
        } catch (Exception e) {
            return null;
        }
    }
}
