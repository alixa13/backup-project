package com.example.util;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;

/**
 * Utility class for proper feature encoding in network traffic analysis.
 * Replaces the problematic hashCode() approach with semantically meaningful encodings.
 */
public class FeatureEncoder {

    // ==================== IP Address Encoding ====================

    /**
     * Encode IP address as 4 normalized octets + private IP flag.
     * Returns [octet1/255, octet2/255, octet3/255, octet4/255, isPrivate]
     */
    public static double[] encodeIPAddress(String ip) {
        if (ip == null || ip.isEmpty()) {
            return new double[]{0.0, 0.0, 0.0, 0.0, 0.0};
        }

        String[] parts = ip.split("\\.");
        if (parts.length != 4) {
            return new double[]{0.0, 0.0, 0.0, 0.0, 0.0};
        }

        try {
            int[] octets = new int[4];
            for (int i = 0; i < 4; i++) {
                octets[i] = Integer.parseInt(parts[i]);
                if (octets[i] < 0 || octets[i] > 255) {
                    return new double[]{0.0, 0.0, 0.0, 0.0, 0.0};
                }
            }

            return new double[]{
                octets[0] / 255.0,
                octets[1] / 255.0,
                octets[2] / 255.0,
                octets[3] / 255.0,
                isPrivateIP(octets) ? 1.0 : 0.0
            };
        } catch (NumberFormatException e) {
            return new double[]{0.0, 0.0, 0.0, 0.0, 0.0};
        }
    }

    /**
     * Check if IP address is in private range
     */
    private static boolean isPrivateIP(int[] octets) {
        // 10.0.0.0/8
        if (octets[0] == 10) return true;

        // 172.16.0.0/12
        if (octets[0] == 172 && octets[1] >= 16 && octets[1] <= 31) return true;

        // 192.168.0.0/16
        if (octets[0] == 192 && octets[1] == 168) return true;

        return false;
    }

    // ==================== Protocol Encoding ====================

    /**
     * Encode network protocol with label encoding.
     * Returns normalized value [0, 1]
     */
    public static double encodeProtocol(String proto) {
        if (proto == null) return 0.0;

        switch (proto.toLowerCase()) {
            case "tcp": return 0.25;   // 1/4
            case "udp": return 0.50;   // 2/4
            case "icmp": return 0.75;  // 3/4
            case "icmp6": return 1.0;  // 4/4
            default: return 0.0;       // Unknown
        }
    }

    // ==================== Service Encoding ====================

    /**
     * Encode service using one-hot encoding for common services.
     * Returns array: [http, https, dns, ssh, ftp, smtp, pop3, imap, other]
     */
    public static double[] encodeService(String service) {
        double[] encoded = new double[9];

        if (service == null || service.isEmpty()) {
            encoded[8] = 1.0;  // "other"
            return encoded;
        }

        switch (service.toLowerCase()) {
            case "http":
                encoded[0] = 1.0;
                break;
            case "https":
            case "ssl":
                encoded[1] = 1.0;
                break;
            case "dns":
                encoded[2] = 1.0;
                break;
            case "ssh":
                encoded[3] = 1.0;
                break;
            case "ftp":
            case "ftp-data":
                encoded[4] = 1.0;
                break;
            case "smtp":
                encoded[5] = 1.0;
                break;
            case "pop3":
                encoded[6] = 1.0;
                break;
            case "imap":
                encoded[7] = 1.0;
                break;
            default:
                encoded[8] = 1.0;
                break;
        }

        return encoded;
    }

    // ==================== Connection State Encoding ====================

    /**
     * Encode connection state with semantic ordering.
     * Returns normalized value [0, 1]
     */
    public static double encodeConnState(String connState) {
        if (connState == null) return 0.0;

        // Ordered by connection completeness/success
        switch (connState) {
            case "S0": return 1.0 / 13.0;    // Connection attempt, no reply
            case "S1": return 2.0 / 13.0;    // Established, not terminated
            case "SF": return 3.0 / 13.0;    // Normal establishment and termination
            case "REJ": return 4.0 / 13.0;   // Connection rejected
            case "S2": return 5.0 / 13.0;    // Established, originator aborted
            case "S3": return 6.0 / 13.0;    // Established, responder aborted
            case "RSTO": return 7.0 / 13.0;  // Originator sent RST
            case "RSTR": return 8.0 / 13.0;  // Responder sent RST
            case "RSTOS0": return 9.0 / 13.0;
            case "RSTRH": return 10.0 / 13.0;
            case "SH": return 11.0 / 13.0;
            case "SHR": return 12.0 / 13.0;
            case "OTH": return 13.0 / 13.0;
            default: return 0.0;
        }
    }

    // ==================== History Encoding (TCP Flags) ====================

    /**
     * Encode TCP connection history into feature vector.
     * Returns: [length, SYN_count, ACK_count, FIN_count, RST_count, Data_count]
     */
    public static double[] encodeHistory(String history) {
        if (history == null || history.isEmpty()) {
            return new double[]{0.0, 0.0, 0.0, 0.0, 0.0, 0.0};
        }

        long synCount = history.chars().filter(c -> c == 'S').count();
        long ackCount = history.chars().filter(c -> c == 'A').count();
        long finCount = history.chars().filter(c -> c == 'F').count();
        long rstCount = history.chars().filter(c -> c == 'R').count();
        long dataCount = history.chars().filter(c -> c == 'D').count();

        // Normalize counts
        double length = history.length();
        double maxCount = Math.max(1.0, length);

        return new double[]{
            Math.log1p(length) / Math.log1p(100),  // Log-scaled length
            synCount / maxCount,
            ackCount / maxCount,
            finCount / maxCount,
            rstCount / maxCount,
            dataCount / maxCount
        };
    }

    // ==================== Normalization Functions ====================

    /**
     * Normalize port number to [0, 1]
     */
    public static double normalizePort(Integer port) {
        if (port == null || port < 0) return 0.0;
        return Math.min(port, 65535) / 65535.0;
    }

    /**
     * Log-scale and normalize byte counts
     */
    public static double normalizeBytes(Long bytes) {
        if (bytes == null || bytes <= 0) return 0.0;
        // Log1p to handle 0, normalize to ~[0, 1] assuming max 1TB
        return Math.log1p(bytes) / Math.log1p(1e12);
    }

    /**
     * Log-scale and normalize duration (seconds)
     */
    public static double normalizeDuration(Double duration) {
        if (duration == null || duration <= 0) return 0.0;
        // Normalize assuming max duration of 1 day (86400 seconds)
        return Math.log1p(duration) / Math.log1p(86400);
    }

    /**
     * Log-scale and normalize packet counts
     */
    public static double normalizePackets(Long packets) {
        if (packets == null || packets <= 0) return 0.0;
        // Normalize assuming max 1 million packets
        return Math.log1p(packets) / Math.log1p(1e6);
    }

    // ==================== Temporal Features ====================

    /**
     * Extract temporal features from timestamp.
     * Returns: [hour_of_day, day_of_week, minute_of_hour, is_night]
     */
    public static double[] encodeTimestamp(double timestamp) {
        try {
            Instant instant = Instant.ofEpochSecond((long) timestamp);
            ZonedDateTime zdt = instant.atZone(ZoneId.systemDefault());

            return new double[]{
                zdt.getHour() / 24.0,                               // Hour normalized
                zdt.getDayOfWeek().getValue() / 7.0,               // Day of week normalized
                zdt.getMinute() / 60.0,                             // Minute normalized
                (zdt.getHour() >= 22 || zdt.getHour() <= 6) ? 1.0 : 0.0  // Night hours flag
            };
        } catch (Exception e) {
            return new double[]{0.0, 0.0, 0.0, 0.0};
        }
    }

    // ==================== Derived Statistical Features ====================

    /**
     * Calculate derived connection features (ratios, rates, etc.)
     * Returns 7 features: [byte_ratio, packet_ratio, throughput, packet_rate,
     *                      avg_orig_pkt_size, avg_resp_pkt_size, symmetry]
     */
    public static double[] derivedConnectionFeatures(
            Long origBytes, Long respBytes,
            Long origPkts, Long respPkts,
            Double duration) {

        double obytes = (origBytes != null) ? origBytes : 0.0;
        double rbytes = (respBytes != null) ? respBytes : 0.0;
        double opkts = (origPkts != null) ? origPkts : 0.0;
        double rpkts = (respPkts != null) ? respPkts : 0.0;
        double dur = (duration != null && duration > 0) ? duration : 0.001;

        double totalBytes = obytes + rbytes;
        double totalPkts = opkts + rpkts;

        return new double[]{
            // Byte ratio (originator's share)
            totalBytes > 0 ? obytes / totalBytes : 0.5,

            // Packet ratio (originator's share)
            totalPkts > 0 ? opkts / totalPkts : 0.5,

            // Throughput (bytes per second) - log scaled
            Math.log1p(totalBytes / dur) / Math.log1p(1e9),

            // Packet rate (packets per second) - log scaled
            Math.log1p(totalPkts / dur) / Math.log1p(1e5),

            // Average packet size originator - log scaled
            opkts > 0 ? Math.log1p(obytes / opkts) / Math.log1p(65536) : 0.0,

            // Average packet size responder - log scaled
            rpkts > 0 ? Math.log1p(rbytes / rpkts) / Math.log1p(65536) : 0.0,

            // Connection symmetry (0 = symmetric, 1 = very asymmetric)
            totalPkts > 0 ? Math.abs(opkts - rpkts) / totalPkts : 0.0
        };
    }

    // ==================== DNS-Specific Features ====================

    /**
     * Calculate Shannon entropy of a string (for DGA domain detection)
     */
    public static double calculateEntropy(String str) {
        if (str == null || str.isEmpty()) return 0.0;

        Map<Character, Integer> freq = new HashMap<>();
        for (char c : str.toCharArray()) {
            freq.put(c, freq.getOrDefault(c, 0) + 1);
        }

        double entropy = 0.0;
        int len = str.length();
        for (int count : freq.values()) {
            double p = (double) count / len;
            entropy -= p * (Math.log(p) / Math.log(2));
        }

        return entropy / 8.0;  // Normalize (max entropy for byte is 8)
    }

    /**
     * Extract features from DNS query string.
     * Returns: [length, subdomain_count, entropy, is_ip_query, hyphen_count]
     */
    public static double[] encodeDNSQuery(String query) {
        if (query == null || query.isEmpty()) {
            return new double[]{0.0, 0.0, 0.0, 0.0, 0.0};
        }

        return new double[]{
            Math.min(query.length(), 255) / 255.0,          // Normalized length
            Math.min(query.split("\\.").length, 10) / 10.0, // Subdomain count (max 10)
            calculateEntropy(query),                         // Domain entropy
            query.matches("^[0-9\\.]+$") ? 1.0 : 0.0,       // Is numeric (reverse DNS)
            Math.min(query.chars().filter(c -> c == '-').count(), 10) / 10.0  // Hyphen count
        };
    }

    /**
     * Encode DNS response code
     */
    public static double encodeDNSRcode(String rcode) {
        if (rcode == null) return 0.0;

        switch (rcode.toUpperCase()) {
            case "NOERROR": return 0.0 / 5.0;
            case "FORMERR": return 1.0 / 5.0;
            case "SERVFAIL": return 2.0 / 5.0;
            case "NXDOMAIN": return 3.0 / 5.0;  // Non-existent domain
            case "NOTIMP": return 4.0 / 5.0;
            case "REFUSED": return 5.0 / 5.0;
            default: return 0.0;
        }
    }

    // ==================== HTTP-Specific Features ====================

    /**
     * Encode HTTP method using one-hot encoding.
     * Returns: [GET, POST, PUT, DELETE, HEAD, OPTIONS, PATCH, CONNECT, OTHER]
     */
    public static double[] encodeHTTPMethod(String method) {
        double[] encoded = new double[9];

        if (method == null) {
            encoded[8] = 1.0;
            return encoded;
        }

        switch (method.toUpperCase()) {
            case "GET": encoded[0] = 1.0; break;
            case "POST": encoded[1] = 1.0; break;
            case "PUT": encoded[2] = 1.0; break;
            case "DELETE": encoded[3] = 1.0; break;
            case "HEAD": encoded[4] = 1.0; break;
            case "OPTIONS": encoded[5] = 1.0; break;
            case "PATCH": encoded[6] = 1.0; break;
            case "CONNECT": encoded[7] = 1.0; break;
            default: encoded[8] = 1.0; break;
        }

        return encoded;
    }

    /**
     * Extract features from HTTP URI.
     * Returns: [length, path_depth, has_params, param_count, entropy, has_suspicious_chars]
     */
    public static double[] encodeURIFeatures(String uri) {
        if (uri == null || uri.isEmpty()) {
            return new double[]{0.0, 0.0, 0.0, 0.0, 0.0, 0.0};
        }

        String[] parts = uri.split("\\?");
        String path = parts[0];

        int paramCount = 0;
        if (parts.length > 1) {
            paramCount = parts[1].split("&").length;
        }

        return new double[]{
            Math.min(uri.length(), 500) / 500.0,            // Normalized length
            Math.min(path.split("/").length, 20) / 20.0,    // Path depth
            parts.length > 1 ? 1.0 : 0.0,                   // Has query params
            Math.min(paramCount, 50) / 50.0,                // Param count
            calculateEntropy(uri),                           // URI entropy
            uri.matches(".*[<>\"'`].*") ? 1.0 : 0.0         // Suspicious XSS chars
        };
    }

    /**
     * Encode HTTP status code with category features.
     * Returns: [normalized_code, is_2xx, is_3xx, is_4xx, is_5xx, is_error]
     */
    public static double[] encodeStatusCode(Integer statusCode) {
        if (statusCode == null) {
            return new double[]{0.0, 0.0, 0.0, 0.0, 0.0, 0.0};
        }

        int code = Math.max(0, Math.min(statusCode, 599));

        return new double[]{
            code / 600.0,                                    // Normalized code
            (code >= 200 && code < 300) ? 1.0 : 0.0,       // Success
            (code >= 300 && code < 400) ? 1.0 : 0.0,       // Redirect
            (code >= 400 && code < 500) ? 1.0 : 0.0,       // Client error
            (code >= 500 && code < 600) ? 1.0 : 0.0,       // Server error
            (code == 404 || code == 403 || code == 401) ? 1.0 : 0.0  // Common attack responses
        };
    }

    // ==================== SSL-Specific Features ====================

    /**
     * Encode SSL/TLS version
     */
    public static double encodeSSLVersion(String version) {
        if (version == null) return 0.0;

        String v = version.toLowerCase();
        if (v.contains("sslv2")) return 1.0 / 6.0;
        if (v.contains("sslv3")) return 2.0 / 6.0;
        if (v.contains("tlsv1.0") || v.contains("tlsv10")) return 3.0 / 6.0;
        if (v.contains("tlsv1.1") || v.contains("tlsv11")) return 4.0 / 6.0;
        if (v.contains("tlsv1.2") || v.contains("tlsv12")) return 5.0 / 6.0;
        if (v.contains("tlsv1.3") || v.contains("tlsv13")) return 6.0 / 6.0;

        return 0.0;
    }

    /**
     * Extract certificate features.
     * Returns: [has_subject, has_issuer, is_self_signed]
     */
    public static double[] encodeSSLCertificate(String subject, String issuer) {
        boolean hasSubject = (subject != null && !subject.isEmpty());
        boolean hasIssuer = (issuer != null && !issuer.isEmpty());
        boolean isSelfSigned = hasSubject && hasIssuer && subject.equals(issuer);

        return new double[]{
            hasSubject ? 1.0 : 0.0,
            hasIssuer ? 1.0 : 0.0,
            isSelfSigned ? 1.0 : 0.0
        };
    }

    /**
     * Safe boolean to double conversion
     */
    public static double booleanToDouble(Boolean value) {
        return (value != null && value) ? 1.0 : 0.0;
    }
}
