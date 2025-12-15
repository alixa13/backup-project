# Zeek Logs Feature Engineering - Analysis & Best Practices

## 🔴 Critical Issues with Current Implementation

### Problem 1: Using Java hashCode() for Categorical Features
**Current Code (UnsupervisedFlinkKafkaConsumerConn.java:121-158):**
```java
// BAD: Using hashCode() for encoding
connLog.getIdOrigH() != null ? (double) connLog.getIdOrigH().hashCode() : 0.0,
connLog.getProto() != null ? (double) connLog.getProto().hashCode() : 0.0,
connLog.getConnState() != null ? (double) connLog.getConnState().hashCode() : 0.0,
```

**Why This Is Wrong:**
1. **No Semantic Meaning**: Hash codes are arbitrary integers with no relationship to the actual value
2. **Hash Collisions**: Different values can produce the same hash code
3. **Not Reproducible**: Hash codes can vary across JVM instances/versions
4. **Negative Values**: String hashCode() can be negative, breaking ML assumptions
5. **No Ordinality**: The model cannot learn meaningful patterns from random numbers
6. **High Cardinality**: IP addresses have millions of possible hash values, creating sparse features

### Problem 2: Missing Temporal Features
- No time-based aggregations (packets per second, connections per minute)
- No behavioral patterns over time windows
- Timestamp used as-is without extracting hour/day patterns

### Problem 3: Missing Derived Features
- No ratios (e.g., orig_bytes/resp_bytes, orig_pkts/resp_pkts)
- No connection duration patterns
- No entropy calculations for strings (domain names, URIs)
- No length-based features (query length, URI length)

### Problem 4: No Feature Scaling/Normalization
- Byte counts range from 0 to billions
- Packet counts range from 0 to thousands
- Duration ranges from milliseconds to hours
- Mixed scales confuse neural networks (LSTM)

---

## ✅ Best Practices for Zeek Log Feature Engineering

Based on research from IEEE, MDPI, and academic papers on network anomaly detection:

### 1. IP Address Encoding

**❌ DON'T:**
```java
// BAD: Hash code encoding
ip.hashCode()
```

**✅ DO:**

**Option A: Network-Based Encoding (Recommended)**
```java
// Convert IP to 4 separate normalized features
private static double[] encodeIPAddress(String ip) {
    if (ip == null) return new double[]{0, 0, 0, 0};
    String[] parts = ip.split("\\.");
    if (parts.length != 4) return new double[]{0, 0, 0, 0};

    return new double[] {
        Integer.parseInt(parts[0]) / 255.0,  // Normalize to [0, 1]
        Integer.parseInt(parts[1]) / 255.0,
        Integer.parseInt(parts[2]) / 255.0,
        Integer.parseInt(parts[3]) / 255.0
    };
}
```

**Option B: Subnet-Based Features**
```java
private static double[] encodeIPFeatures(String ip) {
    if (ip == null) return new double[]{0, 0, 0, 0, 0};

    String[] parts = ip.split("\\.");
    if (parts.length != 4) return new double[]{0, 0, 0, 0, 0};

    int[] octets = Arrays.stream(parts).mapToInt(Integer::parseInt).toArray();

    return new double[] {
        octets[0] / 255.0,           // First octet
        octets[1] / 255.0,           // Second octet
        octets[2] / 255.0,           // Third octet
        octets[3] / 255.0,           // Fourth octet
        isPrivateIP(octets) ? 1.0 : 0.0  // Private IP flag
    };
}

private static boolean isPrivateIP(int[] octets) {
    return (octets[0] == 10) ||
           (octets[0] == 172 && octets[1] >= 16 && octets[1] <= 31) ||
           (octets[0] == 192 && octets[1] == 168);
}
```

**Option C: Frequency-Based Encoding (For High Cardinality)**
```java
// Track IP frequency during processing window
private static Map<String, Integer> ipFrequency = new ConcurrentHashMap<>();
private static double encodeIPFrequency(String ip) {
    return Math.log1p(ipFrequency.getOrDefault(ip, 0));
}
```

### 2. Protocol Encoding

**❌ DON'T:**
```java
proto.hashCode()  // Random number
```

**✅ DO:**
```java
private static double encodeProtocol(String proto) {
    if (proto == null) return 0.0;

    switch (proto.toLowerCase()) {
        case "tcp": return 1.0;
        case "udp": return 2.0;
        case "icmp": return 3.0;
        case "icmp6": return 4.0;
        default: return 0.0;  // Unknown
    }
}
```

### 3. Service Encoding (One-Hot or Label)

**✅ One-Hot Encoding (Preferred for <20 categories):**
```java
private static double[] encodeServiceOneHot(String service) {
    // Common services: http, https, dns, ssh, ftp, smtp, pop3, imap, other
    double[] encoded = new double[9];

    if (service == null) {
        encoded[8] = 1.0;  // "other"
        return encoded;
    }

    switch (service.toLowerCase()) {
        case "http": encoded[0] = 1.0; break;
        case "https": encoded[1] = 1.0; break;
        case "dns": encoded[2] = 1.0; break;
        case "ssh": encoded[3] = 1.0; break;
        case "ftp": encoded[4] = 1.0; break;
        case "smtp": encoded[5] = 1.0; break;
        case "pop3": encoded[6] = 1.0; break;
        case "imap": encoded[7] = 1.0; break;
        default: encoded[8] = 1.0; break;  // "other"
    }

    return encoded;
}
```

### 4. Connection State Encoding

**✅ Label Encoding (Connection states have some ordinality):**
```java
private static double encodeConnState(String connState) {
    if (connState == null) return 0.0;

    // States ordered by completeness/success
    switch (connState) {
        case "S0": return 1.0;   // Connection attempt seen, no reply
        case "S1": return 2.0;   // Connection established, not terminated
        case "SF": return 3.0;   // Normal establishment and termination
        case "REJ": return 4.0;  // Connection attempt rejected
        case "S2": return 5.0;   // Connection established, originator aborted
        case "S3": return 6.0;   // Connection established, responder aborted
        case "RSTO": return 7.0; // Originator sent RST
        case "RSTR": return 8.0; // Responder sent RST
        case "RSTOS0": return 9.0;
        case "RSTRH": return 10.0;
        case "SH": return 11.0;
        case "SHR": return 12.0;
        case "OTH": return 13.0;
        default: return 0.0;
    }
}
```

### 5. History String Encoding (TCP Flags)

**✅ Feature Decomposition:**
```java
private static double[] encodeHistory(String history) {
    if (history == null || history.isEmpty()) {
        return new double[]{0, 0, 0, 0, 0, 0};
    }

    return new double[] {
        history.length(),                              // Number of packets
        history.chars().filter(c -> c == 'S').count(), // SYN count
        history.chars().filter(c -> c == 'A').count(), // ACK count
        history.chars().filter(c -> c == 'F').count(), // FIN count
        history.chars().filter(c -> c == 'R').count(), // RST count
        history.chars().filter(c -> c == 'D').count()  // Data count
    };
}
```

### 6. Temporal Features

**✅ Extract Time Patterns:**
```java
private static double[] encodeTimestamp(double timestamp) {
    Instant instant = Instant.ofEpochSecond((long) timestamp);
    ZonedDateTime zdt = instant.atZone(ZoneId.systemDefault());

    return new double[] {
        zdt.getHour() / 24.0,                    // Hour of day (normalized)
        zdt.getDayOfWeek().getValue() / 7.0,    // Day of week (normalized)
        zdt.getMinute() / 60.0,                  // Minute of hour (normalized)
        (zdt.getHour() >= 22 || zdt.getHour() <= 6) ? 1.0 : 0.0  // Night hours flag
    };
}
```

### 7. Derived Statistical Features

**✅ Calculate Ratios and Patterns:**
```java
private static double[] derivedConnectionFeatures(
    Long origBytes, Long respBytes,
    Long origPkts, Long respPkts,
    Double duration) {

    // Avoid division by zero
    double obytes = (origBytes != null) ? origBytes : 0.0;
    double rbytes = (respBytes != null) ? respBytes : 0.0;
    double opkts = (origPkts != null) ? origPkts : 0.0;
    double rpkts = (respPkts != null) ? respPkts : 0.0;
    double dur = (duration != null && duration > 0) ? duration : 0.001;

    return new double[] {
        // Byte ratios
        (obytes + rbytes > 0) ? obytes / (obytes + rbytes) : 0.5,

        // Packet ratios
        (opkts + rpkts > 0) ? opkts / (opkts + rpkts) : 0.5,

        // Throughput (bytes per second)
        (obytes + rbytes) / dur,

        // Packet rate (packets per second)
        (opkts + rpkts) / dur,

        // Average packet size
        (opkts > 0) ? obytes / opkts : 0.0,
        (rpkts > 0) ? rbytes / rpkts : 0.0,

        // Symmetry (difference in packet counts)
        Math.abs(opkts - rpkts) / Math.max(opkts + rpkts, 1.0)
    };
}
```

### 8. Normalization and Scaling

**✅ Log Scaling for Skewed Features:**
```java
private static double normalizeBytes(Long bytes) {
    if (bytes == null || bytes == 0) return 0.0;
    return Math.log1p(bytes) / Math.log1p(1e12);  // Normalize to ~[0, 1]
}

private static double normalizeDuration(Double duration) {
    if (duration == null || duration == 0) return 0.0;
    return Math.log1p(duration) / Math.log1p(86400);  // Max 1 day
}

private static double normalizePackets(Long packets) {
    if (packets == null || packets == 0) return 0.0;
    return Math.log1p(packets) / Math.log1p(1e6);  // Normalize to ~[0, 1]
}
```

---

## 📊 DNS Log Feature Engineering

### String Entropy (for detecting DGA domains)

```java
private static double calculateEntropy(String str) {
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

    return entropy;
}
```

### DNS Query Features

```java
private static double[] encodeDNSQuery(String query) {
    if (query == null || query.isEmpty()) {
        return new double[]{0, 0, 0, 0, 0};
    }

    return new double[] {
        query.length() / 100.0,                          // Normalized length
        query.split("\\.").length,                       // Subdomain count
        calculateEntropy(query),                         // Domain entropy
        query.matches("^[0-9\\.]+$") ? 1.0 : 0.0,       // Is IP query
        query.chars().filter(c -> c == '-').count()      // Hyphen count (suspicious)
    };
}
```

### DNS Response Codes

```java
private static double encodeRcode(String rcode) {
    if (rcode == null) return 0.0;

    switch (rcode.toUpperCase()) {
        case "NOERROR": return 0.0;
        case "FORMERR": return 1.0;
        case "SERVFAIL": return 2.0;
        case "NXDOMAIN": return 3.0;  // Non-existent domain (suspicious)
        case "NOTIMP": return 4.0;
        case "REFUSED": return 5.0;
        default: return -1.0;
    }
}
```

---

## 🌐 HTTP Log Feature Engineering

### HTTP Method Encoding

```java
private static double[] encodeHTTPMethod(String method) {
    double[] encoded = new double[9];  // GET, POST, PUT, DELETE, HEAD, OPTIONS, PATCH, CONNECT, OTHER

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
```

### URI Features

```java
private static double[] encodeURIFeatures(String uri) {
    if (uri == null || uri.isEmpty()) {
        return new double[]{0, 0, 0, 0, 0, 0};
    }

    return new double[] {
        uri.length() / 200.0,                            // Normalized length
        uri.split("/").length,                           // Path depth
        uri.contains("?") ? 1.0 : 0.0,                  // Has query params
        uri.split("\\?").length > 1 ?
            uri.split("\\?")[1].split("&").length : 0,  // Param count
        calculateEntropy(uri),                           // URI entropy
        uri.matches(".*[<>\"'].*") ? 1.0 : 0.0          // Suspicious chars (XSS)
    };
}
```

### Status Code Encoding

```java
private static double[] encodeStatusCode(Integer statusCode) {
    if (statusCode == null) {
        return new double[]{0, 0, 0, 0, 0, 0};
    }

    return new double[] {
        statusCode / 600.0,                              // Normalized code
        (statusCode >= 200 && statusCode < 300) ? 1.0 : 0.0,  // Success
        (statusCode >= 300 && statusCode < 400) ? 1.0 : 0.0,  // Redirect
        (statusCode >= 400 && statusCode < 500) ? 1.0 : 0.0,  // Client error
        (statusCode >= 500 && statusCode < 600) ? 1.0 : 0.0,  // Server error
        (statusCode == 404 || statusCode == 403) ? 1.0 : 0.0  // Common attack responses
    };
}
```

### User Agent Entropy

```java
private static double[] encodeUserAgent(String userAgent) {
    if (userAgent == null || userAgent.isEmpty()) {
        return new double[]{0, 0, 0};
    }

    return new double[] {
        userAgent.length() / 200.0,
        calculateEntropy(userAgent),
        userAgent.toLowerCase().contains("bot") ? 1.0 : 0.0
    };
}
```

---

## 🔐 SSL Log Feature Engineering

### Certificate Validity

```java
private static double[] encodeSSLCert(String subject, String issuer) {
    return new double[] {
        (subject != null && !subject.isEmpty()) ? 1.0 : 0.0,
        (issuer != null && !issuer.isEmpty()) ? 1.0 : 0.0,
        (subject != null && issuer != null && subject.equals(issuer)) ? 1.0 : 0.0  // Self-signed
    };
}
```

### SSL Version Encoding

```java
private static double encodeSSLVersion(String version) {
    if (version == null) return 0.0;

    if (version.contains("SSLv2")) return 1.0;
    if (version.contains("SSLv3")) return 2.0;
    if (version.contains("TLSv1.0")) return 3.0;
    if (version.contains("TLSv1.1")) return 4.0;
    if (version.contains("TLSv1.2")) return 5.0;
    if (version.contains("TLSv1.3")) return 6.0;

    return 0.0;
}
```

---

## 🎯 Recommended Feature Set for conn.log

### Updated Feature Vector (35 features instead of 20)

```java
List<Double> features = new ArrayList<>();

// 1-4: Source IP (4 features)
features.addAll(Arrays.stream(encodeIPAddress(connLog.getIdOrigH())).boxed().collect(Collectors.toList()));

// 5-8: Destination IP (4 features)
features.addAll(Arrays.stream(encodeIPAddress(connLog.getIdRespH())).boxed().collect(Collectors.toList()));

// 9-10: Ports (normalized)
features.add(connLog.getIdOrigP() / 65535.0);
features.add(connLog.getIdRespP() / 65535.0);

// 11: Protocol
features.add(encodeProtocol(connLog.getProto()));

// 12-20: Service (one-hot, 9 features)
features.addAll(Arrays.stream(encodeServiceOneHot(connLog.getService())).boxed().collect(Collectors.toList()));

// 21: Connection state
features.add(encodeConnState(connLog.getConnState()));

// 22: Duration (log-scaled)
features.add(normalizeDuration(connLog.getDuration()));

// 23-24: Bytes (log-scaled)
features.add(normalizeBytes(connLog.getOrigBytes()));
features.add(normalizeBytes(connLog.getRespBytes()));

// 25-26: Packets (log-scaled)
features.add(normalizePackets(connLog.getOrigPkts()));
features.add(normalizePackets(connLog.getRespPkts()));

// 27-33: Derived features (7 features)
features.addAll(Arrays.stream(derivedConnectionFeatures(
    connLog.getOrigBytes(), connLog.getRespBytes(),
    connLog.getOrigPkts(), connLog.getRespPkts(),
    connLog.getDuration()
)).boxed().collect(Collectors.toList()));

// 34: Local originator flag
features.add(connLog.getLocalOrig() != null && connLog.getLocalOrig() ? 1.0 : 0.0);

// 35: Missed bytes flag
features.add(connLog.getMissedBytes() != null && connLog.getMissedBytes() > 0 ? 1.0 : 0.0);

// Convert to array
double[] featureArray = features.stream().mapToDouble(Double::doubleValue).toArray();
```

---

## 📚 References & Sources

### Research Papers
1. [Comparing Machine Learning Techniques for Zeek Log Analysis (IEEE)](https://ieeexplore.ieee.org/document/9660501/)
2. [Extended Isolation Forest for Intrusion Detection in Zeek Data (MDPI 2024)](https://www.mdpi.com/2078-2489/15/7/404)
3. [Analysis of network traffic features for anomaly detection (Springer)](https://link.springer.com/article/10.1007/s10994-014-5473-9)
4. [Encoding IP Address as a Feature for Network Intrusion Detection (Purdue)](https://hammer.purdue.edu/articles/thesis/Encoding_IP_Address_as_a_Feature_for_Network_Intrusion_Detection/11307287)

### Feature Engineering Resources
5. [ENCODE: Encoding NetFlows for Network Anomaly Detection (arXiv)](https://arxiv.org/html/2207.03890v3)
6. [A Local Feature Engineering Strategy to Improve Network Anomaly Detection (MDPI)](https://www.mdpi.com/1999-5903/12/10/177)
7. [Design and Implementation of an Anomaly Network Traffic Detection Model (Wiley)](https://onlinelibrary.wiley.com/doi/10.1155/2021/7045823)

### Zeek Documentation
8. [Zeek conn.log Documentation](https://docs.zeek.org/en/master/logs/conn.html)
9. [Zeek dns.log Documentation](https://docs.zeek.org/en/master/logs/dns.html)
10. [Zeek http.log Documentation](https://docs.zeek.org/en/master/logs/http.html)
11. [Zeek ssl.log Documentation](https://docs.zeek.org/en/master/logs/ssl.html)

### Machine Learning Resources
12. [SecurityNik: Beginning Machine Learning with Zeek logs (GitHub)](https://github.com/SecurityNik/Data-Science-and-ML/blob/main/Beginning Machine and Deep Learning with Zeek logs/08 - beginning Machine Learning Anomaly Detection - isolation forest and local outlier factor.ipynb)
13. [Stratosphere IPS: Zeek Anomaly Detector (GitHub)](https://github.com/stratosphereips/zeek_anomaly_detector)

---

## 🔧 Implementation Priority

### Phase 1: Critical Fixes (Immediate)
1. ✅ Replace hashCode() with proper encoding for:
   - IP addresses → 4 normalized octets
   - Protocol → Label encoding (tcp=1, udp=2, icmp=3)
   - Connection state → Label encoding
   - Service → One-hot encoding

2. ✅ Add log-scaling for:
   - Byte counts
   - Packet counts
   - Duration

### Phase 2: Enhanced Features (Week 2)
3. ✅ Add derived features:
   - Byte ratios
   - Packet ratios
   - Throughput
   - Packet rate
   - Average packet size

4. ✅ Add temporal features:
   - Hour of day
   - Day of week
   - Night hours flag

### Phase 3: Protocol-Specific (Week 3)
5. ✅ DNS-specific features:
   - Query entropy
   - Domain length
   - Subdomain count

6. ✅ HTTP-specific features:
   - URI entropy
   - Status code categories
   - Method encoding

7. ✅ SSL-specific features:
   - Certificate validity
   - Self-signed flag
   - Version encoding

---

## 🚨 Common Pitfalls to Avoid

1. ❌ **Don't use hashCode()** - It's non-deterministic and meaningless
2. ❌ **Don't ignore feature scaling** - Neural networks need normalized inputs
3. ❌ **Don't one-hot encode high cardinality** - IPs have millions of values
4. ❌ **Don't treat all features equally** - Use domain knowledge
5. ❌ **Don't forget null handling** - Zeek logs have many missing values
6. ❌ **Don't use raw timestamps** - Extract time patterns instead
7. ❌ **Don't ignore data distribution** - Log-scale skewed features
8. ❌ **Don't hardcode thresholds** - Make them configurable

---

## ✅ Summary

The current implementation has **fundamental flaws** in feature encoding that will severely impact model performance. The use of `hashCode()` for categorical features is **not a valid ML practice** and should be replaced immediately with proper encoding techniques:

- **IP Addresses**: 4 normalized octets (0-1 range)
- **Protocols**: Label encoding (1-4)
- **Services**: One-hot encoding (<10 categories) or frequency encoding
- **Connection States**: Label encoding with semantic ordering
- **Numerical Features**: Log-scaling and normalization
- **Derived Features**: Ratios, rates, and statistical measures

This will improve model accuracy, training stability, and detection performance.
