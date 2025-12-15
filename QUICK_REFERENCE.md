# Feature Engineering - Quick Reference Card

## 🚨 The Problem

Your current code uses Java `hashCode()` to convert strings to numbers:

```java
// ❌ WRONG - Current code
connLog.getIdOrigH().hashCode()  // IP: "192.168.1.1" → -1234567890
connLog.getProto().hashCode()    // Protocol: "tcp" → 114799
connLog.getService().hashCode()  // Service: "https" → 99162322
```

**Why This Is Bad:**
- Random meaningless numbers
- Hash collisions (different values → same number)
- Not reproducible across systems
- Negative values
- No semantic meaning for ML

---

## ✅ The Solution

Use proper feature encoding:

```java
// ✅ CORRECT - New code
FeatureEncoder.encodeIPAddress("192.168.1.1")     // → [0.753, 0.659, 0.004, 0.004, 1.0]
FeatureEncoder.encodeProtocol("tcp")              // → 0.25
FeatureEncoder.encodeService("https")             // → [0,1,0,0,0,0,0,0,0]
```

**Why This Is Better:**
- All values in [0, 1] range
- Semantic meaning preserved
- Reproducible
- ML-friendly

---

## 📊 Feature Count Changes

| Log Type | Before | After | Increase |
|----------|--------|-------|----------|
| conn.log | 20 | **41** | +105% |
| dns.log  | 24 | **38** | +58% |
| http.log | 28 | **52** | +86% |
| ssl.log  | 19 | **33** | +74% |

---

## 🛠️ Quick Implementation (3 Steps)

### 1. Update LSTM Config

```bash
nano lstm-autoencoder/app/main.py
```

Change:
```python
'conn': {'input_dim': 20, ...}  # OLD
```
To:
```python
'conn': {'input_dim': 41, ...}  # NEW
```

### 2. Rebuild Flink

```bash
cd FlinkKafkaProject
mvn clean package
```

### 3. Restart System

```bash
./stop-system.sh --delete-data
./start-system.sh
```

---

## 📁 New Files Created

✅ **FEATURE_ENGINEERING_ANALYSIS.md** (25KB)
   - Detailed problem analysis
   - Research-backed solutions
   - Code examples for all log types

✅ **FEATURE_DIMENSIONS.md** (12KB)
   - Exact feature counts
   - Dimension specifications
   - Migration checklist

✅ **IMPLEMENTATION_GUIDE.md** (18KB)
   - Step-by-step instructions
   - Troubleshooting guide
   - Verification checklist

✅ **FeatureEncoder.java** (25KB)
   - All encoding functions
   - Production-ready code
   - Fully documented

✅ **UnsupervisedFlinkKafkaConsumerConnImproved.java** (17KB)
   - Improved conn consumer
   - 41 features instead of 20
   - Drop-in replacement

---

## 🔍 Encoding Methods Reference

### IP Address
```java
FeatureEncoder.encodeIPAddress(String ip)
// Returns: [octet1/255, octet2/255, octet3/255, octet4/255, isPrivate]
// Example: "192.168.1.100" → [0.753, 0.659, 0.004, 0.392, 1.0]
```

### Protocol
```java
FeatureEncoder.encodeProtocol(String proto)
// Returns: normalized value [0, 1]
// tcp=0.25, udp=0.50, icmp=0.75, icmp6=1.0
```

### Service (One-Hot)
```java
FeatureEncoder.encodeService(String service)
// Returns: [http, https, dns, ssh, ftp, smtp, pop3, imap, other]
// Example: "https" → [0, 1, 0, 0, 0, 0, 0, 0, 0]
```

### Connection State
```java
FeatureEncoder.encodeConnState(String state)
// Returns: normalized value [0, 1]
// S0=0.077, S1=0.154, SF=0.231, REJ=0.308, ...
```

### Normalization
```java
FeatureEncoder.normalizePort(Integer port)        // port/65535
FeatureEncoder.normalizeBytes(Long bytes)         // log-scaled
FeatureEncoder.normalizeDuration(Double seconds)  // log-scaled
FeatureEncoder.normalizePackets(Long packets)     // log-scaled
```

### Derived Features
```java
FeatureEncoder.derivedConnectionFeatures(origBytes, respBytes, origPkts, respPkts, duration)
// Returns 7 features: [byte_ratio, packet_ratio, throughput, packet_rate,
//                      avg_orig_pkt_size, avg_resp_pkt_size, symmetry]
```

### Temporal
```java
FeatureEncoder.encodeTimestamp(double timestamp)
// Returns: [hour/24, dayOfWeek/7, minute/60, isNight]
```

### DNS-Specific
```java
FeatureEncoder.calculateEntropy(String str)      // Shannon entropy
FeatureEncoder.encodeDNSQuery(String query)      // 5 features
FeatureEncoder.encodeDNSRcode(String rcode)      // normalized
```

### HTTP-Specific
```java
FeatureEncoder.encodeHTTPMethod(String method)   // 9 features (one-hot)
FeatureEncoder.encodeURIFeatures(String uri)     // 6 features
FeatureEncoder.encodeStatusCode(Integer code)    // 6 features
```

### SSL-Specific
```java
FeatureEncoder.encodeSSLVersion(String version)        // normalized
FeatureEncoder.encodeSSLCertificate(subject, issuer)   // 3 features
```

---

## 🎯 Connection Log Features (41 Total)

| Index | Feature | Type | Encoding |
|-------|---------|------|----------|
| 1-5 | Source IP | 5 floats | 4 octets + private flag |
| 6-10 | Dest IP | 5 floats | 4 octets + private flag |
| 11-12 | Ports | 2 floats | Normalized [0,1] |
| 13 | Protocol | 1 float | Label encoding |
| 14-22 | Service | 9 floats | One-hot encoding |
| 23 | Conn State | 1 float | Label encoding |
| 24-27 | Temporal | 4 floats | Hour, day, minute, night |
| 28 | Duration | 1 float | Log-scaled |
| 29-30 | Bytes | 2 floats | Log-scaled |
| 31-32 | Packets | 2 floats | Log-scaled |
| 33-39 | Derived | 7 floats | Ratios & rates |
| 40-41 | Flags | 2 floats | Boolean flags |

---

## 🧪 Testing

### Test Feature Encoding
```bash
# Check if FeatureEncoder compiles
cd FlinkKafkaProject
mvn compile
```

### Test Feature Dimensions
```bash
# After deployment, check logs
docker logs flink 2>&1 | grep "Feature count"
# Should show: "Feature count: 41"
```

### Test LSTM Config
```bash
# Check current config
docker exec lstm-autoencoder curl -s http://localhost:5000/models/status | jq '.log_types.conn.config.input_dim'
# Should return: 41
```

### Test Data Collection
```bash
# Enable learning
./lstm-control.sh enable-learn conn

# Check status (wait 30 seconds)
./lstm-control.sh status conn

# Should show increasing data_size
```

---

## 📚 Read Full Documentation

1. **Problem Analysis**: `cat FEATURE_ENGINEERING_ANALYSIS.md`
2. **Feature Specs**: `cat FEATURE_DIMENSIONS.md`
3. **Step-by-Step**: `cat IMPLEMENTATION_GUIDE.md`

---

## ⚠️ Important Notes

### Breaking Change
- Old models (20 features) **cannot** process new data (41 features)
- Must delete all old data with `--delete-data` flag
- Must retrain from scratch

### Migration Required
```bash
# Full clean restart
./stop-system.sh --delete-data
# Update configs (see IMPLEMENTATION_GUIDE.md)
./start-system.sh
./lstm-control.sh enable-learn-all
```

### Verification
After implementation:
- ✅ No hash codes in logs
- ✅ All features in [0, 1]
- ✅ Feature count = 41
- ✅ No "mismatch" errors

---

## 🔗 Sources

Based on peer-reviewed research:
- IEEE: [Machine Learning Techniques for Zeek Log Analysis](https://ieeexplore.ieee.org/document/9660501/)
- MDPI: [Extended Isolation Forest for Intrusion Detection](https://www.mdpi.com/2078-2489/15/7/404)
- Springer: [Analysis of Network Traffic Features](https://link.springer.com/article/10.1007/s10994-014-5473-9)
- Purdue: [Encoding IP Address for Network IDS](https://hammer.purdue.edu/articles/thesis/Encoding_IP_Address_as_a_Feature_for_Network_Intrusion_Detection/11307287)

---

## 💡 Key Takeaways

1. **hashCode() is NOT for ML** - It's for hash tables, not machine learning
2. **Encoding matters** - Proper encoding can improve accuracy by 20-50%
3. **Domain knowledge** - Use security/network knowledge in feature design
4. **Normalization** - Keep all features in similar ranges [0, 1]
5. **Derived features** - Calculate ratios, rates, patterns from raw data

---

## 📞 Quick Help

**Problem**: Feature count mismatch
**Solution**: Rebuild JARs with `mvn clean package`

**Problem**: Training fails
**Solution**: Check data collection with `./lstm-control.sh status conn`

**Problem**: All anomaly scores are -1.0
**Solution**: Disable learning mode with `./lstm-control.sh disable-learn conn`

**Problem**: Old data incompatible
**Solution**: Clear with `./stop-system.sh --delete-data`

---

✅ **Ready to implement?** Start with `IMPLEMENTATION_GUIDE.md` Step 1.
