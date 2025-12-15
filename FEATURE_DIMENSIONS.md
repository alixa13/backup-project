# Feature Dimensions Configuration

## Overview

This document specifies the exact feature counts and structures for each log type after implementing proper feature engineering (replacing hashCode() approach).

---

## Connection Logs (conn.log)

### Total Features: 41

| Feature Index | Feature Name | Type | Range | Description |
|---------------|--------------|------|-------|-------------|
| 1-5 | Source IP | 5 floats | [0, 1] | 4 normalized octets + private IP flag |
| 6-10 | Destination IP | 5 floats | [0, 1] | 4 normalized octets + private IP flag |
| 11 | Source Port | float | [0, 1] | Normalized port number |
| 12 | Destination Port | float | [0, 1] | Normalized port number |
| 13 | Protocol | float | [0, 1] | Label encoded (tcp/udp/icmp/icmp6) |
| 14-22 | Service | 9 floats | {0, 1} | One-hot (http/https/dns/ssh/ftp/smtp/pop3/imap/other) |
| 23 | Connection State | float | [0, 1] | Label encoded (S0/S1/SF/REJ/...) |
| 24-27 | Temporal | 4 floats | [0, 1] | Hour, day of week, minute, night flag |
| 28 | Duration | float | [0, 1] | Log-scaled and normalized |
| 29 | Originator Bytes | float | [0, 1] | Log-scaled and normalized |
| 30 | Responder Bytes | float | [0, 1] | Log-scaled and normalized |
| 31 | Originator Packets | float | [0, 1] | Log-scaled and normalized |
| 32 | Responder Packets | float | [0, 1] | Log-scaled and normalized |
| 33 | Byte Ratio | float | [0, 1] | orig_bytes / total_bytes |
| 34 | Packet Ratio | float | [0, 1] | orig_pkts / total_pkts |
| 35 | Throughput | float | [0, 1] | Log-scaled bytes/second |
| 36 | Packet Rate | float | [0, 1] | Log-scaled packets/second |
| 37 | Avg Orig Pkt Size | float | [0, 1] | Log-scaled average packet size |
| 38 | Avg Resp Pkt Size | float | [0, 1] | Log-scaled average packet size |
| 39 | Symmetry | float | [0, 1] | Connection symmetry metric |
| 40 | Local Originator | float | {0, 1} | Boolean flag |
| 41 | Has Missed Bytes | float | {0, 1} | Boolean flag |

### Previous Implementation
- **Old features**: 20 (using hashCode())
- **New features**: 41 (proper encoding)
- **Improvement**: 2.05x more meaningful features

---

## DNS Logs (dns.log)

### Total Features: 38 (Estimated)

| Feature Group | Count | Description |
|---------------|-------|-------------|
| Source IP | 5 | 4 octets + private flag |
| Destination IP | 5 | 4 octets + private flag |
| Ports | 2 | Normalized source/dest ports |
| Protocol | 1 | Label encoded |
| Temporal | 4 | Hour, day, minute, night flag |
| DNS Query | 5 | Length, subdomain count, entropy, is_ip, hyphens |
| Query Type | 1 | Normalized qtype value |
| Response Code | 1 | Label encoded (NOERROR/NXDOMAIN/etc) |
| RTT | 1 | Log-scaled round trip time |
| DNS Flags | 5 | AA, TC, RD, RA, Z (boolean) |
| Answer Count | 1 | Number of answers (normalized) |
| TTL Stats | 3 | Min/Max/Avg TTL (log-scaled) |
| Rejected Flag | 1 | Boolean |
| Trans ID | 1 | Normalized transaction ID |
| Derived | 3 | Custom ratios/metrics |

### Previous Implementation
- **Old features**: 24 (using hashCode())
- **New features**: 38 (proper encoding)
- **Improvement**: 1.58x more meaningful features

---

## HTTP Logs (http.log)

### Total Features: 52 (Estimated)

| Feature Group | Count | Description |
|---------------|-------|-------------|
| Source IP | 5 | 4 octets + private flag |
| Destination IP | 5 | 4 octets + private flag |
| Ports | 2 | Normalized source/dest ports |
| Temporal | 4 | Hour, day, minute, night flag |
| HTTP Method | 9 | One-hot (GET/POST/PUT/DELETE/HEAD/OPTIONS/PATCH/CONNECT/OTHER) |
| URI Features | 6 | Length, depth, params, param_count, entropy, suspicious |
| Status Code | 6 | Normalized code + category flags (2xx/3xx/4xx/5xx/error) |
| Request Body Len | 1 | Log-scaled |
| Response Body Len | 1 | Log-scaled |
| Trans Depth | 1 | Normalized |
| User Agent | 3 | Length, entropy, is_bot |
| Referrer | 2 | Has referrer, referrer entropy |
| Host Entropy | 1 | Domain entropy |
| Derived | 6 | Request/response ratios, timing metrics |

### Previous Implementation
- **Old features**: 28 (using hashCode())
- **New features**: 52 (proper encoding)
- **Improvement**: 1.86x more meaningful features

---

## SSL Logs (ssl.log)

### Total Features: 33 (Estimated)

| Feature Group | Count | Description |
|---------------|-------|-------------|
| Source IP | 5 | 4 octets + private flag |
| Destination IP | 5 | 4 octets + private flag |
| Ports | 2 | Normalized source/dest ports |
| Temporal | 4 | Hour, day, minute, night flag |
| SSL Version | 1 | Label encoded (SSLv2/SSLv3/TLS1.0/1.1/1.2/1.3) |
| Certificate | 3 | Has subject, has issuer, is self-signed |
| Server Name | 2 | Length, entropy |
| Established | 1 | Boolean |
| Cipher Suite | 1 | Frequency-based encoding |
| Curve | 1 | Frequency-based encoding |
| Session ID | 2 | Has session ID, ID length |
| Last Alert | 1 | Alert type encoded |
| Derived | 5 | Connection quality metrics |

### Previous Implementation
- **Old features**: 19 (using hashCode())
- **New features**: 33 (proper encoding)
- **Improvement**: 1.74x more meaningful features

---

## Summary Comparison

| Log Type | Old Features | New Features | Improvement | Complexity |
|----------|--------------|--------------|-------------|------------|
| conn.log | 20 | 41 | +105% | Medium |
| dns.log | 24 | 38 | +58% | Medium |
| http.log | 28 | 52 | +86% | High |
| ssl.log | 19 | 33 | +74% | Medium |

---

## Update Required: LSTM Configuration

### Current Configuration (lstm-autoencoder/app/main.py)

```python
LOG_TYPES = {
    'http': {
        'input_dim': 28,  # ❌ OLD
        'timesteps': 24,
        'encoding_dim': 64
    },
    'ssl': {
        'input_dim': 19,  # ❌ OLD
        'timesteps': 24,
        'encoding_dim': 64
    },
    'dns': {
        'input_dim': 24,  # ❌ OLD
        'timesteps': 24,
        'encoding_dim': 64
    },
    'conn': {
        'input_dim': 20,  # ❌ OLD
        'timesteps': 24,
        'encoding_dim': 64
    }
}
```

### New Configuration (REQUIRED UPDATE)

```python
LOG_TYPES = {
    'http': {
        'input_dim': 52,  # ✅ NEW (improved)
        'timesteps': 24,
        'encoding_dim': 128  # Increased for higher dimensionality
    },
    'ssl': {
        'input_dim': 33,  # ✅ NEW (improved)
        'timesteps': 24,
        'encoding_dim': 96
    },
    'dns': {
        'input_dim': 38,  # ✅ NEW (improved)
        'timesteps': 24,
        'encoding_dim': 96
    },
    'conn': {
        'input_dim': 41,  # ✅ NEW (improved)
        'timesteps': 24,
        'encoding_dim': 96
    }
}
```

### Encoding Dimension Guidelines

- **conn (41 features)**: encoding_dim = 96 (2.3x compression)
- **dns (38 features)**: encoding_dim = 96 (2.5x compression)
- **ssl (33 features)**: encoding_dim = 96 (2.9x compression)
- **http (52 features)**: encoding_dim = 128 (2.5x compression)

Compression ratio should be between 2-4x for optimal reconstruction error detection.

---

## Database Schema Update

### PostgreSQL Expected Bytes

Each feature is stored as float64 (8 bytes):

- **conn**: 41 features × 8 bytes = 328 bytes per row
- **dns**: 38 features × 8 bytes = 304 bytes per row
- **http**: 52 features × 8 bytes = 416 bytes per row
- **ssl**: 33 features × 8 bytes = 264 bytes per row

Update validation in `database.py`:

```python
expected_features = {
    'http': 52,  # Updated from 28
    'ssl': 33,   # Updated from 19
    'dns': 38,   # Updated from 24
    'conn': 41   # Updated from 20
}
```

---

## Migration Strategy

### Phase 1: Deploy FeatureEncoder (✅ Complete)
- Created `FeatureEncoder.java` utility class
- All encoding functions implemented

### Phase 2: Update Consumers
- ✅ Created `UnsupervisedFlinkKafkaConsumerConnImproved.java`
- ⏳ TODO: Create improved versions for DNS, HTTP, SSL

### Phase 3: Update LSTM API
- ⏳ TODO: Update `lstm-autoencoder/app/main.py` with new feature counts
- ⏳ TODO: Update `lstm-autoencoder/app/database.py` validation

### Phase 4: Rebuild & Test
- ⏳ TODO: Rebuild Flink JARs with `mvn clean package`
- ⏳ TODO: Clear existing training data (incompatible feature dimensions)
- ⏳ TODO: Restart system and begin new learning phase

### Phase 5: Model Retraining
- ⏳ TODO: Collect new training data (20,000+ rows per log type)
- ⏳ TODO: Train new models with updated dimensions
- ⏳ TODO: Validate anomaly detection performance

---

## Breaking Changes

⚠️ **WARNING**: This is a **BREAKING CHANGE**

1. **Models are incompatible**: Old models trained on 20 features cannot process 41 features
2. **Data is incompatible**: Old training data (20 features) cannot be mixed with new data (41 features)
3. **Migration required**: Must clear database and retrain from scratch

### Migration Commands

```bash
# Stop the system
./stop-system.sh

# Delete all collected data
./stop-system.sh --delete-data

# Update LSTM configuration
nano lstm-autoencoder/app/main.py
# (Update LOG_TYPES configuration as shown above)

# Rebuild Flink JARs
cd FlinkKafkaProject
mvn clean package
cd ..

# Start fresh system
./start-system.sh

# Begin new learning phase
./lstm-control.sh enable-learn-all
```

---

## Validation Checklist

- [ ] FeatureEncoder.java compiled successfully
- [ ] All Flink consumers updated and compiled
- [ ] LSTM app/main.py updated with new feature counts
- [ ] LSTM app/database.py updated with new validation
- [ ] PostgreSQL data cleared
- [ ] New JARs deployed to flink/jars/
- [ ] System restarted successfully
- [ ] Learning mode enabled for all log types
- [ ] 20,000+ rows collected per log type
- [ ] Models trained successfully
- [ ] Anomaly detection working
- [ ] Performance metrics acceptable

---

## Expected Benefits

### 1. Better Anomaly Detection
- Proper encoding preserves semantic meaning
- Derived features capture behavioral patterns
- Temporal features detect time-based anomalies

### 2. Improved Model Performance
- No hash collisions or random values
- Consistent encoding across JVM restarts
- Normalized features improve gradient descent

### 3. Explainable Features
- Each feature has clear meaning
- Can trace back why something is anomalous
- Easier debugging and tuning

### 4. Domain Knowledge Integration
- Features designed based on security research
- Leverages network traffic patterns
- Incorporates known attack indicators

---

## References

See `FEATURE_ENGINEERING_ANALYSIS.md` for detailed rationale and research citations.
