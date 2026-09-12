# conn-foundation-pipeline — Implementation Record

## What This Document Covers

This document records everything built in the `conn-foundation-pipeline` development branch,
now merged to `main`. It covers the **first vertical slice** of the ML platform: consuming
raw Zeek `conn` JSON from Kafka, parsing and validating each record, extracting a frozen
20-value `float32` feature vector using stateful Flink processing, and publishing the result
back to Kafka — all without ONNX inference, ClickHouse, or any Python component.

This slice corresponds to Days 1–5 of the Roadmap and 14 implementation tasks.

---

## 1. Overall Architecture Strategy

### Hexagonal Architecture (Ports and Adapters)

The entire codebase is organized as a strict one-way dependency chain:

```
domain → ports → application → adapters → bootstrap
```

Each layer has a fixed contract:

| Layer | Purpose | Allowed dependencies |
|---|---|---|
| `domain` | Immutable value objects, feature formulas, schema constants | Nothing external — pure Java 21 |
| `ports` | Input/output port interfaces | `domain` only |
| `application` | Use cases that orchestrate domain logic | `domain` + `ports` |
| `adapter-kafka` | Jackson deserialization, Kafka serializers | `domain` + `ports` |
| `adapter-flink` | Flink operators, stateful processing | `domain` + `ports` + `adapter-kafka` |
| `bootstrap-online-job` | Wires all adapters into a runnable Flink job | All adapters |

**Why this structure?** It means the domain model and feature formulas can be unit-tested
without any Kafka, Flink, ClickHouse, or Jackson dependency on the classpath. Adapters
implement the ports; only the bootstrap module knows which concrete adapter is used.

### Java 21 Idioms

- **`record`** for every immutable data carrier (value objects, DTOs, results)
- **`sealed interface`** + records + pattern-matching `switch` (arrow form, no `default`
  in exhaustive switch) for closed result hierarchies like `MappingResult<T>`
- **Compact constructors** in records for all validation logic
- **`float[]` and `byte[]` defensive copy** in both the compact constructor AND the
  overridden accessor — prevents external mutation of internal arrays

### Test-Driven Development

Every task followed the Red → Green cycle:
1. Write a failing test first (RED — compilation error or assertion failure)
2. Implement the minimum code to make it pass (GREEN)
3. Commit once tests are clean

All 48 tests pass on `main`. The Testcontainers E2E test skips automatically when Docker
is unavailable (`@Testcontainers(disabledWithoutDocker = true)`).

---

## 2. Source Contract and Fixtures

### `contracts/source/zeek-conn-source-v1.json`

Defines the canonical shape of a Zeek `conn` log record as it arrives on the Kafka topic.
This contract is immutable — any field addition or type change creates a new version.

**Required fields:** `id`, `ts`, `id_orig_h`, `id_orig_p`, `id_resp_h`, `id_resp_p`,
`proto`, `conn_state`

**Optional fields:** `service`, `duration`, `orig_bytes`, `resp_bytes`, `orig_pkts`,
`resp_pkts`, `missed_bytes`, `local_orig`, `local_resp`

**Key rule:** `ts` is epoch seconds as a floating-point number. It is multiplied by 1000
and rounded to produce a UTC millisecond `Instant`.

### `tests/fixtures/zeek_conn/`

Three sanitized fixtures covering real Zeek shapes:
- `valid-tcp-ssl.json` — TCP connection on port 443 with SSL service
- `valid-udp-dns.json` — UDP connection on port 53 with DNS service
- `invalid-port.json` — record with a port value outside `[0, 65535]`
- `malformed.json` — intentionally broken JSON (triggers parse rejection)
- `missing-required-field.json` — missing `conn_state` (triggers validation rejection)

---

## 3. Domain Layer — `modules/domain`

The domain module is the **most constrained**: zero external dependencies in production code.
No Jackson, Kafka, Flink, ClickHouse, or ONNX classes appear here.

### 3.1 Category Enums — `domain.event`

Three enums map Zeek string values to typed Java constants:

**`Protocol`** — `TCP`, `UDP`, `ICMP`, `OTHER`
```java
// fromZeekValue("tcp") returns Protocol.TCP; unknown strings return OTHER
Protocol.fromZeekValue(String zeekProto)
```

**`ServiceCode`** — `DNS`, `HTTP`, `SSL`, `OTHER`
```java
// Used for feature booleans: service_dns, service_http, service_ssl
ServiceCode.fromZeekValue(String zeekService)
```

**`ConnectionState`** — `SF`, `S0`, `REJ`, `RSTO`, `RSTR`, `OTH`, `UNKNOWN`, etc.
```java
// isFailed() returns true for S0, REJ, RSTO, RSTR — drives feature index 16
connectionState.isFailed()
```

### 3.2 Identity Value Objects

**`SensorId(String value)`** — wraps a non-blank string identifying the network sensor.
Custom `toString()` returns the value directly.

**`EventId(String value)`** — wraps a non-blank string. Has a static factory:
```java
// Produces "sensor-eu-1:Cabc123XYZ" — namespaces upstream IDs per sensor
EventId.derive(SensorId sensor, String upstreamId)
```
This ensures event IDs are globally unique even when upstream IDs collide across sensors.

### 3.3 Connection Value Objects

**`ConnectionTuple`** — 7-field record holding the network 5-tuple plus protocol and state.
```java
// Validates: IPs must be non-blank, ports must be in [0, 65535]
public record ConnectionTuple(String sourceIp, int sourcePort,
                               String destinationIp, int destinationPort,
                               Protocol protocol, ServiceCode service,
                               ConnectionState connectionState)
```

**`ConnectionMeasurements`** — 6-field record for traffic metrics.
```java
// All fields must be non-negative — compact constructor enforces this
public record ConnectionMeasurements(long durationMillis, long originBytes,
                                      long responseBytes, int originPackets,
                                      int responsePackets, long missedBytes)
```

**`ConnectionLocality`** — trivial 2-field record, no validation (nulls allowed for
records where Zeek did not emit locality).
```java
public record ConnectionLocality(Boolean localOrig, Boolean localResp)
```

### 3.4 Result Type — `MappingResult<T>`

A sealed interface that represents either a successful mapping or a typed failure.
Replaces exceptions for expected validation failures in the mapping pipeline.

```java
// Sealed — only Valid<T> and Invalid<T> can implement it
public sealed interface MappingResult<T> permits MappingResult.Valid, MappingResult.Invalid {

    // Valid wraps the successful value — rejects null
    record Valid<T>(T value) implements MappingResult<T> { ... }

    // Invalid carries a typed reason code + human-readable detail
    record Invalid<T>(ReasonCode reason, String detail) implements MappingResult<T> { ... }

    // All helper methods use exhaustive pattern-matching switch — no default branch needed
    default boolean isValid() {
        return switch (this) {
            case Valid<T> v -> true;
            case Invalid<T> i -> false;
        };
    }
}
```

**`ReasonCode`** enum — `MISSING_REQUIRED_FIELD`, `INVALID_TIMESTAMP`, `INVALID_PORT`,
`INVALID_COUNTER`, `PARSE_ERROR`, `UNKNOWN`

### 3.5 NetworkEvent

The canonical domain event. A sealed interface over a shared `EventEnvelope`, with one record
per log type. Currently permits only `ConnEvent` — the only protocol with a parser, mapper,
and feature schema.

```java
// A sealed interface rather than a record because log types genuinely differ in shape:
// a conn record carries connection measurements, and a dns record carries none of those.
// permits lists only implemented log types — adding a record ahead of its implementation
// defeats the exhaustiveness checking that sealing buys.
public sealed interface NetworkEvent permits ConnEvent {

    // Shared identity block — every log type carries the same fields
    EventEnvelope envelope();

    // Delegating accessors expose envelope fields so call sites that read only identity
    // and timing neither know nor care that the hierarchy exists
    default EventId eventId() {
        return envelope().eventId();
    }

    default Instant eventTime() {
        return envelope().eventTime();
    }

    default SensorId sensor() {
        return envelope().sensor();
    }

    default LogType logType() {
        return envelope().logType();
    }

    default String connectionUid() {
        return envelope().connectionUid();
    }
}
```

### 3.6 Feature Schema — `ConnFeatureSchemaV1`

A compile-time constant that freezes the 20-feature schema for `conn-feature-v1`.

```java
public final class ConnFeatureSchemaV1 {

    // SHA-256 of contracts/features/conn-feature-schema-v1.json
    // This exact literal must match the file on disk — verified by ConnFeatureSchemaV1Test
    public static final String CONTENT_HASH =
        "f42fb1bebb2efe3acc5de634c6a7bb3d6f97fc021207f75d77652533b1c01e1b";

    // Full schema with all 20 FeatureDefinition entries (index, name, unit, formula, policy)
    public static final FeatureSchema SCHEMA = new FeatureSchema("conn-feature-v1", ...);
}
```

**Why a frozen hash?** If someone edits `conn-feature-schema-v1.json` without creating a v2,
the test `ConnFeatureSchemaV1Test.contentHashMatchesCommittedContractFile` fails loudly.
This prevents silent training/serving skew.

### 3.7 FeatureVector

The output of the feature extraction pipeline. Carries the frozen 20 float32 values plus the
complete envelope that identifies and dates them.

```java
public record FeatureVector(String eventId, Instant eventTime, SensorId sensor, LogType logType,
                             String connectionUid, String schemaId, String schemaHash,
                             float[] values, int qualityFlags, Instant producedAt) {
    public FeatureVector {
        // Identity must be present — it is the ClickHouse ORDER BY key tail.
        if (eventId == null || eventId.isBlank()) {
            throw new IllegalArgumentException("eventId must not be blank");
        }
        if (values == null) {
            throw new IllegalArgumentException("values must not be null");
        }

        // Both envelope components are structural; every row must say which sensor and Zeek log produced it.
        Objects.requireNonNull(sensor, "sensor must not be null");
        Objects.requireNonNull(logType, "logType must not be null");
        Objects.requireNonNull(producedAt, "producedAt must not be null");

        // A log type with no correlation uid yields "" rather than null.
        connectionUid = connectionUid == null ? "" : connectionUid;

        // Defensive copy in: the caller keeps no handle on our internal array.
        values = Arrays.copyOf(values, values.length);
    }

    // Defensive copy out: callers cannot mutate our internal array either.
    @Override
    public float[] values() {
        return Arrays.copyOf(values, values.length);
    }
}
```

Defensive copies are required in both directions because a record's accessor returns the field
reference directly by default; overriding it is the only way to make `float[]` truly immutable.
`sensor` makes an archived row self-sufficient for training without joining the network events
table. `producedAt` becomes the ClickHouse `row_version`, so "last emission wins" on replay.

### 3.8 SourceWindowState

A bounded rolling 5-minute window for per-`(sensor, sourceIp)` aggregation.
This is deliberately a **plain `final` class, not a record** — its four parallel `long[]`
ring-buffer arrays are an internal implementation detail, not a public data shape.

```java
public final class SourceWindowState {
    private static final int BUCKET_COUNT = 5; // exactly 5 one-minute buckets

    // Four parallel arrays: one slot per bucket, indexed by (epochMinute % 5)
    private final long[] bucketMinutes;     // which minute owns this slot
    private final long[] connectionCounts;  // connections seen in that minute
    private final long[] byteSums;          // total bytes in that minute
    private final long[] failedCounts;      // failed connections in that minute

    // Start with all slots marked "never written" using Long.MIN_VALUE as sentinel
    public static SourceWindowState empty() { ... }

    // Returns a NEW SourceWindowState — never mutates this instance
    // This is critical: Flink state must be updated atomically via windowState.update()
    public SourceWindowState record(long bucketEpochMinute, long bytes, boolean failed) {
        int slot = (int) Math.floorMod(bucketEpochMinute, (long) BUCKET_COUNT);
        // If the slot holds a different minute, reset it before incrementing
        if (minutes[slot] != bucketEpochMinute) { ... reset ... }
        counts[slot] += 1; ...
        return new SourceWindowState(minutes, counts, sums, fails);
    }

    // Window exclusion: strictly less than BUCKET_COUNT means "within the 5-minute window"
    // Example: currentMinute=100, bucketMinute=96 → 100-96=4 < 5 → included
    //          currentMinute=100, bucketMinute=95 → 100-95=5 ≥ 5 → excluded
    private long sumWithinWindow(long[] values, long currentMinuteHint) {
        for (int i = 0; i < BUCKET_COUNT; i++) {
            if (bucketMinutes[i] != Long.MIN_VALUE
                    && currentMinuteHint - bucketMinutes[i] < BUCKET_COUNT) {
                total += values[i];
            }
        }
    }

    // Three public accessors for feature indices 17, 18, 19
    public long connectionCount5m() { ... }
    public long byteSum5m() { ... }
    public long failedCount5m() { ... }
}
```

**Why 5 buckets instead of a time-range scan?** Fixed-size arrays mean the state size per
key is bounded to exactly 4 × 5 × 8 = 160 bytes regardless of traffic volume. No per-IP
sets, no event histories, no unbounded `MapState`.

### 3.9 SourceKey

The Flink keying type for per-source aggregation.
```java
// Keyed by (sensor, sourceIp) — each unique pair gets its own SourceWindowState
public record SourceKey(SensorId sensor, String sourceIp) { }
```

### 3.10 FeatureBuildResult

Carries the two outputs of a single feature-build call together.
```java
// Both vector and newState are returned together so the caller can atomically
// update Flink state (windowState.update(result.newState())) and emit (out.collect(result.vector()))
public record FeatureBuildResult(FeatureVector vector, SourceWindowState newState) { }
```

---

## 4. Ports Layer — `modules/ports`

### `BuildFeaturesUseCase`

The only port interface implemented in this slice.
```java
// Input port: given a network event and the current window state, produce a result
// The interface lives in ports so application depends on it, not on the implementation
public interface BuildFeaturesUseCase {
    FeatureBuildResult build(NetworkEvent event, SourceWindowState currentState);
}
```

---

## 5. Application Layer — `modules/application`

### 5.1 EventFeatureExtractor

Computes the first 17 event-level features from a `ConnEvent`. Pure Java, zero Flink.
Takes `ConnEvent` rather than the sealed `NetworkEvent` interface because every line reads
`measurements()` or `connection()` — this is the conn-specific extractor. A later unit adds
per-log-type extractors beside this one rather than branches inside it.

```java
public final class EventFeatureExtractor {

    // Returns float[17] — indices 0-16 of the feature schema
    public float[] extractEventLevel(ConnEvent event) {
        long durationMillis = event.measurements().durationMillis();
        long originBytes = event.measurements().originBytes();
        long responseBytes = event.measurements().responseBytes();
        int originPackets = event.measurements().originPackets();
        int responsePackets = event.measurements().responsePackets();
        long totalBytes = originBytes + responseBytes;
        int totalPackets = originPackets + responsePackets;
        int destinationPort = event.connection().destinationPort();
        Protocol protocol = event.connection().protocol();
        ServiceCode service = event.connection().service();

        return new float[]{
            durationMillis,          // [0]  duration in ms
            originBytes,             // [1]  orig_bytes
            responseBytes,           // [2]  resp_bytes
            originPackets,           // [3]  orig_pkts
            responsePackets,         // [4]  resp_pkts
            totalBytes,              // [5]  origin + response bytes
            totalPackets,            // [6]  origin + response packets
            // Division-by-zero guard: max(1, totalPackets) avoids NaN/Inf
            (float) totalBytes / Math.max(1, totalPackets),     // [7]  bytes_per_packet
            (float) responseBytes / Math.max(1, originBytes),   // [8]  response_origin_byte_ratio
            destinationPort,         // [9]  id_resp_p as float32
            (destinationPort >= 1 && destinationPort <= 1023) ? 1f : 0f,     // [10] destination_is_well_known
            protocol == Protocol.TCP ? 1f : 0f,    // [11] protocol_tcp
            protocol == Protocol.UDP ? 1f : 0f,    // [12] protocol_udp
            service == ServiceCode.DNS  ? 1f : 0f,    // [13] service_dns
            service == ServiceCode.HTTP ? 1f : 0f,    // [14] service_http
            service == ServiceCode.SSL  ? 1f : 0f,    // [15] service_ssl
            event.connection().connectionState().isFailed() ? 1f : 0f  // [16] connection_failed (S0,REJ,RSTO,RSTR)
        };
    }
}
```

### 5.2 BuildFeaturesUseCaseImpl

Assembles the full 20-value vector by combining event-level features (0–16) with
window-state features (17–19). Takes `NetworkEvent` but narrows to `ConnEvent` with a
pattern switch — the single site that converts from the sealed interface to the concrete type.

```java
public final class BuildFeaturesUseCaseImpl implements BuildFeaturesUseCase {
    private final EventFeatureExtractor eventFeatureExtractor = new EventFeatureExtractor();
    private final Clock clock;  // For deterministic producedAt timestamp

    @Override
    public FeatureBuildResult build(NetworkEvent event, SourceWindowState currentState) {
        // Pattern switch to narrow from sealed NetworkEvent to ConnEvent
        ConnEvent conn = switch (event) {
            case ConnEvent c -> c;
        };

        // Step 1: extract the 17 event-level features
        float[] eventLevel = eventFeatureExtractor.extractEventLevel(conn);

        // Step 2: advance the window state with this event's data
        // record() returns a NEW state — never mutates the current one
        long totalBytes = conn.measurements().originBytes() + conn.measurements().responseBytes();
        boolean failed = conn.connection().connectionState().isFailed();
        long bucketMinute = event.eventTime().getEpochSecond() / 60;
        SourceWindowState newState = currentState.record(bucketMinute, totalBytes, failed);

        // Step 3: assemble the final 20-value float[] in schema order
        float[] values = new float[20];
        System.arraycopy(eventLevel, 0, values, 0, 17); // copy indices 0-16
        values[17] = newState.connectionCount5m();       // source_connections_5m
        values[18] = newState.byteSum5m();               // source_bytes_5m
        values[19] = newState.failedCount5m();           // source_failed_connections_5m

        // Step 4: wrap in FeatureVector with event identity and schema version
        FeatureVector vector = new FeatureVector(
            event.eventId().value(),
            event.eventTime(),
            event.sensor(),
            event.logType(),
            event.connectionUid(),
            ConnFeatureSchemaV1.SCHEMA.id(),  // "conn-feature-v1"
            ConnFeatureSchemaV1.CONTENT_HASH, // SHA-256 of the JSON contract
            values,
            0,  // qualityFlags = 0 (clean record)
            clock.instant().truncatedTo(ChronoUnit.MILLIS)); // producedAt

        // Return both the vector and the new state — caller must update Flink state
        return new FeatureBuildResult(vector, newState);
    }
}
```

---

## 6. Adapter-Kafka Layer — `modules/adapter-kafka`

### 6.1 ZeekConnEvent — the JSON DTO

A Jackson-deserializable record that mirrors the raw Zeek `conn` JSON exactly.

```java
// Every field uses @JsonProperty to match Zeek's snake_case naming
// Primitive int for ports (required=true) — Jackson throws if missing
// Boxed Long/Double/Boolean for optional fields — null when absent
public record ZeekConnEvent(
    @JsonProperty(value = "id",           required = true) String id,
    @JsonProperty(value = "ts",           required = true) double ts,
    @JsonProperty(value = "id_orig_h",    required = true) String idOrigH,
    @JsonProperty(value = "id_orig_p",    required = true) int idOrigP,
    @JsonProperty(value = "id_resp_h",    required = true) String idRespH,
    @JsonProperty(value = "id_resp_p",    required = true) int idRespP,
    @JsonProperty(value = "proto",        required = true) String proto,
    @JsonProperty("service")                               String service,
    @JsonProperty(value = "conn_state",   required = true) String connState,
    @JsonProperty("duration")     Double duration,
    @JsonProperty("orig_bytes")   Long origBytes,
    @JsonProperty("resp_bytes")   Long respBytes,
    @JsonProperty("orig_pkts")    Long origPkts,
    @JsonProperty("resp_pkts")    Long respPkts,
    @JsonProperty("missed_bytes") Long missedBytes,
    @JsonProperty("local_orig")   Boolean localOrig,
    @JsonProperty("local_resp")   Boolean localResp
)
```

### 6.2 JsonZeekConnParser

Parses raw `byte[]` into a `MappingResult<ZeekConnEvent>`. Uses Jackson `ObjectMapper`.
Parse failures (malformed JSON, missing required fields) return `MappingResult.invalid`
with `ReasonCode.PARSE_ERROR` rather than throwing exceptions.

```java
// ObjectMapper is created once at construction — not per-record
public final class JsonZeekConnParser {
    private final ObjectMapper mapper = new ObjectMapper();

    // Returns Valid<ZeekConnEvent> on success, Invalid<...> on any parse failure
    public MappingResult<ZeekConnEvent> parse(byte[] rawPayload) {
        try {
            return MappingResult.valid(mapper.readValue(rawPayload, ZeekConnEvent.class));
        } catch (Exception e) {
            return MappingResult.invalid(ReasonCode.PARSE_ERROR, e.getMessage());
        }
    }
}
```

### 6.3 EventMapper

Maps a successfully parsed `ZeekConnEvent` to a `ConnEvent` (implementing `NetworkEvent`)
with full domain validation. Uses `MappingResult<NetworkEvent>` for all failure paths.

```java
public final class EventMapper {
    public MappingResult<NetworkEvent> map(ZeekConnEvent dto, SensorId sensor) {

        // Guard 1: upstream ID must be present for stable event identity
        if (dto.id() == null || dto.id().isBlank()) { return invalid(MISSING_REQUIRED_FIELD, ...); }

        // Guard 2: timestamp must be a valid non-negative float
        if (Double.isNaN(dto.ts()) || dto.ts() < 0) { return invalid(INVALID_TIMESTAMP, ...); }

        // Guard 3: both IP addresses must be present
        if (dto.idOrigH() == null || dto.idRespH() == null) { return invalid(MISSING_REQUIRED_FIELD, ...); }

        // Guard 4: connection state must be present
        if (dto.connState() == null || dto.connState().isBlank()) { return invalid(MISSING_REQUIRED_FIELD, ...); }

        // Derive a namespaced event ID: "sensorId:upstreamId"
        EventId eventId = EventId.derive(sensor, dto.id());

        // Convert Zeek's epoch-second float to a millisecond-precision Instant
        // Math.round avoids floating-point truncation errors (e.g. 1234567890.999 → correct ms)
        Instant eventTime = Instant.ofEpochMilli(Math.round(dto.ts() * 1000.0));

        // Build the connection tuple — wrapped in try/catch because port validation
        // throws IllegalArgumentException for out-of-range ports
        try {
            tuple = new ConnectionTuple(dto.idOrigH(), dto.idOrigP(), ...);
        } catch (IllegalArgumentException e) {
            return invalid(INVALID_PORT, e.getMessage());
        }

        // Optional numeric fields default to 0 when absent (not missing-required errors)
        // This matches the feature schema's DEFAULT_ZERO policy
        long durationMillis = dto.duration() == null ? 0L : Math.round(dto.duration() * 1000.0);
        long originBytes    = dto.origBytes() == null ? 0L : dto.origBytes();
        // ... etc

        // Wrap the identity envelope with conn-specific payload, returning a ConnEvent
        // that implements the NetworkEvent sealed interface
        EventEnvelope envelope = new EventEnvelope(eventId, eventTime, sensor, LogType.CONN, dto.id());
        NetworkEvent event = new ConnEvent(envelope, tuple, measurements, locality);
        return MappingResult.valid(event);
    }
}
```

### 6.4 FeatureVectorSerializer

Kafka `Serializer<FeatureVector>` that converts a `FeatureVector` to a JSON `byte[]`.
Created once per `OnlineFeatureJob.build()` call and reused across all records.

```java
// Serializes all fields to a JSON object using Jackson ObjectMapper
// The schemaId and schemaHash fields allow consumers to verify they're reading the right schema
// Output shape: { "eventId": "...", "schemaId": "conn-feature-v1", "schemaHash": "...", "values": [...], ... }
public final class FeatureVectorSerializer implements Serializer<FeatureVector> {
    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public byte[] serialize(String topic, FeatureVector data) {
        try {
            return mapper.writeValueAsBytes(...);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize FeatureVector", e);
        }
    }
}
```

### 6.5 RejectedRecordPayload

Carries rejection metadata for the dead-letter queue sink. Contains a **defensive copy**
of the raw payload bytes — the original Kafka message content.

```java
public record RejectedRecordPayload(byte[] rawPayload, String reasonCode, String detail) {
    public RejectedRecordPayload {
        // Defensive copy on construction — prevents callers from mutating our stored bytes
        rawPayload = Arrays.copyOf(rawPayload, rawPayload.length);
    }

    @Override
    public byte[] rawPayload() {
        // Defensive copy on read — callers cannot mutate our internal array
        return Arrays.copyOf(rawPayload, rawPayload.length);
    }
}
```

### 6.6 RejectedRecordSerializer

Kafka `Serializer<RejectedRecordPayload>` for the DLQ sink. Writes a JSON envelope
that includes a SHA-256 hash of the raw payload rather than the raw bytes themselves.

```java
// Strategy: store the hash, not the raw bytes, to avoid raw-payload retention in Kafka
// The SHA-256 hash lets operators correlate the DLQ record with the original source offset
// Fields: { "reasonCode": "...", "detail": "...", "rawPayloadHash": "<sha256>", "receivedAt": "..." }
public final class RejectedRecordSerializer implements Serializer<RejectedRecordPayload> {
    private String sha256Hex(byte[] data) {
        // Standard Java MessageDigest — no external library needed
        byte[] hash = MessageDigest.getInstance("SHA-256").digest(data);
        // Format as 64 lowercase hex chars
        StringBuilder sb = new StringBuilder();
        for (byte b : hash) sb.append(String.format("%02x", b));
        return sb.toString();
    }
}
```

---

## 7. Adapter-Flink Layer — `modules/adapter-flink`

Uses **Flink 2.2.1 V2 APIs exclusively**. The old `SourceFunction`/`SinkFunction` V1
interfaces were removed in Flink 2.x. All lifecycle methods use `open(OpenContext)`.

### 7.1 RejectedRecord

A Flink-side record for events that fail parsing or validation.

```java
// Defensive byte[] copy required in BOTH directions:
// - compact constructor: prevents the caller's array from being modified after construction
// - rawPayload() override: prevents callers from modifying our stored reference
public record RejectedRecord(byte[] rawPayload, ReasonCode reason, String detail) {
    public RejectedRecord {
        rawPayload = Arrays.copyOf(rawPayload, rawPayload.length);
    }

    @Override
    public byte[] rawPayload() {
        return Arrays.copyOf(rawPayload, rawPayload.length);
    }
}
```

### 7.2 ParseMapValidateFunction

A Flink `ProcessFunction<byte[], NetworkEvent>` that handles the full
parse → map → validate pipeline for a single Kafka record.

```java
public final class ParseMapValidateFunction extends ProcessFunction<byte[], NetworkEvent> {

    // Side output tag — lets the main pipeline stay clean (only valid NetworkEvents)
    // while rejected records are routed separately via getSideOutput(REJECTED_TAG)
    public static final OutputTag<RejectedRecord> REJECTED_TAG =
        new OutputTag<RejectedRecord>("rejected") {};

    // sensor is passed at construction — the function does not hardcode a sensor ID
    private final SensorId sensor;

    // Transient: not serialized by Flink — recreated in open() on each TaskManager
    private transient JsonZeekConnParser parser;
    private transient EventMapper mapper;

    @Override
    public void open(OpenContext openContext) throws Exception {
        // ObjectMapper is expensive to create — initialize once per task lifecycle
        parser = new JsonZeekConnParser();
        mapper = new EventMapper();
    }

    @Override
    public void processElement(byte[] rawPayload, Context ctx, Collector<NetworkEvent> out) {
        // Stage 1: parse JSON bytes → ZeekConnEvent DTO
        MappingResult<ZeekConnEvent> parsed = parser.parse(rawPayload);
        if (!parsed.isValid()) {
            // Route to side output — main stream sees only valid events
            ctx.output(REJECTED_TAG, new RejectedRecord(rawPayload, parsed.reason(), parsed.detail()));
            return;
        }

        // Stage 2: map DTO → validated NetworkEvent domain object
        MappingResult<NetworkEvent> mapped = mapper.map(parsed.value(), sensor);
        if (!mapped.isValid()) {
            ctx.output(REJECTED_TAG, new RejectedRecord(rawPayload, mapped.reason(), mapped.detail()));
            return;
        }

        // Only valid, fully mapped events reach the main output
        out.collect(mapped.value());
    }
}
```

### 7.3 SourceKeySelector

Extracts the Flink partitioning key from a `NetworkEvent`.

```java
// Keys by (sensor, sourceIp) — each unique pair gets independent Flink state
// This ensures window state for sensor-eu-1/10.0.0.5 is completely separate from
// sensor-eu-1/10.0.0.9 even if they arrive on the same Kafka partition
public final class SourceKeySelector implements KeySelector<NetworkEvent, SourceKey> {
    @Override
    public SourceKey getKey(NetworkEvent event) {
        // NetworkEvent is sealed, and only ConnEvent is currently permitted.
        // Pattern matching on the sealed interface extracts sourceIp from the connection.
        String sourceIp = switch (event) {
            case ConnEvent conn -> conn.connection().sourceIp();
        };
        return new SourceKey(event.sensor(), sourceIp);
    }
}
```

### 7.4 ConnFeatureProcessFunction

A `KeyedProcessFunction<SourceKey, NetworkEvent, FeatureVector>` that maintains
per-key rolling window state and emits a `FeatureVector` for every input event.

```java
public final class ConnFeatureProcessFunction
        extends KeyedProcessFunction<SourceKey, NetworkEvent, FeatureVector> {

    // Flink managed state — survives checkpoints and restarts
    // Each unique SourceKey (sensor, sourceIp) has its own ValueState<SourceWindowState>
    private transient ValueState<SourceWindowState> windowState;

    // The use case is stateless itself — the state lives in Flink's ValueState
    private transient BuildFeaturesUseCaseImpl useCase;

    @Override
    public void open(OpenContext openContext) {
        // Register state with Flink — persisted in the configured state backend
        // (default: heap state; production target: EmbeddedRocksDB)
        ValueStateDescriptor<SourceWindowState> descriptor = new ValueStateDescriptor<>(
            "source-window-state", TypeInformation.of(SourceWindowState.class));
        windowState = getRuntimeContext().getState(descriptor);

        // Build the use case once per task — it is immutable and thread-safe
        useCase = new BuildFeaturesUseCaseImpl();
    }

    @Override
    public void processElement(NetworkEvent event, Context ctx, Collector<FeatureVector> out)
            throws Exception {

        // Read current state — null means this key has never been seen before
        SourceWindowState currentState = windowState.value();
        if (currentState == null) {
            currentState = SourceWindowState.empty(); // all buckets set to Long.MIN_VALUE
        }

        // Build features and advance the window state atomically
        FeatureBuildResult result = useCase.build(event, currentState);

        // Persist the new state back to Flink — included in next checkpoint
        windowState.update(result.newState());

        // Emit the completed 20-value feature vector downstream
        out.collect(result.vector());
    }
}
```

---

## 8. Bootstrap Layer — `modules/bootstrap-online-job`

### OnlineFeatureJob

The composition root: wires all adapters into the complete streaming pipeline.
Only this module knows which concrete adapters exist.

```java
public final class OnlineFeatureJob {

    // A pass-through DeserializationSchema that delivers raw bytes to the pipeline unchanged.
    // We parse in ParseMapValidateFunction rather than in the Kafka source
    // so that parse failures are observable as side-output RejectedRecords rather than job crashes.
    private static final DeserializationSchema<byte[]> RAW_BYTES = new DeserializationSchema<>() { ... };

    // build() wires the full pipeline but does NOT call env.execute().
    // This separation allows the E2E test to call env.executeAsync() for non-blocking execution.
    public static void build(StreamExecutionEnvironment env,
                              String bootstrapServers, String inputTopic,
                              String featureVectorTopic, String dlqTopic,
                              SensorId sensor) {

        // --- SOURCE ---
        // FLIP-27 source API (V2 — SourceFunction was removed in Flink 2.x)
        // Reads raw bytes from the conn topic, no deserialization here
        KafkaSource<byte[]> source = KafkaSource.<byte[]>builder()
            .setBootstrapServers(bootstrapServers)
            .setTopics(inputTopic)
            .setGroupId("conn-online-job")
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(RAW_BYTES)
            .build();

        DataStream<byte[]> rawStream = env.fromSource(
            source, WatermarkStrategy.noWatermarks(), "conn-raw-source");

        // --- PARSE / MAP / VALIDATE ---
        // SingleOutputStreamOperator gives access to getSideOutput() for rejected records
        SingleOutputStreamOperator<NetworkEvent> parsed = rawStream
            .process(new ParseMapValidateFunction(sensor))
            .name("parse-map-validate");

        // --- KEYED FEATURE EXTRACTION ---
        // keyBy partitions by SourceKey — ensures per-source state isolation
        DataStream<FeatureVector> featureVectors = parsed
            .keyBy(new SourceKeySelector())
            .process(new ConnFeatureProcessFunction())
            .name("conn-feature-extraction");

        // Serializers are created once — reused across all records in the lambda
        // (avoids allocating a new ObjectMapper per record in the hot path)
        FeatureVectorSerializer featureSerializer = new FeatureVectorSerializer();
        RejectedRecordSerializer rejectedSerializer = new RejectedRecordSerializer();

        // --- FEATURE VECTOR SINK ---
        // AT_LEAST_ONCE: Kafka-to-Kafka exactly-once requires two-phase commit; deferred
        KafkaSink<FeatureVector> featureSink = KafkaSink.<FeatureVector>builder()
            .setBootstrapServers(bootstrapServers)
            .setRecordSerializer(KafkaRecordSerializationSchema.<FeatureVector>builder()
                .setTopic(featureVectorTopic)
                .setValueSerializationSchema(vector -> featureSerializer.serialize(featureVectorTopic, vector))
                .build())
            .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
            .build();
        featureVectors.sinkTo(featureSink).name("feature-vector-sink");

        // --- DLQ SINK ---
        // Rejected records (parse/validation failures) go to the DLQ topic
        // RejectedRecordPayload stores a SHA-256 hash of the raw bytes, not the bytes themselves
        DataStream<RejectedRecord> rejected =
            parsed.getSideOutput(ParseMapValidateFunction.REJECTED_TAG);
        KafkaSink<RejectedRecord> dlqSink = KafkaSink.<RejectedRecord>builder()
            .setBootstrapServers(bootstrapServers)
            .setRecordSerializer(KafkaRecordSerializationSchema.<RejectedRecord>builder()
                .setTopic(dlqTopic)
                .setValueSerializationSchema(r -> rejectedSerializer.serialize(dlqTopic,
                    new RejectedRecordPayload(r.rawPayload(), r.reason().name(), r.detail())))
                .build())
            .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
            .build();
        rejected.sinkTo(dlqSink).name("dlq-sink");
    }

    // main() reads all configuration from environment variables
    // No hardcoded broker addresses, topic names, or sensor IDs
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String bootstrapServers = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
        String inputTopic       = System.getenv().getOrDefault("CONN_INPUT_TOPIC",        "conn");
        String featureTopic     = System.getenv().getOrDefault("FEATURE_VECTOR_TOPIC",    "netsec.conn.feature-vector.v1");
        String dlqTopic         = System.getenv().getOrDefault("DLQ_TOPIC",               "netsec.conn.dlq.v1");
        String sensorId         = System.getenv().getOrDefault("SENSOR_ID",               "sensor-default");

        build(env, bootstrapServers, inputTopic, featureTopic, dlqTopic, new SensorId(sensorId));
        env.execute("conn-online-feature-job");
    }
}
```

---

## 9. Testing Strategy

### Test Pyramid

| Level | Count | Where |
|---|---|---|
| Domain unit tests (pure Java, no frameworks) | 29 | `modules/domain` |
| Application unit tests (no Flink/Kafka) | 5 | `modules/application` |
| Adapter-kafka unit tests (Jackson only) | 10 | `modules/adapter-kafka` |
| Flink operator tests (Flink test harness) | 4 | `modules/adapter-flink` |
| E2E integration test (Testcontainers Kafka) | 1 (skips without Docker) | `modules/bootstrap-online-job` |
| **Total** | **49** | |

### Key Testing Patterns

**Defensive-copy verification** — `FeatureVectorTest` mutates the array passed to the
constructor AND the array returned by `values()` and asserts neither mutation affects
the stored values.

**Window state isolation** — `ConnFeatureProcessFunctionTest` uses Flink's
`ProcessFunctionTestHarnesses.forKeyedProcessFunction` to process two events from the
same source IP and one from a different IP, asserting that state accumulates independently
per key (index 17 = 2 for the repeated key, 1 for the new key).

**Content hash verification** — `ConnFeatureSchemaV1Test.contentHashMatchesCommittedContractFile`
reads `contracts/features/conn-feature-schema-v1.json` from disk, computes its SHA-256,
and asserts it matches `ConnFeatureSchemaV1.CONTENT_HASH`. If someone edits the JSON
without creating a v2, this test fails loudly.

**Side output routing** — `ParseMapValidateFunctionTest` uses `CollectingOutputCollector`
to assert that malformed JSON goes to `REJECTED_TAG` and valid JSON reaches the main output.

**E2E path** — `OnlineFeatureJobE2ETest` spins up a real Confluent Kafka container via
Testcontainers, publishes `valid-tcp-ssl.json`, starts the Flink job via `executeAsync()`,
polls the feature-vector topic for up to 60 seconds, and asserts the output contains
`"schemaId":"conn-feature-v1"` and the correct event ID `sensor-eu-1:Cabc123XYZ`.

---

## 10. Key Design Decisions and Rationale

| Decision | Why |
|---|---|
| `MappingResult<T>` sealed interface | Replaces exceptions for expected validation failures. Makes all callers handle both paths at compile time. |
| Optional numeric fields default to `0` | Matches the feature schema's `DEFAULT_ZERO` policy. A missing `orig_bytes` is not a rejected event — it is treated as 0 bytes. Only structurally invalid records (bad port, missing required ID) are rejected. |
| `SourceWindowState` as a plain class, not a record | Its four parallel `long[]` arrays are a ring-buffer implementation detail, not a public API. Forcing it into a record would expose the internal representation. |
| `record()` returns a new `SourceWindowState` | Flink state updates must be atomic. Creating a new instance means Flink sees either the old state or the new state, never a partially-updated object. |
| `RejectedRecord` and `RejectedRecordPayload` store a SHA-256 hash, not raw bytes | Avoids retaining potentially sensitive raw Zeek payloads in Kafka indefinitely. The hash is sufficient for forensic correlation without data retention risk. |
| Serializers hoisted out of Flink lambdas | `ObjectMapper` is expensive to construct (~millisecond). Creating one per record in the Kafka sink lambda would add significant GC pressure on the hot path. |
| `open(OpenContext)` instead of `open(Configuration)` | Flink 2.x changed the lifecycle method signature. The old `open(Configuration)` was removed. |
| `@Testcontainers(disabledWithoutDocker = true)` | The E2E test requires Docker. Making it skip gracefully (1 skipped, 0 failures) allows the CI reactor to pass even in Docker-free build environments. |

---

## 11. Commit History

| Commit | What was added |
|---|---|
| `49392a2` | `Protocol`, `ServiceCode`, `ConnectionState` enums |
| `5ba1f8b` | `SensorId`, `EventId`, `ConnectionTuple`, `ConnectionMeasurements`, `ConnectionLocality` |
| `ab81bb5` | `ReasonCode`, `MappingResult<T>` sealed interface, `NetworkEvent` |
| `a994728` | `FeatureDefinition`, `FeatureSchema`, `ConnFeatureSchemaV1`, `FeatureVector`, `SourceKey`, `conn-feature-schema-v1.json` |
| `279a1e4` | `SourceWindowState`, `FeatureBuildResult` |
| `476b0b2` | `zeek-conn-source-v1.json` contract, sanitized fixtures, fixture readability test |
| `475cdeb` | `ZeekConnEvent` DTO, `JsonZeekConnParser` |
| `b51c4f8` | `EventMapper` |
| `f690d50` | `EventFeatureExtractor` (indices 0–16) |
| `7a0c10a` | `BuildFeaturesUseCase` port, `BuildFeaturesUseCaseImpl` (full 20-value vector) |
| `9ffda91` | Flink deps in root pom, `RejectedRecord`, `ParseMapValidateFunction` |
| `c4f3aa8` | `SourceKeySelector`, `ConnFeatureProcessFunction` |
| `845aa6b` | kafka-clients dep, `FeatureVectorSerializer`, `RejectedRecordPayload`, `RejectedRecordSerializer` |
| `195dca3` | `OnlineFeatureJob.build()`, `OnlineFeatureJobE2ETest`, Testcontainers deps |
| `88285d6` | Fix: moved fixture-readability test from `domain` to `adapter-kafka`; serializers hoisted from lambdas |

---

## 12. Known Limitations and Next Steps

| Item | Status |
|---|---|
| **Watermarks** | `WatermarkStrategy.noWatermarks()` is used. The Roadmap requires 5-second disorder bound + 30-second idleness watermark. |
| **State backend** | Default Flink heap state. Production target is EmbeddedRocksDB with a 2 GiB managed-memory cap and 30-minute TTL. |
| **`SourceWindowState` Kryo serialization** | Flink will use Kryo fallback for state persistence because `SourceWindowState` has no public no-arg constructor. A custom `TypeSerializer` should be added before production deployment with checkpointing. |
| **Stream contract** | `contracts/stream/feature-vector-v1.json` (the Kafka topic schema for downstream consumers) was not created in this slice. |
| **Golden vector fixtures** | `tests/fixtures/feature_golden/` (expected float vectors for parity testing with Python) was not created. |
| **Documentation** | `docs/kafka.md`, `docs/features.md`, `docs/flink.md` not yet written. |
| **Next section** | **Day 6: ClickHouse DDL + independent archive Flink job** — see `docs/superpowers/plans/` for the next plan. |

---

## 13. How to Build and Run

```sh
# Build and run all tests (48 pass, 1 E2E skips without Docker)
./mvnw clean verify

# Run only domain tests
./mvnw test -pl modules/domain

# Run one specific test class
./mvnw test -pl modules/adapter-flink -Dtest=ConnFeatureProcessFunctionTest

# Run the full E2E test (requires Docker)
./mvnw test -pl modules/bootstrap-online-job -Dtest=OnlineFeatureJobE2ETest

# Build the deployable JAR
./mvnw package -pl modules/bootstrap-online-job -am

# Start the job (requires Kafka running; set env vars to override defaults)
KAFKA_BOOTSTRAP_SERVERS=localhost:9092 \
CONN_INPUT_TOPIC=conn \
FEATURE_VECTOR_TOPIC=netsec.conn.feature-vector.v1 \
DLQ_TOPIC=netsec.conn.dlq.v1 \
SENSOR_ID=sensor-eu-1 \
java -jar modules/bootstrap-online-job/target/bootstrap-online-job-*.jar
```
