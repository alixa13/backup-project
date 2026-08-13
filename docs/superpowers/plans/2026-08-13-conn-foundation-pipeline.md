# Conn Foundation Pipeline Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the first working vertical slice — raw `conn` Zeek JSON bytes flowing through Kafka into a Flink job that parses, validates, extracts a 20-feature vector with bounded per-source rolling state, and publishes it back to Kafka — with every layer unit-tested in isolation before the Flink wiring.

**Architecture:** Hexagonal, per `FINAL_ARCHITECTURE.md` and `PILOT_ARCHITECTURE.md`. `domain` holds pure value objects and formulas with zero framework imports. `application` holds the `BuildFeaturesUseCase` orchestration, depending only on `domain` + `ports`. `adapter-kafka` holds the Jackson DTO/parser/mapper/sinks. `adapter-flink` holds the two process functions and wires them to Flink's watermark/keyed-state machinery. `bootstrap-online-job` is the only module that composes concrete adapters into a runnable job.

**Tech Stack:** Java 21 (records, sealed interfaces, pattern-matching `switch` — all standard, non-preview features as of Java 21), JUnit 5 (`junit-jupiter`, already BOM-managed in the root POM), Jackson 2.17 for JSON (records supported natively since Jackson 2.12), Flink 2.2.1 (`flink-streaming-java`, `flink-connector-kafka:5.0.0-2.2`, `flink-test-utils` for `ProcessFunctionTestHarnesses`), Testcontainers 1.19.8 (Kafka module) for the one true end-to-end test in Task 14.

**Version note:** Flink 2.x removed `SourceFunction`/`SinkFunction`/Sink V1 entirely — every connector in this plan already uses the modern `KafkaSource` (FLIP-27 Source API) and `KafkaSink`/`KafkaRecordSerializationSchema` (Sink V2 API), so no code in this plan touches a removed API. Flink 2.0+ officially supports Java 21 (Java 17 is only the *default*, not a ceiling). Exact builder-method overloads can still shift between Flink minor versions; where that risk is real, the task calls it out explicitly rather than asserting false certainty.

## Global Constraints

- Java 21 throughout. Prefer `record` for simple immutable data carriers (with a compact constructor for validation), `sealed interface` + records + pattern-matching `switch` for closed result-type hierarchies, and `switch` expressions (arrow form) over classic `switch` statements. Not everything is a record — a type with real behavior and a hidden/bounded internal representation (see `SourceWindowState` in Task 5) stays a plain immutable class; forcing every type into a record for its own sake is not the goal.
- `domain` module: **zero** imports of Kafka, Flink, ClickHouse, ONNX, or Jackson classes. This is enforced by the module's own dependency list (only `junit-jupiter` in test scope) — an accidental import will fail to compile, not just fail a lint rule.
- `application` module: depends only on `domain` and `ports`. No Jackson, no Flink.
- All domain value objects are immutable. Record components holding arrays (`float[]`, `byte[]`) get a defensive copy in the compact constructor **and** an overridden accessor that returns a fresh copy on every read — records do not do this automatically, and skipping either half re-opens the exact mutability hole the type exists to prevent.
- Package root for all new code: `io.netsecml.platform`.
- Feature vector is exactly 20 `float32` values, ordered per `contracts/features/conn-feature-schema-v1.json` (Task 4 creates this file). Feature order, names, and count are frozen once Task 4 is committed — a later change is a new schema version, not an edit.
- **Scope boundary, stated explicitly:** this plan covers `conn` only, ending at "a validated `FeatureVector` is published to `netsec.conn.feature-vector.v1`." It does **not** cover the ONNX cascade (stage 1/stage 2), the model registry, ClickHouse archival, or the `dns`/`http`/`ssl` protocols. Per `PILOT_ARCHITECTURE.md`, those protocols replicate this exact pattern mechanically once it's proven — that replication, and the ONNX/ClickHouse work, are separate follow-up plans. This keeps this plan a single, independently testable subsystem instead of an unreviewable everything-at-once slab.
- **Stated simplification vs. `FINAL_ARCHITECTURE.md`:** that document specifies that a missing required numeric field (bytes/packets/duration) should be treated as an invalid event, not defaulted to zero, gated behind a future quality-flags design. This plan defers that nuance: for Task 8 (`EventMapper`), a missing optional numeric field defaults to `0` and the record is still accepted. This is called out here so it isn't mistaken for an oversight — tightening it to the full quality-flag design is a follow-up task once this slice is proven end-to-end.
- Root `pom.xml` already has `maven.compiler.release` set to `21` (done ahead of this plan, not a task step). `README.md`, `FINAL_ARCHITECTURE.md`, and `Roadmap.md` already state Java 21 / Flink 2.2.1 as the committed stack.

---

## Task 1: Category enums — Protocol, ServiceCode, ConnectionState

**Files:**
- Create: `modules/domain/src/main/java/io/netsecml/platform/domain/event/Protocol.java`
- Create: `modules/domain/src/main/java/io/netsecml/platform/domain/event/ServiceCode.java`
- Create: `modules/domain/src/main/java/io/netsecml/platform/domain/event/ConnectionState.java`
- Test: `modules/domain/src/test/java/io/netsecml/platform/domain/event/ProtocolTest.java`
- Test: `modules/domain/src/test/java/io/netsecml/platform/domain/event/ServiceCodeTest.java`
- Test: `modules/domain/src/test/java/io/netsecml/platform/domain/event/ConnectionStateTest.java`

**Interfaces:**
- Produces: `Protocol.fromZeekValue(String raw): Protocol` (enum: `TCP, UDP, ICMP, OTHER`), `ServiceCode.fromZeekValue(String raw): ServiceCode` (enum: `DNS, HTTP, SSL, UNKNOWN`), `ConnectionState.fromZeekValue(String raw): ConnectionState` (enum: `SF, S0, REJ, RSTO, RSTR, SH, OTH, OTHER`) with instance method `isFailed(): boolean`.

- [ ] **Step 1: Write the failing test for `Protocol`**

```java
package io.netsecml.platform.domain.event;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;

class ProtocolTest {
    @Test
    void mapsKnownValuesCaseInsensitively() {
        assertEquals(Protocol.TCP, Protocol.fromZeekValue("tcp"));
        assertEquals(Protocol.TCP, Protocol.fromZeekValue("TCP"));
        assertEquals(Protocol.UDP, Protocol.fromZeekValue("udp"));
        assertEquals(Protocol.ICMP, Protocol.fromZeekValue("icmp"));
    }

    @Test
    void mapsUnknownOrNullToOther() {
        assertEquals(Protocol.OTHER, Protocol.fromZeekValue("sctp"));
        assertEquals(Protocol.OTHER, Protocol.fromZeekValue(null));
        assertEquals(Protocol.OTHER, Protocol.fromZeekValue(""));
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `./mvnw -pl modules/domain test -Dtest=ProtocolTest`
Expected: FAIL — compilation error, `Protocol` does not exist.

- [ ] **Step 3: Implement `Protocol`**

```java
package io.netsecml.platform.domain.event;

public enum Protocol {
    TCP, UDP, ICMP, OTHER;

    public static Protocol fromZeekValue(String raw) {
        if (raw == null || raw.isBlank()) {
            return OTHER;
        }
        return switch (raw.trim().toLowerCase()) {
            case "tcp" -> TCP;
            case "udp" -> UDP;
            case "icmp" -> ICMP;
            default -> OTHER;
        };
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `./mvnw -pl modules/domain test -Dtest=ProtocolTest`
Expected: PASS

- [ ] **Step 5: Write the failing test for `ServiceCode`**

```java
package io.netsecml.platform.domain.event;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;

class ServiceCodeTest {
    @Test
    void mapsKnownValuesCaseInsensitively() {
        assertEquals(ServiceCode.DNS, ServiceCode.fromZeekValue("dns"));
        assertEquals(ServiceCode.HTTP, ServiceCode.fromZeekValue("HTTP"));
        assertEquals(ServiceCode.SSL, ServiceCode.fromZeekValue("ssl"));
    }

    @Test
    void mapsMissingOrUnknownToUnknown() {
        assertEquals(ServiceCode.UNKNOWN, ServiceCode.fromZeekValue(null));
        assertEquals(ServiceCode.UNKNOWN, ServiceCode.fromZeekValue(""));
        assertEquals(ServiceCode.UNKNOWN, ServiceCode.fromZeekValue("ftp"));
    }
}
```

- [ ] **Step 6: Run test to verify it fails**

Run: `./mvnw -pl modules/domain test -Dtest=ServiceCodeTest`
Expected: FAIL — `ServiceCode` does not exist.

- [ ] **Step 7: Implement `ServiceCode`**

```java
package io.netsecml.platform.domain.event;

public enum ServiceCode {
    DNS, HTTP, SSL, UNKNOWN;

    public static ServiceCode fromZeekValue(String raw) {
        if (raw == null || raw.isBlank()) {
            return UNKNOWN;
        }
        return switch (raw.trim().toLowerCase()) {
            case "dns" -> DNS;
            case "http" -> HTTP;
            case "ssl" -> SSL;
            default -> UNKNOWN;
        };
    }
}
```

- [ ] **Step 8: Run test to verify it passes**

Run: `./mvnw -pl modules/domain test -Dtest=ServiceCodeTest`
Expected: PASS

- [ ] **Step 9: Write the failing test for `ConnectionState`**

```java
package io.netsecml.platform.domain.event;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;

class ConnectionStateTest {
    @Test
    void mapsKnownStates() {
        assertEquals(ConnectionState.SF, ConnectionState.fromZeekValue("SF"));
        assertEquals(ConnectionState.S0, ConnectionState.fromZeekValue("S0"));
        assertEquals(ConnectionState.REJ, ConnectionState.fromZeekValue("REJ"));
        assertEquals(ConnectionState.OTHER, ConnectionState.fromZeekValue("XYZ"));
    }

    @Test
    void identifiesFailedStates() {
        assertTrue(ConnectionState.S0.isFailed());
        assertTrue(ConnectionState.REJ.isFailed());
        assertTrue(ConnectionState.RSTO.isFailed());
        assertTrue(ConnectionState.RSTR.isFailed());
        assertFalse(ConnectionState.SF.isFailed());
        assertFalse(ConnectionState.OTHER.isFailed());
    }
}
```

- [ ] **Step 10: Run test to verify it fails**

Run: `./mvnw -pl modules/domain test -Dtest=ConnectionStateTest`
Expected: FAIL — `ConnectionState` does not exist.

- [ ] **Step 11: Implement `ConnectionState`**

```java
package io.netsecml.platform.domain.event;

import java.util.Set;

public enum ConnectionState {
    SF, S0, REJ, RSTO, RSTR, SH, OTH, OTHER;

    private static final Set<ConnectionState> FAILED = Set.of(S0, REJ, RSTO, RSTR);

    public boolean isFailed() {
        return FAILED.contains(this);
    }

    public static ConnectionState fromZeekValue(String raw) {
        if (raw == null || raw.isBlank()) {
            return OTHER;
        }
        return switch (raw.trim().toUpperCase()) {
            case "SF" -> SF;
            case "S0" -> S0;
            case "REJ" -> REJ;
            case "RSTO" -> RSTO;
            case "RSTR" -> RSTR;
            case "SH" -> SH;
            case "OTH" -> OTH;
            default -> OTHER;
        };
    }
}
```

- [ ] **Step 12: Run test to verify it passes**

Run: `./mvnw -pl modules/domain test -Dtest=ConnectionStateTest`
Expected: PASS

- [ ] **Step 13: Commit**

```bash
git add modules/domain/src/main/java/io/netsecml/platform/domain/event/Protocol.java \
        modules/domain/src/main/java/io/netsecml/platform/domain/event/ServiceCode.java \
        modules/domain/src/main/java/io/netsecml/platform/domain/event/ConnectionState.java \
        modules/domain/src/test/java/io/netsecml/platform/domain/event/ProtocolTest.java \
        modules/domain/src/test/java/io/netsecml/platform/domain/event/ServiceCodeTest.java \
        modules/domain/src/test/java/io/netsecml/platform/domain/event/ConnectionStateTest.java
git commit -m "feat(domain): add Protocol, ServiceCode, ConnectionState category enums"
```

---

## Task 2: Connection identity and measurement value objects

**Files:**
- Create: `modules/domain/src/main/java/io/netsecml/platform/domain/event/SensorId.java`
- Create: `modules/domain/src/main/java/io/netsecml/platform/domain/event/EventId.java`
- Create: `modules/domain/src/main/java/io/netsecml/platform/domain/event/ConnectionTuple.java`
- Create: `modules/domain/src/main/java/io/netsecml/platform/domain/event/ConnectionMeasurements.java`
- Create: `modules/domain/src/main/java/io/netsecml/platform/domain/event/ConnectionLocality.java`
- Test: `modules/domain/src/test/java/io/netsecml/platform/domain/event/SensorIdTest.java`
- Test: `modules/domain/src/test/java/io/netsecml/platform/domain/event/EventIdTest.java`
- Test: `modules/domain/src/test/java/io/netsecml/platform/domain/event/ConnectionTupleTest.java`
- Test: `modules/domain/src/test/java/io/netsecml/platform/domain/event/ConnectionMeasurementsTest.java`

**Interfaces:**
- Consumes: `Protocol`, `ServiceCode`, `ConnectionState` from Task 1.
- Produces: `record SensorId(String value)` — construct with `new SensorId(String)`, validated in a compact constructor. `record EventId(String value)` — construct with `new EventId(String)`, plus a static factory `EventId.derive(SensorId, String upstreamId): EventId`. `record ConnectionTuple(String sourceIp, int sourcePort, String destinationIp, int destinationPort, Protocol protocol, ServiceCode service, ConnectionState connectionState)`, compact constructor validates ports/IPs and throws `IllegalArgumentException`. `record ConnectionMeasurements(long durationMillis, long originBytes, long responseBytes, int originPackets, int responsePackets, long missedBytes)`, compact constructor rejects negative values. `record ConnectionLocality(Boolean localOrig, Boolean localResp)`.

- [ ] **Step 1: Write the failing test for `SensorId` and `EventId`**

```java
package io.netsecml.platform.domain.event;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class SensorIdTest {
    @Test
    void rejectsBlank() {
        assertThrows(IllegalArgumentException.class, () -> new SensorId(""));
        assertThrows(IllegalArgumentException.class, () -> new SensorId(null));
    }

    @Test
    void acceptsNonBlankValue() {
        assertEquals("sensor-eu-1", new SensorId("sensor-eu-1").value());
    }
}
```

```java
package io.netsecml.platform.domain.event;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class EventIdTest {
    @Test
    void derivesNamespacedIdFromSensorAndUpstreamId() {
        SensorId sensor = new SensorId("sensor-eu-1");
        EventId id = EventId.derive(sensor, "Cabc123XYZ");
        assertEquals("sensor-eu-1:Cabc123XYZ", id.value());
    }

    @Test
    void rejectsBlankUpstreamId() {
        SensorId sensor = new SensorId("sensor-eu-1");
        assertThrows(IllegalArgumentException.class, () -> EventId.derive(sensor, ""));
    }

    @Test
    void rejectsBlankDirectConstruction() {
        assertThrows(IllegalArgumentException.class, () -> new EventId(""));
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `./mvnw -pl modules/domain test -Dtest=SensorIdTest,EventIdTest`
Expected: FAIL — `SensorId` and `EventId` do not exist.

- [ ] **Step 3: Implement `SensorId` and `EventId`**

```java
package io.netsecml.platform.domain.event;

public record SensorId(String value) {
    public SensorId {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("SensorId must not be blank");
        }
    }

    @Override
    public String toString() {
        return value;
    }
}
```

```java
package io.netsecml.platform.domain.event;

public record EventId(String value) {
    public EventId {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("EventId must not be blank");
        }
    }

    public static EventId derive(SensorId sensor, String upstreamId) {
        if (upstreamId == null || upstreamId.isBlank()) {
            throw new IllegalArgumentException("upstreamId must not be blank");
        }
        return new EventId(sensor.value() + ":" + upstreamId);
    }

    @Override
    public String toString() {
        return value;
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `./mvnw -pl modules/domain test -Dtest=SensorIdTest,EventIdTest`
Expected: PASS

- [ ] **Step 5: Write the failing test for `ConnectionTuple`**

```java
package io.netsecml.platform.domain.event;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class ConnectionTupleTest {
    @Test
    void acceptsValidTuple() {
        ConnectionTuple tuple = new ConnectionTuple(
            "10.0.0.5", 51820, "93.184.216.34", 443,
            Protocol.TCP, ServiceCode.SSL, ConnectionState.SF);
        assertEquals("10.0.0.5", tuple.sourceIp());
        assertEquals(51820, tuple.sourcePort());
        assertEquals(443, tuple.destinationPort());
        assertEquals(Protocol.TCP, tuple.protocol());
    }

    @Test
    void rejectsPortOutOfRange() {
        assertThrows(IllegalArgumentException.class, () -> new ConnectionTuple(
            "10.0.0.5", -1, "93.184.216.34", 443,
            Protocol.TCP, ServiceCode.SSL, ConnectionState.SF));
        assertThrows(IllegalArgumentException.class, () -> new ConnectionTuple(
            "10.0.0.5", 51820, "93.184.216.34", 70000,
            Protocol.TCP, ServiceCode.SSL, ConnectionState.SF));
    }

    @Test
    void rejectsBlankIp() {
        assertThrows(IllegalArgumentException.class, () -> new ConnectionTuple(
            "", 51820, "93.184.216.34", 443,
            Protocol.TCP, ServiceCode.SSL, ConnectionState.SF));
    }
}
```

- [ ] **Step 6: Run test to verify it fails**

Run: `./mvnw -pl modules/domain test -Dtest=ConnectionTupleTest`
Expected: FAIL — `ConnectionTuple` does not exist.

- [ ] **Step 7: Implement `ConnectionTuple`**

```java
package io.netsecml.platform.domain.event;

public record ConnectionTuple(String sourceIp, int sourcePort, String destinationIp, int destinationPort,
                               Protocol protocol, ServiceCode service, ConnectionState connectionState) {
    public ConnectionTuple {
        sourceIp = requireNonBlank(sourceIp, "sourceIp");
        sourcePort = requireValidPort(sourcePort, "sourcePort");
        destinationIp = requireNonBlank(destinationIp, "destinationIp");
        destinationPort = requireValidPort(destinationPort, "destinationPort");
    }

    private static String requireNonBlank(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(field + " must not be blank");
        }
        return value;
    }

    private static int requireValidPort(int port, String field) {
        if (port < 0 || port > 65535) {
            throw new IllegalArgumentException(field + " must be in [0,65535], was " + port);
        }
        return port;
    }
}
```

- [ ] **Step 8: Run test to verify it passes**

Run: `./mvnw -pl modules/domain test -Dtest=ConnectionTupleTest`
Expected: PASS

- [ ] **Step 9: Write the failing test for `ConnectionMeasurements`**

```java
package io.netsecml.platform.domain.event;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class ConnectionMeasurementsTest {
    @Test
    void acceptsNonNegativeValues() {
        ConnectionMeasurements m = new ConnectionMeasurements(1500, 2048, 4096, 10, 12, 0);
        assertEquals(1500, m.durationMillis());
        assertEquals(2048, m.originBytes());
        assertEquals(4096, m.responseBytes());
        assertEquals(10, m.originPackets());
        assertEquals(12, m.responsePackets());
        assertEquals(0, m.missedBytes());
    }

    @Test
    void rejectsNegativeValues() {
        assertThrows(IllegalArgumentException.class, () -> new ConnectionMeasurements(-1, 0, 0, 0, 0, 0));
        assertThrows(IllegalArgumentException.class, () -> new ConnectionMeasurements(0, -1, 0, 0, 0, 0));
        assertThrows(IllegalArgumentException.class, () -> new ConnectionMeasurements(0, 0, 0, -1, 0, 0));
    }
}
```

- [ ] **Step 10: Run test to verify it fails**

Run: `./mvnw -pl modules/domain test -Dtest=ConnectionMeasurementsTest`
Expected: FAIL — `ConnectionMeasurements` does not exist.

- [ ] **Step 11: Implement `ConnectionMeasurements` and `ConnectionLocality`**

```java
package io.netsecml.platform.domain.event;

public record ConnectionMeasurements(long durationMillis, long originBytes, long responseBytes,
                                      int originPackets, int responsePackets, long missedBytes) {
    public ConnectionMeasurements {
        durationMillis = requireNonNegative(durationMillis, "durationMillis");
        originBytes = requireNonNegative(originBytes, "originBytes");
        responseBytes = requireNonNegative(responseBytes, "responseBytes");
        originPackets = (int) requireNonNegative(originPackets, "originPackets");
        responsePackets = (int) requireNonNegative(responsePackets, "responsePackets");
        missedBytes = requireNonNegative(missedBytes, "missedBytes");
    }

    private static long requireNonNegative(long value, String field) {
        if (value < 0) {
            throw new IllegalArgumentException(field + " must be non-negative, was " + value);
        }
        return value;
    }
}
```

```java
package io.netsecml.platform.domain.event;

public record ConnectionLocality(Boolean localOrig, Boolean localResp) {
}
```

- [ ] **Step 12: Run all Task 2 tests to verify they pass**

Run: `./mvnw -pl modules/domain test -Dtest=SensorIdTest,EventIdTest,ConnectionTupleTest,ConnectionMeasurementsTest`
Expected: PASS

- [ ] **Step 13: Commit**

```bash
git add modules/domain/src/main/java/io/netsecml/platform/domain/event/SensorId.java \
        modules/domain/src/main/java/io/netsecml/platform/domain/event/EventId.java \
        modules/domain/src/main/java/io/netsecml/platform/domain/event/ConnectionTuple.java \
        modules/domain/src/main/java/io/netsecml/platform/domain/event/ConnectionMeasurements.java \
        modules/domain/src/main/java/io/netsecml/platform/domain/event/ConnectionLocality.java \
        modules/domain/src/test/java/io/netsecml/platform/domain/event/SensorIdTest.java \
        modules/domain/src/test/java/io/netsecml/platform/domain/event/EventIdTest.java \
        modules/domain/src/test/java/io/netsecml/platform/domain/event/ConnectionTupleTest.java \
        modules/domain/src/test/java/io/netsecml/platform/domain/event/ConnectionMeasurementsTest.java
git commit -m "feat(domain): add SensorId, EventId, ConnectionTuple, ConnectionMeasurements, ConnectionLocality records"
```

---

## Task 3: NetworkEvent, ReasonCode, MappingResult

**Files:**
- Create: `modules/domain/src/main/java/io/netsecml/platform/domain/event/ReasonCode.java`
- Create: `modules/domain/src/main/java/io/netsecml/platform/domain/event/MappingResult.java`
- Create: `modules/domain/src/main/java/io/netsecml/platform/domain/event/NetworkEvent.java`
- Test: `modules/domain/src/test/java/io/netsecml/platform/domain/event/MappingResultTest.java`
- Test: `modules/domain/src/test/java/io/netsecml/platform/domain/event/NetworkEventTest.java`

**Interfaces:**
- Consumes: `SensorId`, `EventId`, `ConnectionTuple`, `ConnectionMeasurements`, `ConnectionLocality` from Task 2.
- Produces: `ReasonCode` enum (`MALFORMED_JSON, MISSING_REQUIRED_FIELD, INVALID_TIMESTAMP, INVALID_PORT, INVALID_COUNTER`). `sealed interface MappingResult<T>` with static factories `MappingResult.valid(T value)` / `MappingResult.invalid(ReasonCode reason, String detail)`, instance methods `isValid(): boolean`, `value(): T` (throws `IllegalStateException` if invalid), `reason(): ReasonCode` (throws if valid), `detail(): String`. `record NetworkEvent(EventId eventId, Instant eventTime, SensorId sensor, ConnectionTuple connection, ConnectionMeasurements measurements, ConnectionLocality locality)`, compact constructor rejects null required fields.

- [ ] **Step 1: Write the failing test for `MappingResult`**

```java
package io.netsecml.platform.domain.event;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class MappingResultTest {
    @Test
    void validResultExposesValue() {
        MappingResult<String> result = MappingResult.valid("ok");
        assertTrue(result.isValid());
        assertEquals("ok", result.value());
        assertThrows(IllegalStateException.class, result::reason);
    }

    @Test
    void invalidResultExposesReasonAndDetail() {
        MappingResult<String> result = MappingResult.invalid(ReasonCode.INVALID_PORT, "port -1 out of range");
        assertFalse(result.isValid());
        assertEquals(ReasonCode.INVALID_PORT, result.reason());
        assertEquals("port -1 out of range", result.detail());
        assertThrows(IllegalStateException.class, result::value);
    }

    @Test
    void validRejectsNullValue() {
        assertThrows(IllegalArgumentException.class, () -> MappingResult.valid(null));
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `./mvnw -pl modules/domain test -Dtest=MappingResultTest`
Expected: FAIL — `ReasonCode` and `MappingResult` do not exist.

- [ ] **Step 3: Implement `ReasonCode` and `MappingResult`**

```java
package io.netsecml.platform.domain.event;

public enum ReasonCode {
    MALFORMED_JSON,
    MISSING_REQUIRED_FIELD,
    INVALID_TIMESTAMP,
    INVALID_PORT,
    INVALID_COUNTER
}
```

`MappingResult<T>` is a sealed interface with exactly two implementations, `Valid` and `Invalid`, both nested records. Pattern-matching `switch` over the two permitted cases is exhaustive, so no `default` branch is needed or wanted — a `default` would silently swallow a future third case instead of failing to compile.

```java
package io.netsecml.platform.domain.event;

public sealed interface MappingResult<T> permits MappingResult.Valid, MappingResult.Invalid {

    record Valid<T>(T value) implements MappingResult<T> {
        public Valid {
            if (value == null) {
                throw new IllegalArgumentException("valid() requires a non-null value");
            }
        }
    }

    record Invalid<T>(ReasonCode reason, String detail) implements MappingResult<T> {
        public Invalid {
            if (reason == null) {
                throw new IllegalArgumentException("invalid() requires a non-null reason");
            }
        }
    }

    static <T> MappingResult<T> valid(T value) {
        return new Valid<>(value);
    }

    static <T> MappingResult<T> invalid(ReasonCode reason, String detail) {
        return new Invalid<>(reason, detail);
    }

    default boolean isValid() {
        return switch (this) {
            case Valid<T> v -> true;
            case Invalid<T> i -> false;
        };
    }

    default T value() {
        return switch (this) {
            case Valid<T> v -> v.value();
            case Invalid<T> i -> throw new IllegalStateException("MappingResult is invalid, reason=" + i.reason());
        };
    }

    default ReasonCode reason() {
        return switch (this) {
            case Invalid<T> i -> i.reason();
            case Valid<T> v -> throw new IllegalStateException("MappingResult is valid, no reason available");
        };
    }

    default String detail() {
        return switch (this) {
            case Invalid<T> i -> i.detail();
            case Valid<T> v -> throw new IllegalStateException("MappingResult is valid, no detail available");
        };
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `./mvnw -pl modules/domain test -Dtest=MappingResultTest`
Expected: PASS

- [ ] **Step 5: Write the failing test for `NetworkEvent`**

```java
package io.netsecml.platform.domain.event;

import org.junit.jupiter.api.Test;
import java.time.Instant;
import static org.junit.jupiter.api.Assertions.*;

class NetworkEventTest {
    private ConnectionTuple sampleTuple() {
        return new ConnectionTuple("10.0.0.5", 51820, "93.184.216.34", 443,
            Protocol.TCP, ServiceCode.SSL, ConnectionState.SF);
    }

    private ConnectionMeasurements sampleMeasurements() {
        return new ConnectionMeasurements(1500, 2048, 4096, 10, 12, 0);
    }

    @Test
    void buildsValidEvent() {
        SensorId sensor = new SensorId("sensor-eu-1");
        EventId id = EventId.derive(sensor, "Cabc123XYZ");
        NetworkEvent event = new NetworkEvent(id, Instant.parse("2026-08-13T10:00:00Z"), sensor,
            sampleTuple(), sampleMeasurements(), new ConnectionLocality(null, null));
        assertEquals(id, event.eventId());
        assertEquals(sensor, event.sensor());
        assertEquals(443, event.connection().destinationPort());
        assertEquals(2048, event.measurements().originBytes());
    }

    @Test
    void rejectsNullRequiredFields() {
        SensorId sensor = new SensorId("sensor-eu-1");
        EventId id = EventId.derive(sensor, "Cabc123XYZ");
        assertThrows(NullPointerException.class, () -> new NetworkEvent(
            id, null, sensor, sampleTuple(), sampleMeasurements(), new ConnectionLocality(null, null)));
        assertThrows(NullPointerException.class, () -> new NetworkEvent(
            id, Instant.now(), sensor, null, sampleMeasurements(), new ConnectionLocality(null, null)));
    }
}
```

- [ ] **Step 6: Run test to verify it fails**

Run: `./mvnw -pl modules/domain test -Dtest=NetworkEventTest`
Expected: FAIL — `NetworkEvent` does not exist.

- [ ] **Step 7: Implement `NetworkEvent`**

`Objects.requireNonNull` throws `NullPointerException`, not `IllegalArgumentException` — that's why the test above asserts `NullPointerException`, unlike Tasks 1-2's blank/range checks which throw `IllegalArgumentException` directly. Keep that distinction consistent in later tasks: "was null" is an NPE, "was present but invalid" is an `IllegalArgumentException`.

```java
package io.netsecml.platform.domain.event;

import java.time.Instant;
import java.util.Objects;

public record NetworkEvent(EventId eventId, Instant eventTime, SensorId sensor, ConnectionTuple connection,
                            ConnectionMeasurements measurements, ConnectionLocality locality) {
    public NetworkEvent {
        Objects.requireNonNull(eventId, "eventId must not be null");
        Objects.requireNonNull(eventTime, "eventTime must not be null");
        Objects.requireNonNull(sensor, "sensor must not be null");
        Objects.requireNonNull(connection, "connection must not be null");
        Objects.requireNonNull(measurements, "measurements must not be null");
        Objects.requireNonNull(locality, "locality must not be null");
    }
}
```

- [ ] **Step 8: Run test to verify it passes**

Run: `./mvnw -pl modules/domain test -Dtest=NetworkEventTest`
Expected: PASS

- [ ] **Step 9: Commit**

```bash
git add modules/domain/src/main/java/io/netsecml/platform/domain/event/ReasonCode.java \
        modules/domain/src/main/java/io/netsecml/platform/domain/event/MappingResult.java \
        modules/domain/src/main/java/io/netsecml/platform/domain/event/NetworkEvent.java \
        modules/domain/src/test/java/io/netsecml/platform/domain/event/MappingResultTest.java \
        modules/domain/src/test/java/io/netsecml/platform/domain/event/NetworkEventTest.java
git commit -m "feat(domain): add ReasonCode, sealed MappingResult, NetworkEvent"
```

---

## Task 4: Feature schema value objects, ConnFeatureSchemaV1, and contract JSON

**Files:**
- Create: `contracts/features/conn-feature-schema-v1.json`
- Create: `modules/domain/src/main/java/io/netsecml/platform/domain/feature/FeatureDefinition.java`
- Create: `modules/domain/src/main/java/io/netsecml/platform/domain/feature/FeatureSchema.java`
- Create: `modules/domain/src/main/java/io/netsecml/platform/domain/feature/FeatureVector.java`
- Create: `modules/domain/src/main/java/io/netsecml/platform/domain/feature/ConnFeatureSchemaV1.java`
- Test: `modules/domain/src/test/java/io/netsecml/platform/domain/feature/FeatureVectorTest.java`
- Test: `modules/domain/src/test/java/io/netsecml/platform/domain/feature/ConnFeatureSchemaV1Test.java`

**Interfaces:**
- Produces: `record FeatureDefinition(int index, String name, String unit, MissingPolicy missingPolicy, String formula)` with nested `enum MissingPolicy { REQUIRED, DEFAULT_ZERO }`. `record FeatureSchema(String id, String semanticVersion, String contentHash, List<FeatureDefinition> definitions)` with an added `featureCount(): int` method. `record FeatureVector(String eventId, Instant eventTime, String schemaId, String schemaHash, float[] values, int qualityFlags)` — compact constructor defensively copies `values`, and the `values()` accessor is overridden to return a fresh defensive copy on every read. `ConnFeatureSchemaV1.SCHEMA: FeatureSchema` (static constant), `ConnFeatureSchemaV1.CONTENT_HASH: String` (static constant, `"f42fb1bebb2efe3acc5de634c6a7bb3d6f97fc021207f75d77652533b1c01e1b"`).

Note on the content hash: the value above is the exact SHA-256 of the `contracts/features/conn-feature-schema-v1.json` content given in Step 5 below, byte-for-byte including the trailing newline. Copy the JSON content exactly as shown — the test in Step 9 recomputes the hash from the file on disk and will fail on any whitespace difference.

- [ ] **Step 1: Write the failing test for `FeatureVector`**

```java
package io.netsecml.platform.domain.feature;

import org.junit.jupiter.api.Test;
import java.time.Instant;
import static org.junit.jupiter.api.Assertions.*;

class FeatureVectorTest {
    @Test
    void storesValuesAndReturnsDefensiveCopy() {
        float[] values = new float[]{1f, 2f, 3f};
        FeatureVector vector = new FeatureVector("sensor-eu-1:abc", Instant.parse("2026-08-13T10:00:00Z"),
            "conn-feature-v1", "hash123", values, 0);

        values[0] = -1f;
        assertEquals(1f, vector.values()[0], "mutating the array passed to the constructor must not affect internal state");

        float[] returned = vector.values();
        returned[0] = 999f;
        assertEquals(1f, vector.values()[0], "mutating the returned array must not affect internal state");
        assertEquals(3, vector.values().length);
    }

    @Test
    void rejectsNullValues() {
        assertThrows(IllegalArgumentException.class, () -> new FeatureVector(
            "sensor-eu-1:abc", Instant.now(), "conn-feature-v1", "hash123", null, 0));
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `./mvnw -pl modules/domain test -Dtest=FeatureVectorTest`
Expected: FAIL — `FeatureVector` does not exist.

- [ ] **Step 3: Implement `FeatureDefinition`, `FeatureSchema`, `FeatureVector`**

```java
package io.netsecml.platform.domain.feature;

public record FeatureDefinition(int index, String name, String unit, MissingPolicy missingPolicy, String formula) {
    public enum MissingPolicy { REQUIRED, DEFAULT_ZERO }
}
```

```java
package io.netsecml.platform.domain.feature;

import java.util.Collections;
import java.util.List;

public record FeatureSchema(String id, String semanticVersion, String contentHash, List<FeatureDefinition> definitions) {
    public FeatureSchema {
        definitions = Collections.unmodifiableList(definitions);
    }

    public int featureCount() {
        return definitions.size();
    }
}
```

```java
package io.netsecml.platform.domain.feature;

import java.time.Instant;
import java.util.Arrays;

public record FeatureVector(String eventId, Instant eventTime, String schemaId, String schemaHash,
                             float[] values, int qualityFlags) {
    public FeatureVector {
        if (eventId == null || eventId.isBlank()) {
            throw new IllegalArgumentException("eventId must not be blank");
        }
        if (values == null) {
            throw new IllegalArgumentException("values must not be null");
        }
        values = Arrays.copyOf(values, values.length);
    }

    @Override
    public float[] values() {
        return Arrays.copyOf(values, values.length);
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `./mvnw -pl modules/domain test -Dtest=FeatureVectorTest`
Expected: PASS

- [ ] **Step 5: Create the canonical feature schema contract JSON**

Create `contracts/features/conn-feature-schema-v1.json` with exactly this content (including the trailing newline after the final `}`):

```json
{
  "id": "conn-feature-v1",
  "semanticVersion": "1.0.0",
  "sourceContractVersion": "zeek-conn-source-v1",
  "inputDtype": "float32",
  "featureCount": 20,
  "features": [
    { "index": 0, "name": "duration_ms", "unit": "milliseconds", "missingPolicy": "DEFAULT_ZERO", "formula": "zeek duration seconds * 1000, non-negative" },
    { "index": 1, "name": "origin_bytes", "unit": "bytes", "missingPolicy": "DEFAULT_ZERO", "formula": "orig_bytes, non-negative" },
    { "index": 2, "name": "response_bytes", "unit": "bytes", "missingPolicy": "DEFAULT_ZERO", "formula": "resp_bytes, non-negative" },
    { "index": 3, "name": "origin_packets", "unit": "count", "missingPolicy": "DEFAULT_ZERO", "formula": "orig_pkts, non-negative" },
    { "index": 4, "name": "response_packets", "unit": "count", "missingPolicy": "DEFAULT_ZERO", "formula": "resp_pkts, non-negative" },
    { "index": 5, "name": "total_bytes", "unit": "bytes", "missingPolicy": "DEFAULT_ZERO", "formula": "origin_bytes + response_bytes" },
    { "index": 6, "name": "total_packets", "unit": "count", "missingPolicy": "DEFAULT_ZERO", "formula": "origin_packets + response_packets" },
    { "index": 7, "name": "bytes_per_packet", "unit": "bytes", "missingPolicy": "DEFAULT_ZERO", "formula": "total_bytes / max(1, total_packets)" },
    { "index": 8, "name": "response_origin_byte_ratio", "unit": "ratio", "missingPolicy": "DEFAULT_ZERO", "formula": "response_bytes / max(1, origin_bytes)" },
    { "index": 9, "name": "destination_port", "unit": "port", "missingPolicy": "REQUIRED", "formula": "id_resp_p, exact integer as float32" },
    { "index": 10, "name": "destination_is_well_known", "unit": "boolean", "missingPolicy": "REQUIRED", "formula": "1 if destination_port in [1,1023] else 0" },
    { "index": 11, "name": "protocol_tcp", "unit": "boolean", "missingPolicy": "REQUIRED", "formula": "1 if proto == tcp else 0" },
    { "index": 12, "name": "protocol_udp", "unit": "boolean", "missingPolicy": "REQUIRED", "formula": "1 if proto == udp else 0" },
    { "index": 13, "name": "service_dns", "unit": "boolean", "missingPolicy": "REQUIRED", "formula": "1 if service == dns else 0" },
    { "index": 14, "name": "service_http", "unit": "boolean", "missingPolicy": "REQUIRED", "formula": "1 if service == http else 0" },
    { "index": 15, "name": "service_ssl", "unit": "boolean", "missingPolicy": "REQUIRED", "formula": "1 if service == ssl else 0" },
    { "index": 16, "name": "connection_failed", "unit": "boolean", "missingPolicy": "REQUIRED", "formula": "1 if conn_state in [S0,REJ,RSTO,RSTR] else 0" },
    { "index": 17, "name": "source_connections_5m", "unit": "count", "missingPolicy": "DEFAULT_ZERO", "formula": "count of prior connections for (sensor,sourceIp) in trailing 5 one-minute buckets" },
    { "index": 18, "name": "source_bytes_5m", "unit": "bytes", "missingPolicy": "DEFAULT_ZERO", "formula": "total_bytes sum for (sensor,sourceIp) in trailing 5 one-minute buckets" },
    { "index": 19, "name": "source_failed_connections_5m", "unit": "count", "missingPolicy": "DEFAULT_ZERO", "formula": "connection_failed count for (sensor,sourceIp) in trailing 5 one-minute buckets" }
  ]
}
```

- [ ] **Step 6: Write the failing test for `ConnFeatureSchemaV1`**

```java
package io.netsecml.platform.domain.feature;

import org.junit.jupiter.api.Test;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import static org.junit.jupiter.api.Assertions.*;

class ConnFeatureSchemaV1Test {
    @Test
    void hasTwentyContiguousUniquelyNamedFeatures() {
        FeatureSchema schema = ConnFeatureSchemaV1.SCHEMA;
        assertEquals(20, schema.featureCount());
        for (int i = 0; i < 20; i++) {
            assertEquals(i, schema.definitions().get(i).index(), "index " + i + " out of order");
        }
        long uniqueNames = schema.definitions().stream().map(FeatureDefinition::name).distinct().count();
        assertEquals(20, uniqueNames, "feature names must be unique");
    }

    @Test
    void contentHashMatchesCommittedContractFile() throws IOException, NoSuchAlgorithmException {
        Path contractPath = Paths.get("..", "..", "contracts", "features", "conn-feature-schema-v1.json");
        byte[] bytes = Files.readAllBytes(contractPath);
        byte[] digest = MessageDigest.getInstance("SHA-256").digest(bytes);
        StringBuilder hex = new StringBuilder();
        for (byte b : digest) {
            hex.append(String.format("%02x", b));
        }
        assertEquals(ConnFeatureSchemaV1.CONTENT_HASH, hex.toString(),
            "ConnFeatureSchemaV1.CONTENT_HASH must match the SHA-256 of contracts/features/conn-feature-schema-v1.json — "
            + "if you edited the JSON, recompute the hash and update the constant");
    }
}
```

- [ ] **Step 7: Run test to verify it fails**

Run: `./mvnw -pl modules/domain test -Dtest=ConnFeatureSchemaV1Test`
Expected: FAIL — `ConnFeatureSchemaV1` does not exist.

- [ ] **Step 8: Implement `ConnFeatureSchemaV1`**

```java
package io.netsecml.platform.domain.feature;

import java.util.List;
import static io.netsecml.platform.domain.feature.FeatureDefinition.MissingPolicy.DEFAULT_ZERO;
import static io.netsecml.platform.domain.feature.FeatureDefinition.MissingPolicy.REQUIRED;

public final class ConnFeatureSchemaV1 {
    public static final String CONTENT_HASH =
        "f42fb1bebb2efe3acc5de634c6a7bb3d6f97fc021207f75d77652533b1c01e1b";

    public static final FeatureSchema SCHEMA = new FeatureSchema(
        "conn-feature-v1", "1.0.0", CONTENT_HASH, List.of(
            new FeatureDefinition(0, "duration_ms", "milliseconds", DEFAULT_ZERO, "zeek duration seconds * 1000, non-negative"),
            new FeatureDefinition(1, "origin_bytes", "bytes", DEFAULT_ZERO, "orig_bytes, non-negative"),
            new FeatureDefinition(2, "response_bytes", "bytes", DEFAULT_ZERO, "resp_bytes, non-negative"),
            new FeatureDefinition(3, "origin_packets", "count", DEFAULT_ZERO, "orig_pkts, non-negative"),
            new FeatureDefinition(4, "response_packets", "count", DEFAULT_ZERO, "resp_pkts, non-negative"),
            new FeatureDefinition(5, "total_bytes", "bytes", DEFAULT_ZERO, "origin_bytes + response_bytes"),
            new FeatureDefinition(6, "total_packets", "count", DEFAULT_ZERO, "origin_packets + response_packets"),
            new FeatureDefinition(7, "bytes_per_packet", "bytes", DEFAULT_ZERO, "total_bytes / max(1, total_packets)"),
            new FeatureDefinition(8, "response_origin_byte_ratio", "ratio", DEFAULT_ZERO, "response_bytes / max(1, origin_bytes)"),
            new FeatureDefinition(9, "destination_port", "port", REQUIRED, "id_resp_p, exact integer as float32"),
            new FeatureDefinition(10, "destination_is_well_known", "boolean", REQUIRED, "1 if destination_port in [1,1023] else 0"),
            new FeatureDefinition(11, "protocol_tcp", "boolean", REQUIRED, "1 if proto == tcp else 0"),
            new FeatureDefinition(12, "protocol_udp", "boolean", REQUIRED, "1 if proto == udp else 0"),
            new FeatureDefinition(13, "service_dns", "boolean", REQUIRED, "1 if service == dns else 0"),
            new FeatureDefinition(14, "service_http", "boolean", REQUIRED, "1 if service == http else 0"),
            new FeatureDefinition(15, "service_ssl", "boolean", REQUIRED, "1 if service == ssl else 0"),
            new FeatureDefinition(16, "connection_failed", "boolean", REQUIRED, "1 if conn_state in [S0,REJ,RSTO,RSTR] else 0"),
            new FeatureDefinition(17, "source_connections_5m", "count", DEFAULT_ZERO, "count of prior connections for (sensor,sourceIp) in trailing 5 one-minute buckets"),
            new FeatureDefinition(18, "source_bytes_5m", "bytes", DEFAULT_ZERO, "total_bytes sum for (sensor,sourceIp) in trailing 5 one-minute buckets"),
            new FeatureDefinition(19, "source_failed_connections_5m", "count", DEFAULT_ZERO, "connection_failed count for (sensor,sourceIp) in trailing 5 one-minute buckets")
        ));

    private ConnFeatureSchemaV1() {
    }
}
```

- [ ] **Step 9: Run test to verify it passes**

Run: `./mvnw -pl modules/domain test -Dtest=ConnFeatureSchemaV1Test`
Expected: PASS. If the hash test fails, re-verify the JSON file matches Step 5 exactly byte-for-byte (no extra trailing spaces, single trailing newline).

- [ ] **Step 10: Commit**

```bash
git add contracts/features/conn-feature-schema-v1.json \
        modules/domain/src/main/java/io/netsecml/platform/domain/feature/FeatureDefinition.java \
        modules/domain/src/main/java/io/netsecml/platform/domain/feature/FeatureSchema.java \
        modules/domain/src/main/java/io/netsecml/platform/domain/feature/FeatureVector.java \
        modules/domain/src/main/java/io/netsecml/platform/domain/feature/ConnFeatureSchemaV1.java \
        modules/domain/src/test/java/io/netsecml/platform/domain/feature/FeatureVectorTest.java \
        modules/domain/src/test/java/io/netsecml/platform/domain/feature/ConnFeatureSchemaV1Test.java
git commit -m "feat(domain): add feature schema records and frozen conn-feature-v1 contract"
```

---

## Task 5: SourceWindowState — bounded rolling 5-minute state

**Files:**
- Create: `modules/domain/src/main/java/io/netsecml/platform/domain/feature/SourceWindowState.java`
- Create: `modules/domain/src/main/java/io/netsecml/platform/domain/feature/SourceKey.java`
- Test: `modules/domain/src/test/java/io/netsecml/platform/domain/feature/SourceWindowStateTest.java`

**Interfaces:**
- Consumes: `SensorId` from Task 2.
- Produces: `record SourceKey(SensorId sensor, String sourceIp)`. `SourceWindowState` — a plain immutable class, deliberately **not** a record: its public surface is `empty()`/`record(...)`, not four parallel `long[]` arrays, and exposing those arrays as record components would leak the bounded-bucket representation as public API. `SourceWindowState.empty(): SourceWindowState`; `SourceWindowState.record(long bucketEpochMinute, long bytes, boolean failed): SourceWindowState` (returns a **new** instance, does not mutate `this`); `connectionCount5m(): long`, `byteSum5m(): long`, `failedCount5m(): long`.

- [ ] **Step 1: Write the failing test**

```java
package io.netsecml.platform.domain.feature;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class SourceWindowStateTest {
    @Test
    void emptyStateHasZeroSums() {
        SourceWindowState state = SourceWindowState.empty();
        assertEquals(0, state.connectionCount5m());
        assertEquals(0, state.byteSum5m());
        assertEquals(0, state.failedCount5m());
    }

    @Test
    void recordAccumulatesWithinFiveMinuteWindow() {
        SourceWindowState state = SourceWindowState.empty();
        long minute = 1000L;
        state = state.record(minute, 500, false);
        state = state.record(minute, 300, true);
        state = state.record(minute + 1, 200, false);
        assertEquals(3, state.connectionCount5m());
        assertEquals(1000, state.byteSum5m());
        assertEquals(1, state.failedCount5m());
    }

    @Test
    void recordIsImmutable() {
        SourceWindowState original = SourceWindowState.empty();
        SourceWindowState updated = original.record(1000L, 500, false);
        assertEquals(0, original.connectionCount5m(), "original state must not be mutated");
        assertEquals(1, updated.connectionCount5m());
    }

    @Test
    void bucketsOlderThanFiveMinutesRollOff() {
        SourceWindowState state = SourceWindowState.empty();
        state = state.record(1000L, 999, false);
        state = state.record(1006L, 1, false);
        assertEquals(1, state.connectionCount5m(), "bucket from minute 1000 is 6 minutes behind minute 1006 and must roll off");
        assertEquals(1, state.byteSum5m());
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `./mvnw -pl modules/domain test -Dtest=SourceWindowStateTest`
Expected: FAIL — `SourceWindowState` does not exist.

- [ ] **Step 3: Implement `SourceKey` and `SourceWindowState`**

```java
package io.netsecml.platform.domain.feature;

import io.netsecml.platform.domain.event.SensorId;

public record SourceKey(SensorId sensor, String sourceIp) {
    public SourceKey {
        if (sensor == null) {
            throw new IllegalArgumentException("sensor must not be null");
        }
        if (sourceIp == null || sourceIp.isBlank()) {
            throw new IllegalArgumentException("sourceIp must not be blank");
        }
    }
}
```

```java
package io.netsecml.platform.domain.feature;

import java.util.Arrays;

/**
 * Five fixed one-minute buckets holding rolling connection-count/byte-sum/failed-count totals
 * for a single (sensor, sourceIp) key. Bounded by construction: exactly 5 longs per array,
 * never a per-IP set or list. See PILOT_ARCHITECTURE.md section 6 for the design rationale.
 */
public final class SourceWindowState {
    private static final int BUCKET_COUNT = 5;

    private final long[] bucketMinutes;
    private final long[] connectionCounts;
    private final long[] byteSums;
    private final long[] failedCounts;

    private SourceWindowState(long[] bucketMinutes, long[] connectionCounts, long[] byteSums, long[] failedCounts) {
        this.bucketMinutes = bucketMinutes;
        this.connectionCounts = connectionCounts;
        this.byteSums = byteSums;
        this.failedCounts = failedCounts;
    }

    public static SourceWindowState empty() {
        long[] minutes = new long[BUCKET_COUNT];
        Arrays.fill(minutes, Long.MIN_VALUE);
        return new SourceWindowState(minutes, new long[BUCKET_COUNT], new long[BUCKET_COUNT], new long[BUCKET_COUNT]);
    }

    public SourceWindowState record(long bucketEpochMinute, long bytes, boolean failed) {
        long[] minutes = Arrays.copyOf(bucketMinutes, BUCKET_COUNT);
        long[] counts = Arrays.copyOf(connectionCounts, BUCKET_COUNT);
        long[] sums = Arrays.copyOf(byteSums, BUCKET_COUNT);
        long[] fails = Arrays.copyOf(failedCounts, BUCKET_COUNT);

        int slot = (int) Math.floorMod(bucketEpochMinute, (long) BUCKET_COUNT);
        if (minutes[slot] != bucketEpochMinute) {
            minutes[slot] = bucketEpochMinute;
            counts[slot] = 0;
            sums[slot] = 0;
            fails[slot] = 0;
        }
        counts[slot] += 1;
        sums[slot] += bytes;
        if (failed) {
            fails[slot] += 1;
        }
        return new SourceWindowState(minutes, counts, sums, fails);
    }

    private long sumWithinWindow(long[] values, long currentMinuteHint) {
        long total = 0;
        for (int i = 0; i < BUCKET_COUNT; i++) {
            if (bucketMinutes[i] != Long.MIN_VALUE && currentMinuteHint - bucketMinutes[i] < BUCKET_COUNT) {
                total += values[i];
            }
        }
        return total;
    }

    private long latestBucketMinute() {
        long latest = Long.MIN_VALUE;
        for (long m : bucketMinutes) {
            if (m > latest) {
                latest = m;
            }
        }
        return latest;
    }

    public long connectionCount5m() {
        return sumWithinWindow(connectionCounts, latestBucketMinute());
    }

    public long byteSum5m() {
        return sumWithinWindow(byteSums, latestBucketMinute());
    }

    public long failedCount5m() {
        return sumWithinWindow(failedCounts, latestBucketMinute());
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `./mvnw -pl modules/domain test -Dtest=SourceWindowStateTest`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add modules/domain/src/main/java/io/netsecml/platform/domain/feature/SourceKey.java \
        modules/domain/src/main/java/io/netsecml/platform/domain/feature/SourceWindowState.java \
        modules/domain/src/test/java/io/netsecml/platform/domain/feature/SourceWindowStateTest.java
git commit -m "feat(domain): add SourceKey record and bounded SourceWindowState rolling buckets"
```

---

## Task 6: Zeek conn source contract and fixtures

**Files:**
- Create: `contracts/source/zeek-conn-source-v1.json`
- Create: `tests/fixtures/zeek_conn/valid-tcp-ssl.json`
- Create: `tests/fixtures/zeek_conn/valid-udp-dns.json`
- Create: `tests/fixtures/zeek_conn/missing-required-field.json`
- Create: `tests/fixtures/zeek_conn/invalid-port.json`
- Create: `tests/fixtures/zeek_conn/malformed.json`
- Test: `modules/domain/src/test/java/io/netsecml/platform/domain/event/ZeekConnFixturesReadableTest.java`

**Interfaces:**
- Produces: five fixture files under `tests/fixtures/zeek_conn/` consumed by Task 7's parser tests and Task 8's mapper tests, and Task 14's end-to-end test.

- [ ] **Step 1: Write the failing sanity test**

This test only proves the fixture directory and files are readable JSON (or, for the malformed one, readable bytes) before later tasks build real behavior on top of them.

```java
package io.netsecml.platform.domain.event;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import static org.junit.jupiter.api.Assertions.*;

class ZeekConnFixturesReadableTest {
    private static final Path FIXTURES = Paths.get("..", "..", "tests", "fixtures", "zeek_conn");

    @Test
    void validFixturesParseAsJsonObjects() throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        for (String name : new String[]{"valid-tcp-ssl.json", "valid-udp-dns.json"}) {
            byte[] bytes = Files.readAllBytes(FIXTURES.resolve(name));
            assertTrue(mapper.readTree(bytes).isObject(), name + " must parse as a JSON object");
        }
    }

    @Test
    void malformedFixtureIsNotValidJson() throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        byte[] bytes = Files.readAllBytes(FIXTURES.resolve("malformed.json"));
        assertThrows(Exception.class, () -> mapper.readTree(bytes));
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `./mvnw -pl modules/domain test -Dtest=ZeekConnFixturesReadableTest`
Expected: FAIL — fixture files do not exist yet, and `domain` has no Jackson dependency yet either (this surfaces now, fixed in Step 3).

- [ ] **Step 3: Add Jackson as a test-scope dependency to `domain`**

This test module needs Jackson only to sanity-check fixtures are valid JSON; production `domain` code still imports nothing from Jackson (checked in Step 5).

Modify `modules/domain/pom.xml` — add inside `<dependencies>`, before the closing tag:

```xml
    <dependency>
      <groupId>com.fasterxml.jackson.core</groupId>
      <artifactId>jackson-databind</artifactId>
      <version>2.17.1</version>
      <scope>test</scope>
    </dependency>
```

- [ ] **Step 4: Create the source contract and fixture files**

Create `contracts/source/zeek-conn-source-v1.json`:

```json
{
  "id": "zeek-conn-source-v1",
  "description": "Required and optional fields for a Zeek conn.log record delivered as one Kafka JSON message.",
  "requiredFields": [
    { "name": "id", "type": "string", "notes": "upstream record id, namespaced with sensor to form EventId" },
    { "name": "ts", "type": "number", "notes": "epoch seconds, may include fractional milliseconds" },
    { "name": "id_orig_h", "type": "string", "notes": "source IP" },
    { "name": "id_orig_p", "type": "integer", "notes": "source port, 0-65535" },
    { "name": "id_resp_h", "type": "string", "notes": "destination IP" },
    { "name": "id_resp_p", "type": "integer", "notes": "destination port, 0-65535" },
    { "name": "proto", "type": "string", "notes": "tcp | udp | icmp | other, case-insensitive" },
    { "name": "conn_state", "type": "string", "notes": "Zeek connection state code" }
  ],
  "optionalFields": [
    { "name": "service", "type": "string", "notes": "dns | http | ssl | other, absent if unidentified" },
    { "name": "duration", "type": "number", "notes": "seconds, absent for connections with no data" },
    { "name": "orig_bytes", "type": "integer" },
    { "name": "resp_bytes", "type": "integer" },
    { "name": "orig_pkts", "type": "integer" },
    { "name": "resp_pkts", "type": "integer" },
    { "name": "missed_bytes", "type": "integer" },
    { "name": "local_orig", "type": "boolean" },
    { "name": "local_resp", "type": "boolean" }
  ],
  "invalidRecordPolicy": "Malformed JSON or a missing required field routes to netsec.conn.dlq.v1 with a reason code; unknown additive fields are tolerated and ignored."
}
```

Create `tests/fixtures/zeek_conn/valid-tcp-ssl.json`:

```json
{
  "id": "Cabc123XYZ",
  "ts": 1786608000.123456,
  "id_orig_h": "10.0.0.5",
  "id_orig_p": 51820,
  "id_resp_h": "93.184.216.34",
  "id_resp_p": 443,
  "proto": "tcp",
  "service": "ssl",
  "conn_state": "SF",
  "duration": 1.5,
  "orig_bytes": 2048,
  "resp_bytes": 4096,
  "orig_pkts": 10,
  "resp_pkts": 12,
  "missed_bytes": 0,
  "local_orig": true,
  "local_resp": false
}
```

Create `tests/fixtures/zeek_conn/valid-udp-dns.json`:

```json
{
  "id": "Cdef456UVW",
  "ts": 1786608010.0,
  "id_orig_h": "10.0.0.5",
  "id_orig_p": 53421,
  "id_resp_h": "8.8.8.8",
  "id_resp_p": 53,
  "proto": "udp",
  "service": "dns",
  "conn_state": "SF",
  "orig_bytes": 64,
  "resp_bytes": 128,
  "orig_pkts": 1,
  "resp_pkts": 1
}
```

Create `tests/fixtures/zeek_conn/missing-required-field.json` (missing `id_resp_p`):

```json
{
  "id": "Cghi789RST",
  "ts": 1786608020.0,
  "id_orig_h": "10.0.0.5",
  "id_orig_p": 51820,
  "id_resp_h": "93.184.216.34",
  "proto": "tcp",
  "conn_state": "SF"
}
```

Create `tests/fixtures/zeek_conn/invalid-port.json`:

```json
{
  "id": "Cjkl012MNO",
  "ts": 1786608030.0,
  "id_orig_h": "10.0.0.5",
  "id_orig_p": 51820,
  "id_resp_h": "93.184.216.34",
  "id_resp_p": 70000,
  "proto": "tcp",
  "conn_state": "SF"
}
```

Create `tests/fixtures/zeek_conn/malformed.json`:

```
{ "id": "Cbroken", "ts": 1786608040.0, "id_orig_h": "10.0.0.5"
```

- [ ] **Step 5: Run test to verify it passes**

Run: `./mvnw -pl modules/domain test -Dtest=ZeekConnFixturesReadableTest`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add contracts/source/zeek-conn-source-v1.json \
        tests/fixtures/zeek_conn/valid-tcp-ssl.json \
        tests/fixtures/zeek_conn/valid-udp-dns.json \
        tests/fixtures/zeek_conn/missing-required-field.json \
        tests/fixtures/zeek_conn/invalid-port.json \
        tests/fixtures/zeek_conn/malformed.json \
        modules/domain/pom.xml \
        modules/domain/src/test/java/io/netsecml/platform/domain/event/ZeekConnFixturesReadableTest.java
git commit -m "feat(contracts): add zeek-conn-source-v1 contract and sanitized fixtures"
```

---

## Task 7: ZeekConnEvent DTO and JsonZeekConnParser

**Files:**
- Modify: `modules/adapter-kafka/pom.xml`
- Create: `modules/adapter-kafka/src/main/java/io/netsecml/platform/adapter/kafka/dto/ZeekConnEvent.java`
- Create: `modules/adapter-kafka/src/main/java/io/netsecml/platform/adapter/kafka/parser/JsonZeekConnParser.java`
- Test: `modules/adapter-kafka/src/test/java/io/netsecml/platform/adapter/kafka/parser/JsonZeekConnParserTest.java`

**Interfaces:**
- Consumes: `MappingResult<T>`, `ReasonCode` from Task 3 (`domain` is already a dependency of `adapter-kafka` per its existing `pom.xml`).
- Produces: `ZeekConnEvent` — a Jackson-deserializable **record** (Jackson 2.12+ deserializes records natively using the canonical constructor, honoring `@JsonProperty` on each component). Component accessors are the record's own — `id()`, `ts()`, `idOrigH()`, `idOrigP()`, `idRespH()`, `idRespP()`, `proto()`, `service()` (nullable), `connState()`, `duration()` (nullable `Double`), `origBytes()` (nullable `Long`), `respBytes()` (nullable `Long`), `origPkts()` (nullable `Long`), `respPkts()` (nullable `Long`), `missedBytes()` (nullable `Long`), `localOrig()` (nullable `Boolean`), `localResp()` (nullable `Boolean`). `JsonZeekConnParser.parse(byte[] json): MappingResult<ZeekConnEvent>`.

- [ ] **Step 1: Add Jackson to `adapter-kafka`**

Modify `modules/adapter-kafka/pom.xml` — add inside `<dependencies>`:

```xml
    <dependency>
      <groupId>com.fasterxml.jackson.core</groupId>
      <artifactId>jackson-databind</artifactId>
      <version>2.17.1</version>
    </dependency>
```

- [ ] **Step 2: Write the failing test**

```java
package io.netsecml.platform.adapter.kafka.parser;

import io.netsecml.platform.adapter.kafka.dto.ZeekConnEvent;
import io.netsecml.platform.domain.event.MappingResult;
import io.netsecml.platform.domain.event.ReasonCode;
import org.junit.jupiter.api.Test;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import static org.junit.jupiter.api.Assertions.*;

class JsonZeekConnParserTest {
    private final JsonZeekConnParser parser = new JsonZeekConnParser();

    private byte[] fixture(String name) throws IOException {
        return Files.readAllBytes(Paths.get("..", "..", "tests", "fixtures", "zeek_conn", name));
    }

    @Test
    void parsesValidTcpSslFixture() throws IOException {
        MappingResult<ZeekConnEvent> result = parser.parse(fixture("valid-tcp-ssl.json"));
        assertTrue(result.isValid());
        ZeekConnEvent dto = result.value();
        assertEquals("Cabc123XYZ", dto.id());
        assertEquals("10.0.0.5", dto.idOrigH());
        assertEquals(443, dto.idRespP());
        assertEquals("ssl", dto.service());
        assertEquals(2048L, dto.origBytes());
    }

    @Test
    void missingRequiredFieldFailsToParse() throws IOException {
        MappingResult<ZeekConnEvent> result = parser.parse(fixture("missing-required-field.json"));
        assertFalse(result.isValid(), "this fixture omits id_resp_p, a Jackson-required int component of the record, "
            + "so it fails to deserialize; EventMapper's own required-field checks (Task 8) are a separate, "
            + "additional layer for fields Jackson cannot enforce structurally");
        assertEquals(ReasonCode.MALFORMED_JSON, result.reason());
    }

    @Test
    void rejectsMalformedJson() throws IOException {
        MappingResult<ZeekConnEvent> result = parser.parse(fixture("malformed.json"));
        assertFalse(result.isValid());
        assertEquals(ReasonCode.MALFORMED_JSON, result.reason());
    }
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `./mvnw -pl modules/adapter-kafka test -Dtest=JsonZeekConnParserTest`
Expected: FAIL — `ZeekConnEvent` and `JsonZeekConnParser` do not exist.

- [ ] **Step 4: Implement `ZeekConnEvent`**

`idOrigP` and `idRespP` are `@JsonProperty(required = true)` primitive `int` components (not boxed `Integer`), so `missing-required-field.json`, which omits `id_resp_p`, fails Jackson deserialization with a `MismatchedInputException` — that is what Step 2's second test exercises. All other components are boxed/nullable because they are genuinely optional per the source contract.

```java
package io.netsecml.platform.adapter.kafka.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

public record ZeekConnEvent(
    @JsonProperty(value = "id", required = true) String id,
    @JsonProperty(value = "ts", required = true) double ts,
    @JsonProperty(value = "id_orig_h", required = true) String idOrigH,
    @JsonProperty(value = "id_orig_p", required = true) int idOrigP,
    @JsonProperty(value = "id_resp_h", required = true) String idRespH,
    @JsonProperty(value = "id_resp_p", required = true) int idRespP,
    @JsonProperty(value = "proto", required = true) String proto,
    @JsonProperty("service") String service,
    @JsonProperty(value = "conn_state", required = true) String connState,
    @JsonProperty("duration") Double duration,
    @JsonProperty("orig_bytes") Long origBytes,
    @JsonProperty("resp_bytes") Long respBytes,
    @JsonProperty("orig_pkts") Long origPkts,
    @JsonProperty("resp_pkts") Long respPkts,
    @JsonProperty("missed_bytes") Long missedBytes,
    @JsonProperty("local_orig") Boolean localOrig,
    @JsonProperty("local_resp") Boolean localResp
) {
}
```

- [ ] **Step 5: Implement `JsonZeekConnParser`**

```java
package io.netsecml.platform.adapter.kafka.parser;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.netsecml.platform.adapter.kafka.dto.ZeekConnEvent;
import io.netsecml.platform.domain.event.MappingResult;
import io.netsecml.platform.domain.event.ReasonCode;

public final class JsonZeekConnParser {
    private final ObjectMapper objectMapper = new ObjectMapper();

    public MappingResult<ZeekConnEvent> parse(byte[] json) {
        try {
            ZeekConnEvent dto = objectMapper.readValue(json, ZeekConnEvent.class);
            return MappingResult.valid(dto);
        } catch (Exception e) {
            return MappingResult.invalid(ReasonCode.MALFORMED_JSON, e.getMessage());
        }
    }
}
```

- [ ] **Step 6: Run test to verify it passes**

Run: `./mvnw -pl modules/adapter-kafka test -Dtest=JsonZeekConnParserTest`
Expected: PASS. If deserialization into the record silently succeeds with `idRespP == 0` instead of failing on the missing-field fixture, Jackson's records module did not register `required = true` enforcement as expected for this Jackson version — pin `jackson-databind` to exactly `2.17.1` as specified and re-run before investigating further.

- [ ] **Step 7: Commit**

```bash
git add modules/adapter-kafka/pom.xml \
        modules/adapter-kafka/src/main/java/io/netsecml/platform/adapter/kafka/dto/ZeekConnEvent.java \
        modules/adapter-kafka/src/main/java/io/netsecml/platform/adapter/kafka/parser/JsonZeekConnParser.java \
        modules/adapter-kafka/src/test/java/io/netsecml/platform/adapter/kafka/parser/JsonZeekConnParserTest.java
git commit -m "feat(adapter-kafka): add ZeekConnEvent record DTO and JsonZeekConnParser"
```

---

## Task 8: EventMapper — DTO to domain NetworkEvent

**Files:**
- Create: `modules/adapter-kafka/src/main/java/io/netsecml/platform/adapter/kafka/mapper/EventMapper.java`
- Test: `modules/adapter-kafka/src/test/java/io/netsecml/platform/adapter/kafka/mapper/EventMapperTest.java`

**Interfaces:**
- Consumes: `ZeekConnEvent` (Task 7), `NetworkEvent`, `ConnectionTuple`, `ConnectionMeasurements`, `ConnectionLocality`, `MappingResult`, `ReasonCode`, `SensorId`, `EventId`, `Protocol`, `ServiceCode`, `ConnectionState` (Tasks 1-3).
- Produces: `EventMapper.map(ZeekConnEvent dto, SensorId sensor): MappingResult<NetworkEvent>`. Per the Global Constraints simplification, missing optional numeric fields (`duration`, `origBytes`, `respBytes`, `origPkts`, `respPkts`, `missedBytes`) default to `0` rather than rejecting the record.

- [ ] **Step 1: Write the failing test**

```java
package io.netsecml.platform.adapter.kafka.mapper;

import io.netsecml.platform.adapter.kafka.dto.ZeekConnEvent;
import io.netsecml.platform.adapter.kafka.parser.JsonZeekConnParser;
import io.netsecml.platform.domain.event.*;
import org.junit.jupiter.api.Test;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import static org.junit.jupiter.api.Assertions.*;

class EventMapperTest {
    private final JsonZeekConnParser parser = new JsonZeekConnParser();
    private final EventMapper mapper = new EventMapper();
    private final SensorId sensor = new SensorId("sensor-eu-1");

    private ZeekConnEvent fixture(String name) throws IOException {
        byte[] bytes = Files.readAllBytes(Paths.get("..", "..", "tests", "fixtures", "zeek_conn", name));
        return parser.parse(bytes).value();
    }

    @Test
    void mapsValidFixtureToNetworkEvent() throws IOException {
        MappingResult<NetworkEvent> result = mapper.map(fixture("valid-tcp-ssl.json"), sensor);
        assertTrue(result.isValid());
        NetworkEvent event = result.value();
        assertEquals("sensor-eu-1:Cabc123XYZ", event.eventId().value());
        assertEquals(Instant.ofEpochMilli(1786608000123L), event.eventTime());
        assertEquals(Protocol.TCP, event.connection().protocol());
        assertEquals(ServiceCode.SSL, event.connection().service());
        assertEquals(ConnectionState.SF, event.connection().connectionState());
        assertEquals(443, event.connection().destinationPort());
        assertEquals(1500L, event.measurements().durationMillis());
        assertEquals(2048L, event.measurements().originBytes());
    }

    @Test
    void defaultsMissingOptionalNumericFieldsToZero() throws IOException {
        MappingResult<NetworkEvent> result = mapper.map(fixture("valid-udp-dns.json"), sensor);
        assertTrue(result.isValid());
        assertEquals(0L, result.value().measurements().durationMillis(), "duration was absent in this fixture");
        assertEquals(0L, result.value().measurements().missedBytes(), "missed_bytes was absent in this fixture");
    }

    @Test
    void rejectsInvalidPortWithReasonCode() throws IOException {
        MappingResult<NetworkEvent> result = mapper.map(fixture("invalid-port.json"), sensor);
        assertFalse(result.isValid());
        assertEquals(ReasonCode.INVALID_PORT, result.reason());
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `./mvnw -pl modules/adapter-kafka test -Dtest=EventMapperTest`
Expected: FAIL — `EventMapper` does not exist.

- [ ] **Step 3: Implement `EventMapper`**

```java
package io.netsecml.platform.adapter.kafka.mapper;

import io.netsecml.platform.adapter.kafka.dto.ZeekConnEvent;
import io.netsecml.platform.domain.event.*;
import java.time.Instant;

public final class EventMapper {
    public MappingResult<NetworkEvent> map(ZeekConnEvent dto, SensorId sensor) {
        if (dto.id() == null || dto.id().isBlank()) {
            return MappingResult.invalid(ReasonCode.MISSING_REQUIRED_FIELD, "id is required");
        }
        if (Double.isNaN(dto.ts()) || dto.ts() < 0) {
            return MappingResult.invalid(ReasonCode.INVALID_TIMESTAMP, "ts must be a non-negative number, was " + dto.ts());
        }
        if (dto.idOrigH() == null || dto.idOrigH().isBlank()
                || dto.idRespH() == null || dto.idRespH().isBlank()) {
            return MappingResult.invalid(ReasonCode.MISSING_REQUIRED_FIELD, "id_orig_h and id_resp_h are required");
        }
        if (dto.connState() == null || dto.connState().isBlank()) {
            return MappingResult.invalid(ReasonCode.MISSING_REQUIRED_FIELD, "conn_state is required");
        }

        EventId eventId = EventId.derive(sensor, dto.id());
        Instant eventTime = Instant.ofEpochMilli(Math.round(dto.ts() * 1000.0));

        ConnectionTuple tuple;
        try {
            tuple = new ConnectionTuple(
                dto.idOrigH(), dto.idOrigP(), dto.idRespH(), dto.idRespP(),
                Protocol.fromZeekValue(dto.proto()),
                ServiceCode.fromZeekValue(dto.service()),
                ConnectionState.fromZeekValue(dto.connState()));
        } catch (IllegalArgumentException e) {
            return MappingResult.invalid(ReasonCode.INVALID_PORT, e.getMessage());
        }

        long durationMillis = dto.duration() == null ? 0L : Math.round(dto.duration() * 1000.0);
        long originBytes = dto.origBytes() == null ? 0L : dto.origBytes();
        long responseBytes = dto.respBytes() == null ? 0L : dto.respBytes();
        int originPackets = dto.origPkts() == null ? 0 : dto.origPkts().intValue();
        int responsePackets = dto.respPkts() == null ? 0 : dto.respPkts().intValue();
        long missedBytes = dto.missedBytes() == null ? 0L : dto.missedBytes();

        ConnectionMeasurements measurements;
        try {
            measurements = new ConnectionMeasurements(
                durationMillis, originBytes, responseBytes, originPackets, responsePackets, missedBytes);
        } catch (IllegalArgumentException e) {
            return MappingResult.invalid(ReasonCode.INVALID_COUNTER, e.getMessage());
        }

        ConnectionLocality locality = new ConnectionLocality(dto.localOrig(), dto.localResp());

        NetworkEvent event = new NetworkEvent(eventId, eventTime, sensor, tuple, measurements, locality);
        return MappingResult.valid(event);
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `./mvnw -pl modules/adapter-kafka test -Dtest=EventMapperTest`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add modules/adapter-kafka/src/main/java/io/netsecml/platform/adapter/kafka/mapper/EventMapper.java \
        modules/adapter-kafka/src/test/java/io/netsecml/platform/adapter/kafka/mapper/EventMapperTest.java
git commit -m "feat(adapter-kafka): add EventMapper from ZeekConnEvent to NetworkEvent"
```

---

## Task 9: EventFeatureExtractor — event-level features (indices 0-16)

**Files:**
- Create: `modules/application/src/main/java/io/netsecml/platform/application/feature/EventFeatureExtractor.java`
- Test: `modules/application/src/test/java/io/netsecml/platform/application/feature/EventFeatureExtractorTest.java`

**Interfaces:**
- Consumes: `NetworkEvent`, `ConnectionTuple`, `ConnectionMeasurements`, `Protocol`, `ServiceCode` (domain, Tasks 1-3).
- Produces: `EventFeatureExtractor.extractEventLevel(NetworkEvent event): float[17]` — indices 0 through 16 of `conn-feature-v1`, in order. This is a static-style pure function with no instance state; later tasks call it as `new EventFeatureExtractor().extractEventLevel(event)`.

- [ ] **Step 1: Write the failing test**

```java
package io.netsecml.platform.application.feature;

import io.netsecml.platform.domain.event.*;
import org.junit.jupiter.api.Test;
import java.time.Instant;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;

class EventFeatureExtractorTest {
    private final EventFeatureExtractor extractor = new EventFeatureExtractor();

    private NetworkEvent event(Protocol proto, ServiceCode service, ConnectionState state,
                                long durationMillis, long originBytes, long responseBytes,
                                int originPackets, int responsePackets, int destinationPort) {
        SensorId sensor = new SensorId("sensor-eu-1");
        ConnectionTuple tuple = new ConnectionTuple("10.0.0.5", 51820, "93.184.216.34", destinationPort,
            proto, service, state);
        ConnectionMeasurements measurements = new ConnectionMeasurements(
            durationMillis, originBytes, responseBytes, originPackets, responsePackets, 0);
        return new NetworkEvent(EventId.derive(sensor, "abc"), Instant.now(), sensor, tuple, measurements,
            new ConnectionLocality(null, null));
    }

    @Test
    void extractsNormalTcpSslConnection() {
        NetworkEvent e = event(Protocol.TCP, ServiceCode.SSL, ConnectionState.SF, 1500, 2048, 4096, 10, 12, 443);
        float[] v = extractor.extractEventLevel(e);
        assertArrayEquals(new float[]{
            1500f,   // 0 duration_ms
            2048f,   // 1 origin_bytes
            4096f,   // 2 response_bytes
            10f,     // 3 origin_packets
            12f,     // 4 response_packets
            6144f,   // 5 total_bytes
            22f,     // 6 total_packets
            279.27273f, // 7 bytes_per_packet = 6144/22
            2f,      // 8 response_origin_byte_ratio = 4096/2048
            443f,    // 9 destination_port
            1f,      // 10 destination_is_well_known (443 in [1,1023])
            1f,      // 11 protocol_tcp
            0f,      // 12 protocol_udp
            0f,      // 13 service_dns
            0f,      // 14 service_http
            1f,      // 15 service_ssl
            0f       // 16 connection_failed (SF is not a failed state)
        }, v, 0.001f);
    }

    @Test
    void extractsZeroPacketConnectionWithoutDivideByZero() {
        NetworkEvent e = event(Protocol.UDP, ServiceCode.DNS, ConnectionState.S0, 0, 0, 0, 0, 0, 53);
        float[] v = extractor.extractEventLevel(e);
        assertArrayEquals(new float[]{
            0f, 0f, 0f, 0f, 0f,
            0f,      // 5 total_bytes
            0f,      // 6 total_packets
            0f,      // 7 bytes_per_packet = 0 / max(1,0) = 0
            0f,      // 8 response_origin_byte_ratio = 0 / max(1,0) = 0
            53f,     // 9 destination_port
            1f,      // 10 well-known
            0f,      // 11 protocol_tcp
            1f,      // 12 protocol_udp
            1f,      // 13 service_dns
            0f, 0f,
            1f       // 16 connection_failed (S0 is a failed state)
        }, v, 0.001f);
    }

    @Test
    void nonWellKnownHighPortIsZero() {
        NetworkEvent e = event(Protocol.TCP, ServiceCode.UNKNOWN, ConnectionState.SF, 100, 10, 10, 1, 1, 51820);
        float[] v = extractor.extractEventLevel(e);
        assertArrayEquals(0f, v[10], 0.001f, "port 51820 is not in [1,1023]");
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `./mvnw -pl modules/application test -Dtest=EventFeatureExtractorTest`
Expected: FAIL — `EventFeatureExtractor` does not exist.

- [ ] **Step 3: Implement `EventFeatureExtractor`**

```java
package io.netsecml.platform.application.feature;

import io.netsecml.platform.domain.event.NetworkEvent;
import io.netsecml.platform.domain.event.Protocol;
import io.netsecml.platform.domain.event.ServiceCode;

public final class EventFeatureExtractor {
    public float[] extractEventLevel(NetworkEvent event) {
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
            durationMillis,
            originBytes,
            responseBytes,
            originPackets,
            responsePackets,
            totalBytes,
            totalPackets,
            (float) totalBytes / Math.max(1, totalPackets),
            (float) responseBytes / Math.max(1, originBytes),
            destinationPort,
            (destinationPort >= 1 && destinationPort <= 1023) ? 1f : 0f,
            protocol == Protocol.TCP ? 1f : 0f,
            protocol == Protocol.UDP ? 1f : 0f,
            service == ServiceCode.DNS ? 1f : 0f,
            service == ServiceCode.HTTP ? 1f : 0f,
            service == ServiceCode.SSL ? 1f : 0f,
            event.connection().connectionState().isFailed() ? 1f : 0f
        };
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `./mvnw -pl modules/application test -Dtest=EventFeatureExtractorTest`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add modules/application/src/main/java/io/netsecml/platform/application/feature/EventFeatureExtractor.java \
        modules/application/src/test/java/io/netsecml/platform/application/feature/EventFeatureExtractorTest.java
git commit -m "feat(application): add EventFeatureExtractor for conn-feature-v1 indices 0-16"
```

---

## Task 10: BuildFeaturesUseCase — full 20-feature vector with windowed state

**Files:**
- Create: `modules/domain/src/main/java/io/netsecml/platform/domain/feature/FeatureBuildResult.java`
- Create: `modules/ports/src/main/java/io/netsecml/platform/port/in/BuildFeaturesUseCase.java`
- Create: `modules/application/src/main/java/io/netsecml/platform/application/usecase/BuildFeaturesUseCaseImpl.java`
- Test: `modules/application/src/test/java/io/netsecml/platform/application/usecase/BuildFeaturesUseCaseImplTest.java`

**Interfaces:**
- Consumes: `EventFeatureExtractor` (Task 9), `SourceWindowState`, `SourceKey`, `ConnFeatureSchemaV1`, `FeatureVector` (Tasks 4-5), `NetworkEvent`.
- Produces: `record FeatureBuildResult(FeatureVector vector, SourceWindowState newState)`. `BuildFeaturesUseCase` port interface with `build(NetworkEvent event, SourceWindowState currentState): FeatureBuildResult`. `BuildFeaturesUseCaseImpl` — the only implementation, used directly by `adapter-flink` in Task 12.

- [ ] **Step 1: Write the failing test**

```java
package io.netsecml.platform.application.usecase;

import io.netsecml.platform.domain.event.*;
import io.netsecml.platform.domain.feature.FeatureBuildResult;
import io.netsecml.platform.domain.feature.SourceWindowState;
import org.junit.jupiter.api.Test;
import java.time.Instant;
import static org.junit.jupiter.api.Assertions.*;

class BuildFeaturesUseCaseImplTest {
    private final BuildFeaturesUseCaseImpl useCase = new BuildFeaturesUseCaseImpl();

    private NetworkEvent event(Instant eventTime, long originBytes, long responseBytes, boolean failed) {
        SensorId sensor = new SensorId("sensor-eu-1");
        ConnectionState state = failed ? ConnectionState.S0 : ConnectionState.SF;
        ConnectionTuple tuple = new ConnectionTuple("10.0.0.5", 51820, "93.184.216.34", 443,
            Protocol.TCP, ServiceCode.SSL, state);
        ConnectionMeasurements measurements = new ConnectionMeasurements(1000, originBytes, responseBytes, 5, 5, 0);
        return new NetworkEvent(EventId.derive(sensor, eventTime.toString()), eventTime, sensor, tuple, measurements,
            new ConnectionLocality(null, null));
    }

    @Test
    void producesTwentyValueVectorWithFrozenSchemaIdentity() {
        NetworkEvent e = event(Instant.ofEpochSecond(60_000), 100, 200, false);
        FeatureBuildResult result = useCase.build(e, SourceWindowState.empty());
        assertEquals(20, result.vector().values().length);
        assertEquals("conn-feature-v1", result.vector().schemaId());
        assertEquals("f42fb1bebb2efe3acc5de634c6a7bb3d6f97fc021207f75d77652533b1c01e1b", result.vector().schemaHash());
        assertEquals(e.eventId().value(), result.vector().eventId());
    }

    @Test
    void windowedFeaturesAccumulateAcrossCallsForSameKey() {
        SourceWindowState state = SourceWindowState.empty();

        NetworkEvent first = event(Instant.ofEpochSecond(60_000), 100, 200, false);
        FeatureBuildResult r1 = useCase.build(first, state);
        assertEquals(1f, r1.vector().values()[17], "source_connections_5m after first event");
        assertEquals(300f, r1.vector().values()[18], "source_bytes_5m after first event (total_bytes=100+200)");
        assertEquals(0f, r1.vector().values()[19], "source_failed_connections_5m, first event was not failed");

        NetworkEvent second = event(Instant.ofEpochSecond(60_030), 50, 50, true);
        FeatureBuildResult r2 = useCase.build(second, r1.newState());
        assertEquals(2f, r2.vector().values()[17], "source_connections_5m after second event, same minute bucket");
        assertEquals(400f, r2.vector().values()[18], "300 + total_bytes(50+50)=100 = 400");
        assertEquals(1f, r2.vector().values()[19], "second event was failed (S0)");
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `./mvnw -pl modules/application test -Dtest=BuildFeaturesUseCaseImplTest`
Expected: FAIL — `FeatureBuildResult`, `BuildFeaturesUseCase`, `BuildFeaturesUseCaseImpl` do not exist.

- [ ] **Step 3: Implement `FeatureBuildResult`**

```java
package io.netsecml.platform.domain.feature;

public record FeatureBuildResult(FeatureVector vector, SourceWindowState newState) {
}
```

- [ ] **Step 4: Implement the `BuildFeaturesUseCase` port**

```java
package io.netsecml.platform.port.in;

import io.netsecml.platform.domain.event.NetworkEvent;
import io.netsecml.platform.domain.feature.FeatureBuildResult;
import io.netsecml.platform.domain.feature.SourceWindowState;

public interface BuildFeaturesUseCase {
    FeatureBuildResult build(NetworkEvent event, SourceWindowState currentState);
}
```

- [ ] **Step 5: `ports` dependency check**

`modules/application/pom.xml` already declares a dependency on `ports` (confirmed in the existing POM) — no change needed here. Skip to Step 6.

- [ ] **Step 6: Implement `BuildFeaturesUseCaseImpl`**

```java
package io.netsecml.platform.application.usecase;

import io.netsecml.platform.application.feature.EventFeatureExtractor;
import io.netsecml.platform.domain.event.NetworkEvent;
import io.netsecml.platform.domain.feature.ConnFeatureSchemaV1;
import io.netsecml.platform.domain.feature.FeatureBuildResult;
import io.netsecml.platform.domain.feature.FeatureVector;
import io.netsecml.platform.domain.feature.SourceWindowState;
import io.netsecml.platform.port.in.BuildFeaturesUseCase;

public final class BuildFeaturesUseCaseImpl implements BuildFeaturesUseCase {
    private final EventFeatureExtractor eventFeatureExtractor = new EventFeatureExtractor();

    @Override
    public FeatureBuildResult build(NetworkEvent event, SourceWindowState currentState) {
        float[] eventLevel = eventFeatureExtractor.extractEventLevel(event);

        long totalBytes = event.measurements().originBytes() + event.measurements().responseBytes();
        boolean failed = event.connection().connectionState().isFailed();
        long bucketMinute = event.eventTime().getEpochSecond() / 60;
        SourceWindowState newState = currentState.record(bucketMinute, totalBytes, failed);

        float[] values = new float[20];
        System.arraycopy(eventLevel, 0, values, 0, 17);
        values[17] = newState.connectionCount5m();
        values[18] = newState.byteSum5m();
        values[19] = newState.failedCount5m();

        FeatureVector vector = new FeatureVector(
            event.eventId().value(),
            event.eventTime(),
            ConnFeatureSchemaV1.SCHEMA.id(),
            ConnFeatureSchemaV1.CONTENT_HASH,
            values,
            0);

        return new FeatureBuildResult(vector, newState);
    }
}
```

- [ ] **Step 7: Run test to verify it passes**

Run: `./mvnw -pl modules/application test -Dtest=BuildFeaturesUseCaseImplTest`
Expected: PASS

- [ ] **Step 8: Commit**

```bash
git add modules/domain/src/main/java/io/netsecml/platform/domain/feature/FeatureBuildResult.java \
        modules/ports/src/main/java/io/netsecml/platform/port/in/BuildFeaturesUseCase.java \
        modules/application/src/main/java/io/netsecml/platform/application/usecase/BuildFeaturesUseCaseImpl.java \
        modules/application/src/test/java/io/netsecml/platform/application/usecase/BuildFeaturesUseCaseImplTest.java
git commit -m "feat(application): add BuildFeaturesUseCase producing the full 20-value FeatureVector"
```

---

## Task 11: Flink dependencies and ParseMapValidateFunction

**Files:**
- Modify: `pom.xml` (root — add Flink to `dependencyManagement`)
- Modify: `modules/adapter-flink/pom.xml`
- Create: `modules/adapter-flink/src/main/java/io/netsecml/platform/adapter/flink/process/ParseMapValidateFunction.java`
- Create: `modules/adapter-flink/src/main/java/io/netsecml/platform/adapter/flink/process/RejectedRecord.java`
- Test: `modules/adapter-flink/src/test/java/io/netsecml/platform/adapter/flink/process/ParseMapValidateFunctionTest.java`

**Interfaces:**
- Consumes: `JsonZeekConnParser`, `EventMapper` (Task 7-8), `SensorId`, `NetworkEvent`, `MappingResult`, `ReasonCode` (Tasks 1-3).
- Produces: `record RejectedRecord(byte[] rawPayload, ReasonCode reason, String detail)` — compact constructor defensively copies `rawPayload`, accessor overridden to return a fresh copy on read, same pattern as `FeatureVector` in Task 4. `ParseMapValidateFunction` — a Flink `ProcessFunction<byte[], NetworkEvent>` with a public static `OutputTag<RejectedRecord> REJECTED_TAG` side output, constructed as `new ParseMapValidateFunction(SensorId sensor)`.

- [ ] **Step 1: Add Flink to root `dependencyManagement` and to `adapter-flink`**

Modify root `pom.xml` — add a `flink.version` property next to the existing `<junit.version>`:

```xml
    <flink.version>2.2.1</flink.version>
```

Add Flink entries to `<dependencyManagement><dependencies>`, after the `junit-bom` entry:

```xml
      <dependency>
        <groupId>org.apache.flink</groupId>
        <artifactId>flink-streaming-java</artifactId>
        <version>${flink.version}</version>
      </dependency>
      <dependency>
        <groupId>org.apache.flink</groupId>
        <artifactId>flink-test-utils</artifactId>
        <version>${flink.version}</version>
        <scope>test</scope>
      </dependency>
```

Modify `modules/adapter-flink/pom.xml` — add inside `<dependencies>`:

```xml
    <dependency>
      <groupId>org.apache.flink</groupId>
      <artifactId>flink-streaming-java</artifactId>
    </dependency>
    <dependency>
      <groupId>io.netsecml.platform</groupId>
      <artifactId>adapter-kafka</artifactId>
      <version>${project.version}</version>
    </dependency>
    <dependency>
      <groupId>org.apache.flink</groupId>
      <artifactId>flink-test-utils</artifactId>
      <scope>test</scope>
    </dependency>
```

If `flink-streaming-java:2.2.1` fails to resolve against Maven Central at build time, check `https://repo.maven.apache.org/maven2/org/apache/flink/flink-streaming-java/` for the exact latest 2.x patch version available and use that instead — do not silently downgrade to a 1.x version, since this plan's code assumes the Source/Sink V2 APIs that are the only APIs available in the 2.x line.

- [ ] **Step 2: Write the failing test**

```java
package io.netsecml.platform.adapter.flink.process;

import io.netsecml.platform.domain.event.NetworkEvent;
import io.netsecml.platform.domain.event.ReasonCode;
import io.netsecml.platform.domain.event.SensorId;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.junit.jupiter.api.Test;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

class ParseMapValidateFunctionTest {
    private byte[] fixture(String name) throws IOException {
        return Files.readAllBytes(Paths.get("..", "..", "tests", "fixtures", "zeek_conn", name));
    }

    @Test
    void validRecordReachesMainOutput() throws Exception {
        ParseMapValidateFunction function = new ParseMapValidateFunction(new SensorId("sensor-eu-1"));
        OneInputStreamOperatorTestHarness<byte[], NetworkEvent> harness =
            ProcessFunctionTestHarnesses.forProcessFunction(function);

        harness.processElement(new StreamRecord<>(fixture("valid-tcp-ssl.json")));

        List<NetworkEvent> output = harness.extractOutputValues();
        assertEquals(1, output.size());
        assertEquals("sensor-eu-1:Cabc123XYZ", output.get(0).eventId().value());

        harness.close();
    }

    @Test
    void malformedRecordGoesToRejectedSideOutputAndJobKeepsRunning() throws Exception {
        ParseMapValidateFunction function = new ParseMapValidateFunction(new SensorId("sensor-eu-1"));
        OneInputStreamOperatorTestHarness<byte[], NetworkEvent> harness =
            ProcessFunctionTestHarnesses.forProcessFunction(function);

        harness.processElement(new StreamRecord<>(fixture("malformed.json")));
        harness.processElement(new StreamRecord<>(fixture("valid-tcp-ssl.json")));

        assertEquals(1, harness.extractOutputValues().size(), "malformed record must not reach main output");
        List<StreamRecord<RejectedRecord>> rejected =
            harness.getSideOutput(ParseMapValidateFunction.REJECTED_TAG);
        assertEquals(1, rejected.size());
        assertEquals(ReasonCode.MALFORMED_JSON, rejected.get(0).getValue().reason());

        harness.close();
    }

    @Test
    void invalidPortGoesToRejectedSideOutputWithReasonCode() throws Exception {
        ParseMapValidateFunction function = new ParseMapValidateFunction(new SensorId("sensor-eu-1"));
        OneInputStreamOperatorTestHarness<byte[], NetworkEvent> harness =
            ProcessFunctionTestHarnesses.forProcessFunction(function);

        harness.processElement(new StreamRecord<>(fixture("invalid-port.json")));

        List<StreamRecord<RejectedRecord>> rejected =
            harness.getSideOutput(ParseMapValidateFunction.REJECTED_TAG);
        assertEquals(1, rejected.size());
        assertEquals(ReasonCode.INVALID_PORT, rejected.get(0).getValue().reason());

        harness.close();
    }
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `./mvnw -pl modules/adapter-flink test -Dtest=ParseMapValidateFunctionTest`
Expected: FAIL — `ParseMapValidateFunction` and `RejectedRecord` do not exist.

- [ ] **Step 4: Implement `RejectedRecord` and `ParseMapValidateFunction`**

```java
package io.netsecml.platform.adapter.flink.process;

import io.netsecml.platform.domain.event.ReasonCode;
import java.util.Arrays;

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

```java
package io.netsecml.platform.adapter.flink.process;

import io.netsecml.platform.adapter.kafka.dto.ZeekConnEvent;
import io.netsecml.platform.adapter.kafka.mapper.EventMapper;
import io.netsecml.platform.adapter.kafka.parser.JsonZeekConnParser;
import io.netsecml.platform.domain.event.MappingResult;
import io.netsecml.platform.domain.event.NetworkEvent;
import io.netsecml.platform.domain.event.SensorId;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public final class ParseMapValidateFunction extends ProcessFunction<byte[], NetworkEvent> {
    public static final OutputTag<RejectedRecord> REJECTED_TAG =
        new OutputTag<RejectedRecord>("rejected") {};

    private final SensorId sensor;
    private transient JsonZeekConnParser parser;
    private transient EventMapper mapper;

    public ParseMapValidateFunction(SensorId sensor) {
        this.sensor = sensor;
    }

    @Override
    public void open(org.apache.flink.configuration.OpenContext openContext) {
        parser = new JsonZeekConnParser();
        mapper = new EventMapper();
    }

    @Override
    public void processElement(byte[] rawPayload, Context ctx, Collector<NetworkEvent> out) {
        MappingResult<ZeekConnEvent> parsed = parser.parse(rawPayload);
        if (!parsed.isValid()) {
            ctx.output(REJECTED_TAG, new RejectedRecord(rawPayload, parsed.reason(), parsed.detail()));
            return;
        }

        MappingResult<NetworkEvent> mapped = mapper.map(parsed.value(), sensor);
        if (!mapped.isValid()) {
            ctx.output(REJECTED_TAG, new RejectedRecord(rawPayload, mapped.reason(), mapped.detail()));
            return;
        }

        out.collect(mapped.value());
    }
}
```

Flink 2.x replaced the single-argument `open(Configuration)` lifecycle method with `open(OpenContext)` (the old signature is deprecated, not removed, in early 2.x but the `OpenContext` overload is the one to implement going forward). If `org.apache.flink.configuration.OpenContext` does not resolve against the pinned `flink-streaming-java:2.2.1` artifact, fall back to `@Override public void open(org.apache.flink.configuration.Configuration parameters)` — functionally identical for this task, since neither implementation reads anything from the parameter.

- [ ] **Step 5: Run test to verify it passes**

Run: `./mvnw -pl modules/adapter-flink test -Dtest=ParseMapValidateFunctionTest`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add pom.xml \
        modules/adapter-flink/pom.xml \
        modules/adapter-flink/src/main/java/io/netsecml/platform/adapter/flink/process/ParseMapValidateFunction.java \
        modules/adapter-flink/src/main/java/io/netsecml/platform/adapter/flink/process/RejectedRecord.java \
        modules/adapter-flink/src/test/java/io/netsecml/platform/adapter/flink/process/ParseMapValidateFunctionTest.java
git commit -m "feat(adapter-flink): add ParseMapValidateFunction with rejected-record side output"
```

---

## Task 12: ConnFeatureProcessFunction — keyed, stateful feature extraction

**Files:**
- Create: `modules/adapter-flink/src/main/java/io/netsecml/platform/adapter/flink/process/SourceKeySelector.java`
- Create: `modules/adapter-flink/src/main/java/io/netsecml/platform/adapter/flink/process/ConnFeatureProcessFunction.java`
- Test: `modules/adapter-flink/src/test/java/io/netsecml/platform/adapter/flink/process/ConnFeatureProcessFunctionTest.java`

**Interfaces:**
- Consumes: `NetworkEvent`, `SourceKey`, `SourceWindowState`, `BuildFeaturesUseCaseImpl`, `FeatureVector`, `FeatureBuildResult` (Tasks 2-10).
- Produces: `SourceKeySelector implements KeySelector<NetworkEvent, SourceKey>`. `ConnFeatureProcessFunction extends KeyedProcessFunction<SourceKey, NetworkEvent, FeatureVector>` — holds `ValueState<SourceWindowState>` internally, calls `BuildFeaturesUseCaseImpl.build(...)` per event, persists the returned `newState()`.

- [ ] **Step 1: Write the failing test**

```java
package io.netsecml.platform.adapter.flink.process;

import io.netsecml.platform.domain.event.*;
import io.netsecml.platform.domain.feature.FeatureVector;
import io.netsecml.platform.domain.feature.SourceKey;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.junit.jupiter.api.Test;
import java.time.Instant;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

class ConnFeatureProcessFunctionTest {
    private NetworkEvent event(SensorId sensor, String sourceIp, Instant eventTime, long originBytes) {
        ConnectionTuple tuple = new ConnectionTuple(sourceIp, 51820, "93.184.216.34", 443,
            Protocol.TCP, ServiceCode.SSL, ConnectionState.SF);
        ConnectionMeasurements measurements = new ConnectionMeasurements(1000, originBytes, 0, 1, 1, 0);
        return new NetworkEvent(EventId.derive(sensor, eventTime.toString() + sourceIp), eventTime, sensor,
            tuple, measurements, new ConnectionLocality(null, null));
    }

    @Test
    void stateAccumulatesPerKeyAcrossEvents() throws Exception {
        KeyedOneInputStreamOperatorTestHarness<SourceKey, NetworkEvent, FeatureVector> harness =
            ProcessFunctionTestHarnesses.forKeyedProcessFunction(
                new ConnFeatureProcessFunction(), new SourceKeySelector(), TypeInformation.of(SourceKey.class));

        SensorId sensor = new SensorId("sensor-eu-1");
        harness.processElement(new StreamRecord<>(event(sensor, "10.0.0.5", Instant.ofEpochSecond(60_000), 100)));
        harness.processElement(new StreamRecord<>(event(sensor, "10.0.0.5", Instant.ofEpochSecond(60_010), 200)));
        harness.processElement(new StreamRecord<>(event(sensor, "10.0.0.9", Instant.ofEpochSecond(60_010), 999)));

        List<FeatureVector> output = harness.extractOutputValues();
        assertEquals(3, output.size());
        assertEquals(1f, output.get(0).values()[17], "first event for 10.0.0.5");
        assertEquals(2f, output.get(1).values()[17], "second event for 10.0.0.5, same key");
        assertEquals(1f, output.get(2).values()[17], "first event for a different source IP, independent state");

        harness.close();
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `./mvnw -pl modules/adapter-flink test -Dtest=ConnFeatureProcessFunctionTest`
Expected: FAIL — `SourceKeySelector` and `ConnFeatureProcessFunction` do not exist.

- [ ] **Step 3: Implement `SourceKeySelector` and `ConnFeatureProcessFunction`**

```java
package io.netsecml.platform.adapter.flink.process;

import io.netsecml.platform.domain.event.NetworkEvent;
import io.netsecml.platform.domain.feature.SourceKey;
import org.apache.flink.api.java.functions.KeySelector;

public final class SourceKeySelector implements KeySelector<NetworkEvent, SourceKey> {
    @Override
    public SourceKey getKey(NetworkEvent event) {
        return new SourceKey(event.sensor(), event.connection().sourceIp());
    }
}
```

```java
package io.netsecml.platform.adapter.flink.process;

import io.netsecml.platform.application.usecase.BuildFeaturesUseCaseImpl;
import io.netsecml.platform.domain.event.NetworkEvent;
import io.netsecml.platform.domain.feature.FeatureBuildResult;
import io.netsecml.platform.domain.feature.FeatureVector;
import io.netsecml.platform.domain.feature.SourceKey;
import io.netsecml.platform.domain.feature.SourceWindowState;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.OpenContext;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public final class ConnFeatureProcessFunction extends KeyedProcessFunction<SourceKey, NetworkEvent, FeatureVector> {
    private transient ValueState<SourceWindowState> windowState;
    private transient BuildFeaturesUseCaseImpl useCase;

    @Override
    public void open(OpenContext openContext) {
        ValueStateDescriptor<SourceWindowState> descriptor = new ValueStateDescriptor<>(
            "source-window-state", TypeInformation.of(SourceWindowState.class));
        windowState = getRuntimeContext().getState(descriptor);
        useCase = new BuildFeaturesUseCaseImpl();
    }

    @Override
    public void processElement(NetworkEvent event, Context ctx, Collector<FeatureVector> out) throws Exception {
        SourceWindowState currentState = windowState.value();
        if (currentState == null) {
            currentState = SourceWindowState.empty();
        }

        FeatureBuildResult result = useCase.build(event, currentState);
        windowState.update(result.newState());
        out.collect(result.vector());
    }
}
```

Same `open(OpenContext)` versus `open(Configuration)` caveat as Task 11 Step 4 applies here.

- [ ] **Step 4: Run test to verify it passes**

Run: `./mvnw -pl modules/adapter-flink test -Dtest=ConnFeatureProcessFunctionTest`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add modules/adapter-flink/src/main/java/io/netsecml/platform/adapter/flink/process/SourceKeySelector.java \
        modules/adapter-flink/src/main/java/io/netsecml/platform/adapter/flink/process/ConnFeatureProcessFunction.java \
        modules/adapter-flink/src/test/java/io/netsecml/platform/adapter/flink/process/ConnFeatureProcessFunctionTest.java
git commit -m "feat(adapter-flink): add ConnFeatureProcessFunction with per-key ValueState"
```

---

## Task 13: Kafka sinks — FeatureVectorSerializer and RejectedRecordSerializer

**Files:**
- Modify: `modules/adapter-kafka/pom.xml`
- Modify: root `pom.xml` (add `kafka-clients` to `dependencyManagement`)
- Create: `modules/adapter-kafka/src/main/java/io/netsecml/platform/adapter/kafka/sink/FeatureVectorSerializer.java`
- Create: `modules/adapter-kafka/src/main/java/io/netsecml/platform/adapter/kafka/sink/RejectedRecordPayload.java`
- Create: `modules/adapter-kafka/src/main/java/io/netsecml/platform/adapter/kafka/sink/RejectedRecordSerializer.java`
- Test: `modules/adapter-kafka/src/test/java/io/netsecml/platform/adapter/kafka/sink/FeatureVectorSerializerTest.java`
- Test: `modules/adapter-kafka/src/test/java/io/netsecml/platform/adapter/kafka/sink/RejectedRecordSerializerTest.java`

**Interfaces:**
- Consumes: `FeatureVector` (Task 4), `ReasonCode` (Task 3). `adapter-kafka` cannot depend on `adapter-flink` (that would invert the hexagonal dependency direction — adapters don't depend on each other), so the DLQ/invalid-event payload is a small standalone record defined in this task, not the Task 11 `RejectedRecord`.
- Produces: `FeatureVectorSerializer implements org.apache.kafka.common.serialization.Serializer<FeatureVector>` — a plain Kafka `Serializer`, not Flink-specific, so it has no Flink dependency and can be unit-tested without a Flink harness. `record RejectedRecordPayload(byte[] rawPayload, String reasonCode, String detail)` — same defensive-copy-plus-overridden-accessor pattern as every other `byte[]`-holding record in this plan. `RejectedRecordSerializer implements Serializer<RejectedRecordPayload>`.

- [ ] **Step 1: Add Kafka client to root and `adapter-kafka`**

Modify root `pom.xml` — add inside `<dependencyManagement><dependencies>`:

```xml
      <dependency>
        <groupId>org.apache.kafka</groupId>
        <artifactId>kafka-clients</artifactId>
        <version>3.7.1</version>
      </dependency>
```

Modify `modules/adapter-kafka/pom.xml` — add inside `<dependencies>`:

```xml
    <dependency>
      <groupId>org.apache.kafka</groupId>
      <artifactId>kafka-clients</artifactId>
    </dependency>
```

- [ ] **Step 2: Write the failing test for `FeatureVectorSerializer`**

```java
package io.netsecml.platform.adapter.kafka.sink;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netsecml.platform.domain.feature.ConnFeatureSchemaV1;
import io.netsecml.platform.domain.feature.FeatureVector;
import org.junit.jupiter.api.Test;
import java.time.Instant;
import static org.junit.jupiter.api.Assertions.*;

class FeatureVectorSerializerTest {
    @Test
    void serializesAllFieldsAsJson() throws Exception {
        FeatureVector vector = new FeatureVector(
            "sensor-eu-1:abc", Instant.parse("2026-08-13T10:00:00Z"),
            ConnFeatureSchemaV1.SCHEMA.id(), ConnFeatureSchemaV1.CONTENT_HASH,
            new float[]{1f, 2f, 3f}, 0);

        FeatureVectorSerializer serializer = new FeatureVectorSerializer();
        byte[] bytes = serializer.serialize("netsec.conn.feature-vector.v1", vector);

        JsonNode json = new ObjectMapper().readTree(bytes);
        assertEquals("sensor-eu-1:abc", json.get("eventId").asText());
        assertEquals("conn-feature-v1", json.get("schemaId").asText());
        assertEquals(ConnFeatureSchemaV1.CONTENT_HASH, json.get("schemaHash").asText());
        assertEquals(3, json.get("values").size());
        assertEquals(1.0, json.get("values").get(0).asDouble(), 0.0001);
    }
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `./mvnw -pl modules/adapter-kafka test -Dtest=FeatureVectorSerializerTest`
Expected: FAIL — `FeatureVectorSerializer` does not exist.

- [ ] **Step 4: Implement `FeatureVectorSerializer`**

```java
package io.netsecml.platform.adapter.kafka.sink;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.netsecml.platform.domain.feature.FeatureVector;
import org.apache.kafka.common.serialization.Serializer;

public final class FeatureVectorSerializer implements Serializer<FeatureVector> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public byte[] serialize(String topic, FeatureVector vector) {
        try {
            ObjectNode node = objectMapper.createObjectNode();
            node.put("eventId", vector.eventId());
            node.put("eventTime", vector.eventTime().toString());
            node.put("schemaId", vector.schemaId());
            node.put("schemaHash", vector.schemaHash());
            node.put("qualityFlags", vector.qualityFlags());
            ArrayNode values = node.putArray("values");
            for (float v : vector.values()) {
                values.add(v);
            }
            return objectMapper.writeValueAsBytes(node);
        } catch (Exception e) {
            throw new RuntimeException("failed to serialize FeatureVector for topic " + topic, e);
        }
    }
}
```

- [ ] **Step 5: Run test to verify it passes**

Run: `./mvnw -pl modules/adapter-kafka test -Dtest=FeatureVectorSerializerTest`
Expected: PASS

- [ ] **Step 6: Write the failing test for `RejectedRecordSerializer`**

```java
package io.netsecml.platform.adapter.kafka.sink;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import java.nio.charset.StandardCharsets;
import static org.junit.jupiter.api.Assertions.*;

class RejectedRecordSerializerTest {
    @Test
    void serializesReasonCodeDetailAndRawPayloadHash() throws Exception {
        RejectedRecordPayload payload = new RejectedRecordPayload(
            "{ broken".getBytes(StandardCharsets.UTF_8), "MALFORMED_JSON", "unexpected end of input");

        RejectedRecordSerializer serializer = new RejectedRecordSerializer();
        byte[] bytes = serializer.serialize("netsec.conn.dlq.v1", payload);

        JsonNode json = new ObjectMapper().readTree(bytes);
        assertEquals("MALFORMED_JSON", json.get("reasonCode").asText());
        assertEquals("unexpected end of input", json.get("detail").asText());
        assertTrue(json.has("rawPayloadHash"));
        assertFalse(json.has("rawPayload"), "raw payload bytes must never be stored, only their hash");
    }
}
```

- [ ] **Step 7: Run test to verify it fails**

Run: `./mvnw -pl modules/adapter-kafka test -Dtest=RejectedRecordSerializerTest`
Expected: FAIL — `RejectedRecordPayload` and `RejectedRecordSerializer` do not exist.

- [ ] **Step 8: Implement `RejectedRecordPayload` and `RejectedRecordSerializer`**

```java
package io.netsecml.platform.adapter.kafka.sink;

import java.util.Arrays;

public record RejectedRecordPayload(byte[] rawPayload, String reasonCode, String detail) {
    public RejectedRecordPayload {
        rawPayload = Arrays.copyOf(rawPayload, rawPayload.length);
    }

    @Override
    public byte[] rawPayload() {
        return Arrays.copyOf(rawPayload, rawPayload.length);
    }
}
```

```java
package io.netsecml.platform.adapter.kafka.sink;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.common.serialization.Serializer;
import java.security.MessageDigest;
import java.time.Instant;

public final class RejectedRecordSerializer implements Serializer<RejectedRecordPayload> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public byte[] serialize(String topic, RejectedRecordPayload payload) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(payload.rawPayload());
            StringBuilder hex = new StringBuilder();
            for (byte b : digest) {
                hex.append(String.format("%02x", b));
            }

            ObjectNode node = objectMapper.createObjectNode();
            node.put("reasonCode", payload.reasonCode());
            node.put("detail", payload.detail());
            node.put("rawPayloadHash", hex.toString());
            node.put("receivedAt", Instant.now().toString());
            return objectMapper.writeValueAsBytes(node);
        } catch (Exception e) {
            throw new RuntimeException("failed to serialize RejectedRecordPayload for topic " + topic, e);
        }
    }
}
```

- [ ] **Step 9: Run test to verify it passes**

Run: `./mvnw -pl modules/adapter-kafka test -Dtest=RejectedRecordSerializerTest`
Expected: PASS

- [ ] **Step 10: Commit**

```bash
git add pom.xml modules/adapter-kafka/pom.xml \
        modules/adapter-kafka/src/main/java/io/netsecml/platform/adapter/kafka/sink/FeatureVectorSerializer.java \
        modules/adapter-kafka/src/main/java/io/netsecml/platform/adapter/kafka/sink/RejectedRecordPayload.java \
        modules/adapter-kafka/src/main/java/io/netsecml/platform/adapter/kafka/sink/RejectedRecordSerializer.java \
        modules/adapter-kafka/src/test/java/io/netsecml/platform/adapter/kafka/sink/FeatureVectorSerializerTest.java \
        modules/adapter-kafka/src/test/java/io/netsecml/platform/adapter/kafka/sink/RejectedRecordSerializerTest.java
git commit -m "feat(adapter-kafka): add FeatureVectorSerializer and RejectedRecordSerializer"
```

---

## Task 14: bootstrap-online-job wiring and Testcontainers end-to-end test

**Files:**
- Modify: root `pom.xml` (add `flink-connector-kafka`, `flink-clients`, `testcontainers` to `dependencyManagement`)
- Modify: `modules/bootstrap-online-job/pom.xml`
- Create: `modules/bootstrap-online-job/src/main/java/io/netsecml/platform/bootstrap/online/OnlineFeatureJob.java`
- Test: `modules/bootstrap-online-job/src/test/java/io/netsecml/platform/bootstrap/online/OnlineFeatureJobE2ETest.java`

**Interfaces:**
- Consumes: everything from Tasks 1-13.
- Produces: `OnlineFeatureJob.build(StreamExecutionEnvironment env, String bootstrapServers, String inputTopic, String featureVectorTopic, String dlqTopic, SensorId sensor): void` — wires `KafkaSource<byte[]> -> ParseMapValidateFunction -> keyBy(SourceKeySelector) -> ConnFeatureProcessFunction -> KafkaSink<FeatureVector>`, plus the rejected side output to a DLQ `KafkaSink<RejectedRecord>`. A `public static void main(String[] args)` reads `bootstrapServers`/topic names from environment variables and calls `build` then `env.execute(...)`.

- [ ] **Step 1: Add remaining dependencies**

Modify root `pom.xml` — add inside `<dependencyManagement><dependencies>`:

```xml
      <dependency>
        <groupId>org.apache.flink</groupId>
        <artifactId>flink-connector-kafka</artifactId>
        <version>5.0.0-2.2</version>
      </dependency>
      <dependency>
        <groupId>org.apache.flink</groupId>
        <artifactId>flink-clients</artifactId>
        <version>${flink.version}</version>
      </dependency>
      <dependency>
        <groupId>org.testcontainers</groupId>
        <artifactId>kafka</artifactId>
        <version>1.19.8</version>
        <scope>test</scope>
      </dependency>
      <dependency>
        <groupId>org.testcontainers</groupId>
        <artifactId>junit-jupiter</artifactId>
        <version>1.19.8</version>
        <scope>test</scope>
      </dependency>
```

If `flink-connector-kafka:5.0.0-2.2` does not resolve, check `https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-kafka/` for the newest artifact whose version suffix matches `-2.2` (the naming convention is `{connector-version}-{flink-minor-version}`) and use that instead.

Modify `modules/bootstrap-online-job/pom.xml` — add inside `<dependencies>`:

```xml
    <dependency>
      <groupId>org.apache.flink</groupId>
      <artifactId>flink-streaming-java</artifactId>
    </dependency>
    <dependency>
      <groupId>org.apache.flink</groupId>
      <artifactId>flink-clients</artifactId>
    </dependency>
    <dependency>
      <groupId>org.apache.flink</groupId>
      <artifactId>flink-connector-kafka</artifactId>
    </dependency>
    <dependency>
      <groupId>org.testcontainers</groupId>
      <artifactId>kafka</artifactId>
      <scope>test</scope>
    </dependency>
    <dependency>
      <groupId>org.testcontainers</groupId>
      <artifactId>junit-jupiter</artifactId>
      <scope>test</scope>
    </dependency>
    <dependency>
      <groupId>org.apache.flink</groupId>
      <artifactId>flink-test-utils</artifactId>
      <scope>test</scope>
    </dependency>
```

- [ ] **Step 2: Write the failing end-to-end test**

```java
package io.netsecml.platform.bootstrap.online;

import io.netsecml.platform.domain.event.SensorId;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
class OnlineFeatureJobE2ETest {
    @Container
    private final ConfluentKafkaContainer kafka =
        new ConfluentKafkaContainer("confluentinc/cp-kafka:7.6.1");

    @Test
    void connFixtureFlowsToFeatureVectorTopic() throws Exception {
        kafka.start();
        String bootstrapServers = kafka.getBootstrapServers();
        String inputTopic = "netsec.conn.raw.v1";
        String featureTopic = "netsec.conn.feature-vector.v1";
        String dlqTopic = "netsec.conn.dlq.v1";

        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(producerProps)) {
            byte[] payload = Files.readAllBytes(Paths.get("..", "..", "tests", "fixtures", "zeek_conn", "valid-tcp-ssl.json"));
            producer.send(new ProducerRecord<>(inputTopic, payload)).get();
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        OnlineFeatureJob.build(env, bootstrapServers, inputTopic, featureTopic, dlqTopic, new SensorId("sensor-eu-1"));

        Thread jobThread = new Thread(() -> {
            try {
                env.executeAsync("conn-foundation-e2e-test");
            } catch (Exception ignored) {
            }
        });
        jobThread.setDaemon(true);
        jobThread.start();

        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "e2e-test-reader");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        try (Consumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
            consumer.subscribe(List.of(featureTopic));
            long deadline = System.currentTimeMillis() + 60_000;
            List<String> collected = new ArrayList<>();
            while (collected.isEmpty() && System.currentTimeMillis() < deadline) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(2));
                records.forEach(r -> collected.add(r.value()));
            }
            assertTrue(collected.size() >= 1, "expected at least one feature vector published within 60s");
            assertTrue(collected.get(0).contains("\"schemaId\":\"conn-feature-v1\""));
            assertTrue(collected.get(0).contains("sensor-eu-1:Cabc123XYZ"));
        }
    }
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `./mvnw -pl modules/bootstrap-online-job test -Dtest=OnlineFeatureJobE2ETest`
Expected: FAIL — `OnlineFeatureJob` does not exist. (This test also requires a Docker daemon reachable by Testcontainers; if Docker is unavailable in the execution environment, it will fail with a container-startup error instead — confirm Docker is running before treating a failure here as "test written correctly.")

- [ ] **Step 4: Implement `OnlineFeatureJob`**

```java
package io.netsecml.platform.bootstrap.online;

import io.netsecml.platform.adapter.flink.process.ConnFeatureProcessFunction;
import io.netsecml.platform.adapter.flink.process.ParseMapValidateFunction;
import io.netsecml.platform.adapter.flink.process.RejectedRecord;
import io.netsecml.platform.adapter.flink.process.SourceKeySelector;
import io.netsecml.platform.adapter.kafka.sink.FeatureVectorSerializer;
import io.netsecml.platform.adapter.kafka.sink.RejectedRecordPayload;
import io.netsecml.platform.adapter.kafka.sink.RejectedRecordSerializer;
import io.netsecml.platform.domain.event.NetworkEvent;
import io.netsecml.platform.domain.event.SensorId;
import io.netsecml.platform.domain.feature.FeatureVector;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public final class OnlineFeatureJob {

    private static final DeserializationSchema<byte[]> RAW_BYTES = new DeserializationSchema<>() {
        @Override
        public byte[] deserialize(byte[] message) {
            return message;
        }

        @Override
        public boolean isEndOfStream(byte[] nextElement) {
            return false;
        }

        @Override
        public TypeInformation<byte[]> getProducedType() {
            return TypeInformation.of(byte[].class);
        }
    };

    public static void build(StreamExecutionEnvironment env, String bootstrapServers, String inputTopic,
                              String featureVectorTopic, String dlqTopic, SensorId sensor) {
        KafkaSource<byte[]> source = KafkaSource.<byte[]>builder()
            .setBootstrapServers(bootstrapServers)
            .setTopics(inputTopic)
            .setGroupId("conn-online-job")
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(RAW_BYTES)
            .build();

        DataStream<byte[]> rawStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "conn-raw-source");

        SingleOutputStreamOperator<NetworkEvent> parsed = rawStream
            .process(new ParseMapValidateFunction(sensor))
            .name("parse-map-validate");

        DataStream<FeatureVector> featureVectors = parsed
            .keyBy(new SourceKeySelector())
            .process(new ConnFeatureProcessFunction())
            .name("conn-feature-extraction");

        KafkaSink<FeatureVector> featureSink = KafkaSink.<FeatureVector>builder()
            .setBootstrapServers(bootstrapServers)
            .setRecordSerializer(KafkaRecordSerializationSchema.<FeatureVector>builder()
                .setTopic(featureVectorTopic)
                .setValueSerializationSchema(vector -> new FeatureVectorSerializer().serialize(featureVectorTopic, vector))
                .build())
            .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
            .build();
        featureVectors.sinkTo(featureSink).name("feature-vector-sink");

        DataStream<RejectedRecord> rejected = parsed.getSideOutput(ParseMapValidateFunction.REJECTED_TAG);
        KafkaSink<RejectedRecord> dlqSink = KafkaSink.<RejectedRecord>builder()
            .setBootstrapServers(bootstrapServers)
            .setRecordSerializer(KafkaRecordSerializationSchema.<RejectedRecord>builder()
                .setTopic(dlqTopic)
                .setValueSerializationSchema(r -> new RejectedRecordSerializer().serialize(dlqTopic,
                    new RejectedRecordPayload(r.rawPayload(), r.reason().name(), r.detail())))
                .build())
            .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
            .build();
        rejected.sinkTo(dlqSink).name("dlq-sink");
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String bootstrapServers = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
        String inputTopic = System.getenv().getOrDefault("CONN_INPUT_TOPIC", "conn");
        String featureTopic = System.getenv().getOrDefault("FEATURE_VECTOR_TOPIC", "netsec.conn.feature-vector.v1");
        String dlqTopic = System.getenv().getOrDefault("DLQ_TOPIC", "netsec.conn.dlq.v1");
        String sensorId = System.getenv().getOrDefault("SENSOR_ID", "sensor-default");

        build(env, bootstrapServers, inputTopic, featureTopic, dlqTopic, new SensorId(sensorId));
        env.execute("conn-online-feature-job");
    }
}
```

`KafkaSource.builder().setValueOnlyDeserializer(DeserializationSchema<T>)` is the stable FLIP-27 Source API overload and has been present unchanged since Flink 1.x through the 2.x line, so this should compile as written against `flink-connector-kafka:5.0.0-2.2`. If the pinned connector version does shift this overload, the fallback is `.setDeserializer(KafkaRecordDeserializationSchema.valueOnly(org.apache.kafka.common.serialization.ByteArrayDeserializer.class))` in its place — confirm which one compiles against the actual resolved artifact rather than guessing.

- [ ] **Step 5: Run test to verify it passes**

Run: `./mvnw -pl modules/bootstrap-online-job test -Dtest=OnlineFeatureJobE2ETest`
Expected: PASS. This test needs a running Docker daemon; if it fails with a Testcontainers startup error rather than an assertion failure, verify Docker is running before debugging the job wiring itself.

- [ ] **Step 6: Run the full reactor build**

Run: `./mvnw clean verify`
Expected: BUILD SUCCESS across all modules — every test from Tasks 1-14 passes together.

- [ ] **Step 7: Commit**

```bash
git add pom.xml modules/bootstrap-online-job/pom.xml \
        modules/bootstrap-online-job/src/main/java/io/netsecml/platform/bootstrap/online/OnlineFeatureJob.java \
        modules/bootstrap-online-job/src/test/java/io/netsecml/platform/bootstrap/online/OnlineFeatureJobE2ETest.java
git commit -m "feat(bootstrap-online-job): wire conn foundation pipeline end-to-end, Testcontainers E2E test"
```

---

## Self-Review

**Spec coverage:** Every element of `PILOT_ARCHITECTURE.md`'s "×4 protocol band" pattern is built once here for `conn` — source contract (Task 6), domain event model (Tasks 1-3), feature schema and pipeline (Tasks 4-5, 9-10), Kafka ingestion adapter (Tasks 7-8), Flink stateful processing (Tasks 11-12), Kafka output adapter (Task 13), and composition root with a real end-to-end proof (Task 14). The ONNX cascade, model registry, ClickHouse archive, and `dns`/`http`/`ssl` replication remain explicitly out of scope per the Global Constraints section.

**Placeholder scan:** No `TBD`/`TODO`. Three places carry an explicit "verify and adjust" fallback instead of false certainty — Task 11 Step 1 (exact `flink-streaming-java:2.2.1` resolution), Task 11/12 Step 4 (`open(OpenContext)` vs. `open(Configuration)`), and Task 14 Step 4 (`setValueOnlyDeserializer` vs. `setDeserializer` overload) — because Flink minor-version builder signatures are the one category of fact in this plan that could not be fully compile-verified from outside a real build. Each includes the concrete fallback code, not just a warning.

**Type consistency:** `MappingResult<T>`'s sealed `Valid`/`Invalid` cases and their `.value()`/`.reason()`/`.detail()` default methods (Task 3) are used identically in Tasks 7, 8, 11 without any call-site awareness of the sealed hierarchy — callers only ever see `isValid()`/`value()`/`reason()`/`detail()`. `SourceWindowState.record(long, long, boolean)` (Task 5) matches its call sites in Task 10's `BuildFeaturesUseCaseImpl` and Task 5's own tests. `FeatureBuildResult.vector()`/`newState()` (Task 10) match their use in Task 12's `ConnFeatureProcessFunction`. `ZeekConnEvent`'s record-accessor names (`id()`, `idOrigH()`, etc., Task 7) are used consistently in Task 8's `EventMapper` and both tasks' tests — no stray `getXxx()` Java-bean-style call survives from the pre-Java-21 version of this plan. `ConnFeatureSchemaV1.CONTENT_HASH` is the same literal string in Tasks 4, 10's test, and 13's test — copied from the one SHA-256 computed against the Task 4 JSON content, not independently re-derived anywhere.

**Java 21 idiom check:** Records are used for every simple immutable data carrier (`SensorId`, `EventId`, `ConnectionTuple`, `ConnectionMeasurements`, `ConnectionLocality`, `NetworkEvent`, `FeatureDefinition`, `FeatureSchema`, `FeatureVector`, `SourceKey`, `FeatureBuildResult`, `ZeekConnEvent`, `RejectedRecord`, `RejectedRecordPayload`). `MappingResult<T>` is a sealed interface with pattern-matching `switch` over its two record cases. `SourceWindowState` deliberately stays a plain class — its four parallel `long[]` arrays are an implementation detail of a bounded ring buffer, not a public data shape, and forcing it into a record would have made that internal representation part of the public API.
