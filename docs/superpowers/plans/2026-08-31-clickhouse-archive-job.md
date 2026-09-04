# ClickHouse Archive Job Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build an independent Flink job that batches the feature-vector and DLQ Kafka streams into ClickHouse, plus the five-table schema it writes into, without ClickHouse ever being able to stop online feature production.

**Architecture:** One Flink job with two independent `KafkaSource → map → ClickHouseBatchSink` chains — the job graph is the router, so no `ArchiveRouter` class exists. The sink is a Sink V2 writer that buffers rows and flushes at 5,000 rows / 4 MiB / 1 s / checkpoint barrier; a failed insert throws, which fails the checkpoint, which prevents Kafka offsets from advancing. `adapter-clickhouse` imports no Kafka and no Flink connector types — `adapter-kafka` deserializes JSON into domain values and `adapter-clickhouse` maps those to rows, meeting at the new `RejectedEvent` domain type.

**Tech Stack:** Java 21, Flink 2.2.1 (Sink V2 / FLIP-27 only), `flink-connector-kafka:5.0.0-2.2`, Jackson 2.17.1, `com.clickhouse:client-v2:0.9.0`, ClickHouse `ReplacingMergeTree`, JUnit 5, Testcontainers 1.21.4 (Kafka + ClickHouse).

**Spec:** `docs/superpowers/specs/2026-08-27-clickhouse-archive-job-design.md`

## Global Constraints

Every task's requirements implicitly include this section.

- Java package root: `io.netsecml.platform`.
- Java 21. Use `record` for immutable data carriers; `sealed interface` + records + pattern-matching `switch` (arrow form, no `default`) for closed hierarchies.
- **Records with array components need defensive copies in the compact constructor AND in the overridden accessor.** No exceptions — the codebase does this everywhere and reviewers enforce it.
- `domain` imports no Kafka, Flink, ClickHouse, ONNX Runtime, Jackson, or Docker. Enforced by its own compile-time classpath.
- `application` imports only `domain` + `ports`. No Jackson, no Flink.
- **Adapters never import each other.** `adapter-flink → adapter-kafka` exists from a prior plan and is grandfathered; do not create any new adapter-to-adapter edge.
- Flink 2.2.1. Sink V2 / FLIP-27 Source API only — `SourceFunction` and `SinkFunction` (V1) were removed in Flink 2.x and do not exist.
- Flink lifecycle hook is `open(org.apache.flink.api.common.functions.OpenContext)`, not the Flink 1.x `Configuration` overload.
- `ConnFeatureSchemaV1.CONTENT_HASH` is `f42fb1bebb2efe3acc5de634c6a7bb3d6f97fc021207f75d77652533b1c01e1b`. It is frozen and must not change. `ConnFeatureSchemaV1Test` asserts it and must keep passing untouched.
- Feature vector is exactly 20 `float32` values, ordered per `contracts/features/conn-feature-schema-v1.json`. The order is frozen.
- `contracts/` is immutable. A change creates a new version (`-v2`); never edit a committed contract file.
- ClickHouse is never on the online scoring path.
- ClickHouse inserts are idempotent via `ReplacingMergeTree`. Never promise exactly-once for the archive sink.
- ClickHouse types: `DateTime64(3, 'UTC')` everywhere. The `values` column is backtick-quoted in every DDL statement and every query — `VALUES` is INSERT syntax.
- Every `Instant` crossing into ClickHouse is formatted `yyyy-MM-dd HH:mm:ss.SSS` in UTC. ISO-8601 with `T`/`Z` is not reliably parsed into `DateTime64`.
- `com.clickhouse:client-v2:0.9.0` is the ONLY new entry in root `pom.xml` `<dependencyManagement>`. Container tests use `GenericContainer` from the already-managed `org.testcontainers:junit-jupiter:1.21.4`, so no `org.testcontainers:clickhouse` module is required — do not add one.
- The ClickHouse container image is pinned to `clickhouse/clickhouse-server:25.8`. Never use `latest`.
- Every container test carries `@Testcontainers(disabledWithoutDocker = true)`.
- **Every code block you write gets inline comments describing what that block does.** This is a standing preference of this repository's owner, not a style suggestion.
- Conventional commits. Small, focused, one per task step where the plan says commit.
- Build: `./mvnw clean verify` at the repo root. Single module: `./mvnw test -pl modules/<module>`. Single class: `./mvnw test -pl modules/<module> -Dtest=ClassName`.

## File Structure

**New — contracts and infrastructure**

| Path | Responsibility |
|---|---|
| `contracts/stream/feature-vector-v1.json` | Frozen 8-field wire descriptor for `netsec.conn.feature-vector.v1` |
| `contracts/stream/dlq-v1.json` | Frozen 6-field wire descriptor for `netsec.conn.dlq.v1` |
| `infrastructure/clickhouse/ddl/001_mvp_tables.sql` | All five MVP tables, idempotent |
| `infrastructure/clickhouse/queries/feature-vector-dedup.sql` | The canonical training deduplication query |
| `scripts/database/apply-ddl.sh` | Applies every DDL file in lexical order over HTTP |

**New — `modules/domain`**

| Path | Responsibility |
|---|---|
| `.../domain/event/RejectedEvent.java` | Consumer-side view of a rejected record: hash, reason, detail, receivedAt, optional eventId |

**New — `modules/adapter-kafka`**

| Path | Responsibility |
|---|---|
| `.../adapter/kafka/sink/FeatureVectorDeserializer.java` | JSON → `FeatureVector`, exact inverse of `FeatureVectorSerializer` |
| `.../adapter/kafka/sink/RejectedEventDeserializer.java` | JSON → `RejectedEvent` |

**New — `modules/adapter-flink`**

| Path | Responsibility |
|---|---|
| `.../adapter/flink/source/RawBytesDeserializationSchema.java` | Pass-through `DeserializationSchema<byte[]>`, shared by both jobs |

**New — `modules/adapter-clickhouse`**

| Path | Responsibility |
|---|---|
| `.../adapter/clickhouse/row/FeatureVectorRow.java` | One `feature_vectors` row; `@JsonProperty` names are the column names |
| `.../adapter/clickhouse/row/InvalidEventRow.java` | One `invalid_events` row |
| `.../adapter/clickhouse/mapper/FeatureVectorRowMapper.java` | `FeatureVector` → `FeatureVectorRow` |
| `.../adapter/clickhouse/mapper/InvalidEventRowMapper.java` | `RejectedEvent` → `InvalidEventRow` |
| `.../adapter/clickhouse/batch/BatchBuffer.java` | Bounded row/byte accumulator. Pure logic, no Flink, no clock |
| `.../adapter/clickhouse/writer/ClickHouseConfig.java` | Serializable connection settings |
| `.../adapter/clickhouse/writer/ClickHouseInserter.java` | Insert seam — one method, so the writer is testable with a fake |
| `.../adapter/clickhouse/writer/ClickHouseInserterFactory.java` | Serializable factory so the sink can build an inserter per subtask |
| `.../adapter/clickhouse/writer/ClientV2Inserter.java` | The real `client-v2` JSONEachRow insert |
| `.../adapter/clickhouse/writer/SinkMetrics.java` | Narrow metric seam so retry logic is unit-testable without a Flink runtime |
| `.../adapter/clickhouse/writer/FlinkSinkMetrics.java` | `SinkMetrics` over Flink's `MetricGroup` |
| `.../adapter/clickhouse/writer/ClickHouseBatchSink.java` | Flink `Sink<T>`; holds config, builds the writer |
| `.../adapter/clickhouse/writer/ClickHouseSinkWriter.java` | `SinkWriter<T>`; buffering, timer, checkpoint flush, bounded retry |

**New — `modules/bootstrap-archive-job`**

| Path | Responsibility |
|---|---|
| `.../bootstrap/archive/ArchiveJob.java` | Composition root: two chains, `build()` + `main()` |
| `.../bootstrap/archive/FeatureVectorRowMapFunction.java` | `byte[]` → `FeatureVectorRow`, deserializer and mapper built in `open()` |
| `.../bootstrap/archive/InvalidEventRowMapFunction.java` | `byte[]` → `InvalidEventRow` |

**Modified**

| Path | Change |
|---|---|
| `modules/domain/.../domain/feature/FeatureVector.java` | Gains `SensorId sensor` and `Instant producedAt` |
| `modules/domain/.../domain/event/ReasonCode.java` | Gains nested `Stage` enum and `stage()` |
| `modules/application/.../application/usecase/BuildFeaturesUseCaseImpl.java` | `Clock` injection; populates the two new components |
| `modules/adapter-kafka/.../sink/FeatureVectorSerializer.java` | Emits `sensor` and `producedAt` |
| `modules/adapter-kafka/.../sink/RejectedRecordPayload.java` | Gains `stage`, `receivedAt`, `eventId` |
| `modules/adapter-kafka/.../sink/RejectedRecordSerializer.java` | Emits `stage`, `receivedAt`, `eventId`; stops calling `Instant.now()` |
| `modules/adapter-flink/.../process/RejectedRecord.java` | Gains `receivedAt` and nullable `eventId` |
| `modules/adapter-flink/.../process/ParseMapValidateFunction.java` | Injectable `Clock`; stamps `receivedAt`; carries the event ID on map-stage rejections |
| `modules/bootstrap-online-job/.../OnlineFeatureJob.java` | Passes the new DLQ fields through; uses the shared `RawBytesDeserializationSchema` |
| `pom.xml` | `client-v2` and `testcontainers:clickhouse` in `dependencyManagement` |
| `modules/adapter-clickhouse/pom.xml` | Jackson, client-v2, Flink, testcontainers |
| `modules/bootstrap-archive-job/pom.xml` | Flink, adapter-flink, testcontainers |
| `.env.example` | Adds the four topic variables |
| `infrastructure/clickhouse/README.md` | Five tables, not four |
| `contracts/stream/README.md` | Two contracts frozen |
| `CLAUDE.md` | Implementation state |

Tests are created alongside the code in each task and are listed in that task's **Files** block.

---

### Task 1: Domain — rejection stage and the neutral `RejectedEvent`

`invalid_events` has a `stage` column that separates parse-stage failures (the bytes never became a valid DTO) from map-stage failures (the DTO parsed but domain validation rejected a value). That mapping is domain knowledge, so it lives on `ReasonCode` rather than being re-derived in an adapter.

`RejectedEvent` is the type `adapter-kafka` deserializes into and `adapter-clickhouse` maps out of, so neither adapter has to import the other.

**Files:**
- Create: `modules/domain/src/main/java/io/netsecml/platform/domain/event/RejectedEvent.java`
- Modify: `modules/domain/src/main/java/io/netsecml/platform/domain/event/ReasonCode.java`
- Test: `modules/domain/src/test/java/io/netsecml/platform/domain/event/ReasonCodeTest.java`
- Test: `modules/domain/src/test/java/io/netsecml/platform/domain/event/RejectedEventTest.java`

**Interfaces:**
- Consumes: nothing.
- Produces:
  - `ReasonCode.Stage` — enum with constants `PARSE`, `MAP`.
  - `ReasonCode.stage()` returning `ReasonCode.Stage`.
  - `RejectedEvent(String eventId, String rawPayloadHash, ReasonCode reason, String detail, Instant receivedAt)` — a record. `eventId` and `detail` are normalized to `""` when null; `rawPayloadHash` must be exactly 64 characters; `reason` and `receivedAt` must be non-null.

- [ ] **Step 1: Write the failing tests**

`modules/domain/src/test/java/io/netsecml/platform/domain/event/ReasonCodeTest.java`:

```java
package io.netsecml.platform.domain.event;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class ReasonCodeTest {
    // Guard test: a new ReasonCode added without a stage fails here rather than
    // silently writing a null into invalid_events.stage months later.
    @Test
    void everyReasonCodeDeclaresAStage() {
        for (ReasonCode code : ReasonCode.values()) {
            assertNotNull(code.stage(), code + " must declare a stage");
        }
    }

    // Parse-stage codes are the ones raised before a ZeekConnEvent exists.
    @Test
    void parseFailuresAreParseStage() {
        assertEquals(ReasonCode.Stage.PARSE, ReasonCode.MALFORMED_JSON.stage());
        assertEquals(ReasonCode.Stage.MAP, ReasonCode.MISSING_REQUIRED_FIELD.stage());
    }

    // Map-stage codes are raised by domain validation after a successful parse.
    @Test
    void domainValidationFailuresAreMapStage() {
        assertEquals(ReasonCode.Stage.MAP, ReasonCode.INVALID_TIMESTAMP.stage());
        assertEquals(ReasonCode.Stage.MAP, ReasonCode.INVALID_PORT.stage());
        assertEquals(ReasonCode.Stage.MAP, ReasonCode.INVALID_COUNTER.stage());
    }
}
```

`modules/domain/src/test/java/io/netsecml/platform/domain/event/RejectedEventTest.java`:

```java
package io.netsecml.platform.domain.event;

import org.junit.jupiter.api.Test;
import java.time.Instant;
import static org.junit.jupiter.api.Assertions.*;

class RejectedEventTest {
    private static final String HASH = "a".repeat(64);

    // The happy path: every component survives construction unchanged.
    @Test
    void storesAllComponents() {
        Instant receivedAt = Instant.parse("2026-08-27T10:03:11.250Z");
        RejectedEvent event = new RejectedEvent("sensor-eu-1:Cabc", HASH,
            ReasonCode.INVALID_PORT, "port 70000 out of range", receivedAt);

        assertEquals("sensor-eu-1:Cabc", event.eventId());
        assertEquals(HASH, event.rawPayloadHash());
        assertEquals(ReasonCode.INVALID_PORT, event.reason());
        assertEquals("port 70000 out of range", event.detail());
        assertEquals(receivedAt, event.receivedAt());
    }

    // Parse-stage rejections have no recoverable identity. ClickHouse's event_id
    // column is non-nullable String, so null normalizes to "" here rather than
    // becoming an adapter's problem later.
    @Test
    void normalizesNullEventIdAndDetailToEmptyString() {
        RejectedEvent event = new RejectedEvent(null, HASH,
            ReasonCode.MALFORMED_JSON, null, Instant.parse("2026-08-27T10:03:11.250Z"));

        assertEquals("", event.eventId());
        assertEquals("", event.detail());
    }

    // The hash is written into a FixedString(64) column; a wrong length would be
    // a silent server-side truncation or error, so reject it at construction.
    @Test
    void rejectsHashThatIsNotSixtyFourCharacters() {
        assertThrows(IllegalArgumentException.class, () -> new RejectedEvent(
            "", "tooshort", ReasonCode.MALFORMED_JSON, "", Instant.now()));
    }

    @Test
    void rejectsNullReasonAndNullReceivedAt() {
        assertThrows(NullPointerException.class, () -> new RejectedEvent(
            "", HASH, null, "", Instant.now()));
        assertThrows(NullPointerException.class, () -> new RejectedEvent(
            "", HASH, ReasonCode.MALFORMED_JSON, "", null));
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `./mvnw test -pl modules/domain -Dtest='ReasonCodeTest+RejectedEventTest'`
Expected: compilation failure — `cannot find symbol: method stage()` and `cannot find symbol: class RejectedEvent`.

- [ ] **Step 3: Add `Stage` to `ReasonCode`**

Replace `modules/domain/src/main/java/io/netsecml/platform/domain/event/ReasonCode.java` entirely:

```java
package io.netsecml.platform.domain.event;

// Why a record is rejected, and which pipeline stage rejected it.
// The archive job writes stage() into invalid_events.stage, so the mapping is
// domain knowledge rather than something an adapter re-derives from the name.
public enum ReasonCode {
    // PARSE: raised before a ZeekConnEvent exists — the bytes are not a usable
    // source record at all.
    MALFORMED_JSON(Stage.PARSE),
    MISSING_REQUIRED_FIELD(Stage.MAP),

    // MAP: the DTO parsed cleanly, but domain validation refused a value.
    INVALID_TIMESTAMP(Stage.MAP),
    INVALID_PORT(Stage.MAP),
    INVALID_COUNTER(Stage.MAP);

    // The two points in the pipeline where a record can be rejected.
    public enum Stage { PARSE, MAP }

    private final Stage stage;

    ReasonCode(Stage stage) {
        this.stage = stage;
    }

    public Stage stage() {
        return stage;
    }
}
```

- [ ] **Step 4: Create `RejectedEvent`**

`modules/domain/src/main/java/io/netsecml/platform/domain/event/RejectedEvent.java`:

```java
package io.netsecml.platform.domain.event;

import java.time.Instant;
import java.util.Objects;

// The consumer-side view of a rejected record: what the archive job reads off
// the DLQ topic and writes into invalid_events.
//
// It deliberately carries the payload HASH and never the payload bytes — the
// raw bytes exist only on the producer side, where RejectedRecord holds them
// long enough for the serializer to hash them.
//
// This type exists so adapter-kafka (which deserializes DLQ JSON) and
// adapter-clickhouse (which maps to a row) have somewhere neutral to meet
// without importing each other.
public record RejectedEvent(String eventId, String rawPayloadHash, ReasonCode reason,
                             String detail, Instant receivedAt) {

    // Length of a hex-encoded SHA-256, matching the FixedString(64) column.
    private static final int SHA256_HEX_LENGTH = 64;

    public RejectedEvent {
        // reason and receivedAt are structural — a row cannot be written without them.
        Objects.requireNonNull(reason, "reason must not be null");
        Objects.requireNonNull(receivedAt, "receivedAt must not be null");

        // The hash lands in a FixedString(64); a wrong length is a silent
        // server-side error, so it is refused here instead.
        Objects.requireNonNull(rawPayloadHash, "rawPayloadHash must not be null");
        if (rawPayloadHash.length() != SHA256_HEX_LENGTH) {
            throw new IllegalArgumentException(
                "rawPayloadHash must be " + SHA256_HEX_LENGTH + " hex characters, was " + rawPayloadHash.length());
        }

        // Parse-stage rejections have no recoverable event identity, and detail
        // may be absent. Both columns are non-nullable String in ClickHouse, so
        // normalize to "" once here rather than in every mapper.
        eventId = eventId == null ? "" : eventId;
        detail = detail == null ? "" : detail;
    }
}
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `./mvnw test -pl modules/domain -Dtest='ReasonCodeTest+RejectedEventTest'`
Expected: PASS, 7 tests.

- [ ] **Step 6: Run the whole domain module to confirm nothing regressed**

Run: `./mvnw test -pl modules/domain`
Expected: PASS. `ConnFeatureSchemaV1Test` in particular must still pass untouched.

- [ ] **Step 7: Commit**

```bash
git add modules/domain/src/main/java/io/netsecml/platform/domain/event/ReasonCode.java \
        modules/domain/src/main/java/io/netsecml/platform/domain/event/RejectedEvent.java \
        modules/domain/src/test/java/io/netsecml/platform/domain/event/ReasonCodeTest.java \
        modules/domain/src/test/java/io/netsecml/platform/domain/event/RejectedEventTest.java
git commit -m "feat(domain): add rejection Stage to ReasonCode and neutral RejectedEvent"
```

---

### Task 2: `FeatureVector` gains `sensor` and `producedAt`

`feature_vectors` must be self-sufficient for training. Sensor identity is technically recoverable today — `EventId.derive` builds `"<sensor>:<upstreamId>"` — but that asks training SQL to split an opaque identifier on its internal delimiter. `producedAt` becomes the ClickHouse `row_version`, which is what makes `argMax(values, row_version)` deduplication meaningful.

Neither addition touches the 20 values, their order, or `ConnFeatureSchemaV1.CONTENT_HASH`. That hash covers the feature schema, not the record envelope.

**Files:**
- Modify: `modules/domain/src/main/java/io/netsecml/platform/domain/feature/FeatureVector.java`
- Modify: `modules/domain/src/test/java/io/netsecml/platform/domain/feature/FeatureVectorTest.java`
- Modify: `modules/application/src/main/java/io/netsecml/platform/application/usecase/BuildFeaturesUseCaseImpl.java`
- Modify: `modules/application/src/test/java/io/netsecml/platform/application/usecase/BuildFeaturesUseCaseImplTest.java`

**Interfaces:**
- Consumes: `SensorId` from `io.netsecml.platform.domain.event` (existing record wrapping a non-blank `String value()`).
- Produces:
  - `FeatureVector(String eventId, Instant eventTime, SensorId sensor, String schemaId, String schemaHash, float[] values, int qualityFlags, Instant producedAt)` — component order matters, later tasks construct it positionally.
  - `BuildFeaturesUseCaseImpl()` — unchanged no-arg constructor, delegates to `Clock.systemUTC()`.
  - `BuildFeaturesUseCaseImpl(Clock clock)` — new; `producedAt` is `clock.instant().truncatedTo(ChronoUnit.MILLIS)`.

- [ ] **Step 1: Write the failing tests**

Replace `modules/domain/src/test/java/io/netsecml/platform/domain/feature/FeatureVectorTest.java` entirely:

```java
package io.netsecml.platform.domain.feature;

import io.netsecml.platform.domain.event.SensorId;
import org.junit.jupiter.api.Test;
import java.time.Instant;
import static org.junit.jupiter.api.Assertions.*;

class FeatureVectorTest {
    private static final SensorId SENSOR = new SensorId("sensor-eu-1");
    private static final Instant EVENT_TIME = Instant.parse("2026-08-13T10:00:00Z");
    private static final Instant PRODUCED_AT = Instant.parse("2026-08-13T10:00:00.402Z");

    // The float[] component must be copied on the way in and on the way out, so
    // no caller can reach into the record's internal state.
    @Test
    void storesValuesAndReturnsDefensiveCopy() {
        float[] values = new float[]{1f, 2f, 3f};
        FeatureVector vector = new FeatureVector("sensor-eu-1:abc", EVENT_TIME, SENSOR,
            "conn-feature-v1", "hash123", values, 0, PRODUCED_AT);

        values[0] = -1f;
        assertEquals(1f, vector.values()[0], "mutating the array passed to the constructor must not affect internal state");

        float[] returned = vector.values();
        returned[0] = 999f;
        assertEquals(1f, vector.values()[0], "mutating the returned array must not affect internal state");
        assertEquals(3, vector.values().length);
    }

    // sensor and producedAt are what make a feature_vectors row self-sufficient
    // for training, so they are structural, not optional.
    @Test
    void exposesSensorAndProducedAt() {
        FeatureVector vector = new FeatureVector("sensor-eu-1:abc", EVENT_TIME, SENSOR,
            "conn-feature-v1", "hash123", new float[]{1f}, 0, PRODUCED_AT);

        assertEquals(SENSOR, vector.sensor());
        assertEquals(PRODUCED_AT, vector.producedAt());
    }

    @Test
    void rejectsNullValues() {
        assertThrows(IllegalArgumentException.class, () -> new FeatureVector(
            "sensor-eu-1:abc", EVENT_TIME, SENSOR, "conn-feature-v1", "hash123", null, 0, PRODUCED_AT));
    }

    @Test
    void rejectsNullSensorAndNullProducedAt() {
        assertThrows(NullPointerException.class, () -> new FeatureVector(
            "sensor-eu-1:abc", EVENT_TIME, null, "conn-feature-v1", "hash123", new float[]{1f}, 0, PRODUCED_AT));
        assertThrows(NullPointerException.class, () -> new FeatureVector(
            "sensor-eu-1:abc", EVENT_TIME, SENSOR, "conn-feature-v1", "hash123", new float[]{1f}, 0, null));
    }
}
```

Add these two tests to `modules/application/src/test/java/io/netsecml/platform/application/usecase/BuildFeaturesUseCaseImplTest.java`, and add the imports `java.time.Clock`, `java.time.ZoneOffset`, `java.time.temporal.ChronoUnit`:

```java
    // An injected Clock is what makes producedAt assertable. Without it the field
    // would only ever be testable as "not null", which asserts nothing useful.
    @Test
    void stampsProducedAtFromTheInjectedClock() {
        Instant fixed = Instant.parse("2026-08-27T10:03:11.402Z");
        BuildFeaturesUseCaseImpl fixedClockUseCase =
            new BuildFeaturesUseCaseImpl(Clock.fixed(fixed, ZoneOffset.UTC));

        NetworkEvent e = event(Instant.ofEpochSecond(60_000), 100, 200, false);
        FeatureBuildResult result = fixedClockUseCase.build(e, SourceWindowState.empty());

        assertEquals(fixed, result.vector().producedAt());
        assertEquals(e.sensor(), result.vector().sensor(), "sensor must propagate from the event");
    }

    // producedAt becomes a DateTime64(3) row_version. Sub-millisecond precision
    // would not survive the round trip, so it is truncated at the source.
    @Test
    void truncatesProducedAtToMilliseconds() {
        Instant subMilli = Instant.parse("2026-08-27T10:03:11.402987654Z");
        BuildFeaturesUseCaseImpl fixedClockUseCase =
            new BuildFeaturesUseCaseImpl(Clock.fixed(subMilli, ZoneOffset.UTC));

        FeatureBuildResult result = fixedClockUseCase.build(
            event(Instant.ofEpochSecond(60_000), 100, 200, false), SourceWindowState.empty());

        assertEquals(subMilli.truncatedTo(ChronoUnit.MILLIS), result.vector().producedAt());
    }
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `./mvnw test -pl modules/domain -Dtest=FeatureVectorTest`
Expected: compilation failure — the 8-argument constructor does not exist.

- [ ] **Step 3: Add the two components to `FeatureVector`**

Replace `modules/domain/src/main/java/io/netsecml/platform/domain/feature/FeatureVector.java` entirely:

```java
package io.netsecml.platform.domain.feature;

import io.netsecml.platform.domain.event.SensorId;
import java.time.Instant;
import java.util.Arrays;
import java.util.Objects;

// One scored-ready feature vector: the frozen 20 float32 values plus the
// envelope that identifies and dates them.
//
// sensor makes an archived row self-sufficient for training without joining to
// the optional network_events table. producedAt is the emission timestamp and
// becomes the ClickHouse row_version, so "last emission wins" on replay.
// Neither field affects the 20 values, their order, or the frozen schema hash.
public record FeatureVector(String eventId, Instant eventTime, SensorId sensor, String schemaId,
                             String schemaHash, float[] values, int qualityFlags, Instant producedAt) {
    public FeatureVector {
        // Identity must be present — it is the ClickHouse ORDER BY key tail.
        if (eventId == null || eventId.isBlank()) {
            throw new IllegalArgumentException("eventId must not be blank");
        }
        if (values == null) {
            throw new IllegalArgumentException("values must not be null");
        }

        // Both new components are structural; a row cannot be archived without them.
        Objects.requireNonNull(sensor, "sensor must not be null");
        Objects.requireNonNull(producedAt, "producedAt must not be null");

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

- [ ] **Step 4: Inject a `Clock` into `BuildFeaturesUseCaseImpl` and populate the new components**

Replace `modules/application/src/main/java/io/netsecml/platform/application/usecase/BuildFeaturesUseCaseImpl.java` entirely:

```java
package io.netsecml.platform.application.usecase;

import io.netsecml.platform.application.feature.EventFeatureExtractor;
import io.netsecml.platform.domain.event.NetworkEvent;
import io.netsecml.platform.domain.feature.ConnFeatureSchemaV1;
import io.netsecml.platform.domain.feature.FeatureBuildResult;
import io.netsecml.platform.domain.feature.FeatureVector;
import io.netsecml.platform.domain.feature.SourceWindowState;
import io.netsecml.platform.port.in.BuildFeaturesUseCase;
import java.time.Clock;
import java.time.temporal.ChronoUnit;
import java.util.Objects;

public final class BuildFeaturesUseCaseImpl implements BuildFeaturesUseCase {
    private final EventFeatureExtractor eventFeatureExtractor = new EventFeatureExtractor();

    // Injected so producedAt is deterministic under test. Clock's JDK
    // implementations are Serializable, which matters if this ever moves into a
    // Flink function field rather than being built in open().
    private final Clock clock;

    // Kept so ConnFeatureProcessFunction.open()'s existing no-arg construction
    // compiles unchanged.
    public BuildFeaturesUseCaseImpl() {
        this(Clock.systemUTC());
    }

    public BuildFeaturesUseCaseImpl(Clock clock) {
        this.clock = Objects.requireNonNull(clock, "clock must not be null");
    }

    @Override
    public FeatureBuildResult build(NetworkEvent event, SourceWindowState currentState) {
        // Indices 0-16: deterministic, event-local features.
        float[] eventLevel = eventFeatureExtractor.extractEventLevel(event);

        // Fold this event into the bounded 5-bucket rolling window. record()
        // returns a NEW state; the caller is responsible for storing it.
        long totalBytes = event.measurements().originBytes() + event.measurements().responseBytes();
        boolean failed = event.connection().connectionState().isFailed();
        long bucketMinute = event.eventTime().getEpochSecond() / 60;
        SourceWindowState newState = currentState.record(bucketMinute, totalBytes, failed);

        // Indices 17-19 come from the window AFTER this event is folded in.
        float[] values = new float[20];
        System.arraycopy(eventLevel, 0, values, 0, 17);
        values[17] = newState.connectionCount5m();
        values[18] = newState.byteSum5m();
        values[19] = newState.failedCount5m();

        // producedAt is truncated to milliseconds because it lands in a
        // DateTime64(3) row_version; finer precision would not round-trip.
        FeatureVector vector = new FeatureVector(
            event.eventId().value(),
            event.eventTime(),
            event.sensor(),
            ConnFeatureSchemaV1.SCHEMA.id(),
            ConnFeatureSchemaV1.CONTENT_HASH,
            values,
            0,
            clock.instant().truncatedTo(ChronoUnit.MILLIS));

        return new FeatureBuildResult(vector, newState);
    }
}
```

- [ ] **Step 5: Run domain and application tests to verify they pass**

Run: `./mvnw test -pl modules/domain,modules/application`
Expected: PASS. `ConnFeatureSchemaV1Test` must still pass with the hash unchanged.

- [ ] **Step 6: Confirm the whole reactor still compiles**

Run: `./mvnw -q clean verify -DskipTests`
Expected: BUILD SUCCESS. If `ConnFeatureProcessFunctionTest` or any other module fails to compile, the `FeatureVector` construction site there needs the two new arguments — fix it before committing.

- [ ] **Step 7: Run the full build**

Run: `./mvnw clean verify`
Expected: BUILD SUCCESS, all tests pass.

- [ ] **Step 8: Commit**

```bash
git add modules/domain/src/main/java/io/netsecml/platform/domain/feature/FeatureVector.java \
        modules/domain/src/test/java/io/netsecml/platform/domain/feature/FeatureVectorTest.java \
        modules/application/src/main/java/io/netsecml/platform/application/usecase/BuildFeaturesUseCaseImpl.java \
        modules/application/src/test/java/io/netsecml/platform/application/usecase/BuildFeaturesUseCaseImplTest.java
git commit -m "feat(domain): add sensor and producedAt to FeatureVector, inject Clock into use case"
```

---
### Task 3: `FeatureVectorSerializer` emits the two new fields

The wire record is about to be frozen as a contract. Getting the two new fields onto it — in the contract's field order — is a prerequisite for Task 5.

**Files:**
- Modify: `modules/adapter-kafka/src/main/java/io/netsecml/platform/adapter/kafka/sink/FeatureVectorSerializer.java`
- Modify: `modules/adapter-kafka/src/test/java/io/netsecml/platform/adapter/kafka/sink/FeatureVectorSerializerTest.java`
- Modify: `modules/bootstrap-online-job/src/test/java/io/netsecml/platform/bootstrap/online/OnlineFeatureJobE2ETest.java`

**Interfaces:**
- Consumes: `FeatureVector(String, Instant, SensorId, String, String, float[], int, Instant)` from Task 2.
- Produces: JSON with exactly these eight keys, in this order — `eventId`, `eventTime`, `sensor`, `schemaId`, `schemaHash`, `values`, `qualityFlags`, `producedAt`. Timestamps are ISO-8601 via `Instant.toString()`.

- [ ] **Step 1: Write the failing test**

Replace `modules/adapter-kafka/src/test/java/io/netsecml/platform/adapter/kafka/sink/FeatureVectorSerializerTest.java` entirely:

```java
package io.netsecml.platform.adapter.kafka.sink;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netsecml.platform.domain.event.SensorId;
import io.netsecml.platform.domain.feature.ConnFeatureSchemaV1;
import io.netsecml.platform.domain.feature.FeatureVector;
import org.junit.jupiter.api.Test;
import java.time.Instant;
import static org.junit.jupiter.api.Assertions.*;

class FeatureVectorSerializerTest {
    // One representative vector reused by every assertion below.
    private FeatureVector vector() {
        return new FeatureVector(
            "sensor-eu-1:abc",
            Instant.parse("2026-08-13T10:00:00Z"),
            new SensorId("sensor-eu-1"),
            ConnFeatureSchemaV1.SCHEMA.id(),
            ConnFeatureSchemaV1.CONTENT_HASH,
            new float[]{1f, 2f, 3f},
            0,
            Instant.parse("2026-08-13T10:00:00.402Z"));
    }

    @Test
    void serializesAllFieldsAsJson() throws Exception {
        byte[] bytes = new FeatureVectorSerializer().serialize("netsec.conn.feature-vector.v1", vector());

        JsonNode json = new ObjectMapper().readTree(bytes);
        assertEquals("sensor-eu-1:abc", json.get("eventId").asText());
        assertEquals("conn-feature-v1", json.get("schemaId").asText());
        assertEquals(ConnFeatureSchemaV1.CONTENT_HASH, json.get("schemaHash").asText());
        assertEquals(3, json.get("values").size());
        assertEquals(1.0, json.get("values").get(0).asDouble(), 0.0001);
    }

    // sensor is what lets a training query slice by sensor without splitting the
    // composite eventId on its internal delimiter.
    @Test
    void emitsSensorAsAPlainString() throws Exception {
        JsonNode json = new ObjectMapper().readTree(
            new FeatureVectorSerializer().serialize("netsec.conn.feature-vector.v1", vector()));

        assertEquals("sensor-eu-1", json.get("sensor").asText());
    }

    // producedAt becomes the ClickHouse row_version, so it must survive the wire
    // exactly, milliseconds included.
    @Test
    void emitsProducedAtAsIso8601WithMilliseconds() throws Exception {
        JsonNode json = new ObjectMapper().readTree(
            new FeatureVectorSerializer().serialize("netsec.conn.feature-vector.v1", vector()));

        assertEquals("2026-08-13T10:00:00.402Z", json.get("producedAt").asText());
        assertEquals(Instant.parse("2026-08-13T10:00:00.402Z"), Instant.parse(json.get("producedAt").asText()));
    }
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `./mvnw test -pl modules/adapter-kafka -Dtest=FeatureVectorSerializerTest`
Expected: FAIL — `json.get("sensor")` is null, producing a `NullPointerException`.

- [ ] **Step 3: Emit the two new fields**

Replace the body of `serialize` in `modules/adapter-kafka/src/main/java/io/netsecml/platform/adapter/kafka/sink/FeatureVectorSerializer.java`:

```java
    @Override
    public byte[] serialize(String topic, FeatureVector vector) {
        try {
            // Field order mirrors contracts/stream/feature-vector-v1.json so a
            // human diffing a Kafka message against the contract reads top to bottom.
            ObjectNode node = objectMapper.createObjectNode();
            node.put("eventId", vector.eventId());
            node.put("eventTime", vector.eventTime().toString());
            node.put("sensor", vector.sensor().value());
            node.put("schemaId", vector.schemaId());
            node.put("schemaHash", vector.schemaHash());

            // The 20 float32 values, in frozen schema order.
            ArrayNode values = node.putArray("values");
            for (float v : vector.values()) {
                values.add(v);
            }

            node.put("qualityFlags", vector.qualityFlags());

            // Emission time. The archive job writes this as row_version, which is
            // what makes "last emission wins" deduplication deterministic.
            node.put("producedAt", vector.producedAt().toString());

            return objectMapper.writeValueAsBytes(node);
        } catch (Exception e) {
            throw new RuntimeException("failed to serialize FeatureVector for topic " + topic, e);
        }
    }
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `./mvnw test -pl modules/adapter-kafka -Dtest=FeatureVectorSerializerTest`
Expected: PASS, 3 tests.

- [ ] **Step 5: Extend the online job's end-to-end assertions**

`OnlineFeatureJobE2ETest` keeps passing untouched — its assertions are `contains`
checks on `"schemaId"` and on the composite event id, and two extra fields sail
straight past them. That is precisely how a wire contract rots. Add assertions for
the new fields to `connFixtureFlowsToFeatureVectorTopic`, after the existing ones:

```java
        assertTrue(collected.get(0).contains("\"sensor\":\"sensor-eu-1\""),
            "the published record must carry the sensor as its own field");
        assertTrue(collected.get(0).contains("\"producedAt\":"),
            "the published record must carry an emission timestamp for row_version");
```

Run: `./mvnw test -pl modules/bootstrap-online-job -Dtest=OnlineFeatureJobE2ETest`
Expected: PASS, 1 test. Needs Docker; without it the class is skipped.

- [ ] **Step 6: Commit**

```bash
git add modules/adapter-kafka/src/main/java/io/netsecml/platform/adapter/kafka/sink/FeatureVectorSerializer.java \
        modules/adapter-kafka/src/test/java/io/netsecml/platform/adapter/kafka/sink/FeatureVectorSerializerTest.java \
        modules/bootstrap-online-job/src/test/java/io/netsecml/platform/bootstrap/online/OnlineFeatureJobE2ETest.java
git commit -m "feat(adapter-kafka): emit sensor and producedAt on the feature-vector wire record"
```

---

### Task 4: DLQ record carries stage, receivedAt and event identity end to end

Three changes travel together because none works alone:

1. `RejectedRecordSerializer` currently calls `Instant.now()` **inside** `serialize()`. That records when the sink ran rather than when the record was rejected, and re-stamps on every serialization attempt. The stamp moves to the point of rejection.
2. `invalid_events.stage` needs the PARSE/MAP discriminator from Task 1.
3. Map-stage rejections can name the record that failed — the DTO parsed, so the same composite event ID the feature path uses is available. That turns a DLQ row from "something failed" into "this event failed", joinable against `feature_vectors.event_id`.

What this does **not** fix: a replay after a restart re-rejects and re-stamps, so `invalid_events` accumulates duplicates. That is accepted — `Roadmap.md` says "expect duplicates after failures", the table is low-volume 30-day forensic data, and `raw_payload_hash` supports `GROUP BY` deduplication at query time.

**Files:**
- Modify: `modules/adapter-flink/src/main/java/io/netsecml/platform/adapter/flink/process/RejectedRecord.java`
- Modify: `modules/adapter-flink/src/main/java/io/netsecml/platform/adapter/flink/process/ParseMapValidateFunction.java`
- Modify: `modules/adapter-flink/src/test/java/io/netsecml/platform/adapter/flink/process/ParseMapValidateFunctionTest.java`
- Modify: `modules/adapter-kafka/src/main/java/io/netsecml/platform/adapter/kafka/sink/RejectedRecordPayload.java`
- Modify: `modules/adapter-kafka/src/main/java/io/netsecml/platform/adapter/kafka/sink/RejectedRecordSerializer.java`
- Modify: `modules/adapter-kafka/src/test/java/io/netsecml/platform/adapter/kafka/sink/RejectedRecordSerializerTest.java`
- Modify: `modules/bootstrap-online-job/src/main/java/io/netsecml/platform/bootstrap/online/OnlineFeatureJob.java`

**Interfaces:**
- Consumes: `ReasonCode.stage()` and `ReasonCode.Stage` from Task 1.
- Produces:
  - `RejectedRecord(byte[] rawPayload, ReasonCode reason, String detail, Instant receivedAt, String eventId)` — `eventId` normalized to `""` when null.
  - `ParseMapValidateFunction(SensorId sensor)` — unchanged signature.
  - `ParseMapValidateFunction(SensorId sensor, Clock clock)` — new.
  - `RejectedRecordPayload(byte[] rawPayload, String eventId, String stage, String reasonCode, String detail, Instant receivedAt)`.
  - DLQ JSON with exactly these six keys in this order: `eventId`, `stage`, `reasonCode`, `detail`, `rawPayloadHash`, `receivedAt`.

- [ ] **Step 1: Write the failing tests**

Add to `modules/adapter-flink/src/test/java/io/netsecml/platform/adapter/flink/process/ParseMapValidateFunctionTest.java`, with imports `java.time.Clock`, `java.time.Instant`, `java.time.ZoneOffset`:

```java
    // A fixed Clock is what makes receivedAt assertable. Stamping it at the point
    // of rejection — rather than inside the serializer, where it used to live —
    // means it records when the record was rejected, not when the sink ran.
    @Test
    void stampsReceivedAtAtTheMomentOfRejection() throws Exception {
        Instant fixed = Instant.parse("2026-08-27T10:03:11.250Z");
        ParseMapValidateFunction function =
            new ParseMapValidateFunction(new SensorId("sensor-eu-1"), Clock.fixed(fixed, ZoneOffset.UTC));
        OneInputStreamOperatorTestHarness<byte[], NetworkEvent> harness =
            ProcessFunctionTestHarnesses.forProcessFunction(function);

        harness.processElement(new StreamRecord<>(fixture("malformed.json")));

        Collection<StreamRecord<RejectedRecord>> rejected =
            harness.getSideOutput(ParseMapValidateFunction.REJECTED_TAG);
        assertEquals(fixed, rejected.iterator().next().getValue().receivedAt());

        harness.close();
    }

    // A parse-stage failure never produced a DTO, so there is no identity to carry.
    @Test
    void parseStageRejectionCarriesNoEventId() throws Exception {
        ParseMapValidateFunction function = new ParseMapValidateFunction(new SensorId("sensor-eu-1"));
        OneInputStreamOperatorTestHarness<byte[], NetworkEvent> harness =
            ProcessFunctionTestHarnesses.forProcessFunction(function);

        harness.processElement(new StreamRecord<>(fixture("malformed.json")));

        Collection<StreamRecord<RejectedRecord>> rejected =
            harness.getSideOutput(ParseMapValidateFunction.REJECTED_TAG);
        assertEquals("", rejected.iterator().next().getValue().eventId());

        harness.close();
    }

    // A map-stage failure parsed cleanly, so it CAN be named. The ID is derived
    // exactly as the feature path derives it, which is what makes an invalid_events
    // row joinable to feature_vectors.event_id.
    @Test
    void mapStageRejectionCarriesTheDerivedEventId() throws Exception {
        ParseMapValidateFunction function = new ParseMapValidateFunction(new SensorId("sensor-eu-1"));
        OneInputStreamOperatorTestHarness<byte[], NetworkEvent> harness =
            ProcessFunctionTestHarnesses.forProcessFunction(function);

        harness.processElement(new StreamRecord<>(fixture("invalid-port.json")));

        Collection<StreamRecord<RejectedRecord>> rejected =
            harness.getSideOutput(ParseMapValidateFunction.REJECTED_TAG);
        RejectedRecord record = rejected.iterator().next().getValue();
        assertEquals(ReasonCode.INVALID_PORT, record.reason());
        assertFalse(record.eventId().isBlank(), "a map-stage rejection knows which event failed");
        assertTrue(record.eventId().startsWith("sensor-eu-1:"), "event id is the same composite the feature path derives");

        harness.close();
    }
```

Replace `modules/adapter-kafka/src/test/java/io/netsecml/platform/adapter/kafka/sink/RejectedRecordSerializerTest.java` entirely:

```java
package io.netsecml.platform.adapter.kafka.sink;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import static org.junit.jupiter.api.Assertions.*;

class RejectedRecordSerializerTest {
    private static final Instant RECEIVED_AT = Instant.parse("2026-08-27T10:03:11.250Z");

    private RejectedRecordPayload payload() {
        return new RejectedRecordPayload(
            "{ broken".getBytes(StandardCharsets.UTF_8),
            "",
            "PARSE",
            "MALFORMED_JSON",
            "unexpected end of input",
            RECEIVED_AT);
    }

    @Test
    void serializesReasonCodeDetailAndRawPayloadHash() throws Exception {
        JsonNode json = new ObjectMapper().readTree(
            new RejectedRecordSerializer().serialize("netsec.conn.dlq.v1", payload()));

        assertEquals("MALFORMED_JSON", json.get("reasonCode").asText());
        assertEquals("unexpected end of input", json.get("detail").asText());
        assertTrue(json.has("rawPayloadHash"));
        assertFalse(json.has("rawPayload"), "raw payload bytes must never be stored, only their hash");
    }

    // The hash lands in a FixedString(64) column.
    @Test
    void emitsASixtyFourCharacterHexHash() throws Exception {
        JsonNode json = new ObjectMapper().readTree(
            new RejectedRecordSerializer().serialize("netsec.conn.dlq.v1", payload()));

        assertEquals(64, json.get("rawPayloadHash").asText().length());
        assertTrue(json.get("rawPayloadHash").asText().matches("[0-9a-f]{64}"));
    }

    // stage is the PARSE/MAP discriminator that invalid_events.stage stores.
    @Test
    void emitsStageAndEventId() throws Exception {
        RejectedRecordPayload mapStage = new RejectedRecordPayload(
            "{}".getBytes(StandardCharsets.UTF_8), "sensor-eu-1:Cabc", "MAP",
            "INVALID_PORT", "port 70000 out of range", RECEIVED_AT);

        JsonNode json = new ObjectMapper().readTree(
            new RejectedRecordSerializer().serialize("netsec.conn.dlq.v1", mapStage));

        assertEquals("MAP", json.get("stage").asText());
        assertEquals("sensor-eu-1:Cabc", json.get("eventId").asText());
    }

    // The serializer must not mint data. Serializing the same payload twice has to
    // produce byte-identical output, which it cannot if it calls Instant.now().
    @Test
    void isDeterministicAndTakesReceivedAtFromThePayload() throws Exception {
        RejectedRecordSerializer serializer = new RejectedRecordSerializer();
        byte[] first = serializer.serialize("netsec.conn.dlq.v1", payload());
        byte[] second = serializer.serialize("netsec.conn.dlq.v1", payload());

        assertArrayEquals(first, second, "serializing the same payload twice must produce identical bytes");
        assertEquals("2026-08-27T10:03:11.250Z",
            new ObjectMapper().readTree(first).get("receivedAt").asText());
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `./mvnw test -pl modules/adapter-kafka,modules/adapter-flink -Dtest='RejectedRecordSerializerTest+ParseMapValidateFunctionTest'`
Expected: compilation failure — the 6-argument `RejectedRecordPayload` and the 2-argument `ParseMapValidateFunction` constructor do not exist.

- [ ] **Step 3: Extend `RejectedRecord`**

Replace `modules/adapter-flink/src/main/java/io/netsecml/platform/adapter/flink/process/RejectedRecord.java` entirely:

```java
package io.netsecml.platform.adapter.flink.process;

import io.netsecml.platform.domain.event.ReasonCode;
import java.time.Instant;
import java.util.Arrays;
import java.util.Objects;

// A record the pipeline refused, travelling on the DLQ side output.
//
// It holds the raw bytes because the Kafka serializer downstream hashes them;
// the bytes themselves are never published. receivedAt is stamped where the
// rejection happens, not where it is serialized. eventId is present only for
// map-stage rejections, where a DTO parsed successfully before domain
// validation refused it.
public record RejectedRecord(byte[] rawPayload, ReasonCode reason, String detail,
                              Instant receivedAt, String eventId) {
    public RejectedRecord {
        Objects.requireNonNull(reason, "reason must not be null");
        Objects.requireNonNull(receivedAt, "receivedAt must not be null");

        // Parse-stage rejections have no identity; normalize once so nothing
        // downstream has to null-check.
        eventId = eventId == null ? "" : eventId;

        // Defensive copy in.
        rawPayload = Arrays.copyOf(rawPayload, rawPayload.length);
    }

    // Defensive copy out.
    @Override
    public byte[] rawPayload() {
        return Arrays.copyOf(rawPayload, rawPayload.length);
    }
}
```

- [ ] **Step 4: Stamp `receivedAt` and derive the event ID in `ParseMapValidateFunction`**

Replace `modules/adapter-flink/src/main/java/io/netsecml/platform/adapter/flink/process/ParseMapValidateFunction.java` entirely:

```java
package io.netsecml.platform.adapter.flink.process;

import io.netsecml.platform.adapter.kafka.dto.ZeekConnEvent;
import io.netsecml.platform.adapter.kafka.mapper.EventMapper;
import io.netsecml.platform.adapter.kafka.parser.JsonZeekConnParser;
import io.netsecml.platform.domain.event.EventId;
import io.netsecml.platform.domain.event.MappingResult;
import io.netsecml.platform.domain.event.NetworkEvent;
import io.netsecml.platform.domain.event.SensorId;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

public final class ParseMapValidateFunction extends ProcessFunction<byte[], NetworkEvent> {
    public static final OutputTag<RejectedRecord> REJECTED_TAG =
        new OutputTag<RejectedRecord>("rejected") {};

    private final SensorId sensor;

    // Injected so receivedAt is assertable in tests. Clock's JDK implementations
    // are Serializable, which they must be to ride along in a Flink function.
    private final Clock clock;

    private transient JsonZeekConnParser parser;
    private transient EventMapper mapper;

    public ParseMapValidateFunction(SensorId sensor) {
        this(sensor, Clock.systemUTC());
    }

    public ParseMapValidateFunction(SensorId sensor, Clock clock) {
        this.sensor = sensor;
        this.clock = clock;
    }

    @Override
    public void open(org.apache.flink.api.common.functions.OpenContext openContext) throws Exception {
        super.open(openContext);
        // Both are stateless and cheap to hold; building them once per subtask
        // keeps them out of the per-record path.
        parser = new JsonZeekConnParser();
        mapper = new EventMapper();
    }

    @Override
    public void processElement(byte[] rawPayload, Context ctx, Collector<NetworkEvent> out) {
        // Truncated to milliseconds because it lands in a DateTime64(3) column.
        Instant receivedAt = clock.instant().truncatedTo(ChronoUnit.MILLIS);

        // Stage 1 — parse. A failure here produced no DTO, so there is no event
        // identity to attach.
        MappingResult<ZeekConnEvent> parsed = parser.parse(rawPayload);
        if (!parsed.isValid()) {
            ctx.output(REJECTED_TAG,
                new RejectedRecord(rawPayload, parsed.reason(), parsed.detail(), receivedAt, null));
            return;
        }

        // Stage 2 — map and validate. A failure here DID parse, so the record can
        // be named using the same composite ID the feature path derives. That is
        // what makes an invalid_events row joinable to feature_vectors.event_id.
        MappingResult<NetworkEvent> mapped = mapper.map(parsed.value(), sensor);
        if (!mapped.isValid()) {
            ctx.output(REJECTED_TAG,
                new RejectedRecord(rawPayload, mapped.reason(), mapped.detail(), receivedAt,
                    deriveEventId(parsed.value())));
            return;
        }

        out.collect(mapped.value());
    }

    // EventId.derive refuses a blank upstream id. A DTO that parsed but carries a
    // blank id would otherwise throw here and kill the subtask, so fall back to
    // no identity rather than failing the whole job over a cosmetic field.
    private String deriveEventId(ZeekConnEvent dto) {
        String upstreamId = dto.id();
        if (upstreamId == null || upstreamId.isBlank()) {
            return null;
        }
        return EventId.derive(sensor, upstreamId).value();
    }
}
```

- [ ] **Step 5: Extend `RejectedRecordPayload` and stop the serializer minting timestamps**

Replace `modules/adapter-kafka/src/main/java/io/netsecml/platform/adapter/kafka/sink/RejectedRecordPayload.java` entirely:

```java
package io.netsecml.platform.adapter.kafka.sink;

import java.time.Instant;
import java.util.Arrays;
import java.util.Objects;

// The producer-side shape of a DLQ message. It still carries the raw bytes
// because RejectedRecordSerializer hashes them on the way out; the bytes are
// never published. This is deliberately NOT the same type as domain
// RejectedEvent, which is the consumer-side view and carries only the hash.
public record RejectedRecordPayload(byte[] rawPayload, String eventId, String stage,
                                     String reasonCode, String detail, Instant receivedAt) {
    public RejectedRecordPayload {
        Objects.requireNonNull(receivedAt, "receivedAt must not be null");

        // Normalize the optional text fields once, so the serializer never emits null.
        eventId = eventId == null ? "" : eventId;
        detail = detail == null ? "" : detail;

        // Defensive copy in.
        rawPayload = Arrays.copyOf(rawPayload, rawPayload.length);
    }

    // Defensive copy out.
    @Override
    public byte[] rawPayload() {
        return Arrays.copyOf(rawPayload, rawPayload.length);
    }
}
```

Replace `modules/adapter-kafka/src/main/java/io/netsecml/platform/adapter/kafka/sink/RejectedRecordSerializer.java` entirely:

```java
package io.netsecml.platform.adapter.kafka.sink;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.common.serialization.Serializer;
import java.security.MessageDigest;
import java.util.HexFormat;

public final class RejectedRecordSerializer implements Serializer<RejectedRecordPayload> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public byte[] serialize(String topic, RejectedRecordPayload payload) {
        try {
            // Only the hash of the rejected payload is published. The bytes may
            // contain customer network data and must not leave the pipeline.
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(payload.rawPayload());
            String rawPayloadHash = HexFormat.of().formatHex(digest);

            // Field order mirrors contracts/stream/dlq-v1.json.
            //
            // Every value comes from the payload. This serializer mints nothing:
            // receivedAt used to be Instant.now() here, which recorded when the
            // sink ran rather than when the record was rejected, and re-stamped on
            // every serialization attempt.
            ObjectNode node = objectMapper.createObjectNode();
            node.put("eventId", payload.eventId());
            node.put("stage", payload.stage());
            node.put("reasonCode", payload.reasonCode());
            node.put("detail", payload.detail());
            node.put("rawPayloadHash", rawPayloadHash);
            node.put("receivedAt", payload.receivedAt().toString());

            return objectMapper.writeValueAsBytes(node);
        } catch (Exception e) {
            throw new RuntimeException("failed to serialize RejectedRecordPayload for topic " + topic, e);
        }
    }
}
```

- [ ] **Step 6: Pass the new fields through in `OnlineFeatureJob`**

In `modules/bootstrap-online-job/src/main/java/io/netsecml/platform/bootstrap/online/OnlineFeatureJob.java`, replace the DLQ value-serialization lambda:

```java
                .setValueSerializationSchema(r -> rejectedSerializer.serialize(dlqTopic,
                    // stage comes from the domain's ReasonCode, so the archive
                    // adapter never has to re-derive it from the reason name.
                    new RejectedRecordPayload(r.rawPayload(), r.eventId(), r.reason().stage().name(),
                        r.reason().name(), r.detail(), r.receivedAt())))
```

- [ ] **Step 7: Run the tests to verify they pass**

Run: `./mvnw test -pl modules/adapter-kafka,modules/adapter-flink`
Expected: PASS. `ParseMapValidateFunctionTest` should now have 6 tests, `RejectedRecordSerializerTest` 4.

- [ ] **Step 8: Run the full build**

Run: `./mvnw clean verify`
Expected: BUILD SUCCESS. `OnlineFeatureJobE2ETest`, extended in Task 3, still passes — the DLQ changes here do not touch the feature-vector path.

- [ ] **Step 9: Commit**

```bash
git add modules/adapter-flink/src/main/java/io/netsecml/platform/adapter/flink/process/RejectedRecord.java \
        modules/adapter-flink/src/main/java/io/netsecml/platform/adapter/flink/process/ParseMapValidateFunction.java \
        modules/adapter-flink/src/test/java/io/netsecml/platform/adapter/flink/process/ParseMapValidateFunctionTest.java \
        modules/adapter-kafka/src/main/java/io/netsecml/platform/adapter/kafka/sink/RejectedRecordPayload.java \
        modules/adapter-kafka/src/main/java/io/netsecml/platform/adapter/kafka/sink/RejectedRecordSerializer.java \
        modules/adapter-kafka/src/test/java/io/netsecml/platform/adapter/kafka/sink/RejectedRecordSerializerTest.java \
        modules/bootstrap-online-job/src/main/java/io/netsecml/platform/bootstrap/online/OnlineFeatureJob.java
git commit -m "feat(adapter-kafka): carry stage, receivedAt and event id on the DLQ wire record"
```

---
### Task 5: Freeze both stream contracts and add the deserializers

`contracts/` is immutable once committed, so this is the moment the two wire shapes stop being incidental and become the interface that Python training and the archive job both read against.

The drift tests are the point of this task. A contract nobody checks rots the first time someone adds a field.

**Files:**
- Create: `contracts/stream/feature-vector-v1.json`
- Create: `contracts/stream/dlq-v1.json`
- Create: `modules/adapter-kafka/src/main/java/io/netsecml/platform/adapter/kafka/sink/FeatureVectorDeserializer.java`
- Create: `modules/adapter-kafka/src/main/java/io/netsecml/platform/adapter/kafka/sink/RejectedEventDeserializer.java`
- Create: `modules/adapter-kafka/src/test/java/io/netsecml/platform/adapter/kafka/sink/StreamContractDriftTest.java`
- Modify: `contracts/stream/README.md`

**Interfaces:**
- Consumes: `FeatureVector` (Task 2), `RejectedEvent` (Task 1), both serializers (Tasks 3-4).
- Produces:
  - `FeatureVectorDeserializer implements org.apache.kafka.common.serialization.Deserializer<FeatureVector>` — method `FeatureVector deserialize(String topic, byte[] data)`.
  - `RejectedEventDeserializer implements Deserializer<RejectedEvent>` — method `RejectedEvent deserialize(String topic, byte[] data)`.
  - Contract JSON shape: an object with `id`, `semanticVersion`, `topic`, `encoding`, and a `fields` array whose entries each carry `name`, `type`, `required`, `description`.

- [ ] **Step 1: Write the frozen contract files**

`contracts/stream/feature-vector-v1.json`:

```json
{
  "id": "feature-vector-v1",
  "semanticVersion": "1.0.0",
  "topic": "netsec.conn.feature-vector.v1",
  "encoding": "application/json",
  "featureSchemaVersion": "conn-feature-v1",
  "fields": [
    { "name": "eventId", "type": "string", "required": true, "description": "Composite <sensor>:<upstreamId> identity, matching feature_vectors.event_id" },
    { "name": "eventTime", "type": "string", "format": "date-time", "required": true, "description": "Zeek event timestamp, ISO-8601 UTC, millisecond precision" },
    { "name": "sensor", "type": "string", "required": true, "description": "Sensor that observed the connection; archived as its own column so training never splits eventId" },
    { "name": "logType", "type": "string", "required": true, "description": "Which Zeek log produced this event. Vocabulary: conn, ssh, dns, http, modbus, s7comm. Only conn is implemented today; the field exists so the contract never needs a v2 when the others land" },
    { "name": "connectionUid", "type": "string", "required": true, "description": "Zeek uid, the cross-protocol correlation key for one connection. Empty string when a log type carries none. NOT a record id — several dns/http records share one uid" },
    { "name": "schemaId", "type": "string", "required": true, "description": "Feature schema id, always conn-feature-v1 for this contract version" },
    { "name": "schemaHash", "type": "string", "required": true, "description": "SHA-256 of the canonical feature schema, 64 lowercase hex characters" },
    { "name": "values", "type": "array<float32>", "required": true, "description": "Exactly 20 values ordered per contracts/features/conn-feature-schema-v1.json" },
    { "name": "qualityFlags", "type": "int32", "required": true, "description": "Bitfield reserved for per-vector quality signals; 0 today" },
    { "name": "producedAt", "type": "string", "format": "date-time", "required": true, "description": "Emission time, ISO-8601 UTC. Archived as row_version, so the latest emission wins deduplication" }
  ]
}
```

`contracts/stream/dlq-v1.json`:

```json
{
  "id": "dlq-v1",
  "semanticVersion": "1.0.0",
  "topic": "netsec.conn.dlq.v1",
  "encoding": "application/json",
  "fields": [
    { "name": "eventId", "type": "string", "required": true, "description": "Composite <sensor>:<upstreamId> identity for MAP-stage rejections; empty string for PARSE-stage, where no DTO existed" },
    { "name": "stage", "type": "string", "required": true, "description": "PARSE when the bytes never became a valid source DTO, MAP when domain validation refused a parsed value" },
    { "name": "reasonCode", "type": "string", "required": true, "description": "Domain ReasonCode name: MALFORMED_JSON, MISSING_REQUIRED_FIELD, INVALID_TIMESTAMP, INVALID_PORT, INVALID_COUNTER" },
    { "name": "detail", "type": "string", "required": true, "description": "Human-readable reason detail; empty string when absent" },
    { "name": "rawPayloadHash", "type": "string", "required": true, "description": "SHA-256 of the rejected bytes, 64 lowercase hex characters. The bytes themselves are never published" },
    { "name": "receivedAt", "type": "string", "format": "date-time", "required": true, "description": "When the pipeline rejected the record, ISO-8601 UTC, stamped at the point of rejection" }
  ]
}
```

- [ ] **Step 2: Write the failing drift tests**

`modules/adapter-kafka/src/test/java/io/netsecml/platform/adapter/kafka/sink/StreamContractDriftTest.java`:

```java
package io.netsecml.platform.adapter.kafka.sink;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netsecml.platform.domain.event.LogType;
import io.netsecml.platform.domain.event.ReasonCode;
import io.netsecml.platform.domain.event.RejectedEvent;
import io.netsecml.platform.domain.event.SensorId;
import io.netsecml.platform.domain.feature.ConnFeatureSchemaV1;
import io.netsecml.platform.domain.feature.FeatureVector;
import org.junit.jupiter.api.Test;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.LinkedHashSet;
import java.util.Set;
import static org.junit.jupiter.api.Assertions.*;

// Contract-versus-code drift is the failure mode that actually happens: someone
// adds a field to a serializer and the frozen contract quietly stops describing
// the wire. These tests turn that into a build failure.
class StreamContractDriftTest {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    // This module lives at modules/adapter-kafka, so the repo root is two up.
    private Path contract(String name) {
        return Paths.get("..", "..", "contracts", "stream", name);
    }

    // The field names the frozen contract declares, in declaration order.
    private Set<String> contractFields(String name) throws IOException {
        JsonNode contract = MAPPER.readTree(contract(name).toFile());
        Set<String> names = new LinkedHashSet<>();
        contract.get("fields").forEach(field -> names.add(field.get("name").asText()));
        return names;
    }

    // The field names a serialized message actually carries.
    private Set<String> messageFields(byte[] message) throws IOException {
        Set<String> names = new LinkedHashSet<>();
        MAPPER.readTree(message).fieldNames().forEachRemaining(names::add);
        return names;
    }

    private FeatureVector vector() {
        return new FeatureVector("sensor-eu-1:abc", Instant.parse("2026-08-13T10:00:00Z"),
            new SensorId("sensor-eu-1"), LogType.CONN, "Cabc123XYZ",
            ConnFeatureSchemaV1.SCHEMA.id(), ConnFeatureSchemaV1.CONTENT_HASH,
            new float[]{1f, 2f, 3f}, 7, Instant.parse("2026-08-13T10:00:00.402Z"));
    }

    private RejectedRecordPayload payload() {
        return new RejectedRecordPayload("{ broken".getBytes(StandardCharsets.UTF_8), "sensor-eu-1:Cabc",
            "MAP", "INVALID_PORT", "port 70000 out of range", Instant.parse("2026-08-27T10:03:11.250Z"));
    }

    @Test
    void featureVectorSerializerEmitsExactlyTheFrozenContractFields() throws Exception {
        Set<String> emitted = messageFields(
            new FeatureVectorSerializer().serialize("netsec.conn.feature-vector.v1", vector()));

        assertEquals(contractFields("feature-vector-v1.json"), emitted,
            "the serializer and contracts/stream/feature-vector-v1.json must describe the same message");
    }

    @Test
    void dlqSerializerEmitsExactlyTheFrozenContractFields() throws Exception {
        Set<String> emitted = messageFields(
            new RejectedRecordSerializer().serialize("netsec.conn.dlq.v1", payload()));

        assertEquals(contractFields("dlq-v1.json"), emitted,
            "the serializer and contracts/stream/dlq-v1.json must describe the same message");
    }

    // Round-tripping proves the deserializer the archive job depends on actually
    // reads what the online job writes — not merely that both mention the same names.
    @Test
    void featureVectorRoundTripsThroughTheDeserializer() throws Exception {
        FeatureVector original = vector();
        byte[] wire = new FeatureVectorSerializer().serialize("netsec.conn.feature-vector.v1", original);

        FeatureVector restored = new FeatureVectorDeserializer()
            .deserialize("netsec.conn.feature-vector.v1", wire);

        assertEquals(original.eventId(), restored.eventId());
        assertEquals(original.eventTime(), restored.eventTime());
        assertEquals(original.sensor(), restored.sensor());
        assertEquals(original.logType(), restored.logType());
        assertEquals(original.connectionUid(), restored.connectionUid());
        assertEquals(original.schemaId(), restored.schemaId());
        assertEquals(original.schemaHash(), restored.schemaHash());
        assertArrayEquals(original.values(), restored.values(), 0.0f);
        assertEquals(original.qualityFlags(), restored.qualityFlags());
        assertEquals(original.producedAt(), restored.producedAt());
    }

    @Test
    void rejectedEventRoundTripsThroughTheDeserializer() throws Exception {
        byte[] wire = new RejectedRecordSerializer().serialize("netsec.conn.dlq.v1", payload());

        RejectedEvent restored = new RejectedEventDeserializer().deserialize("netsec.conn.dlq.v1", wire);

        assertEquals("sensor-eu-1:Cabc", restored.eventId());
        assertEquals(ReasonCode.INVALID_PORT, restored.reason());
        assertEquals("port 70000 out of range", restored.detail());
        assertEquals(64, restored.rawPayloadHash().length());
        assertEquals(Instant.parse("2026-08-27T10:03:11.250Z"), restored.receivedAt());
    }
}
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `./mvnw test -pl modules/adapter-kafka -Dtest=StreamContractDriftTest`
Expected: compilation failure — `FeatureVectorDeserializer` and `RejectedEventDeserializer` do not exist.

- [ ] **Step 4: Write `FeatureVectorDeserializer`**

`modules/adapter-kafka/src/main/java/io/netsecml/platform/adapter/kafka/sink/FeatureVectorDeserializer.java`:

```java
package io.netsecml.platform.adapter.kafka.sink;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netsecml.platform.domain.event.LogType;
import io.netsecml.platform.domain.event.SensorId;
import io.netsecml.platform.domain.feature.FeatureVector;
import org.apache.kafka.common.serialization.Deserializer;
import java.time.Instant;

// Exact inverse of FeatureVectorSerializer. The archive job reads the
// feature-vector topic through this, which is why it lives beside the
// serializer rather than in adapter-clickhouse — keeping JSON knowledge in the
// module that owns Kafka wire formats is what stops the two adapters from
// importing each other.
public final class FeatureVectorDeserializer implements Deserializer<FeatureVector> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public FeatureVector deserialize(String topic, byte[] data) {
        try {
            JsonNode node = objectMapper.readTree(data);

            // values arrives as a JSON array of numbers; narrow each to float32,
            // which is the dtype the model contract fixes.
            JsonNode valuesNode = node.get("values");
            float[] values = new float[valuesNode.size()];
            for (int i = 0; i < values.length; i++) {
                values[i] = (float) valuesNode.get(i).asDouble();
            }

            return new FeatureVector(
                node.get("eventId").asText(),
                Instant.parse(node.get("eventTime").asText()),
                new SensorId(node.get("sensor").asText()),
                LogType.valueOf(node.get("logType").asText().toUpperCase()),
                node.get("connectionUid").asText(),
                node.get("schemaId").asText(),
                node.get("schemaHash").asText(),
                values,
                node.get("qualityFlags").asInt(),
                Instant.parse(node.get("producedAt").asText()));
        } catch (Exception e) {
            // A malformed internal record is a bug, not an expected condition —
            // unlike the external conn topic, this data was written by us.
            throw new IllegalArgumentException("failed to deserialize FeatureVector from topic " + topic, e);
        }
    }
}
```

- [ ] **Step 5: Write `RejectedEventDeserializer`**

`modules/adapter-kafka/src/main/java/io/netsecml/platform/adapter/kafka/sink/RejectedEventDeserializer.java`:

```java
package io.netsecml.platform.adapter.kafka.sink;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netsecml.platform.domain.event.ReasonCode;
import io.netsecml.platform.domain.event.RejectedEvent;
import org.apache.kafka.common.serialization.Deserializer;
import java.time.Instant;

// Reads the DLQ topic into the neutral domain RejectedEvent.
//
// Note the asymmetry with RejectedRecordPayload on the producer side: that type
// carries raw bytes because the serializer hashes them. By the time a message is
// on the wire only the hash exists, so the consumer-side type carries no payload.
//
// stage is deliberately NOT read back — it is derivable from reason.stage(), and
// re-reading it would let a hand-edited message disagree with the domain.
public final class RejectedEventDeserializer implements Deserializer<RejectedEvent> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public RejectedEvent deserialize(String topic, byte[] data) {
        try {
            JsonNode node = objectMapper.readTree(data);
            return new RejectedEvent(
                node.get("eventId").asText(),
                node.get("rawPayloadHash").asText(),
                ReasonCode.valueOf(node.get("reasonCode").asText()),
                node.get("detail").asText(),
                Instant.parse(node.get("receivedAt").asText()));
        } catch (Exception e) {
            throw new IllegalArgumentException("failed to deserialize RejectedEvent from topic " + topic, e);
        }
    }
}
```

- [ ] **Step 6: Run the tests to verify they pass**

Run: `./mvnw test -pl modules/adapter-kafka -Dtest=StreamContractDriftTest`
Expected: PASS, 4 tests.

- [ ] **Step 7: Update `contracts/stream/README.md`**

Replace its contents entirely:

```markdown
# contracts/stream/

Internal, versioned Kafka record contracts. Software owns with AI reason-code
review. Frozen Days 3-9 (Roadmap.md Section 5).

| Contract | Topic | Status |
|---|---|---|
| `feature-vector-v1.json` | `netsec.conn.feature-vector.v1` | **Frozen.** Written by the online job, read by the archive job and by Python training. |
| `dlq-v1.json` | `netsec.conn.dlq.v1` | **Frozen.** Carries both PARSE-stage and MAP-stage rejections, distinguished by the `stage` field. |
| `network-event-v1.json` | `netsec.network-event.v1` | Not frozen. No producer exists; the online job has no normalized-event sink. |
| `prediction-v1.json` | `netsec.prediction.v1` | Not frozen. Arrives with inference on Roadmap Day 9. |
| `invalid-event-v1.json` | `netsec.invalid-event.v1` | Not frozen. Deferred — `dlq-v1` carries both stages today, and no consumer needs them on separate topics yet. |

These files are immutable. A change to a frozen contract creates a new version
(`-v2`); it never edits the committed file. `StreamContractDriftTest` in
`modules/adapter-kafka` asserts that the serializers and these descriptors
describe the same messages.
```

- [ ] **Step 8: Run the full build**

Run: `./mvnw clean verify`
Expected: BUILD SUCCESS.

- [ ] **Step 9: Commit**

```bash
git add contracts/stream/feature-vector-v1.json contracts/stream/dlq-v1.json contracts/stream/README.md \
        modules/adapter-kafka/src/main/java/io/netsecml/platform/adapter/kafka/sink/FeatureVectorDeserializer.java \
        modules/adapter-kafka/src/main/java/io/netsecml/platform/adapter/kafka/sink/RejectedEventDeserializer.java \
        modules/adapter-kafka/src/test/java/io/netsecml/platform/adapter/kafka/sink/StreamContractDriftTest.java
git commit -m "feat(contracts): freeze feature-vector-v1 and dlq-v1 stream contracts with drift tests"
```

---
### Task 6: ClickHouse schema and migration runner

All five MVP tables in one idempotent file, plus the script that applies it. Only two of the five are written by this plan; `predictions` and `model_releases` are created now so Day 9 and Day 11 arrive to tables that already exist, and `network_events` is created but stays optional.

Applying the file against a real server is what verifies it. The tests mount **the same file** the production runner uses — never a test-only copy — so schema drift between test and production is structurally impossible.

**Files:**
- Create: `infrastructure/clickhouse/ddl/001_mvp_tables.sql`
- Create: `scripts/database/apply-ddl.sh`
- Create: `modules/adapter-clickhouse/src/test/java/io/netsecml/platform/adapter/clickhouse/DdlMigrationTest.java`
- Modify: `pom.xml`
- Modify: `modules/adapter-clickhouse/pom.xml`
- Modify: `infrastructure/clickhouse/README.md`
- Delete: `modules/adapter-clickhouse/src/test/java/io/netsecml/platform/adapter/clickhouse/.gitkeep`

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces:
  - Tables `network_events`, `feature_vectors`, `predictions`, `invalid_events`, `model_releases`.
  - `feature_vectors` columns: `event_id String`, `event_time DateTime64(3,'UTC')`, `sensor LowCardinality(String)`, `log_type LowCardinality(String)`, `connection_uid String`, `schema_id LowCardinality(String)`, `schema_hash FixedString(64)`, `` `values` Array(Float32) ``, `quality_flags UInt32`, `archived_at` (server DEFAULT), `row_version DateTime64(3,'UTC')`.
  - `invalid_events` columns: `event_id String`, `event_time Nullable(DateTime64(3,'UTC'))`, `received_at DateTime64(3,'UTC')`, `stage LowCardinality(String)`, `reason_code LowCardinality(String)`, `detail String`, `source_version LowCardinality(String)`, `raw_payload_hash FixedString(64)`, `created_at` (server DEFAULT).
  - `scripts/database/apply-ddl.sh` reading `CLICKHOUSE_HOST`, `CLICKHOUSE_PORT`, `CLICKHOUSE_DATABASE`, `CLICKHOUSE_USER`, `CLICKHOUSE_PASSWORD`.

- [ ] **Step 1: Add `client-v2` to root `dependencyManagement`**

In `pom.xml`, add inside `<dependencyManagement><dependencies>`:

```xml
      <dependency>
        <groupId>com.clickhouse</groupId>
        <artifactId>client-v2</artifactId>
        <version>0.9.0</version>
      </dependency>
```

- [ ] **Step 2: Give `adapter-clickhouse` its dependencies**

Replace the `<dependencies>` block of `modules/adapter-clickhouse/pom.xml` entirely:

```xml
  <dependencies>
    <!--
      domain only. This adapter maps domain values (FeatureVector, RejectedEvent)
      to ClickHouse rows; it needs neither ports nor application, and it must
      never import adapter-kafka or adapter-flink. The unused ports/application
      dependencies from the Step 1 skeleton are removed so the narrow boundary
      this module claims is enforced by its own classpath.
    -->
    <dependency>
      <groupId>io.netsecml.platform</groupId>
      <artifactId>domain</artifactId>
      <version>${project.version}</version>
    </dependency>

    <!-- Rows are serialized to JSONEachRow lines; the @JsonProperty names on the
         row records ARE the ClickHouse column names. -->
    <dependency>
      <groupId>com.fasterxml.jackson.core</groupId>
      <artifactId>jackson-databind</artifactId>
      <version>2.17.1</version>
    </dependency>

    <!-- The official ClickHouse client. Pulls httpclient5 and clickhouse-data. -->
    <dependency>
      <groupId>com.clickhouse</groupId>
      <artifactId>client-v2</artifactId>
    </dependency>

    <!-- Sink V2 interfaces (org.apache.flink.api.connector.sink2). No Kafka
         connector types are used here — the sink is transport-agnostic. -->
    <dependency>
      <groupId>org.apache.flink</groupId>
      <artifactId>flink-streaming-java</artifactId>
    </dependency>

    <dependency>
      <groupId>org.junit.jupiter</groupId>
      <artifactId>junit-jupiter</artifactId>
      <scope>test</scope>
    </dependency>

    <!-- Brings testcontainers core (GenericContainer) transitively. -->
    <dependency>
      <groupId>org.testcontainers</groupId>
      <artifactId>junit-jupiter</artifactId>
      <scope>test</scope>
    </dependency>
  </dependencies>
```

- [ ] **Step 3: Write `001_mvp_tables.sql`**

`infrastructure/clickhouse/ddl/001_mvp_tables.sql`:

```sql
-- ClickHouse MVP tables for the network-security ML platform.
--
-- Applied by scripts/database/apply-ddl.sh and, unchanged, by the Testcontainers
-- integration tests. There is deliberately no second copy of this schema.
--
-- Every statement is CREATE ... IF NOT EXISTS, so re-applying is a no-op.
-- Table names are unqualified: the target database comes from CLICKHOUSE_DATABASE.
--
-- The runner splits this file on the semicolon after stripping line comments, so
-- no statement may contain a semicolon inside a string literal.
--
-- `values` is backtick-quoted everywhere because VALUES is INSERT syntax.

-- feature_vectors: the training source, and the only table this job writes on the
-- happy path. row_version is the producer's producedAt, so a replayed emission is
-- strictly newer and wins deduplication. archived_at is server-set on insert.
CREATE TABLE IF NOT EXISTS feature_vectors (
  event_id      String,
  event_time    DateTime64(3, 'UTC'),
  sensor        LowCardinality(String),
  log_type      LowCardinality(String),
  connection_uid String,
  schema_id     LowCardinality(String),
  schema_hash   FixedString(64),
  `values`      Array(Float32),
  quality_flags UInt32,
  archived_at   DateTime64(3, 'UTC') DEFAULT now64(3),
  row_version   DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(row_version)
PARTITION BY toYYYYMMDD(event_time)
ORDER BY (schema_hash, event_time, event_id)
TTL toDateTime(event_time) + INTERVAL 90 DAY;

-- invalid_events: low-volume forensic quarantine. Plain MergeTree, because a
-- replay legitimately re-rejects and re-stamps a record; duplicates are expected
-- after failures and are deduplicated by raw_payload_hash at query time.
--
-- event_id is populated for MAP-stage rejections, where a DTO parsed before
-- domain validation refused it, and is '' for PARSE-stage. event_time is
-- reserved for a future map-stage improvement and is null today.
CREATE TABLE IF NOT EXISTS invalid_events (
  event_id         String,
  event_time       Nullable(DateTime64(3, 'UTC')),
  received_at      DateTime64(3, 'UTC'),
  stage            LowCardinality(String),
  reason_code      LowCardinality(String),
  detail           String,
  source_version   LowCardinality(String),
  raw_payload_hash FixedString(64),
  created_at       DateTime64(3, 'UTC') DEFAULT now64(3)
) ENGINE = MergeTree
PARTITION BY toYYYYMM(received_at)
ORDER BY (stage, received_at, raw_payload_hash)
TTL toDateTime(received_at) + INTERVAL 30 DAY;

-- predictions: created now, first written on Roadmap Day 9 when inference lands.
CREATE TABLE IF NOT EXISTS predictions (
  prediction_id FixedString(64),
  event_id      String,
  event_time    DateTime64(3, 'UTC'),
  model_name    LowCardinality(String),
  model_version LowCardinality(String),
  model_sha     FixedString(64),
  schema_hash   FixedString(64),
  score         Float32,
  decision      UInt8,
  threshold     Float32,
  inference_us  UInt32,
  quality_flags UInt32,
  created_at    DateTime64(3, 'UTC') DEFAULT now64(3),
  row_version   DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(row_version)
PARTITION BY toYYYYMMDD(event_time)
ORDER BY (model_name, model_version, event_time, event_id)
TTL toDateTime(event_time) + INTERVAL 180 DAY;

-- network_events: optional normalized-event audit. Created but never written by
-- this plan — the online job has no normalized-event sink. This is the only
-- IP-bearing table; enabling it requires a least-privilege ClickHouse user and a
-- documented retention approval first. See docs/clickhouse.md.
CREATE TABLE IF NOT EXISTS network_events (
  event_id      String,
  event_time    DateTime64(3, 'UTC'),
  ingested_at   DateTime64(3, 'UTC'),
  sensor        LowCardinality(String),
  src_ip        IPv6,
  src_port      UInt16,
  dst_ip        IPv6,
  dst_port      UInt16,
  protocol      LowCardinality(String),
  service       LowCardinality(String),
  conn_state    LowCardinality(String),
  duration_ms   UInt64,
  orig_bytes    UInt64,
  resp_bytes    UInt64,
  orig_pkts     UInt64,
  resp_pkts     UInt64,
  quality_flags UInt32,
  row_version   DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(row_version)
PARTITION BY toYYYYMMDD(event_time)
ORDER BY (sensor, event_time, event_id)
TTL toDateTime(event_time) + INTERVAL 14 DAY;

-- model_releases: small release audit mirror, not artifact storage. No TTL.
-- Created now, first written on Roadmap Day 11.
CREATE TABLE IF NOT EXISTS model_releases (
  model_name    LowCardinality(String),
  model_version LowCardinality(String),
  model_sha     FixedString(64),
  schema_hash   FixedString(64),
  dataset_hash  FixedString(64),
  code_hash     String,
  threshold     Float32,
  metrics_json  String,
  status        LowCardinality(String),
  released_by   String,
  released_at   DateTime64(3, 'UTC')
) ENGINE = MergeTree
ORDER BY (model_name, model_version);
```

- [ ] **Step 4: Write `scripts/database/apply-ddl.sh`**

```bash
#!/usr/bin/env bash
# Apply every ClickHouse DDL migration in lexical order.
#
# Idempotent: all statements are CREATE ... IF NOT EXISTS, so re-running is safe.
# Reads the CLICKHOUSE_* variables documented in .env.example.
set -euo pipefail

CLICKHOUSE_HOST="${CLICKHOUSE_HOST:-localhost}"
CLICKHOUSE_PORT="${CLICKHOUSE_PORT:-8123}"
CLICKHOUSE_DATABASE="${CLICKHOUSE_DATABASE:-netsec_ml}"
CLICKHOUSE_USER="${CLICKHOUSE_USER:-default}"
CLICKHOUSE_PASSWORD="${CLICKHOUSE_PASSWORD:-}"

# Resolve the repository root from this script's own location, so the script
# works from any working directory.
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
DDL_DIR="${REPO_ROOT}/infrastructure/clickhouse/ddl"
BASE_URL="http://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT}"

# POST one SQL statement. $2 is the database, or empty for server-level DDL.
post() {
  local sql="$1" db="${2:-}" url="${BASE_URL}/?"
  if [ -n "$db" ]; then
    url="${url}database=${db}&"
  fi
  curl --fail --silent --show-error \
       --user "${CLICKHOUSE_USER}:${CLICKHOUSE_PASSWORD}" \
       --data-binary "$sql" "$url"
}

# Apply one .sql file. The ClickHouse HTTP interface accepts a single statement
# per request, so line comments are stripped first (a ';' inside a comment would
# otherwise split a statement) and the remainder is executed statement by
# statement.
apply_file() {
  local file="$1" statement
  while IFS= read -r -d ';' statement; do
    if [ -n "${statement//[[:space:]]/}" ]; then
      post "$statement" "${CLICKHOUSE_DATABASE}"
    fi
  done < <(sed 's/--.*//' "$file")
}

echo "Creating database ${CLICKHOUSE_DATABASE} if absent"
post "CREATE DATABASE IF NOT EXISTS ${CLICKHOUSE_DATABASE}" ""

shopt -s nullglob
for ddl in "${DDL_DIR}"/*.sql; do
  echo "Applying $(basename "$ddl")"
  apply_file "$ddl"
done

echo "DDL applied to ${CLICKHOUSE_DATABASE}"
```

Then make it executable:

```bash
chmod +x scripts/database/apply-ddl.sh
```

- [ ] **Step 5: Write the migration test**

Delete the placeholder first:

```bash
git rm modules/adapter-clickhouse/src/test/java/io/netsecml/platform/adapter/clickhouse/.gitkeep
```

`modules/adapter-clickhouse/src/test/java/io/netsecml/platform/adapter/clickhouse/DdlMigrationTest.java`:

```java
package io.netsecml.platform.adapter.clickhouse;

import com.clickhouse.client.api.Client;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Set;
import java.util.stream.Collectors;
import static org.junit.jupiter.api.Assertions.*;

// Applying the DDL against a real server is the only thing that verifies it.
// These tests use the SAME file scripts/database/apply-ddl.sh applies — there is
// no test-only copy of the schema to drift.
@Testcontainers(disabledWithoutDocker = true)
class DdlMigrationTest {
    // Exactly what 001_mvp_tables.sql must create.
    private static final Set<String> EXPECTED_TABLES = Set.of(
        "feature_vectors", "invalid_events", "predictions", "network_events", "model_releases");

    // A bare ClickHouse container, deliberately GenericContainer rather than
    // testcontainers' ClickHouseContainer: that one extends JdbcDatabaseContainer
    // and waits for readiness through a JDBC driver this project does not ship.
    // One container for the whole class; each test isolates itself in its own
    // database instead.
    @Container
    private static final GenericContainer<?> CLICKHOUSE =
        new GenericContainer<>(DockerImageName.parse("clickhouse/clickhouse-server:25.8"))
            .withExposedPorts(8123)
            .waitingFor(Wait.forHttp("/ping").forPort(8123).forStatusCode(200));

    // This module lives at modules/adapter-clickhouse, so the repo root is two up.
    private static Path repoPath(String... parts) {
        return Paths.get("..", "..", String.join("/", parts));
    }

    private static Client clientFor(String database) {
        return new Client.Builder()
            .addEndpoint("http://" + CLICKHOUSE.getHost() + ":" + CLICKHOUSE.getMappedPort(8123))
            .setUsername("default")
            .setPassword("")
            .setDefaultDatabase(database)
            .build();
    }

    private static void createDatabase(String database) throws Exception {
        try (Client bootstrap = clientFor("default")) {
            bootstrap.execute("CREATE DATABASE IF NOT EXISTS " + database).get();
        }
    }

    // Applies the DDL file statement by statement. The HTTP interface takes one
    // statement per request, so line comments are stripped and the remainder is
    // split on ';' — the same treatment apply-ddl.sh gives it.
    private static void applyDdl(Client client) throws Exception {
        String sql = Files.readString(repoPath("infrastructure", "clickhouse", "ddl", "001_mvp_tables.sql"))
            .replaceAll("(?m)--.*$", "");
        for (String statement : sql.split(";")) {
            if (!statement.isBlank()) {
                client.execute(statement).get();
            }
        }
    }

    private static Set<String> tableNames(Client client) {
        return client.queryAll("SHOW TABLES").stream()
            .map(record -> record.getString("name"))
            .collect(Collectors.toSet());
    }

    @Test
    void createsAllFiveMvpTables() throws Exception {
        createDatabase("ddl_fresh");
        try (Client client = clientFor("ddl_fresh")) {
            applyDdl(client);
            assertEquals(EXPECTED_TABLES, tableNames(client));
        }
    }

    // A migration runner that cannot be run twice is a migration runner nobody
    // dares run once.
    @Test
    void reapplyingTheDdlIsANoOp() throws Exception {
        createDatabase("ddl_twice");
        try (Client client = clientFor("ddl_twice")) {
            applyDdl(client);
            applyDdl(client);
            assertEquals(EXPECTED_TABLES, tableNames(client));
        }
    }

    // Exercises the actual shipped script, not a Java reimplementation of it.
    // Skipped off Linux, where bash and curl may be absent.
    @Test
    @EnabledOnOs(OS.LINUX)
    void applyDdlScriptCreatesTheSameTables() throws Exception {
        String database = "ddl_via_script";

        ProcessBuilder builder = new ProcessBuilder(
            "bash", repoPath("scripts", "database", "apply-ddl.sh").toAbsolutePath().toString());
        builder.environment().put("CLICKHOUSE_HOST", CLICKHOUSE.getHost());
        builder.environment().put("CLICKHOUSE_PORT", String.valueOf(CLICKHOUSE.getMappedPort(8123)));
        builder.environment().put("CLICKHOUSE_DATABASE", database);
        builder.environment().put("CLICKHOUSE_USER", "default");
        builder.environment().put("CLICKHOUSE_PASSWORD", "");
        builder.redirectErrorStream(true);

        Process process = builder.start();
        String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        assertEquals(0, process.waitFor(), "apply-ddl.sh exited non-zero:\n" + output);

        try (Client client = clientFor(database)) {
            assertEquals(EXPECTED_TABLES, tableNames(client), "script output:\n" + output);
        }
    }
}
```

- [ ] **Step 6: Run the migration test**

Run: `./mvnw test -pl modules/adapter-clickhouse -Dtest=DdlMigrationTest`
Expected: PASS, 3 tests. Requires Docker; without it the class is skipped.

If a `CREATE TABLE` fails, read the ClickHouse error in the exception — it names the offending column or expression. Common causes: a `Nullable` column placed in `ORDER BY` (not allowed), or a `TTL` expression on a column that is not a Date/DateTime.

- [ ] **Step 7: Rewrite `infrastructure/clickhouse/README.md`**

```markdown
# infrastructure/clickhouse/

`ddl/001_mvp_tables.sql` defines the five MVP tables. Apply it with
`scripts/database/apply-ddl.sh`, which is idempotent — every statement is
`CREATE ... IF NOT EXISTS`.

| Table | Engine | Retention | First writer |
|---|---|---|---|
| `feature_vectors` | `ReplacingMergeTree(row_version)` | 90 days | archive job (today) |
| `invalid_events` | `MergeTree` | 30 days | archive job (today) |
| `predictions` | `ReplacingMergeTree(row_version)` | 180 days | Roadmap Day 9 |
| `network_events` | `ReplacingMergeTree(row_version)` | 14 days | optional; no producer exists |
| `model_releases` | `MergeTree` | none | Roadmap Day 11 |

The five-table set and the `model_releases` naming follow `FINAL_ARCHITECTURE.md`
Step 8 rather than the four-table list in `Roadmap.md`; see
`docs/adr/0001-clickhouse-table-set.md` for why. Column types, deduplication
semantics and the access requirements for `network_events` are documented in
`docs/clickhouse.md`.

`DdlMigrationTest` in `modules/adapter-clickhouse` applies this exact file against
a ClickHouse container, so there is no test-only copy of the schema.
```

- [ ] **Step 8: Run the full build**

Run: `./mvnw clean verify`
Expected: BUILD SUCCESS.

- [ ] **Step 9: Commit**

```bash
git add pom.xml modules/adapter-clickhouse/pom.xml \
        infrastructure/clickhouse/ddl/001_mvp_tables.sql infrastructure/clickhouse/README.md \
        scripts/database/apply-ddl.sh \
        modules/adapter-clickhouse/src/test/java/io/netsecml/platform/adapter/clickhouse/DdlMigrationTest.java
git commit -m "feat(infrastructure): add ClickHouse MVP schema and idempotent DDL runner"
```

---
### Task 7: ClickHouse rows and mappers

The rows are the boundary between domain values and ClickHouse columns. Their `@JsonProperty` names **are** the column names — the sink serializes a row straight to a JSONEachRow line — so renaming one renames a column in the insert.

`adapter-clickhouse` consumes `FeatureVector` and `RejectedEvent`, both domain types. It imports nothing from `adapter-kafka` or `adapter-flink`.

**Files:**
- Create: `modules/adapter-clickhouse/src/main/java/io/netsecml/platform/adapter/clickhouse/row/FeatureVectorRow.java`
- Create: `modules/adapter-clickhouse/src/main/java/io/netsecml/platform/adapter/clickhouse/row/InvalidEventRow.java`
- Create: `modules/adapter-clickhouse/src/main/java/io/netsecml/platform/adapter/clickhouse/mapper/ClickHouseTimestamps.java`
- Create: `modules/adapter-clickhouse/src/main/java/io/netsecml/platform/adapter/clickhouse/mapper/FeatureVectorRowMapper.java`
- Create: `modules/adapter-clickhouse/src/main/java/io/netsecml/platform/adapter/clickhouse/mapper/InvalidEventRowMapper.java`
- Test: `modules/adapter-clickhouse/src/test/java/io/netsecml/platform/adapter/clickhouse/mapper/FeatureVectorRowMapperTest.java`
- Test: `modules/adapter-clickhouse/src/test/java/io/netsecml/platform/adapter/clickhouse/mapper/InvalidEventRowMapperTest.java`

**Interfaces:**
- Consumes: `FeatureVector` (Task 2), `RejectedEvent` and `ReasonCode.stage()` (Task 1).
- Produces:
  - `FeatureVectorRow(String eventId, String eventTime, String sensor, String logType, String connectionUid, String schemaId, String schemaHash, float[] values, int qualityFlags, String rowVersion)` — JSON keys `event_id`, `event_time`, `sensor`, `log_type`, `connection_uid`, `schema_id`, `schema_hash`, `values`, `quality_flags`, `row_version`.
  - `InvalidEventRow(String eventId, String eventTime, String receivedAt, String stage, String reasonCode, String detail, String sourceVersion, String rawPayloadHash)` — JSON keys `event_id`, `event_time`, `received_at`, `stage`, `reason_code`, `detail`, `source_version`, `raw_payload_hash`.
  - `FeatureVectorRowMapper.toRow(FeatureVector): FeatureVectorRow`
  - `InvalidEventRowMapper.toRow(RejectedEvent): InvalidEventRow`
  - `InvalidEventRowMapper.SOURCE_VERSION` — the constant `"zeek-conn-source-v1"`.

- [ ] **Step 1: Write the failing tests**

`modules/adapter-clickhouse/src/test/java/io/netsecml/platform/adapter/clickhouse/mapper/FeatureVectorRowMapperTest.java`:

```java
package io.netsecml.platform.adapter.clickhouse.mapper;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netsecml.platform.adapter.clickhouse.row.FeatureVectorRow;
import io.netsecml.platform.domain.event.SensorId;
import io.netsecml.platform.domain.feature.FeatureVector;
import org.junit.jupiter.api.Test;
import java.time.Instant;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import static org.junit.jupiter.api.Assertions.*;

class FeatureVectorRowMapperTest {
    private final FeatureVectorRowMapper mapper = new FeatureVectorRowMapper();

    private FeatureVector vector() {
        return new FeatureVector("sensor-eu-1:abc", Instant.parse("2026-08-27T10:03:11.250Z"),
            new SensorId("sensor-eu-1"), LogType.CONN, "Cabc123XYZ", "conn-feature-v1", "f".repeat(64),
            new float[]{1f, 2f, 3f}, 7, Instant.parse("2026-08-27T10:03:11.402Z"));
    }

    @Test
    void copiesIdentityAndPayloadAcross() {
        FeatureVectorRow row = mapper.toRow(vector());

        assertEquals("sensor-eu-1:abc", row.eventId());
        assertEquals("sensor-eu-1", row.sensor());
        assertEquals("conn-feature-v1", row.schemaId());
        assertEquals("f".repeat(64), row.schemaHash());
        assertArrayEquals(new float[]{1f, 2f, 3f}, row.values(), 0.0f);
        assertEquals(7, row.qualityFlags());
    }

    // ClickHouse parses DateTime64(3) from 'yyyy-MM-dd HH:mm:ss.SSS'. ISO-8601
    // with T and Z is not reliably accepted, so the mapper must reformat.
    @Test
    void formatsTimestampsForDateTime64() {
        FeatureVectorRow row = mapper.toRow(vector());

        assertEquals("2026-08-27 10:03:11.250", row.eventTime());
        assertEquals("2026-08-27 10:03:11.402", row.rowVersion());
    }

    // producedAt becomes row_version. That is the whole deduplication contract.
    @Test
    void usesProducedAtAsRowVersion() {
        FeatureVector later = new FeatureVector("sensor-eu-1:abc", Instant.parse("2026-08-27T10:03:11.250Z"),
            new SensorId("sensor-eu-1"), LogType.CONN, "Cabc123XYZ", "conn-feature-v1", "f".repeat(64),
            new float[]{1f}, 0, Instant.parse("2026-08-27T11:00:00.000Z"));

        assertEquals("2026-08-27 11:00:00.000", mapper.toRow(later).rowVersion());
    }

    // The @JsonProperty names ARE the ClickHouse column names — the sink writes
    // this record straight out as a JSONEachRow line. archived_at is absent on
    // purpose: the column carries DEFAULT now64(3) and is set server-side.
    @Test
    void serializesToExactlyTheClickHouseColumnNames() throws Exception {
        String json = new ObjectMapper().writeValueAsString(mapper.toRow(vector()));

        Set<String> emitted = new LinkedHashSet<>();
        new ObjectMapper().readTree(json).fieldNames().forEachRemaining(emitted::add);

        assertEquals(new LinkedHashSet<>(List.of(
            "event_id", "event_time", "sensor", "log_type", "connection_uid",
            "schema_id", "schema_hash", "values", "quality_flags", "row_version")), emitted);
    }

    // The row is handed to a serializer on another thread's flush; it must not
    // share the array it was built from.
    @Test
    void defensivelyCopiesValues() {
        float[] values = new float[]{1f, 2f, 3f};
        FeatureVectorRow row = new FeatureVectorRow("id", "t", "s", "conn", "Cuid", "sid", "h", values, 0, "v");

        values[0] = -1f;
        assertEquals(1f, row.values()[0]);

        row.values()[0] = 99f;
        assertEquals(1f, row.values()[0]);
    }
}
```

`modules/adapter-clickhouse/src/test/java/io/netsecml/platform/adapter/clickhouse/mapper/InvalidEventRowMapperTest.java`:

```java
package io.netsecml.platform.adapter.clickhouse.mapper;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.netsecml.platform.adapter.clickhouse.row.InvalidEventRow;
import io.netsecml.platform.domain.event.ReasonCode;
import io.netsecml.platform.domain.event.RejectedEvent;
import org.junit.jupiter.api.Test;
import java.time.Instant;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import static org.junit.jupiter.api.Assertions.*;

class InvalidEventRowMapperTest {
    private static final String HASH = "a".repeat(64);
    private final InvalidEventRowMapper mapper = new InvalidEventRowMapper();

    // A map-stage rejection parsed cleanly before domain validation refused it,
    // so it knows which event failed.
    @Test
    void mapsAMapStageRejection() {
        RejectedEvent event = new RejectedEvent("sensor-eu-1:Cabc", HASH, ReasonCode.INVALID_PORT,
            "port 70000 out of range", Instant.parse("2026-08-27T10:03:11.250Z"));

        InvalidEventRow row = mapper.toRow(event);

        assertEquals("sensor-eu-1:Cabc", row.eventId());
        assertEquals("MAP", row.stage());
        assertEquals("INVALID_PORT", row.reasonCode());
        assertEquals("port 70000 out of range", row.detail());
        assertEquals(HASH, row.rawPayloadHash());
        assertEquals("2026-08-27 10:03:11.250", row.receivedAt());
        assertEquals(InvalidEventRowMapper.SOURCE_VERSION, row.sourceVersion());
    }

    // A parse-stage rejection never produced a DTO. event_id is the empty string
    // and event_time stays null — the column is Nullable for exactly this case.
    @Test
    void mapsAParseStageRejectionWithNoIdentity() {
        RejectedEvent event = new RejectedEvent(null, HASH, ReasonCode.MALFORMED_JSON,
            "unexpected end of input", Instant.parse("2026-08-27T10:03:11.250Z"));

        InvalidEventRow row = mapper.toRow(event);

        assertEquals("", row.eventId());
        assertNull(row.eventTime());
        assertEquals("PARSE", row.stage());
    }

    // The stage is taken from the domain's ReasonCode, never re-derived here.
    @Test
    void takesStageFromTheDomainReasonCode() {
        for (ReasonCode code : ReasonCode.values()) {
            RejectedEvent event = new RejectedEvent("", HASH, code, "", Instant.parse("2026-08-27T10:03:11.250Z"));
            assertEquals(code.stage().name(), mapper.toRow(event).stage());
        }
    }

    @Test
    void serializesToExactlyTheClickHouseColumnNames() throws Exception {
        RejectedEvent event = new RejectedEvent("id", HASH, ReasonCode.INVALID_PORT, "d",
            Instant.parse("2026-08-27T10:03:11.250Z"));

        String json = new ObjectMapper().writeValueAsString(mapper.toRow(event));
        Set<String> emitted = new LinkedHashSet<>();
        new ObjectMapper().readTree(json).fieldNames().forEachRemaining(emitted::add);

        assertEquals(new LinkedHashSet<>(List.of(
            "event_id", "event_time", "received_at", "stage", "reason_code",
            "detail", "source_version", "raw_payload_hash")), emitted);
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `./mvnw test -pl modules/adapter-clickhouse -Dtest='FeatureVectorRowMapperTest+InvalidEventRowMapperTest'`
Expected: compilation failure — none of the row or mapper classes exist.

- [ ] **Step 3: Write the row records**

`modules/adapter-clickhouse/src/main/java/io/netsecml/platform/adapter/clickhouse/row/FeatureVectorRow.java`:

```java
package io.netsecml.platform.adapter.clickhouse.row;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Arrays;

// One feature_vectors row.
//
// The @JsonProperty names ARE the ClickHouse column names: the sink serializes
// this record straight to a JSONEachRow line, so renaming a property renames a
// column in the insert.
//
// Timestamps are already formatted strings, not Instants — ClickHouse parses
// DateTime64(3) from 'yyyy-MM-dd HH:mm:ss.SSS', and doing the conversion in the
// mapper keeps the serializer free of date logic.
//
// archived_at is absent on purpose: that column carries DEFAULT now64(3) and is
// set server-side, so sending it would only let the client disagree with the server.
public record FeatureVectorRow(
    @JsonProperty("event_id") String eventId,
    @JsonProperty("event_time") String eventTime,
    @JsonProperty("sensor") String sensor,
    @JsonProperty("log_type") String logType,
    @JsonProperty("connection_uid") String connectionUid,
    @JsonProperty("schema_id") String schemaId,
    @JsonProperty("schema_hash") String schemaHash,
    @JsonProperty("values") float[] values,
    @JsonProperty("quality_flags") int qualityFlags,
    @JsonProperty("row_version") String rowVersion) {

    public FeatureVectorRow {
        // Defensive copy in: the row outlives the call that built it, travelling
        // in a buffer until the next flush.
        values = Arrays.copyOf(values, values.length);
    }

    // Defensive copy out.
    @Override
    public float[] values() {
        return Arrays.copyOf(values, values.length);
    }
}
```

`modules/adapter-clickhouse/src/main/java/io/netsecml/platform/adapter/clickhouse/row/InvalidEventRow.java`:

```java
package io.netsecml.platform.adapter.clickhouse.row;

import com.fasterxml.jackson.annotation.JsonProperty;

// One invalid_events row. As with FeatureVectorRow, the @JsonProperty names are
// the ClickHouse column names.
//
// eventTime is null for every rejection this pipeline currently produces — the
// column is Nullable(DateTime64(3,'UTC')) and is reserved for a future map-stage
// improvement that recovers the source timestamp.
//
// created_at is absent on purpose: server-side DEFAULT now64(3).
public record InvalidEventRow(
    @JsonProperty("event_id") String eventId,
    @JsonProperty("event_time") String eventTime,
    @JsonProperty("received_at") String receivedAt,
    @JsonProperty("stage") String stage,
    @JsonProperty("reason_code") String reasonCode,
    @JsonProperty("detail") String detail,
    @JsonProperty("source_version") String sourceVersion,
    @JsonProperty("raw_payload_hash") String rawPayloadHash) {
}
```

- [ ] **Step 4: Write the timestamp helper and both mappers**

`modules/adapter-clickhouse/src/main/java/io/netsecml/platform/adapter/clickhouse/mapper/ClickHouseTimestamps.java`:

```java
package io.netsecml.platform.adapter.clickhouse.mapper;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

// ClickHouse parses DateTime64(3) from 'yyyy-MM-dd HH:mm:ss.SSS'. ISO-8601 with
// 'T' and 'Z' — what Instant.toString() produces — is not reliably accepted, so
// every Instant crossing into a row goes through here.
final class ClickHouseTimestamps {
    private static final DateTimeFormatter DATETIME64 =
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS").withZone(ZoneOffset.UTC);

    private ClickHouseTimestamps() {
    }

    static String format(Instant instant) {
        return DATETIME64.format(instant);
    }
}
```

`modules/adapter-clickhouse/src/main/java/io/netsecml/platform/adapter/clickhouse/mapper/FeatureVectorRowMapper.java`:

```java
package io.netsecml.platform.adapter.clickhouse.mapper;

import io.netsecml.platform.adapter.clickhouse.row.FeatureVectorRow;
import io.netsecml.platform.domain.feature.FeatureVector;
import java.io.Serializable;

// Turns a domain FeatureVector into a feature_vectors row.
//
// Serializable because Flink map functions hold an instance across the network.
// Stateless, so one instance per subtask is enough.
public final class FeatureVectorRowMapper implements Serializable {

    public FeatureVectorRow toRow(FeatureVector vector) {
        return new FeatureVectorRow(
            vector.eventId(),
            ClickHouseTimestamps.format(vector.eventTime()),
            vector.sensor().value(),
            vector.logType().wireName(),
            vector.connectionUid(),
            vector.schemaId(),
            vector.schemaHash(),
            vector.values(),
            vector.qualityFlags(),
            // producedAt becomes row_version. After a checkpoint restore the
            // online job re-stamps it, so the replayed emission is strictly newer
            // and wins argMax deduplication — which is correct, because its
            // indices 17-19 came from the restored window state.
            ClickHouseTimestamps.format(vector.producedAt()));
    }
}
```

`modules/adapter-clickhouse/src/main/java/io/netsecml/platform/adapter/clickhouse/mapper/InvalidEventRowMapper.java`:

```java
package io.netsecml.platform.adapter.clickhouse.mapper;

import io.netsecml.platform.adapter.clickhouse.row.InvalidEventRow;
import io.netsecml.platform.domain.event.RejectedEvent;
import java.io.Serializable;

// Turns a domain RejectedEvent into an invalid_events row.
public final class InvalidEventRowMapper implements Serializable {

    // The source contract the rejected bytes claimed to satisfy. Constant while
    // the platform ingests only Zeek conn logs; it becomes a parameter the day a
    // second source type appears.
    public static final String SOURCE_VERSION = "zeek-conn-source-v1";

    public InvalidEventRow toRow(RejectedEvent event) {
        return new InvalidEventRow(
            event.eventId(),
            // No rejection path recovers a source timestamp today. The column is
            // Nullable for exactly this reason.
            null,
            ClickHouseTimestamps.format(event.receivedAt()),
            // stage comes from the domain, never re-derived from the reason name.
            event.reason().stage().name(),
            event.reason().name(),
            event.detail(),
            SOURCE_VERSION,
            event.rawPayloadHash());
    }
}
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `./mvnw test -pl modules/adapter-clickhouse -Dtest='FeatureVectorRowMapperTest+InvalidEventRowMapperTest'`
Expected: PASS, 9 tests.

- [ ] **Step 6: Commit**

```bash
git add modules/adapter-clickhouse/src/main/java/io/netsecml/platform/adapter/clickhouse/row/ \
        modules/adapter-clickhouse/src/main/java/io/netsecml/platform/adapter/clickhouse/mapper/ \
        modules/adapter-clickhouse/src/test/java/io/netsecml/platform/adapter/clickhouse/mapper/
git commit -m "feat(adapter-clickhouse): add feature_vectors and invalid_events row mappers"
```

---

### Task 8: `BatchBuffer`

The bounded accumulator behind the sink's flush triggers. Pure logic — no Flink, no ClickHouse, no clock — so every threshold is testable in milliseconds. The time trigger lives in the sink writer, which owns Flink's `ProcessingTimeService`; the buffer only knows about rows and bytes.

**Files:**
- Create: `modules/adapter-clickhouse/src/main/java/io/netsecml/platform/adapter/clickhouse/batch/BatchBuffer.java`
- Test: `modules/adapter-clickhouse/src/test/java/io/netsecml/platform/adapter/clickhouse/batch/BatchBufferTest.java`

**Interfaces:**
- Consumes: nothing.
- Produces:
  - `BatchBuffer(int maxRows, long maxBytes)`
  - `boolean add(String jsonLine)` — returns `true` when a flush trigger is reached.
  - `boolean isFull()`, `boolean isEmpty()`, `int size()`, `long byteSize()`
  - `List<String> drain()` — returns everything buffered and empties the buffer.

- [ ] **Step 1: Write the failing test**

`modules/adapter-clickhouse/src/test/java/io/netsecml/platform/adapter/clickhouse/batch/BatchBufferTest.java`:

```java
package io.netsecml.platform.adapter.clickhouse.batch;

import org.junit.jupiter.api.Test;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

class BatchBufferTest {
    // A line whose UTF-8 length is exactly 10 bytes, so byte thresholds are easy
    // to reason about below.
    private static final String TEN_BYTE_LINE = "0123456789";

    @Test
    void startsEmpty() {
        BatchBuffer buffer = new BatchBuffer(5, 1000);

        assertTrue(buffer.isEmpty());
        assertFalse(buffer.isFull());
        assertEquals(0, buffer.size());
        assertEquals(0, buffer.byteSize());
    }

    // The row-count trigger. add() reports the trigger so the caller flushes
    // without having to ask twice.
    @Test
    void signalsFullAtTheRowLimit() {
        BatchBuffer buffer = new BatchBuffer(3, 1_000_000);

        assertFalse(buffer.add("a"));
        assertFalse(buffer.add("b"));
        assertTrue(buffer.add("c"), "third row reaches the 3-row limit");
        assertTrue(buffer.isFull());
    }

    // The byte trigger. Each line counts its UTF-8 length plus the newline that
    // separates JSONEachRow records.
    @Test
    void signalsFullAtTheByteLimit() {
        BatchBuffer buffer = new BatchBuffer(1_000_000, 22);

        assertFalse(buffer.add(TEN_BYTE_LINE), "11 bytes buffered, under the limit");
        assertTrue(buffer.add(TEN_BYTE_LINE), "22 bytes buffered, reaches the limit");
        assertEquals(22, buffer.byteSize());
    }

    // Byte accounting must be UTF-8, not character count, or a batch of non-ASCII
    // detail strings would silently exceed the configured payload bound.
    @Test
    void countsBytesNotCharacters() {
        BatchBuffer buffer = new BatchBuffer(1_000_000, 1_000_000);

        // Two 3-byte UTF-8 characters plus the newline.
        buffer.add("中文");

        assertEquals(7, buffer.byteSize());
    }

    @Test
    void drainReturnsEverythingAndEmptiesTheBuffer() {
        BatchBuffer buffer = new BatchBuffer(10, 1_000_000);
        buffer.add("a");
        buffer.add("b");

        assertEquals(List.of("a", "b"), buffer.drain());
        assertTrue(buffer.isEmpty());
        assertEquals(0, buffer.size());
        assertEquals(0, buffer.byteSize(), "byte accounting resets with the rows");
        assertFalse(buffer.isFull());
    }

    @Test
    void drainOnAnEmptyBufferReturnsAnEmptyList() {
        assertEquals(List.of(), new BatchBuffer(10, 1000).drain());
    }

    // The drained list must not alias internal state — the sink holds it across a
    // retry loop while new rows may already be arriving.
    @Test
    void drainedListIsIndependentOfLaterAdds() {
        BatchBuffer buffer = new BatchBuffer(10, 1_000_000);
        buffer.add("a");

        List<String> drained = buffer.drain();
        buffer.add("b");

        assertEquals(List.of("a"), drained);
    }

    @Test
    void rejectsNonPositiveLimits() {
        assertThrows(IllegalArgumentException.class, () -> new BatchBuffer(0, 1000));
        assertThrows(IllegalArgumentException.class, () -> new BatchBuffer(10, 0));
    }
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `./mvnw test -pl modules/adapter-clickhouse -Dtest=BatchBufferTest`
Expected: compilation failure — `BatchBuffer` does not exist.

- [ ] **Step 3: Write `BatchBuffer`**

`modules/adapter-clickhouse/src/main/java/io/netsecml/platform/adapter/clickhouse/batch/BatchBuffer.java`:

```java
package io.netsecml.platform.adapter.clickhouse.batch;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

// Accumulates encoded JSONEachRow lines until a bounded flush trigger fires.
//
// Deliberately knows nothing about Flink, ClickHouse or time: the row and byte
// limits are the only triggers it owns, and the 1-second timer lives in the sink
// writer, which has Flink's ProcessingTimeService. That split is what makes every
// threshold here testable without a runtime.
//
// The bounds are also the memory bound. A ClickHouse outage must produce Kafka
// lag, never unbounded heap growth, and the buffer can never exceed one batch
// because the writer flushes or throws the moment a trigger fires.
public final class BatchBuffer {
    private final int maxRows;
    private final long maxBytes;
    private final List<String> lines = new ArrayList<>();

    // Running UTF-8 size of the serialized batch, including the newline that
    // separates JSONEachRow records.
    private long bytes;

    public BatchBuffer(int maxRows, long maxBytes) {
        if (maxRows <= 0) {
            throw new IllegalArgumentException("maxRows must be positive, was " + maxRows);
        }
        if (maxBytes <= 0) {
            throw new IllegalArgumentException("maxBytes must be positive, was " + maxBytes);
        }
        this.maxRows = maxRows;
        this.maxBytes = maxBytes;
    }

    // Buffers one line. Returns true when a flush trigger has been reached, so
    // the caller can act without a second call.
    public boolean add(String jsonLine) {
        lines.add(jsonLine);

        // UTF-8 length, not character count: a batch of non-ASCII detail strings
        // would otherwise slip past the configured payload bound.
        bytes += jsonLine.getBytes(StandardCharsets.UTF_8).length + 1;

        return isFull();
    }

    public boolean isFull() {
        return lines.size() >= maxRows || bytes >= maxBytes;
    }

    public boolean isEmpty() {
        return lines.isEmpty();
    }

    public int size() {
        return lines.size();
    }

    public long byteSize() {
        return bytes;
    }

    // Removes and returns everything buffered. The returned list is a snapshot:
    // the sink holds it across a retry loop while new rows may already be arriving.
    public List<String> drain() {
        List<String> drained = List.copyOf(lines);
        lines.clear();
        bytes = 0;
        return drained;
    }
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `./mvnw test -pl modules/adapter-clickhouse -Dtest=BatchBufferTest`
Expected: PASS, 8 tests.

- [ ] **Step 5: Commit**

```bash
git add modules/adapter-clickhouse/src/main/java/io/netsecml/platform/adapter/clickhouse/batch/BatchBuffer.java \
        modules/adapter-clickhouse/src/test/java/io/netsecml/platform/adapter/clickhouse/batch/BatchBufferTest.java
git commit -m "feat(adapter-clickhouse): add bounded BatchBuffer with row and byte flush triggers"
```

---
### Task 9: Connection config and the `client-v2` inserter

A one-method seam (`ClickHouseInserter`) separates "talk to ClickHouse" from "decide when and how often". That is what lets Task 10 test the retry and flush logic against a fake, and lets this task test the real HTTP path against a real server.

This task's integration test is also where the row records from Task 7 meet the real schema from Task 6: an insert with a wrong `@JsonProperty` name fails here, loudly, rather than in the end-to-end test.

**Files:**
- Create: `modules/adapter-clickhouse/src/main/java/io/netsecml/platform/adapter/clickhouse/writer/ClickHouseConfig.java`
- Create: `modules/adapter-clickhouse/src/main/java/io/netsecml/platform/adapter/clickhouse/writer/ClickHouseInserter.java`
- Create: `modules/adapter-clickhouse/src/main/java/io/netsecml/platform/adapter/clickhouse/writer/ClickHouseInserterFactory.java`
- Create: `modules/adapter-clickhouse/src/main/java/io/netsecml/platform/adapter/clickhouse/writer/ClientV2Inserter.java`
- Create: `modules/adapter-clickhouse/src/test/java/io/netsecml/platform/adapter/clickhouse/ClickHouseTestSupport.java`
- Create: `modules/adapter-clickhouse/src/test/java/io/netsecml/platform/adapter/clickhouse/writer/ClientV2InserterTest.java`
- Modify: `modules/adapter-clickhouse/src/test/java/io/netsecml/platform/adapter/clickhouse/DdlMigrationTest.java`

**Interfaces:**
- Consumes: `FeatureVectorRow`, `InvalidEventRow`, both mappers (Task 7); the DDL (Task 6).
- Produces:
  - `ClickHouseConfig(String endpoint, String database, String username, String password) implements Serializable`, plus `static ClickHouseConfig of(String host, int port, String database, String username, String password)`.
  - `interface ClickHouseInserter extends AutoCloseable` — `void insert(String table, List<String> jsonLines) throws Exception` and `void close()`.
  - `interface ClickHouseInserterFactory extends Serializable` — `ClickHouseInserter create(ClickHouseConfig config)`.
  - `ClientV2Inserter(ClickHouseConfig config)`.
  - Test helper `ClickHouseTestSupport` with `newContainer()`, `clientFor(GenericContainer, String database)`, `createDatabase(GenericContainer, String database)`, `applyDdl(Client)`, `tableNames(Client)`, `repoPath(String...)`.

- [ ] **Step 1: Extract the container helpers into `ClickHouseTestSupport`**

A second test now needs the container, database and DDL plumbing that Task 6 wrote inline in `DdlMigrationTest`. Move it, then have `DdlMigrationTest` call the extracted version.

`modules/adapter-clickhouse/src/test/java/io/netsecml/platform/adapter/clickhouse/ClickHouseTestSupport.java`:

```java
package io.netsecml.platform.adapter.clickhouse;

import com.clickhouse.client.api.Client;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Set;
import java.util.stream.Collectors;

// Shared plumbing for every ClickHouse container test in this module.
//
// Container tests here deliberately use GenericContainer rather than
// testcontainers' ClickHouseContainer: that one extends JdbcDatabaseContainer and
// waits for readiness through a JDBC driver this project does not ship.
final class ClickHouseTestSupport {
    static final int HTTP_PORT = 8123;

    private ClickHouseTestSupport() {
    }

    static GenericContainer<?> newContainer() {
        return new GenericContainer<>(DockerImageName.parse("clickhouse/clickhouse-server:25.8"))
            .withExposedPorts(HTTP_PORT)
            .waitingFor(Wait.forHttp("/ping").forPort(HTTP_PORT).forStatusCode(200));
    }

    // This module lives at modules/adapter-clickhouse, so the repo root is two up.
    static Path repoPath(String... parts) {
        return Paths.get("..", "..", String.join("/", parts));
    }

    static Client clientFor(GenericContainer<?> container, String database) {
        return new Client.Builder()
            .addEndpoint("http://" + container.getHost() + ":" + container.getMappedPort(HTTP_PORT))
            .setUsername("default")
            .setPassword("")
            .setDefaultDatabase(database)
            .build();
    }

    // Each test isolates itself in its own database rather than paying for a
    // fresh container.
    static void createDatabase(GenericContainer<?> container, String database) throws Exception {
        try (Client bootstrap = clientFor(container, "default")) {
            bootstrap.execute("CREATE DATABASE IF NOT EXISTS " + database).get();
        }
    }

    // Applies infrastructure/clickhouse/ddl/001_mvp_tables.sql — the same file the
    // production runner applies, so there is no test-only copy of the schema.
    //
    // The HTTP interface takes one statement per request, so line comments are
    // stripped (a ';' inside a comment would split a statement) and the remainder
    // is executed statement by statement, exactly as apply-ddl.sh does it.
    static void applyDdl(Client client) throws Exception {
        String sql = Files.readString(repoPath("infrastructure", "clickhouse", "ddl", "001_mvp_tables.sql"))
            .replaceAll("(?m)--.*$", "");
        for (String statement : sql.split(";")) {
            if (!statement.isBlank()) {
                client.execute(statement).get();
            }
        }
    }

    // Convenience: create the database and apply the DDL, returning a bound client.
    static Client freshDatabase(GenericContainer<?> container, String database) throws Exception {
        createDatabase(container, database);
        Client client = clientFor(container, database);
        applyDdl(client);
        return client;
    }

    static Set<String> tableNames(Client client) {
        return client.queryAll("SHOW TABLES").stream()
            .map(record -> record.getString("name"))
            .collect(Collectors.toSet());
    }
}
```

Then in `DdlMigrationTest`, delete the private `repoPath`, `clientFor`, `createDatabase`, `applyDdl` and `tableNames` methods and the inline container construction, replacing them with calls to `ClickHouseTestSupport`. The container field becomes:

```java
    @Container
    private static final GenericContainer<?> CLICKHOUSE = ClickHouseTestSupport.newContainer();
```

and the three test bodies call `ClickHouseTestSupport.createDatabase(CLICKHOUSE, "...")`, `ClickHouseTestSupport.clientFor(CLICKHOUSE, "...")`, `ClickHouseTestSupport.applyDdl(client)`, `ClickHouseTestSupport.tableNames(client)` and `ClickHouseTestSupport.repoPath(...)`.

- [ ] **Step 2: Confirm the refactor did not break anything**

Run: `./mvnw test -pl modules/adapter-clickhouse -Dtest=DdlMigrationTest`
Expected: PASS, 3 tests, same as before the extraction.

- [ ] **Step 3: Write the failing inserter test**

`modules/adapter-clickhouse/src/test/java/io/netsecml/platform/adapter/clickhouse/writer/ClientV2InserterTest.java`:

```java
package io.netsecml.platform.adapter.clickhouse.writer;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.query.GenericRecord;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netsecml.platform.adapter.clickhouse.ClickHouseTestSupport;
import io.netsecml.platform.adapter.clickhouse.mapper.FeatureVectorRowMapper;
import io.netsecml.platform.adapter.clickhouse.mapper.InvalidEventRowMapper;
import io.netsecml.platform.domain.event.ReasonCode;
import io.netsecml.platform.domain.event.RejectedEvent;
import io.netsecml.platform.domain.event.SensorId;
import io.netsecml.platform.domain.feature.FeatureVector;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import java.time.Instant;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

// The real HTTP path against a real server.
//
// This is also where the row records' @JsonProperty names meet the actual DDL:
// a mismatched column name fails here, on a two-row insert with a readable error,
// rather than deep inside the end-to-end test.
@Testcontainers(disabledWithoutDocker = true)
class ClientV2InserterTest {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Container
    private static final GenericContainer<?> CLICKHOUSE = ClickHouseTestSupport.newContainer();

    private ClickHouseConfig configFor(String database) {
        return ClickHouseConfig.of(CLICKHOUSE.getHost(), CLICKHOUSE.getMappedPort(8123),
            database, "default", "");
    }

    private FeatureVector vector(String eventId, float first, Instant producedAt) {
        float[] values = new float[20];
        values[0] = first;
        return new FeatureVector(eventId, Instant.parse("2026-08-27T10:03:11.250Z"),
            new SensorId("sensor-eu-1"), "conn-feature-v1", "f".repeat(64), values, 0, producedAt);
    }

    @Test
    void insertsFeatureVectorRowsWithAllTwentyValuesIntact() throws Exception {
        String database = "inserter_features";
        FeatureVectorRowMapper mapper = new FeatureVectorRowMapper();

        try (Client query = ClickHouseTestSupport.freshDatabase(CLICKHOUSE, database);
             ClickHouseInserter inserter = new ClientV2Inserter(configFor(database))) {

            List<String> lines = List.of(
                MAPPER.writeValueAsString(mapper.toRow(vector("sensor-eu-1:a", 1.5f, Instant.parse("2026-08-27T10:03:11.402Z")))),
                MAPPER.writeValueAsString(mapper.toRow(vector("sensor-eu-1:b", 2.5f, Instant.parse("2026-08-27T10:03:11.403Z")))));
            inserter.insert("feature_vectors", lines);

            List<GenericRecord> rows = query.queryAll(
                "SELECT event_id, sensor, schema_hash, length(`values`) AS n, `values`[1] AS first "
                    + "FROM feature_vectors ORDER BY event_id");

            assertEquals(2, rows.size());
            assertEquals("sensor-eu-1:a", rows.get(0).getString("event_id"));
            assertEquals("sensor-eu-1", rows.get(0).getString("sensor"));
            assertEquals("f".repeat(64), rows.get(0).getString("schema_hash"));
            assertEquals(20, rows.get(0).getInteger("n"), "all 20 feature values must survive the insert");
            assertEquals(1.5f, rows.get(0).getFloat("first"), 0.0001f);
        }
    }

    @Test
    void insertsInvalidEventRows() throws Exception {
        String database = "inserter_invalid";
        InvalidEventRowMapper mapper = new InvalidEventRowMapper();

        try (Client query = ClickHouseTestSupport.freshDatabase(CLICKHOUSE, database);
             ClickHouseInserter inserter = new ClientV2Inserter(configFor(database))) {

            RejectedEvent event = new RejectedEvent("sensor-eu-1:Cabc", "a".repeat(64),
                ReasonCode.INVALID_PORT, "port 70000 out of range", Instant.parse("2026-08-27T10:03:11.250Z"));
            inserter.insert("invalid_events", List.of(MAPPER.writeValueAsString(mapper.toRow(event))));

            List<GenericRecord> rows = query.queryAll(
                "SELECT event_id, stage, reason_code, detail, source_version, raw_payload_hash FROM invalid_events");

            assertEquals(1, rows.size());
            assertEquals("sensor-eu-1:Cabc", rows.get(0).getString("event_id"));
            assertEquals("MAP", rows.get(0).getString("stage"));
            assertEquals("INVALID_PORT", rows.get(0).getString("reason_code"));
            assertEquals("zeek-conn-source-v1", rows.get(0).getString("source_version"));
        }
    }

    // Server-side DEFAULT columns must be populated even though the client never
    // sends them — that is the whole reason they are omitted from the row records.
    @Test
    void serverSetsArchivedAtEvenThoughTheClientNeverSendsIt() throws Exception {
        String database = "inserter_defaults";

        try (Client query = ClickHouseTestSupport.freshDatabase(CLICKHOUSE, database);
             ClickHouseInserter inserter = new ClientV2Inserter(configFor(database))) {

            inserter.insert("feature_vectors", List.of(MAPPER.writeValueAsString(
                new FeatureVectorRowMapper().toRow(vector("sensor-eu-1:a", 1f, Instant.parse("2026-08-27T10:03:11.402Z"))))));

            List<GenericRecord> rows = query.queryAll(
                "SELECT toUnixTimestamp64Milli(archived_at) AS archived FROM feature_vectors");

            assertEquals(1, rows.size());
            assertTrue(rows.get(0).getLong("archived") > 0L, "archived_at must be set by the server DEFAULT");
        }
    }

    // A failed insert must surface as an exception, because the sink turns that
    // into a failed checkpoint and therefore into unadvanced Kafka offsets.
    @Test
    void throwsWhenTheServerRejectsTheBatch() throws Exception {
        String database = "inserter_rejects";
        ClickHouseTestSupport.freshDatabase(CLICKHOUSE, database).close();

        try (ClickHouseInserter inserter = new ClientV2Inserter(configFor(database))) {
            assertThrows(Exception.class,
                () -> inserter.insert("feature_vectors", List.of("{\"no_such_column\": 1}")),
                "an unknown column must not be silently discarded");
        }
    }
}
```

- [ ] **Step 4: Run the test to verify it fails**

Run: `./mvnw test -pl modules/adapter-clickhouse -Dtest=ClientV2InserterTest`
Expected: compilation failure — `ClickHouseConfig`, `ClickHouseInserter` and `ClientV2Inserter` do not exist.

- [ ] **Step 5: Write the config and the seam**

`modules/adapter-clickhouse/src/main/java/io/netsecml/platform/adapter/clickhouse/writer/ClickHouseConfig.java`:

```java
package io.netsecml.platform.adapter.clickhouse.writer;

import java.io.Serializable;
import java.util.Objects;

// Connection settings for the archive sink.
//
// Serializable because the sink carries it from the job graph out to every
// subtask; the live client is built on the far side, in createWriter.
public record ClickHouseConfig(String endpoint, String database, String username, String password)
    implements Serializable {

    public ClickHouseConfig {
        Objects.requireNonNull(endpoint, "endpoint must not be null");
        Objects.requireNonNull(database, "database must not be null");
        Objects.requireNonNull(username, "username must not be null");

        // An empty password is normal for a local ClickHouse; null is not.
        password = password == null ? "" : password;
    }

    // Convenience for the host/port form the environment variables carry.
    public static ClickHouseConfig of(String host, int port, String database, String username, String password) {
        return new ClickHouseConfig("http://" + host + ":" + port, database, username, password);
    }
}
```

`modules/adapter-clickhouse/src/main/java/io/netsecml/platform/adapter/clickhouse/writer/ClickHouseInserter.java`:

```java
package io.netsecml.platform.adapter.clickhouse.writer;

import java.util.List;

// The narrow seam between "talk to ClickHouse" and "decide when to talk to it".
//
// One method, deliberately: it is what lets ClickHouseSinkWriter's retry and
// flush logic be tested against a fake with no server and no Docker.
public interface ClickHouseInserter extends AutoCloseable {

    // Inserts the given JSONEachRow lines into the table, blocking until the
    // server acknowledges. Throws on any failure — the caller owns retry policy.
    void insert(String table, List<String> jsonLines) throws Exception;

    @Override
    void close();
}
```

`modules/adapter-clickhouse/src/main/java/io/netsecml/platform/adapter/clickhouse/writer/ClickHouseInserterFactory.java`:

```java
package io.netsecml.platform.adapter.clickhouse.writer;

import java.io.Serializable;

// Builds an inserter on the subtask that will use it.
//
// A live HTTP client cannot travel through Flink's job graph, so the sink holds
// this Serializable factory plus the config and constructs the real inserter
// inside createWriter. Tests substitute a factory returning a fake.
@FunctionalInterface
public interface ClickHouseInserterFactory extends Serializable {
    ClickHouseInserter create(ClickHouseConfig config);
}
```

- [ ] **Step 6: Write `ClientV2Inserter`**

`modules/adapter-clickhouse/src/main/java/io/netsecml/platform/adapter/clickhouse/writer/ClientV2Inserter.java`:

```java
package io.netsecml.platform.adapter.clickhouse.writer;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.insert.InsertResponse;
import com.clickhouse.data.ClickHouseFormat;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

// The real insert path: POST a JSONEachRow batch and block until the server
// acknowledges it.
public final class ClientV2Inserter implements ClickHouseInserter {
    private final Client client;

    public ClientV2Inserter(ClickHouseConfig config) {
        this.client = new Client.Builder()
            .addEndpoint(config.endpoint())
            .setUsername(config.username())
            .setPassword(config.password())
            .setDefaultDatabase(config.database())
            // Compressing the request is worthwhile: batches are up to 4 MiB of
            // highly repetitive JSON.
            .compressClientRequest(true)
            .build();
    }

    @Override
    public void insert(String table, List<String> jsonLines) throws Exception {
        // JSONEachRow is one JSON object per line. The trailing newline keeps the
        // last record well-formed.
        byte[] body = (String.join("\n", jsonLines) + "\n").getBytes(StandardCharsets.UTF_8);

        // get() is what makes this synchronous. The future completing normally IS
        // the server's acknowledgement; a server-side error arrives as an
        // ExecutionException and propagates to the caller's retry loop.
        try (InputStream in = new ByteArrayInputStream(body);
             InsertResponse response = client.insert(table, in, ClickHouseFormat.JSONEachRow).get()) {
            // The response is closed to release the connection back to the pool.
            // Row counts are deliberately not asserted here: getWrittenRows()
            // depends on server summary settings, and the integration test verifies
            // persistence by querying the table instead.
        }
    }

    @Override
    public void close() {
        client.close();
    }
}
```

- [ ] **Step 7: Run the test to verify it passes**

Run: `./mvnw test -pl modules/adapter-clickhouse -Dtest=ClientV2InserterTest`
Expected: PASS, 4 tests.

If `client.insert(...)` does not compile, check the pinned `client-v2:0.9.0` signature: `CompletableFuture<InsertResponse> insert(String tableName, InputStream data, ClickHouseFormat format)`. `ClickHouseFormat` lives in `com.clickhouse.data`, which arrives transitively via `clickhouse-data`.

- [ ] **Step 8: Commit**

```bash
git add modules/adapter-clickhouse/src/main/java/io/netsecml/platform/adapter/clickhouse/writer/ \
        modules/adapter-clickhouse/src/test/java/io/netsecml/platform/adapter/clickhouse/
git commit -m "feat(adapter-clickhouse): add ClickHouseConfig and the client-v2 JSONEachRow inserter"
```

---
### Task 10: `ClickHouseBatchSink` — flush triggers, bounded retry, metrics

The load-bearing task. Everything about "ClickHouse must never stop feature production" reduces to one decision here: **a failed insert throws.**

Throwing fails the checkpoint. Kafka offsets are part of that checkpoint, so they do not advance. The job restarts from the last successful checkpoint and replays; `ReplacingMergeTree` absorbs the duplicates. That is how a Flink job satisfies the Roadmap's "commit consumer progress only after successful insert acknowledgement" without a manual `commitSync`.

The alternative — retrying forever inside `write()` — would convert a ClickHouse outage into silent backpressure. Throwing converts it into a restart, the restart strategy backs off, and Kafka lag grows visibly. Lag, not RAM.

**Files:**
- Create: `modules/adapter-clickhouse/src/main/java/io/netsecml/platform/adapter/clickhouse/writer/SinkMetrics.java`
- Create: `modules/adapter-clickhouse/src/main/java/io/netsecml/platform/adapter/clickhouse/writer/FlinkSinkMetrics.java`
- Create: `modules/adapter-clickhouse/src/main/java/io/netsecml/platform/adapter/clickhouse/writer/ClickHouseSinkWriter.java`
- Create: `modules/adapter-clickhouse/src/main/java/io/netsecml/platform/adapter/clickhouse/writer/ClickHouseBatchSink.java`
- Test: `modules/adapter-clickhouse/src/test/java/io/netsecml/platform/adapter/clickhouse/writer/ClickHouseSinkWriterTest.java`

**Interfaces:**
- Consumes: `BatchBuffer` (Task 8); `ClickHouseInserter`, `ClickHouseInserterFactory`, `ClickHouseConfig`, `ClientV2Inserter` (Task 9).
- Produces:
  - `interface SinkMetrics` — `void recordBatch(int rows, long bytes, long latencyMillis)`, `void recordFailure()`.
  - `FlinkSinkMetrics(MetricGroup group, String table) implements SinkMetrics`.
  - `ClickHouseSinkWriter<T> implements SinkWriter<T>`, package-private constructor `(String table, ClickHouseInserter inserter, BatchBuffer buffer, ObjectMapper objectMapper, int maxRetries, long initialBackoffMillis, long flushIntervalMillis, ProcessingTimeService timeService, SinkMetrics metrics)`.
  - `ClickHouseBatchSink<T> implements Sink<T>` with `ClickHouseBatchSink(String table, ClickHouseConfig config)` and a full constructor `(String table, ClickHouseConfig config, ClickHouseInserterFactory inserterFactory, int maxRows, long maxBytes, long flushIntervalMillis, int maxRetries, long initialBackoffMillis)`.
  - Public defaults on `ClickHouseBatchSink`: `DEFAULT_MAX_ROWS = 5_000`, `DEFAULT_MAX_BYTES = 4L * 1024 * 1024`, `DEFAULT_FLUSH_INTERVAL_MILLIS = 1_000L`, `DEFAULT_MAX_RETRIES = 3`, `DEFAULT_INITIAL_BACKOFF_MILLIS = 200L`.

- [ ] **Step 1: Write the failing test**

`modules/adapter-clickhouse/src/test/java/io/netsecml/platform/adapter/clickhouse/writer/ClickHouseSinkWriterTest.java`:

```java
package io.netsecml.platform.adapter.clickhouse.writer;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.netsecml.platform.adapter.clickhouse.batch.BatchBuffer;
import org.apache.flink.api.common.operators.ProcessingTimeService;
import org.junit.jupiter.api.Test;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import static org.junit.jupiter.api.Assertions.*;

// The retry and flush logic, exercised with no server and no Docker. That is the
// entire point of the ClickHouseInserter seam.
class ClickHouseSinkWriterTest {

    // Records every batch it is handed, and fails on demand.
    private static final class FakeInserter implements ClickHouseInserter {
        final List<List<String>> attempts = new ArrayList<>();
        int failuresRemaining;
        boolean closed;

        @Override
        public void insert(String table, List<String> jsonLines) throws Exception {
            attempts.add(List.copyOf(jsonLines));
            if (failuresRemaining > 0) {
                failuresRemaining--;
                throw new IOException("simulated ClickHouse failure");
            }
        }

        @Override
        public void close() {
            closed = true;
        }
    }

    // Captures registered timers so a test can fire them deliberately rather than
    // sleeping on wall-clock time.
    private static final class FakeProcessingTimeService implements ProcessingTimeService {
        final List<ProcessingTimeCallback> pending = new ArrayList<>();
        long now;

        @Override
        public long getCurrentProcessingTime() {
            return now;
        }

        @Override
        public ScheduledFuture<?> registerTimer(long time, ProcessingTimeCallback target) {
            pending.add(target);
            return null;
        }

        void fireOldestTimer() throws Exception {
            pending.remove(0).onProcessingTime(now);
        }
    }

    private static final class RecordingMetrics implements SinkMetrics {
        int batches;
        int failures;
        int lastRows;

        @Override
        public void recordBatch(int rows, long bytes, long latencyMillis) {
            batches++;
            lastRows = rows;
        }

        @Override
        public void recordFailure() {
            failures++;
        }
    }

    private final FakeInserter inserter = new FakeInserter();
    private final FakeProcessingTimeService timeService = new FakeProcessingTimeService();
    private final RecordingMetrics metrics = new RecordingMetrics();

    // Backoff is 1 ms in tests: the production 200/400/800 ms would add 1.4 s to
    // every retry assertion for no extra coverage.
    private ClickHouseSinkWriter<Map<String, Object>> writer(int maxRows) {
        return new ClickHouseSinkWriter<>("feature_vectors", inserter, new BatchBuffer(maxRows, 1_000_000),
            new ObjectMapper(), 3, 1L, 1_000L, timeService, metrics);
    }

    private Map<String, Object> row(String id) {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("event_id", id);
        return row;
    }

    // The checkpoint-barrier path: flush() drains whatever is buffered.
    @Test
    void flushSendsBufferedRows() throws Exception {
        ClickHouseSinkWriter<Map<String, Object>> writer = writer(1000);
        writer.write(row("a"), null);
        writer.write(row("b"), null);

        assertEquals(0, inserter.attempts.size(), "nothing is sent before a trigger fires");

        writer.flush(false);

        assertEquals(1, inserter.attempts.size());
        assertEquals(2, inserter.attempts.get(0).size());
        assertTrue(inserter.attempts.get(0).get(0).contains("\"event_id\":\"a\""));
        assertEquals(2, metrics.lastRows);
    }

    // The row-count trigger fires inside write(), without waiting for a checkpoint.
    @Test
    void rowLimitTriggersAnImmediateFlush() throws Exception {
        ClickHouseSinkWriter<Map<String, Object>> writer = writer(2);
        writer.write(row("a"), null);
        assertEquals(0, inserter.attempts.size());

        writer.write(row("b"), null);

        assertEquals(1, inserter.attempts.size(), "the second row reaches the 2-row limit");
        assertEquals(2, inserter.attempts.get(0).size());
    }

    // The 1-second trigger, driven deterministically.
    @Test
    void processingTimeTimerFlushesAndReschedules() throws Exception {
        ClickHouseSinkWriter<Map<String, Object>> writer = writer(1000);
        writer.write(row("a"), null);

        assertEquals(1, timeService.pending.size(), "a flush timer is registered at construction");
        timeService.fireOldestTimer();

        assertEquals(1, inserter.attempts.size());
        assertEquals(1, timeService.pending.size(), "the timer re-arms itself after firing");
    }

    // Transient failures must not lose the batch: the same content is retried.
    @Test
    void retriesTheSameBatchThenSucceeds() throws Exception {
        inserter.failuresRemaining = 2;
        ClickHouseSinkWriter<Map<String, Object>> writer = writer(1000);
        writer.write(row("a"), null);

        writer.flush(false);

        assertEquals(3, inserter.attempts.size(), "two failures then one success");
        assertEquals(inserter.attempts.get(0), inserter.attempts.get(2), "the retried batch is unchanged");
        assertEquals(2, metrics.failures);
        assertEquals(1, metrics.batches, "only the successful attempt counts as a batch");
    }

    // The load-bearing behaviour. Exhausted retries throw, which fails the
    // checkpoint, which keeps the Kafka offsets where they are.
    @Test
    void throwsAfterExhaustingRetries() throws Exception {
        inserter.failuresRemaining = Integer.MAX_VALUE;
        ClickHouseSinkWriter<Map<String, Object>> writer = writer(1000);
        writer.write(row("a"), null);

        IOException thrown = assertThrows(IOException.class, () -> writer.flush(false));

        assertTrue(thrown.getMessage().contains("feature_vectors"), "the error names the table");
        assertEquals(4, inserter.attempts.size(), "one initial attempt plus three retries");
        assertEquals(4, metrics.failures);
        assertEquals(0, metrics.batches, "a batch that never landed is not counted as written");
    }

    // A batch is never dropped silently — the failure has to reach the caller.
    @Test
    void neverSwallowsAFailedBatch() throws Exception {
        inserter.failuresRemaining = Integer.MAX_VALUE;
        ClickHouseSinkWriter<Map<String, Object>> writer = writer(1000);
        writer.write(row("a"), null);

        assertThrows(IOException.class, () -> writer.flush(false));
        for (List<String> attempt : inserter.attempts) {
            assertEquals(1, attempt.size(), "every attempt carried the full batch");
        }
    }

    @Test
    void flushOnAnEmptyBufferDoesNothing() throws Exception {
        writer(1000).flush(false);

        assertEquals(0, inserter.attempts.size());
    }

    @Test
    void closeReleasesTheInserter() throws Exception {
        ClickHouseSinkWriter<Map<String, Object>> writer = writer(1000);

        writer.close();

        assertTrue(inserter.closed);
    }
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `./mvnw test -pl modules/adapter-clickhouse -Dtest=ClickHouseSinkWriterTest`
Expected: compilation failure — `SinkMetrics` and `ClickHouseSinkWriter` do not exist.

- [ ] **Step 3: Write the metrics seam**

`modules/adapter-clickhouse/src/main/java/io/netsecml/platform/adapter/clickhouse/writer/SinkMetrics.java`:

```java
package io.netsecml.platform.adapter.clickhouse.writer;

// A narrow seam over Flink's metric group.
//
// It exists so the writer's retry and flush logic can be unit-tested without a
// Flink runtime: constructing a real SinkWriterMetricGroup outside a running task
// means reaching into Flink internals, which breaks on every minor upgrade.
public interface SinkMetrics {

    // One batch landed successfully.
    void recordBatch(int rows, long bytes, long latencyMillis);

    // One insert attempt failed. Counted per attempt, not per batch, so the
    // counter distinguishes a flaky server from a dead one.
    void recordFailure();
}
```

`modules/adapter-clickhouse/src/main/java/io/netsecml/platform/adapter/clickhouse/writer/FlinkSinkMetrics.java`:

```java
package io.netsecml.platform.adapter.clickhouse.writer;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;

// Publishes the archive sink's four numbers through Flink's metric system.
//
// Gauges of the most recent batch rather than histograms: Flink's Histogram needs
// either the dropwizard bridge or an internal implementation, and neither is
// worth a dependency for four values. Day 12 owns real observability.
public final class FlinkSinkMetrics implements SinkMetrics {
    private final Counter insertFailures;

    // Read by the gauges from the metric reporter's thread.
    private volatile int lastBatchRows;
    private volatile long lastBatchBytes;
    private volatile long lastFlushLatencyMillis;

    public FlinkSinkMetrics(MetricGroup group, String table) {
        // Scoped per table, so feature_vectors and invalid_events report separately.
        MetricGroup scoped = group.addGroup("archive").addGroup("table", table);

        this.insertFailures = scoped.counter("insert.failures");
        scoped.gauge("batch.rows", () -> lastBatchRows);
        scoped.gauge("batch.bytes", () -> lastBatchBytes);
        scoped.gauge("flush.latency.ms", () -> lastFlushLatencyMillis);
    }

    @Override
    public void recordBatch(int rows, long bytes, long latencyMillis) {
        lastBatchRows = rows;
        lastBatchBytes = bytes;
        lastFlushLatencyMillis = latencyMillis;
    }

    @Override
    public void recordFailure() {
        insertFailures.inc();
    }
}
```

- [ ] **Step 4: Write `ClickHouseSinkWriter`**

`modules/adapter-clickhouse/src/main/java/io/netsecml/platform/adapter/clickhouse/writer/ClickHouseSinkWriter.java`:

```java
package io.netsecml.platform.adapter.clickhouse.writer;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.netsecml.platform.adapter.clickhouse.batch.BatchBuffer;
import org.apache.flink.api.common.operators.ProcessingTimeService;
import org.apache.flink.api.connector.sink2.SinkWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

// Buffers rows and flushes them to ClickHouse on four triggers: row count, byte
// size, a processing-time timer, and the checkpoint barrier.
//
// The writer holds no state across flushes — everything buffered is drained by
// flush() — so this sink needs no committer and no snapshotState.
final class ClickHouseSinkWriter<T> implements SinkWriter<T> {
    private final String table;
    private final ClickHouseInserter inserter;
    private final BatchBuffer buffer;
    private final ObjectMapper objectMapper;
    private final int maxRetries;
    private final long initialBackoffMillis;
    private final long flushIntervalMillis;
    private final ProcessingTimeService timeService;
    private final SinkMetrics metrics;

    ClickHouseSinkWriter(String table, ClickHouseInserter inserter, BatchBuffer buffer,
                         ObjectMapper objectMapper, int maxRetries, long initialBackoffMillis,
                         long flushIntervalMillis, ProcessingTimeService timeService, SinkMetrics metrics) {
        this.table = table;
        this.inserter = inserter;
        this.buffer = buffer;
        this.objectMapper = objectMapper;
        this.maxRetries = maxRetries;
        this.initialBackoffMillis = initialBackoffMillis;
        this.flushIntervalMillis = flushIntervalMillis;
        this.timeService = timeService;
        this.metrics = metrics;

        // Arm the latency trigger immediately, so a trickle of rows still reaches
        // ClickHouse within a second instead of waiting for a full batch.
        registerFlushTimer();
    }

    @Override
    public void write(T element, Context context) throws IOException {
        // Rows are buffered as their final JSONEachRow text, so the retry loop
        // re-sends bytes rather than re-serializing objects.
        if (buffer.add(objectMapper.writeValueAsString(element))) {
            flushBuffer();
        }
    }

    // Called by Flink when the checkpoint barrier reaches this writer, and again
    // at end of input.
    //
    // This is the whole delivery contract: if the insert fails, this throws, the
    // checkpoint fails, and the Kafka offsets in that checkpoint never advance.
    // The job restarts and replays, and ReplacingMergeTree absorbs the duplicates.
    @Override
    public void flush(boolean endOfInput) throws IOException {
        flushBuffer();
    }

    @Override
    public void close() {
        inserter.close();
    }

    // Re-arms itself after every firing, so the latency trigger keeps running for
    // the life of the subtask.
    private void registerFlushTimer() {
        timeService.registerTimer(
            timeService.getCurrentProcessingTime() + flushIntervalMillis,
            timestamp -> {
                flushBuffer();
                registerFlushTimer();
            });
    }

    private void flushBuffer() throws IOException {
        if (buffer.isEmpty()) {
            return;
        }

        // Drain first: the batch is now owned by this call, and new rows arriving
        // during the retry loop accumulate separately.
        List<String> batch = buffer.drain();
        long batchBytes = 0;
        for (String line : batch) {
            batchBytes += line.getBytes(StandardCharsets.UTF_8).length + 1;
        }

        long startNanos = System.nanoTime();
        Exception lastFailure = null;

        // One initial attempt plus maxRetries retries, backing off exponentially.
        for (int attempt = 0; attempt <= maxRetries; attempt++) {
            try {
                inserter.insert(table, batch);
                metrics.recordBatch(batch.size(), batchBytes, (System.nanoTime() - startNanos) / 1_000_000L);
                return;
            } catch (Exception e) {
                lastFailure = e;
                metrics.recordFailure();

                if (attempt < maxRetries) {
                    sleepBackoff(initialBackoffMillis << attempt);
                }
            }
        }

        // Retries are bounded on purpose. Retrying forever here would turn a
        // ClickHouse outage into silent backpressure on the archive job; throwing
        // turns it into a job restart, the restart strategy backs off, and the
        // failure becomes visible Kafka lag instead of growing heap.
        throw new IOException("ClickHouse insert into " + table + " failed after "
            + (maxRetries + 1) + " attempts", lastFailure);
    }

    private void sleepBackoff(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            // A cancelling task must not be delayed by our backoff.
            Thread.currentThread().interrupt();
        }
    }
}
```

- [ ] **Step 5: Write `ClickHouseBatchSink`**

`modules/adapter-clickhouse/src/main/java/io/netsecml/platform/adapter/clickhouse/writer/ClickHouseBatchSink.java`:

```java
package io.netsecml.platform.adapter.clickhouse.writer;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.netsecml.platform.adapter.clickhouse.batch.BatchBuffer;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;

// Flink Sink V2 that batches rows of type T into one ClickHouse table.
//
// Transport-agnostic on purpose: this class knows nothing about Kafka. T is any
// Jackson-serializable row whose property names are the table's column names.
public final class ClickHouseBatchSink<T> implements Sink<T> {

    // Initial values from FINAL_ARCHITECTURE.md Step 8, to be benchmarked rather
    // than treated as settled. They are constructor parameters for that reason.
    public static final int DEFAULT_MAX_ROWS = 5_000;
    public static final long DEFAULT_MAX_BYTES = 4L * 1024 * 1024;
    public static final long DEFAULT_FLUSH_INTERVAL_MILLIS = 1_000L;
    public static final int DEFAULT_MAX_RETRIES = 3;
    public static final long DEFAULT_INITIAL_BACKOFF_MILLIS = 200L;

    private final String table;
    private final ClickHouseConfig config;
    private final ClickHouseInserterFactory inserterFactory;
    private final int maxRows;
    private final long maxBytes;
    private final long flushIntervalMillis;
    private final int maxRetries;
    private final long initialBackoffMillis;

    public ClickHouseBatchSink(String table, ClickHouseConfig config) {
        this(table, config, ClientV2Inserter::new, DEFAULT_MAX_ROWS, DEFAULT_MAX_BYTES,
            DEFAULT_FLUSH_INTERVAL_MILLIS, DEFAULT_MAX_RETRIES, DEFAULT_INITIAL_BACKOFF_MILLIS);
    }

    public ClickHouseBatchSink(String table, ClickHouseConfig config, ClickHouseInserterFactory inserterFactory,
                               int maxRows, long maxBytes, long flushIntervalMillis,
                               int maxRetries, long initialBackoffMillis) {
        this.table = table;
        this.config = config;
        this.inserterFactory = inserterFactory;
        this.maxRows = maxRows;
        this.maxBytes = maxBytes;
        this.flushIntervalMillis = flushIntervalMillis;
        this.maxRetries = maxRetries;
        this.initialBackoffMillis = initialBackoffMillis;
    }

    @Override
    public SinkWriter<T> createWriter(WriterInitContext context) {
        // The live HTTP client is built here, on the subtask that will use it —
        // only the Serializable config and factory crossed the job graph.
        return new ClickHouseSinkWriter<>(
            table,
            inserterFactory.create(config),
            new BatchBuffer(maxRows, maxBytes),
            new ObjectMapper(),
            maxRetries,
            initialBackoffMillis,
            flushIntervalMillis,
            context.getProcessingTimeService(),
            new FlinkSinkMetrics(context.metricGroup(), table));
    }
}
```

- [ ] **Step 6: Run the test to verify it passes**

Run: `./mvnw test -pl modules/adapter-clickhouse -Dtest=ClickHouseSinkWriterTest`
Expected: PASS, 8 tests.

- [ ] **Step 7: Run the whole module**

Run: `./mvnw test -pl modules/adapter-clickhouse`
Expected: PASS — `BatchBufferTest`, both mapper tests, `ClickHouseSinkWriterTest`, plus `DdlMigrationTest` and `ClientV2InserterTest` when Docker is available.

- [ ] **Step 8: Commit**

```bash
git add modules/adapter-clickhouse/src/main/java/io/netsecml/platform/adapter/clickhouse/writer/ \
        modules/adapter-clickhouse/src/test/java/io/netsecml/platform/adapter/clickhouse/writer/
git commit -m "feat(adapter-clickhouse): add Sink V2 batch sink with bounded retry and checkpoint flush"
```

---
### Task 11: `ArchiveJob` — wire both chains and prove one record lands

The composition root. Two independent `KafkaSource → map → ClickHouseBatchSink` chains in one job: the Flink job graph is the router, which is why no `ArchiveRouter` class exists anywhere in this plan.

`adapter-clickhouse` never sees a Kafka type and `adapter-kafka` never sees a ClickHouse type. They meet here, in the only module allowed to know about both.

**Files:**
- Create: `modules/adapter-flink/src/main/java/io/netsecml/platform/adapter/flink/source/RawBytesDeserializationSchema.java`
- Create: `modules/bootstrap-archive-job/src/main/java/io/netsecml/platform/bootstrap/archive/FeatureVectorRowMapFunction.java`
- Create: `modules/bootstrap-archive-job/src/main/java/io/netsecml/platform/bootstrap/archive/InvalidEventRowMapFunction.java`
- Create: `modules/bootstrap-archive-job/src/main/java/io/netsecml/platform/bootstrap/archive/ArchiveJob.java`
- Create: `modules/bootstrap-archive-job/src/test/java/io/netsecml/platform/bootstrap/archive/ArchiveJobE2ETest.java`
- Modify: `modules/adapter-clickhouse/pom.xml` (publish a test-jar)
- Modify: `modules/adapter-clickhouse/src/test/java/io/netsecml/platform/adapter/clickhouse/ClickHouseTestSupport.java` (make public)
- Modify: `modules/bootstrap-archive-job/pom.xml`
- Modify: `modules/bootstrap-online-job/src/main/java/io/netsecml/platform/bootstrap/online/OnlineFeatureJob.java` (use the shared schema)
- Modify: `.env.example`
- Delete: `modules/bootstrap-archive-job/src/test/java/io/netsecml/platform/bootstrap/archive/.gitkeep`

**Interfaces:**
- Consumes: `FeatureVectorDeserializer`, `RejectedEventDeserializer` (Task 5); `FeatureVectorRowMapper`, `InvalidEventRowMapper` (Task 7); `ClickHouseConfig` (Task 9); `ClickHouseBatchSink` (Task 10).
- Produces:
  - `RawBytesDeserializationSchema implements DeserializationSchema<byte[]>`.
  - `ArchiveJob.build(StreamExecutionEnvironment env, String bootstrapServers, String featureVectorTopic, String dlqTopic, ClickHouseConfig clickHouse)`.
  - `ArchiveJob.main(String[] args)`.
  - `ArchiveJob.CONSUMER_GROUP` — the constant `"conn-archive-job"`.
  - `ClickHouseTestSupport` becomes `public` with `public static` methods.

- [ ] **Step 1: Extract `RawBytesDeserializationSchema` and use it in both jobs**

`OnlineFeatureJob` holds this as a private anonymous class. The archive job needs the same nine lines, so it moves to `adapter-flink`, which both bootstraps can depend on. It does **not** go in `adapter-kafka`: that module has no Flink dependency today and giving it one to save nine lines would be a bad trade.

`modules/adapter-flink/src/main/java/io/netsecml/platform/adapter/flink/source/RawBytesDeserializationSchema.java`:

```java
package io.netsecml.platform.adapter.flink.source;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

// Pass-through deserializer: the Kafka source hands the pipeline raw bytes and
// each job decides how to interpret them. Shared by the online and archive jobs
// so the same nine lines do not exist in two composition roots.
public final class RawBytesDeserializationSchema implements DeserializationSchema<byte[]> {

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
}
```

Then in `OnlineFeatureJob`, delete the `private static final DeserializationSchema<byte[]> RAW_BYTES = ...` field and its now-unused imports, and change the source builder line to:

```java
            .setValueOnlyDeserializer(new RawBytesDeserializationSchema())
```

adding `import io.netsecml.platform.adapter.flink.source.RawBytesDeserializationSchema;`.

- [ ] **Step 2: Publish `adapter-clickhouse` test classes and make the support class public**

`bootstrap-archive-job`'s container tests need the same container, database and DDL plumbing. Rather than a second copy, `adapter-clickhouse` publishes a test-jar.

Add to `modules/adapter-clickhouse/pom.xml`, after `</dependencies>`:

```xml
  <build>
    <plugins>
      <!--
        Publishes this module's test classes so bootstrap-archive-job's container
        tests can reuse ClickHouseTestSupport instead of keeping a second copy of
        the container, database and DDL plumbing.
      -->
      <plugin>
        <groupId>org.apache.maven.plugins</groupId>
        <artifactId>maven-jar-plugin</artifactId>
        <version>3.4.1</version>
        <executions>
          <execution>
            <goals>
              <goal>test-jar</goal>
            </goals>
          </execution>
        </executions>
      </plugin>
    </plugins>
  </build>
```

Then change `ClickHouseTestSupport` from package-private to public: `public final class ClickHouseTestSupport`, and make `newContainer`, `repoPath`, `clientFor`, `createDatabase`, `applyDdl`, `freshDatabase`, `tableNames` and `HTTP_PORT` all `public static`.

One caveat to note in the class comment: `repoPath` resolves `../..`, which is correct from any module directory at `modules/<name>`, so it works unchanged from `bootstrap-archive-job`.

- [ ] **Step 3: Give `bootstrap-archive-job` its dependencies**

Replace the `<dependencies>` block of `modules/bootstrap-archive-job/pom.xml` entirely:

```xml
  <dependencies>
    <dependency>
      <groupId>io.netsecml.platform</groupId>
      <artifactId>domain</artifactId>
      <version>${project.version}</version>
    </dependency>

    <!-- Deserializes both internal topics into domain values. -->
    <dependency>
      <groupId>io.netsecml.platform</groupId>
      <artifactId>adapter-kafka</artifactId>
      <version>${project.version}</version>
    </dependency>

    <!-- Maps domain values to rows and owns the batch sink. -->
    <dependency>
      <groupId>io.netsecml.platform</groupId>
      <artifactId>adapter-clickhouse</artifactId>
      <version>${project.version}</version>
    </dependency>

    <!-- RawBytesDeserializationSchema, shared with the online job. -->
    <dependency>
      <groupId>io.netsecml.platform</groupId>
      <artifactId>adapter-flink</artifactId>
      <version>${project.version}</version>
    </dependency>

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
      <artifactId>flink-connector-base</artifactId>
    </dependency>
    <dependency>
      <groupId>org.apache.flink</groupId>
      <artifactId>flink-connector-kafka</artifactId>
    </dependency>

    <dependency>
      <groupId>org.junit.jupiter</groupId>
      <artifactId>junit-jupiter</artifactId>
      <scope>test</scope>
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

    <!-- ClickHouseTestSupport: container, database and DDL plumbing, shared
         rather than copied. -->
    <dependency>
      <groupId>io.netsecml.platform</groupId>
      <artifactId>adapter-clickhouse</artifactId>
      <version>${project.version}</version>
      <type>test-jar</type>
      <scope>test</scope>
    </dependency>
  </dependencies>
```

`ports`, `application` and `adapter-monitoring` are dropped from the Step-1 skeleton: this job uses none of them, and `adapter-monitoring` is still empty until Day 12.

- [ ] **Step 4: Write the failing end-to-end test**

Delete the placeholder first:

```bash
git rm modules/bootstrap-archive-job/src/test/java/io/netsecml/platform/bootstrap/archive/.gitkeep
```

`modules/bootstrap-archive-job/src/test/java/io/netsecml/platform/bootstrap/archive/ArchiveJobE2ETest.java`:

```java
package io.netsecml.platform.bootstrap.archive;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.query.GenericRecord;
import io.netsecml.platform.adapter.clickhouse.ClickHouseTestSupport;
import io.netsecml.platform.adapter.clickhouse.writer.ClickHouseConfig;
import io.netsecml.platform.adapter.kafka.sink.FeatureVectorSerializer;
import io.netsecml.platform.adapter.kafka.sink.RejectedRecordPayload;
import io.netsecml.platform.adapter.kafka.sink.RejectedRecordSerializer;
import io.netsecml.platform.domain.event.SensorId;
import io.netsecml.platform.domain.feature.ConnFeatureSchemaV1;
import io.netsecml.platform.domain.feature.FeatureVector;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import static org.junit.jupiter.api.Assertions.*;

// Roadmap Day 6, test 1: a feature vector published to Kafka becomes a queryable
// ClickHouse row, and a rejected record becomes a queryable invalid_events row.
@Testcontainers(disabledWithoutDocker = true)
class ArchiveJobE2ETest {
    private static final String FEATURE_TOPIC = "netsec.conn.feature-vector.v1";
    private static final String DLQ_TOPIC = "netsec.conn.dlq.v1";
    private static final String DATABASE = "archive_e2e";

    @Container
    private static final ConfluentKafkaContainer KAFKA =
        new ConfluentKafkaContainer("confluentinc/cp-kafka:7.6.1");

    @Container
    private static final GenericContainer<?> CLICKHOUSE = ClickHouseTestSupport.newContainer();

    // 20 values so the row matches what a real vector carries; index 0 is
    // distinctive so the assertion proves the payload, not just the row count.
    private FeatureVector vector() {
        float[] values = new float[20];
        values[0] = 42.5f;
        values[19] = 7f;
        return new FeatureVector("sensor-eu-1:Cabc123XYZ", Instant.parse("2026-08-27T10:03:11.250Z"),
            new SensorId("sensor-eu-1"), ConnFeatureSchemaV1.SCHEMA.id(), ConnFeatureSchemaV1.CONTENT_HASH,
            values, 0, Instant.parse("2026-08-27T10:03:11.402Z"));
    }

    private void produce(String topic, byte[] payload) throws Exception {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(props)) {
            producer.send(new ProducerRecord<>(topic, payload)).get();
        }
    }

    // Polls until the query returns at least one row or the deadline passes. The
    // archive path is asynchronous by design, so a fixed sleep would be either
    // slow or flaky.
    private List<GenericRecord> awaitRows(Client client, String sql) throws Exception {
        long deadline = System.currentTimeMillis() + 90_000;
        List<GenericRecord> rows = List.of();
        while (rows.isEmpty() && System.currentTimeMillis() < deadline) {
            rows = client.queryAll(sql);
            if (rows.isEmpty()) {
                Thread.sleep(500);
            }
        }
        return rows;
    }

    @Test
    void featureVectorAndRejectedRecordBothReachClickHouse() throws Exception {
        // Publish one record to each internal topic before the job starts, so the
        // source reads them from the beginning of the log.
        produce(FEATURE_TOPIC, new FeatureVectorSerializer().serialize(FEATURE_TOPIC, vector()));
        produce(DLQ_TOPIC, new RejectedRecordSerializer().serialize(DLQ_TOPIC,
            new RejectedRecordPayload("{ broken".getBytes(StandardCharsets.UTF_8), "",
                "PARSE", "MALFORMED_JSON", "unexpected end of input",
                Instant.parse("2026-08-27T10:03:11.250Z"))));

        try (Client query = ClickHouseTestSupport.freshDatabase(CLICKHOUSE, DATABASE)) {
            ClickHouseConfig config = ClickHouseConfig.of(CLICKHOUSE.getHost(),
                CLICKHOUSE.getMappedPort(ClickHouseTestSupport.HTTP_PORT), DATABASE, "default", "");

            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(1);
            // A short checkpoint interval keeps the test's flush latency low;
            // production uses 30 s.
            env.enableCheckpointing(1_000L);
            ArchiveJob.build(env, KAFKA.getBootstrapServers(), FEATURE_TOPIC, DLQ_TOPIC, config);

            // executeAsync returns a JobClient immediately — no helper thread needed.
            JobClient job = env.executeAsync("archive-job-e2e-test");
            try {
                List<GenericRecord> features = awaitRows(query,
                    "SELECT event_id, sensor, schema_hash, length(`values`) AS n, `values`[1] AS first, "
                        + "toUnixTimestamp64Milli(row_version) AS version FROM feature_vectors");

                assertEquals(1, features.size(), "one feature vector must reach feature_vectors");
                assertEquals("sensor-eu-1:Cabc123XYZ", features.get(0).getString("event_id"));
                assertEquals("sensor-eu-1", features.get(0).getString("sensor"));
                assertEquals(ConnFeatureSchemaV1.CONTENT_HASH, features.get(0).getString("schema_hash"));
                assertEquals(20, features.get(0).getInteger("n"), "all 20 values must survive the round trip");
                assertEquals(42.5f, features.get(0).getFloat("first"), 0.0001f);
                assertEquals(Instant.parse("2026-08-27T10:03:11.402Z").toEpochMilli(),
                    features.get(0).getLong("version"), "row_version is the producer's producedAt");

                List<GenericRecord> invalid = awaitRows(query,
                    "SELECT event_id, stage, reason_code, source_version FROM invalid_events");

                assertEquals(1, invalid.size(), "one rejected record must reach invalid_events");
                assertEquals("PARSE", invalid.get(0).getString("stage"));
                assertEquals("MALFORMED_JSON", invalid.get(0).getString("reason_code"));
                assertEquals("zeek-conn-source-v1", invalid.get(0).getString("source_version"));
                assertEquals("", invalid.get(0).getString("event_id"), "a parse-stage rejection has no identity");
            } finally {
                job.cancel().get();
            }
        }
    }
}
```

- [ ] **Step 5: Run the test to verify it fails**

Run: `./mvnw test -pl modules/bootstrap-archive-job -Dtest=ArchiveJobE2ETest`
Expected: compilation failure — `ArchiveJob` does not exist.

- [ ] **Step 6: Write the two map functions**

`modules/bootstrap-archive-job/src/main/java/io/netsecml/platform/bootstrap/archive/FeatureVectorRowMapFunction.java`:

```java
package io.netsecml.platform.bootstrap.archive;

import io.netsecml.platform.adapter.clickhouse.mapper.FeatureVectorRowMapper;
import io.netsecml.platform.adapter.clickhouse.row.FeatureVectorRow;
import io.netsecml.platform.adapter.kafka.sink.FeatureVectorDeserializer;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;

// Kafka bytes to a feature_vectors row.
//
// This lives in the bootstrap module rather than in either adapter because it is
// the only place allowed to know about both: adapter-kafka owns the JSON, and
// adapter-clickhouse owns the row. Neither imports the other.
public final class FeatureVectorRowMapFunction extends RichMapFunction<byte[], FeatureVectorRow> {

    // Only used for error messages from the Kafka Deserializer contract.
    private final String topic;

    // Built in open(), so nothing non-serializable travels through the job graph
    // and nothing is allocated per record.
    private transient FeatureVectorDeserializer deserializer;
    private transient FeatureVectorRowMapper mapper;

    public FeatureVectorRowMapFunction(String topic) {
        this.topic = topic;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        super.open(openContext);
        deserializer = new FeatureVectorDeserializer();
        mapper = new FeatureVectorRowMapper();
    }

    @Override
    public FeatureVectorRow map(byte[] message) {
        return mapper.toRow(deserializer.deserialize(topic, message));
    }
}
```

`modules/bootstrap-archive-job/src/main/java/io/netsecml/platform/bootstrap/archive/InvalidEventRowMapFunction.java`:

```java
package io.netsecml.platform.bootstrap.archive;

import io.netsecml.platform.adapter.clickhouse.mapper.InvalidEventRowMapper;
import io.netsecml.platform.adapter.clickhouse.row.InvalidEventRow;
import io.netsecml.platform.adapter.kafka.sink.RejectedEventDeserializer;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;

// Kafka bytes to an invalid_events row, via the neutral domain RejectedEvent.
public final class InvalidEventRowMapFunction extends RichMapFunction<byte[], InvalidEventRow> {

    private final String topic;

    private transient RejectedEventDeserializer deserializer;
    private transient InvalidEventRowMapper mapper;

    public InvalidEventRowMapFunction(String topic) {
        this.topic = topic;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        super.open(openContext);
        deserializer = new RejectedEventDeserializer();
        mapper = new InvalidEventRowMapper();
    }

    @Override
    public InvalidEventRow map(byte[] message) {
        return mapper.toRow(deserializer.deserialize(topic, message));
    }
}
```

- [ ] **Step 7: Write `ArchiveJob`**

`modules/bootstrap-archive-job/src/main/java/io/netsecml/platform/bootstrap/archive/ArchiveJob.java`:

```java
package io.netsecml.platform.bootstrap.archive;

import io.netsecml.platform.adapter.clickhouse.row.FeatureVectorRow;
import io.netsecml.platform.adapter.clickhouse.row.InvalidEventRow;
import io.netsecml.platform.adapter.clickhouse.writer.ClickHouseBatchSink;
import io.netsecml.platform.adapter.clickhouse.writer.ClickHouseConfig;
import io.netsecml.platform.adapter.flink.source.RawBytesDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

// The independent Kafka-to-ClickHouse archive job.
//
// Two source-to-sink chains in one job. The Flink job graph IS the routing, which
// is why there is no ArchiveRouter class: a router on top of a topology that
// already routes would be an abstraction with no behaviour.
//
// The two chains share checkpoint fate deliberately. Both write to the same
// ClickHouse, so if it is unreachable both should stall and let Kafka lag grow
// rather than one quietly racing ahead.
//
// This job must never be able to stop online feature production. That is why it
// is a separate deployment with its own restart strategy and its own lag.
public final class ArchiveJob {

    // One consumer group for both topics: this is one logical archiver.
    public static final String CONSUMER_GROUP = "conn-archive-job";

    private ArchiveJob() {
    }

    public static void build(StreamExecutionEnvironment env, String bootstrapServers,
                             String featureVectorTopic, String dlqTopic, ClickHouseConfig clickHouse) {

        // Chain 1 — feature vectors. The required Day 6 path: a versioned vector
        // observable in Kafka must become queryable in ClickHouse.
        env.fromSource(source(bootstrapServers, featureVectorTopic),
                WatermarkStrategy.noWatermarks(), "feature-vector-source")
            .map(new FeatureVectorRowMapFunction(featureVectorTopic))
            .name("feature-vector-row")
            .sinkTo(new ClickHouseBatchSink<FeatureVectorRow>("feature_vectors", clickHouse))
            .name("feature-vectors-clickhouse-sink");

        // Chain 2 — rejected records. Low volume, and duplicates after a replay
        // are expected rather than prevented.
        env.fromSource(source(bootstrapServers, dlqTopic),
                WatermarkStrategy.noWatermarks(), "dlq-source")
            .map(new InvalidEventRowMapFunction(dlqTopic))
            .name("invalid-event-row")
            .sinkTo(new ClickHouseBatchSink<InvalidEventRow>("invalid_events", clickHouse))
            .name("invalid-events-clickhouse-sink");
    }

    // No watermarks: nothing downstream is event-time windowed. The archive job
    // batches and inserts; it never reasons about time.
    private static KafkaSource<byte[]> source(String bootstrapServers, String topic) {
        return KafkaSource.<byte[]>builder()
            .setBootstrapServers(bootstrapServers)
            .setTopics(topic)
            .setGroupId(CONSUMER_GROUP)
            // Resume from committed offsets so a redeploy continues where it left
            // off, falling back to the earliest offset on a first run.
            .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
            .setValueOnlyDeserializer(new RawBytesDeserializationSchema())
            .build();
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Same variable names the online job reads, all documented in .env.example.
        String bootstrapServers = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
        String featureTopic = System.getenv().getOrDefault("FEATURE_VECTOR_TOPIC", "netsec.conn.feature-vector.v1");
        String dlqTopic = System.getenv().getOrDefault("DLQ_TOPIC", "netsec.conn.dlq.v1");

        ClickHouseConfig clickHouse = ClickHouseConfig.of(
            System.getenv().getOrDefault("CLICKHOUSE_HOST", "localhost"),
            Integer.parseInt(System.getenv().getOrDefault("CLICKHOUSE_PORT", "8123")),
            System.getenv().getOrDefault("CLICKHOUSE_DATABASE", "netsec_ml"),
            System.getenv().getOrDefault("CLICKHOUSE_USER", "default"),
            System.getenv().getOrDefault("CLICKHOUSE_PASSWORD", ""));

        build(env, bootstrapServers, featureTopic, dlqTopic, clickHouse);
        env.execute("conn-archive-job");
    }
}
```

- [ ] **Step 8: Document the topic variables in `.env.example`**

The online job already reads four topic variables with inline defaults, and none were ever documented. Add them under the Kafka section:

```
# --- Kafka topics (external input, internal outputs) ---
CONN_INPUT_TOPIC=conn
FEATURE_VECTOR_TOPIC=netsec.conn.feature-vector.v1
DLQ_TOPIC=netsec.conn.dlq.v1
SENSOR_ID=sensor-default
```

- [ ] **Step 9: Run the test to verify it passes**

Run: `./mvnw test -pl modules/bootstrap-archive-job -Dtest=ArchiveJobE2ETest`
Expected: PASS, 1 test. Needs Docker; it starts a Kafka container and a ClickHouse container.

If no rows appear before the deadline, the job most likely failed rather than stalled — inspect the surefire output for the Flink exception. A `Unknown column` error means a row's `@JsonProperty` name disagrees with the DDL; `ClientV2InserterTest` from Task 9 should have caught that first.

- [ ] **Step 10: Run the full build**

Run: `./mvnw clean verify`
Expected: BUILD SUCCESS across all modules.

- [ ] **Step 11: Commit**

```bash
git add modules/adapter-flink/src/main/java/io/netsecml/platform/adapter/flink/source/ \
        modules/bootstrap-online-job/src/main/java/io/netsecml/platform/bootstrap/online/OnlineFeatureJob.java \
        modules/adapter-clickhouse/pom.xml \
        modules/adapter-clickhouse/src/test/java/io/netsecml/platform/adapter/clickhouse/ClickHouseTestSupport.java \
        modules/bootstrap-archive-job/pom.xml \
        modules/bootstrap-archive-job/src/main/java/io/netsecml/platform/bootstrap/archive/ \
        modules/bootstrap-archive-job/src/test/java/io/netsecml/platform/bootstrap/archive/ \
        .env.example
git commit -m "feat(bootstrap-archive-job): wire the Kafka-to-ClickHouse archive job end-to-end"
```

---
### Task 12: The deduplication contract, committed and tested

`ReplacingMergeTree` compacts eventually, never immediately. Training reads this table from Day 7, so the query that resolves duplicates is committed as a file — not described in prose and reinvented later — and tested against duplicates that actually exist.

The realistic duplicate is not two identical rows. After a checkpoint restore the online job replays the same source event, and indices 17-19 come from the restored rolling-window state, so the values can legitimately differ. `row_version` is what makes "which one wins" deterministic.

**Files:**
- Create: `infrastructure/clickhouse/queries/feature-vector-dedup.sql`
- Test: `modules/adapter-clickhouse/src/test/java/io/netsecml/platform/adapter/clickhouse/FeatureVectorDeduplicationTest.java`

**Interfaces:**
- Consumes: `ClickHouseTestSupport` (Task 9), the DDL (Task 6).
- Produces: `infrastructure/clickhouse/queries/feature-vector-dedup.sql`, taking one server-side parameter `hash` of type `FixedString(64)`.

- [ ] **Step 1: Write the failing test**

`modules/adapter-clickhouse/src/test/java/io/netsecml/platform/adapter/clickhouse/FeatureVectorDeduplicationTest.java`:

```java
package io.netsecml.platform.adapter.clickhouse;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.query.GenericRecord;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.*;

// Roadmap Day 6, test 2: a retried archive write produces an allowed duplicate,
// and the snapshot query still returns one deterministic latest row.
@Testcontainers(disabledWithoutDocker = true)
class FeatureVectorDeduplicationTest {
    private static final String SCHEMA_HASH = "f".repeat(64);
    private static final String OTHER_HASH = "e".repeat(64);

    @Container
    private static final GenericContainer<?> CLICKHOUSE = ClickHouseTestSupport.newContainer();

    // The committed query, read from disk. The test must exercise the shipped
    // file verbatim — a copy pasted into the test would prove nothing about what
    // training actually runs.
    private static String dedupQuery() throws Exception {
        return Files.readString(
            ClickHouseTestSupport.repoPath("infrastructure", "clickhouse", "queries", "feature-vector-dedup.sql"));
    }

    // Inserts one row directly, so the test controls row_version precisely.
    private void insert(Client client, String eventId, String schemaHash, float first, String rowVersion) throws Exception {
        client.execute("INSERT INTO feature_vectors "
            + "(event_id, event_time, sensor, schema_id, schema_hash, `values`, quality_flags, row_version) VALUES ("
            + "'" + eventId + "', '2026-08-27 10:03:11.250', 'sensor-eu-1', 'conn-feature-v1', "
            + "'" + schemaHash + "', [" + first + ", 2, 3], 0, '" + rowVersion + "')").get();
    }

    // Two emissions of the same event with DIFFERENT values — what a replay after
    // a checkpoint restore actually produces, because indices 17-19 come from the
    // restored window state.
    @Test
    void returnsOneDeterministicLatestRowPerEventAndSchema() throws Exception {
        try (Client client = ClickHouseTestSupport.freshDatabase(CLICKHOUSE, "dedup_latest")) {
            insert(client, "sensor-eu-1:a", SCHEMA_HASH, 1.0f, "2026-08-27 10:03:11.402");
            insert(client, "sensor-eu-1:a", SCHEMA_HASH, 9.0f, "2026-08-27 11:00:00.000");

            // No OPTIMIZE ... FINAL anywhere: the point is that the query is
            // correct without physical compaction having happened.
            assertEquals(2, client.queryAll("SELECT count() AS c FROM feature_vectors").get(0).getLong("c"),
                "both rows are physically present; ReplacingMergeTree has not compacted");

            List<GenericRecord> deduplicated =
                client.queryAll(dedupQuery(), Map.of("hash", SCHEMA_HASH));

            assertEquals(1, deduplicated.size(), "the query must collapse the duplicate");
            assertEquals("sensor-eu-1:a", deduplicated.get(0).getString("event_id"));
            assertEquals(9.0f, deduplicated.get(0).getFloatArray("values")[0], 0.0001f,
                "the later row_version wins, so the replayed emission's values survive");
        }
    }

    // Re-running the same query must give the same answer; "deterministic" is
    // half the requirement.
    @Test
    void isStableAcrossRepeatedRuns() throws Exception {
        try (Client client = ClickHouseTestSupport.freshDatabase(CLICKHOUSE, "dedup_stable")) {
            insert(client, "sensor-eu-1:a", SCHEMA_HASH, 1.0f, "2026-08-27 10:03:11.402");
            insert(client, "sensor-eu-1:a", SCHEMA_HASH, 9.0f, "2026-08-27 11:00:00.000");

            float firstRun = client.queryAll(dedupQuery(), Map.of("hash", SCHEMA_HASH))
                .get(0).getFloatArray("values")[0];
            float secondRun = client.queryAll(dedupQuery(), Map.of("hash", SCHEMA_HASH))
                .get(0).getFloatArray("values")[0];

            assertEquals(firstRun, secondRun, 0.0f);
        }
    }

    // A snapshot is always taken for one frozen feature schema. Rows from another
    // schema version must not leak into it.
    @Test
    void filtersToTheRequestedSchemaHash() throws Exception {
        try (Client client = ClickHouseTestSupport.freshDatabase(CLICKHOUSE, "dedup_filter")) {
            insert(client, "sensor-eu-1:a", SCHEMA_HASH, 1.0f, "2026-08-27 10:03:11.402");
            insert(client, "sensor-eu-1:b", OTHER_HASH, 5.0f, "2026-08-27 10:03:11.402");

            List<GenericRecord> rows = client.queryAll(dedupQuery(), Map.of("hash", SCHEMA_HASH));

            assertEquals(1, rows.size());
            assertEquals("sensor-eu-1:a", rows.get(0).getString("event_id"));
        }
    }
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `./mvnw test -pl modules/adapter-clickhouse -Dtest=FeatureVectorDeduplicationTest`
Expected: FAIL — `NoSuchFileException` for `infrastructure/clickhouse/queries/feature-vector-dedup.sql`.

- [ ] **Step 3: Write the deduplication query**

`infrastructure/clickhouse/queries/feature-vector-dedup.sql`:

```sql
-- Canonical deduplication for the training snapshot.
--
-- feature_vectors is a ReplacingMergeTree, which compacts eventually and never
-- immediately. No query may assume physical deduplication has happened, so the
-- snapshot resolves duplicates explicitly.
--
-- row_version is the producer's producedAt. After a checkpoint restore the online
-- job replays the source event and re-stamps it, so the replayed emission is
-- strictly newer and wins — which is correct, because its indices 17-19 come from
-- the restored rolling-window state and are the values consistent with it.
--
-- Parameter: hash (FixedString(64)) — the frozen feature schema this snapshot is for.
--
-- `values` is backtick-quoted because VALUES is INSERT syntax.
SELECT
    event_id,
    schema_hash,
    argMax(`values`, row_version)      AS `values`,
    argMax(sensor, row_version)        AS sensor,
    argMax(event_time, row_version)    AS event_time,
    argMax(quality_flags, row_version) AS quality_flags,
    max(row_version)                   AS row_version
FROM feature_vectors
WHERE schema_hash = {hash:FixedString(64)}
GROUP BY event_id, schema_hash
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `./mvnw test -pl modules/adapter-clickhouse -Dtest=FeatureVectorDeduplicationTest`
Expected: PASS, 3 tests.

If `queryAll(sql, params)` does not bind the server-side parameter, confirm against `client-v2:0.9.0` that the overload `queryAll(String, Map<String, Object>)` sends `param_hash`. Should it prove unusable, keep the committed SQL exactly as written and have the test substitute the literal before executing — the shipped file must not change shape to suit the test.

- [ ] **Step 5: Commit**

```bash
git add infrastructure/clickhouse/queries/feature-vector-dedup.sql \
        modules/adapter-clickhouse/src/test/java/io/netsecml/platform/adapter/clickhouse/FeatureVectorDeduplicationTest.java
git commit -m "feat(infrastructure): commit and test the feature-vector deduplication query"
```

---

### Task 13: Prove ClickHouse cannot stop feature production

The Definition of Done's teeth, and the reason the archive job is a separate deployment at all.

This is also the most flake-prone test in the plan: two Flink jobs in one JVM, a container killed mid-flight. Its assertions are deliberately coarse — records still arriving, job still `RUNNING`. It must **not** assert on lag values, restart counts, or timing, all of which are nondeterministic and would buy flakiness in exchange for no information.

**Files:**
- Create: `modules/bootstrap-archive-job/src/test/java/io/netsecml/platform/bootstrap/archive/ClickHouseOutageTest.java`
- Modify: `modules/bootstrap-archive-job/pom.xml` (test-scoped dependency on `bootstrap-online-job`)

**Interfaces:**
- Consumes: `ArchiveJob.build` (Task 11), `OnlineFeatureJob.build` (existing), `ClickHouseTestSupport` (Task 9).
- Produces: nothing consumed by later tasks.

- [ ] **Step 1: Add the test-scoped dependency on the online job**

Both jobs must run for this test to mean anything, and the archive module is the one whose failure mode is under examination. Add to `modules/bootstrap-archive-job/pom.xml`:

```xml
    <!-- Test scope only: ClickHouseOutageTest runs both jobs to prove an archive
         failure cannot reach the online job. No cycle — bootstrap-online-job does
         not depend on this module. -->
    <dependency>
      <groupId>io.netsecml.platform</groupId>
      <artifactId>bootstrap-online-job</artifactId>
      <version>${project.version}</version>
      <scope>test</scope>
    </dependency>
```

- [ ] **Step 2: Write the test**

`modules/bootstrap-archive-job/src/test/java/io/netsecml/platform/bootstrap/archive/ClickHouseOutageTest.java`:

```java
package io.netsecml.platform.bootstrap.archive;

import com.clickhouse.client.api.Client;
import io.netsecml.platform.adapter.clickhouse.ClickHouseTestSupport;
import io.netsecml.platform.adapter.clickhouse.writer.ClickHouseConfig;
import io.netsecml.platform.bootstrap.online.OnlineFeatureJob;
import io.netsecml.platform.domain.event.SensorId;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.core.execution.JobClient;
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
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import java.nio.file.Files;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import static org.junit.jupiter.api.Assertions.*;

// Roadmap Day 6, test 3, and the Definition of Done's teeth: ClickHouse failure
// must not stop feature production.
//
// Assertions here are deliberately coarse — records still arriving on the feature
// topic, online job still RUNNING. Asserting on lag values, restart counts or
// timings would buy flakiness in exchange for no information.
@Testcontainers(disabledWithoutDocker = true)
class ClickHouseOutageTest {
    private static final String INPUT_TOPIC = "netsec.conn.raw.v1";
    private static final String FEATURE_TOPIC = "netsec.conn.feature-vector.v1";
    private static final String DLQ_TOPIC = "netsec.conn.dlq.v1";
    private static final String DATABASE = "outage_test";

    @Container
    private static final ConfluentKafkaContainer KAFKA =
        new ConfluentKafkaContainer("confluentinc/cp-kafka:7.6.1");

    @Container
    private static final GenericContainer<?> CLICKHOUSE = ClickHouseTestSupport.newContainer();

    // The same fixture the online job's own E2E test uses.
    private byte[] fixture() throws Exception {
        return Files.readAllBytes(
            ClickHouseTestSupport.repoPath("tests", "fixtures", "zeek_conn", "valid-tcp-ssl.json"));
    }

    private void produce(int count) throws Exception {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(props)) {
            for (int i = 0; i < count; i++) {
                producer.send(new ProducerRecord<>(INPUT_TOPIC, fixture())).get();
            }
        }
    }

    private Consumer<String, String> featureTopicConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "outage-test-reader");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        Consumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(List.of(FEATURE_TOPIC));
        return consumer;
    }

    // Collects up to `wanted` records, or gives up at the deadline. Returns what
    // it got either way, so the caller decides what a shortfall means.
    private List<String> drain(Consumer<String, String> consumer, int wanted, Duration timeout) {
        List<String> collected = new ArrayList<>();
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        while (collected.size() < wanted && System.currentTimeMillis() < deadline) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(2));
            records.forEach(record -> collected.add(record.value()));
        }
        return collected;
    }

    @Test
    void onlineJobKeepsPublishingAfterClickHouseDies() throws Exception {
        Client query = ClickHouseTestSupport.freshDatabase(CLICKHOUSE, DATABASE);
        ClickHouseConfig config = ClickHouseConfig.of(CLICKHOUSE.getHost(),
            CLICKHOUSE.getMappedPort(ClickHouseTestSupport.HTTP_PORT), DATABASE, "default", "");

        StreamExecutionEnvironment onlineEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        onlineEnv.setParallelism(1);
        OnlineFeatureJob.build(onlineEnv, KAFKA.getBootstrapServers(), INPUT_TOPIC,
            FEATURE_TOPIC, DLQ_TOPIC, new SensorId("sensor-eu-1"));

        StreamExecutionEnvironment archiveEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        archiveEnv.setParallelism(1);
        archiveEnv.enableCheckpointing(1_000L);
        ArchiveJob.build(archiveEnv, KAFKA.getBootstrapServers(), FEATURE_TOPIC, DLQ_TOPIC, config);

        JobClient onlineJob = onlineEnv.executeAsync("outage-test-online");
        JobClient archiveJob = archiveEnv.executeAsync("outage-test-archive");

        try (Consumer<String, String> consumer = featureTopicConsumer()) {
            // Phase 1 — everything healthy. Prove the whole chain works before
            // breaking anything, otherwise a later "no rows" result is ambiguous.
            produce(5);
            assertEquals(5, drain(consumer, 5, Duration.ofSeconds(90)).size(),
                "the online job must publish five vectors while ClickHouse is up");

            long deadline = System.currentTimeMillis() + 90_000;
            long archived = 0;
            while (archived == 0 && System.currentTimeMillis() < deadline) {
                archived = query.queryAll("SELECT count() AS c FROM feature_vectors").get(0).getLong("c");
                if (archived == 0) {
                    Thread.sleep(500);
                }
            }
            assertTrue(archived > 0, "the archive job must reach ClickHouse before the outage");
            query.close();

            // Phase 2 — kill ClickHouse. The archive job will now fail its
            // checkpoints and restart on a loop; that is the designed behaviour,
            // and Kafka lag is where the failure surfaces.
            CLICKHOUSE.stop();

            // Phase 3 — the online job must not have noticed.
            produce(5);
            assertEquals(5, drain(consumer, 5, Duration.ofSeconds(120)).size(),
                "feature production must continue after ClickHouse dies");
            assertEquals(JobStatus.RUNNING, onlineJob.getJobStatus().get(),
                "an archive-side failure must never reach the online job");
        } finally {
            onlineJob.cancel().get();
            // The archive job may already be in a restart loop; cancelling it can
            // race with that, and its final state is not what this test is about.
            try {
                archiveJob.cancel().get();
            } catch (Exception ignored) {
                // Cancelling a restarting job is best-effort.
            }
        }
    }
}
```

- [ ] **Step 3: Run the test**

Run: `./mvnw test -pl modules/bootstrap-archive-job -Dtest=ClickHouseOutageTest`
Expected: PASS, 1 test. Expect noisy logs after phase 2 — the archive job restarting on a loop is the behaviour under test, not a defect.

If phase 1 times out, the chain is broken rather than slow: check that `ArchiveJobE2ETest` still passes before debugging this one.

- [ ] **Step 4: Run the full build**

Run: `./mvnw clean verify`
Expected: BUILD SUCCESS.

- [ ] **Step 5: Commit**

```bash
git add modules/bootstrap-archive-job/pom.xml \
        modules/bootstrap-archive-job/src/test/java/io/netsecml/platform/bootstrap/archive/ClickHouseOutageTest.java
git commit -m "test(bootstrap-archive-job): prove ClickHouse failure cannot stop feature production"
```

---
### Task 14: Documentation and the table-set ADR

`docs/clickhouse.md` is a named Day 6 deliverable. The ADR records why this plan's schema follows `FINAL_ARCHITECTURE.md` over `Roadmap.md`, so the next person to notice the disagreement finds the decision instead of re-litigating it.

**Files:**
- Create: `docs/clickhouse.md`
- Create: `docs/adr/0001-clickhouse-table-set.md`
- Modify: `CLAUDE.md`

**Interfaces:**
- Consumes: everything.
- Produces: nothing consumed by later tasks.

- [ ] **Step 1: Write `docs/clickhouse.md`**

```markdown
# ClickHouse

ClickHouse is an archive and training-query system. It is never on the online
scoring path. The online job publishes to internal Kafka topics; a separate
archive job batches those into ClickHouse. If ClickHouse is unreachable, the
archive job stalls and Kafka lag grows — feature production continues.

## Tables

`infrastructure/clickhouse/ddl/001_mvp_tables.sql` defines five tables. Two are
written today.

| Table | Engine | Partition | Order | TTL | Written by |
|---|---|---|---|---|---|
| `feature_vectors` | `ReplacingMergeTree(row_version)` | `toYYYYMMDD(event_time)` | `(schema_hash, event_time, event_id)` | 90 days | archive job |
| `invalid_events` | `MergeTree` | `toYYYYMM(received_at)` | `(stage, received_at, raw_payload_hash)` | 30 days | archive job |
| `predictions` | `ReplacingMergeTree(row_version)` | `toYYYYMMDD(event_time)` | `(model_name, model_version, event_time, event_id)` | 180 days | Roadmap Day 9 |
| `network_events` | `ReplacingMergeTree(row_version)` | `toYYYYMMDD(event_time)` | `(sensor, event_time, event_id)` | 14 days | nothing — optional |
| `model_releases` | `MergeTree` | none | `(model_name, model_version)` | none | Roadmap Day 11 |

`DateTime64(3, 'UTC')` throughout. `IPv6` for IP columns, so IPv4 is consistently
mapped. `LowCardinality(String)` only for bounded values — sensor, protocol,
service, state, stage, reason code.

The `values` column is backtick-quoted in every statement, because `VALUES` is
INSERT syntax and an unquoted identifier invites ambiguity.

## Applying the schema

```sh
CLICKHOUSE_HOST=localhost CLICKHOUSE_DATABASE=netsec_ml ./scripts/database/apply-ddl.sh
```

Every statement is `CREATE ... IF NOT EXISTS`, so re-running is a no-op. The
script creates the database first, then applies `infrastructure/clickhouse/ddl/*.sql`
in lexical order. It reads the `CLICKHOUSE_*` variables documented in `.env.example`.

`DdlMigrationTest` applies this exact file against a ClickHouse container and also
runs the script itself, so there is no test-only copy of the schema to drift.

## Deduplication

`ReplacingMergeTree` compacts eventually, never immediately. **No query may assume
physical deduplication has happened.** The canonical resolution is committed at
`infrastructure/clickhouse/queries/feature-vector-dedup.sql`:

```sql
SELECT event_id, schema_hash,
       argMax(`values`, row_version) AS `values`,
       max(row_version) AS row_version
FROM feature_vectors
WHERE schema_hash = {hash:FixedString(64)}
GROUP BY event_id, schema_hash
```

`row_version` is the producer's `producedAt`. After a checkpoint restore the online
job replays the source event and re-stamps it, so the replayed emission is strictly
newer and wins. That is the correct outcome: its feature indices 17-19 come from
the restored rolling-window state and are the values consistent with it.

`invalid_events` is duplicate-tolerant by design rather than deduplicated. A replay
re-rejects and re-stamps the record, so duplicate rows accumulate. It is low-volume
30-day forensic data; deduplicate with `GROUP BY raw_payload_hash` at query time if
a count needs to be exact.

## Batching and back pressure

The archive sink flushes on whichever comes first:

| Trigger | Value |
|---|---|
| Rows | 5,000 |
| Serialized payload | 4 MiB |
| Processing-time timer | 1 s |
| Checkpoint barrier | always |

These are initial values to benchmark, not settled ones. They are constructor
parameters on `ClickHouseBatchSink`, with the defaults above.

A failed insert is retried three times with 200/400/800 ms backoff, then throws.
Throwing fails the checkpoint; the Kafka offsets in that checkpoint do not advance;
the job restarts and replays. This is how the job satisfies "commit consumer
progress only after successful insert acknowledgement" without a manual commit.

Retrying without bound would convert a ClickHouse outage into silent backpressure.
Throwing converts it into visible Kafka lag. **Lag, not RAM** — the buffer cannot
grow past one batch, because a trigger either flushes it or throws.

## When ClickHouse is down

1. The archive job's inserts fail, exhaust their retries, and throw.
2. Its checkpoints fail, so its Kafka offsets stop advancing.
3. The restart strategy backs off; consumer lag on the internal topics grows.
4. **The online job is unaffected.** It shares no process, no checkpoint and no
   state with the archive job, and keeps publishing to Kafka throughout.
5. When ClickHouse returns, the archive job replays from its last successful
   checkpoint. Replayed rows are duplicates; `ReplacingMergeTree` absorbs them in
   `feature_vectors`, and `invalid_events` tolerates them.

`ClickHouseOutageTest` asserts points 4 and 5.

## Metrics

Per table, on the sink writer's metric group:

| Metric | Type |
|---|---|
| `archive.table.<name>.batch.rows` | gauge, last batch |
| `archive.table.<name>.batch.bytes` | gauge, last batch |
| `archive.table.<name>.flush.latency.ms` | gauge, last flush |
| `archive.table.<name>.insert.failures` | counter, per failed attempt |

Failures are counted per attempt rather than per batch, so the counter
distinguishes a flaky server from a dead one. Real observability — dashboards,
alerting, histograms — is Roadmap Day 12.

## Access and retention

`network_events` is the only IP-bearing table. Before it is enabled it needs a
least-privilege ClickHouse user and a documented retention approval; neither is in
place, and nothing writes to it today.

`feature_vectors` carries no IP addresses — only the 20 numeric features, the
sensor name and the composite event id. `invalid_events` carries a payload hash,
never the payload.

TTL values are initial. Calculate storage from observed compressed bytes per event
times events per second times retention before treating any of them as settled.
```

- [ ] **Step 2: Write the ADR**

`docs/adr/0001-clickhouse-table-set.md`:

```markdown
# ADR 0001 — ClickHouse table set and naming

**Date:** 2026-08-31
**Status:** Accepted
**Context:** Repository_Structure.md Section E Step 8 / Roadmap.md Day 6

## Context

Two planning documents disagree about the ClickHouse schema.

`Roadmap.md`, under "Minimum ClickHouse scope", says to create **only four
tables**: `network_events`, `feature_vectors`, `predictions`, `model_metadata`.

`FINAL_ARCHITECTURE.md` Step 8 lists **five**, with full column types:
`network_events` (marked optional), `feature_vectors`, `predictions`,
`invalid_events`, `model_releases`.

The archive job archives the DLQ stream, which needs `invalid_events` — a table
only the second list contains. The four-table list therefore cannot stand as
written.

## Decision

Follow `FINAL_ARCHITECTURE.md`: five tables, and `model_releases` rather than
`model_metadata`.

Reasons:

1. It contains `invalid_events`, which the chosen archive scope requires.
2. It specifies exact column types, engines, partitioning and ordering. The
   Roadmap's list is a summary; this one is implementable.
3. `model_releases` is the more accurate name. `FINAL_ARCHITECTURE.md` describes
   the table as "an audit mirror, not artifact storage", which is a release log,
   not metadata.

## Deviations from FINAL_ARCHITECTURE.md

Two, both in `invalid_events`, both deliberate.

**`ORDER BY` changed** from `(stage, created_at, event_id)` to
`(stage, received_at, raw_payload_hash)`. The specified key does not work: it
sorts on `created_at`, which is a server-side `DEFAULT` and therefore absent from
the insert, and on `event_id`, which is empty for exactly the parse-stage failures
this table exists to hold. Producer-stamped `received_at` and the payload hash give
real locality, and make query-time deduplication a `GROUP BY raw_payload_hash`.

**"Optional redacted sample" column dropped.** The pipeline carries a payload hash
and never the payload, so the column would be empty on every row.

## Consequences

- `predictions`, `model_releases` and `network_events` exist from day one with no
  writer. Applying the DDL is what verifies them; Day 9 and Day 11 arrive to
  tables that already exist.
- `network_events` stays disabled in practice. It is the only IP-bearing table and
  needs a least-privilege user and retention approval first.
- `infrastructure/clickhouse/README.md` was rewritten; it previously repeated the
  four-table claim.
- A `model_metadata` table is never created. Anything referring to that name means
  `model_releases`.
```

- [ ] **Step 3: Update `CLAUDE.md`**

Replace the "Implementation state" section:

```markdown
## Implementation state

The conn foundation pipeline (Steps 2-7) and the ClickHouse archive job (Step 8)
are implemented and merged. Implementation order is tracked in
`Repository_Structure.md` Section E (18 steps).

Working today, end to end: external `conn` topic → parse/validate → bounded keyed
state → 20-value `FeatureVector` → `netsec.conn.feature-vector.v1` and
`netsec.conn.dlq.v1` → archive job → ClickHouse `feature_vectors` and
`invalid_events`.

Not yet implemented: ONNX inference (Day 9), predictions and `netsec.prediction.v1`
(Day 9), the model registry (Day 7), and the Python training project.

Design records: `docs/conn-foundation-pipeline.md` (Steps 2-7),
`docs/superpowers/specs/2026-08-27-clickhouse-archive-job-design.md` and
`docs/clickhouse.md` (Step 8).
```

- [ ] **Step 4: Run the full build one last time**

Run: `./mvnw clean verify`
Expected: BUILD SUCCESS, all tests pass.

- [ ] **Step 5: Commit**

```bash
git add docs/clickhouse.md docs/adr/0001-clickhouse-table-set.md CLAUDE.md
git commit -m "docs: add ClickHouse operations guide and table-set ADR"
```

---

## Deviations from the spec

Recorded here so the reviewer can check them against
`docs/superpowers/specs/2026-08-27-clickhouse-archive-job-design.md` rather than
discovering them one at a time. Each is a refinement found while making the design
concrete; none changes a decision the spec settled.

| Spec says | This plan does | Why |
|---|---|---|
| `dlq-v1` has five fields | Six — adds `eventId` | A map-stage rejection parsed cleanly, so the record that failed can be named. `invalid_events.event_id` exists in the spec's own schema and would otherwise be empty on every row. |
| `adapter-clickhouse` depends on "domain + client-v2 + Sink V2" | Also Jackson | Rows are serialized to JSONEachRow. The alternative — hand-rolling JSON escaping — is a bug farm. Jackson in an adapter is allowed; only `domain` and `application` are forbidden it. |
| `org.testcontainers:clickhouse` as a new dependency | Not added | `GenericContainer` from the already-managed `org.testcontainers:junit-jupiter` is enough, and avoids `ClickHouseContainer`'s JDBC-driver readiness probe for a driver this project does not ship. |
| Metrics are histograms | Gauges of the last batch, plus a failure counter | Flink `Histogram` needs the dropwizard bridge or an internal class. Not worth a dependency for four numbers; Day 12 owns real observability. |
| Test named `ReasonCodeStageTest` | `ReasonCodeTest` | It covers the whole enum, not only the stage mapping. |
| No inserter-level test named | `ClientV2InserterTest` added | It is where the row records' `@JsonProperty` names meet the real DDL. Catching a column-name mismatch on a two-row insert beats catching it inside the end-to-end test. |
| `OnlineFeatureJobE2ETest` untouched beyond `sensor`/`producedAt` assertions | Also loses its private `RAW_BYTES` schema | `ArchiveJob` needs the same nine lines; they move to `adapter-flink`, which both bootstraps can depend on. |

## Definition of done

1. `./mvnw clean verify` passes from a clean tree.
2. A feature vector produced to `netsec.conn.feature-vector.v1` is queryable in `feature_vectors` with all 20 values intact — `ArchiveJobE2ETest`.
3. A rejected record produced to `netsec.conn.dlq.v1` is queryable in `invalid_events` with the correct stage — `ArchiveJobE2ETest`.
4. The committed deduplication query returns exactly one deterministic row per `(event_id, schema_hash)` in the presence of duplicates, without `OPTIMIZE ... FINAL` — `FeatureVectorDeduplicationTest`.
5. With ClickHouse stopped, the online job keeps publishing to Kafka and stays `RUNNING` — `ClickHouseOutageTest`.
6. `001_mvp_tables.sql` applies cleanly to an empty ClickHouse and is idempotent on a second run, through both the Java path and `apply-ddl.sh` — `DdlMigrationTest`.
7. Both stream contracts are committed and their drift tests pass — `StreamContractDriftTest`.
8. `docs/clickhouse.md` and `docs/adr/0001-clickhouse-table-set.md` are committed; `infrastructure/clickhouse/README.md`, `contracts/stream/README.md`, `.env.example` and `CLAUDE.md` are corrected.
9. `modules/adapter-clickhouse` imports no Kafka and no Flink-connector types. Verify with:
   `grep -rn "org.apache.kafka\|connector.kafka" modules/adapter-clickhouse/src/main` returns nothing.
