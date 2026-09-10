# Common Feature Tier Foundation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the protocol-agnostic foundation every per-protocol feature schema will sit on — the common feature tier, the `conn.log` enrichment carrier, and an `ArchiveJob` that scales to many log types without hand-written chains.

**Architecture:** Three independent pieces. Two new pure-domain value types (`ConnSnapshot`, `RecordTimingState`) carry the inputs the existing `SourceWindowState` cannot; an application-layer `CommonFeatureExtractor` turns them into the frozen 12-value common tier; and `ArchiveJob.build` is refactored from two hand-written chains into a loop over registered log types. Nothing here consumes a protocol, so nothing here changes an existing schema.

**Tech Stack:** Java 21 (records, sealed types), Flink 2.2.1, JUnit 5, Jackson 2.17, ClickHouse client-v2 0.9.0, Testcontainers 1.21.4.

**Spec:** `docs/superpowers/specs/2026-09-10-per-protocol-feature-schemas-design.md` (this plan implements Unit 1 of §12)

## Global Constraints

- **Java package root:** `io.netsecml.platform`
- **Dependency chain is one-way:** `domain → ports → application → adapters → bootstrap`. `domain` imports no framework code — no Kafka, Flink, ClickHouse, ONNX, Jackson, Docker. `application` may import `domain` and `ports` only — no Jackson, no Flink. Adapters never import each other.
- **`conn-feature-v1` and its 20 frozen values are untouched.** `ConnFeatureSchemaV1.CONTENT_HASH` stays `f42fb1bebb2efe3acc5de634c6a7bb3d6f97fc021207f75d77652533b1c01e1b`.
- **`contracts/` is immutable.** A change creates a new version file, never an edit. New contract files are additions and are allowed.
- **`infrastructure/clickhouse/ddl/` is immutable.** Not touched by this plan.
- **`SourceWindowState` must not be widened.** Its serialized shape is shared with the `conn` path; new state goes in new types.
- **Inline comments describing each block are mandatory** — a standing user requirement. A comment that misdescribes the code is worse than no comment.
- **Records with array components need defensive copies in BOTH the compact constructor AND the accessor.**
- **Commit with explicit paths only.** Never `git add -A`, `git add .`, or a bare directory. `.mvn/wrapper/maven-wrapper.jar` must remain untracked.
- **Build in stages.** `./mvnw clean verify` is OOM-killed on a 5.7 GiB machine. Use `./mvnw install -DskipTests` once, then one module at a time, clearing containers between (`docker ps -aq | xargs -r docker rm -f`).
- **`-pl <module>` without `-am`** resolves siblings from `~/.m2` and produces phantom "cannot find symbol" errors.
- **`-Dtest=X` with `-Dsurefire.failIfNoSpecifiedTests=false`** reports BUILD SUCCESS having run zero tests. Always confirm the real `Tests run:` count.
- **Commit message trailer:**
  ```
  Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
  ```

---

## File Structure

| File | Responsibility |
|---|---|
| `modules/domain/.../feature/ConnSnapshot.java` | One `conn.log` observation: cumulative counters plus the connection's start time. Pure value. |
| `modules/domain/.../feature/ConnSnapshotDelta.java` | The per-interval difference between two consecutive snapshots. A distinct type so a delta and an absolute observation are not interchangeable at compile time. |
| `modules/domain/.../feature/RecordTimingState.java` | Bounded inter-arrival statistics for one key, via Welford. Holds no timestamp history. |
| `modules/domain/.../feature/CommonFeatureTierV1.java` | The frozen name and order of the 12 common features, and its content hash. |
| `contracts/features/common-feature-tier-v1.json` | Language-neutral contract for the same, read by the Python side. |
| `modules/application/.../feature/CommonFeatureExtractor.java` | Turns `SourceWindowState` + `RecordTimingState` + optional `ConnSnapshotDelta` into `float[12]`. |
| `modules/bootstrap-archive-job/.../ArchiveJob.java` | Chain loop over registered log types, replacing two hand-written chains. |

---

## Task 1: `ConnSnapshot` — the conn.log enrichment carrier

`conn.log` arrives periodically for open connections (spec §2.4) and its counters are **cumulative**, not per-interval. Rate features need the delta between consecutive snapshots, so the type must be able to subtract a predecessor.

**Files:**
- Create: `modules/domain/src/main/java/io/netsecml/platform/domain/feature/ConnSnapshot.java`
- Test: `modules/domain/src/test/java/io/netsecml/platform/domain/feature/ConnSnapshotTest.java`

**Interfaces:**
- Consumes: nothing.
- Produces: `record ConnSnapshot(String connectionUid, Instant connectionStart, Instant observedAt, long origBytes, long respBytes, long origPkts, long respPkts)`, with `ConnSnapshotDelta deltaFrom(ConnSnapshot previous)` and `long ageSeconds()` (clamped to zero when `observedAt` precedes `connectionStart`).

- [ ] **Step 1: Write the failing test**

`modules/domain/src/test/java/io/netsecml/platform/domain/feature/ConnSnapshotTest.java`:

```java
package io.netsecml.platform.domain.feature;

import org.junit.jupiter.api.Test;
import java.time.Instant;
import static org.junit.jupiter.api.Assertions.*;

// conn.log snapshots are cumulative, so the only useful rate signal is the
// difference between two of them. These tests pin that arithmetic and the
// guards around it.
class ConnSnapshotTest {

    private static final Instant START = Instant.parse("2026-09-10T10:00:00Z");

    // A snapshot five minutes in, then another five minutes later. The delta must
    // be the difference, never the raw cumulative totals.
    @Test
    void deltaFromSubtractsCumulativeCounters() {
        ConnSnapshot first = new ConnSnapshot("Cabc", START, START.plusSeconds(300), 1000L, 2000L, 10L, 20L);
        ConnSnapshot second = new ConnSnapshot("Cabc", START, START.plusSeconds(600), 1500L, 2600L, 14L, 27L);

        ConnSnapshot delta = second.deltaFrom(first);

        assertEquals(500L, delta.origBytes());
        assertEquals(600L, delta.respBytes());
        assertEquals(4L, delta.origPkts());
        assertEquals(7L, delta.respPkts());
    }

    // Zeek can restart or a counter can be reset between snapshots. A negative
    // delta is meaningless as a rate, so it clamps to zero rather than emitting
    // a negative feature the model would have to learn around.
    @Test
    void deltaFromClampsNegativeCountersToZero() {
        ConnSnapshot first = new ConnSnapshot("Cabc", START, START.plusSeconds(300), 1000L, 2000L, 10L, 20L);
        ConnSnapshot reset = new ConnSnapshot("Cabc", START, START.plusSeconds(600), 5L, 5L, 1L, 1L);

        ConnSnapshot delta = reset.deltaFrom(first);

        assertEquals(0L, delta.origBytes());
        assertEquals(0L, delta.respBytes());
        assertEquals(0L, delta.origPkts());
        assertEquals(0L, delta.respPkts());
    }

    // Two snapshots from different connections must never be subtracted; that
    // would silently blend unrelated traffic into one rate.
    @Test
    void deltaFromRejectsAMismatchedConnection() {
        ConnSnapshot mine = new ConnSnapshot("Cabc", START, START.plusSeconds(600), 1500L, 2600L, 14L, 27L);
        ConnSnapshot theirs = new ConnSnapshot("Cxyz", START, START.plusSeconds(300), 1000L, 2000L, 10L, 20L);

        assertThrows(IllegalArgumentException.class, () -> mine.deltaFrom(theirs));
    }

    // Age is what distinguishes a long-lived SCADA poll loop from a short burst,
    // and it is the reason conn.log is worth joining at all.
    @Test
    void ageSecondsMeasuresFromConnectionStartToObservation() {
        ConnSnapshot snapshot = new ConnSnapshot("Cabc", START, START.plusSeconds(3600), 1L, 1L, 1L, 1L);

        assertEquals(3600L, snapshot.ageSeconds());
    }

    // Identity is structural: without a uid the snapshot cannot be joined to
    // anything, so an absent one is a construction error rather than a default.
    @Test
    void rejectsABlankConnectionUid() {
        assertThrows(IllegalArgumentException.class,
            () -> new ConnSnapshot("  ", START, START, 1L, 1L, 1L, 1L));
    }
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `./mvnw test -pl modules/domain -am -Dtest=ConnSnapshotTest`
Expected: FAIL — compilation error, `ConnSnapshot` does not exist.

- [ ] **Step 3: Write the implementation**

`modules/domain/src/main/java/io/netsecml/platform/domain/feature/ConnSnapshot.java`:

```java
package io.netsecml.platform.domain.feature;

import java.time.Duration;
import java.time.Instant;

// One observation of a Zeek conn.log record for a connection that may still be
// open. The sensors are configured to emit conn.log every 5 minutes rather than
// only at connection close, so a long-lived connection produces a series of
// these.
//
// The counters are CUMULATIVE totals since the connection began, not per-interval
// values. Any rate feature must therefore be computed as the difference between
// two consecutive snapshots -- see deltaFrom.
public record ConnSnapshot(String connectionUid, Instant connectionStart, Instant observedAt,
                           long origBytes, long respBytes, long origPkts, long respPkts) {

    public ConnSnapshot {
        // Without a uid this snapshot cannot be joined to a protocol record, which
        // is its only purpose.
        if (connectionUid == null || connectionUid.isBlank()) {
            throw new IllegalArgumentException("connectionUid must not be blank");
        }
        if (connectionStart == null || observedAt == null) {
            throw new IllegalArgumentException("connectionStart and observedAt must not be null");
        }
    }

    // The per-interval difference against the previous snapshot of the SAME
    // connection. Returned as a ConnSnapshot so callers get the delta in the same
    // shape, with observedAt and connectionStart carried from this snapshot.
    public ConnSnapshot deltaFrom(ConnSnapshot previous) {
        // Subtracting across connections would blend unrelated traffic into one
        // rate, so it is a programming error rather than a recoverable case.
        if (previous == null || !previous.connectionUid.equals(connectionUid)) {
            throw new IllegalArgumentException(
                "deltaFrom requires a previous snapshot of the same connection: " + connectionUid);
        }
        return new ConnSnapshot(connectionUid, connectionStart, observedAt,
            nonNegativeDifference(origBytes, previous.origBytes),
            nonNegativeDifference(respBytes, previous.respBytes),
            nonNegativeDifference(origPkts, previous.origPkts),
            nonNegativeDifference(respPkts, previous.respPkts));
    }

    // A counter that went backwards means Zeek restarted or the connection was
    // re-keyed. Zero is the honest answer; a negative rate is not a signal the
    // model can use.
    private static long nonNegativeDifference(long current, long previous) {
        return Math.max(0L, current - previous);
    }

    // How long the connection had been alive when this snapshot was taken. This
    // is the feature that separates a persistent SCADA poll loop from a short
    // burst of traffic.
    public long ageSeconds() {
        return Duration.between(connectionStart, observedAt).getSeconds();
    }
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `./mvnw test -pl modules/domain -am -Dtest=ConnSnapshotTest`
Expected: PASS, 5 tests run.

- [ ] **Step 5: Commit**

```bash
git add modules/domain/src/main/java/io/netsecml/platform/domain/feature/ConnSnapshot.java \
        modules/domain/src/test/java/io/netsecml/platform/domain/feature/ConnSnapshotTest.java
git commit -m "feat(domain): add ConnSnapshot for periodic conn.log enrichment"
```

---

## Task 2: `RecordTimingState` — bounded inter-arrival statistics

The common tier needs inter-arrival mean and standard deviation. `SourceWindowState` holds no timestamps and **must not be widened** (Global Constraints) because its serialized shape is shared with the frozen `conn` path. This is a separate, additive type.

It uses Welford's online algorithm so the state is three numbers regardless of how many records arrive — no timestamp list, consistent with the bounded-state invariant.

**Files:**
- Create: `modules/domain/src/main/java/io/netsecml/platform/domain/feature/RecordTimingState.java`
- Test: `modules/domain/src/test/java/io/netsecml/platform/domain/feature/RecordTimingStateTest.java`

**Interfaces:**
- Consumes: nothing.
- Produces: `RecordTimingState.empty()`, `RecordTimingState observe(Instant recordTime)`, `double meanIntervalMillis()`, `double stddevIntervalMillis()`, `long observationCount()`.

- [ ] **Step 1: Write the failing test**

`modules/domain/src/test/java/io/netsecml/platform/domain/feature/RecordTimingStateTest.java`:

```java
package io.netsecml.platform.domain.feature;

import org.junit.jupiter.api.Test;
import java.time.Instant;
import static org.junit.jupiter.api.Assertions.*;

// Inter-arrival statistics for one key, kept in constant space. The bounded-state
// invariant forbids holding a timestamp history, so these tests pin the online
// arithmetic that replaces one.
class RecordTimingStateTest {

    private static final Instant T0 = Instant.parse("2026-09-10T10:00:00Z");

    // A single record has no interval yet -- there is nothing to measure against.
    // Zero is the defined answer so the feature is always emittable.
    @Test
    void aSingleObservationHasNoIntervalYet() {
        RecordTimingState state = RecordTimingState.empty().observe(T0);

        assertEquals(0.0, state.meanIntervalMillis(), 0.0001);
        assertEquals(0.0, state.stddevIntervalMillis(), 0.0001);
        assertEquals(1L, state.observationCount());
    }

    // Evenly spaced records: the mean is the spacing and the deviation is zero.
    // This is what a healthy SCADA poll loop looks like.
    @Test
    void evenlySpacedRecordsHaveZeroDeviation() {
        RecordTimingState state = RecordTimingState.empty()
            .observe(T0)
            .observe(T0.plusMillis(100))
            .observe(T0.plusMillis(200))
            .observe(T0.plusMillis(300));

        assertEquals(100.0, state.meanIntervalMillis(), 0.0001);
        assertEquals(0.0, state.stddevIntervalMillis(), 0.0001);
    }

    // Uneven spacing must produce a non-zero deviation; this is the signal that
    // separates a steady poll from bursty traffic.
    @Test
    void unevenSpacingProducesANonZeroDeviation() {
        RecordTimingState state = RecordTimingState.empty()
            .observe(T0)
            .observe(T0.plusMillis(100))
            .observe(T0.plusMillis(1100));

        // Intervals are 100 ms and 1000 ms: mean 550, population stddev 450.
        assertEquals(550.0, state.meanIntervalMillis(), 0.0001);
        assertEquals(450.0, state.stddevIntervalMillis(), 0.0001);
    }

    // The whole point of Welford: state size is constant. Ten thousand records
    // must leave exactly the same footprint as three.
    @Test
    void stateIsBoundedRegardlessOfObservationCount() {
        RecordTimingState state = RecordTimingState.empty();
        for (int i = 0; i < 10_000; i++) {
            state = state.observe(T0.plusMillis(i * 10L));
        }

        assertEquals(10_000L, state.observationCount());
        assertEquals(10.0, state.meanIntervalMillis(), 0.0001);
        assertEquals(0.0, state.stddevIntervalMillis(), 0.0001);
    }

    // Zeek can deliver slightly out of order. A negative interval would corrupt
    // the running mean, so it is clamped rather than trusted.
    @Test
    void outOfOrderRecordsDoNotProduceNegativeIntervals() {
        RecordTimingState state = RecordTimingState.empty()
            .observe(T0.plusMillis(500))
            .observe(T0);

        assertTrue(state.meanIntervalMillis() >= 0.0,
            "an out-of-order record must not drive the mean interval negative");
    }
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `./mvnw test -pl modules/domain -am -Dtest=RecordTimingStateTest`
Expected: FAIL — compilation error, `RecordTimingState` does not exist.

- [ ] **Step 3: Write the implementation**

`modules/domain/src/main/java/io/netsecml/platform/domain/feature/RecordTimingState.java`:

```java
package io.netsecml.platform.domain.feature;

import java.time.Duration;
import java.time.Instant;

// Running inter-arrival statistics for a single (sensor, sourceIp) key.
//
// Deliberately NOT part of SourceWindowState: that type's serialized shape is
// shared with the frozen conn path, and widening it would change checkpoint
// state for a job that is already running.
//
// Uses Welford's online algorithm, so the footprint is a count, a mean and a
// sum-of-squared-differences no matter how many records pass through. The
// bounded-state invariant forbids keeping a timestamp history.
public final class RecordTimingState {

    private final Instant lastRecordTime;
    private final long observationCount;
    private final double meanMillis;
    private final double sumSquaredDifferences;

    private RecordTimingState(Instant lastRecordTime, long observationCount,
                              double meanMillis, double sumSquaredDifferences) {
        this.lastRecordTime = lastRecordTime;
        this.observationCount = observationCount;
        this.meanMillis = meanMillis;
        this.sumSquaredDifferences = sumSquaredDifferences;
    }

    public static RecordTimingState empty() {
        return new RecordTimingState(null, 0L, 0.0, 0.0);
    }

    // Folds one record's timestamp into the running statistics, returning a new
    // state. The first record establishes a baseline but contributes no interval,
    // because there is nothing preceding it to measure against.
    public RecordTimingState observe(Instant recordTime) {
        if (recordTime == null) {
            throw new IllegalArgumentException("recordTime must not be null");
        }
        if (lastRecordTime == null) {
            return new RecordTimingState(recordTime, 1L, 0.0, 0.0);
        }

        // Zeek can deliver slightly out of order. A negative interval would drag
        // the running mean below zero, so it is clamped to zero.
        long intervalMillis = Math.max(0L, Duration.between(lastRecordTime, recordTime).toMillis());

        // Welford: update the mean, then accumulate the squared difference using
        // both the old and new means. This is numerically stable where a naive
        // sum-of-squares is not.
        long nextCount = observationCount + 1L;
        long intervalCount = nextCount - 1L;
        double delta = intervalMillis - meanMillis;
        double nextMean = meanMillis + delta / intervalCount;
        double nextSumSquares = sumSquaredDifferences + delta * (intervalMillis - nextMean);

        return new RecordTimingState(recordTime, nextCount, nextMean, nextSumSquares);
    }

    public long observationCount() {
        return observationCount;
    }

    public double meanIntervalMillis() {
        return meanMillis;
    }

    // Population standard deviation over the intervals observed so far. Fewer
    // than two records means no interval exists, so the answer is zero rather
    // than undefined -- the feature must always be emittable.
    public double stddevIntervalMillis() {
        long intervalCount = observationCount - 1L;
        if (intervalCount < 1L) {
            return 0.0;
        }
        return Math.sqrt(sumSquaredDifferences / intervalCount);
    }
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `./mvnw test -pl modules/domain -am -Dtest=RecordTimingStateTest`
Expected: PASS, 5 tests run.

- [ ] **Step 5: Commit**

```bash
git add modules/domain/src/main/java/io/netsecml/platform/domain/feature/RecordTimingState.java \
        modules/domain/src/test/java/io/netsecml/platform/domain/feature/RecordTimingStateTest.java
git commit -m "feat(domain): add bounded inter-arrival statistics via Welford"
```

---

## Task 3: Freeze the common feature tier

The tier is shared by every per-protocol schema (spec §3), so its names and order are frozen once, here, and each later unit embeds them. Frozen in two places that must agree: a Java constant for the streaming side and a JSON contract for the Python side.

**Files:**
- Create: `modules/domain/src/main/java/io/netsecml/platform/domain/feature/CommonFeatureTierV1.java`
- Create: `contracts/features/common-feature-tier-v1.json`
- Test: `modules/domain/src/test/java/io/netsecml/platform/domain/feature/CommonFeatureTierV1Test.java`
- Test: `modules/adapter-kafka/src/test/java/io/netsecml/platform/adapter/kafka/sink/CommonFeatureTierContractDriftTest.java`

**Why the drift test lives in `adapter-kafka`, not `domain`:** the `domain` module's pom carries
only `junit-jupiter` — Jackson was deliberately removed from its test scope in commit `88285d6`,
and the Global Constraints forbid framework imports there. `adapter-kafka` already hosts
`StreamContractDriftTest`, which reads `contracts/` with Jackson and imports domain types, so the
drift check follows that established pattern. The three pure-Java assertions stay in `domain`.

**Interfaces:**
- Consumes: nothing.
- Produces: `CommonFeatureTierV1.FEATURE_NAMES` (`List<String>`, size 12), `CommonFeatureTierV1.FEATURE_COUNT` (`int` = 12), `CommonFeatureTierV1.SCHEMA_ID` (`String` = `"common-feature-tier-v1"`).

- [ ] **Step 1: Write the failing test**

`modules/domain/src/test/java/io/netsecml/platform/domain/feature/CommonFeatureTierV1Test.java`:

```java
package io.netsecml.platform.domain.feature;

import org.junit.jupiter.api.Test;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

// The common tier is shared by every per-protocol schema, so drift between the
// Java constant and the JSON contract would silently misalign the streaming side
// and the training side.
class CommonFeatureTierV1Test {

    // Order IS the contract: a feature's index is its position in the vector, so
    // reordering is a breaking change even though the set is unchanged.
    @Test
    void featureOrderIsFrozen() {
        assertEquals(List.of(
            "record_count_5m",
            "byte_sum_5m",
            "failed_count_5m",
            "inter_arrival_mean_ms",
            "inter_arrival_stddev_ms",
            "is_orig",
            "conn_orig_bytes",
            "conn_resp_bytes",
            "conn_orig_pkts",
            "conn_resp_pkts",
            "conn_age_seconds",
            "conn_enrichment_present"
        ), CommonFeatureTierV1.FEATURE_NAMES);
    }

    @Test
    void featureCountMatchesTheNameList() {
        assertEquals(12, CommonFeatureTierV1.FEATURE_COUNT);
        assertEquals(CommonFeatureTierV1.FEATURE_NAMES.size(), CommonFeatureTierV1.FEATURE_COUNT);
    }

    // The list is handed to callers that build vectors; an accidental mutation
    // would corrupt every schema that embeds this tier.
    @Test
    void featureNamesAreImmutable() {
        assertThrows(UnsupportedOperationException.class,
            () -> CommonFeatureTierV1.FEATURE_NAMES.add("injected"));
    }
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `./mvnw test -pl modules/domain -am -Dtest=CommonFeatureTierV1Test`
Expected: FAIL — compilation error, `CommonFeatureTierV1` does not exist.

- [ ] **Step 3: Write the contract file**

`contracts/features/common-feature-tier-v1.json`:

```json
{
  "id": "common-feature-tier-v1",
  "semanticVersion": "1.0.0",
  "description": "Protocol-agnostic feature tier shared by every per-protocol feature schema. Embedded as the leading block of each <log-type>-feature-schema-v1. Order is frozen; a change creates common-feature-tier-v2.",
  "encoding": "float32",
  "features": [
    { "name": "record_count_5m", "index": 0, "description": "Records seen for this (sensor, sourceIp) key in the 5-minute rolling window." },
    { "name": "byte_sum_5m", "index": 1, "description": "Byte total for this key in the 5-minute rolling window." },
    { "name": "failed_count_5m", "index": 2, "description": "Failed records for this key in the 5-minute rolling window." },
    { "name": "inter_arrival_mean_ms", "index": 3, "description": "Running mean interval between records for this key, in milliseconds." },
    { "name": "inter_arrival_stddev_ms", "index": 4, "description": "Running population standard deviation of the inter-arrival interval, in milliseconds." },
    { "name": "is_orig", "index": 5, "description": "1 if the record was sent by the connection originator, else 0." },
    { "name": "conn_orig_bytes", "index": 6, "description": "Originator bytes since the previous conn.log snapshot. 0 when no snapshot is available." },
    { "name": "conn_resp_bytes", "index": 7, "description": "Responder bytes since the previous conn.log snapshot. 0 when no snapshot is available." },
    { "name": "conn_orig_pkts", "index": 8, "description": "Originator packets since the previous conn.log snapshot. 0 when no snapshot is available." },
    { "name": "conn_resp_pkts", "index": 9, "description": "Responder packets since the previous conn.log snapshot. 0 when no snapshot is available." },
    { "name": "conn_age_seconds", "index": 10, "description": "Connection age at the time of the latest conn.log snapshot, in seconds. 0 when no snapshot is available." },
    { "name": "conn_enrichment_present", "index": 11, "description": "1 if a conn.log snapshot was available for this record, else 0. Distinguishes a genuinely idle connection from one whose first snapshot has not yet been emitted." }
  ]
}
```

- [ ] **Step 4: Write the Java constant**

`modules/domain/src/main/java/io/netsecml/platform/domain/feature/CommonFeatureTierV1.java`:

```java
package io.netsecml.platform.domain.feature;

import java.util.List;

// The frozen names and order of the protocol-agnostic feature tier that leads
// every per-protocol feature schema.
//
// Order IS the contract: a feature's index is its position in the emitted
// vector, so reordering breaks every model trained against it even though the
// set of names is unchanged. A change creates common-feature-tier-v2; this file
// is never edited.
//
// The same list is published language-neutrally at
// contracts/features/common-feature-tier-v1.json, which the Python training
// project reads. CommonFeatureTierV1Test pins the two together.
public final class CommonFeatureTierV1 {

    public static final String SCHEMA_ID = "common-feature-tier-v1";

    // Indices 0-5 come from ml-platform's own keyed state and are always
    // populated. Indices 6-11 come from conn.log enrichment, which is a
    // non-blocking left join, so they are zero when no snapshot has arrived --
    // index 11 is what makes that case distinguishable from genuine zeroes.
    public static final List<String> FEATURE_NAMES = List.of(
        "record_count_5m",
        "byte_sum_5m",
        "failed_count_5m",
        "inter_arrival_mean_ms",
        "inter_arrival_stddev_ms",
        "is_orig",
        "conn_orig_bytes",
        "conn_resp_bytes",
        "conn_orig_pkts",
        "conn_resp_pkts",
        "conn_age_seconds",
        "conn_enrichment_present");

    public static final int FEATURE_COUNT = FEATURE_NAMES.size();

    // Non-instantiable: every member is static.
    private CommonFeatureTierV1() {
    }
}
```

- [ ] **Step 5: Run the domain test to verify it passes**

Run: `./mvnw test -pl modules/domain -am -Dtest=CommonFeatureTierV1Test`
Expected: PASS, 3 tests run.

**If the contract test fails on the path**, check the working directory assumption: Maven runs a module's tests with that module's directory as CWD, and every module lives at `modules/<name>`, so `../..` reaches the repo root. This matches `ClickHouseTestSupport.repoPath`.

- [ ] **Step 6: Write the contract-drift test in `adapter-kafka`**

`modules/adapter-kafka/src/test/java/io/netsecml/platform/adapter/kafka/sink/CommonFeatureTierContractDriftTest.java`:

```java
package io.netsecml.platform.adapter.kafka.sink;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netsecml.platform.domain.feature.CommonFeatureTierV1;
import org.junit.jupiter.api.Test;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

// The Java constant and the shipped contract must not drift. Training reads the
// JSON; the online job reads the constant. If they disagree, feature i means two
// different things on the two sides and nothing errors anywhere.
//
// This lives here rather than in domain because domain's test classpath carries
// only junit-jupiter -- Jackson was deliberately removed from it -- and because
// StreamContractDriftTest in this same package already does exactly this job for
// the stream contracts.
class CommonFeatureTierContractDriftTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    // This module lives at modules/adapter-kafka, so the repo root is two up.
    private Path contract() {
        return Paths.get("..", "..", "contracts", "features", "common-feature-tier-v1.json");
    }

    @Test
    void javaConstantMatchesTheShippedContract() throws Exception {
        JsonNode root = MAPPER.readTree(contract().toFile());

        assertEquals(CommonFeatureTierV1.SCHEMA_ID, root.get("id").asText());

        List<String> fromContract = new ArrayList<>();
        root.get("features").forEach(feature -> fromContract.add(feature.get("name").asText()));

        assertEquals(CommonFeatureTierV1.FEATURE_NAMES, fromContract,
            "the contract file and the Java constant must list the same features in the same order");
    }

    // Index is the vector position, so a contract whose declared indices do not
    // ascend from zero would misplace every feature after the gap.
    @Test
    void contractIndicesAscendFromZero() throws Exception {
        JsonNode features = MAPPER.readTree(contract().toFile()).get("features");

        for (int i = 0; i < features.size(); i++) {
            assertEquals(i, features.get(i).get("index").asInt(),
                "feature at position " + i + " must declare index " + i);
        }
    }
}
```

- [ ] **Step 7: Run both test classes**

Run: `./mvnw test -pl modules/adapter-kafka -am -Dtest='CommonFeatureTierV1Test+CommonFeatureTierContractDriftTest'`
Expected: PASS, 5 tests run total (3 in domain, 2 in adapter-kafka). Confirm the real count — a
zero-test run reports BUILD SUCCESS.

- [ ] **Step 8: Commit**

```bash
git add modules/adapter-kafka/src/test/java/io/netsecml/platform/adapter/kafka/sink/CommonFeatureTierContractDriftTest.java \
        contracts/features/common-feature-tier-v1.json \
        modules/domain/src/main/java/io/netsecml/platform/domain/feature/CommonFeatureTierV1.java \
        modules/domain/src/test/java/io/netsecml/platform/domain/feature/CommonFeatureTierV1Test.java
git commit -m "feat(contracts): freeze the common feature tier shared by all protocols"
```

---

## Task 4: `CommonFeatureExtractor` — build the tier's 12 values

Turns the three state inputs into the frozen vector. Lives in `application` because it orchestrates domain types and holds no framework code.

The `conn.log` join is **non-blocking** (spec §6.2): an absent snapshot yields zeroes plus the `conn_enrichment_present` flag, never a wait.

**Files:**
- Create: `modules/application/src/main/java/io/netsecml/platform/application/feature/CommonFeatureExtractor.java`
- Test: `modules/application/src/test/java/io/netsecml/platform/application/feature/CommonFeatureExtractorTest.java`

**Interfaces:**
- Consumes: `SourceWindowState.connectionCount5m()`, `byteSum5m()`, `failedCount5m()`; `RecordTimingState.meanIntervalMillis()`, `stddevIntervalMillis()`; `ConnSnapshotDelta` and its `origBytes()`, `respBytes()`, `origPkts()`, `respPkts()`, `ageSeconds()`.
- Produces: `static float[] extract(SourceWindowState window, RecordTimingState timing, boolean isOrig, ConnSnapshotDelta enrichmentDelta)` returning exactly `CommonFeatureTierV1.FEATURE_COUNT` values. `enrichmentDelta` is nullable and null means "no snapshot available".

- [ ] **Step 1: Write the failing test**

`modules/application/src/test/java/io/netsecml/platform/application/feature/CommonFeatureExtractorTest.java`:

```java
package io.netsecml.platform.application.feature;

import io.netsecml.platform.domain.feature.CommonFeatureTierV1;
import io.netsecml.platform.domain.feature.ConnSnapshotDelta;
import io.netsecml.platform.domain.feature.RecordTimingState;
import io.netsecml.platform.domain.feature.SourceWindowState;
import org.junit.jupiter.api.Test;
import java.time.Instant;
import static org.junit.jupiter.api.Assertions.*;

// The common tier's construction. Every per-protocol schema embeds this block,
// so an index error here would misalign all of them at once.
class CommonFeatureExtractorTest {

    private static final Instant START = Instant.parse("2026-09-10T10:00:00Z");

    private static SourceWindowState windowWithThreeRecords() {
        long minute = START.getEpochSecond() / 60L;
        return SourceWindowState.empty()
            .record(minute, 100L, false)
            .record(minute, 200L, true)
            .record(minute, 300L, false);
    }

    private static RecordTimingState evenlySpacedTiming() {
        return RecordTimingState.empty()
            .observe(START)
            .observe(START.plusMillis(100))
            .observe(START.plusMillis(200));
    }

    // The vector's width is the contract. A schema that embeds this tier computes
    // its own offsets from FEATURE_COUNT, so a wrong length corrupts every
    // protocol feature that follows it.
    @Test
    void producesExactlyTheFrozenFeatureCount() {
        float[] values = CommonFeatureExtractor.extract(
            windowWithThreeRecords(), evenlySpacedTiming(), true, null);

        assertEquals(CommonFeatureTierV1.FEATURE_COUNT, values.length);
    }

    // Indices 0-5 come from ml-platform's own state and are always present.
    @Test
    void mapsWindowAndTimingToTheirFrozenIndices() {
        float[] values = CommonFeatureExtractor.extract(
            windowWithThreeRecords(), evenlySpacedTiming(), true, null);

        assertEquals(3.0f, values[0], 0.0001f, "record_count_5m");
        assertEquals(600.0f, values[1], 0.0001f, "byte_sum_5m");
        assertEquals(1.0f, values[2], 0.0001f, "failed_count_5m");
        assertEquals(100.0f, values[3], 0.0001f, "inter_arrival_mean_ms");
        assertEquals(0.0f, values[4], 0.0001f, "inter_arrival_stddev_ms");
        assertEquals(1.0f, values[5], 0.0001f, "is_orig");
    }

    // The non-blocking left join: no snapshot means zeroes, and index 11 records
    // that they are absent rather than measured.
    @Test
    void absentEnrichmentYieldsZeroesAndClearsThePresenceFlag() {
        float[] values = CommonFeatureExtractor.extract(
            windowWithThreeRecords(), evenlySpacedTiming(), false, null);

        assertEquals(0.0f, values[6], 0.0001f, "conn_orig_bytes");
        assertEquals(0.0f, values[7], 0.0001f, "conn_resp_bytes");
        assertEquals(0.0f, values[8], 0.0001f, "conn_orig_pkts");
        assertEquals(0.0f, values[9], 0.0001f, "conn_resp_pkts");
        assertEquals(0.0f, values[10], 0.0001f, "conn_age_seconds");
        assertEquals(0.0f, values[11], 0.0001f, "conn_enrichment_present must be 0 when absent");
    }

    // A present snapshot populates 6-10 and sets the flag. Without the flag a
    // model could not tell a genuinely idle connection from one whose first
    // snapshot has not been emitted yet.
    @Test
    void presentEnrichmentPopulatesItsIndicesAndSetsTheFlag() {
        ConnSnapshotDelta delta = new ConnSnapshotDelta(500L, 600L, 4L, 7L, 600L);

        float[] values = CommonFeatureExtractor.extract(
            windowWithThreeRecords(), evenlySpacedTiming(), true, delta);

        assertEquals(500.0f, values[6], 0.0001f, "conn_orig_bytes");
        assertEquals(600.0f, values[7], 0.0001f, "conn_resp_bytes");
        assertEquals(4.0f, values[8], 0.0001f, "conn_orig_pkts");
        assertEquals(7.0f, values[9], 0.0001f, "conn_resp_pkts");
        assertEquals(600.0f, values[10], 0.0001f, "conn_age_seconds");
        assertEquals(1.0f, values[11], 0.0001f, "conn_enrichment_present must be 1 when present");
    }

    // A zero-valued snapshot is NOT the same as an absent one, and the flag is
    // the only thing that distinguishes them. This is the case the flag exists
    // for, so it is asserted directly rather than implied.
    @Test
    void aZeroValuedSnapshotIsDistinguishableFromAnAbsentOne() {
        ConnSnapshotDelta idle = new ConnSnapshotDelta(0L, 0L, 0L, 0L, 600L);

        float[] present = CommonFeatureExtractor.extract(
            windowWithThreeRecords(), evenlySpacedTiming(), true, idle);
        float[] absent = CommonFeatureExtractor.extract(
            windowWithThreeRecords(), evenlySpacedTiming(), true, null);

        assertEquals(0.0f, present[6], 0.0001f);
        assertEquals(0.0f, absent[6], 0.0001f);
        assertNotEquals(present[11], absent[11],
            "an idle connection and a missing snapshot must not look identical");
    }

    // is_orig is a direction flag, not a count. Both values are pinned so a
    // boolean-to-float slip cannot pass.
    @Test
    void isOrigIsEncodedAsZeroOrOne() {
        float[] originator = CommonFeatureExtractor.extract(
            windowWithThreeRecords(), evenlySpacedTiming(), true, null);
        float[] responder = CommonFeatureExtractor.extract(
            windowWithThreeRecords(), evenlySpacedTiming(), false, null);

        assertEquals(1.0f, originator[5], 0.0001f);
        assertEquals(0.0f, responder[5], 0.0001f);
    }
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `./mvnw test -pl modules/application -am -Dtest=CommonFeatureExtractorTest`
Expected: FAIL — compilation error, `CommonFeatureExtractor` does not exist.

- [ ] **Step 3: Write the implementation**

`modules/application/src/main/java/io/netsecml/platform/application/feature/CommonFeatureExtractor.java`:

```java
package io.netsecml.platform.application.feature;

import io.netsecml.platform.domain.feature.CommonFeatureTierV1;
import io.netsecml.platform.domain.feature.ConnSnapshotDelta;
import io.netsecml.platform.domain.feature.RecordTimingState;
import io.netsecml.platform.domain.feature.SourceWindowState;

// Builds the protocol-agnostic feature tier that leads every per-protocol
// feature vector.
//
// Indices here are the frozen positions declared in CommonFeatureTierV1, and
// every per-protocol schema appends its own features after FEATURE_COUNT. An
// index error in this class therefore misaligns every protocol at once, which
// is why each position is asserted individually in the tests.
public final class CommonFeatureExtractor {

    // Non-instantiable: every member is static.
    private CommonFeatureExtractor() {
    }

    // enrichmentDelta is NULLABLE and null means "no conn.log snapshot available
    // for this connection yet". The join is deliberately non-blocking: a
    // connection's first snapshot does not exist until it has been alive five
    // minutes, and waiting for it would stall every record from a new connection.
    public static float[] extract(SourceWindowState window, RecordTimingState timing,
                                  boolean isOrig, ConnSnapshotDelta enrichmentDelta) {
        if (window == null || timing == null) {
            throw new IllegalArgumentException("window and timing must not be null");
        }

        float[] values = new float[CommonFeatureTierV1.FEATURE_COUNT];

        // Indices 0-2: the rolling window this platform already maintains per
        // (sensor, sourceIp). Always populated.
        values[0] = window.connectionCount5m();
        values[1] = window.byteSum5m();
        values[2] = window.failedCount5m();

        // Indices 3-4: inter-arrival shape. A steady poll loop and a burst have
        // the same record count but very different deviation.
        values[3] = (float) timing.meanIntervalMillis();
        values[4] = (float) timing.stddevIntervalMillis();

        // Index 5: direction, as a flag rather than a count.
        values[5] = isOrig ? 1.0f : 0.0f;

        // Indices 6-11: conn.log enrichment. Absent is not an error -- it is the
        // expected state for the first five minutes of every connection -- so the
        // features default to zero and index 11 records that they were not
        // measured. Without that flag an idle connection and a missing snapshot
        // would be indistinguishable to the model.
        if (enrichmentDelta == null) {
            values[11] = 0.0f;
            return values;
        }

        values[6] = enrichmentDelta.origBytes();
        values[7] = enrichmentDelta.respBytes();
        values[8] = enrichmentDelta.origPkts();
        values[9] = enrichmentDelta.respPkts();
        values[10] = enrichmentDelta.ageSeconds();
        values[11] = 1.0f;
        return values;
    }
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `./mvnw test -pl modules/application -am -Dtest=CommonFeatureExtractorTest`
Expected: PASS, 6 tests run.

- [ ] **Step 5: Commit**

```bash
git add modules/application/src/main/java/io/netsecml/platform/application/feature/CommonFeatureExtractor.java \
        modules/application/src/test/java/io/netsecml/platform/application/feature/CommonFeatureExtractorTest.java
git commit -m "feat(application): build the common feature tier from window, timing and conn enrichment"
```

---

## Task 5: `ArchiveJob` chain loop

Spec §6.3. Today `build` wires two hand-written chains. At six log types that becomes twelve. This replaces them with a loop over a registration list so adding a log type is one line, and derives each operator's `.uid()` from its topic so a topology change cannot silently invalidate checkpoint state.

**Behaviour must be unchanged.** The existing `ArchiveJobE2ETest` and `ClickHouseOutageTest` are the regression gate: same two chains, same operator UIDs, same table names.

**Files:**
- Modify: `modules/bootstrap-archive-job/src/main/java/io/netsecml/platform/bootstrap/archive/ArchiveJob.java`
- Test: `modules/bootstrap-archive-job/src/test/java/io/netsecml/platform/bootstrap/archive/ArchiveJobTopologyTest.java`

**Interfaces:**
- Consumes: `FeatureVectorRowMapFunction(String topic)`, `InvalidEventRowMapFunction(String topic)`, `ClickHouseBatchSink<T>(String table, ClickHouseConfig config)`.
- Produces: `ArchiveJob.build(StreamExecutionEnvironment, String bootstrapServers, String featureVectorTopic, String dlqTopic, ClickHouseConfig)` — signature **unchanged**, so no caller or test is affected.

- [ ] **Step 1: Write the failing test**

`modules/bootstrap-archive-job/src/test/java/io/netsecml/platform/bootstrap/archive/ArchiveJobTopologyTest.java`:

```java
package io.netsecml.platform.bootstrap.archive;

import io.netsecml.platform.adapter.clickhouse.writer.ClickHouseConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;
import java.util.HashSet;
import java.util.Set;
import static org.junit.jupiter.api.Assertions.*;

// The chain loop's structure, asserted without Docker.
//
// getStreamGraph() builds the job graph in memory, so the topology can be
// inspected with no Kafka and no ClickHouse. That matters: the container tests
// that would otherwise cover this are the slowest in the project and one of them
// cannot run on a small machine at all.
class ArchiveJobTopologyTest {

    private static StreamExecutionEnvironment buildJob() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        ArchiveJob.build(env, "localhost:9092", "netsec.conn.feature-vector.v1", "netsec.conn.dlq.v1",
            ClickHouseConfig.of("localhost", 8123, "netsec_ml", "default", "test-password"));
        return env;
    }

    // Two log types in, two source-to-sink chains out. This is what must scale to
    // six without the method growing.
    @Test
    void buildsOneChainPerRegisteredLogType() {
        Set<String> uids = operatorUids();

        assertTrue(uids.contains("feature-vector-source"), "feature vector chain must be present");
        assertTrue(uids.contains("dlq-source"), "dlq chain must be present");
    }

    // Every operator needs a stable uid or Flink derives one from the topology
    // hash, and any later edit silently discards checkpoint state instead of
    // failing loudly. The loop must not drop these.
    @Test
    void everyOperatorCarriesAnExplicitUid() {
        Set<String> uids = operatorUids();

        assertTrue(uids.containsAll(Set.of(
            "feature-vector-source", "feature-vector-row", "feature-vectors-clickhouse-sink",
            "dlq-source", "invalid-event-row", "invalid-events-clickhouse-sink")),
            "expected the six pre-existing operator uids, found: " + uids);
    }

    // Uids must be unique across chains. Two operators sharing one is how a
    // six-log-type loop silently collides state, and it is exactly the failure a
    // hand-written topology cannot have but a generated one can.
    @Test
    void operatorUidsAreUniqueAcrossChains() {
        StreamExecutionEnvironment env = buildJob();

        Set<String> seen = new HashSet<>();
        env.getStreamGraph(false).getStreamNodes().forEach(node -> {
            String uid = node.getTransformationUID();
            if (uid != null) {
                assertTrue(seen.add(uid), "duplicate operator uid across chains: " + uid);
            }
        });
    }

    private static Set<String> operatorUids() {
        StreamExecutionEnvironment env = buildJob();
        Set<String> uids = new HashSet<>();
        env.getStreamGraph(false).getStreamNodes().forEach(node -> {
            if (node.getTransformationUID() != null) {
                uids.add(node.getTransformationUID());
            }
        });
        return uids;
    }
}
```

- [ ] **Step 2: Run the test to verify the current topology already satisfies it**

Run: `./mvnw test -pl modules/bootstrap-archive-job -am -Dtest=ArchiveJobTopologyTest`
Expected: **PASS, 3 tests run.** This test characterises the *existing* behaviour before the refactor, so it passes first and then guards the change. If it fails, the assumption about `getStreamGraph` is wrong — fix the test against the real API before touching `ArchiveJob`.

- [ ] **Step 3: Refactor `build` into a loop**

Replace the body of `build` in `modules/bootstrap-archive-job/src/main/java/io/netsecml/platform/bootstrap/archive/ArchiveJob.java`, keeping the signature and the class comment above it:

```java
    // One archive chain: a Kafka topic, the map function that turns its raw bytes
    // into a ClickHouse row, the target table, and the three operator uids.
    //
    // The uids are stored explicitly rather than derived from the chain name.
    // They are checkpoint state identity, and the two pre-existing chains named
    // their operators inconsistently -- "feature-vector-source" but
    // "invalid-event-row" under a "dlq" source, and a pluralised
    // "feature-vectors-clickhouse-sink". Any rule that generated those names
    // would be more intricate than the names themselves, so they are data.
    private record ChainSpec<T>(String topic, RichMapFunction<byte[], T> rowMapper, String table,
                                String sourceUid, String mapUid, String sinkUid) {
    }

    public static void build(StreamExecutionEnvironment env, String bootstrapServers,
                             String featureVectorTopic, String dlqTopic, ClickHouseConfig clickHouse) {

        // Every chain is identical in shape: source -> map -> ClickHouse sink.
        // Registering them as data rather than writing each one out keeps this
        // method the same size at six log types as at two. Adding a log type is a
        // new entry in this list.
        List<ChainSpec<?>> chains = List.of(
            // Chain 1 -- feature vectors. The required Day 6 path: a versioned
            // vector observable in Kafka must become queryable in ClickHouse.
            new ChainSpec<>(featureVectorTopic,
                new FeatureVectorRowMapFunction(featureVectorTopic), "feature_vectors",
                "feature-vector-source", "feature-vector-row", "feature-vectors-clickhouse-sink"),
            // Chain 2 -- rejected records. Low volume, and duplicates after a
            // replay are expected rather than prevented.
            new ChainSpec<>(dlqTopic,
                new InvalidEventRowMapFunction(dlqTopic), "invalid_events",
                "dlq-source", "invalid-event-row", "invalid-events-clickhouse-sink"));

        for (ChainSpec<?> chain : chains) {
            wire(env, bootstrapServers, clickHouse, chain);
        }
    }

    // Wires one chain. Generic so the row type flows from the map function to the
    // sink without a cast.
    //
    // Every operator gets its explicit, stable uid. Without one Flink derives the
    // operator ID from the topology hash, so ANY future edit to this graph
    // silently discards state on restore rather than failing loudly -- and a
    // generated topology can collide uids in a way a hand-written one cannot,
    // which ArchiveJobTopologyTest guards.
    private static <T> void wire(StreamExecutionEnvironment env, String bootstrapServers,
                                 ClickHouseConfig clickHouse, ChainSpec<T> chain) {
        env.fromSource(source(bootstrapServers, chain.topic()),
                WatermarkStrategy.noWatermarks(), chain.sourceUid())
            .uid(chain.sourceUid())
            .map(chain.rowMapper())
            .name(chain.mapUid())
            .uid(chain.mapUid())
            .sinkTo(new ClickHouseBatchSink<T>(chain.table(), clickHouse))
            .name(chain.sinkUid())
            .uid(chain.sinkUid());
    }
```

Add these imports to the file:

```java
import org.apache.flink.api.common.functions.RichMapFunction;
import java.util.List;
```

- [ ] **Step 4: Run the topology test to verify the refactor preserved every uid**

Run: `./mvnw test -pl modules/bootstrap-archive-job -am -Dtest=ArchiveJobTopologyTest`
Expected: PASS, 3 tests run — the same six uids as before the refactor.

If `everyOperatorCarriesAnExplicitUid` fails, a `ChainSpec` entry carries a different uid than the original topology used. **Do not change the test to match the code** — the historical uids are checkpoint state identity. Fix the `ChainSpec` entry.

- [ ] **Step 5: Run the archive module's full suite**

Run: `docker ps -aq | xargs -r docker rm -f; ./mvnw test -pl modules/bootstrap-archive-job -am`
Expected: `ArchiveJobE2ETest` 1/1 and `ArchiveJobTopologyTest` 3/3 pass. `ClickHouseOutageTest` is expected to be OOM-killed on a small machine — report it as unverified, never as passing.

- [ ] **Step 6: Commit**

```bash
git add modules/bootstrap-archive-job/src/main/java/io/netsecml/platform/bootstrap/archive/ArchiveJob.java \
        modules/bootstrap-archive-job/src/test/java/io/netsecml/platform/bootstrap/archive/ArchiveJobTopologyTest.java
git commit -m "refactor(bootstrap-archive-job): wire archive chains from a registration list"
```

---

## Task 6: Document the foundation

**Files:**
- Modify: `docs/clickhouse.md`
- Modify: `CLAUDE.md`

- [ ] **Step 1: Add a common-tier section to `docs/clickhouse.md`**

Insert after the feature-count section:

```markdown
## The common feature tier

Every per-protocol feature schema begins with the same 12 protocol-agnostic
features, frozen at `contracts/features/common-feature-tier-v1.json` and mirrored
in `CommonFeatureTierV1`. A protocol's own features start at index 12.

Indices 0-5 come from the online job's own keyed state and are always populated.
Indices 6-11 come from `conn.log` enrichment, which is a **non-blocking left
join**: a connection's first `conn.log` snapshot does not exist until it has been
alive five minutes, so those features are zero until one arrives. Index 11,
`conn_enrichment_present`, is what distinguishes a genuinely idle connection from
one whose snapshot has not yet been emitted — without it the two are identical to
a model.

`conn.log` counters are cumulative, so indices 6-9 carry the *delta* between
consecutive snapshots rather than the raw totals.
```

- [ ] **Step 2: Update the implementation-state section of `CLAUDE.md`**

Add to the "Not yet implemented" paragraph:

```markdown
The common feature tier (`contracts/features/common-feature-tier-v1.json`) and
its `conn.log` enrichment carrier are implemented, but no protocol consumes them
yet — `conn-feature-v1` predates the tier and is frozen without it. The first
consumer is the DNS unit.
```

- [ ] **Step 3: Verify no factual drift**

Re-read both edits against the code: 12 features, index 11 is the presence flag, the contract path exists, and `conn-feature-v1` is unchanged. Do not describe any container-backed test as passing.

- [ ] **Step 4: Commit**

```bash
git add docs/clickhouse.md CLAUDE.md
git commit -m "docs: describe the common feature tier and its conn.log enrichment"
```

---

## Self-Review

**Spec coverage.** §3.1 common tier → Tasks 1-4. §6.2 non-blocking enrichment → Tasks 1 and 4 (the domain carrier and the null-means-absent contract); the Flink state wiring is deliberately **not** here, because nothing consumes the tier until the DNS unit and a source with no consumer could not be tested end-to-end. §6.3 chain fan-out and per-log-type uids → Task 5. §12's "changes no existing schema" → verified by `conn-feature-v1` and `SourceWindowState` being untouched throughout.

**Deliberate deferral.** Spec §6.1's protocol-internal merge is S7comm-only and belongs to Unit 7, not here.

**Type consistency.** `ConnSnapshotDelta` accessors (`origBytes`, `respBytes`, `origPkts`, `respPkts`, `ageSeconds`) are produced by Task 1's `deltaFrom` and consumed identically in Task 4. `RecordTimingState.meanIntervalMillis`/`stddevIntervalMillis` match between Tasks 2 and 4. `CommonFeatureTierV1.FEATURE_COUNT` is used in Tasks 3 and 4. `SourceWindowState.connectionCount5m`/`byteSum5m`/`failedCount5m` match the existing class exactly.

**Known risk carried into Task 5.** The two pre-existing chains name their operators inconsistently (`feature-vector-source` but `invalid-event-row` under a `dlq` source, and a pluralised `feature-vectors-clickhouse-sink`). `ChainSpec` stores all three uids explicitly rather than deriving them, because a uid change orphans checkpoint state and any generating rule would be more intricate than the six literal names. Task 5 Step 4 explicitly forbids fixing the test instead of the code. A reviewer may reasonably dislike the inconsistent names; a reviewer must not propose renaming them.

**API verified, not assumed.** `StreamExecutionEnvironment.getStreamGraph(boolean)`, `StreamGraph.getStreamNodes()` returning `Collection<StreamNode>`, and `StreamNode.getTransformationUID()` were each confirmed with `javap` against `flink-runtime-2.2.1.jar` before Task 5's test was written. In Flink 2.x these classes live in `flink-runtime`, not `flink-streaming-java`.
