# Sealed Event Hierarchy Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the conn-shaped `NetworkEvent` record with a sealed hierarchy, so a protocol that is not a TCP connection can be represented at all.

**Architecture:** `NetworkEvent` becomes a sealed interface over a shared `EventEnvelope`, with `ConnEvent` as its only implementation. The interface declares the five envelope accessors as `default` methods, so the fourteen files that use only shared fields keep compiling untouched; only the ten that touch conn-specific state or construct events change.

**Tech Stack:** Java 21 (sealed interfaces, records, pattern-matching switch), Flink 2.2.1, JUnit 5.

**Spec:** `docs/superpowers/specs/2026-09-04-multi-protocol-feature-schema-design.md` — decision **D5** and §5.2. Also constrained by `docs/superpowers/specs/2026-09-10-per-protocol-feature-schemas-design.md` §12, whose Unit 3 (DNS) cannot begin until this lands.

**Base branch:** `feat/common-feature-tier` — which carries Units 1 and 2 and is itself unmerged.

---

## Why this plan exists, and why it is not Unit 3

§12 of the per-protocol spec lists Unit 3 as **DNS**: "best coverage of the six, single log, no merge — proves the whole per-protocol pattern at the lowest risk." That ordering is right, but it has an unmet prerequisite that the spec does not mention, because it belongs to the *earlier* spec.

**Decision D5 of the 2026-09-04 redesign was never implemented.** That spec says:

> `NetworkEvent` becomes a **sealed interface** with a shared `EventEnvelope` and one record per log type

and sketches:

```java
public sealed interface NetworkEvent permits ConnEvent {   // grows per protocol
public record ConnEvent(EventEnvelope envelope, ConnClassification classification, …)
        implements NetworkEvent { }
```

The code still has `NetworkEvent` as a plain record whose compact constructor **requires** `connection`, `measurements` and `locality` to be non-null — all conn concepts. A `dns.log` record has none of them: duration and byte counts live in `conn.log`, not `dns.log`. So DNS cannot produce a `NetworkEvent` without fabricating connection measurements, which is exactly the "generic event" `PILOT_ARCHITECTURE` rejects.

Parts of the 09-04 redesign did land — `LogType`, `connectionUid`, the widened `FeatureVector`. D5 did not. This plan closes that gap and nothing else.

**Verified blast radius** (by grep, before writing any task):

| | Files |
|---|---|
| Mention `NetworkEvent` | 19 |
| Use conn-specific accessors (`connection()`, `measurements()`, `locality()`) | **5** |
| Construct `new NetworkEvent(...)` | **5** (4 tests + `EventMapper`) |

The other fourteen touch only shared fields. Declaring those five accessors as `default` methods on the interface is what keeps them compiling, and is why this is a tractable unit rather than a hexagon-wide rewrite.

---

## A SECOND prerequisite, found while writing this plan — DNS needs its own unit before it

The sealed hierarchy is not the only thing standing between here and DNS. While checking whether the
spec's synthetic-schema proof was viable, I found the shared feature-building path is conn-bound in
**five** separate ways. `BuildFeaturesUseCaseImpl.build` (lines 35-68) does all of this:

```java
        long totalBytes = event.measurements().originBytes() + …   // conn-only accessor
        boolean failed  = event.connection().connectionState()…    // conn-only accessor
        float[] values = new float[20];                            // hardcoded length
        System.arraycopy(eventLevel, 0, values, 0, 17);            // hardcoded split
        values[17] = newState.connectionCount5m();                 // hardcoded indices
        …
            ConnFeatureSchemaV1.SCHEMA.id(),                       // hardcoded schema
            ConnFeatureSchemaV1.CONTENT_HASH,
```

`FeatureSchema` already exposes `featureCount()`, so the schema is capable of any width — the use
case simply ignores it. A DNS vector is 12 common + 13 protocol = **25 values**; it would be
allocated at 20 and silently truncated, with `ConnFeatureSchemaV1`'s id and content hash stamped on
it.

**Unit 1's `CommonFeatureExtractor` also has no consumer.** The whole-branch review of that unit
noted no production code referenced the new types; this is where they were supposed to land, and
they never did.

So the path to DNS is three units, not one:

| | Unit | Why separate |
|---|---|---|
| **3a** | The sealed event hierarchy — **this plan** | Representation only, no behaviour change, self-contained and reviewable on its own |
| **3b** | A log-type-generic feature build path | Replace the hardcoded 20/17/19 and `ConnFeatureSchemaV1` with the schema's own `featureCount()` and the common tier; prove it with a synthetic `FeatureSchema` at a width other than 20, which is the 09-04 spec's proof #1 and *is* viable in test sources |
| **3c** | DNS itself | The parser, mapper, event record, feature schema and fixtures |

This plan is 3a only. Attempting all three at once would produce a diff spanning a domain refactor,
an application rewrite and a new protocol, where a reviewer could not reject one part while
approving another.

---

## Global Constraints

- **Java package root:** `io.netsecml.platform`. Dependency chain is one-way: `domain → ports → application → adapters → bootstrap`. `domain` imports no framework code — no Kafka, Flink, ClickHouse, Jackson. Adapters never import each other.
- **`permits` lists only implemented log types.** Today that is `ConnEvent` alone. Do **not** add a `DnsEvent`, `HttpEvent` or any other record for a protocol with no parser behind it — the same rule `LogType` states about its own constants. The compiler then flags every non-exhaustive `switch` when a real one arrives.
- **Behaviour must not change.** This is a representation refactor. Every existing test must pass unmodified except where it constructs an event or reads a conn-specific field, and those changes are mechanical.
- **`contracts/` and `infrastructure/clickhouse/ddl/` are immutable.** This plan touches neither. `conn-feature-v1`'s content hash stays `f42fb1bebb2efe3acc5de634c6a7bb3d6f97fc021207f75d77652533b1c01e1b`.
- **Flink operator uids are checkpoint state identity** — none of them change here.
- **Inline comments describing each block are mandatory** — a standing user requirement. A comment that misdescribes the code is worse than no comment.
- **Build in stages.** `./mvnw clean verify` is OOM-killed on a 5.7 GiB machine. Run `./mvnw install -DskipTests -q -o` once, then per module.
- **Never combine `-am` with `-Dtest=`.** Surefire fails hard on the first upstream module that has tests but none matching the pattern, and the suppression flag is forbidden because it reports BUILD SUCCESS on zero tests. Install first, then run a module alone without `-am`.
- **`ClickHouseOutageTest` is OOM-killed on this machine** — never run it, never claim it passes.
- **Commit with explicit paths only** — never `git add -A`, `git add .`, or a bare directory.
- **Commit trailer:**
  ```
  Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
  ```

---

## File Structure

| File | Responsibility |
|---|---|
| `modules/domain/.../event/EventEnvelope.java` | The identity and timing block every log type shares: who, when, which log, which connection. |
| `modules/domain/.../event/NetworkEvent.java` | Becomes a sealed interface over that envelope, with `default` accessors delegating to it. |
| `modules/domain/.../event/ConnEvent.java` | The conn record: envelope plus the three conn-only components. |
| `modules/domain/src/test/.../event/NetworkEventSurfaceTest.java` | Pins that the shared interface exposes only envelope-derived accessors, stays sealed, and permits only implemented log types. |

---

## Task 1: `EventEnvelope` — what every log type shares

**Files:**
- Create: `modules/domain/src/main/java/io/netsecml/platform/domain/event/EventEnvelope.java`
- Test: `modules/domain/src/test/java/io/netsecml/platform/domain/event/EventEnvelopeTest.java`

**Interfaces:**
- Consumes: `EventId`, `SensorId`, `LogType` — all existing in the same package.
- Produces: `record EventEnvelope(EventId eventId, Instant eventTime, SensorId sensor, LogType logType, String connectionUid)`.

This is purely additive — nothing references it yet, so the build stays green.

- [ ] **Step 1: Write the failing test**

`modules/domain/src/test/java/io/netsecml/platform/domain/event/EventEnvelopeTest.java`:

```java
package io.netsecml.platform.domain.event;

import org.junit.jupiter.api.Test;
import java.time.Instant;
import static org.junit.jupiter.api.Assertions.*;

// The identity block every log type carries, whatever its payload shape.
// These tests pin which fields are structural and which are optional, because
// every protocol record added later inherits exactly this contract.
class EventEnvelopeTest {

    private static final Instant WHEN = Instant.parse("2026-09-11T10:00:00Z");
    private static final SensorId SENSOR = new SensorId("sensor-eu-1");

    @Test
    void carriesIdentityTimingSensorAndLogType() {
        EventEnvelope envelope = new EventEnvelope(
            EventId.derive(SENSOR, "Cabc123XYZ"), WHEN, SENSOR, LogType.CONN, "Cabc123XYZ");

        assertEquals("sensor-eu-1:Cabc123XYZ", envelope.eventId().value());
        assertEquals(WHEN, envelope.eventTime());
        assertEquals(SENSOR, envelope.sensor());
        assertEquals(LogType.CONN, envelope.logType());
        assertEquals("Cabc123XYZ", envelope.connectionUid());
    }

    // eventId is the ClickHouse ORDER BY key tail and eventTime partitions the
    // table, so neither can be absent. A row cannot be archived without them.
    @Test
    void rejectsAMissingEventIdOrEventTime() {
        assertThrows(NullPointerException.class, () -> new EventEnvelope(
            null, WHEN, SENSOR, LogType.CONN, "Cabc"));
        assertThrows(NullPointerException.class, () -> new EventEnvelope(
            EventId.derive(SENSOR, "Cabc"), null, SENSOR, LogType.CONN, "Cabc"));
    }

    // logType is structural: every event must say which Zeek log produced it, or
    // the archive job cannot route it and the feature schema cannot be chosen.
    @Test
    void rejectsAMissingLogType() {
        assertThrows(NullPointerException.class, () -> new EventEnvelope(
            EventId.derive(SENSOR, "Cabc"), WHEN, SENSOR, null, "Cabc"));
    }

    // connectionUid is a CORRELATION key, not an identity, and some log types
    // legitimately have none. It normalises to "" rather than rejecting null, so
    // a protocol without a Zeek uid can still produce an envelope.
    @Test
    void normalisesAnAbsentConnectionUidToEmpty() {
        EventEnvelope envelope = new EventEnvelope(
            EventId.derive(SENSOR, "x"), WHEN, SENSOR, LogType.CONN, null);

        assertEquals("", envelope.connectionUid());
    }
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `./mvnw install -DskipTests -q -o && ./mvnw test -pl modules/domain -o -Dtest=EventEnvelopeTest`
Expected: FAIL — compilation error, `EventEnvelope` does not exist.

- [ ] **Step 3: Write the implementation**

`modules/domain/src/main/java/io/netsecml/platform/domain/event/EventEnvelope.java`:

```java
package io.netsecml.platform.domain.event;

import java.time.Instant;
import java.util.Objects;

// The identity block every log type shares, whatever the shape of its payload.
//
// Extracted so a protocol record carries only its own fields: a dns.log record
// has a uid and a timestamp like a conn record does, but none of a connection's
// duration or byte counts. Before this existed, NetworkEvent required all three
// conn components non-null, so a non-conn protocol could not be represented at
// all without fabricating them.
public record EventEnvelope(EventId eventId, Instant eventTime, SensorId sensor,
                            LogType logType, String connectionUid) {

    public EventEnvelope {
        // Identity and time are structural: eventId is the ClickHouse ORDER BY key
        // tail and eventTime is the partition key, so a row cannot be archived
        // without either.
        Objects.requireNonNull(eventId, "eventId must not be null");
        Objects.requireNonNull(eventTime, "eventTime must not be null");
        Objects.requireNonNull(sensor, "sensor must not be null");

        // Which Zeek log produced this. Without it the archive job cannot route the
        // event and the feature schema cannot be chosen.
        Objects.requireNonNull(logType, "logType must not be null");

        // connectionUid is a CORRELATION key, never an identity -- several dns or
        // http records legitimately share one uid. Some log types have no uid at
        // all, so an absent one normalises to "" rather than being rejected.
        connectionUid = connectionUid == null ? "" : connectionUid;
    }
}
```

- [ ] **Step 4: Run it to verify it passes**

Run: `./mvnw test -pl modules/domain -o -Dtest=EventEnvelopeTest`
Expected: PASS, 4 tests run.

- [ ] **Step 5: Commit**

```bash
git add modules/domain/src/main/java/io/netsecml/platform/domain/event/EventEnvelope.java \
        modules/domain/src/test/java/io/netsecml/platform/domain/event/EventEnvelopeTest.java
git commit -m "feat(domain): add EventEnvelope, the identity block every log type shares"
```

---

## Task 2: The flip — `NetworkEvent` becomes sealed, `ConnEvent` implements it

This is the atomic task. A sealed interface cannot be introduced incrementally: the moment `NetworkEvent` stops being a record, every construction site and every conn-specific accessor breaks at once. **All of them are fixed in this one commit**, so the build is green before and after and never in between.

**Files:**
- Create: `modules/domain/src/main/java/io/netsecml/platform/domain/event/ConnEvent.java`
- Modify: `modules/domain/src/main/java/io/netsecml/platform/domain/event/NetworkEvent.java` — record → sealed interface
- Modify (conn-specific accessors): `modules/application/.../usecase/BuildFeaturesUseCaseImpl.java`, `modules/application/.../feature/EventFeatureExtractor.java`, `modules/adapter-flink/.../process/SourceKeySelector.java`
- Modify (construction sites): `modules/adapter-kafka/.../mapper/EventMapper.java`
- Modify (tests that construct or read conn fields): `modules/domain/src/test/.../event/NetworkEventTest.java`, `modules/application/src/test/.../usecase/BuildFeaturesUseCaseImplTest.java`, `modules/application/src/test/.../feature/EventFeatureExtractorTest.java`, `modules/adapter-flink/src/test/.../process/ConnFeatureProcessFunctionTest.java`, `modules/adapter-kafka/src/test/.../mapper/EventMapperTest.java`

**Interfaces:**
- Consumes: `EventEnvelope` from Task 1; existing `ConnectionTuple`, `ConnectionMeasurements`, `ConnectionLocality`.
- Produces: `sealed interface NetworkEvent permits ConnEvent` declaring `envelope()` plus five `default` accessors; `record ConnEvent(EventEnvelope envelope, ConnectionTuple connection, ConnectionMeasurements measurements, ConnectionLocality locality) implements NetworkEvent`.

- [ ] **Step 1: Write the failing test**

Add to `modules/domain/src/test/java/io/netsecml/platform/domain/event/NetworkEventTest.java` (keep the existing tests; they will be adjusted in Step 3):

```java
    // The default accessors are what let the fourteen files that read only shared
    // fields keep compiling across this refactor. If they stopped delegating, every
    // one of those call sites would have to learn about ConnEvent.
    @Test
    void sharedAccessorsDelegateToTheEnvelope() {
        EventEnvelope envelope = new EventEnvelope(
            EventId.derive(new SensorId("sensor-eu-1"), "Cabc"),
            Instant.parse("2026-09-11T10:00:00Z"),
            new SensorId("sensor-eu-1"), LogType.CONN, "Cabc");

        NetworkEvent event = new ConnEvent(envelope, TUPLE, MEASUREMENTS, LOCALITY);

        assertEquals(envelope.eventId(), event.eventId());
        assertEquals(envelope.eventTime(), event.eventTime());
        assertEquals(envelope.sensor(), event.sensor());
        assertEquals(LogType.CONN, event.logType());
        assertEquals("Cabc", event.connectionUid());
    }

    // permits lists only implemented log types, so a switch over NetworkEvent is
    // exhaustive with a single case today. When a real second protocol lands, the
    // compiler flags every switch that did not grow with it -- which is the whole
    // reason for sealing rather than leaving the interface open.
    @Test
    void aSwitchOverTheHierarchyIsExhaustiveWithoutADefaultBranch() {
        NetworkEvent event = new ConnEvent(
            new EventEnvelope(EventId.derive(new SensorId("s"), "C"),
                Instant.parse("2026-09-11T10:00:00Z"), new SensorId("s"), LogType.CONN, "C"),
            TUPLE, MEASUREMENTS, LOCALITY);

        String described = switch (event) {
            case ConnEvent conn -> "conn:" + conn.connection().sourceIp();
        };

        assertTrue(described.startsWith("conn:"));
    }
```

Declare `TUPLE`, `MEASUREMENTS` and `LOCALITY` as private static finals in that test class, built the same way the existing tests build them — read the file and reuse its current values verbatim so the assertions stay comparable.

- [ ] **Step 2: Run it to verify it fails**

Run: `./mvnw install -DskipTests -q -o && ./mvnw test -pl modules/domain -o -Dtest=NetworkEventTest`
Expected: FAIL — `ConnEvent` does not exist and `NetworkEvent` cannot be used as a type with `new ConnEvent(...)`.

- [ ] **Step 3: Flip the domain types**

Replace `NetworkEvent.java` entirely:

```java
package io.netsecml.platform.domain.event;

import java.time.Instant;

// One parsed, validated Zeek record, whatever log produced it.
//
// A sealed interface rather than a record because the log types genuinely differ
// in shape: a conn record carries a connection's duration and byte counts, and a
// dns record carries none of those. Modelling that as one record with nullable
// conn fields is the "generic event" PILOT_ARCHITECTURE rejects -- every consumer
// would have to know which fields are meaningful for which log type, and the
// compiler could not help.
//
// permits lists ONLY implemented log types. A record is added when a protocol has
// a parser, a mapper and a feature schema behind it, never before -- the same
// rule LogType states about its own constants. Sealing then makes the compiler
// flag every non-exhaustive switch the moment a real second protocol arrives.
public sealed interface NetworkEvent permits ConnEvent {

    // Every log type carries the same identity block; only the payload differs.
    EventEnvelope envelope();

    // The shared fields are exposed directly, delegating to the envelope, so the
    // many call sites that read only identity and timing neither know nor care
    // that the hierarchy exists.
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

Create `ConnEvent.java`:

```java
package io.netsecml.platform.domain.event;

import java.util.Objects;

// A conn.log record: the shared envelope plus the three components only a
// connection has.
//
// These were fields on NetworkEvent itself until the hierarchy was sealed. They
// are required here, exactly as they were before, because a conn record without
// a tuple, measurements or locality is malformed -- but they are now required of
// ConnEvent alone, so a log type that has no such notion is not forced to invent
// them.
public record ConnEvent(EventEnvelope envelope, ConnectionTuple connection,
                        ConnectionMeasurements measurements, ConnectionLocality locality)
        implements NetworkEvent {

    public ConnEvent {
        Objects.requireNonNull(envelope, "envelope must not be null");

        // Unchanged from the pre-refactor NetworkEvent: a conn record is not
        // meaningful without all three.
        Objects.requireNonNull(connection, "connection must not be null");
        Objects.requireNonNull(measurements, "measurements must not be null");
        Objects.requireNonNull(locality, "locality must not be null");
    }
}
```

- [ ] **Step 4: Fix the five conn-specific readers and the construction sites**

These are mechanical. In `BuildFeaturesUseCaseImpl`, `EventFeatureExtractor` and `SourceKeySelector`, the parameter type stays `NetworkEvent` wherever only shared fields are read; where `connection()`, `measurements()` or `locality()` is called, switch on the event to reach `ConnEvent`:

```java
        // Conn-specific state lives on ConnEvent, not on the interface. A pattern
        // switch rather than a cast, so the compiler flags this site when a second
        // log type joins the hierarchy and this code has to decide what it means
        // for that type.
        ConnectionMeasurements measurements = switch (event) {
            case ConnEvent conn -> conn.measurements();
        };
```

In `EventMapper` and the four tests, replace `new NetworkEvent(a, b, c, d, e, f, g, h)` with:

```java
        new ConnEvent(new EventEnvelope(eventId, eventTime, sensor, logType, connectionUid),
            connection, measurements, locality)
```

keeping every argument value exactly as it was.

- [ ] **Step 5: Run the whole affected chain**

Run each and confirm the real counts:

```bash
./mvnw install -DskipTests -q -o
./mvnw test -pl modules/domain -o
./mvnw test -pl modules/application -o
./mvnw test -pl modules/adapter-kafka -o
./mvnw test -pl modules/adapter-flink -o
```

Expected: all green. Domain gains the 2 new tests; every other count is unchanged from before the refactor, because no behaviour changed. **If a count drops, a test was deleted rather than adjusted — restore it.**

- [ ] **Step 6: Commit**

```bash
git add modules/domain/src/main/java/io/netsecml/platform/domain/event/NetworkEvent.java \
        modules/domain/src/main/java/io/netsecml/platform/domain/event/ConnEvent.java \
        modules/domain/src/test/java/io/netsecml/platform/domain/event/NetworkEventTest.java \
        modules/application/src/main/java/io/netsecml/platform/application/usecase/BuildFeaturesUseCaseImpl.java \
        modules/application/src/main/java/io/netsecml/platform/application/feature/EventFeatureExtractor.java \
        modules/application/src/test/java/io/netsecml/platform/application/usecase/BuildFeaturesUseCaseImplTest.java \
        modules/application/src/test/java/io/netsecml/platform/application/feature/EventFeatureExtractorTest.java \
        modules/adapter-flink/src/main/java/io/netsecml/platform/adapter/flink/process/SourceKeySelector.java \
        modules/adapter-flink/src/test/java/io/netsecml/platform/adapter/flink/process/ConnFeatureProcessFunctionTest.java \
        modules/adapter-kafka/src/main/java/io/netsecml/platform/adapter/kafka/mapper/EventMapper.java \
        modules/adapter-kafka/src/test/java/io/netsecml/platform/adapter/kafka/mapper/EventMapperTest.java
git commit -m "refactor(domain): seal NetworkEvent over an envelope, add ConnEvent"
```

---

## Task 3: Pin that the shared interface exposes nothing conn-specific

The 09-04 spec asks for "a synthetic second event type implementing the sealed hierarchy, defined in
test sources only". **That is not achievable here, and I verified why rather than assuming.**

A sealed interface can only be implemented by types its `permits` clause names. Maven compiles
`src/main/java` and `src/test/java` as separate javac invocations, so a `permits` clause in main
sources naming a test-source type fails the main compile outright:

```
error: package TestHolder does not exist
public sealed interface Iface permits Impl, TestHolder.Probe {
error: invalid permits clause
```

I confirmed this with a throwaway compile: the two source sets compile fine **together**, and fail
when main is compiled alone, which is exactly what Maven does. The only ways around it are putting a
fake protocol record in main sources — which the `permits` rule forbids — or unsealing, which throws
away the exhaustiveness checking that motivates the whole design.

So this task proves the weaker property that *is* provable: that `NetworkEvent`'s own surface leaks
nothing conn-specific, so a future protocol record is not forced to satisfy a connection-shaped
contract.

**Files:**
- Test: `modules/domain/src/test/java/io/netsecml/platform/domain/event/NetworkEventSurfaceTest.java`

- [ ] **Step 1: Write the test**

```java
package io.netsecml.platform.domain.event;

import org.junit.jupiter.api.Test;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import static org.junit.jupiter.api.Assertions.*;

// The sealed hierarchy exists so a log type that is not a connection can be
// represented. That only holds if the shared interface demands nothing
// connection-shaped, so this pins its surface.
//
// The 09-04 spec asked for a synthetic second implementation in test sources to
// prove this instead. That cannot compile: a permits clause in main sources
// cannot name a test-source type, because Maven compiles the two source sets
// separately. This is the provable half of that intent -- weaker, because it
// constrains the interface rather than exercising a second shape, and saying so
// plainly matters more than implying the stronger proof is in place.
class NetworkEventSurfaceTest {

    // Exactly the envelope-derived accessors and nothing else. If someone adds a
    // measurements() or connection() method to the interface to save a pattern
    // switch at one call site, every future protocol record inherits an obligation
    // it cannot meaningfully satisfy -- and this fails.
    @Test
    void theInterfaceDeclaresOnlyEnvelopeDerivedAccessors() {
        Set<String> declared = Arrays.stream(NetworkEvent.class.getDeclaredMethods())
            .map(Method::getName)
            .collect(java.util.stream.Collectors.toSet());

        assertEquals(Set.of("envelope", "eventId", "eventTime", "sensor", "logType", "connectionUid"),
            declared, "NetworkEvent must expose only the shared envelope block");
    }

    // permits is the guest list, and it must name only log types with a parser,
    // mapper and feature schema behind them. A record added ahead of its
    // implementation lets code compile against a protocol that does not exist.
    @Test
    void permitsListsOnlyImplementedLogTypes() {
        List<String> permitted = Arrays.stream(NetworkEvent.class.getPermittedSubclasses())
            .map(Class::getSimpleName)
            .toList();

        assertEquals(List.of("ConnEvent"), permitted,
            "only conn has a parser, mapper and schema today; add a record when its protocol lands");
    }

    // Sealing is what makes the compiler flag an unhandled case later. An
    // accidentally non-sealed interface would compile identically today and fail
    // silently the day a second protocol arrives.
    @Test
    void theHierarchyIsActuallySealed() {
        assertTrue(NetworkEvent.class.isSealed(), "NetworkEvent must stay sealed");
    }
}
```

- [ ] **Step 2: Run it**

Run: `./mvnw install -DskipTests -q -o && ./mvnw test -pl modules/domain -o -Dtest=NetworkEventSurfaceTest`
Expected: PASS, 3 tests run.

- [ ] **Step 3: Commit**

```bash
git add modules/domain/src/test/java/io/netsecml/platform/domain/event/NetworkEventSurfaceTest.java
git commit -m "test(domain): pin that NetworkEvent's surface stays envelope-only"
```

---

## Task 4: Verify the online path still works end to end, and document

**Files:**
- Modify: `CLAUDE.md`
- Modify: `docs/conn-foundation-pipeline.md` if it describes `NetworkEvent`'s shape

- [ ] **Step 1: Run the full chain**

The refactor touches every layer between the Kafka parser and the feature vector, so the end-to-end test is what proves the wiring survived:

```bash
docker ps -aq | xargs -r docker rm -f
./mvnw install -DskipTests -q -o
./mvnw test -pl modules/bootstrap-online-job -o
```

Expected: `OnlineFeatureJobE2ETest` 1/1. Report the real count. Do **not** run `ClickHouseOutageTest`.

- [ ] **Step 2: Update `CLAUDE.md`'s architecture section**

Add to the Key invariants list:

```markdown
- `NetworkEvent` is a **sealed interface** over a shared `EventEnvelope`, with one record per log
  type. `permits` lists only log types that have a parser, mapper and feature schema — adding a
  record ahead of its implementation defeats the exhaustiveness checking that sealing buys.
```

- [ ] **Step 3: Check for stale descriptions**

Grep the docs for descriptions of `NetworkEvent` as a record with connection fields, and correct any found. If none exist, say so in the report rather than inventing an edit.

- [ ] **Step 4: Commit**

```bash
git add CLAUDE.md
git commit -m "docs: record the sealed event hierarchy invariant"
```

---

## Self-Review

**Spec coverage.** D5 of the 09-04 spec — "`NetworkEvent` becomes a sealed interface with a shared `EventEnvelope` and one record per log type" — is Tasks 1 and 2. Its §5.2 requirement that `permits` list only implemented log types is a Global Constraint and is enforced by the comment on the interface. The spec's explicit demand for "a synthetic second event type implementing the sealed hierarchy, defined in test sources only" is Task 3.

**What this plan deliberately does not do.** It adds no `DnsEvent`, no DNS parser and no DNS feature schema. Those are Unit 3 proper, which becomes writable the moment this lands. Mixing them would produce a single plan whose review surface spans a domain refactor and a new protocol, and a reviewer could not reject one while approving the other.

**Type consistency.** `EventEnvelope`'s five components match the five `default` accessors declared on `NetworkEvent` in Task 2 and the fields read by `SyntheticProbeEvent` in Task 3. `ConnEvent`'s three conn components are the same three `NetworkEvent` required before the flip, with the same non-null rules.

**The risk concentrated in Task 2.** It is one commit touching eleven files because a sealed interface cannot be introduced incrementally — the moment `NetworkEvent` stops being a record, every construction site breaks at once. Splitting it would leave the build red between tasks, which is worse than one larger reviewable diff. The mitigation is that every change in it is mechanical and every existing test must still pass at its original count; a dropped count means a test was deleted rather than adjusted.

**A risk I resolved by compiling rather than reasoning.** Task 3 originally called for a synthetic second event type in test sources, as the 09-04 spec asks. I compiled a throwaway probe: two source sets compile together, but main compiled **alone** — which is what Maven does — fails with `package TestHolder does not exist` / `invalid permits clause`. The spec's proof #2 is therefore unachievable without putting a fake protocol record in main sources, which the `permits` rule forbids. Task 3 now proves the weaker property that is provable and says so in the test's own comment, rather than implying the stronger proof is in place. The spec's proof #1 — a synthetic `FeatureSchema` at a width other than 20 — remains viable and belongs to Unit 3b, where the hardcoded width actually lives.
