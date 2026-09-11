# Generic Feature Machinery Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove the last places where the feature-building path assumes conn, so a protocol with a different feature count can be built at all.

**Architecture:** A schema registry resolves `LogType → FeatureSchema`, the use-case port becomes generic in both event and state type, and the vector's length comes from the schema rather than a literal. `conn`'s behaviour is unchanged throughout — its schema still reports 20, so it still gets 20.

**Tech Stack:** Java 21 (sealed interfaces, records, generics), Flink 2.2.1, JUnit 5.

**Spec:** `docs/superpowers/specs/2026-09-04-multi-protocol-feature-schema-design.md` §6 — this plan implements §6.1, §6.2, §6.3 and §6.5 exactly as written.

**Base branch:** `feat/common-feature-tier`, which carries Units 1, 2 and 3a.

---

## A correction to what this unit was previously described as doing

I earlier said 3b would give Unit 1's `CommonFeatureExtractor` its first consumer. **That is wrong, and checking the two contracts is what showed it.**

| | index 0 | index 1 | index 2 | total |
|---|---|---|---|---|
| `conn-feature-schema-v1.json` | `duration_ms` | `origin_bytes` | `response_bytes` | 20 |
| `common-feature-tier-v1.json` | `record_count_5m` | `byte_sum_5m` | `failed_count_5m` | 12 |

`conn-feature-v1` is frozen **without** the common tier and has an entirely different layout — `CLAUDE.md` says as much. Conn will never consume the tier; doing so would change a frozen contract. **The common tier's first consumer is DNS, in 3c.**

So this unit does not wire the tier to anything. It removes the obstacles that stop 3c from wiring it: the hardcoded width, the hardcoded schema, and a port that cannot express a second event type.

---

## What this unit deliberately does not do

**The `Endpoints` split is NOT in this plan**, despite my saying in 3a that it would land here.

Spec §5.1 gives `EventEnvelope` a sixth component, `Endpoints(sourceIp, sourcePort, destinationIp, destinationPort)`; 3a shipped five and recorded the deviation in `EventEnvelope`'s own comment. Folding it in here looked attractive because both touch `BuildFeaturesUseCaseImpl` and `EventFeatureExtractor` — but on sizing it, the overlap is 2 files of roughly 5, and the two changes have nothing else in common. This unit is about the *feature machinery*; the split is about *event shape*. Combining them would produce one diff where a reviewer could not reject the width change while approving the envelope change, or vice versa.

It remains recorded in the code where the next unit will find it. If you would rather it landed here, say so and I will fold it in.

**Window state stays conn-specific.** Spec §6.5 is explicit that none of the three possible generalizations should happen yet, and names the trigger: a second log type that actually needs a rolling window. This plan performs only the rename that §6.5 does call for.

---

## Global Constraints

- **Java package root:** `io.netsecml.platform`. Dependency chain is one-way: `domain → ports → application → adapters → bootstrap`. `domain` imports no framework code. Adapters never import each other.
- **`conn`'s behaviour must not change.** Its schema reports 20 features, so every vector it produces must still be 20 values in the same order with the same schema id and content hash. The existing tests are the proof; they must pass at their current counts.
- **`contracts/` and `infrastructure/clickhouse/ddl/` are immutable and untouched here.** `ConnFeatureSchemaV1.CONTENT_HASH` stays `f42fb1bebb2efe3acc5de634c6a7bb3d6f97fc021207f75d77652533b1c01e1b`.
- **`permits` still lists only `ConnEvent`.** No protocol record may be added for a protocol with no parser.
- **Registry lookups throw on an unknown key** rather than returning null — spec §6.1: "an unresolvable schema is a deployment error, not a runtime condition."
- **Inline comments describing each block are mandatory** — a standing user requirement. A comment that misdescribes the code is worse than none.
- **Build in stages.** `./mvnw clean verify` is OOM-killed on a 5.7 GiB machine. Run `./mvnw install -DskipTests -q -o` once, then per module.
- **Never combine `-am` with `-Dtest=`.** Surefire fails on the first upstream module lacking a match, and the suppression flag is forbidden because it reports BUILD SUCCESS on zero tests.
- **`ClickHouseOutageTest` is OOM-killed** — never run it, never claim it passes.
- **Commit with explicit paths only.** Trailer:
  ```
  Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
  ```

**Current test counts, which must not drop:** domain 70, application 13, adapter-kafka 23, adapter-flink 8, adapter-clickhouse 45.

---

## File Structure

| File | Responsibility |
|---|---|
| `modules/domain/.../feature/FeatureSchemaRegistry.java` | Resolves `LogType → FeatureSchema` and `schemaId → FeatureSchema`, throwing on unknown keys. |
| `modules/domain/.../feature/ConnWindowState.java` | `SourceWindowState`, renamed so the name stops claiming to be generic. |
| `modules/ports/.../in/BuildFeaturesUseCase.java` | Becomes generic in event and state type. |
| `modules/domain/.../feature/FeatureBuildResult.java` | Becomes generic in state type. |
| `modules/application/.../usecase/ConnBuildFeaturesUseCase.java` | `BuildFeaturesUseCaseImpl`, renamed and typed to `ConnEvent`/`ConnWindowState`. Its vector length comes from the schema. |

---

## Task 1: The schema registry

Spec §6.1. Additive — nothing consumes it yet, so the build stays green.

**Files:**
- Create: `modules/domain/src/main/java/io/netsecml/platform/domain/feature/FeatureSchemaRegistry.java`
- Test: `modules/domain/src/test/java/io/netsecml/platform/domain/feature/FeatureSchemaRegistryTest.java`

**Interfaces:**
- Consumes: `LogType`, `FeatureSchema`, `ConnFeatureSchemaV1.SCHEMA`.
- Produces: `FeatureSchemaRegistry.byLogType(LogType)` and `byId(String)`, both returning `FeatureSchema`, both throwing `IllegalArgumentException` on an unknown key.

- [ ] **Step 1: Write the failing test**

```java
package io.netsecml.platform.domain.feature;

import io.netsecml.platform.domain.event.LogType;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

// Inference and training both need "what is schema X, and how many values does it
// have?" without touching Kafka or Flink. This is that lookup, over the static
// schema constants.
class FeatureSchemaRegistryTest {

    // The wiring-time question: given a log type, which schema does it produce?
    @Test
    void resolvesTheSchemaForAnImplementedLogType() {
        FeatureSchema schema = FeatureSchemaRegistry.byLogType(LogType.CONN);

        assertEquals("conn-feature-v1", schema.id());
        assertEquals(20, schema.featureCount());
    }

    // The consumer-side question: an archived row carries a schema id, and training
    // resolves the definitions from it. That is what makes a row self-describing.
    @Test
    void resolvesTheSchemaFromAnArchivedRowsId() {
        FeatureSchema schema = FeatureSchemaRegistry.byId("conn-feature-v1");

        assertSame(ConnFeatureSchemaV1.SCHEMA, schema,
            "byId must return the same frozen instance, not a copy");
    }

    // Both lookups agree, so a row's id resolves to the schema its log type
    // produces. A registry where these could disagree would let a vector be
    // validated against the wrong definitions.
    @Test
    void bothLookupsResolveToTheSameSchema() {
        assertSame(FeatureSchemaRegistry.byLogType(LogType.CONN),
            FeatureSchemaRegistry.byId("conn-feature-v1"));
    }

    // An unresolvable schema is a deployment error, not a runtime condition -- a
    // null return would let a misconfigured job start and fail later, per input,
    // instead of failing at startup.
    @Test
    void throwsOnAnUnknownSchemaIdRatherThanReturningNull() {
        assertThrows(IllegalArgumentException.class,
            () -> FeatureSchemaRegistry.byId("dns-feature-v1"));
    }

    // Every LogType constant must resolve. This is the fail-fast startup check:
    // adding a constant without registering its schema is caught here rather than
    // when the first record of that type arrives in production.
    @Test
    void everyLogTypeConstantHasARegisteredSchema() {
        for (LogType logType : LogType.values()) {
            assertNotNull(FeatureSchemaRegistry.byLogType(logType),
                logType + " has no registered schema");
        }
    }
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `./mvnw install -DskipTests -q -o && ./mvnw test -pl modules/domain -o -Dtest=FeatureSchemaRegistryTest`
Expected: FAIL — compilation error, `FeatureSchemaRegistry` does not exist.

- [ ] **Step 3: Write the implementation**

```java
package io.netsecml.platform.domain.feature;

import io.netsecml.platform.domain.event.LogType;
import java.util.Map;

// Resolves a feature schema two ways: from a log type at wiring time, and from a
// schema id held by something that already has a record.
//
// byId is what makes an archived row self-describing -- training reads the
// schemaId off the row and resolves the definitions, rather than assuming which
// schema a table's rows follow.
//
// A static map over the frozen schema constants: no framework, no configuration,
// no I/O. Adding a log type means adding one entry here, and the test that walks
// LogType.values() fails until you do.
public final class FeatureSchemaRegistry {

    // Only conn today. An entry is added when a log type has a frozen schema
    // behind it -- the same rule LogType states about its own constants.
    private static final Map<LogType, FeatureSchema> BY_LOG_TYPE =
        Map.of(LogType.CONN, ConnFeatureSchemaV1.SCHEMA);

    // Derived from the same source, so the two lookups cannot disagree about
    // which schema a log type produces.
    private static final Map<String, FeatureSchema> BY_ID =
        BY_LOG_TYPE.values().stream()
            .collect(java.util.stream.Collectors.toUnmodifiableMap(FeatureSchema::id, schema -> schema));

    // Non-instantiable: every member is static.
    private FeatureSchemaRegistry() {
    }

    // Throws rather than returning null: an unresolvable schema means the
    // deployment is wrong, and failing at wiring time is cheaper than failing per
    // record once traffic arrives.
    public static FeatureSchema byLogType(LogType logType) {
        FeatureSchema schema = BY_LOG_TYPE.get(logType);
        if (schema == null) {
            throw new IllegalArgumentException("no feature schema registered for log type " + logType);
        }
        return schema;
    }

    public static FeatureSchema byId(String schemaId) {
        FeatureSchema schema = BY_ID.get(schemaId);
        if (schema == null) {
            throw new IllegalArgumentException("no feature schema registered with id " + schemaId);
        }
        return schema;
    }
}
```

- [ ] **Step 4: Run it to verify it passes**

Run: `./mvnw test -pl modules/domain -o -Dtest=FeatureSchemaRegistryTest`
Expected: PASS, 5 tests run.

- [ ] **Step 5: Commit**

```bash
git add modules/domain/src/main/java/io/netsecml/platform/domain/feature/FeatureSchemaRegistry.java \
        modules/domain/src/test/java/io/netsecml/platform/domain/feature/FeatureSchemaRegistryTest.java
git commit -m "feat(domain): add a feature schema registry keyed by log type and id"
```

---

## Task 2: Rename `SourceWindowState` to `ConnWindowState`

Spec §6.5: "Rename `SourceWindowState` → `ConnWindowState` so the name stops lying." Its three counters — connections, bytes, failures — are conn concepts. SSH would want auth failures, DNS would want NXDOMAIN counts.

**It is a pure rename in Java, but NOT from Flink's perspective — and that is the one real risk in this unit.**

`ConnFeatureProcessFunction` keeps the window in Flink keyed state:

```java
        ValueStateDescriptor<SourceWindowState> descriptor = new ValueStateDescriptor<>(
            "source-window-state", TypeInformation.of(SourceWindowState.class));
```

`SourceWindowState` is a plain final class, not `Serializable`, so Flink serialises it with Kryo — which embeds the class name. **Renaming the type changes the state serialiser, so a job restoring from an existing checkpoint cannot deserialise its window state.** This is the same class of silent failure as changing an operator uid, and it must be a decision rather than a discovery.

**Ruling: accept the state loss, and keep the descriptor's name string unchanged.**

Accept the loss because it is bounded and small: the window is five one-minute buckets, so the worst case is a cold window for about five minutes after a restore — a cooler feature value, not a wrong one, and not a correctness bug. Paying it now is cheaper than paying it after more protocols share this path.

Keep `"source-window-state"` as the descriptor name because a state name is identity in the same way an operator uid is, and this project's established position — set when Unit 1 preserved three historically inconsistent uids — is that identity strings survive even when a newer naming scheme would spell them differently. The type change alone already forces the break; renaming the descriptor too would compound two breaks where one is unavoidable, and would lose the ability to recognise this state in tooling and metrics across the change.

Add a comment at the descriptor recording exactly that, so the next person to touch it does not "tidy" the name to match the type.

No field, method or behaviour changes. It touches 10 files and must be one commit, because a rename cannot be half-applied.

**Files (all 10):**
- `modules/domain/src/main/java/io/netsecml/platform/domain/feature/SourceWindowState.java` → `ConnWindowState.java`
- `modules/domain/src/test/java/io/netsecml/platform/domain/feature/SourceWindowStateTest.java` → `ConnWindowStateTest.java`
- `modules/domain/.../feature/FeatureBuildResult.java`, `RecordTimingState.java`
- `modules/application/.../usecase/BuildFeaturesUseCaseImpl.java`, `.../feature/CommonFeatureExtractor.java`
- `modules/application/src/test/.../usecase/BuildFeaturesUseCaseImplTest.java`, `.../feature/CommonFeatureExtractorTest.java`
- `modules/ports/.../in/BuildFeaturesUseCase.java`
- `modules/adapter-flink/.../process/ConnFeatureProcessFunction.java`

- [ ] **Step 1: Rename the type and its test, and update every reference**

Rename the class and file, and update the class-level comment to say why the name is specific rather than generic:

```java
// Five fixed one-minute buckets holding rolling connection-count/byte-sum/
// failed-count totals for a single (sensor, sourceIp) key.
//
// Named for conn deliberately. These three counters are connection concepts: a
// DNS window would want NXDOMAIN counts and an SSH window would want auth
// failures. The ring mechanics are worth extracting when a second log type
// actually needs a rolling window -- not before, because each of the three
// possible generalizations trades away something real, and there is no second
// consumer yet to say which trade is right.
```

Do **not** rename `RecordTimingState`, which is genuinely log-type-agnostic — it measures inter-arrival timing, which every protocol has.

- [ ] **Step 2: Verify the rename changed nothing but names**

Run:
```bash
./mvnw install -DskipTests -q -o
./mvnw test -pl modules/domain -o
./mvnw test -pl modules/application -o
./mvnw test -pl modules/adapter-flink -o
```

Expected: domain 70, application 13, adapter-flink 8 — **identical to before the rename.** A changed count means something other than a name changed.

Then confirm no reference survives:
```bash
grep -rn "SourceWindowState" --include=*.java modules/ || echo "clean"
```
Expected: `clean`.

- [ ] **Step 3: Commit**

```bash
git add -u modules/
git commit -m "refactor(domain): rename SourceWindowState to ConnWindowState"
```

Note: `git add -u modules/` is the one place a non-explicit add is correct — a rename is a delete plus an add, and listing both halves of ten files invites a typo. It stages only tracked files under `modules/`, so the untracked wrapper jar cannot be swept in. Verify with `git status` before committing.

---

## Task 3: The generic port, and the dynamic length

Spec §6.2 and §6.3. This is the task the unit exists for.

**Files:**
- Modify: `modules/ports/src/main/java/io/netsecml/platform/port/in/BuildFeaturesUseCase.java`
- Modify: `modules/domain/src/main/java/io/netsecml/platform/domain/feature/FeatureBuildResult.java`
- Rename + modify: `modules/application/.../usecase/BuildFeaturesUseCaseImpl.java` → `ConnBuildFeaturesUseCase.java`
- Modify: `modules/application/src/test/.../usecase/BuildFeaturesUseCaseImplTest.java` → `ConnBuildFeaturesUseCaseTest.java`
- Modify: `modules/adapter-flink/.../process/ConnFeatureProcessFunction.java`

**Interfaces:**
- Produces: `interface BuildFeaturesUseCase<E extends NetworkEvent, S> { FeatureBuildResult<S> build(E event, S currentState); }`; `record FeatureBuildResult<S>(FeatureVector vector, S newState)`; `ConnBuildFeaturesUseCase implements BuildFeaturesUseCase<ConnEvent, ConnWindowState>`.

- [ ] **Step 1: Write the failing test**

Add to the renamed use-case test:

```java
    // The width comes from the schema, not a literal. For conn the schema reports
    // 20, so this asserts the same number the old hardcoded array produced -- the
    // point is where the number comes from, not what it is.
    @Test
    void vectorLengthComesFromTheRegisteredSchema() {
        FeatureBuildResult<ConnWindowState> result =
            new ConnBuildFeaturesUseCase(FIXED_CLOCK).build(CONN_EVENT, ConnWindowState.empty());

        assertEquals(FeatureSchemaRegistry.byLogType(LogType.CONN).featureCount(),
            result.vector().values().length);
        assertEquals(20, result.vector().values().length,
            "conn's frozen schema is 20 wide and must stay so");
    }

    // The schema id and content hash on the vector must come from the registry
    // too, so a vector can never claim a schema its values do not match.
    @Test
    void vectorCarriesTheRegisteredSchemasIdentity() {
        FeatureBuildResult<ConnWindowState> result =
            new ConnBuildFeaturesUseCase(FIXED_CLOCK).build(CONN_EVENT, ConnWindowState.empty());

        FeatureSchema schema = FeatureSchemaRegistry.byLogType(LogType.CONN);
        assertEquals(schema.id(), result.vector().schemaId());
        assertEquals(schema.contentHash(), result.vector().schemaHash());
    }
```

Reuse whatever `CONN_EVENT` and `FIXED_CLOCK` fixtures the existing test file already defines; read it first rather than inventing new ones.

- [ ] **Step 2: Run it to verify it fails**

Run: `./mvnw install -DskipTests -q -o && ./mvnw test -pl modules/application -o`
Expected: FAIL — `ConnBuildFeaturesUseCase` does not exist and `FeatureBuildResult` is not generic.

- [ ] **Step 3: Make the port and result generic**

`BuildFeaturesUseCase`:

```java
package io.netsecml.platform.port.in;

import io.netsecml.platform.domain.event.NetworkEvent;
import io.netsecml.platform.domain.feature.FeatureBuildResult;

// Building features from one event of one log type, given that key's current
// window state.
//
// Generic in BOTH the event and the state, because they vary together: a DNS
// implementation takes a DnsEvent and whatever window state DNS needs, and
// neither is a ConnEvent or a ConnWindowState. One implementation per log type,
// so no implementation ever casts or switches to discover what it was given.
public interface BuildFeaturesUseCase<E extends NetworkEvent, S> {
    FeatureBuildResult<S> build(E event, S currentState);
}
```

`FeatureBuildResult`:

```java
// The vector built from one event, plus the window state to store for that key.
//
// Generic in the state so it carries whatever state its log type uses, rather
// than naming one log type's state in a type every log type returns.
public record FeatureBuildResult<S>(FeatureVector vector, S newState) {
}
```

- [ ] **Step 4: Rename the implementation and take its width from the schema**

`BuildFeaturesUseCaseImpl` becomes `ConnBuildFeaturesUseCase implements BuildFeaturesUseCase<ConnEvent, ConnWindowState>`.

**The pattern switch at the top of `build` disappears.** The method now takes a `ConnEvent` directly, so there is nothing to narrow — spec §6.2's "no casting anywhere" is achieved by the signature rather than inside the body. Delete the switch and the now-unused `NetworkEvent` import.

Replace the hardcoded width and schema:

```java
        // The width comes from the schema rather than a literal, which is what
        // lets a log type with a different feature count use this same shape. For
        // conn the schema reports 20, so this produces exactly what the old
        // new float[20] produced.
        FeatureSchema schema = FeatureSchemaRegistry.byLogType(LogType.CONN);
        float[] values = new float[schema.featureCount()];
```

and use `schema.id()` and `schema.contentHash()` in the `FeatureVector` rather than `ConnFeatureSchemaV1`'s constants directly.

**Leave `System.arraycopy(eventLevel, 0, values, 0, 17)` and `values[17..19]` exactly as they are.** Those indices are conn's frozen layout, not a generic concern — this class is conn's implementation and is entitled to know them. Making them dynamic would be inventing a generality nothing needs.

- [ ] **Step 5: Update the Flink consumer**

`ConnFeatureProcessFunction` currently holds the **concrete** `BuildFeaturesUseCaseImpl` in a field (line 18), not the interface, so it breaks on the rename regardless. Change the field to the interface type `BuildFeaturesUseCase<ConnEvent, ConnWindowState>` and construct a `ConnBuildFeaturesUseCase` in `open()`. Its `ValueState<ConnWindowState>` and stream types follow.

**Do not change the `ValueStateDescriptor`'s name string** — see Task 2's ruling.

**One logic change IS required, contrary to what this step originally said.** `ConnFeatureProcessFunction` is declared `KeyedProcessFunction<SourceKey, NetworkEvent, FeatureVector>`, so `processElement` receives a `NetworkEvent` — that type is fixed by the `DataStream<NetworkEvent>` `OnlineFeatureJob` keys. The generic use case takes a `ConnEvent`. So `processElement` must narrow before calling it:

```java
        // The stream is DataStream<NetworkEvent> and KeyedProcessFunction's input
        // type follows it, so this cannot take ConnEvent directly the way the use
        // case does -- same constraint as SourceKeySelector. It narrows here
        // instead, and the compiler will flag this site when a second log type
        // joins the hierarchy and this function has to decide what it means.
        ConnEvent conn = switch (event) {
            case ConnEvent c -> c;
        };
```

This is the switch that moves out of the use case and into the adapter: §6.2's "no casting anywhere" applies to the use-case implementations, which now receive their own event type, not to the Flink adapter whose type parameters are fixed by the framework.

- [ ] **Step 6: Run every affected module**

```bash
./mvnw install -DskipTests -q -o
./mvnw test -pl modules/domain -o
./mvnw test -pl modules/application -o
./mvnw test -pl modules/adapter-flink -o
```

Expected: domain 70, application 15 (13 + the 2 new tests), adapter-flink 8. **A drop means a test was deleted rather than adjusted.**

- [ ] **Step 7: Commit**

```bash
git add modules/ports/src/main/java/io/netsecml/platform/port/in/BuildFeaturesUseCase.java \
        modules/domain/src/main/java/io/netsecml/platform/domain/feature/FeatureBuildResult.java \
        modules/application/src/main/java/io/netsecml/platform/application/usecase/ConnBuildFeaturesUseCase.java \
        modules/application/src/test/java/io/netsecml/platform/application/usecase/ConnBuildFeaturesUseCaseTest.java \
        modules/adapter-flink/src/main/java/io/netsecml/platform/adapter/flink/process/ConnFeatureProcessFunction.java
git commit -m "feat(application): take vector width and identity from the registered schema"
```

Stage the deleted `BuildFeaturesUseCaseImpl.java` and `BuildFeaturesUseCaseImplTest.java` too — check `git status` and add their paths explicitly.

---

## Task 4: Prove the online path survived, and document

- [ ] **Step 1: Run the end-to-end test**

This unit changed the port every feature flows through, so the E2E is what proves the wiring survived rather than just the types:

```bash
docker ps -aq | xargs -r docker rm -f
./mvnw install -DskipTests -q -o
./mvnw test -pl modules/bootstrap-online-job -o
```

Expected: `OnlineFeatureJobE2ETest` 1/1. Report the real count. **If it fails, report the failure verbatim rather than fixing it** — a break here means the generic port broke runtime wiring that no unit test caught.

- [ ] **Step 2: Update `CLAUDE.md`**

Add to Key invariants:

```markdown
- Feature schemas resolve through `FeatureSchemaRegistry`, by `LogType` at wiring time or by
  `schemaId` for an archived row. Both throw on an unknown key: an unresolvable schema is a
  deployment error, not a runtime condition. A vector's width, id and content hash all come from
  its registered schema, never from a literal.
- `BuildFeaturesUseCase<E, S>` has one implementation per log type, so no implementation casts or
  switches to discover what it was given.
```

- [ ] **Step 3: Check for stale descriptions**

Grep the docs for `SourceWindowState`, `BuildFeaturesUseCaseImpl`, and `new float[20]`, and correct what you find. **If nothing is stale, say so in your report rather than inventing an edit.**

- [ ] **Step 4: Commit**

```bash
git add CLAUDE.md
git commit -m "docs: record the schema registry and generic use-case port"
```

---

## Self-Review

**Spec coverage.** §6.1 registry → Task 1. §6.2 generic port → Task 3 Steps 3-4. §6.3 dynamic length → Task 3 Step 4. §6.5's rename → Task 2, and its "none of them yet" on generalizing window state is honoured by doing only the rename.

**What this unit does not do, deliberately.** It does not wire the common tier — `conn-feature-v1` is frozen without it and the layouts differ, so the tier's first consumer is 3c. It does not split `Endpoints` — that is event shape, not feature machinery, and it stays recorded in `EventEnvelope`'s comment. It does not generalize the rolling window, per §6.5's explicit instruction and named trigger.

**Type consistency.** `FeatureSchemaRegistry.byLogType` returns the same `FeatureSchema` instance `byId` returns, asserted by `assertSame` in Task 1 and relied on by Task 3's identity test. `FeatureBuildResult<S>` and `BuildFeaturesUseCase<E, S>` agree on `S`. `ConnWindowState` is the name used from Task 2 onward, including in Task 3's test code.

**The risk concentrated in Task 2, and it is not the one it looks like.** A ten-file rename is mechanical, and the check for half-application is easy: three module counts stay *identical* and `grep SourceWindowState` returns nothing. The real risk is invisible to every test — the renamed class is held in Flink keyed state serialised by Kryo, so the rename invalidates existing checkpoint state. No test on this machine can detect that, because none restores from a checkpoint. It is handled by ruling rather than by verification: accept a bounded five-minute cold window, keep the descriptor name, and comment it at the site.

**A test I could not write.** Nothing here proves the registry resolves a *second* log type correctly, because only `CONN` exists. `everyLogTypeConstantHasARegisteredSchema` walks `LogType.values()`, so it starts guarding that the moment 3c adds `DNS` — but today it asserts one entry, and its name promises more than it currently delivers. That is stated rather than hidden, and it is the same limit Unit 2 hit with `dlqChain`'s non-CONN branch.
