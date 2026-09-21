# Modbus Stage 1, Unit M1 — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Turn Zeek/ICSNPP `modbus_detailed` records from Kafka into the frozen 42-value Modbus Stage 1 feature vector, archived to ClickHouse, with semantics identical to the authoritative Python engine.

**Architecture:** The existing per-protocol pattern — source contract, DTO, parser, mapper, sealed-hierarchy record, frozen feature schema, a use case, and one `KeyedProcessFunction` holding bounded causal state — applied to a feature set that is frozen upstream rather than designed here. Groups A and B of the contract are a pure per-record mapping; groups C and D come from keyed state that must be read *before* the current event mutates it.

**Tech Stack:** Java 21, Flink 2.2.1, `flink-connector-kafka:5.0.0-2.2`, Jackson 2.17.1, JUnit 5, Testcontainers 1.21.4.

**Spec:** `docs/superpowers/specs/2026-09-21-modbus-stage1-design.md`

## Global Constraints

- **The upstream artifacts win every disagreement.** `two-models-info/modbus_/FEATURE_CONTRACT_V1.json` and `07b_materialize_feature_engine_v1.py` are the authority. A vector that does not match `modbus_feature_contract_v1` cannot be scored by the frozen model, which is the entire point of this unit.
- **`two-models-info/`, `new-models/`, `modbus_rf_attack_type_v1/` and `PROJECT_OVERVIEW.md` are untracked and must never be staged.** ~139 MB of model binaries sit in the repo root. Never `git add -A` or `git add .`; stage explicit file paths only. Note that `git commit -m "..." -- <pathspec>` ignores partial index staging and commits the whole working-tree state of those paths — check `git show --stat` afterwards.
- Java package root `io.netsecml.platform`. Java 21: `record` for immutable data carriers, `sealed interface` + records + pattern-matching `switch` with explicit arms and **never** a `default` arm.
- Hexagonal, one-way: `domain → ports → application → adapters → bootstrap`. `domain` imports no framework. `application` imports only `domain` + `ports`. Adapters do not import each other, except the one recorded exception (`adapter-flink → adapter-kafka`).
- **A comment that misdescribes its code is worse than no comment.** Verify every claim against the code. Never cite anything under `.superpowers/`, a task number, or this plan's task numbers in a shipped comment — cite a committed document by file path or put the reasoning in the comment itself.
- Feature schemas resolve through `FeatureSchemaRegistry` and throw on an unknown key. A vector's width, id and content hash come from its registered schema, never from a literal.
- Contracts under `contracts/` are immutable and content-hashed. **No file under `contracts/features/` other than the new `modbus-feature-schema-v1.json` may change.**
- Records with array components need defensive copies in the compact constructor **and** in the accessor.
- **Hard rules on this machine (~5.7 GiB RAM):** never run `ClickHouseOutageTest` — it has never run here and must never be described as passing. Never run `adapter-clickhouse` or either bootstrap module unfiltered. Never combine `-Dtest` with `-am`. Always pass `-o`. Only Task 11 may run the end-to-end suites, one at a time.
- `-Dtest=X` with `-Dsurefire.failIfNoSpecifiedTests=false` reports BUILD SUCCESS having run zero tests. Always confirm the actual `Tests run:` count.
- If a module fails to compile against code you did not change, `~/.m2` holds stale sibling jars: run `./mvnw install -DskipTests -pl modules/domain,modules/ports -o` first.

## Five rulings this plan makes up front

**Ruling 1 — a negative inter-arrival starts a new segment; the engine never throws.**
The Python engine raises `RuntimeError` on an in-segment inter-arrival outside `[0, 15]` and on a negative RTT, because offline it has already asserted `capture_event_index` is strictly increasing. A Flink operator has no such guarantee and must not fail the job on one out-of-order record. So the segment rule becomes:

```
newSegment = (lastTs == null) || (ts - lastTs) < 0.0 || (ts - lastTs) > 15.0
```

This is a strict superset of the Python rule (which only tests `> 15.0`), and it makes both of the engine's exceptions unreachable rather than merely unlikely: within a segment `ts - lastTs` is now in `[0, 15]` by construction, and since `pending[tid]` was recorded at some earlier event whose timestamp became `lastTs`, `rtt = ts - pending[tid] >= ts - lastTs >= 0`. The out-of-order case sets a quality flag so it stays observable instead of silently resetting state.

**Ruling 2 — function code 23 counts as BOTH read and write.**
`READ_FUNCTIONS = {1,2,3,4,20,24,23}` and `WRITE_FUNCTIONS = {5,6,15,16,21,22,23}` in the engine. The contract's `function_registry` presents `read`, `write` and `read_write` as three lists, and an implementer building three disjoint sets would silently produce different ratios. FC 23 is counted in both tallies, which is exactly what makes `read_ratio_10s` and `write_ratio_10s` the "READ/READ_WRITE" and "WRITE/READ_WRITE" fractions the contract names.

**Ruling 3 — the trailing windows are half-open `(t-w, t]` and include the current event.**
The engine purges with `while q and q[0] <= cutoff: popleft()` where `cutoff = ts - w`, so an event exactly `w` seconds old is EXCLUDED. It then appends the current event *before* reading the window sizes, so the current event is always counted. `read_ratio_10s` and `write_ratio_10s` divide by the 10-second window's event count **after** that append, never by the literal 10.

**Ruling 4 — `event_rate_1s` is a raw count.**
The engine writes `float(len(w1_ts))` for the 1-second window but `count / 10.0` and `count / 60.0` for the others. Dividing the 1-second count by 1.0 is numerically identical, so either spelling is correct; do not "fix" the asymmetry into `count / 1.0` and do not scale it by anything else.

**Ruling 5 — before-event read, then mutate.**
The contract's `transaction_rule` is explicit: compute every current-event feature from the state as it stands, and only then record the request's pending TID, clear a matched response's TID, and update `prevFc`/`lastTs`/`lastAddress`/`lastQuantity`. `outstanding_requests_before_event` is named for this: a request must NOT count itself. Getting this backwards changes the emitted value for every request and no test catches it unless one pins it deliberately — Task 6 pins it.

## File structure

| File | Responsibility |
|---|---|
| `tests/fixtures/contracts/modbus_feature_contract_v1.json` | The upstream frozen contract, committed so the parity test is durable |
| `contracts/source/zeek-modbus-source-v1.json` | The Zeek/ICSNPP input contract |
| `contracts/features/modbus-feature-schema-v1.json` | The 42-value output contract |
| `domain/feature/ModbusFeatureSchemaV1.java` | The registered schema, 42 definitions |
| `domain/event/ModbusEvent.java` | The sealed-hierarchy record |
| `domain/feature/ModbusEntityKey.java` | `(sensor, clientIp, serverIp, unitId)` keyed-state key |
| `domain/feature/ModbusEntityState.java` | Segment, previous-event, pending-TID and window state |
| `adapter-kafka/dto/ZeekModbusRecord.java` | Jackson binding for one `modbus_detailed` line |
| `adapter-kafka/parser/JsonZeekModbusParser.java` | Bytes → DTO, strict JSON |
| `adapter-kafka/mapper/ModbusEventMapper.java` | DTO → `ModbusEvent`, identity and validation |
| `application/feature/ModbusFeatureExtractor.java` | All 42 values from (event, state-before) |
| `application/usecase/ModbusBuildFeaturesUseCase.java` | Orchestrates extract-then-mutate |
| `adapter-flink/process/ModbusFeatureProcessFunction.java` | Hosts the state in Flink |
| `adapter-flink/process/ModbusParseMapValidateFunction.java` | Parse/map/validate with DLQ side output |
| `adapter-flink/process/ModbusEntityKeySelector.java` | Key selector |

---

### Task 1: The three contract files and the registered schema

**Files:**
- Create: `tests/fixtures/contracts/modbus_feature_contract_v1.json` (copy of the upstream frozen contract)
- Create: `contracts/source/zeek-modbus-source-v1.json`
- Create: `contracts/features/modbus-feature-schema-v1.json`
- Create: `modules/domain/src/main/java/io/netsecml/platform/domain/feature/ModbusFeatureSchemaV1.java`
- Test: `modules/domain/src/test/java/io/netsecml/platform/domain/feature/ModbusFeatureSchemaV1Test.java`

**Interfaces:**
- Produces: `ModbusFeatureSchemaV1.SCHEMA` (a `FeatureSchema` with id `modbus-feature-v1`, 42 features) and `ModbusFeatureSchemaV1.CONTENT_HASH`.

Copy the upstream contract into the repo first — it is the evidence every later parity claim rests on, and `two-models-info/` is untracked and may not survive:

```bash
mkdir -p tests/fixtures/contracts
cp two-models-info/modbus_/FEATURE_CONTRACT_V1.json tests/fixtures/contracts/modbus_feature_contract_v1.json
```

`contracts/features/modbus-feature-schema-v1.json` mirrors `contracts/features/dns-feature-schema-v1.json`'s shape (`id`, `semanticVersion`, `inputDtype`, `featureCount`, `features[]` with `index`, `name`, `unit`, `missingPolicy`, `formula`). Indices are **0-based** here, against the upstream contract's 1-based `index`; the NAMES and their ORDER are what must match. Every feature's `missingPolicy` is `DEFAULT_ZERO` except none — the upstream contract's `missing_rule` values ("required", "never missing", "0 if absent", "use address_present") all describe a value that is present or defaulted to zero, never a rejection, so `DEFAULT_ZERO` throughout is the honest mapping. Record that reasoning in the JSON's own `notes` field and in the Java comment.

`contracts/source/zeek-modbus-source-v1.json` mirrors `contracts/source/zeek-dns-source-v1.json`. Required: `ts`, `uid`, `is_orig`, `tid`, `func`. Optional: `unit`/`uint`, `address`, `quantity`, `matched`, `request_values`, `response_values`, and the endpoint fields. Document that `exception_code`, `request_data`, `response_data`, the subfunction codes, `mei_type` and `modbus_detailed_link_id` are deliberately excluded per the upstream contract's `excluded_raw_fields`.

- [ ] **Step 1: Write the failing test**

```java
package io.netsecml.platform.domain.feature;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import static org.junit.jupiter.api.Assertions.assertEquals;

class ModbusFeatureSchemaV1Test {

    // The upstream frozen contract, committed to this repo so the parity claim
    // survives the working copy of the model project going away.
    private static final Path UPSTREAM =
        Path.of("..", "..", "tests", "fixtures", "contracts", "modbus_feature_contract_v1.json");

    private static final Path OURS =
        Path.of("..", "..", "contracts", "features", "modbus-feature-schema-v1.json");

    @Test
    void theSchemaIsExactlyTheUpstreamContractsFeatureOrder() throws Exception {
        // This is the whole point of the unit: a vector whose order differs from
        // the frozen contract cannot be scored by the frozen model at all.
        JsonNode upstream = new ObjectMapper().readTree(UPSTREAM.toFile());
        List<String> expected = new ArrayList<>();
        upstream.get("feature_order").forEach(n -> expected.add(n.asText()));

        assertEquals(42, expected.size(), "the upstream contract must carry 42 features");
        assertEquals(expected, ModbusFeatureSchemaV1.SCHEMA.features().stream()
            .map(FeatureDefinition::name).toList());
    }

    @Test
    void theCommittedContractFileMatchesTheRegisteredSchema() throws Exception {
        // The JSON is what a model author reads; the Java is what the pipeline
        // emits. They are two statements of one fact and must not drift.
        JsonNode ours = new ObjectMapper().readTree(OURS.toFile());
        List<String> inFile = new ArrayList<>();
        ours.get("features").forEach(f -> inFile.add(f.get("name").asText()));

        assertEquals(42, ours.get("featureCount").asInt());
        assertEquals(inFile, ModbusFeatureSchemaV1.SCHEMA.features().stream()
            .map(FeatureDefinition::name).toList());
    }

    @Test
    void theSchemaCarriesNoCommonTier() throws Exception {
        // modbus-feature-v1 deliberately does NOT lead with the common tier, unlike
        // dns-feature-v1: it mirrors an externally frozen contract, so it carries
        // exactly what that contract specifies. See CLAUDE.md's invariant.
        List<String> names = ModbusFeatureSchemaV1.SCHEMA.features().stream()
            .map(FeatureDefinition::name).toList();
        assertEquals("is_response", names.get(0));
        assertEquals("write_ratio_10s", names.get(41));
    }

    @Test
    void indicesAreZeroBasedAndDense() {
        for (int i = 0; i < 42; i++) {
            assertEquals(i, ModbusFeatureSchemaV1.SCHEMA.features().get(i).index());
        }
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `./mvnw test -pl modules/domain -o -Dtest=ModbusFeatureSchemaV1Test`
Expected: compilation failure, `cannot find symbol: ModbusFeatureSchemaV1`.

- [ ] **Step 3: Implement**

Write `ModbusFeatureSchemaV1` with 42 `FeatureDefinition`s in the upstream order, taking each name from `feature_order` and each `formula` from the upstream contract's matching `definition` string. Compute `CONTENT_HASH` as the SHA-256 of the committed `contracts/features/modbus-feature-schema-v1.json`, exactly as `DnsFeatureSchemaV1` does for its own file — derive it with `sha256sum` and paste the value.

- [ ] **Step 4: Run to verify it passes**

Run: `./mvnw test -pl modules/domain -o`
Expected: the previous count plus 4, 0 failures. Record the before and after `Tests run:` lines.

- [ ] **Step 5: Commit**

```bash
git add tests/fixtures/contracts/modbus_feature_contract_v1.json \
        contracts/source/zeek-modbus-source-v1.json \
        contracts/features/modbus-feature-schema-v1.json \
        modules/domain/src/main/java/io/netsecml/platform/domain/feature/ModbusFeatureSchemaV1.java \
        modules/domain/src/test/java/io/netsecml/platform/domain/feature/ModbusFeatureSchemaV1Test.java
git commit -m "feat(domain): register modbus-feature-v1, mirroring the frozen upstream contract"
```

---

### Task 2: `LogType.MODBUS` and the `ModbusEvent` record

**Files:**
- Modify: `modules/domain/src/main/java/io/netsecml/platform/domain/event/LogType.java`
- Modify: `modules/domain/src/main/java/io/netsecml/platform/domain/event/NetworkEvent.java`
- Create: `modules/domain/src/main/java/io/netsecml/platform/domain/event/ModbusEvent.java`
- Test: `modules/domain/src/test/java/io/netsecml/platform/domain/event/ModbusEventTest.java`

**Interfaces:**
- Consumes: `ModbusFeatureSchemaV1.SCHEMA` from Task 1 — the sealed hierarchy's rule is that a record joins `permits` only once it has a parser, a mapper and a feature schema, and Task 1 supplies the schema.
- Produces: `record ModbusEvent(EventEnvelope envelope, ModbusDirection direction, int functionCode, String transactionId, String unitId, Double address, Double quantity, boolean matched, double[] requestValues, double[] responseValues) implements NetworkEvent`, and `enum ModbusDirection { REQUEST, RESPONSE }` nested in `ModbusEvent`.

`address` and `quantity` are boxed `Double` because absence is meaningful and drives the `*_present` masks — a primitive with a sentinel would make "absent" indistinguishable from a real 0. The two value arrays need defensive copies in the compact constructor **and** the accessor.

Adding `ModbusEvent` to `permits` will fail compilation at every non-exhaustive pattern `switch`. That is the point of sealing. Add an explicit `case ModbusEvent` arm at each site; never a `default`.

- [ ] **Step 1: Write the failing test**

```java
@Test
void theValueArraysAreDefensivelyCopiedBothWays() {
    double[] request = {1.0, 2.0};
    ModbusEvent event = event(request, new double[] {3.0});

    request[0] = 99.0;
    assertEquals(1.0, event.requestValues()[0], "the constructor must copy");

    event.requestValues()[1] = 99.0;
    assertEquals(2.0, event.requestValues()[1], "the accessor must copy too");
}

@Test
void anAbsentAddressIsNullRatherThanZero() {
    // address_present is feature index 9 and address_value index 8; a sentinel
    // zero would make a genuine address of 0 indistinguishable from an absent one.
    ModbusEvent event = eventWithAddress(null);
    assertNull(event.address());
}

@Test
void theSealedHierarchyPermitsExactlyTheImplementedLogTypes() {
    assertEquals(
        Set.of(ConnEvent.class, DnsEvent.class, ModbusEvent.class),
        Set.of(NetworkEvent.class.getPermittedSubclasses()));
}
```

Define `event(...)` and `eventWithAddress(...)` as private helpers in the test class; nothing in the codebase provides them.

- [ ] **Step 2: Run to verify it fails**

Run: `./mvnw test -pl modules/domain -o -Dtest=ModbusEventTest`
Expected: compilation failure, `cannot find symbol: ModbusEvent`.

- [ ] **Step 3: Implement**

Add `MODBUS` to `LogType`, create `ModbusEvent`, add it to `permits`, then fix every compilation error the sealing produces with an explicit `case ModbusEvent` arm. Update `NetworkEventSurfaceTest`'s `permittedSubclasses` assertion — it is reflective, so it fails at run time rather than compile time and is easy to miss.

- [ ] **Step 4: Run to verify it passes**

Run: `./mvnw test -pl modules/domain -o` then `./mvnw test -pl modules/application -o` and `./mvnw test -pl modules/adapter-kafka -o`
Expected: all green. Report every `Tests run:` line; a DROP means something was deleted — stop and report.

- [ ] **Step 5: Commit**

```bash
git add modules/domain/src/main/java/io/netsecml/platform/domain/event/LogType.java \
        modules/domain/src/main/java/io/netsecml/platform/domain/event/NetworkEvent.java \
        modules/domain/src/main/java/io/netsecml/platform/domain/event/ModbusEvent.java \
        modules/domain/src/test/java/io/netsecml/platform/domain/event/ModbusEventTest.java
git commit -m "feat(domain): add LogType.MODBUS and the ModbusEvent record"
```

(Stage any other file the sealing forced you to touch, naming each explicitly.)

---

### Task 3: The Zeek DTO and the strict JSON parser

**Files:**
- Create: `modules/adapter-kafka/src/main/java/io/netsecml/platform/adapter/kafka/dto/ZeekModbusRecord.java`
- Create: `modules/adapter-kafka/src/main/java/io/netsecml/platform/adapter/kafka/parser/JsonZeekModbusParser.java`
- Test: `modules/adapter-kafka/src/test/java/io/netsecml/platform/adapter/kafka/parser/JsonZeekModbusParserTest.java`

**Interfaces:**
- Produces: `JsonZeekModbusParser.parse(byte[]) → ParseResult<ZeekModbusRecord>` (mirror `JsonZeekDnsParser`'s exact return shape — read it first).

Bind with `@JsonProperty(required = true)` for `ts`, `uid`, `tid`, `func`. **`required = true` enforces PRESENCE, not non-nullness**, and Jackson's `FAIL_ON_NULL_FOR_PRIMITIVES` is off by default, so an explicit `"func": null` would silently become `0` — a valid-looking function code. Enable `DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES` on this parser's `ObjectMapper` and test it. This exact defect was found and fixed elsewhere in this repo; do not reintroduce it.

Accept both endpoint spellings (`id.orig_h`/`id.resp_h` and `source_h`/`destination_h`) and both unit spellings (`unit`, `uint`) via `@JsonAlias`.

`request_values` and `response_values` arrive as JSON arrays of numbers. **Require strict JSON.** Do NOT reproduce the offline engine's `ast.literal_eval` fallback: it exists to tolerate Python-repr strings read back from research files, and on a production wire it would accept a malformed payload instead of rejecting it.

- [ ] **Step 1: Write the failing test**

```java
@Test
void aWellFormedModbusDetailedLineParses() {
    String json = """
        {"ts":1758000000.5,"uid":"CXY1","id.orig_h":"10.0.0.5","id.orig_p":50001,
         "id.resp_h":"10.0.0.9","id.resp_p":502,"is_orig":true,"tid":17,"unit":1,
         "func":"READ_HOLDING_REGISTERS","address":40001,"quantity":2,
         "matched":true,"request_values":[7,9],"response_values":[]}
        """;
    ParseResult<ZeekModbusRecord> result = new JsonZeekModbusParser().parse(json.getBytes(UTF_8));
    assertTrue(result.isSuccess());
    assertEquals(17, result.value().tid());
    assertEquals(2, result.value().requestValues().size());
}

@Test
void anExplicitNullFunctionCodeIsRejectedRatherThanBecomingZero() {
    // required=true only enforces that the KEY is present. Without
    // FAIL_ON_NULL_FOR_PRIMITIVES an explicit null binds to 0, which is a
    // plausible-looking function code and would be scored rather than rejected.
    String json = """
        {"ts":1758000000.5,"uid":"CXY1","tid":17,"func":null}
        """;
    ParseResult<ZeekModbusRecord> result = new JsonZeekModbusParser().parse(json.getBytes(UTF_8));
    assertFalse(result.isSuccess());
}

@Test
void aPythonReprValueVectorIsRejectedNotSalvaged() {
    // The offline engine falls back to ast.literal_eval for research files.
    // On the wire that tolerance would accept a malformed payload silently.
    String json = """
        {"ts":1758000000.5,"uid":"CXY1","tid":17,"func":3,"request_values":"[7, 9]"}
        """;
    ParseResult<ZeekModbusRecord> result = new JsonZeekModbusParser().parse(json.getBytes(UTF_8));
    assertFalse(result.isSuccess());
}

@Test
void bothEndpointSpellingsBindToTheSameFields() {
    String alternate = """
        {"ts":1758000000.5,"uid":"CXY1","source_h":"10.0.0.5","destination_h":"10.0.0.9",
         "is_orig":true,"tid":17,"uint":3,"func":3}
        """;
    ParseResult<ZeekModbusRecord> result = new JsonZeekModbusParser().parse(alternate.getBytes(UTF_8));
    assertTrue(result.isSuccess());
    assertEquals("10.0.0.5", result.value().sourceHost());
    assertEquals("3", result.value().unitId());
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `./mvnw test -pl modules/adapter-kafka -o -Dtest=JsonZeekModbusParserTest`
Expected: compilation failure.

- [ ] **Step 3: Implement**

`ZeekModbusRecord` as a record with `@JsonIgnoreProperties(ignoreUnknown = true)`; `JsonZeekModbusParser` mirroring `JsonZeekDnsParser`, with `FAIL_ON_NULL_FOR_PRIMITIVES` enabled and a comment saying why.

`func` arrives as either a name (`"READ_HOLDING_REGISTERS"`) or a number, depending on ICSNPP version and configuration. Bind it as a `String` on the DTO and resolve it to a numeric code in the mapper (Task 4), so the parser stays a pure binding step.

- [ ] **Step 4: Run to verify it passes**

Run: `./mvnw test -pl modules/adapter-kafka -o`
Expected: previous count + 4.

- [ ] **Step 5: Commit**

```bash
git add modules/adapter-kafka/src/main/java/io/netsecml/platform/adapter/kafka/dto/ZeekModbusRecord.java \
        modules/adapter-kafka/src/main/java/io/netsecml/platform/adapter/kafka/parser/JsonZeekModbusParser.java \
        modules/adapter-kafka/src/test/java/io/netsecml/platform/adapter/kafka/parser/JsonZeekModbusParserTest.java
git commit -m "feat(adapter-kafka): parse Zeek modbus_detailed records, strictly"
```

---

### Task 4: The mapper — identity, direction and the function-code registry

**Files:**
- Create: `modules/adapter-kafka/src/main/java/io/netsecml/platform/adapter/kafka/mapper/ModbusEventMapper.java`
- Create: `modules/domain/src/main/java/io/netsecml/platform/domain/event/ModbusFunctionCode.java`
- Test: `modules/adapter-kafka/src/test/java/io/netsecml/platform/adapter/kafka/mapper/ModbusEventMapperTest.java`
- Test: `modules/domain/src/test/java/io/netsecml/platform/domain/event/ModbusFunctionCodeTest.java`

**Interfaces:**
- Produces: `ModbusEventMapper.map(ZeekModbusRecord, SensorId) → MapResult<ModbusEvent>` (mirror `DnsEventMapper`'s exact return shape); `ModbusFunctionCode.READ_FUNCTIONS`, `.WRITE_FUNCTIONS` as `Set<Integer>`, and `ModbusFunctionCode.codeOf(String) → OptionalInt`.

**Event identity is `sensor:uid:tid:direction:ts_millis`.** Direction and timestamp are both required: ICSNPP emits request and response as separate records sharing a `tid`, and `tid` is a client-chosen 16-bit counter that a 10 Hz poll loop exhausts in under two hours on connections that live far longer.

**Direction is validated, never guessed.** Resolve it from `request_response` if present, else from `is_orig` (orig ⇒ REQUEST). If neither resolves, reject with `MISSING_REQUIRED_FIELD` — `is_response` is feature index 0 and both entity-key components depend on orientation, so a guess would corrupt the key and one frozen feature at once.

**Timestamp bound.** Reuse the same rule `EventMapper` and `DnsEventMapper` already carry: reject NaN, infinite, negative, and anything at or beyond `Instant.parse("2300-01-01T00:00:00Z").getEpochSecond()`, because `event_time` is `DateTime64(3,'UTC')` and the archive table is `PARTITION BY toYYYYMMDD(event_time)`. Pin the constant's value in a test.

`ModbusFunctionCode` holds the frozen sets from Ruling 2 — **23 appears in both** — plus the name→code mapping for the string form of `func`.

- [ ] **Step 1: Write the failing test**

```java
@Test
void functionCode23IsBothReadAndWrite() {
    // READ_WRITE_MULTIPLE_REGISTERS. The upstream engine counts it in both
    // tallies, which is what makes read_ratio_10s and write_ratio_10s the
    // "READ/READ_WRITE" and "WRITE/READ_WRITE" fractions the contract names.
    assertTrue(ModbusFunctionCode.READ_FUNCTIONS.contains(23));
    assertTrue(ModbusFunctionCode.WRITE_FUNCTIONS.contains(23));
}

@Test
void theFrozenFunctionSetsAreExactlyTheUpstreamContracts() {
    assertEquals(Set.of(1, 2, 3, 4, 20, 24, 23), ModbusFunctionCode.READ_FUNCTIONS);
    assertEquals(Set.of(5, 6, 15, 16, 21, 22, 23), ModbusFunctionCode.WRITE_FUNCTIONS);
}

@Test
void aRequestAndItsResponseGetDistinctEventIds() {
    // Both carry the same uid and tid; only direction differs.
    MapResult<ModbusEvent> request = map(record(1758000000.5, "CXY1", 17, true));
    MapResult<ModbusEvent> response = map(record(1758000000.6, "CXY1", 17, false));
    assertNotEquals(request.value().eventId(), response.value().eventId());
}

@Test
void twoSameDirectionRecordsSharingATidDifferOnlyByTimestamp() {
    // The counter-wrap case: tid is 16 bits and repeats inside one connection.
    MapResult<ModbusEvent> first = map(record(1758000000.5, "CXY1", 17, true));
    MapResult<ModbusEvent> second = map(record(1758000900.5, "CXY1", 17, true));
    assertNotEquals(first.value().eventId(), second.value().eventId());
}

@Test
void anUnresolvableDirectionIsRejectedRatherThanGuessed() {
    ZeekModbusRecord noDirection = recordWithNoDirectionFields();
    assertFalse(map(noDirection).isSuccess());
}

@Test
void aTimestampAtOrBeyondTheClickHouseCeilingIsRejected() {
    assertEquals(10413792000L, ModbusEventMapper.MAX_VALID_TS_SECONDS);
    assertFalse(map(record(10413792000.0, "CXY1", 17, true)).isSuccess());
    assertTrue(map(record(10413791999.0, "CXY1", 17, true)).isSuccess());
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `./mvnw test -pl modules/adapter-kafka -o -Dtest=ModbusEventMapperTest`
Expected: compilation failure.

- [ ] **Step 3: Implement**

Mirror `DnsEventMapper`'s structure and rejection ordering. Derive `MAX_VALID_TS_SECONDS` yourself from `Instant.parse("2300-01-01T00:00:00Z").getEpochSecond()` and comment which column type it protects.

- [ ] **Step 4: Run to verify it passes**

Run: `./mvnw test -pl modules/adapter-kafka -o` and `./mvnw test -pl modules/domain -o`
Expected: both green, counts up.

- [ ] **Step 5: Commit**

```bash
git add modules/domain/src/main/java/io/netsecml/platform/domain/event/ModbusFunctionCode.java \
        modules/domain/src/test/java/io/netsecml/platform/domain/event/ModbusFunctionCodeTest.java \
        modules/adapter-kafka/src/main/java/io/netsecml/platform/adapter/kafka/mapper/ModbusEventMapper.java \
        modules/adapter-kafka/src/test/java/io/netsecml/platform/adapter/kafka/mapper/ModbusEventMapperTest.java
git commit -m "feat(adapter-kafka): map modbus records to events with a wrap-safe identity"
```

---

### Task 5: `ModbusEntityKey`

**Files:**
- Create: `modules/domain/src/main/java/io/netsecml/platform/domain/feature/ModbusEntityKey.java`
- Test: `modules/domain/src/test/java/io/netsecml/platform/domain/feature/ModbusEntityKeyTest.java`

**Interfaces:**
- Produces: `record ModbusEntityKey(SensorId sensor, String clientIp, String serverIp, String unitId)` with `ModbusEntityKey.of(ModbusEvent) → ModbusEntityKey`.

Orientation normalization, from the upstream engine:

```
clientIp = (direction == REQUEST) ? srcIp : dstIp
serverIp = (direction == REQUEST) ? dstIp : srcIp
```

**`hashCode()` must be overridden and built only from Strings.** Flink assigns key groups from `key.hashCode()`, so a key type whose hash varies between JVMs silently breaks keyed-state restore and can split one logical key across subtasks at parallelism > 1. `SourceKey` shipped with exactly that defect via an enum component, because `Enum.hashCode()` is `final` and returns the identity hash. Every component here is already a `String`, so the risk is a future edit rather than today's code — pin the formula so an edit that reintroduces a non-String component fails.

`unitId` is a `String` because the upstream `int_key` helper falls back to the literal `"NA"` for an absent unit rather than dropping the record.

- [ ] **Step 1: Write the failing test**

```java
@Test
void theHashIsTheSpecifiedFormulaOverStringsOnly() {
    // Pinning the formula, not a magic number: Flink derives key groups from
    // hashCode(), so any component whose hash is JVM-dependent breaks state
    // restore. A single-JVM test cannot observe that directly.
    ModbusEntityKey key = new ModbusEntityKey(new SensorId("sensor-eu-1"), "10.0.0.5", "10.0.0.9", "1");
    assertEquals(Objects.hash("sensor-eu-1", "10.0.0.5", "10.0.0.9", "1"), key.hashCode());
}

@Test
void aRequestAndItsResponseShareOneKey() {
    // Orientation normalization is the whole reason this key exists: the two
    // directions carry src and dst swapped and must land in the same state.
    ModbusEvent request = event(ModbusDirection.REQUEST, "10.0.0.5", "10.0.0.9", "1");
    ModbusEvent response = event(ModbusDirection.RESPONSE, "10.0.0.9", "10.0.0.5", "1");
    assertEquals(ModbusEntityKey.of(request), ModbusEntityKey.of(response));
}

@Test
void adifferentUnitIsADifferentKey() {
    ModbusEvent unitOne = event(ModbusDirection.REQUEST, "10.0.0.5", "10.0.0.9", "1");
    ModbusEvent unitTwo = event(ModbusDirection.REQUEST, "10.0.0.5", "10.0.0.9", "2");
    assertNotEquals(ModbusEntityKey.of(unitOne), ModbusEntityKey.of(unitTwo));
}

@Test
void anAbsentUnitBecomesTheLiteralNaRatherThanDroppingTheRecord() {
    ModbusEvent noUnit = event(ModbusDirection.REQUEST, "10.0.0.5", "10.0.0.9", null);
    assertEquals("NA", ModbusEntityKey.of(noUnit).unitId());
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `./mvnw test -pl modules/domain -o -Dtest=ModbusEntityKeyTest`
Expected: compilation failure.

- [ ] **Step 3: Implement**

```java
@Override
public int hashCode() {
    return Objects.hash(sensor.value(), clientIp, serverIp, unitId);
}
```

Do NOT override `equals` — the generated one is correct and stays consistent with this hash. Comment the WHY (Flink assigns key groups from `key.hashCode()`), not the how.

- [ ] **Step 4: Run to verify it passes**

Run: `./mvnw test -pl modules/domain -o`

- [ ] **Step 5: Commit**

```bash
git add modules/domain/src/main/java/io/netsecml/platform/domain/feature/ModbusEntityKey.java \
        modules/domain/src/test/java/io/netsecml/platform/domain/feature/ModbusEntityKeyTest.java
git commit -m "feat(domain): add ModbusEntityKey with a JVM-stable hash"
```

---

### Task 6: `ModbusEntityState` — segments, pending TIDs and trailing windows

**Files:**
- Create: `modules/domain/src/main/java/io/netsecml/platform/domain/feature/ModbusEntityState.java`
- Test: `modules/domain/src/test/java/io/netsecml/platform/domain/feature/ModbusEntityStateTest.java`

**Interfaces:**
- Produces:
  - `ModbusEntityState.empty() → ModbusEntityState`
  - `boolean startsNewSegment(double ts)` — Ruling 1's predicate
  - `ModbusEntityState resetForNewSegment()`
  - read accessors: `Double lastTs()`, `Integer prevFunctionCode()`, `Double lastAddress()`, `Double lastQuantity()`, `int outstandingRequests()`, `boolean hasPending(String tid)`, `Double pendingTs(String tid)`, `int windowCount1s(double ts)`, `int windowCount10s(double ts)`, `int windowCount60s(double ts)`, `int uniqueFunctions10s(double ts)`, `int uniqueAddresses10s(double ts)`, `int readCount10s(double ts)`, `int writeCount10s(double ts)`
  - `ModbusEntityState afterEvent(double ts, int functionCode, String tid, ModbusDirection direction, Double address, Double quantity)` — Ruling 5's mutation, returning the new state

This class is the heart of the unit. It is pure Java with no Flink, so it can be tested exhaustively and fast.

**Window semantics (Ruling 3).** Purge with `storedTs <= ts - window`, so an event exactly `window` seconds old is excluded. The current event is appended in `afterEvent`, and the extractor reads window counts **after** that append — so every window count includes the current event. Keep the 10-second window as a deque of `(ts, functionCode, addressPresent, address, isRead, isWrite)` so purging can decrement the function and address counters and the read/write tallies exactly as the upstream engine does.

**State must be bounded.** The three deques are naturally bounded by their time windows. The pending-TID map is NOT: a stream of requests that never get responses grows it without limit. Cap it at 4096 entries, evicting the oldest by timestamp, and comment that the cap exists because a request flood is precisely one of the attack shapes this detector is for. Record the cap as a known limit in Task 11.

- [ ] **Step 1: Write the failing test**

```java
@Test
void aGapLongerThanFifteenSecondsStartsANewSegment() {
    ModbusEntityState state = ModbusEntityState.empty()
        .afterEvent(1000.0, 3, "17", ModbusDirection.REQUEST, 40001.0, 2.0);
    assertTrue(state.startsNewSegment(1015.01));
    assertFalse(state.startsNewSegment(1015.0), "exactly 15s is still the same segment");
}

@Test
void aNegativeGapAlsoStartsANewSegment() {
    // The upstream engine raises here, because offline it has already asserted a
    // strictly increasing event index. A streaming operator must not fail the job
    // on one out-of-order record, and resetting keeps in-segment inter-arrival
    // within [0, 15] by construction.
    ModbusEntityState state = ModbusEntityState.empty()
        .afterEvent(1000.0, 3, "17", ModbusDirection.REQUEST, 40001.0, 2.0);
    assertTrue(state.startsNewSegment(999.9));
}

@Test
void anEmptyStateAlwaysStartsANewSegment() {
    assertTrue(ModbusEntityState.empty().startsNewSegment(1000.0));
}

@Test
void aRequestDoesNotCountItselfAsOutstanding() {
    // The contract's transaction_rule: compute before-event features, mutate
    // pending state afterwards. outstanding_requests_before_event is named for it.
    ModbusEntityState before = ModbusEntityState.empty();
    assertEquals(0, before.outstandingRequests());
    ModbusEntityState after = before.afterEvent(1000.0, 3, "17", ModbusDirection.REQUEST, null, null);
    assertEquals(1, after.outstandingRequests());
}

@Test
void aMatchedResponseClearsItsPendingTid() {
    ModbusEntityState state = ModbusEntityState.empty()
        .afterEvent(1000.0, 3, "17", ModbusDirection.REQUEST, null, null)
        .afterEvent(1000.5, 3, "17", ModbusDirection.RESPONSE, null, null);
    assertEquals(0, state.outstandingRequests());
    assertFalse(state.hasPending("17"));
}

@Test
void theTenSecondWindowExcludesAnEventExactlyTenSecondsOld() {
    // Half-open (t-w, t]: the upstream purge is `stored <= ts - w`.
    ModbusEntityState state = ModbusEntityState.empty()
        .afterEvent(1000.0, 3, "17", ModbusDirection.REQUEST, null, null);
    assertEquals(1, state.windowCount10s(1009.99));
    assertEquals(0, state.windowCount10s(1010.0));
}

@Test
void purgingTheTenSecondWindowDecrementsItsFunctionAndAddressCounters() {
    ModbusEntityState state = ModbusEntityState.empty()
        .afterEvent(1000.0, 3, "17", ModbusDirection.REQUEST, 40001.0, null)
        .afterEvent(1001.0, 6, "18", ModbusDirection.REQUEST, 40002.0, null);
    assertEquals(2, state.uniqueFunctions10s(1001.0));
    assertEquals(2, state.uniqueAddresses10s(1001.0));
    assertEquals(1, state.uniqueFunctions10s(1010.5), "the FC-3 event has aged out");
    assertEquals(1, state.uniqueAddresses10s(1010.5));
}

@Test
void functionCode23CountsInBothTheReadAndWriteTallies() {
    ModbusEntityState state = ModbusEntityState.empty()
        .afterEvent(1000.0, 23, "17", ModbusDirection.REQUEST, null, null);
    assertEquals(1, state.readCount10s(1000.0));
    assertEquals(1, state.writeCount10s(1000.0));
}

@Test
void anAbsentAddressIsNotCountedAsAUniqueAddress() {
    ModbusEntityState state = ModbusEntityState.empty()
        .afterEvent(1000.0, 3, "17", ModbusDirection.REQUEST, null, null);
    assertEquals(0, state.uniqueAddresses10s(1000.0));
}

@Test
void resetForNewSegmentClearsEverything() {
    ModbusEntityState state = ModbusEntityState.empty()
        .afterEvent(1000.0, 3, "17", ModbusDirection.REQUEST, 40001.0, 2.0)
        .resetForNewSegment();
    assertNull(state.lastTs());
    assertNull(state.prevFunctionCode());
    assertNull(state.lastAddress());
    assertNull(state.lastQuantity());
    assertEquals(0, state.outstandingRequests());
    assertEquals(0, state.windowCount60s(1000.0));
}

@Test
void theLastAddressSurvivesAnEventWithNoAddress() {
    // "previous APPLICABLE address" -- the delta reaches back past events that
    // carried none, which is why lastAddress is only updated when one is present.
    ModbusEntityState state = ModbusEntityState.empty()
        .afterEvent(1000.0, 3, "17", ModbusDirection.REQUEST, 40001.0, null)
        .afterEvent(1001.0, 3, "18", ModbusDirection.REQUEST, null, null);
    assertEquals(40001.0, state.lastAddress());
}

@Test
void thePendingMapIsCappedSoARequestFloodCannotGrowItWithoutLimit() {
    ModbusEntityState state = ModbusEntityState.empty();
    for (int i = 0; i < 5000; i++) {
        state = state.afterEvent(1000.0 + i * 0.001, 3, "tid-" + i, ModbusDirection.REQUEST, null, null);
    }
    assertEquals(4096, state.outstandingRequests());
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `./mvnw test -pl modules/domain -o -Dtest=ModbusEntityStateTest`
Expected: compilation failure.

- [ ] **Step 3: Implement**

Follow `RollingCounters`' shape: a class in `domain.feature` with read accessors and a method returning the next state. It must be serializable by Flink — give it whatever `DnsWindowState` needed, and note in a comment which serializer Flink actually resolves for it (`DnsWindowState`'s components fall back to Kryo because they lack a public no-arg constructor; say plainly whichever applies here rather than assuming).

- [ ] **Step 4: Run to verify it passes**

Run: `./mvnw test -pl modules/domain -o`
Expected: previous count + 12.

- [ ] **Step 5: Commit**

```bash
git add modules/domain/src/main/java/io/netsecml/platform/domain/feature/ModbusEntityState.java \
        modules/domain/src/test/java/io/netsecml/platform/domain/feature/ModbusEntityStateTest.java
git commit -m "feat(domain): add the modbus causal state machine"
```

---

### Task 7: `ModbusFeatureExtractor` — all 42 values

**Files:**
- Create: `modules/application/src/main/java/io/netsecml/platform/application/feature/ModbusFeatureExtractor.java`
- Test: `modules/application/src/test/java/io/netsecml/platform/application/feature/ModbusFeatureExtractorTest.java`

**Interfaces:**
- Consumes: `ModbusEvent`, `ModbusEntityState` (the state **before** this event), `ModbusFunctionCode`.
- Produces: `ModbusFeatureExtractor.extract(ModbusEvent event, ModbusEntityState before, boolean newSegment) → float[]` of length 42.

The caller passes the state as it stands before the event and the `newSegment` decision already made, so this method is a pure function and the whole of Ruling 5 lives in the caller (Task 8). Window counts must be read from the state **after** the current event has been folded in — so `extract` takes the *before* state for groups C and the *after* state for group D. Give it both explicitly rather than mutating inside:

`extract(ModbusEvent event, ModbusEntityState before, ModbusEntityState after, boolean newSegment) → float[]`

Index-by-index rules, all from the upstream engine:

| Index | Feature | Rule |
|---|---|---|
| 0 | `is_response` | 1 iff direction is RESPONSE |
| 1–6 | `fc_1`..`fc_6` | 1 iff code equals that number |
| 7 | `fc_other` | 1 iff code ∉ {1..6}; exactly one of indices 1–7 is always 1 |
| 8 | `address_value` | the address, else 0 |
| 9 | `address_present` | 1 iff present |
| 10 | `quantity_value` | the quantity, else 0 |
| 11 | `quantity_present` | 1 iff present |
| 12 | `response_matched` | 1 iff RESPONSE **and** `matched`; a request is always 0 |
| 13–17 | request value summaries | present/count/min/max/mean; **all five stay 0 when the array is empty** |
| 18–22 | response value summaries | same |
| 23 | `prev_event_available` | 1 iff `before.lastTs() != null` |
| 24 | `inter_arrival_s` | `ts - before.lastTs()`, else 0 |
| 25 | `function_changed` | 1 iff previous exists and differs |
| 26/27 | `address_delta_valid`/`address_delta` | only when both current and previous applicable address exist |
| 28/29 | `quantity_delta_valid`/`quantity_delta` | same |
| 30 | `outstanding_requests_before_event` | `before.outstandingRequests()` |
| 31 | `response_without_request` | RESPONSE with no pending tid ⇒ 1; a request leaves it 0 |
| 32 | `request_overwrite_same_tid` | REQUEST whose tid is already pending ⇒ 1; a response leaves it 0 |
| 33/34 | `rtt_valid`/`rtt_s` | RESPONSE with a pending tid only; a request leaves both 0 |
| 35 | `event_rate_1s` | `after.windowCount1s(ts)` — a raw count (Ruling 4) |
| 36 | `event_rate_10s` | `after.windowCount10s(ts) / 10.0` |
| 37 | `event_rate_60s` | `after.windowCount60s(ts) / 60.0` |
| 38 | `unique_function_count_10s` | `after.uniqueFunctions10s(ts)` |
| 39 | `unique_address_count_10s` | `after.uniqueAddresses10s(ts)` |
| 40 | `read_ratio_10s` | `after.readCount10s(ts) / after.windowCount10s(ts)` |
| 41 | `write_ratio_10s` | `after.writeCount10s(ts) / after.windowCount10s(ts)` |

When `newSegment` is true, indices 23, 24, 25, 26, 28, 30, 33 and 34 are all 0 by construction because the reset state carries nothing — assert it rather than special-casing it.

- [ ] **Step 1: Write the failing test**

```java
@Test
void theVectorIsExactlyFortyTwoValues() {
    assertEquals(42, extractFor(request(1000.0, 3, "17")).length);
}

@Test
void exactlyOneFunctionIndicatorIsSetForEveryFunctionCode() {
    for (int code : new int[] {1, 2, 3, 4, 5, 6, 7, 23, 43, 99}) {
        float[] v = extractFor(request(1000.0, code, "17"));
        float sum = 0f;
        for (int i = 1; i <= 7; i++) {
            sum += v[i];
        }
        assertEquals(1.0f, sum, "exactly one of fc_1..fc_other must be set for code " + code);
    }
}

@Test
void anUnenumeratedFunctionCodeSetsFcOther() {
    float[] v = extractFor(request(1000.0, 23, "17"));
    assertEquals(1.0f, v[7]);
}

@Test
void aRequestNeverCarriesResponseMatchedOrRtt() {
    float[] v = extractFor(request(1000.0, 3, "17"));
    assertEquals(0.0f, v[12], "response_matched is response-only");
    assertEquals(0.0f, v[33], "rtt_valid is response-only");
    assertEquals(0.0f, v[34], "rtt_s is response-only");
}

@Test
void aResponseWithoutAPendingRequestIsFlagged() {
    float[] v = extractFor(response(1000.0, 3, "17", ModbusEntityState.empty()));
    assertEquals(1.0f, v[31]);
    assertEquals(0.0f, v[33], "an unmatched response has no valid rtt");
}

@Test
void aMatchedResponseCarriesItsRoundTripTime() {
    ModbusEntityState before = ModbusEntityState.empty()
        .afterEvent(1000.0, 3, "17", ModbusDirection.REQUEST, null, null);
    float[] v = extractFor(response(1000.25, 3, "17", before));
    assertEquals(0.0f, v[31], "it had a pending request");
    assertEquals(1.0f, v[33]);
    assertEquals(0.25f, v[34], 1e-6f);
}

@Test
void aRequestReusingAPendingTidIsFlagged() {
    ModbusEntityState before = ModbusEntityState.empty()
        .afterEvent(1000.0, 3, "17", ModbusDirection.REQUEST, null, null);
    float[] v = extractFor(requestAgainst(1000.5, 3, "17", before));
    assertEquals(1.0f, v[32]);
}

@Test
void emptyValueArraysLeaveAllFiveSummariesAtZero() {
    // The upstream engine only writes the summaries when the list is non-empty,
    // so *_present stays 0 too -- an empty array is "absent", not "present, count 0".
    float[] v = extractFor(request(1000.0, 3, "17"));
    for (int i = 13; i <= 22; i++) {
        assertEquals(0.0f, v[i], "index " + i + " must stay zero for absent values");
    }
}

@Test
void requestValueSummariesAreCountMinMaxMean() {
    float[] v = extractFor(requestWithValues(new double[] {4.0, 8.0, 6.0}));
    assertEquals(1.0f, v[13]);
    assertEquals(3.0f, v[14]);
    assertEquals(4.0f, v[15]);
    assertEquals(8.0f, v[16]);
    assertEquals(6.0f, v[17], 1e-6f);
}

@Test
void aSegmentStartZeroesEveryCausalFeature() {
    float[] v = extractAtSegmentStart(request(1000.0, 3, "17"));
    for (int i : new int[] {23, 24, 25, 26, 28, 30, 33, 34}) {
        assertEquals(0.0f, v[i], "index " + i + " must be zero at a segment start");
    }
}

@Test
void theReadAndWriteRatiosDivideByTheTenSecondWindowCountNotByTen() {
    // Two events in the window, one of them a read: the ratio is 1/2, not 1/10.
    ModbusEntityState before = ModbusEntityState.empty()
        .afterEvent(1000.0, 5, "16", ModbusDirection.REQUEST, null, null);
    float[] v = extractFor(requestAgainst(1000.5, 3, "17", before));
    assertEquals(0.5f, v[40], 1e-6f);
    assertEquals(0.5f, v[41], 1e-6f);
}

@Test
void theCurrentEventIsCountedInItsOwnWindows() {
    float[] v = extractFor(request(1000.0, 3, "17"));
    assertEquals(1.0f, v[35], "event_rate_1s is a raw count including this event");
    assertEquals(0.1f, v[36], 1e-6f, "one event in the 10s window is 1/10");
    assertEquals(1.0f / 60.0f, v[37], 1e-6f);
}

@Test
void theAddressDeltaReachesPastAnEventThatCarriedNoAddress() {
    ModbusEntityState before = ModbusEntityState.empty()
        .afterEvent(1000.0, 3, "16", ModbusDirection.REQUEST, 40001.0, null)
        .afterEvent(1000.5, 3, "17", ModbusDirection.REQUEST, null, null);
    float[] v = extractFor(requestWithAddress(1001.0, 40005.0, before));
    assertEquals(1.0f, v[26]);
    assertEquals(4.0f, v[27], 1e-6f);
}
```

Define every helper (`request`, `response`, `requestAgainst`, `requestWithValues`, `requestWithAddress`, `extractFor`, `extractAtSegmentStart`) in the test class — nothing in the codebase provides them.

- [ ] **Step 2: Run to verify it fails**

Run: `./mvnw test -pl modules/application -o -Dtest=ModbusFeatureExtractorTest`
Expected: compilation failure.

- [ ] **Step 3: Implement**

Write the extractor against the table above. Read indices from `ModbusFeatureSchemaV1.SCHEMA` by name at class-init time rather than hard-coding integer literals in the body, so a schema edit cannot silently misalign the vector.

- [ ] **Step 4: Run to verify it passes**

Run: `./mvnw test -pl modules/application -o`
Expected: previous count + 13.

- [ ] **Step 5: Commit**

```bash
git add modules/application/src/main/java/io/netsecml/platform/application/feature/ModbusFeatureExtractor.java \
        modules/application/src/test/java/io/netsecml/platform/application/feature/ModbusFeatureExtractorTest.java
git commit -m "feat(application): extract the frozen 42-value modbus vector"
```

---

### Task 8: `ModbusBuildFeaturesUseCase`

**Files:**
- Create: `modules/application/src/main/java/io/netsecml/platform/application/usecase/ModbusBuildFeaturesUseCase.java`
- Test: `modules/application/src/test/java/io/netsecml/platform/application/usecase/ModbusBuildFeaturesUseCaseTest.java`

**Interfaces:**
- Consumes: `ModbusFeatureExtractor`, `ModbusEntityState`, `FeatureSchemaRegistry`.
- Produces: `ModbusBuildFeaturesUseCase implements BuildFeaturesUseCase<ModbusEvent, ModbusEntityState>` — `FeatureBuildResult<ModbusEntityState> build(ModbusEvent event, ModbusEntityState currentState)`. Read `FeatureBuildResult` before writing this; it is the existing carrier for (vector, next state).

This class owns Ruling 5 end to end, in this exact order:

1. decide `newSegment = currentState.startsNewSegment(ts)`
2. `before = newSegment ? currentState.resetForNewSegment() : currentState`
3. `after = before.afterEvent(...)`
4. `values = ModbusFeatureExtractor.extract(event, before, after, newSegment)`
5. build the `FeatureVector` with width, id and hash from `FeatureSchemaRegistry.byLogType(LogType.MODBUS)` — never from a literal
6. `qualityFlags` = `QualityFlags.MODBUS_OUT_OF_ORDER` when the segment reset was caused by a negative gap, else `QualityFlags.NONE`

Add `MODBUS_OUT_OF_ORDER = 2` to `QualityFlags` with a comment. Per that class's existing bit-layout convention, bit 0 is the shared `CONN_ENRICHMENT_ABSENT` and bit 1 is each protocol's own first flag, read relative to `log_type` — so modbus reuses bit 1 exactly as DNS does for `DNS_RESPONSE_ABSENT`. Note in the comment that modbus never sets bit 0, because `modbus-feature-v1` carries no common tier.

Take a `Clock` in the constructor for `producedAt`, as every other `BuildFeaturesUseCase` implementation does.

- [ ] **Step 1: Write the failing test**

```java
@Test
void theVectorTakesItsWidthIdAndHashFromTheRegisteredSchema() {
    FeatureBuildResult<ModbusEntityState> result =
        useCase().build(request(1000.0, 3, "17"), ModbusEntityState.empty());
    assertEquals(42, result.vector().values().length);
    assertEquals("modbus-feature-v1", result.vector().schemaId());
    assertEquals(ModbusFeatureSchemaV1.CONTENT_HASH, result.vector().schemaHash());
}

@Test
void aRequestDoesNotCountItselfInOutstandingRequests() {
    // Ruling 5 through the whole use case, not just the state machine: the
    // emitted value must be the before-event count.
    FeatureBuildResult<ModbusEntityState> result =
        useCase().build(request(1000.0, 3, "17"), ModbusEntityState.empty());
    assertEquals(0.0f, result.vector().values()[30]);
    assertEquals(1, result.state().outstandingRequests(), "but the state advanced");
}

@Test
void aSixteenSecondGapResetsTheSegmentWithoutFlagging() {
    ModbusEntityState state = ModbusEntityState.empty()
        .afterEvent(1000.0, 3, "17", ModbusDirection.REQUEST, null, null);
    FeatureBuildResult<ModbusEntityState> result = useCase().build(request(1016.0, 3, "18"), state);
    assertEquals(0.0f, result.vector().values()[23], "prev_event_available is zeroed");
    assertEquals(QualityFlags.NONE, result.vector().qualityFlags());
}

@Test
void anOutOfOrderRecordResetsTheSegmentAndFlagsIt() {
    // The upstream engine raises here. A streaming operator resets instead, and
    // says so in qualityFlags so the condition stays observable.
    ModbusEntityState state = ModbusEntityState.empty()
        .afterEvent(1000.0, 3, "17", ModbusDirection.REQUEST, null, null);
    FeatureBuildResult<ModbusEntityState> result = useCase().build(request(999.5, 3, "18"), state);
    assertEquals(QualityFlags.MODBUS_OUT_OF_ORDER, result.vector().qualityFlags());
    assertEquals(0.0f, result.vector().values()[23]);
}

@Test
void inSegmentInterArrivalIsAlwaysWithinZeroAndFifteen() {
    // The invariant the upstream engine asserts. After the segment rule it holds
    // by construction, so this test is what keeps the rule honest.
    ModbusEntityState state = ModbusEntityState.empty()
        .afterEvent(1000.0, 3, "17", ModbusDirection.REQUEST, null, null);
    for (double ts : new double[] {1000.0, 1007.5, 1015.0}) {
        float ia = useCase().build(request(ts, 3, "18"), state).vector().values()[24];
        assertTrue(ia >= 0.0f && ia <= 15.0f, "inter_arrival_s out of range: " + ia);
    }
}

@Test
void producedAtComesFromTheInjectedClock() {
    Instant fixed = Instant.parse("2026-09-21T10:00:00Z");
    ModbusBuildFeaturesUseCase useCase =
        new ModbusBuildFeaturesUseCase(Clock.fixed(fixed, ZoneOffset.UTC));
    assertEquals(fixed, useCase.build(request(1000.0, 3, "17"), ModbusEntityState.empty())
        .vector().producedAt());
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `./mvnw test -pl modules/application -o -Dtest=ModbusBuildFeaturesUseCaseTest`
Expected: compilation failure.

- [ ] **Step 3: Implement**

- [ ] **Step 4: Run to verify it passes**

Run: `./mvnw test -pl modules/application -o` and `./mvnw test -pl modules/domain -o`

- [ ] **Step 5: Commit**

```bash
git add modules/domain/src/main/java/io/netsecml/platform/domain/feature/QualityFlags.java \
        modules/application/src/main/java/io/netsecml/platform/application/usecase/ModbusBuildFeaturesUseCase.java \
        modules/application/src/test/java/io/netsecml/platform/application/usecase/ModbusBuildFeaturesUseCaseTest.java
git commit -m "feat(application): build modbus feature vectors, extract-then-mutate"
```

---

### Task 9: The Flink operators

**Files:**
- Create: `modules/adapter-flink/src/main/java/io/netsecml/platform/adapter/flink/process/ModbusParseMapValidateFunction.java`
- Create: `modules/adapter-flink/src/main/java/io/netsecml/platform/adapter/flink/process/ModbusEntityKeySelector.java`
- Create: `modules/adapter-flink/src/main/java/io/netsecml/platform/adapter/flink/process/ModbusFeatureProcessFunction.java`
- Test: `modules/adapter-flink/src/test/java/io/netsecml/platform/adapter/flink/process/ModbusFeatureProcessFunctionTest.java`

**Interfaces:**
- Consumes: `JsonZeekModbusParser`, `ModbusEventMapper`, `ModbusBuildFeaturesUseCase`, `ModbusEntityKey`.
- Produces: the three operators above; `ModbusParseMapValidateFunction` reuses `ParseMapValidateFunction.REJECTED_TAG` for its DLQ side output.

Mirror `DnsParseMapValidateFunction` and `DnsFeatureProcessFunction` exactly. State is a single `ValueState<ModbusEntityState>` named `modbus-entity-state`.

**No TTL, and say so.** `ConnFeatureProcessFunction` and `DnsFeatureProcessFunction` both lack one, so the key set grows forever; this operator inherits that gap and Task 11 records it. Do not add a TTL here — it would make this operator inconsistent with its two siblings and belongs in a unit that fixes all three.

Follow `DnsFeatureProcessFunctionTest` for harness setup, including a snapshot/restore round trip — that test exists because state that cannot be restored is invisible until a real restart.

- [ ] **Step 1: Write the failing test**

```java
@Test
void oneRecordProducesOneFeatureVectorOfFortyTwoValues() throws Exception {
    OneInputStreamOperatorTestHarness<ModbusEvent, FeatureVector> harness = harness();
    harness.open();
    harness.processElement(new StreamRecord<>(request(1000.0, 3, "17")));
    List<FeatureVector> out = harness.extractOutputValues();
    assertEquals(1, out.size());
    assertEquals(42, out.get(0).values().length);
    harness.close();
}

@Test
void stateIsKeptPerEntityKeySoTwoUnitsDoNotShareAWindow() throws Exception {
    OneInputStreamOperatorTestHarness<ModbusEvent, FeatureVector> harness = harness();
    harness.open();
    harness.processElement(new StreamRecord<>(requestForUnit(1000.0, "1")));
    harness.processElement(new StreamRecord<>(requestForUnit(1000.5, "2")));
    List<FeatureVector> out = harness.extractOutputValues();
    assertEquals(1.0f, out.get(1).values()[35],
        "unit 2's 1s window must count only its own event");
    harness.close();
}

@Test
void entityStateSurvivesASnapshotRestoreRoundTrip() throws Exception {
    OneInputStreamOperatorTestHarness<ModbusEvent, FeatureVector> first = harness();
    first.open();
    first.processElement(new StreamRecord<>(request(1000.0, 3, "17")));
    OperatorSubtaskState snapshot = first.snapshot(1L, 1L);
    first.close();

    OneInputStreamOperatorTestHarness<ModbusEvent, FeatureVector> second = harness();
    second.initializeState(snapshot);
    second.open();
    second.processElement(new StreamRecord<>(request(1000.5, 3, "18")));
    assertEquals(1.0f, second.extractOutputValues().get(0).values()[23],
        "prev_event_available proves the restored state was seen");
    second.close();
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `./mvnw test -pl modules/adapter-flink -o -Dtest=ModbusFeatureProcessFunctionTest`
Expected: compilation failure.

- [ ] **Step 3: Implement**

- [ ] **Step 4: Run to verify it passes**

Run: `./mvnw test -pl modules/adapter-flink -o`
Expected: previous count + 3.

- [ ] **Step 5: Commit**

```bash
git add modules/adapter-flink/src/main/java/io/netsecml/platform/adapter/flink/process/ModbusParseMapValidateFunction.java \
        modules/adapter-flink/src/main/java/io/netsecml/platform/adapter/flink/process/ModbusEntityKeySelector.java \
        modules/adapter-flink/src/main/java/io/netsecml/platform/adapter/flink/process/ModbusFeatureProcessFunction.java \
        modules/adapter-flink/src/test/java/io/netsecml/platform/adapter/flink/process/ModbusFeatureProcessFunctionTest.java
git commit -m "feat(adapter-flink): window modbus events into the frozen vector"
```

---

### Task 10: Wiring both jobs

**Files:**
- Modify: `modules/bootstrap-online-job/src/main/java/io/netsecml/platform/bootstrap/online/OnlineFeatureJob.java`
- Modify: `modules/bootstrap-archive-job/src/main/java/io/netsecml/platform/bootstrap/archive/ArchiveJob.java`
- Modify: `.env.example`
- Test: `OnlineFeatureJobTopologyTest`, `ArchiveJobTopologyTest`

**Interfaces:**
- Produces: a third `ProtocolTopics` argument on `OnlineFeatureJob.build(...)`; `ArchiveJob.connDnsAndModbusChains(6 topics)`.

Topics: `netsec.modbus.feature-vector.v1`, `netsec.modbus.dlq.v1`, input `netsec.modbus.raw.v1`.

New operator uids, all additive: `modbus-source`, `modbus-parse`, `modbus-features`, `modbus-sink`, `modbus-dlq-sink` (online); `modbus-feature-vector-source`, `modbus-feature-vector-row`, `modbus-feature-vectors-clickhouse-sink` and the matching three for the DLQ chain (archive).

**Conn's and dns's existing uids are checkpoint state identity — do not rename, reorder or regularise any of them.** `featureVectorChain` derives them from a prefix that is empty for `CONN`, which is what keeps conn's three at their historical spellings. **Do not touch `ArchiveJob.CONSUMER_GROUP`**: it is the string `"conn-archive-job"` used as the Kafka consumer group, and renaming it would reset committed offsets. It is deliberately not the job name.

`connAndDnsChains` is named and shaped for exactly two protocols — a recorded known seam. Rather than adding a third pair of parameters to it, add `connDnsAndModbusChains(...)` alongside it and have `main()` call the new one, leaving the old method and its tests untouched.

**Serialization schemas must be anonymous classes, never lambdas.** This repo has already lost `env.execute()` three times to lambdas erasing their generic type.

- [ ] **Step 1: Write the failing topology tests**

```java
@Test
void theThreeProtocolTopologyCarriesModbusUidsAndLeavesConnsUntouched() {
    Set<String> uids = uidsOf(buildThreeProtocol());
    assertTrue(uids.containsAll(Set.of(
        "modbus-source", "modbus-parse", "modbus-features", "modbus-sink", "modbus-dlq-sink")));
    assertTrue(uids.containsAll(CONN_UIDS), "conn's historical uids must be byte-identical");
}

@Test
void sixChainsProduceEighteenDistinctUids() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    ArchiveJob.build(env, "localhost:9092", ArchiveJob.connDnsAndModbusChains(
        "netsec.conn.feature-vector.v1", "netsec.conn.dlq.v1",
        "netsec.dns.feature-vector.v1", "netsec.dns.dlq.v1",
        "netsec.modbus.feature-vector.v1", "netsec.modbus.dlq.v1"), config());
    assertEquals(18, uidsOf(env).size(), "a uid collision would silently merge two chains' state");
}
```

- [ ] **Step 2: Run to verify they fail**

Run: `./mvnw test -pl modules/bootstrap-online-job -o -Dtest=OnlineFeatureJobTopologyTest`
Expected: compilation failure.

- [ ] **Step 3: Implement**

`.env.example` gains `MODBUS_RAW_TOPIC=netsec.modbus.raw.v1`, `MODBUS_FEATURE_VECTOR_TOPIC=netsec.modbus.feature-vector.v1`, `MODBUS_DLQ_TOPIC=netsec.modbus.dlq.v1`.

- [ ] **Step 4: Run to verify they pass**

```
./mvnw test -pl modules/bootstrap-online-job -o -Dtest=OnlineFeatureJobTopologyTest
./mvnw test -pl modules/bootstrap-archive-job -o -Dtest=ArchiveJobTopologyTest
```

Record both before and after counts. Never run either module unfiltered.

- [ ] **Step 5: Commit**

```bash
git add modules/bootstrap-online-job/src/main/java/io/netsecml/platform/bootstrap/online/OnlineFeatureJob.java \
        modules/bootstrap-archive-job/src/main/java/io/netsecml/platform/bootstrap/archive/ArchiveJob.java \
        .env.example \
        modules/bootstrap-online-job/src/test/java/io/netsecml/platform/bootstrap/online/OnlineFeatureJobTopologyTest.java \
        modules/bootstrap-archive-job/src/test/java/io/netsecml/platform/bootstrap/archive/ArchiveJobTopologyTest.java
git commit -m "feat: wire modbus through both jobs"
```

---

### Task 11: End-to-end proof and documentation

**Files:**
- Modify: `modules/bootstrap-online-job/src/test/java/io/netsecml/platform/bootstrap/online/OnlineFeatureJobE2ETest.java`
- Modify: `modules/bootstrap-archive-job/src/test/java/io/netsecml/platform/bootstrap/archive/ArchiveJobE2ETest.java`
- Modify: `CLAUDE.md`

**This is the ONLY task permitted to run the end-to-end suites**, and only under these conditions: one suite at a time, never a bootstrap module unfiltered, nothing else running concurrently. `ClickHouseOutageTest` remains absolutely forbidden — it has never run on this machine and must never be described as passing.

Both E2E classes are `@Testcontainers(disabledWithoutDocker = true)`, so a Docker hiccup turns the whole class green-but-empty. Report the full `Tests run: N, Failures: 0, Errors: 0, Skipped: 0` line verbatim for each; if `Skipped` is not 0, say so and do not claim the proof. This trap already hid a ClickHouse dedup query on this project that was syntactically invalid and could never have executed.

- [ ] **Step 1: Add the online end-to-end case**

A new method publishing a request and its matching response to `netsec.modbus.raw.v1`, asserting that two 42-value vectors arrive on `netsec.modbus.feature-vector.v1`, that the response's `rtt_valid` is 1 and its `rtt_s` equals the published gap, and that a malformed modbus record reaches `netsec.modbus.dlq.v1` and never conn's or dns's DLQ.

Keep the existing methods' assertions byte-identical. Prove it — this must print nothing:

```bash
git diff -w <base> -- modules/bootstrap-online-job/src/test/java/io/netsecml/platform/bootstrap/online/OnlineFeatureJobE2ETest.java | grep -E '^-.*assert'
```

- [ ] **Step 2: Add the archive end-to-end case**

A **new** method (do not mutate the existing four-chain method — it proves `ArchiveJob.main()`'s own wiring, and changing its chain set changes what it proves) publishing one 42-value modbus vector and one modbus rejection, asserting both land in ClickHouse under `log_type = 'modbus'`. Use its own database name, as the existing methods do. Apply the same byte-identical proof to this file.

- [ ] **Step 3: Run both, staged and alone**

```
./mvnw install -DskipTests -q -o
./mvnw test -pl modules/bootstrap-online-job -o -Dtest=OnlineFeatureJobE2ETest
./mvnw test -pl modules/bootstrap-archive-job -o -Dtest=ArchiveJobE2ETest
```

- [ ] **Step 4: Update CLAUDE.md**

Record what is now true:

- the data-flow line gains modbus and its topics
- **the common-tier invariant gains its scope clause**: platform-designed schemas lead with the common tier; a schema mirroring an externally frozen contract carries exactly what that contract specifies, and `modbus-feature-v1` (42 values) is the first of those
- event identity for `MODBUS` is `sensor:uid:tid:direction:ts_millis`
- the verification table gains the new suite counts and the two new end-to-end cases
- `ClickHouseOutageTest` stays recorded as never executed

New Known limits, each of which was ruled on deliberately rather than overlooked:

- **Per-key arrival order is a deployment requirement, not an enforced one.** The sensor's Kafka producer must partition by `(client_ip, server_ip)`. The upstream offline engine asserts a strictly increasing event index and refuses to run otherwise; Kafka offers no such guarantee, and the pending-TID machine, `rtt_s`, both deltas, `function_changed`, `inter_arrival_s` and the segment boundary all depend on it. An out-of-order record resets the segment and sets `MODBUS_OUT_OF_ORDER` rather than failing the job.
- **The pending-TID map is capped at 4096 per entity key**, evicting oldest-first. Uncapped it would grow without limit under a request flood — which is one of the attack shapes this detector exists to find.
- **`ModbusFeatureProcessFunction`'s keyed state has no TTL**, so the `(sensor, clientIp, serverIp, unitId)` key set grows forever. It inherits this from its conn and dns siblings; fixing all three belongs in its own unit.
- **`modbus-feature-v1` carries no conn-derived context.** A later model wanting it needs a `-v2`. The archived vectors are raw, so refitting stays possible.

Do not write a commit count into CLAUDE.md — one was before and went stale twice in a day. Give the command instead, as the file already does elsewhere.

- [ ] **Step 5: Commit**

```bash
git add modules/bootstrap-online-job/src/test/java/io/netsecml/platform/bootstrap/online/OnlineFeatureJobE2ETest.java \
        modules/bootstrap-archive-job/src/test/java/io/netsecml/platform/bootstrap/archive/ArchiveJobE2ETest.java \
        CLAUDE.md
git commit -m "test: prove modbus end to end, and record it"
```

---

## Self-Review

**Spec coverage.** §1 upstream authority → Task 1 commits the frozen contract and pins parity against it. §2 Stage 1 only → the plan contains no Stage 2 work. §3 preprocessing in ONNX → nothing in M1 transforms a value, which is the whole point. §4 the 42 features → Tasks 1, 6, 7. §5 source contract → Tasks 1, 3, 4. §6 event identity → Task 4. §7 keying, state, segments → Tasks 5, 6, 8. §8 arrival order → Ruling 1, Task 8, Task 11's known limit. §9 DECIDED 1 → Task 1's `theSchemaCarriesNoCommonTier` and Task 11's CLAUDE.md clause; DECIDED 2 → correctly absent, it belongs to M2. §10 M1 scope → Tasks 1–11. §11 parity proof → Task 1 (contract pinning), Task 7 (worked vectors), Task 8 (ordering pin), Tasks 6–8 (the engine's own invariants). §12 out of scope → nothing here implements Stage 2, S7comm, sequence assembly or scoring.

**Placeholder scan.** No "TBD", no "add appropriate error handling", no "similar to Task N". Every test step carries runnable code; every helper the tests use is explicitly called out as needing definition in its test class.

**Type consistency.** `ModbusEntityState` is created by `empty()` and advanced by `afterEvent(...)` in Tasks 6, 7, 8 and 9 with the same signature throughout. `ModbusFeatureExtractor.extract` takes `(event, before, after, newSegment)` in both Task 7's definition and Task 8's call. `ModbusEntityKey.of(ModbusEvent)` is defined in Task 5 and used in Task 9. `ModbusDirection` is nested in `ModbusEvent` (Task 2) and referenced by that name from Tasks 5–9. `QualityFlags.MODBUS_OUT_OF_ORDER` is added in Task 8 and asserted in Task 8.

**One known imperfection, stated rather than hidden.** Task 7's index table is the single place the 42 rules are written down, and it is prose. The tests pin thirteen of the rules directly and the schema-order test pins the layout, but a reviewer should read the extractor against `07b_materialize_feature_engine_v1.py`'s `process_capture` line by line rather than trusting the table — that file is the authority, and the table is a summary of it.
