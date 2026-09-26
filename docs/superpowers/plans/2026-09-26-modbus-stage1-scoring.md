# Modbus Stage 1 Scoring Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make two Modbus inputs match the delivered detector's training data, then score every Modbus feature vector with the frozen Stage 1 detector in the online job, publish one prediction per event to Kafka, and archive predictions to ClickHouse.

**Architecture:** Two input-fidelity fixes land first (Zeek's `values` string is parsed; a response borrows its pending request's address and quantity). Then `modbus-features` gains a side output of `(stream key, vector)`; a new keyed operator `modbus-score` keeps a 20-vector window of preprocessed vectors per stream, runs the delivered ONNX graph through a `SequenceScorer` port, and emits a `ModbusDetectorPrediction` (`WARMUP` / `NORMAL` / `ANOMALY` / `UNSCORABLE`) to `netsec.modbus.prediction.v1`; the archive job's ninth chain writes them to `modbus_detector_predictions`. The model travels as a SHA-pinned bundle under `models/`, mounted read-only.

**Tech Stack:** Java 21, Flink 2.2.1, ONNX Runtime Java 1.20.0, Jackson 2.17, JUnit 5, ClickHouse 25.8, bash + jq for deploy, Python 3 + pandas + pyarrow + numpy + onnxruntime for the oracle generator only.

**Spec:** `docs/superpowers/specs/2026-09-26-modbus-stage1-scoring-design.md` (read it; this plan argues from it).

## Global Constraints

- Branch `feat/modbus-scoring` (already created, holds the spec). Java package root `io.netsecml.platform`.
- Hexagonal: `domain` imports no Jackson, Flink, Kafka, ONNX Runtime or ClickHouse; `ports` depends only on `domain`; `application` on `domain` + `ports`; adapters never import each other, except the recorded `adapter-flink` → `adapter-kafka`. Anything that needs two adapters (bundle loader + ONNX) lives in a bootstrap module.
- Records with array components copy defensively in the compact constructor **and** the accessor.
- Every code block carries a short comment saying why (the codebase's convention and the user's preference).
- Test commands: `./mvnw test -pl <module> -am -Dtest='<Class>' -Dsurefire.failIfNoSpecifiedTests=false`, then **read the actual `Tests run:` line** — a green build can run zero tests. Never `-pl` without `-am`. Never run Testcontainers tests or start the deploy stack on this machine.
- Frozen detector values come from the bundle, never code: input `sequence_20x42` `[?,20,42]`; outputs `modbus_dense_autoencoder` `[?,20,42]` and `modbus_causal_next_event_predictor` `[?,19,42]`; thresholds dense `0.2483385056257248`, temporal `0.4121147692203522` (tests may assert these numbers; main code must read them).
- State names are checkpoint identity: `modbus-entity-state-v2` (renamed once, in Task 2) and `modbus-score-window`. Never rename either again.
- Operator uids: online `modbus-score`, `modbus-prediction-sink`; archive `modbus-prediction-source`, `modbus-prediction-row`, `modbus-predictions-clickhouse-sink`.
- Topic `netsec.modbus.prediction.v1` (1 partition, 168 h). Env: `MODBUS_PREDICTION_TOPIC`, `MODBUS_DETECTOR_BUNDLE` (deploy default `modbus-stage1-detector/v1`; empty disables scoring), `NETSEC_MODELS_DIR` (`/opt/netsec/models`).
- Contracts in `contracts/` are immutable: add files, never edit existing ones.
- Never edit `modules/application/src/test/java/io/netsecml/platform/application/feature/reference/` (the verbatim upstream engine copy) or anything under `models/modbus/` (the delivery).
- The test fixture bundle `tests/fixtures/models/modbus-stage1-detector/v1/` is produced only by the packaging script (Task 6); its `model.onnx` (2.8 MB) is committed, following the `conn-demo-v1` fixture precedent.
- Commit messages end with these two lines; where a task's commit step shows `<attribution lines>`, it means exactly these:
  ```
  Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
  Claude-Session: https://claude.ai/code/session_01WXKAFPjfNQKQ6biy659oN3
  ```

## Review Focus

1. An older `deploy/.env` with no `MODBUS_DETECTOR_BUNDLE` line must score with the default bundle, while one that sets it empty must not score — pinned in Task 15.
2. Zeek `values` edge formats — one value, `T`/`F`, `""`, spaces, a trailing comma, `\x00\x00`, `see …log`, an 11-digit number — must parse or be absent, never reject a record — pinned in Task 1.
3. A response whose request is no longer pending (unanswered, evicted by the 4096 cap, or across a >15 s segment start) must not borrow an address — pinned in Task 2.
4. A window filled under bundle v1 and restored under v2 must never be scored by v2 — pinned in Task 5 (`aWindowFromAnotherBundleIsEmptiedFirst`).
5. An older `deploy/.env` with no `MODBUS_PREDICTION_TOPIC` line must still get the topic created (`create_topics` otherwise aborts `up`) — pinned in Task 15.

---

### Task 1: Parse Zeek's `values` string (spec 2.1, F1)

**Files:**
- Create: `modules/adapter-kafka/src/main/java/io/netsecml/platform/adapter/kafka/mapper/ZeekModbusValues.java`
- Create: `modules/adapter-kafka/src/test/java/io/netsecml/platform/adapter/kafka/mapper/ZeekModbusValuesTest.java`
- Modify: `modules/adapter-kafka/src/main/java/io/netsecml/platform/adapter/kafka/dto/ZeekModbusRecord.java` (append a `values` component; add the 16-argument constructor)
- Modify: `modules/adapter-kafka/src/main/java/io/netsecml/platform/adapter/kafka/mapper/ModbusEventMapper.java:127-134`
- Test: `modules/adapter-kafka/src/test/java/io/netsecml/platform/adapter/kafka/mapper/ModbusEventMapperTest.java`, `modules/adapter-kafka/src/test/java/io/netsecml/platform/adapter/kafka/parser/JsonZeekModbusParserTest.java`

**Interfaces:**
- Produces: `ZeekModbusValues.parse(String values) -> double[]` (empty array = absent); `ZeekModbusRecord.values()`; the record keeps a 16-argument constructor with the old signature.

- [ ] **Step 1: Write the failing parser tests**

```java
package io.netsecml.platform.adapter.kafka.mapper;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

// icsnpp-modbus v1.0.0's `values` string, in every shape the real ICSNPP
// sample (tests/fixtures/zeek/) and Review Focus 2 name.
class ZeekModbusValuesTest {

    private static final double[] ABSENT = new double[0];

    @Test
    void registersParseAsNumbers() {
        assertArrayEquals(new double[]{170}, ZeekModbusValues.parse("170"));
        assertArrayEquals(new double[]{170, 170, 187, 204, 61316},
            ZeekModbusValues.parse("170,170,187,204,61316"));
    }

    @Test
    void coilsParseAsOneAndZero() {
        assertArrayEquals(new double[]{1, 0, 0, 1}, ZeekModbusValues.parse("T,F,F,T"));
        assertArrayEquals(new double[]{1}, ZeekModbusValues.parse("T"));
    }

    @Test
    void spacesAroundATokenAreIgnored() {
        assertArrayEquals(new double[]{170, 171}, ZeekModbusValues.parse("170, 171"));
    }

    @Test
    void anAbsentOrBlankStringIsAbsent() {
        assertArrayEquals(ABSENT, ZeekModbusValues.parse(null));
        assertArrayEquals(ABSENT, ZeekModbusValues.parse(""));
        assertArrayEquals(ABSENT, ZeekModbusValues.parse("   "));
    }

    // Anything not wholly numeric is absent, never a rejection: upstream's
    // parse_numeric_vector cannot read these either.
    @Test
    void aStringThatIsNotWhollyNumericIsAbsent() {
        assertArrayEquals(ABSENT, ZeekModbusValues.parse("\\x00\\x00"));
        assertArrayEquals(ABSENT, ZeekModbusValues.parse("see modbus_mask_write_register.log"));
        assertArrayEquals(ABSENT, ZeekModbusValues.parse("ILLEGAL_DATA_ADDRESS"));
        assertArrayEquals(ABSENT, ZeekModbusValues.parse("170,x"));
        assertArrayEquals(ABSENT, ZeekModbusValues.parse("170,"));
        assertArrayEquals(ABSENT, ZeekModbusValues.parse("1.5"));
        assertArrayEquals(ABSENT, ZeekModbusValues.parse("-3"));
    }

    // Eleven digits exceed any Modbus register and could overflow a parse.
    @Test
    void anOverlongNumberIsAbsent() {
        assertArrayEquals(ABSENT, ZeekModbusValues.parse("12345678901"));
    }
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `./mvnw test -pl modules/adapter-kafka -am -Dtest='ZeekModbusValuesTest' -Dsurefire.failIfNoSpecifiedTests=false`
Expected: COMPILATION ERROR — `cannot find symbol: class ZeekModbusValues`.

- [ ] **Step 3: Implement `ZeekModbusValues`**

```java
package io.netsecml.platform.adapter.kafka.mapper;

// icsnpp-modbus v1.0.0 writes a record's register or coil values into one
// `values` string: comma-separated decimals for registers ("170,171"), T/F for
// coils and discrete inputs ("T,F,F"). The detector was trained on numeric
// arrays built by upstream's own capture adapter, so this turns the string into
// the same numbers (spec section 2.1, F1). A string that is not wholly numeric
// -- "\x00\x00", "see modbus_mask_write_register.log", an exception name, "" --
// is absent: upstream's parse_numeric_vector cannot read such strings either,
// so its training data never held them.
public final class ZeekModbusValues {

    private static final double[] ABSENT = new double[0];

    // Ten digits hold any Modbus register (at most 65535) with room to spare,
    // and cannot overflow Long.parseLong.
    private static final int MAX_DIGITS = 10;

    private ZeekModbusValues() {
    }

    // The values as numbers, or an empty array when absent or not wholly numeric.
    public static double[] parse(String values) {
        // Absent or blank: no values at all.
        if (values == null || values.isBlank()) {
            return ABSENT;
        }
        // limit -1 keeps a trailing empty token, so "170," is not wholly numeric.
        String[] tokens = values.split(",", -1);
        double[] result = new double[tokens.length];
        for (int i = 0; i < tokens.length; i++) {
            String token = tokens[i].trim();
            if (token.equals("T")) {
                // A coil or discrete input that is on.
                result[i] = 1.0;
            } else if (token.equals("F")) {
                // A coil or discrete input that is off.
                result[i] = 0.0;
            } else if (isUnsignedDecimal(token)) {
                // A register value.
                result[i] = Long.parseLong(token);
            } else {
                // One non-numeric token makes the whole string absent.
                return ABSENT;
            }
        }
        return result;
    }

    // ASCII digits only (Character.isDigit would admit other scripts' digits).
    private static boolean isUnsignedDecimal(String token) {
        if (token.isEmpty() || token.length() > MAX_DIGITS) {
            return false;
        }
        for (int i = 0; i < token.length(); i++) {
            char c = token.charAt(i);
            if (c < '0' || c > '9') {
                return false;
            }
        }
        return true;
    }
}
```

- [ ] **Step 4: Run the parser tests to verify they pass**

Run: the Step 2 command. Expected: `Tests run: 7, Failures: 0, Errors: 0`.

- [ ] **Step 5: Write the failing mapper and binding tests**

Append to `ModbusEventMapperTest` (it already imports `List`, `ZeekModbusRecord`, `MappingResult`, `NetworkEvent` and uses `map(dto)` / `asModbusEvent(...)`; it builds records positionally with 16 arguments — `ts, uid, id_orig_h, id_resp_h, source_h, destination_h, is_orig, request_response, tid, unit, func, address, quantity, matched, request_values, response_values`):

```java
    // A record as icsnpp-modbus v1.0.0 writes it: `values` as a string and no
    // arrays. The 17th argument is the new `values` component.
    private static ZeekModbusRecord v1Record(boolean isOrig, String func, String values) {
        return new ZeekModbusRecord(1758000000.5, "CXY1", "10.0.0.5", "10.0.0.9", null, null,
            isOrig, null, 17, "1", func, null, null, null, null, null, values);
    }

    // F1: a response's `values` becomes its response values.
    @Test
    void aResponsesValuesStringBecomesItsResponseValues() {
        MappingResult<NetworkEvent> result = map(v1Record(false, "READ_HOLDING_REGISTERS", "170,171"));
        assertTrue(result.isValid());
        assertArrayEquals(new double[]{170, 171}, asModbusEvent(result.value()).responseValues());
        assertArrayEquals(new double[0], asModbusEvent(result.value()).requestValues());
    }

    // F1: a request's `values` (a write) becomes its request values; coils are 1/0.
    @Test
    void aRequestsValuesStringBecomesItsRequestValues() {
        MappingResult<NetworkEvent> result = map(v1Record(true, "WRITE_MULTIPLE_COILS", "T,F,T"));
        assertTrue(result.isValid());
        assertArrayEquals(new double[]{1, 0, 1}, asModbusEvent(result.value()).requestValues());
        assertArrayEquals(new double[0], asModbusEvent(result.value()).responseValues());
    }

    // F1: a `values` that is not wholly numeric leaves the record valid, valueless.
    @Test
    void aNonNumericValuesStringIsAbsentNotARejection() {
        MappingResult<NetworkEvent> result =
            map(v1Record(false, "READ_HOLDING_REGISTERS", "see modbus_mask_write_register.log"));
        assertTrue(result.isValid());
        assertArrayEquals(new double[0], asModbusEvent(result.value()).responseValues());
    }

    // F1: an array, when present, wins over `values`.
    @Test
    void anArrayWinsOverTheValuesString() {
        ZeekModbusRecord dto = new ZeekModbusRecord(1758000000.5, "CXY1", "10.0.0.5", "10.0.0.9", null, null,
            false, null, 17, "1", "READ_HOLDING_REGISTERS", null, null, null, null, List.of(7.0, 9.0), "170,171");
        assertArrayEquals(new double[]{7, 9}, asModbusEvent(map(dto).value()).responseValues());
    }
```

Append to `JsonZeekModbusParserTest` (`JsonZeekModbusParser.parse(byte[])` returns `MappingResult<ZeekModbusRecord>`):

```java
    // icsnpp-modbus v1.0.0's `values` string binds to the new component.
    @Test
    void theValuesStringBinds() {
        ZeekModbusRecord record = new JsonZeekModbusParser().parse(("{\"ts\":1.5,\"uid\":\"C1\",\"tid\":1,"
            + "\"func\":\"READ_COILS\",\"values\":\"T,F,F,F\"}").getBytes(java.nio.charset.StandardCharsets.UTF_8))
            .value();
        assertEquals("T,F,F,F", record.values());
    }
```

- [ ] **Step 6: Run them to verify they fail**

Run: `./mvnw test -pl modules/adapter-kafka -am -Dtest='ModbusEventMapperTest,JsonZeekModbusParserTest' -Dsurefire.failIfNoSpecifiedTests=false`
Expected: COMPILATION ERROR — no 17-argument `ZeekModbusRecord` constructor, no `values()`.

- [ ] **Step 7: Add the component and the old-signature constructor to `ZeekModbusRecord`**

Replace the record's last two components and closing brace (`@JsonProperty("request_values") … responseValues` … `) {` … `}`) with:

```java
    @JsonProperty("request_values") List<Double> requestValues,
    @JsonProperty("response_values") List<Double> responseValues,

    // icsnpp-modbus v1.0.0's own register/coil field: one comma-separated
    // string, read by ZeekModbusValues when neither array above is present
    // (spec section 2.1, F1).
    @JsonProperty("values") String values
) {
    // The record as it was before `values` existed, so every positional
    // caller keeps compiling; a record built this way has no `values`.
    public ZeekModbusRecord(double ts, String uid, String origHost, String respHost, String sourceHost,
                            String destinationHost, Boolean isOrig, String requestResponse, int tid,
                            String unitId, String func, Double address, Double quantity, Boolean matched,
                            List<Double> requestValues, List<Double> responseValues) {
        this(ts, uid, origHost, respHost, sourceHost, destinationHost, isOrig, requestResponse, tid, unitId,
            func, address, quantity, matched, requestValues, responseValues, null);
    }
}
```

If the parser test then shows Jackson binding through the 16-argument constructor (`values()` null), annotate the canonical constructor by declaring it explicitly with `@JsonCreator`; Jackson 2.17 normally prefers a record's canonical constructor, so this is a fallback only.

- [ ] **Step 8: Use `values` in `ModbusEventMapper` when the arrays are absent**

Replace the block at lines 127-134 (`MappingResult<double[]> requestValues = …` through the `responseValues` validity check) with:

```java
        MappingResult<double[]> requestValues = toValidatedArray(dto.requestValues(), "request_values");
        if (!requestValues.isValid()) {
            return MappingResult.invalid(requestValues.reason(), requestValues.detail());
        }
        MappingResult<double[]> responseValues = toValidatedArray(dto.responseValues(), "response_values");
        if (!responseValues.isValid()) {
            return MappingResult.invalid(responseValues.reason(), responseValues.detail());
        }

        // F1 (spec section 2.1): icsnpp-modbus v1.0.0 writes no arrays, only a
        // `values` string, which belongs to this record's own direction. An
        // array, when present, wins; a string that is not wholly numeric is
        // absent (ZeekModbusValues), never a rejection.
        double[] zeekValues = ZeekModbusValues.parse(dto.values());
        if (zeekValues.length > 0) {
            if (direction == ModbusEvent.ModbusDirection.REQUEST && requestValues.value().length == 0) {
                requestValues = MappingResult.valid(zeekValues);
            } else if (direction == ModbusEvent.ModbusDirection.RESPONSE && responseValues.value().length == 0) {
                responseValues = MappingResult.valid(zeekValues);
            }
        }
```

(`direction` is the mapper's resolved `ModbusEvent.ModbusDirection` local, assigned at lines 70-83, before this block.)

- [ ] **Step 9: Run the adapter-kafka tests**

Run: `./mvnw test -pl modules/adapter-kafka -am -Dtest='ZeekModbusValuesTest,ModbusEventMapperTest,JsonZeekModbusParserTest' -Dsurefire.failIfNoSpecifiedTests=false`
Expected: all pass; `ModbusEventMapperTest` 35 (31 + 4), `ZeekModbusValuesTest` 7, and the parser test count one higher than before.

- [ ] **Step 10: Run the real-Zeek record check**

Run: `./mvnw test -pl modules/bootstrap-online-job -am -Dtest='ZeekRecordCheckTest' -Dsurefire.failIfNoSpecifiedTests=false`
Expected: `Tests run: 8, Failures: 0` — still 45/48 Modbus accepted and 84/84 S7: F1 never rejects.

- [ ] **Step 11: Commit**

```bash
git add modules/adapter-kafka
git commit -m "fix(modbus): read Zeek's values string into request and response values

icsnpp-modbus v1.0.0 writes register and coil values as one comma-
separated string; the mapper read only the request_values and
response_values arrays, so every value summary was absent -- while
every training response carried values (spec section 2.1, F1).

<attribution lines>"
```

---

### Task 2: A response borrows its pending request's address and quantity (spec 2.1, F2)

**Files:**
- Create: `modules/domain/src/main/java/io/netsecml/platform/domain/feature/PendingRequest.java`
- Modify: `modules/domain/src/main/java/io/netsecml/platform/domain/feature/ModbusEntityState.java` (pending map value type; `pendingRequest`; `pendingTs`; `advance`; `evictOldestIfOverCap`)
- Modify: `modules/domain/src/main/java/io/netsecml/platform/domain/event/ModbusEvent.java` (add `withAddressAndQuantity`)
- Modify: `modules/application/src/main/java/io/netsecml/platform/application/usecase/ModbusBuildFeaturesUseCase.java:104-148`
- Modify: `modules/adapter-flink/src/main/java/io/netsecml/platform/adapter/flink/process/ModbusFeatureProcessFunction.java` (state name)
- Test: `modules/application/src/test/java/io/netsecml/platform/application/usecase/ModbusBuildFeaturesUseCaseTest.java`, `modules/adapter-flink/src/test/java/io/netsecml/platform/adapter/flink/process/ModbusFeatureProcessFunctionTest.java`

**Interfaces:**
- Produces: `record PendingRequest(double ts, Double address, Double quantity)`; `ModbusEntityState.pendingRequest(String tid) -> PendingRequest` (null when not pending); `ModbusEvent.withAddressAndQuantity(Double address, Double quantity) -> ModbusEvent`; state name `modbus-entity-state-v2`.

- [ ] **Step 1: Write the failing use-case tests**

Append to `ModbusBuildFeaturesUseCaseTest` (it builds its use case with a `useCase()` method; the indices are 0-based — `address_value` 8, `address_present` 9, `quantity_value` 10, `quantity_present` 11):

```java
    // An event on the fixed key (10.0.0.5 -> 10.0.0.9, unit 1) with its own
    // address and quantity, which may be null as Zeek v1.0.0 writes them.
    private static ModbusEvent event(double ts, ModbusDirection direction, String tid, Double address,
                                     Double quantity) {
        String uid = "u-" + ts;
        EventEnvelope envelope = new EventEnvelope(EventId.derive(new SensorId("sensor-eu-1"), uid),
            Instant.ofEpochMilli((long) (ts * 1000)), new SensorId("sensor-eu-1"), LogType.MODBUS, uid);
        boolean request = direction == ModbusDirection.REQUEST;
        return new ModbusEvent(envelope, ts, direction, request ? "10.0.0.5" : "10.0.0.9",
            request ? "10.0.0.9" : "10.0.0.5", 3, tid, "1", address, quantity, null, new double[0], new double[0]);
    }

    // F2: Zeek leaves a response's address off; it takes its request's.
    @Test
    void aResponseWithoutAddressTakesItsPendingRequestsAddressAndQuantity() {
        ModbusEntityState state = ModbusEntityState.empty();
        useCase().build(event(1000.0, ModbusDirection.REQUEST, "7", 100.0, 2.0), state);
        float[] v = useCase().build(event(1000.25, ModbusDirection.RESPONSE, "7", null, null), state).vector().values();
        assertEquals(100f, v[8], "address_value");
        assertEquals(1f, v[9], "address_present");
        assertEquals(2f, v[10], "quantity_value");
        assertEquals(1f, v[11], "quantity_present");
    }

    // F2: a response that carries its own address keeps it.
    @Test
    void aResponsesOwnAddressWins() {
        ModbusEntityState state = ModbusEntityState.empty();
        useCase().build(event(1000.0, ModbusDirection.REQUEST, "7", 100.0, 2.0), state);
        float[] v = useCase().build(event(1000.25, ModbusDirection.RESPONSE, "7", 300.0, null), state).vector().values();
        assertEquals(300f, v[8], "its own address");
        assertEquals(2f, v[10], "the request's quantity, since it had none");
    }

    // Review Focus 3: no pending request, no borrowing.
    @Test
    void anUnansweredResponseBorrowsNothing() {
        float[] v = useCase().build(event(1000.0, ModbusDirection.RESPONSE, "7", null, null),
            ModbusEntityState.empty()).vector().values();
        assertEquals(0f, v[9], "address_present");
        assertEquals(0f, v[11], "quantity_present");
    }

    // Review Focus 3: a >15 s gap starts a new segment, which forgets the request.
    @Test
    void aResponseAcrossASegmentStartBorrowsNothing() {
        ModbusEntityState state = ModbusEntityState.empty();
        useCase().build(event(1000.0, ModbusDirection.REQUEST, "7", 100.0, 2.0), state);
        float[] v = useCase().build(event(1016.0, ModbusDirection.RESPONSE, "7", null, null), state).vector().values();
        assertEquals(0f, v[9], "address_present");
    }

    // Review Focus 3: a request evicted by the 4096 cap is no longer pending.
    @Test
    void aResponseWhoseRequestWasEvictedBorrowsNothing() {
        ModbusEntityState state = ModbusEntityState.empty();
        useCase().build(event(1000.0, ModbusDirection.REQUEST, "evicted", 100.0, 2.0), state);
        for (int i = 0; i < 4096; i++) {
            useCase().build(event(1000.0 + (i + 1) * 0.001, ModbusDirection.REQUEST, "t" + i, 200.0, 1.0), state);
        }
        float[] v = useCase().build(event(1005.0, ModbusDirection.RESPONSE, "evicted", null, null), state)
            .vector().values();
        assertEquals(0f, v[9], "address_present");
    }
```

Add any imports the file lacks: `EventEnvelope`, `EventId`, `LogType`, `SensorId`, `ModbusEvent`, `ModbusEvent.ModbusDirection`, `java.time.Instant`.

- [ ] **Step 2: Run them to verify they fail**

Run: `./mvnw test -pl modules/application -am -Dtest='ModbusBuildFeaturesUseCaseTest' -Dsurefire.failIfNoSpecifiedTests=false`
Expected: FAIL — `aResponseWithoutAddressTakesItsPendingRequestsAddressAndQuantity` and `aResponsesOwnAddressWins` get `address_present` / `quantity_value` 0; the three "borrows nothing" tests already pass.

- [ ] **Step 3: Add `PendingRequest`**

```java
package io.netsecml.platform.domain.feature;

// One pending request: when it was sent and the address and quantity it
// asked for (either may be null, as Zeek writes them). The address and
// quantity are kept so its response, which icsnpp-modbus v1.0.0 mostly writes
// without them, can take them (spec section 2.1, F2).
public record PendingRequest(double ts, Double address, Double quantity) {
}
```

- [ ] **Step 4: Store `PendingRequest` in `ModbusEntityState`**

In `ModbusEntityState`:

1. Change the field (line 135) to `private LinkedHashMap<String, PendingRequest> pending = new LinkedHashMap<>();` and `reset()`'s `pending = new LinkedHashMap<>();` stays as is.
2. Replace `pendingTs` (lines 298-300) with:

```java
    // The pending request's timestamp for a tid, or null if it is not pending.
    public Double pendingTs(String tid) {
        PendingRequest request = pending.get(tid);
        return request == null ? null : request.ts();
    }

    // The pending request for a tid, or null if it is not pending. A plain
    // get: the map is insertion-ordered, so reading it reorders nothing.
    public PendingRequest pendingRequest(String tid) {
        return pending.get(tid);
    }
```

3. In `advance`, the before-event snapshot (line 377-378) becomes:

```java
        // 0. The before-event snapshot, before anything below changes.
        PendingRequest pendingForTid = pending.get(tid);
        BeforeEvent before = new BeforeEvent(lastTs, prevFunctionCode, lastAddress, lastQuantity,
            pending.size(), pendingForTid == null ? null : pendingForTid.ts());
```

4. The request branch of the pending mutation (`pending.put(tid, ts);`) becomes `pending.put(tid, new PendingRequest(ts, address, quantity));`.
5. `evictOldestIfOverCap(Map<String, Double> pending)` (line 509) becomes generic: `private static <V> void evictOldestIfOverCap(Map<String, V> pending)` with an unchanged body.

- [ ] **Step 5: Add `ModbusEvent.withAddressAndQuantity`**

Inside the `ModbusEvent` record body, after the accessor overrides:

```java
    // This event with the given address and quantity, everything else equal:
    // how ModbusBuildFeaturesUseCase hands a response its request's (F2).
    public ModbusEvent withAddressAndQuantity(Double newAddress, Double newQuantity) {
        return new ModbusEvent(envelope, tsSeconds, direction, sourceIp, destinationIp, functionCode,
            transactionId, unitId, newAddress, newQuantity, matched, requestValues, responseValues);
    }
```

(The canonical constructor copies both arrays, so the new event shares none with this one.)

- [ ] **Step 6: Resolve the effective event in `ModbusBuildFeaturesUseCase.build`**

After Step 2's segment reset (`if (newSegment) { currentState.reset(); }`) and before Step 3's `advance`, insert:

```java
        // Step 2b (spec section 2.1, F2): a response Zeek wrote without an
        // address or quantity takes its pending request's, as upstream's capture
        // adapter did (100% of training rows carry both). After the reset above,
        // so a new segment -- which forgets every pending request -- borrows
        // nothing. Everything below reads the effective event.
        event = withPendingRequestFields(event, currentState);
```

and add the helper to the class:

```java
    // The event with its pending request's address and quantity filled in
    // where a response lacks them; the event itself in every other case.
    static ModbusEvent withPendingRequestFields(ModbusEvent event, ModbusEntityState state) {
        // Only a response lacking a field can borrow.
        if (event.direction() != ModbusEvent.ModbusDirection.RESPONSE
                || (event.address() != null && event.quantity() != null)) {
            return event;
        }
        // Unanswered, evicted by the cap, or forgotten by a new segment: nothing to borrow.
        PendingRequest request = state.pendingRequest(event.transactionId());
        if (request == null) {
            return event;
        }
        // The response's own value wins; the request fills only what is missing.
        Double address = event.address() != null ? event.address() : request.address();
        Double quantity = event.quantity() != null ? event.quantity() : request.quantity();
        return event.withAddressAndQuantity(address, quantity);
    }
```

`event` is the method parameter; if it is declared `final`, introduce `ModbusEvent effective = withPendingRequestFields(event, currentState);` instead and use `effective` in the `advance(...)` and `extract(...)` calls and in the vector's envelope reads below. Import `io.netsecml.platform.domain.feature.PendingRequest`.

- [ ] **Step 7: Run the application tests, including parity**

Run: `./mvnw test -pl modules/application -am -Dtest='ModbusBuildFeaturesUseCaseTest,ModbusEngineParityTest,ModbusFloodTest,ModbusWindowSaturationTest,ModbusFeatureExtractorTest' -Dsurefire.failIfNoSpecifiedTests=false`
Expected: all pass. `ModbusEngineParityTest` must stay green untouched: its responses always carry an address and quantity (`ModbusEventStreams`), so F2 never fires there.

- [ ] **Step 8: Rename the state and pin what the rename means**

In `ModbusFeatureProcessFunction.open()`, replace the name comment and descriptor name:

```java
        // Named "modbus-entity-state-v2": a state name IS checkpoint identity.
        // It was "modbus-entity-state" until 2026-09-26, when F2 (spec section
        // 2.1) changed ModbusEntityState's layout -- pending requests now keep
        // their address and quantity -- and the state is Kryo-serialized, so
        // the old bytes cannot be read as the new layout. The rename makes a
        // restore of an older savepoint start every Modbus key fresh instead of
        // failing; the old state stays unread in the savepoint (it held test
        // data only). Never rename it again without the same deliberate step.
        ValueStateDescriptor<ModbusEntityState> descriptor = new ValueStateDescriptor<>(
            "modbus-entity-state-v2", TypeInformation.of(ModbusEntityState.class));
```

In `ModbusFeatureProcessFunctionTest`, replace `aSavepointWrittenBeforeTheStateHadATtlRestoresIntoIt` and its `PreTtlModbusFeatureProcessFunction` helper's comment (keep the helper; it writes `"modbus-entity-state"`) with:

```java
    // Spec section 2.1: a savepoint holding the pre-2026-09-26 state (named
    // "modbus-entity-state") restores without failing, and the key starts
    // fresh -- the pending request is not read, so its response is unmatched.
    @Test
    void aSavepointOfTheRenamedStateRestoresAndTheKeyStartsFresh() throws Exception {
        OneInputStreamOperatorTestHarness<ModbusEvent, FeatureVector> before =
            new KeyedOneInputStreamOperatorTestHarness<>(
                new KeyedProcessOperator<>(new PreTtlModbusFeatureProcessFunction()),
                new ModbusEntityKeySelector(), TypeInformation.of(ModbusEntityKey.class));
        before.open();
        before.processElement(new StreamRecord<>(request(1000.0, 3, "7")));
        OperatorSubtaskState snapshot = before.snapshot(1L, 1L);
        before.close();

        OneInputStreamOperatorTestHarness<ModbusEvent, FeatureVector> after = harness();
        after.initializeState(snapshot);
        after.open();
        after.processElement(new StreamRecord<>(response(1000.25, 3, "7")));

        float[] responseVector = after.extractOutputValues().get(0).values();
        assertEquals(0f, responseVector[30], "outstanding_requests_before_event: nothing restored");
        assertEquals(1f, responseVector[31], "response_without_request");
        after.close();
    }
```

- [ ] **Step 9: Run the adapter-flink tests**

Run: `./mvnw test -pl modules/adapter-flink -am -Dtest='ModbusFeatureProcessFunctionTest,ModbusEntityStateSerializerTest,ModbusGoldenVectorTest,ModbusParseMapValidateFunctionTest' -Dsurefire.failIfNoSpecifiedTests=false`
Expected: all pass (`ModbusFeatureProcessFunctionTest` 8). If `ModbusEntityStateSerializerTest` fails on `PendingRequest` (Kryo and records), replace the record with a `final class PendingRequest` holding the three fields as `private final` with a private no-argument constructor for Kryo and the same accessors, and rerun.

- [ ] **Step 10: Commit**

```bash
git add modules/domain modules/application modules/adapter-flink
git commit -m "fix(modbus): a response takes its pending request's address and quantity

icsnpp-modbus v1.0.0 leaves address off most responses; upstream's
capture adapter filled it on every training row (spec section 2.1, F2).
Pending requests now keep their address and quantity, so the state is
renamed modbus-entity-state-v2: an older savepoint's Modbus state is
left unread and every Modbus key starts fresh once.

<attribution lines>"
```

---

### Task 3: The frozen preprocessing, in the domain

**Files:**
- Create: `modules/domain/src/main/java/io/netsecml/platform/domain/model/PreprocessingPolicy.java`
- Create: `modules/domain/src/main/java/io/netsecml/platform/domain/model/FeaturePreprocessing.java`
- Create: `modules/domain/src/main/java/io/netsecml/platform/domain/model/ModbusPreprocessing.java`
- Test: `modules/domain/src/test/java/io/netsecml/platform/domain/model/ModbusPreprocessingTest.java`

**Interfaces:**
- Produces: `enum PreprocessingPolicy { PASSTHROUGH_BINARY, PASSTHROUGH_BOUNDED_OR_CONSTANT, GLOBAL_STANDARD, GLOBAL_LOG1P_ONLY, CONDITIONAL_STANDARD, CONDITIONAL_LOG1P_ONLY, CONDITIONAL_LOG1P_THEN_STANDARD }` with `boolean conditional()` and `boolean standardized()`; `record FeaturePreprocessing(String feature, PreprocessingPolicy policy, int maskIndex, double mean, double std)`; `final class ModbusPreprocessing` with `ModbusPreprocessing(List<FeaturePreprocessing>)`, `int width()`, `List<String> featureOrder()`, `float[] apply(float[] raw)`.

- [ ] **Step 1: Write the failing test**

```java
package io.netsecml.platform.domain.model;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

// The seven transform_definitions of modbus_preprocessing_contract_v1, on a
// hand-built contract whose expected outputs are worked out by hand. The real
// contract's numbers are pinned against Python in SequenceDetectorBundleLoaderTest.
class ModbusPreprocessingTest {

    // Feature 0 is the mask every conditional feature below points at.
    private static ModbusPreprocessing contract() {
        return new ModbusPreprocessing(List.of(
            new FeaturePreprocessing("mask", PreprocessingPolicy.PASSTHROUGH_BINARY, -1, Double.NaN, Double.NaN),
            new FeaturePreprocessing("bounded", PreprocessingPolicy.PASSTHROUGH_BOUNDED_OR_CONSTANT, -1, Double.NaN, Double.NaN),
            new FeaturePreprocessing("gstd", PreprocessingPolicy.GLOBAL_STANDARD, -1, 2.0, 4.0),
            new FeaturePreprocessing("glog", PreprocessingPolicy.GLOBAL_LOG1P_ONLY, -1, Double.NaN, Double.NaN),
            new FeaturePreprocessing("cstd", PreprocessingPolicy.CONDITIONAL_STANDARD, 0, 2.0, 4.0),
            new FeaturePreprocessing("clog", PreprocessingPolicy.CONDITIONAL_LOG1P_ONLY, 0, Double.NaN, Double.NaN),
            new FeaturePreprocessing("clogstd", PreprocessingPolicy.CONDITIONAL_LOG1P_THEN_STANDARD, 0, 1.0, 2.0)));
    }

    @Test
    void everyPolicyAppliesItsFormulaWhereTheMaskIsOne() {
        float[] out = contract().apply(new float[]{1f, 7f, 10f, 3f, 6f, 3f, 0f});
        assertEquals(1f, out[0], "passthrough binary");
        assertEquals(7f, out[1], "passthrough bounded");
        assertEquals(2f, out[2], "(10 - 2) / 4");
        assertEquals((float) Math.log1p(3), out[3], "log1p(3)");
        assertEquals(1f, out[4], "(6 - 2) / 4");
        assertEquals((float) Math.log1p(3), out[5], "log1p(3)");
        assertEquals(-0.5f, out[6], "(log1p(0) - 1) / 2");
    }

    // A conditional feature whose mask is 0 is exactly 0.0, whatever its value.
    @Test
    void aConditionalFeatureWhoseMaskIsZeroIsExactlyZero() {
        float[] out = contract().apply(new float[]{0f, 7f, 10f, 3f, 6f, 3f, 5f});
        assertEquals(0f, out[4]);
        assertEquals(0f, out[5]);
        assertEquals(0f, out[6]);
    }

    // The contract never clips; a value outside log1p's domain comes out
    // non-finite, and the caller (the use case) turns that into UNSCORABLE.
    @Test
    void aValueOutsideLog1psDomainComesOutNonFinite() {
        float[] out = contract().apply(new float[]{1f, 0f, 0f, -2f, 0f, 0f, 0f});
        assertTrue(Float.isNaN(out[3]));
    }

    @Test
    void theWidthMustMatch() {
        assertThrows(IllegalArgumentException.class, () -> contract().apply(new float[3]));
    }

    @Test
    void aStandardizedPolicyNeedsAPositiveStd() {
        assertThrows(IllegalArgumentException.class, () -> new ModbusPreprocessing(List.of(
            new FeaturePreprocessing("gstd", PreprocessingPolicy.GLOBAL_STANDARD, -1, 2.0, 0.0))));
    }

    @Test
    void aConditionalPolicyNeedsAMaskInsideTheVector() {
        assertThrows(IllegalArgumentException.class, () -> new ModbusPreprocessing(List.of(
            new FeaturePreprocessing("clog", PreprocessingPolicy.CONDITIONAL_LOG1P_ONLY, 5, Double.NaN, Double.NaN))));
    }

    @Test
    void theFeatureOrderIsTheContractsOrder() {
        assertEquals(List.of("mask", "bounded", "gstd", "glog", "cstd", "clog", "clogstd"), contract().featureOrder());
        assertEquals(7, contract().width());
    }
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `./mvnw test -pl modules/domain -am -Dtest='ModbusPreprocessingTest' -Dsurefire.failIfNoSpecifiedTests=false`
Expected: COMPILATION ERROR — the three types do not exist.

- [ ] **Step 3: Implement the three types**

`PreprocessingPolicy.java`:

```java
package io.netsecml.platform.domain.model;

// modbus_preprocessing_contract_v1's seven transform families, by the names
// the contract itself uses, so a loader maps them with valueOf.
public enum PreprocessingPolicy {
    PASSTHROUGH_BINARY(false, false),
    PASSTHROUGH_BOUNDED_OR_CONSTANT(false, false),
    GLOBAL_STANDARD(false, true),
    GLOBAL_LOG1P_ONLY(false, false),
    CONDITIONAL_STANDARD(true, true),
    CONDITIONAL_LOG1P_ONLY(true, false),
    CONDITIONAL_LOG1P_THEN_STANDARD(true, true);

    private final boolean conditional;
    private final boolean standardized;

    PreprocessingPolicy(boolean conditional, boolean standardized) {
        this.conditional = conditional;
        this.standardized = standardized;
    }

    // Applied only where the feature's mask is 1; exactly 0.0 elsewhere.
    public boolean conditional() {
        return conditional;
    }

    // Uses the fitted mean and std.
    public boolean standardized() {
        return standardized;
    }
}
```

`FeaturePreprocessing.java`:

```java
package io.netsecml.platform.domain.model;

import java.util.Objects;

// One feature's frozen transform: its policy, the index of its mask feature
// (-1 for none), and its TRAIN mean and std (NaN where the policy uses none).
public record FeaturePreprocessing(String feature, PreprocessingPolicy policy, int maskIndex, double mean,
                                   double std) {

    public FeaturePreprocessing {
        // A feature is looked up by name and by policy; neither may be missing.
        Objects.requireNonNull(feature, "feature");
        Objects.requireNonNull(policy, "policy");
        // A standardized policy divides by std, so it must be a positive number.
        if (policy.standardized() && !(Double.isFinite(mean) && Double.isFinite(std) && std > 0)) {
            throw new IllegalArgumentException(feature + ": " + policy + " needs a finite mean and a positive std");
        }
    }
}
```

`ModbusPreprocessing.java`:

```java
package io.netsecml.platform.domain.model;

import java.util.List;

// modbus_preprocessing_contract_v1's 42 -> 42 transform: each feature's
// frozen policy, applied in double precision and carried as float32, the
// model's input type. No clipping, as the contract says; a non-finite result
// is left for the caller to reject.
public final class ModbusPreprocessing {

    private final List<FeaturePreprocessing> features;

    public ModbusPreprocessing(List<FeaturePreprocessing> features) {
        this.features = List.copyOf(features);
        // Every conditional feature's mask must be a feature of this vector.
        for (FeaturePreprocessing f : this.features) {
            if (f.policy().conditional() && (f.maskIndex() < 0 || f.maskIndex() >= this.features.size())) {
                throw new IllegalArgumentException(f.feature() + ": mask index " + f.maskIndex()
                    + " is outside the " + this.features.size() + "-feature vector");
            }
        }
    }

    public int width() {
        return features.size();
    }

    public List<String> featureOrder() {
        return features.stream().map(FeaturePreprocessing::feature).toList();
    }

    // The transformed vector, feature by feature, per the contract's
    // transform_definitions.
    public float[] apply(float[] raw) {
        if (raw.length != features.size()) {
            throw new IllegalArgumentException("expected " + features.size() + " values, got " + raw.length);
        }
        float[] out = new float[raw.length];
        for (int i = 0; i < raw.length; i++) {
            FeaturePreprocessing f = features.get(i);
            double x = raw[i];
            // A conditional feature is exactly 0.0 where its mask is 0.
            if (f.policy().conditional() && raw[f.maskIndex()] != 1f) {
                out[i] = 0f;
                continue;
            }
            double y = switch (f.policy()) {
                case PASSTHROUGH_BINARY, PASSTHROUGH_BOUNDED_OR_CONSTANT -> x;
                case GLOBAL_STANDARD, CONDITIONAL_STANDARD -> (x - f.mean()) / f.std();
                case GLOBAL_LOG1P_ONLY, CONDITIONAL_LOG1P_ONLY -> Math.log1p(x);
                case CONDITIONAL_LOG1P_THEN_STANDARD -> (Math.log1p(x) - f.mean()) / f.std();
            };
            out[i] = (float) y;
        }
        return out;
    }
}
```

- [ ] **Step 4: Run it to verify it passes**

Run: the Step 2 command. Expected: `Tests run: 7, Failures: 0`.

- [ ] **Step 5: Commit**

```bash
git add modules/domain
git commit -m "feat(scoring): the Modbus detector's frozen preprocessing, in the domain

<attribution lines>"
```

---

### Task 4: The scoring value types and the window

**Files:**
- Create: `modules/domain/src/main/java/io/netsecml/platform/domain/model/SequenceDetectorBundle.java`
- Create: `modules/domain/src/main/java/io/netsecml/platform/domain/inference/DetectorScores.java`
- Create: `modules/domain/src/main/java/io/netsecml/platform/domain/inference/DetectorVerdict.java`
- Create: `modules/domain/src/main/java/io/netsecml/platform/domain/inference/DetectorTrigger.java`
- Create: `modules/domain/src/main/java/io/netsecml/platform/domain/inference/ModbusDetectorPrediction.java`
- Create: `modules/domain/src/main/java/io/netsecml/platform/domain/inference/ModbusScoreWindow.java`
- Test: `modules/domain/src/test/java/io/netsecml/platform/domain/inference/ModbusScoreWindowTest.java`, `modules/domain/src/test/java/io/netsecml/platform/domain/inference/ModbusDetectorPredictionTest.java`

**Interfaces:**
- Produces:
  - `record SequenceDetectorBundle(String name, String version, String modelSha, String schemaId, String schemaHash, int sequenceLength, int featureCount, String inputName, String denseOutputName, String temporalOutputName, double denseThreshold, double temporalThreshold, ModbusPreprocessing preprocessing)` with `String bundleId()` (`name + "/" + version`).
  - `record DetectorScores(double dense, double temporal)`.
  - `enum DetectorVerdict { WARMUP, NORMAL, ANOMALY, UNSCORABLE }`.
  - `enum DetectorTrigger { NONE, DENSE, TEMPORAL, BOTH }` with `static DetectorTrigger of(boolean dense, boolean temporal)`.
  - `record ModbusDetectorPrediction(String predictionId, String eventId, Instant eventTime, SensorId sensor, String connectionUid, String clientIp, String serverIp, String unitId, String modelName, String modelVersion, String modelSha, String schemaId, String schemaHash, DetectorVerdict verdict, Float denseScore, Float temporalScore, float denseThreshold, float temporalThreshold, DetectorTrigger trigger, int windowEvents, int qualityFlags, long inferenceMicros, Instant producedAt)`.
  - `final class ModbusScoreWindow` with `static ModbusScoreWindow empty(String bundleId, int capacity, int width)`, `String bundleId()`, `int size()`, `boolean isFull()`, `void reset()`, `void append(float[] row, int qualityFlags)`, `float[][] sequence()` (oldest first, a copy), `int flagsOr()`.

- [ ] **Step 1: Write the failing tests**

`ModbusScoreWindowTest.java`:

```java
package io.netsecml.platform.domain.inference;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

// The per-stream ring of the last `capacity` preprocessed vectors.
class ModbusScoreWindowTest {

    private static float[] row(float v) {
        return new float[]{v, v + 0.5f};
    }

    @Test
    void itFillsThenSlidesKeepingTheNewestOldestFirst() {
        ModbusScoreWindow window = ModbusScoreWindow.empty("m/v1", 3, 2);
        for (int i = 0; i < 5; i++) {
            window.append(row(i), 0);
        }
        assertEquals(3, window.size());
        assertTrue(window.isFull());
        assertArrayEquals(new float[][]{row(2), row(3), row(4)}, window.sequence());
    }

    @Test
    void resetEmptiesIt() {
        ModbusScoreWindow window = ModbusScoreWindow.empty("m/v1", 3, 2);
        window.append(row(1), 8);
        window.reset();
        assertEquals(0, window.size());
        assertFalse(window.isFull());
        assertEquals(0, window.flagsOr());
    }

    // qualityFlags OR over the rows still in the window: a flag leaves with its row.
    @Test
    void theFlagsAreTheOrOverTheRowsStillInTheWindow() {
        ModbusScoreWindow window = ModbusScoreWindow.empty("m/v1", 2, 2);
        window.append(row(1), 8);
        window.append(row(2), 4);
        assertEquals(12, window.flagsOr());
        window.append(row(3), 0);
        assertEquals(4, window.flagsOr());
    }

    // sequence() hands out a copy the caller cannot use to change the window.
    @Test
    void theSequenceIsACopy() {
        ModbusScoreWindow window = ModbusScoreWindow.empty("m/v1", 2, 2);
        window.append(row(1), 0);
        window.sequence()[0][0] = 99f;
        assertEquals(1f, window.sequence()[0][0]);
    }

    @Test
    void aRowOfTheWrongWidthIsRejected() {
        ModbusScoreWindow window = ModbusScoreWindow.empty("m/v1", 2, 2);
        assertThrows(IllegalArgumentException.class, () -> window.append(new float[3], 0));
    }
}
```

`ModbusDetectorPredictionTest.java`:

```java
package io.netsecml.platform.domain.inference;

import io.netsecml.platform.domain.event.SensorId;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

// Scores exist exactly when the detector ran: NORMAL and ANOMALY carry both,
// WARMUP and UNSCORABLE carry neither.
class ModbusDetectorPredictionTest {

    private static ModbusDetectorPrediction prediction(DetectorVerdict verdict, Float dense, Float temporal) {
        return new ModbusDetectorPrediction("a".repeat(64), "s:u:1:REQUEST:1", Instant.EPOCH,
            new SensorId("s"), "u", "10.0.0.5", "10.0.0.9", "1", "modbus-stage1-detector", "v1",
            "b".repeat(64), "modbus-feature-v1", "c".repeat(64), verdict, dense, temporal,
            0.25f, 0.41f, DetectorTrigger.NONE, 20, 0, 0L, Instant.EPOCH);
    }

    @Test
    void scoredVerdictsCarryBothScores() {
        assertDoesNotThrow(() -> prediction(DetectorVerdict.NORMAL, 0.1f, 0.2f));
        assertThrows(IllegalArgumentException.class, () -> prediction(DetectorVerdict.ANOMALY, null, 0.2f));
    }

    @Test
    void unscoredVerdictsCarryNoScores() {
        assertDoesNotThrow(() -> prediction(DetectorVerdict.WARMUP, null, null));
        assertThrows(IllegalArgumentException.class, () -> prediction(DetectorVerdict.UNSCORABLE, 0.1f, null));
    }

    @Test
    void theTriggerFollowsTheTwoHeads() {
        assertEquals(DetectorTrigger.NONE, DetectorTrigger.of(false, false));
        assertEquals(DetectorTrigger.DENSE, DetectorTrigger.of(true, false));
        assertEquals(DetectorTrigger.TEMPORAL, DetectorTrigger.of(false, true));
        assertEquals(DetectorTrigger.BOTH, DetectorTrigger.of(true, true));
    }
}
```

- [ ] **Step 2: Run them to verify they fail**

Run: `./mvnw test -pl modules/domain -am -Dtest='ModbusScoreWindowTest,ModbusDetectorPredictionTest' -Dsurefire.failIfNoSpecifiedTests=false`
Expected: COMPILATION ERROR — the types do not exist.

- [ ] **Step 3: Implement the types**

`SequenceDetectorBundle.java`:

```java
package io.netsecml.platform.domain.model;

import java.util.Objects;

// A loaded, verified sequence detector: its identity, the feature schema it
// was trained on, its graph's input and output names and shapes, its two
// frozen thresholds, and its frozen preprocessing. Built only by the bundle
// loader, after every SHA-256 and the feature order have been checked.
public record SequenceDetectorBundle(String name, String version, String modelSha, String schemaId,
                                     String schemaHash, int sequenceLength, int featureCount, String inputName,
                                     String denseOutputName, String temporalOutputName, double denseThreshold,
                                     double temporalThreshold, ModbusPreprocessing preprocessing) {

    public SequenceDetectorBundle {
        // Identity and graph names are looked up by string; none may be missing.
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(version, "version");
        Objects.requireNonNull(modelSha, "modelSha");
        Objects.requireNonNull(schemaId, "schemaId");
        Objects.requireNonNull(schemaHash, "schemaHash");
        Objects.requireNonNull(inputName, "inputName");
        Objects.requireNonNull(denseOutputName, "denseOutputName");
        Objects.requireNonNull(temporalOutputName, "temporalOutputName");
        Objects.requireNonNull(preprocessing, "preprocessing");
        // The temporal head predicts event L from events 1..L-1, so L >= 2.
        if (sequenceLength < 2) {
            throw new IllegalArgumentException("sequenceLength must be at least 2, was " + sequenceLength);
        }
        // The preprocessing transforms exactly the vector the graph reads.
        if (featureCount != preprocessing.width()) {
            throw new IllegalArgumentException("featureCount " + featureCount + " but preprocessing covers "
                + preprocessing.width());
        }
        // A threshold compares a mean absolute error, so it is a positive number.
        if (!(denseThreshold > 0 && Double.isFinite(denseThreshold)
                && temporalThreshold > 0 && Double.isFinite(temporalThreshold))) {
            throw new IllegalArgumentException("thresholds must be positive and finite");
        }
    }

    // How a window records which bundle filled it: name/version.
    public String bundleId() {
        return name + "/" + version;
    }
}
```

`DetectorScores.java`:

```java
package io.netsecml.platform.domain.inference;

// The detector's two scores for one window: the dense head's reconstruction
// MAE and the temporal head's endpoint MAE.
public record DetectorScores(double dense, double temporal) {
}
```

`DetectorVerdict.java`:

```java
package io.netsecml.platform.domain.inference;

// One Modbus event's outcome. WARMUP: fewer than a full window in its stream's
// segment. UNSCORABLE: its preprocessed vector was not finite. NORMAL and
// ANOMALY: the detector ran.
public enum DetectorVerdict {
    WARMUP, NORMAL, ANOMALY, UNSCORABLE;

    // The detector ran, so both scores exist.
    public boolean scored() {
        return this == NORMAL || this == ANOMALY;
    }
}
```

`DetectorTrigger.java`:

```java
package io.netsecml.platform.domain.inference;

// Which head exceeded its threshold.
public enum DetectorTrigger {
    NONE, DENSE, TEMPORAL, BOTH;

    public static DetectorTrigger of(boolean dense, boolean temporal) {
        if (dense && temporal) {
            return BOTH;
        }
        if (dense) {
            return DENSE;
        }
        return temporal ? TEMPORAL : NONE;
    }
}
```

`ModbusDetectorPrediction.java`:

```java
package io.netsecml.platform.domain.inference;

import io.netsecml.platform.domain.event.SensorId;

import java.time.Instant;
import java.util.Objects;

// One Modbus event's prediction (contracts/stream/modbus-detector-prediction-v1.json):
// which device, which model, the verdict and, when the detector ran, both scores.
public record ModbusDetectorPrediction(String predictionId, String eventId, Instant eventTime, SensorId sensor,
                                       String connectionUid, String clientIp, String serverIp, String unitId,
                                       String modelName, String modelVersion, String modelSha, String schemaId,
                                       String schemaHash, DetectorVerdict verdict, Float denseScore,
                                       Float temporalScore, float denseThreshold, float temporalThreshold,
                                       DetectorTrigger trigger, int windowEvents, int qualityFlags,
                                       long inferenceMicros, Instant producedAt) {

    public ModbusDetectorPrediction {
        // The join keys and the model identity are required on every prediction.
        Objects.requireNonNull(predictionId, "predictionId");
        Objects.requireNonNull(eventId, "eventId");
        Objects.requireNonNull(eventTime, "eventTime");
        Objects.requireNonNull(sensor, "sensor");
        Objects.requireNonNull(verdict, "verdict");
        Objects.requireNonNull(trigger, "trigger");
        Objects.requireNonNull(producedAt, "producedAt");
        // Scores exist exactly when the detector ran.
        boolean hasScores = denseScore != null && temporalScore != null;
        boolean hasNoScores = denseScore == null && temporalScore == null;
        if (verdict.scored() ? !hasScores : !hasNoScores) {
            throw new IllegalArgumentException(verdict + " must " + (verdict.scored() ? "" : "not ")
                + "carry both scores");
        }
    }
}
```

`ModbusScoreWindow.java`:

```java
package io.netsecml.platform.domain.inference;

import java.util.Arrays;

// One stream's last `capacity` preprocessed vectors, a ring, plus each row's
// quality flags and the bundle that filled it. Mutable and updated in place,
// like ModbusEntityState: keyed Flink state, read through value() on every
// call, never cached.
public final class ModbusScoreWindow {

    private final String bundleId;
    private final int capacity;
    private final int width;
    private final float[][] rows;
    private final int[] flags;
    // Index of the oldest row, and how many rows are held.
    private int start;
    private int size;

    private ModbusScoreWindow(String bundleId, int capacity, int width) {
        this.bundleId = bundleId;
        this.capacity = capacity;
        this.width = width;
        this.rows = new float[capacity][width];
        this.flags = new int[capacity];
    }

    public static ModbusScoreWindow empty(String bundleId, int capacity, int width) {
        if (bundleId == null || capacity < 1 || width < 1) {
            throw new IllegalArgumentException("a window needs a bundle id, a capacity and a width");
        }
        return new ModbusScoreWindow(bundleId, capacity, width);
    }

    public String bundleId() {
        return bundleId;
    }

    public int size() {
        return size;
    }

    public boolean isFull() {
        return size == capacity;
    }

    // A segment start: the next sequence begins at the next row.
    public void reset() {
        start = 0;
        size = 0;
        Arrays.fill(flags, 0);
    }

    // Appends a row; once full, the oldest row leaves.
    public void append(float[] row, int qualityFlags) {
        if (row.length != width) {
            throw new IllegalArgumentException("expected a row of " + width + " values, got " + row.length);
        }
        int slot;
        if (size < capacity) {
            slot = (start + size) % capacity;
            size++;
        } else {
            slot = start;
            start = (start + 1) % capacity;
        }
        System.arraycopy(row, 0, rows[slot], 0, width);
        flags[slot] = qualityFlags;
    }

    // The held rows, oldest first, as a fresh array.
    public float[][] sequence() {
        float[][] out = new float[size][];
        for (int i = 0; i < size; i++) {
            out[i] = Arrays.copyOf(rows[(start + i) % capacity], width);
        }
        return out;
    }

    // The OR of the held rows' quality flags.
    public int flagsOr() {
        int or = 0;
        for (int i = 0; i < size; i++) {
            or |= flags[(start + i) % capacity];
        }
        return or;
    }
}
```

- [ ] **Step 4: Run them to verify they pass**

Run: the Step 2 command. Expected: `ModbusScoreWindowTest` 5 and `ModbusDetectorPredictionTest` 3, all passing.

- [ ] **Step 5: Commit**

```bash
git add modules/domain
git commit -m "feat(scoring): prediction, verdict, window and bundle value types

<attribution lines>"
```

---

### Task 5: The scoring port and `ScoreModbusSequenceUseCase`

**Files:**
- Create: `modules/ports/src/main/java/io/netsecml/platform/port/out/SequenceScorer.java`
- Create: `modules/ports/src/main/java/io/netsecml/platform/port/out/SequenceScorerFactory.java`
- Create: `modules/application/src/main/java/io/netsecml/platform/application/usecase/ModbusScoringResult.java`
- Create: `modules/application/src/main/java/io/netsecml/platform/application/usecase/ScoreModbusSequenceUseCase.java`
- Test: `modules/application/src/test/java/io/netsecml/platform/application/usecase/ScoreModbusSequenceUseCaseTest.java`

**Interfaces:**
- Consumes: Task 3's `ModbusPreprocessing`; Task 4's types; `Prediction.deriveId(String eventId, String modelName, String modelVersion)`; `ModbusFeatureSchemaV1.SCHEMA`.
- Produces: `interface SequenceScorer extends AutoCloseable { SequenceDetectorBundle bundle(); DetectorScores score(float[][] sequence); void close(); }`; `interface SequenceScorerFactory extends Serializable { SequenceScorer create(); }`; `record ModbusScoringResult(ModbusDetectorPrediction prediction, ModbusScoreWindow window)`; `ScoreModbusSequenceUseCase(SequenceScorer scorer, Clock clock)` with `ModbusScoringResult score(ModbusEntityKey key, FeatureVector vector, ModbusScoreWindow window)` (`window` may be null).

- [ ] **Step 1: Write the ports (interfaces only; they carry no logic to test)**

`SequenceScorer.java`:

```java
package io.netsecml.platform.port.out;

import io.netsecml.platform.domain.inference.DetectorScores;
import io.netsecml.platform.domain.model.SequenceDetectorBundle;

// Scores one window of preprocessed vectors against a loaded sequence
// detector. AutoCloseable because the real one holds an ONNX Runtime session;
// close() declares no checked exception, so an implementation rethrows its
// own close failure unchecked rather than dropping it.
public interface SequenceScorer extends AutoCloseable {

    // The bundle this scorer was built from: preprocessing, thresholds, identity.
    SequenceDetectorBundle bundle();

    // sequence is bundle().sequenceLength() rows of bundle().featureCount()
    // preprocessed values, oldest first.
    DetectorScores score(float[][] sequence);

    @Override
    void close();
}
```

`SequenceScorerFactory.java`:

```java
package io.netsecml.platform.port.out;

import java.io.Serializable;

// Creates one SequenceScorer per Flink subtask, inside that subtask's open():
// an ONNX Runtime session is not shared across subtasks. Serializable so the
// factory (a bundle path) travels with the operator to every TaskManager.
public interface SequenceScorerFactory extends Serializable {
    SequenceScorer create();
}
```

- [ ] **Step 2: Write the failing use-case test**

```java
package io.netsecml.platform.application.usecase;

import io.netsecml.platform.domain.event.LogType;
import io.netsecml.platform.domain.event.SensorId;
import io.netsecml.platform.domain.feature.FeatureVector;
import io.netsecml.platform.domain.feature.ModbusEntityKey;
import io.netsecml.platform.domain.feature.ModbusFeatureSchemaV1;
import io.netsecml.platform.domain.inference.DetectorScores;
import io.netsecml.platform.domain.inference.DetectorTrigger;
import io.netsecml.platform.domain.inference.DetectorVerdict;
import io.netsecml.platform.domain.inference.ModbusDetectorPrediction;
import io.netsecml.platform.domain.inference.ModbusScoreWindow;
import io.netsecml.platform.domain.inference.Prediction;
import io.netsecml.platform.domain.model.FeaturePreprocessing;
import io.netsecml.platform.domain.model.ModbusPreprocessing;
import io.netsecml.platform.domain.model.PreprocessingPolicy;
import io.netsecml.platform.domain.model.SequenceDetectorBundle;
import io.netsecml.platform.port.out.SequenceScorer;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

// The window rules, the decision rule and the prediction's fields, with a stub
// scorer: no ONNX Runtime here. The preprocessing is 42 passthroughs except
// feature 35 (event_rate_1s), GLOBAL_LOG1P_ONLY, so a negative value there
// yields NaN on demand.
class ScoreModbusSequenceUseCaseTest {

    private static final int PREV_EVENT_AVAILABLE = 23;
    private static final ModbusEntityKey KEY = new ModbusEntityKey(new SensorId("s"), "10.0.0.5", "10.0.0.9", "1");

    // A stub that records every sequence it was given and returns fixed scores.
    private static final class StubScorer implements SequenceScorer {
        private final SequenceDetectorBundle bundle;
        final List<float[][]> seen = new ArrayList<>();
        DetectorScores next = new DetectorScores(0.1, 0.1);

        StubScorer(String version) {
            this.bundle = bundle(version);
        }

        @Override
        public SequenceDetectorBundle bundle() {
            return bundle;
        }

        @Override
        public DetectorScores score(float[][] sequence) {
            seen.add(sequence);
            return next;
        }

        @Override
        public void close() {
        }
    }

    private static SequenceDetectorBundle bundle(String version) {
        List<FeaturePreprocessing> features = IntStream.range(0, 42)
            .mapToObj(i -> new FeaturePreprocessing("f" + i,
                i == 35 ? PreprocessingPolicy.GLOBAL_LOG1P_ONLY : PreprocessingPolicy.PASSTHROUGH_BINARY,
                -1, Double.NaN, Double.NaN))
            .toList();
        return new SequenceDetectorBundle("modbus-stage1-detector", version, "b".repeat(64),
            ModbusFeatureSchemaV1.SCHEMA.id(), ModbusFeatureSchemaV1.CONTENT_HASH, 20, 42, "sequence_20x42",
            "modbus_dense_autoencoder", "modbus_causal_next_event_predictor", 0.5, 0.5,
            new ModbusPreprocessing(features));
    }

    // Event i of a stream: feature 0 carries i (so sequences are recognisable),
    // prev_event_available is 1 except where a segment starts.
    private static FeatureVector vector(int i, boolean segmentStart, int flags) {
        float[] values = new float[42];
        values[0] = i;
        values[PREV_EVENT_AVAILABLE] = segmentStart ? 0f : 1f;
        return new FeatureVector("s:u:" + i + ":REQUEST:" + i, Instant.ofEpochSecond(1000 + i), new SensorId("s"),
            LogType.MODBUS, "u", ModbusFeatureSchemaV1.SCHEMA.id(), ModbusFeatureSchemaV1.CONTENT_HASH, values,
            flags, Instant.ofEpochSecond(1000 + i));
    }

    private final Clock clock = Clock.fixed(Instant.parse("2026-09-26T12:00:00Z"), ZoneOffset.UTC);

    // Runs events 0..n-1 of one segment through the use case.
    private static List<ModbusDetectorPrediction> run(ScoreModbusSequenceUseCase useCase, int n) {
        List<ModbusDetectorPrediction> out = new ArrayList<>();
        ModbusScoreWindow window = null;
        for (int i = 0; i < n; i++) {
            ModbusScoringResult result = useCase.score(KEY, vector(i, i == 0, 0), window);
            window = result.window();
            out.add(result.prediction());
        }
        return out;
    }

    @Test
    void theFirstNineteenEventsWarmUpAndTheTwentiethIsScored() {
        StubScorer scorer = new StubScorer("v1");
        List<ModbusDetectorPrediction> out = run(new ScoreModbusSequenceUseCase(scorer, clock), 20);
        for (int i = 0; i < 19; i++) {
            assertEquals(DetectorVerdict.WARMUP, out.get(i).verdict());
            assertEquals(i + 1, out.get(i).windowEvents());
            assertNull(out.get(i).denseScore());
            assertEquals(0L, out.get(i).inferenceMicros());
        }
        assertEquals(DetectorVerdict.NORMAL, out.get(19).verdict());
        assertEquals(20, out.get(19).windowEvents());
        assertEquals(1, scorer.seen.size(), "the detector runs only on a full window");
    }

    // The detector sees the last 20 preprocessed rows, oldest first.
    @Test
    void theDetectorSeesTheLastTwentyRowsOldestFirst() {
        StubScorer scorer = new StubScorer("v1");
        run(new ScoreModbusSequenceUseCase(scorer, clock), 25);
        float[][] last = scorer.seen.get(scorer.seen.size() - 1);
        assertEquals(20, last.length);
        assertEquals(5f, last[0][0]);
        assertEquals(24f, last[19][0]);
    }

    // Strictly greater: a score equal to its threshold is not an anomaly; the
    // trigger names the head that exceeded its threshold.
    @Test
    void theThresholdsAreStrictAndTheTriggerNamesTheHead() {
        StubScorer scorer = new StubScorer("v1");
        ScoreModbusSequenceUseCase useCase = new ScoreModbusSequenceUseCase(scorer, clock);
        ModbusScoreWindow window = null;
        for (int i = 0; i < 19; i++) {
            window = useCase.score(KEY, vector(i, i == 0, 0), window).window();
        }
        double[][] cases = {{0.5, 0.5}, {0.6, 0.1}, {0.1, 0.6}, {0.6, 0.6}};
        DetectorTrigger[] triggers = {DetectorTrigger.NONE, DetectorTrigger.DENSE, DetectorTrigger.TEMPORAL,
            DetectorTrigger.BOTH};
        for (int c = 0; c < cases.length; c++) {
            scorer.next = new DetectorScores(cases[c][0], cases[c][1]);
            ModbusScoringResult r = useCase.score(KEY, vector(19 + c, false, 0), window);
            window = r.window();
            assertEquals(triggers[c], r.prediction().trigger());
            assertEquals(c == 0 ? DetectorVerdict.NORMAL : DetectorVerdict.ANOMALY, r.prediction().verdict());
            assertEquals((float) cases[c][0], r.prediction().denseScore());
        }
    }

    // A segment start empties the window: the stream warms up again.
    @Test
    void aSegmentStartReWarms() {
        StubScorer scorer = new StubScorer("v1");
        ScoreModbusSequenceUseCase useCase = new ScoreModbusSequenceUseCase(scorer, clock);
        ModbusScoreWindow window = null;
        for (int i = 0; i < 20; i++) {
            window = useCase.score(KEY, vector(i, i == 0, 0), window).window();
        }
        ModbusScoringResult restart = useCase.score(KEY, vector(20, true, 0), window);
        assertEquals(DetectorVerdict.WARMUP, restart.prediction().verdict());
        assertEquals(1, restart.prediction().windowEvents());
    }

    // A non-finite preprocessed vector is UNSCORABLE and never enters the window.
    @Test
    void aNonFiniteVectorIsUnscorableAndLeavesTheWindowAsItWas() {
        StubScorer scorer = new StubScorer("v1");
        ScoreModbusSequenceUseCase useCase = new ScoreModbusSequenceUseCase(scorer, clock);
        ModbusScoreWindow window = useCase.score(KEY, vector(0, true, 0), null).window();
        FeatureVector bad = vector(1, false, 0);
        float[] values = bad.values();
        values[35] = -2f;
        FeatureVector poisoned = new FeatureVector(bad.eventId(), bad.eventTime(), bad.sensor(), bad.logType(),
            bad.connectionUid(), bad.schemaId(), bad.schemaHash(), values, 0, bad.producedAt());
        ModbusScoringResult r = useCase.score(KEY, poisoned, window);
        assertEquals(DetectorVerdict.UNSCORABLE, r.prediction().verdict());
        assertEquals(1, r.prediction().windowEvents(), "the window still holds only event 0");
        assertNull(r.prediction().denseScore());
    }

    // Review Focus 4: a window filled under another bundle is never scored.
    @Test
    void aWindowFromAnotherBundleIsEmptiedFirst() {
        ModbusScoreWindow fromV1 = null;
        ScoreModbusSequenceUseCase v1 = new ScoreModbusSequenceUseCase(new StubScorer("v1"), clock);
        for (int i = 0; i < 19; i++) {
            fromV1 = v1.score(KEY, vector(i, i == 0, 0), fromV1).window();
        }
        StubScorer v2Scorer = new StubScorer("v2");
        ModbusScoringResult r = new ScoreModbusSequenceUseCase(v2Scorer, clock).score(KEY, vector(19, false, 0), fromV1);
        assertEquals(DetectorVerdict.WARMUP, r.prediction().verdict());
        assertEquals(1, r.prediction().windowEvents());
        assertEquals("modbus-stage1-detector/v2", r.window().bundleId());
        assertTrue(v2Scorer.seen.isEmpty());
    }

    // qualityFlags is the OR of this vector's flags and the window's.
    @Test
    void theFlagsAreOredOverTheWindow() {
        ScoreModbusSequenceUseCase useCase = new ScoreModbusSequenceUseCase(new StubScorer("v1"), clock);
        ModbusScoreWindow window = useCase.score(KEY, vector(0, true, 8), null).window();
        ModbusScoringResult r = useCase.score(KEY, vector(1, false, 4), window);
        assertEquals(12, r.prediction().qualityFlags());
    }

    // The prediction names the device, the model and the event it came from.
    @Test
    void thePredictionCarriesTheKeyTheModelAndADeterministicId() {
        ModbusDetectorPrediction p = new ScoreModbusSequenceUseCase(new StubScorer("v1"), clock)
            .score(KEY, vector(0, true, 0), null).prediction();
        assertEquals("10.0.0.5", p.clientIp());
        assertEquals("10.0.0.9", p.serverIp());
        assertEquals("1", p.unitId());
        assertEquals("modbus-stage1-detector", p.modelName());
        assertEquals(Prediction.deriveId("s:u:0:REQUEST:0", "modbus-stage1-detector", "v1"), p.predictionId());
        assertEquals(0.5f, p.denseThreshold());
        assertEquals(Instant.parse("2026-09-26T12:00:00Z"), p.producedAt());
    }
}
```

- [ ] **Step 3: Run it to verify it fails**

Run: `./mvnw test -pl modules/application -am -Dtest='ScoreModbusSequenceUseCaseTest' -Dsurefire.failIfNoSpecifiedTests=false`
Expected: COMPILATION ERROR — `ScoreModbusSequenceUseCase` and `ModbusScoringResult` do not exist.

- [ ] **Step 4: Implement `ModbusScoringResult` and the use case**

`ModbusScoringResult.java`:

```java
package io.netsecml.platform.application.usecase;

import io.netsecml.platform.domain.inference.ModbusDetectorPrediction;
import io.netsecml.platform.domain.inference.ModbusScoreWindow;

// One event's prediction and the stream's window after it.
public record ModbusScoringResult(ModbusDetectorPrediction prediction, ModbusScoreWindow window) {
}
```

`ScoreModbusSequenceUseCase.java`:

```java
package io.netsecml.platform.application.usecase;

import io.netsecml.platform.domain.feature.FeatureDefinition;
import io.netsecml.platform.domain.feature.FeatureVector;
import io.netsecml.platform.domain.feature.ModbusEntityKey;
import io.netsecml.platform.domain.feature.ModbusFeatureSchemaV1;
import io.netsecml.platform.domain.inference.DetectorScores;
import io.netsecml.platform.domain.inference.DetectorTrigger;
import io.netsecml.platform.domain.inference.DetectorVerdict;
import io.netsecml.platform.domain.inference.ModbusDetectorPrediction;
import io.netsecml.platform.domain.inference.ModbusScoreWindow;
import io.netsecml.platform.domain.inference.Prediction;
import io.netsecml.platform.domain.model.SequenceDetectorBundle;
import io.netsecml.platform.port.out.SequenceScorer;

import java.time.Clock;

// Scores one Modbus feature vector (spec sections 4, 5 and 8): keeps its
// stream's window of preprocessed vectors, runs the detector once the window
// is full, and turns the two scores into a verdict. The window is updated in
// place and handed back, like ModbusEntityState.
public final class ScoreModbusSequenceUseCase {

    private final SequenceScorer scorer;
    private final SequenceDetectorBundle bundle;
    private final Clock clock;
    // prev_event_available is 0 exactly on a segment's first event (spec section 5).
    private final int prevEventAvailable;

    public ScoreModbusSequenceUseCase(SequenceScorer scorer, Clock clock) {
        this.scorer = scorer;
        this.bundle = scorer.bundle();
        this.clock = clock;
        // The detector must be the one trained on the vectors this job builds.
        if (!ModbusFeatureSchemaV1.SCHEMA.id().equals(bundle.schemaId())
                || bundle.featureCount() != ModbusFeatureSchemaV1.SCHEMA.featureCount()) {
            throw new IllegalStateException("bundle " + bundle.bundleId() + " was trained on " + bundle.schemaId()
                + ", not " + ModbusFeatureSchemaV1.SCHEMA.id());
        }
        this.prevEventAvailable = ModbusFeatureSchemaV1.SCHEMA.definitions().stream()
            .filter(d -> d.name().equals("prev_event_available")).mapToInt(FeatureDefinition::index)
            .findFirst().orElseThrow();
    }

    public ModbusScoringResult score(ModbusEntityKey key, FeatureVector vector, ModbusScoreWindow window) {
        // A window filled under another bundle, or none yet: start empty (spec S5).
        if (window == null || !window.bundleId().equals(bundle.bundleId())) {
            window = ModbusScoreWindow.empty(bundle.bundleId(), bundle.sequenceLength(), bundle.featureCount());
        }
        float[] raw = vector.values();
        // A segment start: no sequence crosses it.
        if (raw[prevEventAvailable] == 0f) {
            window.reset();
        }
        float[] preprocessed = bundle.preprocessing().apply(raw);
        // Non-finite input is never scored and never enters the window (spec S7).
        if (!allFinite(preprocessed)) {
            return new ModbusScoringResult(prediction(key, vector, DetectorVerdict.UNSCORABLE, null,
                DetectorTrigger.NONE, window.size(), vector.qualityFlags() | window.flagsOr(), 0L), window);
        }
        window.append(preprocessed, vector.qualityFlags());
        // Fewer than a full window: WARMUP.
        if (!window.isFull()) {
            return new ModbusScoringResult(prediction(key, vector, DetectorVerdict.WARMUP, null,
                DetectorTrigger.NONE, window.size(), window.flagsOr(), 0L), window);
        }
        // A full window: run the detector and apply the frozen decision rule.
        long started = System.nanoTime();
        DetectorScores scores = scorer.score(window.sequence());
        long micros = (System.nanoTime() - started) / 1_000;
        boolean dense = scores.dense() > bundle.denseThreshold();
        boolean temporal = scores.temporal() > bundle.temporalThreshold();
        DetectorVerdict verdict = dense || temporal ? DetectorVerdict.ANOMALY : DetectorVerdict.NORMAL;
        return new ModbusScoringResult(prediction(key, vector, verdict, scores, DetectorTrigger.of(dense, temporal),
            window.size(), window.flagsOr(), micros), window);
    }

    private ModbusDetectorPrediction prediction(ModbusEntityKey key, FeatureVector vector, DetectorVerdict verdict,
                                                DetectorScores scores, DetectorTrigger trigger, int windowEvents,
                                                int flags, long micros) {
        return new ModbusDetectorPrediction(
            Prediction.deriveId(vector.eventId(), bundle.name(), bundle.version()),
            vector.eventId(), vector.eventTime(), vector.sensor(), vector.connectionUid(),
            key.clientIp(), key.serverIp(), key.unitId(),
            bundle.name(), bundle.version(), bundle.modelSha(), bundle.schemaId(), bundle.schemaHash(),
            verdict,
            scores == null ? null : (float) scores.dense(),
            scores == null ? null : (float) scores.temporal(),
            (float) bundle.denseThreshold(), (float) bundle.temporalThreshold(),
            trigger, windowEvents, flags, micros, clock.instant());
    }

    private static boolean allFinite(float[] values) {
        for (float v : values) {
            if (!Float.isFinite(v)) {
                return false;
            }
        }
        return true;
    }
}
```

- [ ] **Step 5: Run it to verify it passes**

Run: the Step 3 command. Expected: `Tests run: 8, Failures: 0`.

- [ ] **Step 6: Commit**

```bash
git add modules/ports modules/application
git commit -m "feat(scoring): SequenceScorer port and ScoreModbusSequenceUseCase

<attribution lines>"
```

---

### Task 6: The bundle contract, the packaging script and the fixture bundle

**Files:**
- Create: `contracts/model/sequence-detector-bundle-v1.json`
- Create: `deploy/models/package-modbus-detector.sh`
- Create: `deploy/tests/test_models.sh`
- Modify: `deploy/tests/run-all.sh` (run `test_models.sh`; shellcheck `models/*.sh`)
- Create (generated, committed): `tests/fixtures/models/modbus-stage1-detector/v1/{model.onnx,preprocessing.json,thresholds.json,bundle.json}`

**Interfaces:**
- Produces: `bundle.json` with exactly the fields `name, version, schemaId, sequenceLength, featureCount, inputName, denseOutputName, temporalOutputName, modelSha, preprocessingSha, thresholdsSha`; the script `deploy/models/package-modbus-detector.sh <delivery-dir> [out-root]` writing `<out-root>/modbus-stage1-detector/v1/`.

- [ ] **Step 1: Write the contract file**

`contracts/model/sequence-detector-bundle-v1.json`:

```json
{
  "id": "sequence-detector-bundle-v1",
  "semanticVersion": "1.0.0",
  "encoding": "application/json",
  "fields": [
    {"name": "name", "type": "string", "required": true, "description": "Model family, e.g. modbus-stage1-detector; with version, the bundle's identity in every prediction"},
    {"name": "version", "type": "string", "required": true, "description": "Version within the family, e.g. v1"},
    {"name": "schemaId", "type": "string", "required": true, "description": "Feature schema the detector was trained on, e.g. modbus-feature-v1; must resolve in FeatureSchemaRegistry, whose feature order must equal preprocessing.json's feature_order"},
    {"name": "sequenceLength", "type": "integer", "required": true, "description": "Events per window, e.g. 20"},
    {"name": "featureCount", "type": "integer", "required": true, "description": "Values per event, e.g. 42"},
    {"name": "inputName", "type": "string", "required": true, "description": "The ONNX graph's input, shape [?, sequenceLength, featureCount]"},
    {"name": "denseOutputName", "type": "string", "required": true, "description": "The reconstruction output, shape [?, sequenceLength, featureCount]"},
    {"name": "temporalOutputName", "type": "string", "required": true, "description": "The next-event output, shape [?, sequenceLength - 1, featureCount]"},
    {"name": "modelSha", "type": "string", "required": true, "description": "SHA-256 of model.onnx, 64 lowercase hex"},
    {"name": "preprocessingSha", "type": "string", "required": true, "description": "SHA-256 of preprocessing.json, the delivered PREPROCESSING_CONTRACT_V1.json byte for byte"},
    {"name": "thresholdsSha", "type": "string", "required": true, "description": "SHA-256 of thresholds.json, the delivered THRESHOLD_CONTRACT_V1.json byte for byte"}
  ]
}
```

- [ ] **Step 2: Write the failing deploy test**

`deploy/tests/test_models.sh`:

```bash
#!/usr/bin/env bash
# Pins deploy/models/package-modbus-detector.sh on a small fake delivery:
# the bundle's layout and bundle.json, the check of the ONNX file against the
# delivery's own manifest, and that bundles are never overwritten.
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
. "$HERE/lib.sh"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
script="$HERE/../models/package-modbus-detector.sh"

# A fake delivery in the real delivery's layout.
fake_delivery() {
  local d="$1"
  mkdir -p "$d/models" "$d/contracts/modbus_preprocessing_contract_v1" \
    "$d/contracts/modbus_dual_head_temporal_dense_detector_v1"
  printf 'not really onnx' > "$d/models/dual_head_model_fp32.onnx"
  printf '{"feature_order":["a"]}' > "$d/contracts/modbus_preprocessing_contract_v1/PREPROCESSING_CONTRACT_V1.json"
  printf '{"dense":{"threshold":0.25}}' > "$d/contracts/modbus_dual_head_temporal_dense_detector_v1/THRESHOLD_CONTRACT_V1.json"
  jq -n --arg sha "$(sha256sum "$d/models/dual_head_model_fp32.onnx" | cut -c1-64)" \
    '{onnx_sha256: $sha, parity: {onnx_input_names: ["sequence_20x42"],
      onnx_output_names: ["modbus_dense_autoencoder", "modbus_causal_next_event_predictor"]}}' \
    > "$d/models/dual_head_model_fp32.onnx.manifest.json"
}

fake_delivery "$tmp/delivery"
out="$(bash "$script" "$tmp/delivery" "$tmp/out" 2>&1)"; status=$?
b="$tmp/out/modbus-stage1-detector/v1"
assert_eq 0 "$status" "packaging a sound delivery succeeds"
assert_eq "bundle.json model.onnx preprocessing.json thresholds.json" "$(cd "$b" && printf '%s\n' * | sort | tr '\n' ' ' | sed 's/ $//')" "the bundle's four files"
assert_eq modbus-stage1-detector "$(jq -r .name "$b/bundle.json")" "name"
assert_eq v1 "$(jq -r .version "$b/bundle.json")" "version"
assert_eq modbus-feature-v1 "$(jq -r .schemaId "$b/bundle.json")" "schemaId"
assert_eq 20 "$(jq -r .sequenceLength "$b/bundle.json")" "sequenceLength"
assert_eq 42 "$(jq -r .featureCount "$b/bundle.json")" "featureCount"
assert_eq sequence_20x42 "$(jq -r .inputName "$b/bundle.json")" "inputName from the delivery's manifest"
assert_eq modbus_causal_next_event_predictor "$(jq -r .temporalOutputName "$b/bundle.json")" "temporalOutputName"
assert_eq "$(sha256sum "$b/model.onnx" | cut -c1-64)" "$(jq -r .modelSha "$b/bundle.json")" "modelSha"
assert_eq "$(sha256sum "$b/preprocessing.json" | cut -c1-64)" "$(jq -r .preprocessingSha "$b/bundle.json")" "preprocessingSha"
assert_eq "$(sha256sum "$b/thresholds.json" | cut -c1-64)" "$(jq -r .thresholdsSha "$b/bundle.json")" "thresholdsSha"
assert_eq "$(sha256sum "$tmp/delivery/models/dual_head_model_fp32.onnx" | cut -c1-64)" \
  "$(sha256sum "$b/model.onnx" | cut -c1-64)" "the model is copied byte for byte"

# A bundle is immutable: packaging over an existing one is refused.
out="$(bash "$script" "$tmp/delivery" "$tmp/out" 2>&1)"; status=$?
assert_eq 1 "$status" "an existing bundle is never overwritten"
assert_eq 1 "$(grep -c 'already exists' <<< "$out")" "and says so"

# A corrupted model never gets packaged.
fake_delivery "$tmp/bad"
printf 'tampered' >> "$tmp/bad/models/dual_head_model_fp32.onnx"
out="$(bash "$script" "$tmp/bad" "$tmp/out2" 2>&1)"; status=$?
assert_eq 1 "$status" "a model that does not match the delivery's manifest is refused"
assert_eq 1 "$(grep -c "does not match the delivery's own manifest" <<< "$out")" "and says why"
assert_eq no "$([ -e "$tmp/out2/modbus-stage1-detector" ] && echo yes || echo no)" "and writes nothing"

finish
```

- [ ] **Step 3: Run it to verify it fails**

Run: `bash deploy/tests/test_models.sh`
Expected: FAIL lines (the script does not exist), non-zero exit.

- [ ] **Step 4: Write the packaging script**

`deploy/models/package-modbus-detector.sh`:

```bash
#!/usr/bin/env bash
# Packages the model team's Modbus Stage 1 delivery into the SHA-pinned bundle
# the online job loads (spec section 7): models/modbus-stage1-detector/v1/ with
# model.onnx, preprocessing.json and thresholds.json byte for byte as
# delivered, plus bundle.json. Refuses a model that does not match the
# delivery's own manifest, and never overwrites a bundle.
# Usage: package-modbus-detector.sh <delivery-dir> [out-root, default: models]
set -euo pipefail

die() { printf 'package-modbus-detector: %s\n' "$*" >&2; exit 1; }

delivery="${1:?usage: package-modbus-detector.sh <delivery-dir> [out-root]}"
out_root="${2:-models}"
name=modbus-stage1-detector
version=v1

# The delivery's four files, where the model team's layout puts them.
onnx="$delivery/models/dual_head_model_fp32.onnx"
manifest="$onnx.manifest.json"
preprocessing="$delivery/contracts/modbus_preprocessing_contract_v1/PREPROCESSING_CONTRACT_V1.json"
thresholds="$delivery/contracts/modbus_dual_head_temporal_dense_detector_v1/THRESHOLD_CONTRACT_V1.json"
for f in "$onnx" "$manifest" "$preprocessing" "$thresholds"; do
  [ -f "$f" ] || die "missing $f"
done

# The ONNX file must be the one the delivery's own manifest records.
expected="$(jq -r .onnx_sha256 "$manifest")"
actual="$(sha256sum "$onnx" | cut -c1-64)"
[ "$expected" = "$actual" ] || die "$onnx does not match the delivery's own manifest (expected $expected, got $actual)"

# Bundles are immutable: a new model is a new version, never an overwrite.
out="$out_root/$name/$version"
[ ! -e "$out" ] || die "$out already exists; bundles are immutable -- package a new version instead"

# The three files, byte for byte, then the manifest that pins them.
mkdir -p "$out"
cp "$onnx" "$out/model.onnx"
cp "$preprocessing" "$out/preprocessing.json"
cp "$thresholds" "$out/thresholds.json"
sha() { sha256sum "$1" | cut -c1-64; }
jq -n \
  --arg name "$name" --arg version "$version" \
  --arg input "$(jq -r '.parity.onnx_input_names[0]' "$manifest")" \
  --arg dense "$(jq -r '.parity.onnx_output_names[0]' "$manifest")" \
  --arg temporal "$(jq -r '.parity.onnx_output_names[1]' "$manifest")" \
  --arg model "$(sha "$out/model.onnx")" \
  --arg prep "$(sha "$out/preprocessing.json")" \
  --arg thr "$(sha "$out/thresholds.json")" \
  '{name: $name, version: $version, schemaId: "modbus-feature-v1", sequenceLength: 20, featureCount: 42,
    inputName: $input, denseOutputName: $dense, temporalOutputName: $temporal,
    modelSha: $model, preprocessingSha: $prep, thresholdsSha: $thr}' > "$out/bundle.json"
printf 'packaged %s\n' "$out"
```

Make it executable: `chmod +x deploy/models/package-modbus-detector.sh`.

- [ ] **Step 5: Run the deploy test to verify it passes**

Run: `bash deploy/tests/test_models.sh`
Expected: `test_models.sh: 17 checks, 0 failed`.

- [ ] **Step 6: Wire it into run-all**

In `deploy/tests/run-all.sh`, add `test_models.sh` to the `for test in …` list, and add `models/*.sh` to the shellcheck file list (after `zeek/run-zeek.sh`).

Run: `bash deploy/tests/run-all.sh`
Expected: `run-all: every check passed` (shellcheck clean on the new scripts).

- [ ] **Step 7: Generate the fixture bundle from the real delivery**

Run: `bash deploy/models/package-modbus-detector.sh models/modbus/stage1_anomaly_detector tests/fixtures/models`
Expected: `packaged tests/fixtures/models/modbus-stage1-detector/v1`, and `jq -r .modelSha tests/fixtures/models/modbus-stage1-detector/v1/bundle.json` prints `b5f28fec103bddceb9b1bfcf1f6cc2e36d0780eba5340bd4a663be04bb72bf4f`.

- [ ] **Step 8: Commit**

```bash
git add contracts/model/sequence-detector-bundle-v1.json deploy/models deploy/tests/test_models.sh deploy/tests/run-all.sh tests/fixtures/models/modbus-stage1-detector
git commit -m "feat(scoring): model bundle contract, packaging script and fixture bundle

The fixture bundle is the real delivery packaged by the script; its
2.8 MB model.onnx is committed so the loader, scorer and oracle tests
run the delivered graph, as the conn-demo-v1 fixture does.

<attribution lines>"
```

---

### Task 7: The bundle loader

**Files:**
- Create: `modules/adapter-registry-filesystem/src/main/java/io/netsecml/platform/adapter/registry/LoadedSequenceDetector.java`
- Create: `modules/adapter-registry-filesystem/src/main/java/io/netsecml/platform/adapter/registry/SequenceDetectorBundleLoader.java`
- Test: `modules/adapter-registry-filesystem/src/test/java/io/netsecml/platform/adapter/registry/SequenceDetectorBundleLoaderTest.java`

**Interfaces:**
- Consumes: Task 6's fixture bundle; Task 3's `FeaturePreprocessing` / `ModbusPreprocessing`; Task 4's `SequenceDetectorBundle`; `FeatureSchemaRegistry.byId(String)`.
- Produces: `record LoadedSequenceDetector(SequenceDetectorBundle bundle, byte[] model)` (defensive copies); `SequenceDetectorBundleLoader.load(Path bundleDir) -> LoadedSequenceDetector` throwing `IllegalStateException` naming the failed check.

- [ ] **Step 1: Write the failing test**

```java
package io.netsecml.platform.adapter.registry;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.netsecml.platform.domain.feature.ModbusFeatureSchemaV1;
import io.netsecml.platform.domain.model.SequenceDetectorBundle;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

// The fixture bundle (the real delivery, packaged) loads, and every check the
// spec lists (section 7) refuses a bundle that fails it, naming the failure.
class SequenceDetectorBundleLoaderTest {

    private static final Path FIXTURE = Path.of("..", "..", "tests", "fixtures", "models",
        "modbus-stage1-detector", "v1");
    private static final ObjectMapper MAPPER = new ObjectMapper();

    // A realistic READ_HOLDING_REGISTERS response vector and its preprocessing,
    // computed by the contract's own transform_definitions in Python.
    private static final float[] RAW = {1, 0, 0, 1, 0, 0, 0, 0, 100, 1, 2, 1, 1, 0, 0, 0, 0, 0, 1, 2, 7, 9, 8, 1,
        0.25f, 0, 1, 0, 1, 0, 1, 0, 0, 1, 0.25f, 2, 0.2f, 0.0333333333f, 1, 1, 1, 0};
    private static final float[] PREPROCESSED = {1, 0, 0, 1, 0, 0, 0, 0, 11.1887788772583f, 1, 2, 1, 1, 0, 0, 0, 0,
        0, 1, 2, -0.05083692446351051f, 0.011502460576593876f, -0.017931997776031494f, 1, -0.17137247323989868f,
        0, 1, 8.680343307787552e-05f, 1, 0, 0.23271413147449493f, 0, 0, 1, 0.2231435477733612f,
        1.0986123085021973f, 0.18232156336307526f, 0.03278982266783714f, -9.88362979888916f,
        -4.549434185028076f, 1, 0};

    @Test
    void theFixtureBundleLoadsWithTheDeliveredValues() throws IOException {
        LoadedSequenceDetector loaded = SequenceDetectorBundleLoader.load(FIXTURE);
        SequenceDetectorBundle b = loaded.bundle();
        assertEquals("modbus-stage1-detector/v1", b.bundleId());
        assertEquals("b5f28fec103bddceb9b1bfcf1f6cc2e36d0780eba5340bd4a663be04bb72bf4f", b.modelSha());
        assertEquals(ModbusFeatureSchemaV1.CONTENT_HASH, b.schemaHash(), "the registered schema's hash");
        assertEquals(0.2483385056257248, b.denseThreshold());
        assertEquals(0.4121147692203522, b.temporalThreshold());
        assertEquals(20, b.sequenceLength());
        assertEquals("sequence_20x42", b.inputName());
        assertEquals(Files.size(FIXTURE.resolve("model.onnx")), loaded.model().length);
    }

    // The loader's preprocessing reproduces the contract's own arithmetic.
    @Test
    void thePreprocessingMatchesPython() throws IOException {
        float[] out = SequenceDetectorBundleLoader.load(FIXTURE).bundle().preprocessing().apply(RAW);
        for (int i = 0; i < out.length; i++) {
            assertEquals(PREPROCESSED[i], out[i], 1e-6f, "feature " + i);
        }
    }

    @Test
    void aModelThatDoesNotMatchItsShaIsRefused(@TempDir Path dir) throws Exception {
        Path b = copyFixture(dir);
        Files.write(b.resolve("model.onnx"), new byte[]{1, 2, 3});
        assertRefused(b, "model.onnx");
    }

    @Test
    void aPreprocessingFileThatDoesNotMatchItsShaIsRefused(@TempDir Path dir) throws Exception {
        Path b = copyFixture(dir);
        Files.writeString(b.resolve("preprocessing.json"), Files.readString(b.resolve("preprocessing.json")) + " ");
        assertRefused(b, "preprocessing.json");
    }

    @Test
    void aThresholdsFileThatDoesNotMatchItsShaIsRefused(@TempDir Path dir) throws Exception {
        Path b = copyFixture(dir);
        Files.writeString(b.resolve("thresholds.json"), Files.readString(b.resolve("thresholds.json")) + " ");
        assertRefused(b, "thresholds.json");
    }

    // A preprocessing contract whose feature order differs from the schema's --
    // two features swapped, and its SHA updated so only the order is wrong.
    @Test
    void aFeatureOrderThatDiffersFromTheSchemaIsRefused(@TempDir Path dir) throws Exception {
        Path b = copyFixture(dir);
        ObjectNode prep = (ObjectNode) new ObjectMapper()
            .enable(com.fasterxml.jackson.core.JsonParser.Feature.ALLOW_NON_NUMERIC_NUMBERS)
            .readTree(b.resolve("preprocessing.json").toFile());
        ArrayNode order = (ArrayNode) prep.get("feature_order");
        String first = order.get(0).asText();
        order.set(0, order.get(1));
        order.set(1, MAPPER.getNodeFactory().textNode(first));
        Files.writeString(b.resolve("preprocessing.json"), prep.toString());
        rewriteSha(b, "preprocessingSha", "preprocessing.json");
        assertRefused(b, "feature order");
    }

    @Test
    void aMissingFileIsRefused(@TempDir Path dir) throws Exception {
        Path b = copyFixture(dir);
        Files.delete(b.resolve("thresholds.json"));
        assertThrows(IOException.class, () -> SequenceDetectorBundleLoader.load(b));
    }

    @Test
    void anUnknownSchemaIsRefused(@TempDir Path dir) throws Exception {
        Path b = copyFixture(dir);
        ObjectNode bundle = (ObjectNode) MAPPER.readTree(b.resolve("bundle.json").toFile());
        bundle.put("schemaId", "no-such-schema");
        Files.writeString(b.resolve("bundle.json"), bundle.toString());
        assertThrows(IllegalArgumentException.class, () -> SequenceDetectorBundleLoader.load(b));
    }

    private static void assertRefused(Path bundle, String named) {
        IllegalStateException e = assertThrows(IllegalStateException.class,
            () -> SequenceDetectorBundleLoader.load(bundle));
        assertTrue(e.getMessage().contains(named), e.getMessage());
    }

    private static Path copyFixture(Path dir) throws IOException {
        Path b = dir.resolve("v1");
        Files.createDirectories(b);
        try (Stream<Path> files = Files.list(FIXTURE)) {
            for (Path f : files.toList()) {
                Files.copy(f, b.resolve(f.getFileName().toString()));
            }
        }
        return b;
    }

    private static void rewriteSha(Path bundle, String field, String file) throws Exception {
        ObjectNode json = (ObjectNode) MAPPER.readTree(bundle.resolve("bundle.json").toFile());
        byte[] digest = MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(bundle.resolve(file)));
        json.put(field, HexFormat.of().formatHex(digest));
        Files.writeString(bundle.resolve("bundle.json"), json.toString());
    }
}
```

(`FeatureSchemaRegistry.byId` throws `IllegalArgumentException` on an unknown id — check its message style in `FeatureSchemaRegistry.java:56` and keep the assertion's exception type in step with it.)

- [ ] **Step 2: Run it to verify it fails**

Run: `./mvnw test -pl modules/adapter-registry-filesystem -am -Dtest='SequenceDetectorBundleLoaderTest' -Dsurefire.failIfNoSpecifiedTests=false`
Expected: COMPILATION ERROR — the loader does not exist.

- [ ] **Step 3: Implement `LoadedSequenceDetector` and the loader**

`LoadedSequenceDetector.java`:

```java
package io.netsecml.platform.adapter.registry;

import io.netsecml.platform.domain.model.SequenceDetectorBundle;

import java.util.Arrays;

// A verified bundle and its model bytes, for the ONNX scorer to open.
public record LoadedSequenceDetector(SequenceDetectorBundle bundle, byte[] model) {

    public LoadedSequenceDetector {
        model = Arrays.copyOf(model, model.length);
    }

    @Override
    public byte[] model() {
        return Arrays.copyOf(model, model.length);
    }
}
```

`SequenceDetectorBundleLoader.java`:

```java
package io.netsecml.platform.adapter.registry;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.json.JsonReadFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.json.JsonMapper;
import io.netsecml.platform.domain.feature.FeatureDefinition;
import io.netsecml.platform.domain.feature.FeatureSchema;
import io.netsecml.platform.domain.feature.FeatureSchemaRegistry;
import io.netsecml.platform.domain.model.FeaturePreprocessing;
import io.netsecml.platform.domain.model.ModbusPreprocessing;
import io.netsecml.platform.domain.model.PreprocessingPolicy;
import io.netsecml.platform.domain.model.SequenceDetectorBundle;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;

// Loads a sequence-detector bundle (contracts/model/sequence-detector-bundle-v1.json)
// and refuses it unless every check of spec section 7 passes: each file's
// SHA-256, the feature schema, and the preprocessing's feature order. The
// graph's names and shapes are checked by the ONNX scorer that opens it.
public final class SequenceDetectorBundleLoader {

    // The delivered preprocessing contract writes NaN for unused means and
    // stds -- not strict JSON -- so the mapper must accept it.
    private static final JsonMapper MAPPER = JsonMapper.builder()
        .enable(JsonReadFeature.ALLOW_NON_NUMERIC_NUMBERS)
        .build();

    private SequenceDetectorBundleLoader() {
    }

    public static LoadedSequenceDetector load(Path bundleDir) throws IOException {
        BundleJson bundle = MAPPER.readValue(bundleDir.resolve("bundle.json").toFile(), BundleJson.class);
        byte[] model = Files.readAllBytes(bundleDir.resolve("model.onnx"));
        byte[] preprocessing = Files.readAllBytes(bundleDir.resolve("preprocessing.json"));
        byte[] thresholds = Files.readAllBytes(bundleDir.resolve("thresholds.json"));

        // Every file must be exactly the one bundle.json pins.
        requireSha("model.onnx", bundle.modelSha, model);
        requireSha("preprocessing.json", bundle.preprocessingSha, preprocessing);
        requireSha("thresholds.json", bundle.thresholdsSha, thresholds);

        // The schema must be one this platform builds, and the preprocessing
        // must cover its features in its order.
        FeatureSchema schema = FeatureSchemaRegistry.byId(bundle.schemaId);
        JsonNode prep = MAPPER.readTree(preprocessing);
        List<String> order = new ArrayList<>();
        prep.get("feature_order").forEach(n -> order.add(n.asText()));
        List<String> schemaOrder = schema.definitions().stream().map(FeatureDefinition::name).toList();
        if (!order.equals(schemaOrder)) {
            throw new IllegalStateException("preprocessing.json's feature order differs from " + schema.id()
                + "'s: " + order + " vs " + schemaOrder);
        }

        // Each feature's frozen policy, mask and parameters, in feature order.
        List<FeaturePreprocessing> features = new ArrayList<>();
        for (JsonNode p : prep.get("parameters")) {
            String mask = p.path("mask_feature").asText("");
            features.add(new FeaturePreprocessing(p.get("feature").asText(),
                PreprocessingPolicy.valueOf(p.get("policy").asText()),
                mask.isEmpty() ? -1 : order.indexOf(mask),
                p.get("mean").asDouble(), p.get("std").asDouble()));
        }
        if (!features.stream().map(FeaturePreprocessing::feature).toList().equals(order)) {
            throw new IllegalStateException("preprocessing.json's parameters are not in its feature order");
        }

        // Both thresholds come from the delivered threshold contract.
        JsonNode thr = MAPPER.readTree(thresholds);
        double dense = thr.get("dense").get("threshold").asDouble();
        double temporal = thr.get("temporal").get("threshold").asDouble();

        return new LoadedSequenceDetector(new SequenceDetectorBundle(bundle.name, bundle.version, bundle.modelSha,
            schema.id(), schema.contentHash(), bundle.sequenceLength, bundle.featureCount, bundle.inputName,
            bundle.denseOutputName, bundle.temporalOutputName, dense, temporal, new ModbusPreprocessing(features)),
            model);
    }

    private static void requireSha(String file, String expected, byte[] bytes) {
        String actual = sha256Hex(bytes);
        if (!actual.equals(expected)) {
            throw new IllegalStateException(file + " does not match bundle.json's recorded SHA-256: expected "
                + expected + " but computed " + actual);
        }
    }

    private static String sha256Hex(byte[] bytes) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is required by every JVM", e);
        }
    }

    // bundle.json, every field required.
    @JsonIgnoreProperties(ignoreUnknown = false)
    private record BundleJson(
        @JsonProperty(value = "name", required = true) String name,
        @JsonProperty(value = "version", required = true) String version,
        @JsonProperty(value = "schemaId", required = true) String schemaId,
        @JsonProperty(value = "sequenceLength", required = true) int sequenceLength,
        @JsonProperty(value = "featureCount", required = true) int featureCount,
        @JsonProperty(value = "inputName", required = true) String inputName,
        @JsonProperty(value = "denseOutputName", required = true) String denseOutputName,
        @JsonProperty(value = "temporalOutputName", required = true) String temporalOutputName,
        @JsonProperty(value = "modelSha", required = true) String modelSha,
        @JsonProperty(value = "preprocessingSha", required = true) String preprocessingSha,
        @JsonProperty(value = "thresholdsSha", required = true) String thresholdsSha) {
    }
}
```

In the test, `ALLOW_NON_NUMERIC_NUMBERS` is enabled through `JsonParser.Feature` (deprecated but present in 2.17); if it does not compile, use `JsonMapper.builder().enable(JsonReadFeature.ALLOW_NON_NUMERIC_NUMBERS).build()` there too.

- [ ] **Step 4: Run it to verify it passes**

Run: the Step 2 command. Expected: `Tests run: 8, Failures: 0`.

- [ ] **Step 5: Commit**

```bash
git add modules/adapter-registry-filesystem
git commit -m "feat(scoring): SequenceDetectorBundleLoader verifies and loads a detector bundle

<attribution lines>"
```

---

### Task 8: The ONNX sequence scorer

**Files:**
- Create: `modules/adapter-onnx/src/main/java/io/netsecml/platform/adapter/onnx/runtime/OnnxSequenceScorer.java`
- Test: `modules/adapter-onnx/src/test/java/io/netsecml/platform/adapter/onnx/runtime/OnnxSequenceScorerTest.java`

**Interfaces:**
- Consumes: Task 5's `SequenceScorer`; Task 4's `SequenceDetectorBundle`, `DetectorScores`.
- Produces: `OnnxSequenceScorer(byte[] model, SequenceDetectorBundle bundle)` implementing `SequenceScorer`.

`adapter-onnx` must not import the registry adapter, so the test builds its `SequenceDetectorBundle` by hand from the fixture's `bundle.json` values, with a 42-feature passthrough preprocessing (the scorer never applies preprocessing).

- [ ] **Step 1: Write the failing test**

```java
package io.netsecml.platform.adapter.onnx.runtime;

import io.netsecml.platform.domain.feature.ModbusFeatureSchemaV1;
import io.netsecml.platform.domain.inference.DetectorScores;
import io.netsecml.platform.domain.model.FeaturePreprocessing;
import io.netsecml.platform.domain.model.ModbusPreprocessing;
import io.netsecml.platform.domain.model.PreprocessingPolicy;
import io.netsecml.platform.domain.model.SequenceDetectorBundle;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

// The delivered graph, run from Java: both scores equal Python ONNX Runtime's
// (1.30) for the same windows, to 1e-5, and a bundle whose names or shapes do
// not fit the graph is refused.
class OnnxSequenceScorerTest {

    private static final Path MODEL = Path.of("..", "..", "tests", "fixtures", "models", "modbus-stage1-detector",
        "v1", "model.onnx");

    // One preprocessed READ_HOLDING_REGISTERS response (see
    // SequenceDetectorBundleLoaderTest.PREPROCESSED), repeated 20 times.
    private static final float[] ROW = {1, 0, 0, 1, 0, 0, 0, 0, 11.1887788772583f, 1, 2, 1, 1, 0, 0, 0, 0, 0, 1, 2,
        -0.05083692446351051f, 0.011502460576593876f, -0.017931997776031494f, 1, -0.17137247323989868f, 0, 1,
        8.680343307787552e-05f, 1, 0, 0.23271413147449493f, 0, 0, 1, 0.2231435477733612f, 1.0986123085021973f,
        0.18232156336307526f, 0.03278982266783714f, -9.88362979888916f, -4.549434185028076f, 1, 0};

    private static byte[] model() throws IOException {
        return Files.readAllBytes(MODEL);
    }

    private static SequenceDetectorBundle bundle(int sequenceLength, String input, String dense, String temporal) {
        return new SequenceDetectorBundle("modbus-stage1-detector", "v1", "b".repeat(64),
            ModbusFeatureSchemaV1.SCHEMA.id(), ModbusFeatureSchemaV1.CONTENT_HASH, sequenceLength, 42, input, dense,
            temporal, 0.2483385056257248, 0.4121147692203522, new ModbusPreprocessing(IntStream.range(0, 42)
                .mapToObj(i -> new FeaturePreprocessing("f" + i, PreprocessingPolicy.PASSTHROUGH_BINARY, -1,
                    Double.NaN, Double.NaN)).toList()));
    }

    private static SequenceDetectorBundle delivered() {
        return bundle(20, "sequence_20x42", "modbus_dense_autoencoder", "modbus_causal_next_event_predictor");
    }

    @Test
    void anAllZeroWindowScoresAsPythonDoes() throws IOException {
        try (OnnxSequenceScorer scorer = new OnnxSequenceScorer(model(), delivered())) {
            DetectorScores s = scorer.score(new float[20][42]);
            assertEquals(0.3135979175567627, s.dense(), 1e-5);
            assertEquals(0.3785625994205475, s.temporal(), 1e-5);
        }
    }

    @Test
    void aRealisticWindowScoresAsPythonDoes() throws IOException {
        float[][] window = new float[20][];
        for (int i = 0; i < 20; i++) {
            window[i] = ROW.clone();
        }
        try (OnnxSequenceScorer scorer = new OnnxSequenceScorer(model(), delivered())) {
            DetectorScores s = scorer.score(window);
            assertEquals(0.8671411275863647, s.dense(), 1e-5);
            assertEquals(0.7443642616271973, s.temporal(), 1e-5);
        }
    }

    @Test
    void aWindowOfTheWrongShapeIsRejected() throws IOException {
        try (OnnxSequenceScorer scorer = new OnnxSequenceScorer(model(), delivered())) {
            assertThrows(IllegalArgumentException.class, () -> scorer.score(new float[19][42]));
            assertThrows(IllegalArgumentException.class, () -> scorer.score(new float[20][41]));
        }
    }

    @Test
    void aBundleNamingAnotherInputIsRefused() {
        assertThrows(IllegalStateException.class, () -> new OnnxSequenceScorer(model(),
            bundle(20, "input", "modbus_dense_autoencoder", "modbus_causal_next_event_predictor")));
    }

    @Test
    void aBundleNamingAnotherOutputIsRefused() {
        assertThrows(IllegalStateException.class, () -> new OnnxSequenceScorer(model(),
            bundle(20, "sequence_20x42", "reconstruction", "modbus_causal_next_event_predictor")));
    }

    @Test
    void aBundleWhoseSequenceLengthDiffersFromTheGraphIsRefused() {
        assertThrows(IllegalStateException.class, () -> new OnnxSequenceScorer(model(),
            bundle(16, "sequence_20x42", "modbus_dense_autoencoder", "modbus_causal_next_event_predictor")));
    }

    @Test
    void closingTwiceIsHarmless() throws IOException {
        OnnxSequenceScorer scorer = new OnnxSequenceScorer(model(), delivered());
        scorer.close();
        assertDoesNotThrow(scorer::close);
    }
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `./mvnw test -pl modules/adapter-onnx -am -Dtest='OnnxSequenceScorerTest' -Dsurefire.failIfNoSpecifiedTests=false`
Expected: COMPILATION ERROR — `OnnxSequenceScorer` does not exist.

- [ ] **Step 3: Implement the scorer**

```java
package io.netsecml.platform.adapter.onnx.runtime;

import ai.onnxruntime.NodeInfo;
import ai.onnxruntime.OnnxTensor;
import ai.onnxruntime.OnnxValue;
import ai.onnxruntime.OrtEnvironment;
import ai.onnxruntime.OrtException;
import ai.onnxruntime.OrtSession;
import ai.onnxruntime.TensorInfo;
import io.netsecml.platform.domain.inference.DetectorScores;
import io.netsecml.platform.domain.model.SequenceDetectorBundle;
import io.netsecml.platform.port.out.SequenceScorer;

import java.util.Map;

// Runs a dual-head sequence detector (spec section 3): one window in, the
// dense head's reconstruction MAE and the temporal head's endpoint MAE out --
// upstream's score_dense and score_temporal, computed in double. One session
// per subtask, one thread each way (CLAUDE.md's CPU rule).
public final class OnnxSequenceScorer implements SequenceScorer {

    private static final OrtEnvironment ENVIRONMENT = OrtEnvironment.getEnvironment();

    private final SequenceDetectorBundle bundle;
    private final OrtSession session;
    private boolean closed = false;

    public OnnxSequenceScorer(byte[] model, SequenceDetectorBundle bundle) {
        this.bundle = bundle;
        try {
            OrtSession.SessionOptions options = new OrtSession.SessionOptions();
            options.setIntraOpNumThreads(1);
            options.setInterOpNumThreads(1);
            this.session = ENVIRONMENT.createSession(model, options);
        } catch (OrtException e) {
            throw new IllegalStateException("failed to open the ONNX graph of " + bundle.bundleId(), e);
        }
        // The graph must have exactly the names and shapes the bundle declares.
        try {
            int length = bundle.sequenceLength();
            int width = bundle.featureCount();
            requireShape("input", bundle.inputName(), session.getInputInfo().get(bundle.inputName()), length, width);
            requireShape("dense output", bundle.denseOutputName(),
                session.getOutputInfo().get(bundle.denseOutputName()), length, width);
            requireShape("temporal output", bundle.temporalOutputName(),
                session.getOutputInfo().get(bundle.temporalOutputName()), length - 1, width);
        } catch (OrtException | RuntimeException e) {
            IllegalStateException failure = e instanceof IllegalStateException ise ? ise
                : new IllegalStateException("failed to read the graph of " + bundle.bundleId(), e);
            try {
                session.close();
            } catch (OrtException closeFailure) {
                failure.addSuppressed(closeFailure);
            }
            throw failure;
        }
    }

    // A [batch, rows, width] float tensor with this exact name.
    private static void requireShape(String role, String name, NodeInfo info, int rows, int width) {
        if (info == null || !(info.getInfo() instanceof TensorInfo tensor)) {
            throw new IllegalStateException("the graph has no " + role + " tensor named '" + name + "'");
        }
        long[] shape = tensor.getShape();
        if (shape.length != 3 || shape[1] != rows || shape[2] != width) {
            throw new IllegalStateException("the graph's " + role + " '" + name + "' has shape "
                + java.util.Arrays.toString(shape) + ", not [?, " + rows + ", " + width + "]");
        }
    }

    @Override
    public SequenceDetectorBundle bundle() {
        return bundle;
    }

    @Override
    public DetectorScores score(float[][] sequence) {
        int length = bundle.sequenceLength();
        int width = bundle.featureCount();
        // Exactly one window of the bundle's shape.
        if (sequence.length != length) {
            throw new IllegalArgumentException("expected " + length + " rows, got " + sequence.length);
        }
        for (float[] row : sequence) {
            if (row.length != width) {
                throw new IllegalArgumentException("expected rows of " + width + " values, got " + row.length);
            }
        }
        try (OnnxTensor input = OnnxTensor.createTensor(ENVIRONMENT, new float[][][]{sequence});
             OrtSession.Result result = session.run(Map.of(bundle.inputName(), input))) {
            float[][] dense = output(result, bundle.denseOutputName())[0];
            float[][] temporal = output(result, bundle.temporalOutputName())[0];
            // score_dense: mean |input - reconstruction| over the whole window.
            double denseSum = 0;
            for (int i = 0; i < length; i++) {
                for (int j = 0; j < width; j++) {
                    denseSum += Math.abs(sequence[i][j] - dense[i][j]);
                }
            }
            // score_temporal: mean |prediction of the last event - the last event|.
            double temporalSum = 0;
            for (int j = 0; j < width; j++) {
                temporalSum += Math.abs(temporal[length - 2][j] - sequence[length - 1][j]);
            }
            return new DetectorScores(denseSum / (length * width), temporalSum / width);
        } catch (OrtException e) {
            throw new IllegalStateException("ONNX Runtime failed scoring with " + bundle.bundleId(), e);
        }
    }

    private static float[][][] output(OrtSession.Result result, String name) throws OrtException {
        OnnxValue value = result.get(name).orElseThrow(
            () -> new IllegalStateException("the graph returned no output named '" + name + "'"));
        return (float[][][]) value.getValue();
    }

    // Idempotent; a close failure is rethrown unchecked, never dropped.
    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        try {
            session.close();
        } catch (OrtException e) {
            throw new IllegalStateException("failed to close the ONNX session of " + bundle.bundleId(), e);
        }
    }
}
```

- [ ] **Step 4: Run it to verify it passes**

Run: the Step 2 command. Expected: `Tests run: 7, Failures: 0`.

- [ ] **Step 5: Commit**

```bash
git add modules/adapter-onnx
git commit -m "feat(scoring): OnnxSequenceScorer runs the dual-head detector

Both scores equal Python ONNX Runtime's for the same windows to 1e-5.

<attribution lines>"
```

---

### Task 9: The prediction stream contract and its serializer

**Files:**
- Create: `contracts/stream/modbus-detector-prediction-v1.json`
- Create: `modules/adapter-kafka/src/main/java/io/netsecml/platform/adapter/kafka/sink/ModbusDetectorPredictionSerializer.java`
- Create: `modules/adapter-kafka/src/main/java/io/netsecml/platform/adapter/kafka/sink/ModbusDetectorPredictionDeserializer.java`
- Test: `modules/adapter-kafka/src/test/java/io/netsecml/platform/adapter/kafka/sink/StreamContractDriftTest.java` (two tests), `modules/adapter-kafka/src/test/java/io/netsecml/platform/adapter/kafka/sink/ModbusDetectorPredictionSerializerTest.java`

**Interfaces:**
- Consumes: Task 4's `ModbusDetectorPrediction`, `DetectorVerdict`, `DetectorTrigger`.
- Produces: `ModbusDetectorPredictionSerializer implements Serializer<ModbusDetectorPrediction>, Serializable`; `ModbusDetectorPredictionDeserializer implements Deserializer<ModbusDetectorPrediction>`; JSON field names exactly the contract's.

- [ ] **Step 1: Write the contract**

`contracts/stream/modbus-detector-prediction-v1.json`:

```json
{
  "id": "modbus-detector-prediction-v1",
  "semanticVersion": "1.0.0",
  "topic": "netsec.modbus.prediction.v1",
  "encoding": "application/json",
  "fields": [
    {"name": "predictionId", "type": "string", "required": true, "description": "Prediction.deriveId(eventId, modelName, modelVersion): 64 lowercase hex, stable on replay"},
    {"name": "eventId", "type": "string", "required": true, "description": "The scored feature vector's eventId: the join back to it"},
    {"name": "eventTime", "type": "string", "format": "date-time", "required": true, "description": "The event's time, copied from the feature vector, ISO-8601 UTC"},
    {"name": "sensor", "type": "string", "required": true, "description": "The sensor that captured the event"},
    {"name": "connectionUid", "type": "string", "required": true, "description": "Zeek's connection uid, copied from the feature vector"},
    {"name": "clientIp", "type": "string", "required": true, "description": "The stream's client: the device polling"},
    {"name": "serverIp", "type": "string", "required": true, "description": "The stream's server: the device polled"},
    {"name": "unitId", "type": "string", "required": true, "description": "The stream's Modbus unit id"},
    {"name": "modelName", "type": "string", "required": true, "description": "The detector bundle's name"},
    {"name": "modelVersion", "type": "string", "required": true, "description": "The detector bundle's version"},
    {"name": "modelSha", "type": "string", "required": true, "description": "SHA-256 of the detector's model.onnx"},
    {"name": "schemaId", "type": "string", "required": true, "description": "The feature schema scored, modbus-feature-v1"},
    {"name": "schemaHash", "type": "string", "required": true, "description": "That schema's content hash"},
    {"name": "verdict", "type": "string", "required": true, "description": "WARMUP (fewer than a full window in the stream's segment), NORMAL, ANOMALY, or UNSCORABLE (non-finite preprocessed input)"},
    {"name": "denseScore", "type": "number", "required": true, "nullable": true, "description": "Dense head reconstruction MAE; null unless NORMAL or ANOMALY"},
    {"name": "temporalScore", "type": "number", "required": true, "nullable": true, "description": "Temporal head endpoint MAE; null unless NORMAL or ANOMALY"},
    {"name": "denseThreshold", "type": "number", "required": true, "description": "The bundle's dense threshold"},
    {"name": "temporalThreshold", "type": "number", "required": true, "description": "The bundle's temporal threshold"},
    {"name": "trigger", "type": "string", "required": true, "description": "NONE, DENSE, TEMPORAL or BOTH: which head exceeded its threshold"},
    {"name": "windowEvents", "type": "integer", "required": true, "description": "The window's size after this event, 0-20; unchanged by an UNSCORABLE event"},
    {"name": "qualityFlags", "type": "integer", "required": true, "description": "OR of this vector's quality flags and those of every vector in the window"},
    {"name": "inferenceMicros", "type": "integer", "required": true, "description": "Inference time in microseconds; 0 unless scored"},
    {"name": "producedAt", "type": "string", "format": "date-time", "required": true, "description": "When the prediction was emitted, ISO-8601 UTC"}
  ]
}
```

- [ ] **Step 2: Write the failing tests**

Append to `StreamContractDriftTest` (it has `contractFields(String)` and `messageFields(byte[])`):

```java
    private static ModbusDetectorPrediction scoredPrediction() {
        return new ModbusDetectorPrediction("a".repeat(64), "sensor-eu-1:u:7:RESPONSE:1", Instant.parse(
            "2026-09-26T12:00:00Z"), new SensorId("sensor-eu-1"), "u", "10.0.0.5", "10.0.0.9", "1",
            "modbus-stage1-detector", "v1", "b".repeat(64), "modbus-feature-v1", "c".repeat(64),
            DetectorVerdict.ANOMALY, 0.3f, 0.2f, 0.2483385f, 0.4121148f, DetectorTrigger.DENSE, 20, 8, 180L,
            Instant.parse("2026-09-26T12:00:00.004Z"));
    }

    @Test
    void modbusDetectorPredictionSerializerEmitsExactlyTheContractFields() throws Exception {
        Set<String> emitted = messageFields(new ModbusDetectorPredictionSerializer()
            .serialize("netsec.modbus.prediction.v1", scoredPrediction()));
        assertEquals(contractFields("modbus-detector-prediction-v1.json"), emitted,
            "the serializer and contracts/stream/modbus-detector-prediction-v1.json must describe the same message");
    }
```

`ModbusDetectorPredictionSerializerTest.java`:

```java
package io.netsecml.platform.adapter.kafka.sink;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netsecml.platform.domain.event.SensorId;
import io.netsecml.platform.domain.inference.DetectorTrigger;
import io.netsecml.platform.domain.inference.DetectorVerdict;
import io.netsecml.platform.domain.inference.ModbusDetectorPrediction;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

// Round trips, and a WARMUP prediction's scores are JSON nulls, not absent keys.
class ModbusDetectorPredictionSerializerTest {

    private static ModbusDetectorPrediction warmup() {
        return new ModbusDetectorPrediction("a".repeat(64), "s:u:7:REQUEST:1", Instant.parse("2026-09-26T12:00:00Z"),
            new SensorId("s"), "u", "10.0.0.5", "10.0.0.9", "1", "modbus-stage1-detector", "v1", "b".repeat(64),
            "modbus-feature-v1", "c".repeat(64), DetectorVerdict.WARMUP, null, null, 0.2483385f, 0.4121148f,
            DetectorTrigger.NONE, 3, 0, 0L, Instant.parse("2026-09-26T12:00:00.001Z"));
    }

    @Test
    void aWarmupsScoresAreJsonNulls() throws Exception {
        JsonNode node = new ObjectMapper().readTree(
            new ModbusDetectorPredictionSerializer().serialize("t", warmup()));
        assertTrue(node.has("denseScore") && node.get("denseScore").isNull());
        assertTrue(node.has("temporalScore") && node.get("temporalScore").isNull());
        assertEquals("WARMUP", node.get("verdict").asText());
    }

    @Test
    void everyFieldSurvivesARoundTrip() {
        ModbusDetectorPrediction original = warmup();
        ModbusDetectorPrediction restored = new ModbusDetectorPredictionDeserializer()
            .deserialize("t", new ModbusDetectorPredictionSerializer().serialize("t", original));
        assertEquals(original, restored);
        assertNull(restored.denseScore());
    }
}
```

- [ ] **Step 3: Run them to verify they fail**

Run: `./mvnw test -pl modules/adapter-kafka -am -Dtest='StreamContractDriftTest,ModbusDetectorPredictionSerializerTest' -Dsurefire.failIfNoSpecifiedTests=false`
Expected: COMPILATION ERROR — the serializer and deserializer do not exist.

- [ ] **Step 4: Implement them**

`ModbusDetectorPredictionSerializer.java`:

```java
package io.netsecml.platform.adapter.kafka.sink;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.netsecml.platform.domain.inference.ModbusDetectorPrediction;
import org.apache.kafka.common.serialization.Serializer;

import java.io.Serializable;

// contracts/stream/modbus-detector-prediction-v1.json, field for field. The
// two scores are always written -- as JSON null when the detector did not run
// -- so every message carries the same keys.
public final class ModbusDetectorPredictionSerializer implements Serializer<ModbusDetectorPrediction>, Serializable {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public byte[] serialize(String topic, ModbusDetectorPrediction p) {
        try {
            ObjectNode node = objectMapper.createObjectNode();
            node.put("predictionId", p.predictionId());
            node.put("eventId", p.eventId());
            node.put("eventTime", p.eventTime().toString());
            node.put("sensor", p.sensor().value());
            node.put("connectionUid", p.connectionUid());
            node.put("clientIp", p.clientIp());
            node.put("serverIp", p.serverIp());
            node.put("unitId", p.unitId());
            node.put("modelName", p.modelName());
            node.put("modelVersion", p.modelVersion());
            node.put("modelSha", p.modelSha());
            node.put("schemaId", p.schemaId());
            node.put("schemaHash", p.schemaHash());
            node.put("verdict", p.verdict().name());
            node.put("denseScore", p.denseScore());
            node.put("temporalScore", p.temporalScore());
            node.put("denseThreshold", p.denseThreshold());
            node.put("temporalThreshold", p.temporalThreshold());
            node.put("trigger", p.trigger().name());
            node.put("windowEvents", p.windowEvents());
            node.put("qualityFlags", p.qualityFlags());
            node.put("inferenceMicros", p.inferenceMicros());
            node.put("producedAt", p.producedAt().toString());
            return objectMapper.writeValueAsBytes(node);
        } catch (Exception e) {
            throw new RuntimeException("failed to serialize ModbusDetectorPrediction for topic " + topic, e);
        }
    }
}
```

(`ObjectNode.put(String, Float)` writes JSON null for a null `Float`.)

`ModbusDetectorPredictionDeserializer.java`:

```java
package io.netsecml.platform.adapter.kafka.sink;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netsecml.platform.domain.event.SensorId;
import io.netsecml.platform.domain.inference.DetectorTrigger;
import io.netsecml.platform.domain.inference.DetectorVerdict;
import io.netsecml.platform.domain.inference.ModbusDetectorPrediction;
import org.apache.kafka.common.serialization.Deserializer;

import java.time.Instant;

// The archive job's side of contracts/stream/modbus-detector-prediction-v1.json.
public final class ModbusDetectorPredictionDeserializer implements Deserializer<ModbusDetectorPrediction> {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public ModbusDetectorPrediction deserialize(String topic, byte[] data) {
        try {
            JsonNode n = objectMapper.readTree(data);
            return new ModbusDetectorPrediction(
                n.get("predictionId").asText(), n.get("eventId").asText(),
                Instant.parse(n.get("eventTime").asText()), new SensorId(n.get("sensor").asText()),
                n.get("connectionUid").asText(), n.get("clientIp").asText(), n.get("serverIp").asText(),
                n.get("unitId").asText(), n.get("modelName").asText(), n.get("modelVersion").asText(),
                n.get("modelSha").asText(), n.get("schemaId").asText(), n.get("schemaHash").asText(),
                DetectorVerdict.valueOf(n.get("verdict").asText()),
                nullableFloat(n.get("denseScore")), nullableFloat(n.get("temporalScore")),
                (float) n.get("denseThreshold").asDouble(), (float) n.get("temporalThreshold").asDouble(),
                DetectorTrigger.valueOf(n.get("trigger").asText()), n.get("windowEvents").asInt(),
                n.get("qualityFlags").asInt(), n.get("inferenceMicros").asLong(),
                Instant.parse(n.get("producedAt").asText()));
        } catch (Exception e) {
            throw new IllegalArgumentException("failed to deserialize ModbusDetectorPrediction from topic " + topic, e);
        }
    }

    // A score is null when the detector did not run.
    private static Float nullableFloat(JsonNode node) {
        return node == null || node.isNull() ? null : (float) node.asDouble();
    }
}
```

- [ ] **Step 5: Run them to verify they pass**

Run: the Step 3 command. Expected: all pass (`StreamContractDriftTest` one more than before; `ModbusDetectorPredictionSerializerTest` 2).

- [ ] **Step 6: Commit**

```bash
git add contracts/stream/modbus-detector-prediction-v1.json modules/adapter-kafka
git commit -m "feat(scoring): modbus-detector-prediction-v1 contract and its serializer

<attribution lines>"
```

---

### Task 10: The ClickHouse table, row and mapper

**Files:**
- Create: `infrastructure/clickhouse/ddl/003_modbus_detector_predictions.sql`
- Create: `modules/adapter-clickhouse/src/main/java/io/netsecml/platform/adapter/clickhouse/row/ModbusDetectorPredictionRow.java`
- Create: `modules/adapter-clickhouse/src/main/java/io/netsecml/platform/adapter/clickhouse/mapper/ModbusDetectorPredictionRowMapper.java`
- Test: `modules/adapter-clickhouse/src/test/java/io/netsecml/platform/adapter/clickhouse/mapper/ModbusDetectorPredictionRowMapperTest.java`; modify `SchemaDriftTest.java` (one test) and `DdlDirectoryTest.java` (the pinned file list)

**Interfaces:**
- Consumes: Task 4's `ModbusDetectorPrediction`; `ClickHouseTimestamps.format(Instant)` (package-private in the `mapper` package).
- Produces: `record ModbusDetectorPredictionRow(...)` with snake_case `@JsonProperty` names equal to the table's columns (except `created_at`); `ModbusDetectorPredictionRowMapper implements Serializable` with `ModbusDetectorPredictionRow toRow(ModbusDetectorPrediction)`.

- [ ] **Step 1: Write the DDL**

`infrastructure/clickhouse/ddl/003_modbus_detector_predictions.sql`:

```sql
-- Modbus Stage 1 detector predictions (docs/superpowers/specs/2026-09-26-modbus-stage1-scoring-design.md,
-- section 6): one row per Modbus event, from netsec.modbus.prediction.v1. The
-- existing predictions table's rules -- ReplacingMergeTree, the same ORDER BY
-- and 180-day TTL -- so a replayed prediction collapses into one row. Scores
-- are null unless the verdict is NORMAL or ANOMALY. Idempotent.
CREATE TABLE IF NOT EXISTS modbus_detector_predictions (
  prediction_id      FixedString(64),
  event_id           String,
  event_time         DateTime64(3, 'UTC'),
  sensor             LowCardinality(String),
  connection_uid     String,
  client_ip          String,
  server_ip          String,
  unit_id            LowCardinality(String),
  model_name         LowCardinality(String),
  model_version      LowCardinality(String),
  model_sha          FixedString(64),
  schema_id          LowCardinality(String),
  schema_hash        FixedString(64),
  verdict            LowCardinality(String),
  dense_score        Nullable(Float32),
  temporal_score     Nullable(Float32),
  dense_threshold    Float32,
  temporal_threshold Float32,
  trigger            LowCardinality(String),
  window_events      UInt8,
  quality_flags      UInt32,
  inference_us       UInt32,
  created_at         DateTime64(3, 'UTC') DEFAULT now64(3),
  row_version        DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(row_version)
PARTITION BY toYYYYMMDD(event_time)
ORDER BY (model_name, model_version, event_time, event_id)
TTL toDateTime(event_time) + INTERVAL 180 DAY;
```

- [ ] **Step 2: Write the failing tests**

Add to `SchemaDriftTest` next to its two existing tests:

```java
    @Test
    void modbusDetectorPredictionRowPropertiesMatchDdlColumns() throws Exception {
        assertJsonPropertiesAreDdlColumns(ModbusDetectorPredictionRow.class, "modbus_detector_predictions");
    }
```

In `DdlDirectoryTest`, change the pinned list in `ddlFilesAreReturnedInLexicalOrder` to
`List.of("001_mvp_tables.sql", "002_add_invalid_events_log_type.sql", "003_modbus_detector_predictions.sql")`
and its message to `"DDL files must be applied in lexical order"`.

`ModbusDetectorPredictionRowMapperTest.java`:

```java
package io.netsecml.platform.adapter.clickhouse.mapper;

import io.netsecml.platform.adapter.clickhouse.row.ModbusDetectorPredictionRow;
import io.netsecml.platform.domain.event.SensorId;
import io.netsecml.platform.domain.inference.DetectorTrigger;
import io.netsecml.platform.domain.inference.DetectorVerdict;
import io.netsecml.platform.domain.inference.ModbusDetectorPrediction;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

// A prediction becomes one row: ClickHouse timestamps, enum names, null scores kept.
class ModbusDetectorPredictionRowMapperTest {

    @Test
    void aWarmupMapsWithNullScores() {
        ModbusDetectorPrediction p = new ModbusDetectorPrediction("a".repeat(64), "s:u:7:REQUEST:1",
            Instant.parse("2026-09-26T12:00:00.123Z"), new SensorId("s"), "u", "10.0.0.5", "10.0.0.9", "1",
            "modbus-stage1-detector", "v1", "b".repeat(64), "modbus-feature-v1", "c".repeat(64),
            DetectorVerdict.WARMUP, null, null, 0.2483385f, 0.4121148f, DetectorTrigger.NONE, 3, 8, 0L,
            Instant.parse("2026-09-26T12:00:00.456Z"));
        ModbusDetectorPredictionRow row = new ModbusDetectorPredictionRowMapper().toRow(p);
        assertEquals("2026-09-26 12:00:00.123", row.eventTime());
        assertEquals("2026-09-26 12:00:00.456", row.rowVersion(), "row_version is producedAt");
        assertEquals("WARMUP", row.verdict());
        assertEquals("NONE", row.trigger());
        assertNull(row.denseScore());
        assertEquals("s", row.sensor());
        assertEquals(3, row.windowEvents());
    }
}
```

- [ ] **Step 3: Run them to verify they fail**

Run: `./mvnw test -pl modules/adapter-clickhouse -am -Dtest='SchemaDriftTest,DdlDirectoryTest,ModbusDetectorPredictionRowMapperTest' -Dsurefire.failIfNoSpecifiedTests=false`
Expected: COMPILATION ERROR — the row and mapper do not exist.

- [ ] **Step 4: Implement the row and mapper**

`ModbusDetectorPredictionRow.java`:

```java
package io.netsecml.platform.adapter.clickhouse.row;

import com.fasterxml.jackson.annotation.JsonProperty;

// One modbus_detector_predictions row, column for column (created_at is the
// server's default). SchemaDriftTest pins these names to the DDL.
public record ModbusDetectorPredictionRow(
    @JsonProperty("prediction_id") String predictionId,
    @JsonProperty("event_id") String eventId,
    @JsonProperty("event_time") String eventTime,
    @JsonProperty("sensor") String sensor,
    @JsonProperty("connection_uid") String connectionUid,
    @JsonProperty("client_ip") String clientIp,
    @JsonProperty("server_ip") String serverIp,
    @JsonProperty("unit_id") String unitId,
    @JsonProperty("model_name") String modelName,
    @JsonProperty("model_version") String modelVersion,
    @JsonProperty("model_sha") String modelSha,
    @JsonProperty("schema_id") String schemaId,
    @JsonProperty("schema_hash") String schemaHash,
    @JsonProperty("verdict") String verdict,
    @JsonProperty("dense_score") Float denseScore,
    @JsonProperty("temporal_score") Float temporalScore,
    @JsonProperty("dense_threshold") float denseThreshold,
    @JsonProperty("temporal_threshold") float temporalThreshold,
    @JsonProperty("trigger") String trigger,
    @JsonProperty("window_events") int windowEvents,
    @JsonProperty("quality_flags") int qualityFlags,
    @JsonProperty("inference_us") long inferenceUs,
    @JsonProperty("row_version") String rowVersion) {
}
```

`ModbusDetectorPredictionRowMapper.java`:

```java
package io.netsecml.platform.adapter.clickhouse.mapper;

import io.netsecml.platform.adapter.clickhouse.row.ModbusDetectorPredictionRow;
import io.netsecml.platform.domain.inference.ModbusDetectorPrediction;

import java.io.Serializable;

// A prediction as one row. row_version is producedAt, as for feature vectors,
// so a replay's later copy wins the ReplacingMergeTree merge.
public final class ModbusDetectorPredictionRowMapper implements Serializable {

    public ModbusDetectorPredictionRow toRow(ModbusDetectorPrediction p) {
        return new ModbusDetectorPredictionRow(
            p.predictionId(), p.eventId(), ClickHouseTimestamps.format(p.eventTime()), p.sensor().value(),
            p.connectionUid(), p.clientIp(), p.serverIp(), p.unitId(), p.modelName(), p.modelVersion(),
            p.modelSha(), p.schemaId(), p.schemaHash(), p.verdict().name(), p.denseScore(), p.temporalScore(),
            p.denseThreshold(), p.temporalThreshold(), p.trigger().name(), p.windowEvents(), p.qualityFlags(),
            p.inferenceMicros(), ClickHouseTimestamps.format(p.producedAt()));
    }
}
```

- [ ] **Step 5: Run them to verify they pass**

Run: the Step 3 command. Expected: all pass (`SchemaDriftTest` +1, `DdlDirectoryTest` unchanged count, mapper test 1). Also run `bash scripts/database/apply-ddl.sh --help 2>/dev/null; grep -n 'for ddl in' scripts/database/apply-ddl.sh` to confirm it applies every `*.sql` in order — no change needed.

- [ ] **Step 6: Commit**

```bash
git add infrastructure/clickhouse/ddl/003_modbus_detector_predictions.sql modules/adapter-clickhouse
git commit -m "feat(scoring): modbus_detector_predictions table, row and mapper

<attribution lines>"
```

---

### Task 11: The Flink side output and the scoring operator

**Files:**
- Create: `modules/adapter-flink/src/main/java/io/netsecml/platform/adapter/flink/process/KeyedModbusVector.java`
- Create: `modules/adapter-flink/src/main/java/io/netsecml/platform/adapter/flink/process/KeyedModbusVectorKeySelector.java`
- Create: `modules/adapter-flink/src/main/java/io/netsecml/platform/adapter/flink/process/ModbusScoringProcessFunction.java`
- Modify: `modules/adapter-flink/src/main/java/io/netsecml/platform/adapter/flink/process/ModbusFeatureProcessFunction.java` (side output)
- Test: `modules/adapter-flink/src/test/java/io/netsecml/platform/adapter/flink/process/ModbusScoringProcessFunctionTest.java`; add one test to `ModbusFeatureProcessFunctionTest`

**Interfaces:**
- Consumes: Task 5's `SequenceScorerFactory`, `ScoreModbusSequenceUseCase`, `ModbusScoringResult`; Task 4's types.
- Produces: `record KeyedModbusVector(ModbusEntityKey key, FeatureVector vector)`; `KeyedModbusVectorKeySelector implements KeySelector<KeyedModbusVector, ModbusEntityKey>`; `ModbusFeatureProcessFunction.SCORING_TAG` (`OutputTag<KeyedModbusVector>`, id `"modbus-scoring"`); `ModbusScoringProcessFunction(SequenceScorerFactory factory, Duration stateTtl)` extending `KeyedProcessFunction<ModbusEntityKey, KeyedModbusVector, ModbusDetectorPrediction>`; state `modbus-score-window`; metric counter `unscorable`.

- [ ] **Step 1: Write the failing tests**

Add to `ModbusFeatureProcessFunctionTest`:

```java
    // The side output carries every vector with its stream key; a response
    // lands in its request's stream.
    @Test
    void theScoringSideOutputCarriesEachVectorWithItsStreamKey() throws Exception {
        OneInputStreamOperatorTestHarness<ModbusEvent, FeatureVector> harness = harness();
        harness.open();
        harness.processElement(new StreamRecord<>(request(1000.0, 3, "7")));
        harness.processElement(new StreamRecord<>(response(1000.25, 3, "7")));
        List<KeyedModbusVector> side = harness.getSideOutput(ModbusFeatureProcessFunction.SCORING_TAG).stream()
            .map(StreamRecord::getValue).toList();
        assertEquals(2, side.size());
        assertEquals(side.get(0).key(), side.get(1).key(), "request and response share one stream");
        assertEquals("10.0.0.5", side.get(0).key().clientIp());
        assertEquals(harness.extractOutputValues().get(1).eventId(), side.get(1).vector().eventId());
        harness.close();
    }
```

`ModbusScoringProcessFunctionTest.java`:

```java
package io.netsecml.platform.adapter.flink.process;

import io.netsecml.platform.domain.event.LogType;
import io.netsecml.platform.domain.event.SensorId;
import io.netsecml.platform.domain.feature.FeatureVector;
import io.netsecml.platform.domain.feature.ModbusEntityKey;
import io.netsecml.platform.domain.feature.ModbusFeatureSchemaV1;
import io.netsecml.platform.domain.inference.DetectorScores;
import io.netsecml.platform.domain.inference.DetectorVerdict;
import io.netsecml.platform.domain.inference.ModbusDetectorPrediction;
import io.netsecml.platform.domain.model.FeaturePreprocessing;
import io.netsecml.platform.domain.model.ModbusPreprocessing;
import io.netsecml.platform.domain.model.PreprocessingPolicy;
import io.netsecml.platform.domain.model.SequenceDetectorBundle;
import io.netsecml.platform.port.out.SequenceScorer;
import io.netsecml.platform.port.out.SequenceScorerFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;

// The scoring operator in a Flink harness, with a stub scorer whose dense
// score is the last row's feature 0: warm-up, the idle TTL, and a restore
// mid-window giving exactly an uninterrupted run's predictions.
class ModbusScoringProcessFunctionTest {

    private static final ModbusEntityKey KEY = new ModbusEntityKey(new SensorId("s"), "10.0.0.5", "10.0.0.9", "1");

    // Serializable, as the operator requires; builds its stub on the "TaskManager".
    static final class StubFactory implements SequenceScorerFactory {
        @Override
        public SequenceScorer create() {
            SequenceDetectorBundle bundle = new SequenceDetectorBundle("modbus-stage1-detector", "v1",
                "b".repeat(64), ModbusFeatureSchemaV1.SCHEMA.id(), ModbusFeatureSchemaV1.CONTENT_HASH, 20, 42,
                "sequence_20x42", "d", "t", 0.5, 0.5, new ModbusPreprocessing(IntStream.range(0, 42)
                    .mapToObj(i -> new FeaturePreprocessing("f" + i, PreprocessingPolicy.PASSTHROUGH_BINARY, -1,
                        Double.NaN, Double.NaN)).toList()));
            return new SequenceScorer() {
                @Override
                public SequenceDetectorBundle bundle() {
                    return bundle;
                }

                @Override
                public DetectorScores score(float[][] sequence) {
                    return new DetectorScores(sequence[sequence.length - 1][0], 0.0);
                }

                @Override
                public void close() {
                }
            };
        }
    }

    private static OneInputStreamOperatorTestHarness<KeyedModbusVector, ModbusDetectorPrediction> harness()
            throws Exception {
        return new KeyedOneInputStreamOperatorTestHarness<>(
            new KeyedProcessOperator<>(new ModbusScoringProcessFunction(new StubFactory(), Duration.ofHours(1))),
            new KeyedModbusVectorKeySelector(), TypeInformation.of(ModbusEntityKey.class));
    }

    // Event i: feature 0 is i / 100 (so later events score higher), segment start only at 0.
    private static KeyedModbusVector event(int i) {
        float[] values = new float[42];
        values[0] = i / 100f;
        values[23] = i == 0 ? 0f : 1f;
        return new KeyedModbusVector(KEY, new FeatureVector("s:u:" + i + ":REQUEST:" + i,
            Instant.ofEpochSecond(1000 + i), new SensorId("s"), LogType.MODBUS, "u",
            ModbusFeatureSchemaV1.SCHEMA.id(), ModbusFeatureSchemaV1.CONTENT_HASH, values, 0,
            Instant.ofEpochSecond(1000 + i)));
    }

    @Test
    void itWarmsUpThenScores() throws Exception {
        var harness = harness();
        harness.open();
        for (int i = 0; i < 21; i++) {
            harness.processElement(new StreamRecord<>(event(i)));
        }
        List<ModbusDetectorPrediction> out = harness.extractOutputValues();
        assertEquals(21, out.size());
        assertEquals(DetectorVerdict.WARMUP, out.get(18).verdict());
        assertEquals(DetectorVerdict.NORMAL, out.get(19).verdict(), "0.19 is not above 0.5");
        assertEquals(0.20f, out.get(20).denseScore(), 1e-6f);
        harness.close();
    }

    @Test
    void anIdleWindowExpiresAfterTheTtl() throws Exception {
        var harness = harness();
        harness.setStateTtlProcessingTime(0L);
        harness.open();
        for (int i = 0; i < 19; i++) {
            harness.processElement(new StreamRecord<>(event(i)));
        }
        harness.setStateTtlProcessingTime(Duration.ofMinutes(61).toMillis());
        harness.processElement(new StreamRecord<>(event(19)));
        ModbusDetectorPrediction last = harness.extractOutputValues().get(19);
        assertEquals(DetectorVerdict.WARMUP, last.verdict(), "the 19 earlier rows expired");
        assertEquals(1, last.windowEvents());
        harness.close();
    }

    @Test
    void aRestoreMidWindowGivesTheUninterruptedPredictions() throws Exception {
        var uninterrupted = harness();
        uninterrupted.open();
        for (int i = 0; i < 25; i++) {
            uninterrupted.processElement(new StreamRecord<>(event(i)));
        }
        List<ModbusDetectorPrediction> expected = uninterrupted.extractOutputValues();
        uninterrupted.close();

        var first = harness();
        first.open();
        for (int i = 0; i < 12; i++) {
            first.processElement(new StreamRecord<>(event(i)));
        }
        OperatorSubtaskState snapshot = first.snapshot(1L, 1L);
        first.close();

        var second = harness();
        second.initializeState(snapshot);
        second.open();
        for (int i = 12; i < 25; i++) {
            second.processElement(new StreamRecord<>(event(i)));
        }
        List<ModbusDetectorPrediction> actual = second.extractOutputValues();
        second.close();

        for (int i = 0; i < actual.size(); i++) {
            ModbusDetectorPrediction e = expected.get(12 + i);
            assertEquals(e.verdict(), actual.get(i).verdict(), "event " + (12 + i));
            assertEquals(e.windowEvents(), actual.get(i).windowEvents(), "event " + (12 + i));
            assertEquals(e.denseScore(), actual.get(i).denseScore(), "event " + (12 + i));
        }
    }
}
```

- [ ] **Step 2: Run them to verify they fail**

Run: `./mvnw test -pl modules/adapter-flink -am -Dtest='ModbusScoringProcessFunctionTest,ModbusFeatureProcessFunctionTest' -Dsurefire.failIfNoSpecifiedTests=false`
Expected: COMPILATION ERROR — the new types and `SCORING_TAG` do not exist.

- [ ] **Step 3: Implement the record, the key selector and the side output**

`KeyedModbusVector.java`:

```java
package io.netsecml.platform.adapter.flink.process;

import io.netsecml.platform.domain.feature.FeatureVector;
import io.netsecml.platform.domain.feature.ModbusEntityKey;

// A Modbus feature vector with the stream key it was built under: what
// modbus-features hands modbus-score (spec section 4). The vector itself
// carries no client, server or unit.
public record KeyedModbusVector(ModbusEntityKey key, FeatureVector vector) {
}
```

`KeyedModbusVectorKeySelector.java`:

```java
package io.netsecml.platform.adapter.flink.process;

import io.netsecml.platform.domain.feature.ModbusEntityKey;
import org.apache.flink.api.java.functions.KeySelector;

// modbus-score is keyed by the same stream key as modbus-features.
public final class KeyedModbusVectorKeySelector implements KeySelector<KeyedModbusVector, ModbusEntityKey> {
    @Override
    public ModbusEntityKey getKey(KeyedModbusVector value) {
        return value.key();
    }
}
```

In `ModbusFeatureProcessFunction`, add the tag after the TTL constant:

```java
    // Every vector, with its stream key, for modbus-score (spec section 4). A
    // side output, so the main output and the feature-vector topic are unchanged.
    public static final OutputTag<KeyedModbusVector> SCORING_TAG = new OutputTag<>("modbus-scoring") {};
```

and at the end of `processElement`, after `out.collect(result.vector());`:

```java
        ctx.output(SCORING_TAG, new KeyedModbusVector(ctx.getCurrentKey(), result.vector()));
```

Import `org.apache.flink.util.OutputTag`.

- [ ] **Step 4: Implement the scoring operator**

```java
package io.netsecml.platform.adapter.flink.process;

import io.netsecml.platform.application.usecase.ModbusScoringResult;
import io.netsecml.platform.application.usecase.ScoreModbusSequenceUseCase;
import io.netsecml.platform.domain.feature.ModbusEntityKey;
import io.netsecml.platform.domain.inference.DetectorVerdict;
import io.netsecml.platform.domain.inference.ModbusDetectorPrediction;
import io.netsecml.platform.domain.inference.ModbusScoreWindow;
import io.netsecml.platform.port.out.SequenceScorer;
import io.netsecml.platform.port.out.SequenceScorerFactory;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Clock;
import java.time.Duration;
import java.util.Objects;

// modbus-score (spec section 4): holds each stream's window of preprocessed
// vectors and emits one prediction per Modbus event. The scorer is created per
// subtask in open() and closed in close().
public final class ModbusScoringProcessFunction
        extends KeyedProcessFunction<ModbusEntityKey, KeyedModbusVector, ModbusDetectorPrediction> {

    private final SequenceScorerFactory factory;
    private final Duration stateTtl;

    private transient ValueState<ModbusScoreWindow> windowState;
    private transient SequenceScorer scorer;
    private transient ScoreModbusSequenceUseCase useCase;
    private transient Counter unscorable;

    public ModbusScoringProcessFunction(SequenceScorerFactory factory, Duration stateTtl) {
        this.factory = Objects.requireNonNull(factory, "factory");
        Objects.requireNonNull(stateTtl, "stateTtl");
        if (stateTtl.isZero() || stateTtl.isNegative()) {
            throw new IllegalArgumentException("stateTtl must be positive, was " + stateTtl);
        }
        this.stateTtl = stateTtl;
    }

    @Override
    public void open(OpenContext openContext) {
        // "modbus-score-window": a state name is checkpoint identity; never
        // rename it. The same idle TTL as the feature state (spec section 5).
        StateTtlConfig ttl = StateTtlConfig.newBuilder(stateTtl)
            .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
            .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
            .cleanupFullSnapshot()
            .build();
        ValueStateDescriptor<ModbusScoreWindow> descriptor = new ValueStateDescriptor<>(
            "modbus-score-window", TypeInformation.of(ModbusScoreWindow.class));
        descriptor.enableTimeToLive(ttl);
        windowState = getRuntimeContext().getState(descriptor);
        // The model is loaded here, once per subtask; a bad bundle fails the job.
        scorer = factory.create();
        useCase = new ScoreModbusSequenceUseCase(scorer, Clock.systemUTC());
        unscorable = getRuntimeContext().getMetricGroup().counter("unscorable");
    }

    @Override
    public void processElement(KeyedModbusVector in, Context ctx, Collector<ModbusDetectorPrediction> out)
            throws Exception {
        // Read through value() on every call, never cached, as the feature state is.
        ModbusScoringResult result = useCase.score(ctx.getCurrentKey(), in.vector(), windowState.value());
        windowState.update(result.window());
        if (result.prediction().verdict() == DetectorVerdict.UNSCORABLE) {
            unscorable.inc();
        }
        out.collect(result.prediction());
    }

    @Override
    public void close() throws Exception {
        if (scorer != null) {
            scorer.close();
        }
        super.close();
    }
}
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: the Step 2 command. Expected: `ModbusScoringProcessFunctionTest` 3 and `ModbusFeatureProcessFunctionTest` 9, all passing.

- [ ] **Step 6: Commit**

```bash
git add modules/adapter-flink
git commit -m "feat(scoring): modbus-features side output and the modbus-score operator

<attribution lines>"
```

---

### Task 12: Wire scoring into the online job

**Files:**
- Create: `modules/bootstrap-online-job/src/main/java/io/netsecml/platform/bootstrap/online/ModbusDetectorScorerFactory.java`
- Modify: `modules/bootstrap-online-job/src/main/java/io/netsecml/platform/bootstrap/online/OnlineFeatureJob.java`
- Test: `modules/bootstrap-online-job/src/test/java/io/netsecml/platform/bootstrap/online/ModbusDetectorScorerFactoryTest.java`; modify `OnlineFeatureJobTopologyTest.java`

**Interfaces:**
- Consumes: Task 7's `SequenceDetectorBundleLoader.load(Path)`; Task 8's `OnnxSequenceScorer`; Task 9's serializer; Task 11's operator, tag and key selector.
- Produces: `ModbusDetectorScorerFactory(String bundleDir) implements SequenceScorerFactory`; `OnlineFeatureJob.ModbusScoring(String bundleDir, String predictionTopic)`; a new overload `build(env, bootstrapServers, conn, dns, modbus, s7comm, sensor, Duration modbusStateTtl, Duration s7commStateTtl, ModbusScoring scoring)` (`scoring` null = off), to which the existing two-TTL overload delegates with `null`.

- [ ] **Step 1: Write the failing tests**

`ModbusDetectorScorerFactoryTest.java`:

```java
package io.netsecml.platform.bootstrap.online;

import io.netsecml.platform.domain.inference.DetectorScores;
import io.netsecml.platform.port.out.SequenceScorer;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

// The composition root's factory: the real bundle, loaded and opened, scores
// the all-zero window as Python does (OnnxSequenceScorerTest's numbers).
class ModbusDetectorScorerFactoryTest {

    private static final String FIXTURE = Path.of("..", "..", "tests", "fixtures", "models",
        "modbus-stage1-detector", "v1").toString();

    @Test
    void itLoadsTheBundleAndScores() {
        try (SequenceScorer scorer = new ModbusDetectorScorerFactory(FIXTURE).create()) {
            DetectorScores s = scorer.score(new float[20][42]);
            assertEquals(0.3135979175567627, s.dense(), 1e-5);
            assertEquals("modbus-stage1-detector/v1", scorer.bundle().bundleId());
        }
    }

    @Test
    void aMissingBundleFailsLoudly() {
        assertThrows(IllegalStateException.class, () -> new ModbusDetectorScorerFactory("/no/such/bundle").create());
    }
}
```

In `OnlineFeatureJobTopologyTest`, add:

```java
    // Scoring adds exactly two uids, modbus-score and modbus-prediction-sink,
    // and a null ModbusScoring adds none (spec S10).
    @Test
    void scoringAddsItsTwoOperatorsAndOnlyWhenEnabled() {
        StreamExecutionEnvironment on = StreamExecutionEnvironment.getExecutionEnvironment();
        on.setParallelism(1);
        OnlineFeatureJob.build(on, "localhost:9092",
            new OnlineFeatureJob.ProtocolTopics("conn", "netsec.conn.feature-vector.v1", "netsec.conn.dlq.v1"),
            new OnlineFeatureJob.ProtocolTopics("dns", "netsec.dns.feature-vector.v1", "netsec.dns.dlq.v1"),
            new OnlineFeatureJob.ProtocolTopics("netsec.modbus.raw.v1", "netsec.modbus.feature-vector.v1",
                "netsec.modbus.dlq.v1"),
            new OnlineFeatureJob.ProtocolTopics("netsec.s7comm.raw.v1", "netsec.s7comm.feature-vector.v1",
                "netsec.s7comm.dlq.v1"),
            new SensorId("sensor-eu-1"), Duration.ofMinutes(60), Duration.ofMinutes(60),
            new OnlineFeatureJob.ModbusScoring("/opt/netsec/models/modbus-stage1-detector/v1",
                "netsec.modbus.prediction.v1"));
        Set<String> withScoring = uidsOf(on);
        Set<String> without = uidsOf(buildFourProtocol());
        assertEquals(without.size() + 2, withScoring.size(), "found: " + withScoring);
        assertTrue(withScoring.containsAll(Set.of("modbus-score", "modbus-prediction-sink")));
        assertTrue(withScoring.containsAll(without), "no existing uid changes");
    }
```

- [ ] **Step 2: Run them to verify they fail**

Run: `./mvnw test -pl modules/bootstrap-online-job -am -Dtest='ModbusDetectorScorerFactoryTest,OnlineFeatureJobTopologyTest' -Dsurefire.failIfNoSpecifiedTests=false`
Expected: COMPILATION ERROR — the factory, `ModbusScoring` and the overload do not exist.

- [ ] **Step 3: Implement the factory**

```java
package io.netsecml.platform.bootstrap.online;

import io.netsecml.platform.adapter.onnx.runtime.OnnxSequenceScorer;
import io.netsecml.platform.adapter.registry.LoadedSequenceDetector;
import io.netsecml.platform.adapter.registry.SequenceDetectorBundleLoader;
import io.netsecml.platform.port.out.SequenceScorer;
import io.netsecml.platform.port.out.SequenceScorerFactory;

import java.io.IOException;
import java.nio.file.Path;

// The composition root's SequenceScorerFactory: loads and verifies the bundle
// (adapter-registry-filesystem), then opens its graph (adapter-onnx) -- two
// adapters, so it lives here. Serializable as a path string; runs in each
// subtask's open() on the TaskManager, where models/ is mounted.
public final class ModbusDetectorScorerFactory implements SequenceScorerFactory {

    private final String bundleDir;

    public ModbusDetectorScorerFactory(String bundleDir) {
        this.bundleDir = bundleDir;
    }

    @Override
    public SequenceScorer create() {
        try {
            LoadedSequenceDetector loaded = SequenceDetectorBundleLoader.load(Path.of(bundleDir));
            return new OnnxSequenceScorer(loaded.model(), loaded.bundle());
        } catch (IOException e) {
            throw new IllegalStateException("cannot read the Modbus detector bundle at " + bundleDir, e);
        }
    }
}
```

- [ ] **Step 4: Wire the chain and the overloads in `OnlineFeatureJob`**

1. Add the record next to `ProtocolTopics`:

```java
    // Modbus scoring's settings (spec section 7): the bundle directory the
    // TaskManagers load and the prediction topic. null means scoring is off.
    public record ModbusScoring(String bundleDir, String predictionTopic) {
    }
```

2. The existing two-TTL overload's body becomes a delegation:

```java
        build(env, bootstrapServers, conn, dns, modbus, s7comm, sensor, modbusStateTtl, s7commStateTtl, null);
```

and add the new overload after it:

```java
    // As above, with Modbus scoring; main() passes the pinned bundle, or null
    // when MODBUS_DETECTOR_BUNDLE is empty (spec S10).
    public static void build(StreamExecutionEnvironment env, String bootstrapServers, ProtocolTopics conn,
                              ProtocolTopics dns, ProtocolTopics modbus, ProtocolTopics s7comm, SensorId sensor,
                              Duration modbusStateTtl, Duration s7commStateTtl, ModbusScoring scoring) {
        build(env, bootstrapServers, conn, dns, sensor);
        modbusChain(env, bootstrapServers, modbus, sensor, modbusStateTtl, scoring);
        s7commChain(env, bootstrapServers, s7comm, sensor, s7commStateTtl);
    }
```

3. `modbusChain` gains `ModbusScoring scoring` as its last parameter; the three-protocol overload passes `null`. Inside it, `modbusFeatureVectors` becomes a `SingleOutputStreamOperator<FeatureVector>`, and after the DLQ sink add:

```java
        // Scoring (spec section 4): the side output, keyed by the same stream
        // key, into modbus-score, then to the prediction topic. Off when null.
        if (scoring != null) {
            DataStream<ModbusDetectorPrediction> predictions = modbusFeatureVectors
                .getSideOutput(ModbusFeatureProcessFunction.SCORING_TAG)
                .keyBy(new KeyedModbusVectorKeySelector())
                .process(new ModbusScoringProcessFunction(new ModbusDetectorScorerFactory(scoring.bundleDir()),
                    stateTtl))
                .name("modbus-score")
                .uid("modbus-score");
            sinkModbusPredictions(predictions, bootstrapServers, scoring.predictionTopic(), "modbus-prediction-sink");
        }
```

4. Add the sink helper next to `sinkFeatureVectors`:

```java
    private static void sinkModbusPredictions(DataStream<ModbusDetectorPrediction> predictions,
                                              String bootstrapServers, String topic, String uid) {
        ModbusDetectorPredictionSerializer serializer = new ModbusDetectorPredictionSerializer();
        KafkaSink<ModbusDetectorPrediction> sink = KafkaSink.<ModbusDetectorPrediction>builder()
            .setBootstrapServers(bootstrapServers)
            .setRecordSerializer(KafkaRecordSerializationSchema.<ModbusDetectorPrediction>builder()
                .setTopic(topic)
                // An anonymous class, not a lambda, so Flink keeps the generic type.
                .setValueSerializationSchema(new SerializationSchema<ModbusDetectorPrediction>() {
                    @Override
                    public byte[] serialize(ModbusDetectorPrediction prediction) {
                        return serializer.serialize(topic, prediction);
                    }
                })
                .build())
            .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
            .build();
        predictions.sinkTo(sink).name(uid).uid(uid);
    }
```

5. In `main()`, before `build(...)`:

```java
        // Modbus scoring (spec section 7): the pinned bundle under the models
        // mount, verified here -- before submission -- so a missing or corrupt
        // bundle fails `up` with a clear message instead of a crash loop. An
        // empty MODBUS_DETECTOR_BUNDLE runs features only.
        String modelsDir = System.getenv().getOrDefault("NETSEC_MODELS_DIR", "/opt/netsec/models");
        String detectorBundle = System.getenv().getOrDefault("MODBUS_DETECTOR_BUNDLE", "");
        ModbusScoring scoring = null;
        if (!detectorBundle.isBlank()) {
            Path bundleDir = Path.of(modelsDir, detectorBundle);
            SequenceDetectorBundleLoader.load(bundleDir);
            scoring = new ModbusScoring(bundleDir.toString(),
                System.getenv().getOrDefault("MODBUS_PREDICTION_TOPIC", "netsec.modbus.prediction.v1"));
        }
```

and pass `scoring` as the last argument of `main()`'s `build(...)` call. Add the imports this needs (`ModbusDetectorPrediction`, `ModbusDetectorPredictionSerializer`, `KeyedModbusVectorKeySelector`, `ModbusScoringProcessFunction`, `SequenceDetectorBundleLoader`, `SingleOutputStreamOperator`, `java.nio.file.Path`).

- [ ] **Step 5: Run the tests to verify they pass**

Run: `./mvnw test -pl modules/bootstrap-online-job -am -Dtest='ModbusDetectorScorerFactoryTest,OnlineFeatureJobTopologyTest,OnlineFeatureJobRestartStrategyTest,ZeekRecordCheckTest' -Dsurefire.failIfNoSpecifiedTests=false`
Expected: all pass (`ModbusDetectorScorerFactoryTest` 2, `OnlineFeatureJobTopologyTest` 9, `OnlineFeatureJobRestartStrategyTest` 2, `ZeekRecordCheckTest` 8). The container test `OnlineFeatureJobE2ETest` must still **compile** (it calls the unchanged overloads); do not run it here.

- [ ] **Step 6: Commit**

```bash
git add modules/bootstrap-online-job
git commit -m "feat(scoring): the online job scores Modbus when a detector bundle is pinned

<attribution lines>"
```

---

### Task 13: The archive job's ninth chain

**Files:**
- Create: `modules/bootstrap-archive-job/src/main/java/io/netsecml/platform/bootstrap/archive/ModbusDetectorPredictionRowMapFunction.java`
- Modify: `modules/bootstrap-archive-job/src/main/java/io/netsecml/platform/bootstrap/archive/ArchiveJob.java`
- Test: `modules/bootstrap-archive-job/src/test/java/io/netsecml/platform/bootstrap/archive/ArchiveJobTopologyTest.java`

**Interfaces:**
- Consumes: Task 9's deserializer; Task 10's row and mapper.
- Produces: `ArchiveJob.modbusPredictionChain(String topic) -> LogTypeChain<ModbusDetectorPredictionRow>` (uids `modbus-prediction-source`, `modbus-prediction-row`, `modbus-predictions-clickhouse-sink`, table `modbus_detector_predictions`); `ArchiveJob.connDnsModbusS7commAndModbusPredictionChains(9 topics)`; `main()` reads `MODBUS_PREDICTION_TOPIC`.

- [ ] **Step 1: Write the failing topology test**

Add to `ArchiveJobTopologyTest`:

```java
    // The nine chains main() wires: the eight of connDnsModbusAndS7commChains
    // plus Modbus predictions, whose three uids are new and distinct.
    @Test
    void nineChainsProduceTwentySevenDistinctUids() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        ArchiveJob.build(env, "localhost:9092", ArchiveJob.connDnsModbusS7commAndModbusPredictionChains(
            "netsec.conn.feature-vector.v1", "netsec.conn.dlq.v1",
            "netsec.dns.feature-vector.v1", "netsec.dns.dlq.v1",
            "netsec.modbus.feature-vector.v1", "netsec.modbus.dlq.v1",
            "netsec.s7comm.feature-vector.v1", "netsec.s7comm.dlq.v1",
            "netsec.modbus.prediction.v1"),
            ClickHouseConfig.of("localhost", 8123, "netsec_ml", "default", "test-password"));
        Set<String> uids = new HashSet<>();
        for (StreamNode node : env.getStreamGraph(false).getStreamNodes()) {
            assertTrue(uids.add(node.getTransformationUID()), "duplicate uid " + node.getTransformationUID());
        }
        assertEquals(27, uids.size());
        assertTrue(uids.containsAll(Set.of("modbus-prediction-source", "modbus-prediction-row",
            "modbus-predictions-clickhouse-sink")), "found: " + uids);
    }
```

- [ ] **Step 2: Run it to verify it fails**

Run: `./mvnw test -pl modules/bootstrap-archive-job -am -Dtest='ArchiveJobTopologyTest' -Dsurefire.failIfNoSpecifiedTests=false`
Expected: COMPILATION ERROR — `connDnsModbusS7commAndModbusPredictionChains` does not exist.

- [ ] **Step 3: Implement the map function, the chain and `main()`**

`ModbusDetectorPredictionRowMapFunction.java`:

```java
package io.netsecml.platform.bootstrap.archive;

import io.netsecml.platform.adapter.clickhouse.mapper.ModbusDetectorPredictionRowMapper;
import io.netsecml.platform.adapter.clickhouse.row.ModbusDetectorPredictionRow;
import io.netsecml.platform.adapter.kafka.sink.ModbusDetectorPredictionDeserializer;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;

// One prediction message from netsec.modbus.prediction.v1 as one
// modbus_detector_predictions row, as FeatureVectorRowMapFunction does for vectors.
public final class ModbusDetectorPredictionRowMapFunction
        extends RichMapFunction<byte[], ModbusDetectorPredictionRow> {

    private final String topic;
    private transient ModbusDetectorPredictionDeserializer deserializer;
    private transient ModbusDetectorPredictionRowMapper mapper;

    public ModbusDetectorPredictionRowMapFunction(String topic) {
        this.topic = topic;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        super.open(openContext);
        deserializer = new ModbusDetectorPredictionDeserializer();
        mapper = new ModbusDetectorPredictionRowMapper();
    }

    @Override
    public ModbusDetectorPredictionRow map(byte[] message) {
        return mapper.toRow(deserializer.deserialize(topic, message));
    }
}
```

In `ArchiveJob`, next to `featureVectorChain`:

```java
    // Modbus Stage 1 predictions (spec section 6), their own three uids.
    public static LogTypeChain<ModbusDetectorPredictionRow> modbusPredictionChain(String topic) {
        return new LogTypeChain<>(topic, new ModbusDetectorPredictionRowMapFunction(topic),
            "modbus_detector_predictions", "modbus-prediction-source", "modbus-prediction-row",
            "modbus-predictions-clickhouse-sink");
    }

    // The nine chains main() wires: connDnsModbusAndS7commChains' eight plus
    // Modbus predictions.
    public static List<LogTypeChain<?>> connDnsModbusS7commAndModbusPredictionChains(
            String connFeatureTopic, String connDlqTopic, String dnsFeatureTopic, String dnsDlqTopic,
            String modbusFeatureTopic, String modbusDlqTopic, String s7commFeatureTopic, String s7commDlqTopic,
            String modbusPredictionTopic) {
        List<LogTypeChain<?>> chains = new ArrayList<>(connDnsModbusAndS7commChains(connFeatureTopic, connDlqTopic,
            dnsFeatureTopic, dnsDlqTopic, modbusFeatureTopic, modbusDlqTopic, s7commFeatureTopic, s7commDlqTopic));
        chains.add(modbusPredictionChain(modbusPredictionTopic));
        return List.copyOf(chains);
    }
```

In `main()`, read the topic with the other topic variables:

```java
        String modbusPredictionTopic = System.getenv().getOrDefault("MODBUS_PREDICTION_TOPIC",
            "netsec.modbus.prediction.v1");
```

and replace the `connDnsModbusAndS7commChains(...)` call in `build(...)` with
`connDnsModbusS7commAndModbusPredictionChains(featureTopic, dlqTopic, dnsFeatureTopic, dnsDlqTopic, modbusFeatureTopic, modbusDlqTopic, s7commFeatureTopic, s7commDlqTopic, modbusPredictionTopic)`. Import `ModbusDetectorPredictionRow` and `java.util.ArrayList`.

- [ ] **Step 4: Run it to verify it passes**

Run: the Step 2 command. Expected: `Tests run: 11, Failures: 0`. `ArchiveJobE2ETest` must still compile; do not run it here.

- [ ] **Step 5: Commit**

```bash
git add modules/bootstrap-archive-job
git commit -m "feat(scoring): the archive job's ninth chain writes Modbus predictions to ClickHouse

<attribution lines>"
```

---

### Task 14: The upstream oracle

**Files:**
- Create: `tests/fixtures/modbus/generate_detector_oracle.py`
- Create (generated, committed): `tests/fixtures/modbus/detector_oracle_v1.jsonl`
- Create: `modules/bootstrap-online-job/src/test/java/io/netsecml/platform/bootstrap/online/ModbusDetectorOracleTest.java`

**Interfaces:**
- Consumes: the delivery's `07b_materialize_feature_engine_v1.py` (SHA `e2abcfa3…c225`), `FEATURE_CONTRACT_V1.json`, `PREPROCESSING_CONTRACT_V1.json`, `dual_head_model_fp32.onnx`; the Java parser, mapper, `ModbusBuildFeaturesUseCase`, `ScoreModbusSequenceUseCase`, `ModbusDetectorScorerFactory`.
- Produces: `detector_oracle_v1.jsonl`, one line per event: `{"raw": <Zeek v1.0.0 record>, "key": "<client>|<server>|<unit>", "vector": [42 floats], "preprocessed": [42 floats], "verdict": "WARMUP"|"NORMAL"|"ANOMALY", "dense": float|null, "temporal": float|null}`.

- [ ] **Step 1: Write the generator**

```python
#!/usr/bin/env python3
"""Generate the Modbus detector oracle by running the model team's own code.

Builds seeded synthetic Modbus traffic as icsnpp-modbus v1.0.0 writes it --
`values` strings, responses without address or quantity -- then does what
upstream's capture adapter did before training (spec section 2.1: values as
numeric arrays; a response's address, quantity and `matched` from its pending
request), runs upstream's own 07b engine for the 42 features, applies the
frozen preprocessing contract, and runs the delivered ONNX graph in Python
ONNX Runtime for both scores. Refuses to run unless each upstream file's
SHA-256 matches the one this fixture was designed against.

Usage (from the repository root, in a venv with pandas, pyarrow, numpy, onnxruntime):
    python3 tests/fixtures/modbus/generate_detector_oracle.py \
        --delivery models/modbus/stage1_anomaly_detector \
        --out tests/fixtures/modbus/detector_oracle_v1.jsonl
The output is deterministic.
"""
from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import random
from pathlib import Path

import numpy as np
import onnxruntime as ort
import pandas as pd

PINS = {
    "src/07b_materialize_feature_engine_v1.py": "e2abcfa3",
    "contracts/modbus_feature_contract_v1/FEATURE_CONTRACT_V1.json": "620c9d00",
    "contracts/modbus_preprocessing_contract_v1/PREPROCESSING_CONTRACT_V1.json": "9ecff68f",
    "models/dual_head_model_fp32.onnx": "b5f28fec",
}
L, T_DENSE, T_TEMPORAL = 20, 0.2483385056257248, 0.4121147692203522
FUNCS = {"READ_COILS": 1, "READ_HOLDING_REGISTERS": 3, "WRITE_SINGLE_REGISTER": 6}


def check_pins(delivery: Path) -> None:
    # Each upstream file must be the exact one this fixture was designed on.
    for rel, prefix in PINS.items():
        digest = hashlib.sha256((delivery / rel).read_bytes()).hexdigest()
        if not digest.startswith(prefix):
            raise SystemExit(f"{rel}: SHA-256 {digest[:8]} is not the pinned {prefix}")


def traffic(seed: int) -> list[dict]:
    # Three streams (units 1, 2, 3) of polling; unit 1 runs 30 events, then a
    # 20 s gap (a segment break), then 22 more; unit 2 runs 24; unit 3 runs 10.
    rng = random.Random(seed)
    records, ts, tid = [], 1_790_000_000.0, 0
    plan = [(1, 15), (2, 12), (3, 5), (1, 0), (1, 11)]
    for unit, pairs in plan:
        if pairs == 0:
            ts += 20.0
            continue
        for _ in range(pairs):
            tid = (tid + 1) % 65536
            func = rng.choice(list(FUNCS))
            address, quantity = rng.choice([0, 100, 200]), rng.choice([1, 2, 4])
            request = {"ts": round(ts, 6), "uid": f"CORACLE{unit}", "id_orig_h": "10.0.0.5", "id_orig_p": 50200,
                       "id_resp_h": "10.0.0.9", "id_resp_p": 502, "is_orig": True, "source_h": "10.0.0.5",
                       "source_p": 50200, "destination_h": "10.0.0.9", "destination_p": 502, "tid": tid,
                       "unit": unit, "func": func, "request_response": "REQUEST", "address": address,
                       "quantity": quantity}
            if func == "WRITE_SINGLE_REGISTER":
                request["values"] = str(rng.randrange(65536))
            ts += rng.uniform(0.002, 0.02)
            response = {"ts": round(ts, 6), "uid": f"CORACLE{unit}", "id_orig_h": "10.0.0.5", "id_orig_p": 50200,
                        "id_resp_h": "10.0.0.9", "id_resp_p": 502, "is_orig": False, "source_h": "10.0.0.9",
                        "source_p": 502, "destination_h": "10.0.0.5", "destination_p": 50200, "tid": tid,
                        "unit": unit, "func": func, "request_response": "RESPONSE"}
            if func == "READ_COILS":
                response["values"] = ",".join(rng.choice("TF") for _ in range(quantity))
            elif func == "READ_HOLDING_REGISTERS":
                response["values"] = ",".join(str(rng.randrange(65536)) for _ in range(quantity))
            else:
                response["values"] = request["values"]
            records += [request, response]
            ts += rng.uniform(0.1, 0.5)
    return records


def values_of(text: str | None) -> list[float]:
    # Spec F1: T/F -> 1/0, decimals -> numbers.
    if not text:
        return []
    return [1.0 if t == "T" else 0.0 if t == "F" else float(t) for t in text.split(",")]


def canonical(records: list[dict]) -> pd.DataFrame:
    # Upstream's adapter shape: arrays, and a response's address, quantity and
    # matched from its pending request (spec F2), per stream and segment.
    rows, pending, last_ts = [], {}, {}
    for i, r in enumerate(records):
        key = (r["unit"],)
        if key in last_ts and r["ts"] - last_ts[key] > 15.0:
            pending[key] = {}
        last_ts[key] = r["ts"]
        stream = pending.setdefault(key, {})
        is_req = r["request_response"] == "REQUEST"
        address, quantity, matched = r.get("address"), r.get("quantity"), False
        if is_req:
            stream[r["tid"]] = (address, quantity)
        elif r["tid"] in stream:
            req_address, req_quantity = stream.pop(r["tid"])
            address = address if address is not None else req_address
            quantity = quantity if quantity is not None else req_quantity
            matched = True
        rows.append({"capture_key": "oracle", "capture_event_index": i, "ts": r["ts"],
                     "src_ip": r["source_h"], "dst_ip": r["destination_h"],
                     "direction": "request" if is_req else "response", "transaction_id": r["tid"],
                     "unit_id": r["unit"], "function_code": FUNCS[r["func"]], "address": address,
                     "quantity": quantity, "matched": matched,
                     "request_values": json.dumps(values_of(r.get("values")) if is_req else []),
                     "response_values": json.dumps([] if is_req else values_of(r.get("values")))})
    return pd.DataFrame(rows)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--delivery", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    check_pins(args.delivery)

    # Upstream's own engine, imported from the delivery as it is.
    spec = importlib.util.spec_from_file_location("engine07b", args.delivery / "src/07b_materialize_feature_engine_v1.py")
    engine = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(engine)
    names = json.loads((args.delivery / "contracts/modbus_feature_contract_v1/FEATURE_CONTRACT_V1.json")
                       .read_text())["feature_order"]
    prep = json.loads((args.delivery / "contracts/modbus_preprocessing_contract_v1/PREPROCESSING_CONTRACT_V1.json")
                      .read_text())
    params = {p["feature"]: p for p in prep["parameters"]}
    session = ort.InferenceSession(str(args.delivery / "models/dual_head_model_fp32.onnx"),
                                   providers=["CPUExecutionProvider"])

    records = traffic(seed=20260926)
    output, _qa = engine.process_capture(canonical(records), names)
    matrix = output[names].to_numpy(dtype=np.float32)

    def preprocess(v: np.ndarray) -> np.ndarray:
        # The contract's transform_definitions, in double, carried as float32.
        out = np.zeros(len(names))
        for i, n in enumerate(names):
            p, x = params[n], float(v[i])
            mask = 1.0 if not p["mask_feature"] else float(v[names.index(p["mask_feature"])])
            pol = p["policy"]
            if pol.startswith("CONDITIONAL") and mask != 1.0:
                out[i] = 0.0
            elif pol.startswith("PASSTHROUGH"):
                out[i] = x
            elif pol in ("GLOBAL_STANDARD", "CONDITIONAL_STANDARD"):
                out[i] = (x - p["mean"]) / p["std"]
            elif pol in ("GLOBAL_LOG1P_ONLY", "CONDITIONAL_LOG1P_ONLY"):
                out[i] = np.log1p(x)
            else:
                out[i] = (np.log1p(x) - p["mean"]) / p["std"]
        return out.astype(np.float32)

    windows: dict[str, list[np.ndarray]] = {}
    with args.out.open("w") as f:
        for i, record in enumerate(records):
            key = f"{output['client_ip'].iloc[i]}|{output['server_ip'].iloc[i]}|{output['unit_id'].iloc[i]}"
            vector = matrix[i]
            pre = preprocess(vector)
            # A segment start (prev_event_available == 0) empties the window.
            window = windows.setdefault(key, [])
            if vector[names.index("prev_event_available")] == 0.0:
                window.clear()
            window.append(pre)
            del window[:-L]
            verdict, dense, temporal = "WARMUP", None, None
            if len(window) == L:
                x = np.asarray([window], dtype=np.float32)
                d, t = session.run(None, {"sequence_20x42": x})
                dense = float(np.mean(np.abs(x - d)))
                temporal = float(np.mean(np.abs(t[0, -1] - x[0, -1])))
                verdict = "ANOMALY" if dense > T_DENSE or temporal > T_TEMPORAL else "NORMAL"
            f.write(json.dumps({"raw": record, "key": key, "vector": [float(v) for v in vector],
                                "preprocessed": [float(v) for v in pre], "verdict": verdict,
                                "dense": dense, "temporal": temporal}, separators=(",", ":")) + "\n")


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Generate the fixture**

Run (in the scratchpad venv created for the offline scorer, or a fresh one):
```bash
python3 -m venv /tmp/oracle-venv && /tmp/oracle-venv/bin/pip install -q pandas pyarrow numpy onnxruntime
/tmp/oracle-venv/bin/python tests/fixtures/modbus/generate_detector_oracle.py \
  --delivery models/modbus/stage1_anomaly_detector --out tests/fixtures/modbus/detector_oracle_v1.jsonl
```
Expected: 86 lines; `grep -c '"verdict":"WARMUP"'` well above 0 and at least one line with a numeric `dense`. Run it twice and `cmp` the outputs: identical.

- [ ] **Step 3: Write the Java oracle test**

```java
package io.netsecml.platform.bootstrap.online;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netsecml.platform.adapter.kafka.mapper.ModbusEventMapper;
import io.netsecml.platform.adapter.kafka.parser.JsonZeekModbusParser;
import io.netsecml.platform.application.usecase.ModbusBuildFeaturesUseCase;
import io.netsecml.platform.application.usecase.ModbusScoringResult;
import io.netsecml.platform.application.usecase.ScoreModbusSequenceUseCase;
import io.netsecml.platform.domain.event.ModbusEvent;
import io.netsecml.platform.domain.event.SensorId;
import io.netsecml.platform.domain.feature.FeatureBuildResult;
import io.netsecml.platform.domain.feature.FeatureVector;
import io.netsecml.platform.domain.feature.ModbusEntityKey;
import io.netsecml.platform.domain.feature.ModbusEntityState;
import io.netsecml.platform.domain.inference.ModbusDetectorPrediction;
import io.netsecml.platform.domain.inference.ModbusScoreWindow;
import io.netsecml.platform.port.out.SequenceScorer;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

// The Modbus scoring proof (spec section 10): every event of the upstream-
// generated oracle, as Zeek v1.0.0 writes it, through this platform's real
// parser, mapper, feature engine, preprocessing and Java ONNX scorer. The
// vectors and preprocessed values equal upstream's exactly, and both scores
// to 1e-5.
class ModbusDetectorOracleTest {

    private static final Path ORACLE = Path.of("..", "..", "tests", "fixtures", "modbus", "detector_oracle_v1.jsonl");
    private static final String BUNDLE = Path.of("..", "..", "tests", "fixtures", "models",
        "modbus-stage1-detector", "v1").toString();

    @Test
    void everyEventMatchesUpstream() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        List<String> lines = Files.readAllLines(ORACLE);
        JsonZeekModbusParser parser = new JsonZeekModbusParser();
        ModbusEventMapper eventMapper = new ModbusEventMapper();
        SensorId sensor = new SensorId("sensor-oracle");
        ModbusBuildFeaturesUseCase features = new ModbusBuildFeaturesUseCase();
        Map<ModbusEntityKey, ModbusEntityState> states = new HashMap<>();
        Map<ModbusEntityKey, ModbusScoreWindow> windows = new HashMap<>();
        int scored = 0;
        try (SequenceScorer scorer = new ModbusDetectorScorerFactory(BUNDLE).create()) {
            ScoreModbusSequenceUseCase scoring = new ScoreModbusSequenceUseCase(scorer, Clock.systemUTC());
            for (int i = 0; i < lines.size(); i++) {
                JsonNode expected = mapper.readTree(lines.get(i));
                // The real parse -> map -> features path, per stream.
                ModbusEvent event = (ModbusEvent) eventMapper.map(parser.parse(
                    expected.get("raw").toString().getBytes(java.nio.charset.StandardCharsets.UTF_8)).value(),
                    sensor).value();
                ModbusEntityKey key = ModbusEntityKey.of(event);
                ModbusEntityState state = states.computeIfAbsent(key, k -> ModbusEntityState.empty());
                FeatureBuildResult<ModbusEntityState> built = features.build(event, state);
                FeatureVector vector = built.vector();
                for (int j = 0; j < 42; j++) {
                    assertEquals((float) expected.get("vector").get(j).asDouble(), vector.values()[j],
                        "event " + i + " feature " + j);
                }
                // The preprocessing, against upstream's own arithmetic.
                float[] preprocessed = scorer.bundle().preprocessing().apply(vector.values());
                for (int j = 0; j < 42; j++) {
                    assertEquals((float) expected.get("preprocessed").get(j).asDouble(), preprocessed[j], 1e-6f,
                        "event " + i + " preprocessed " + j);
                }
                // Then scoring, per stream.
                ModbusScoringResult result = scoring.score(key, vector, windows.get(key));
                windows.put(key, result.window());
                ModbusDetectorPrediction p = result.prediction();
                assertEquals(expected.get("verdict").asText(), p.verdict().name(), "event " + i);
                if (!expected.get("dense").isNull()) {
                    scored++;
                    assertEquals(expected.get("dense").asDouble(), p.denseScore(), 1e-5, "event " + i + " dense");
                    assertEquals(expected.get("temporal").asDouble(), p.temporalScore(), 1e-5,
                        "event " + i + " temporal");
                }
            }
        }
        assertEquals(lines.stream().filter(l -> !l.contains("\"dense\":null")).count(), scored);
    }
}
```

- [ ] **Step 4: Run it**

Run: `./mvnw test -pl modules/bootstrap-online-job -am -Dtest='ModbusDetectorOracleTest' -Dsurefire.failIfNoSpecifiedTests=false`
Expected: `Tests run: 1, Failures: 0`. If a vector feature differs, the fault is in Tasks 1-2 or the generator's adapter emulation, never in the reference engine: fix the side that departs from spec section 2.1.

- [ ] **Step 5: Plant a bug, see it caught, restore**

Change `ZeekModbusValues` to map `T` to `0.0`, rerun Step 4: expected FAIL on a READ_COILS response's value summary. Restore, rerun: PASS.

- [ ] **Step 6: Commit**

```bash
git add tests/fixtures/modbus modules/bootstrap-online-job/src/test/java/io/netsecml/platform/bootstrap/online/ModbusDetectorOracleTest.java
git commit -m "test(scoring): upstream-generated oracle proves Modbus scoring end to end

<attribution lines>"
```

---

### Task 15: Deploy: topic, pin, mounts and preflight

**Files:**
- Modify: `deploy/kafka/topics.conf`, `deploy/.env.template`, `.env.example`, `deploy/docker-compose.yml`, `deploy/lib/stack.sh`
- Test: `deploy/tests/test_compose.sh`, `deploy/tests/test_stack.sh`

**Interfaces:**
- Produces: topic line `MODBUS_PREDICTION_TOPIC 1 168`; env keys `MODBUS_PREDICTION_TOPIC`, `MODBUS_DETECTOR_BUNDLE`; compose mounts `../models:/opt/netsec/models:ro` on `flink-taskmanager` and `job-submitter`; `stack.sh` functions `topic_name VAR`, `detector_bundle_setting` and `check_detector_bundle` (called at the end of `stack_preflight`).

- [ ] **Step 1: Write the failing deploy tests**

Append to `deploy/tests/test_compose.sh` (before the `# P6:` block; it renders with `config` and queries with `q`):

```bash
# Modbus scoring (spec section 7): the pin and the models mount reach the jobs.
assert_eq modbus-stage1-detector/v1 "$(q '.services["job-submitter"].environment.MODBUS_DETECTOR_BUNDLE')" "the detector pin reaches the jobs"
assert_eq /opt/netsec/models "$(q '.services["job-submitter"].environment.NETSEC_MODELS_DIR')" "the models dir"
assert_eq netsec.modbus.prediction.v1 "$(q '.services["job-submitter"].environment.MODBUS_PREDICTION_TOPIC')" "the prediction topic"
for svc in job-submitter flink-taskmanager; do
  assert_eq "/opt/netsec/models:true" "$(q ".services[\"$svc\"].volumes[] | select(.target == \"/opt/netsec/models\") | \"\(.target):\(.read_only)\"")" "$svc mounts models/ read-only"
done
# Review Focus 1: unset (an older .env) scores with the default; set empty turns scoring off.
sed -i '/^MODBUS_DETECTOR_BUNDLE=/d; /^MODBUS_PREDICTION_TOPIC=/d' "$tmp/env"
assert_eq modbus-stage1-detector/v1 "$(config | jq -r '.services["job-submitter"].environment.MODBUS_DETECTOR_BUNDLE')" "an older .env scores with the default bundle"
assert_eq netsec.modbus.prediction.v1 "$(config | jq -r '.services["job-submitter"].environment.MODBUS_PREDICTION_TOPIC')" "and gets the default topic"
printf 'MODBUS_DETECTOR_BUNDLE=\n' >> "$tmp/env"
assert_eq "" "$(config | jq -r '.services["job-submitter"].environment.MODBUS_DETECTOR_BUNDLE')" "an empty pin turns scoring off"
make_env 127.0.0.1
```

Append to `deploy/tests/test_stack.sh` (before `finish`):

```bash
# Review Focus 5: an older .env without MODBUS_PREDICTION_TOPIC still gets it
# from the template, so create_topics creates every topic instead of aborting.
set -a; . "${DEPLOY_DIR}/.env.template"; set +a
unset MODBUS_PREDICTION_TOPIC
: > "$tmp/calls"
compose() {
  if [ "$1" = exec ]; then
    cat > /dev/null
    printf '%s\n' "$*" >> "$tmp/calls"
  fi
}
create_topics >/dev/null
assert_eq 13 "$(grep -c -- '--create' "$tmp/calls")" "create_topics creates all 13 topics"
assert_eq 1 "$(grep -c -- '--topic netsec.modbus.prediction.v1 ' "$tmp/calls")" "including the prediction topic from the template"
unset -f compose

# The preflight refuses a pinned bundle that is missing, and accepts an empty pin.
REPO_ROOT_SAVED="$REPO_ROOT"; REPO_ROOT="$tmp/repo"; mkdir -p "$REPO_ROOT/models"
ENV_FILE_SAVED="$ENV_FILE"; ENV_FILE="$tmp/stack.env"; printf 'MODBUS_DETECTOR_BUNDLE=modbus-stage1-detector/v1\n' > "$ENV_FILE"
out="$(check_detector_bundle 2>&1; echo "exit=$?")"
assert_eq 1 "$(grep -c 'models/modbus-stage1-detector/v1 is missing' <<< "$out")" "a missing pinned bundle is refused"
assert_eq 1 "$(grep -c 'exit=1' <<< "$out")" "and stops"
printf 'MODBUS_DETECTOR_BUNDLE=\n' > "$ENV_FILE"
out="$(check_detector_bundle 2>&1; echo "exit=$?")"
assert_eq 1 "$(grep -c 'exit=0' <<< "$out")" "an empty pin needs no bundle"
mkdir -p "$REPO_ROOT/models/modbus-stage1-detector/v1"; touch "$REPO_ROOT/models/modbus-stage1-detector/v1/bundle.json"
printf 'MODBUS_DETECTOR_BUNDLE=modbus-stage1-detector/v1\n' > "$ENV_FILE"
out="$(check_detector_bundle 2>&1; echo "exit=$?")"
assert_eq 1 "$(grep -c 'exit=0' <<< "$out")" "a present bundle passes"
REPO_ROOT="$REPO_ROOT_SAVED"; ENV_FILE="$ENV_FILE_SAVED"
```

and change the existing `create_topics creates every topic` assertion's expected count from `12` to `13`.

- [ ] **Step 2: Run them to verify they fail**

Run: `bash deploy/tests/test_compose.sh; bash deploy/tests/test_stack.sh`
Expected: FAIL lines for every new check (no pin, no mount, 12 topics, `check_detector_bundle` not found).

- [ ] **Step 3: Implement the config**

`deploy/kafka/topics.conf`, after `S7COMM_DLQ_TOPIC`:

```
MODBUS_PREDICTION_TOPIC       1  168
```

`deploy/.env.template`: in the topics section, after `S7COMM_DLQ_TOPIC=…`:

```
MODBUS_PREDICTION_TOPIC=netsec.modbus.prediction.v1
```

and at the end of the job settings:

```
# The Modbus Stage 1 detector bundle, under the repository's models/ folder
# (spec: docs/superpowers/specs/2026-09-26-modbus-stage1-scoring-design.md).
# Package it with deploy/models/package-modbus-detector.sh <delivery-dir> and
# copy it there. Set it empty to run Modbus features without scoring.
MODBUS_DETECTOR_BUNDLE=modbus-stage1-detector/v1
```

`.env.example`: the same two keys with the same comments.

`deploy/docker-compose.yml`, `job-submitter.environment`, after `MODBUS_STATE_TTL_MINUTES`:

```yaml
      # '-' (not ':-'): unset -- an older deploy/.env -- scores with the default
      # bundle, while an explicit empty value turns scoring off.
      MODBUS_DETECTOR_BUNDLE: ${MODBUS_DETECTOR_BUNDLE-modbus-stage1-detector/v1}
      MODBUS_PREDICTION_TOPIC: ${MODBUS_PREDICTION_TOPIC:-netsec.modbus.prediction.v1}
      NETSEC_MODELS_DIR: /opt/netsec/models
```

and in both `job-submitter.volumes` and `flink-taskmanager.volumes`:

```yaml
      - ../models:/opt/netsec/models:ro
```

`deploy/lib/stack.sh`: replace `create_topics`' name lookup and add the bundle check:

```bash
# A topic's name: deploy/.env's value, else the template's -- a .env written
# before a topic existed must not abort 'up' (Review Focus 5).
topic_name() {
  local value="${!1:-}"
  [ -n "$value" ] || value="$(env_value "${DEPLOY_DIR}/.env.template" "$1")"
  [ -n "$value" ] || die "topics.conf names $1, which neither deploy/.env nor .env.template sets"
  printf '%s\n' "$value"
}
```

in `create_topics`, replace `topic="${!var:?topics.conf names ${var}, which deploy/.env does not set}"` with `topic="$(topic_name "$var")"`, and add:

```bash
# The pinned detector bundle: deploy/.env's MODBUS_DETECTOR_BUNDLE when it has
# the line (even empty), else the template's default -- what compose resolves.
detector_bundle_setting() {
  if grep -q '^MODBUS_DETECTOR_BUNDLE=' "$ENV_FILE" 2>/dev/null; then
    env_value "$ENV_FILE" MODBUS_DETECTOR_BUNDLE
  else
    env_value "${DEPLOY_DIR}/.env.template" MODBUS_DETECTOR_BUNDLE
  fi
}

# A pinned bundle must be on disk before the jobs start (spec section 7).
check_detector_bundle() {
  local bundle
  bundle="$(detector_bundle_setting)"
  [ -z "$bundle" ] && return 0
  [ -f "${REPO_ROOT}/models/${bundle}/bundle.json" ] \
    || die "the Modbus detector bundle models/${bundle} is missing: package it with deploy/models/package-modbus-detector.sh <delivery-dir> and copy it to models/${bundle}/, or set MODBUS_DETECTOR_BUNDLE= (empty) in deploy/.env to run without scoring"
}
```

and call `check_detector_bundle` at the end of `stack_preflight`.

- [ ] **Step 4: Run the deploy checks**

Run: `bash deploy/tests/run-all.sh`
Expected: `run-all: every check passed` (compose and stack with their new checks; shellcheck clean).

- [ ] **Step 5: Commit**

```bash
git add deploy .env.example
git commit -m "feat(deploy): prediction topic, detector pin, models mount and bundle preflight

<attribution lines>"
```

---

### Task 16: Documentation and full verification

**Files:**
- Modify: `CLAUDE.md` (data flow, key invariants, implementation state, verification state, Modbus limits), `deploy/README.md` (a "Scoring" section), `docs/superpowers/specs/2026-09-24-server-deployment-design.md` (a dated note: scoring added, see the scoring spec)

- [ ] **Step 1: Update CLAUDE.md**

- *Architecture → data flow*: after the feature-vector topics, add Modbus predictions: `modbus-features` → side output → `modbus-score` → `netsec.modbus.prediction.v1` → archive (ninth chain) → `modbus_detector_predictions`; ONNX inference is wired for Modbus Stage 1 only.
- *Key invariants*: the scorer's window is `modbus-score-window` (TTL as the feature state); the Modbus feature state is `modbus-entity-state-v2`.
- *Implementation state*: a paragraph for the scoring unit on `feat/modbus-scoring` (spec path, the fixture bundle, the oracle), and replace "Not yet implemented: ONNX inference in either job" with the accurate statement (Modbus Stage 1 scored; conn, dns, S7 and both Stage 2s not).
- *Modbus limits*: F1 and F2 (spec section 2.1) as ruled items; the one-time state rename; that the first 19 events of every stream segment are `WARMUP`; that F1/F2 await the model team's confirmation.
- *Verification state*: the new counts from Step 2.

- [ ] **Step 2: Run the full verification**

Run, and record every `Tests run:` line:
```bash
EXC=$(grep -rl --include=*.java "org.testcontainers" modules/*/src/test | xargs -n1 basename | sed 's/\.java$//' | sort -u | sed 's/^/!/' | paste -sd, -)
find modules -path "*/target/surefire-reports" -type d -exec rm -rf {} +
./mvnw test -fae -Dtest="$EXC" -Dsurefire.failIfNoSpecifiedTests=false
bash deploy/tests/run-all.sh
```
Expected: BUILD SUCCESS, every module green, and the container test classes compiled (not run). Put the per-module counts into CLAUDE.md's verification table.

- [ ] **Step 3: Commit**

```bash
git add CLAUDE.md deploy/README.md docs/superpowers/specs/2026-09-24-server-deployment-design.md
git commit -m "docs(scoring): CLAUDE.md, operator guide and verification for Modbus scoring

<attribution lines>"
```

---

### Task 17: Roll out to server3 and verify live

No code. Each step's output is read before the next; any failure stops the rollout.

- [ ] **Step 1: Package the bundle on the workstation**

Run: `bash deploy/models/package-modbus-detector.sh models/modbus/stage1_anomaly_detector models`
Expected: `packaged models/modbus-stage1-detector/v1`; its `bundle.json` byte-identical to the fixture's: `cmp models/modbus-stage1-detector/v1/bundle.json tests/fixtures/models/modbus-stage1-detector/v1/bundle.json`.

- [ ] **Step 2: Copy it to the server**

Run: `scp -r models/modbus-stage1-detector server3:/root/mvp-project/backup-project/models/` then on the server `sha256sum models/modbus-stage1-detector/v1/model.onnx` — expected `b5f28fec…bf4f`.

- [ ] **Step 3: Push, pull, build, restart**

Run: `git push -u origin feat/modbus-scoring`; on the server `git fetch origin && git switch feat/modbus-scoring && git pull --ff-only`, then `./deploy/deploy.sh build --jars-only`, then `deploy.sh restart` detached (`nohup … < /dev/null &`) with its log polled until `RESTART_EXIT=0`.
Expected: both jobs RUNNING; `logs job-submitter` shows `resuming from …/savepoints/…` for both; `sql "SHOW TABLES"` includes `modbus_detector_predictions`; `kafka-topics --list` includes `netsec.modbus.prediction.v1`.

- [ ] **Step 4: Selftest**

Run: `./deploy/deploy.sh selftest` — expected `selftest PASSED`.

- [ ] **Step 5: A 25-event stream gives 19 WARMUP and 6 scored rows**

Publish 13 request/response pairs (26 events; use 25 by dropping the last response) on one stream (`RESIL-SCORE-…` uids, the helper `mb_req`/`mb_resp` pattern) spaced 0.1 s apart, then query:
`SELECT verdict, count() FROM modbus_detector_predictions WHERE connection_uid LIKE 'RESIL-SCORE-%' GROUP BY verdict`
Expected: `WARMUP 19` and `NORMAL`+`ANOMALY` summing to `6`. Fetch the 25 feature vectors from `netsec.modbus.feature-vector.v1` and score them with the offline Python scorer (the scratchpad `score_modbus.py`): the six scores equal ClickHouse's to 1e-5.

- [ ] **Step 6: Input fidelity on real Zeek output**

Publish the 48 records of `tests/fixtures/zeek/icsnpp-modbus-v1.0.0_modbus_detailed.jsonl` to `netsec.modbus.raw.v1` (fresh uids: prefix each `uid` with `RESIL-FID-`), then from the resulting vectors confirm: every response whose record carried numeric `values` has `response_values_present` 1, and every response whose request carried an address has `address_present` 1 (spec section 2.1).

- [ ] **Step 7: Clean up and report**

Delete the test rows (`connection_uid LIKE 'RESIL-%'` in `feature_vectors` and `modbus_detector_predictions`; `event_id LIKE '%RESIL-%'` in `invalid_events`), confirm zero remain, and report the numbers from Steps 3-6.
