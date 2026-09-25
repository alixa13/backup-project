package io.netsecml.platform.adapter.flink.process;

import io.netsecml.platform.application.usecase.ModbusBuildFeaturesUseCase;
import io.netsecml.platform.domain.event.EventEnvelope;
import io.netsecml.platform.domain.event.EventId;
import io.netsecml.platform.domain.event.LogType;
import io.netsecml.platform.domain.event.ModbusEvent.ModbusDirection;
import io.netsecml.platform.domain.event.ModbusEvent;
import io.netsecml.platform.domain.event.SensorId;
import io.netsecml.platform.domain.feature.FeatureBuildResult;
import io.netsecml.platform.domain.feature.FeatureVector;
import io.netsecml.platform.domain.feature.ModbusEntityKey;
import io.netsecml.platform.domain.feature.ModbusEntityState;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

// Windows modbus events into modbus-feature-v1 vectors, mirroring
// DnsFeatureProcessFunctionTest's harness shape: a keyed operator built
// through the DIRECT KeyedOneInputStreamOperatorTestHarness constructor (not
// ProcessFunctionTestHarnesses.forKeyedProcessFunction, which opens the
// harness internally and so cannot be used for the restore-before-open case
// below), keyed by the log type's own selector rather than SourceKey.
// Unlike Dns/ConnFeatureProcessFunction, ModbusFeatureProcessFunction is keyed
// and typed directly on ModbusEvent -- the modbus chain is its own pipeline
// end to end (its own topics, own key, own DLQ; see
// DnsFeatureProcessFunction's own ModbusEvent arm), so there is no
// NetworkEvent narrowing switch to test here the way there is for dns.
class ModbusFeatureProcessFunctionTest {

    private static final SensorId SENSOR = new SensorId("sensor-eu-1");

    // ts -> Instant via nanos, matching ModbusBuildFeaturesUseCaseTest's own
    // helper, so a fractional ts (e.g. 1000.5) keeps sub-millisecond precision
    // rather than being rounded to whole milliseconds.
    private static Instant instantOf(double ts) {
        long seconds = (long) Math.floor(ts);
        long nanos = Math.round((ts - seconds) * 1_000_000_000.0);
        return Instant.ofEpochSecond(seconds, nanos);
    }

    private static EventEnvelope envelope(double ts, String uidSuffix) {
        String uid = "u-" + ts + "-" + uidSuffix;
        return new EventEnvelope(EventId.derive(SENSOR, uid), instantOf(ts), SENSOR, LogType.MODBUS, uid);
    }

    // A request for the fixed (client 10.0.0.5, server 10.0.0.9, unit "1")
    // entity key, mirroring ModbusBuildFeaturesUseCaseTest's own request()
    // helper -- only ts, functionCode and tid vary, per that test's reduction
    // for the same reason.
    private ModbusEvent request(double ts, int functionCode, String tid) {
        // tsSeconds is the same `ts` double, not re-derived from instantOf(ts)'s
        // Instant -- see ModbusBuildFeaturesUseCaseTest's own request() helper.
        return new ModbusEvent(envelope(ts, tid), ts, ModbusDirection.REQUEST, "10.0.0.5", "10.0.0.9",
            functionCode, tid, "1", null, null, false, new double[0], new double[0]);
    }

    // A request carrying an address, for the tests that need the 10 s
    // window's address and function counts to be non-trivial.
    private ModbusEvent requestAt(double ts, int functionCode, String tid, double address) {
        return new ModbusEvent(envelope(ts, tid), ts, ModbusDirection.REQUEST, "10.0.0.5", "10.0.0.9",
            functionCode, tid, "1", address, null, false, new double[0], new double[0]);
    }

    // Same fixed client/server pair and function code (3), varying only ts and
    // unit -- so two calls at different units key to two different
    // ModbusEntityKeys and must not share a window.
    private ModbusEvent requestForUnit(double ts, String unitId) {
        return new ModbusEvent(envelope(ts, unitId), ts, ModbusDirection.REQUEST, "10.0.0.5", "10.0.0.9",
            3, "tid-" + unitId, unitId, null, null, false, new double[0], new double[0]);
    }

    // Direct constructor, per DnsFeatureProcessFunctionTest's own harness B:
    // a restore must happen BEFORE open(), which
    // ProcessFunctionTestHarnesses.forKeyedProcessFunction cannot support
    // because it calls open() itself. Never opened here -- each call site
    // opens (or initializeState()s-then-opens) explicitly.
    private OneInputStreamOperatorTestHarness<ModbusEvent, FeatureVector> harness() throws Exception {
        return new KeyedOneInputStreamOperatorTestHarness<>(
            new KeyedProcessOperator<>(new ModbusFeatureProcessFunction()),
            new ModbusEntityKeySelector(), TypeInformation.of(ModbusEntityKey.class));
    }

    // As harness(), with the idle TTL chosen by the test.
    private OneInputStreamOperatorTestHarness<ModbusEvent, FeatureVector> harness(Duration ttl) throws Exception {
        return new KeyedOneInputStreamOperatorTestHarness<>(
            new KeyedProcessOperator<>(new ModbusFeatureProcessFunction(ttl)),
            new ModbusEntityKeySelector(), TypeInformation.of(ModbusEntityKey.class));
    }

    // The response to request(ts, functionCode, tid): per-packet endpoints, so
    // server to client; ModbusEntityKey.of swaps it back into the request's key.
    private ModbusEvent response(double ts, int functionCode, String tid) {
        return new ModbusEvent(envelope(ts, tid + "-r"), ts, ModbusDirection.RESPONSE, "10.0.0.9", "10.0.0.5",
            functionCode, tid, "1", null, null, false, new double[0], new double[0]);
    }

    // The operator as deployed before its state had a TTL (up to 2026-09-25):
    // the same "modbus-entity-state" name and type, with no TTL. Only here to
    // write the kind of savepoint the server already holds.
    private static final class PreTtlModbusFeatureProcessFunction
            extends KeyedProcessFunction<ModbusEntityKey, ModbusEvent, FeatureVector> {
        private transient ValueState<ModbusEntityState> entityState;
        private transient ModbusBuildFeaturesUseCase useCase;

        @Override
        public void open(OpenContext openContext) {
            entityState = getRuntimeContext().getState(new ValueStateDescriptor<>(
                "modbus-entity-state", TypeInformation.of(ModbusEntityState.class)));
            useCase = new ModbusBuildFeaturesUseCase();
        }

        @Override
        public void processElement(ModbusEvent event, Context ctx, Collector<FeatureVector> out) throws Exception {
            ModbusEntityState state = entityState.value();
            FeatureBuildResult<ModbusEntityState> result =
                useCase.build(event, state == null ? ModbusEntityState.empty() : state);
            entityState.update(result.newState());
            out.collect(result.vector());
        }
    }

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

    @Test
    void restoredRunningCountsGiveTheSameVectorsAsAnUninterruptedRun() throws Exception {
        // The entity state carries running 10 s counts (per function code, per
        // address, reads, writes) that are never recomputed from the window:
        // whatever a restore brings back is what every later vector reads. So
        // a run checkpointed and restored midway must emit, after the restore,
        // exactly what one uninterrupted run emits -- including once the
        // pre-checkpoint entries purge out and decrement those counts.
        ModbusEvent[] events = {
            requestAt(1000.0, 3, "1", 40001.0),
            requestAt(1002.0, 6, "2", 40002.0),
            requestAt(1004.0, 23, "3", 40001.0),
            requestAt(1006.0, 16, "4", 40003.0),
            requestAt(1011.0, 3, "5", 40001.0),
            requestAt(1013.5, 6, "6", 40004.0),
            requestAt(1016.0, 3, "7", 40002.0),
        };
        int checkpointAfter = 4;

        OneInputStreamOperatorTestHarness<ModbusEvent, FeatureVector> uninterrupted = harness();
        uninterrupted.open();
        for (ModbusEvent event : events) {
            uninterrupted.processElement(new StreamRecord<>(event));
        }
        List<FeatureVector> expected = uninterrupted.extractOutputValues();
        uninterrupted.close();

        OneInputStreamOperatorTestHarness<ModbusEvent, FeatureVector> first = harness();
        first.open();
        for (int i = 0; i < checkpointAfter; i++) {
            first.processElement(new StreamRecord<>(events[i]));
        }
        OperatorSubtaskState snapshot = first.snapshot(1L, 1L);
        first.close();

        OneInputStreamOperatorTestHarness<ModbusEvent, FeatureVector> second = harness();
        second.initializeState(snapshot);
        second.open();
        for (int i = checkpointAfter; i < events.length; i++) {
            second.processElement(new StreamRecord<>(events[i]));
        }
        List<FeatureVector> actual = second.extractOutputValues();
        second.close();

        assertEquals(events.length - checkpointAfter, actual.size());
        for (int i = 0; i < actual.size(); i++) {
            assertArrayEquals(expected.get(checkpointAfter + i).values(), actual.get(i).values(),
                "vector for event " + (checkpointAfter + i) + " after the restore");
        }
    }

    // Idle TTL (OnCreateAndWrite): every event writes the state, so the clock
    // restarts at each event and only a key with NO event for the whole TTL
    // expires. Event time moves 1 s per event, well inside the 15 s segment
    // gap, so any fresh start here is the TTL's doing, not the segment rule's.
    // outstanding_requests_before_event (index 30) counts this key's pending
    // requests: it grows while the state lives and is 0 once it has expired.
    @Test
    void idleStateExpiresAfterTheTtlButSurvivesJustBeforeIt() throws Exception {
        OneInputStreamOperatorTestHarness<ModbusEvent, FeatureVector> harness = harness(Duration.ofHours(1));
        harness.setStateTtlProcessingTime(0L);
        harness.open();
        harness.processElement(new StreamRecord<>(request(1000.0, 3, "1")));

        harness.setStateTtlProcessingTime(Duration.ofMinutes(59).toMillis());
        harness.processElement(new StreamRecord<>(request(1001.0, 3, "2")));

        harness.setStateTtlProcessingTime(Duration.ofMinutes(59 + 59).toMillis());
        harness.processElement(new StreamRecord<>(request(1002.0, 3, "3")));

        harness.setStateTtlProcessingTime(Duration.ofMinutes(59 + 59 + 61).toMillis());
        harness.processElement(new StreamRecord<>(request(1003.0, 3, "4")));

        List<FeatureVector> out = harness.extractOutputValues();
        assertEquals(1f, out.get(1).values()[30], "59 minutes idle: alive, one request pending");
        assertEquals(2f, out.get(2).values()[30], "59 minutes after the last write: alive, two pending");
        assertEquals(0f, out.get(3).values()[30], "61 minutes idle: expired, a fresh state");
        harness.close();
    }

    // The TTL counts PROCESSING time, and each key's last-write time is
    // restored with the checkpoint: after an outage longer than the TTL a key
    // restarts from empty state even though, in event time, its traffic has no
    // gap -- the request below is forgotten, so its response is unmatched
    // (response_without_request, index 31; rtt_valid, index 33). S7comm's
    // ruling R4 accepts the same.
    @Test
    void aRestoreAfterAnOutageLongerThanTheTtlStartsTheKeyFresh() throws Exception {
        OneInputStreamOperatorTestHarness<ModbusEvent, FeatureVector> before = harness(Duration.ofHours(1));
        before.setStateTtlProcessingTime(0L);
        before.open();
        before.processElement(new StreamRecord<>(request(1000.0, 3, "7")));
        OperatorSubtaskState snapshot = before.snapshot(1L, 1L);
        before.close();

        OneInputStreamOperatorTestHarness<ModbusEvent, FeatureVector> after = harness(Duration.ofHours(1));
        after.initializeState(snapshot);
        after.setStateTtlProcessingTime(Duration.ofMinutes(61).toMillis());
        after.open();
        after.processElement(new StreamRecord<>(response(1000.25, 3, "7")));

        float[] responseVector = after.extractOutputValues().get(0).values();
        assertEquals(1f, responseVector[31], "response_without_request: the request was forgotten");
        assertEquals(0f, responseVector[33], "rtt_valid");
        after.close();
    }

    // The deploy guarantee: a savepoint written before this state had a TTL
    // (what the server holds) restores into the TTL state under the same name,
    // so a request pending at the savepoint still matches its response after
    // it -- outstanding_requests_before_event (30) 1, response_without_request
    // (31) 0, rtt_valid (33) 1.
    @Test
    void aSavepointWrittenBeforeTheStateHadATtlRestoresIntoIt() throws Exception {
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
        assertEquals(1f, responseVector[30], "outstanding_requests_before_event");
        assertEquals(0f, responseVector[31], "response_without_request");
        assertEquals(1f, responseVector[33], "rtt_valid");
        after.close();
    }

    @Test
    void aTtlThatIsNotPositiveIsRejected() {
        assertThrows(IllegalArgumentException.class, () -> new ModbusFeatureProcessFunction(Duration.ZERO));
        assertThrows(IllegalArgumentException.class, () -> new ModbusFeatureProcessFunction(Duration.ofMinutes(-1)));
        assertThrows(NullPointerException.class, () -> new ModbusFeatureProcessFunction(null));
    }
}
