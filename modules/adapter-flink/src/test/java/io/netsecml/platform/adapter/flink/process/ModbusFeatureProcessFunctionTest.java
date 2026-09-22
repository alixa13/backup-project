package io.netsecml.platform.adapter.flink.process;

import io.netsecml.platform.domain.event.EventEnvelope;
import io.netsecml.platform.domain.event.EventId;
import io.netsecml.platform.domain.event.LogType;
import io.netsecml.platform.domain.event.ModbusEvent;
import io.netsecml.platform.domain.event.ModbusEvent.ModbusDirection;
import io.netsecml.platform.domain.event.SensorId;
import io.netsecml.platform.domain.feature.FeatureVector;
import io.netsecml.platform.domain.feature.ModbusEntityKey;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

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
        return new ModbusEvent(envelope(ts, tid), ModbusDirection.REQUEST, "10.0.0.5", "10.0.0.9",
            functionCode, tid, "1", null, null, false, new double[0], new double[0]);
    }

    // Same fixed client/server pair and function code (3), varying only ts and
    // unit -- so two calls at different units key to two different
    // ModbusEntityKeys and must not share a window.
    private ModbusEvent requestForUnit(double ts, String unitId) {
        return new ModbusEvent(envelope(ts, unitId), ModbusDirection.REQUEST, "10.0.0.5", "10.0.0.9",
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
}
