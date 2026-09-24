package io.netsecml.platform.adapter.flink.process;

import io.netsecml.platform.domain.event.EventEnvelope;
import io.netsecml.platform.domain.event.EventId;
import io.netsecml.platform.domain.event.LogType;
import io.netsecml.platform.domain.event.S7commEvent;
import io.netsecml.platform.domain.event.SensorId;
import io.netsecml.platform.domain.feature.FeatureVector;
import io.netsecml.platform.domain.feature.S7commConnectionKey;
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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

// The s7comm feature operator: state per (sensor, uid), a checkpoint/restore
// that changes nothing, and the idle TTL -- ModbusFeatureProcessFunctionTest's
// harness shape (a keyed harness built directly, so a restore can precede open()).
class S7commFeatureProcessFunctionTest {
    private static final SensorId SENSOR = new SensorId("sensor-eu-1");

    // A request on `uid` with PDU reference `pdu`, function READ_VAR.
    private static S7commEvent request(String uid, double ts, int pdu) {
        return new S7commEvent(
            new EventEnvelope(EventId.derive(SENSOR, uid + ":" + pdu + ":REQUEST:" + ts),
                Instant.ofEpochMilli(Math.round(ts * 1000)), SENSOR, LogType.S7COMM, uid),
            ts, "10.0.0.5", 50001, "10.0.0.9", 102, pdu, 1, 4, null);
    }

    // The matching response: the PLC answering from port 102.
    private static S7commEvent response(String uid, double ts, int pdu) {
        return new S7commEvent(
            new EventEnvelope(EventId.derive(SENSOR, uid + ":" + pdu + ":RESPONSE:" + ts),
                Instant.ofEpochMilli(Math.round(ts * 1000)), SENSOR, LogType.S7COMM, uid),
            ts, "10.0.0.9", 102, "10.0.0.5", 50001, pdu, 3, 4, null);
    }

    private static OneInputStreamOperatorTestHarness<S7commEvent, FeatureVector> harness(Duration ttl)
            throws Exception {
        return new KeyedOneInputStreamOperatorTestHarness<>(
            new KeyedProcessOperator<>(new S7commFeatureProcessFunction(ttl)),
            new S7commConnectionKeySelector(), TypeInformation.of(S7commConnectionKey.class));
    }

    private static float outstanding(FeatureVector vector) {
        return vector.values()[0];
    }

    @Test
    void oneRecordProducesOneSixteenValueVector() throws Exception {
        var harness = harness(S7commFeatureProcessFunction.DEFAULT_STATE_TTL);
        harness.open();
        harness.processElement(new StreamRecord<>(request("CA", 1000.0, 1)));
        List<FeatureVector> out = harness.extractOutputValues();
        assertEquals(1, out.size());
        assertEquals(16, out.get(0).values().length);
        harness.close();
    }

    @Test
    void stateIsKeptPerUid() throws Exception {
        var harness = harness(S7commFeatureProcessFunction.DEFAULT_STATE_TTL);
        harness.open();
        harness.processElement(new StreamRecord<>(request("CA", 1000.0, 1)));
        harness.processElement(new StreamRecord<>(request("CB", 1000.1, 2)));
        assertEquals(1f, outstanding(harness.extractOutputValues().get(1)), "CB's own first request only");
        harness.close();
    }

    @Test
    void aRestoredStateGivesTheSameVectorsAsAnUninterruptedRun() throws Exception {
        List<S7commEvent> events = List.of(request("CA", 1000.0, 1), request("CA", 1000.1, 2),
            response("CA", 1000.2, 1), request("CA", 1000.3, 3), response("CA", 1000.4, 3),
            request("CA", 1000.5, 4), response("CA", 1000.6, 2));
        int checkpointAfter = 3;

        var uninterrupted = harness(S7commFeatureProcessFunction.DEFAULT_STATE_TTL);
        uninterrupted.open();
        for (S7commEvent event : events) {
            uninterrupted.processElement(new StreamRecord<>(event));
        }
        List<FeatureVector> expected = uninterrupted.extractOutputValues();
        uninterrupted.close();

        var first = harness(S7commFeatureProcessFunction.DEFAULT_STATE_TTL);
        first.open();
        for (int i = 0; i < checkpointAfter; i++) {
            first.processElement(new StreamRecord<>(events.get(i)));
        }
        OperatorSubtaskState snapshot = first.snapshot(1L, 1L);
        first.close();

        var second = harness(S7commFeatureProcessFunction.DEFAULT_STATE_TTL);
        second.initializeState(snapshot);
        second.open();
        for (int i = checkpointAfter; i < events.size(); i++) {
            second.processElement(new StreamRecord<>(events.get(i)));
        }
        List<FeatureVector> actual = second.extractOutputValues();
        second.close();

        assertEquals(events.size() - checkpointAfter, actual.size());
        for (int i = 0; i < actual.size(); i++) {
            assertArrayEquals(expected.get(checkpointAfter + i).values(), actual.get(i).values(),
                "vector for event " + (checkpointAfter + i) + " after the restore");
        }
    }

    @Test
    void idleStateExpiresAfterTheTtlButSurvivesJustBeforeIt() throws Exception {
        // One hour, OnCreateAndWrite: every event writes the state, so the clock
        // restarts at each event and expiry needs an hour with NO event.
        var harness = harness(Duration.ofHours(1));
        harness.setStateTtlProcessingTime(0L);
        harness.open();
        harness.processElement(new StreamRecord<>(request("CA", 1000.0, 1)));

        harness.setStateTtlProcessingTime(Duration.ofMinutes(59).toMillis());
        harness.processElement(new StreamRecord<>(request("CA", 1001.0, 2)));

        harness.setStateTtlProcessingTime(Duration.ofMinutes(59 + 59).toMillis());
        harness.processElement(new StreamRecord<>(request("CA", 1002.0, 3)));

        harness.setStateTtlProcessingTime(Duration.ofMinutes(59 + 59 + 61).toMillis());
        harness.processElement(new StreamRecord<>(request("CA", 1003.0, 4)));

        List<FeatureVector> out = harness.extractOutputValues();
        assertEquals(2f, outstanding(out.get(1)), "59 minutes idle: still alive");
        assertEquals(3f, outstanding(out.get(2)), "59 minutes after the last write: still alive");
        assertEquals(1f, outstanding(out.get(3)), "61 minutes idle: expired, a fresh state");
        harness.close();
    }

    @Test
    void aRestoreAfterAnOutageLongerThanTheTtlStartsTheConnectionFresh() throws Exception {
        // The TTL counts PROCESSING time and each key's last-write time is
        // restored with the checkpoint, so if the job is down for longer than
        // the TTL, every connection is expired the moment it is next read --
        // even though, in event time, its traffic continues without a gap.
        var before = harness(Duration.ofHours(1));
        before.setStateTtlProcessingTime(0L);
        before.open();
        before.processElement(new StreamRecord<>(request("CA", 1000.0, 1)));
        OperatorSubtaskState snapshot = before.snapshot(1L, 1L);
        before.close();

        var after = harness(Duration.ofHours(1));
        after.initializeState(snapshot);
        after.setStateTtlProcessingTime(Duration.ofMinutes(61).toMillis());
        after.open();
        after.processElement(new StreamRecord<>(request("CA", 1000.1, 2)));
        assertEquals(1f, outstanding(after.extractOutputValues().get(0)),
            "a 61-minute outage expires the connection: it restarts from empty state, with no quality bit");
        after.close();
    }

    @Test
    void theDefaultTtlIsOneHourAndMustBePositive() {
        assertEquals(Duration.ofHours(1), S7commFeatureProcessFunction.DEFAULT_STATE_TTL);
        assertThrows(IllegalArgumentException.class, () -> new S7commFeatureProcessFunction(Duration.ZERO));
    }
}
