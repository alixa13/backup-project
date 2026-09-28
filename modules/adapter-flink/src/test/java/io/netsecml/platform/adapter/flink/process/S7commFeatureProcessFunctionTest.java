package io.netsecml.platform.adapter.flink.process;

import io.netsecml.platform.application.usecase.S7commBuildFeaturesUseCase;
import io.netsecml.platform.domain.event.EventEnvelope;
import io.netsecml.platform.domain.event.EventId;
import io.netsecml.platform.domain.event.LogType;
import io.netsecml.platform.domain.event.S7commEvent;
import io.netsecml.platform.domain.event.SensorId;
import io.netsecml.platform.domain.feature.FeatureBuildResult;
import io.netsecml.platform.domain.feature.FeatureVector;
import io.netsecml.platform.domain.feature.QualityFlags;
import io.netsecml.platform.domain.feature.S7commConnectionKey;
import io.netsecml.platform.domain.feature.S7commConnectionState;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.StateTtlConfig;
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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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

    // What the operator put on the scoring side output, in order.
    private static List<KeyedS7commVector> scoring(OneInputStreamOperatorTestHarness<S7commEvent, FeatureVector> harness) {
        return harness.getSideOutput(S7commFeatureProcessFunction.SCORING_TAG).stream()
            .map(StreamRecord::getValue).toList();
    }

    // Scoring design section 4: every vector also reaches s7comm-score, with its
    // connection, whether the connection started from empty state, and its
    // client and server whichever way the record went.
    @Test
    void everyVectorAlsoGoesToScoringWithItsConnection() throws Exception {
        var harness = harness(S7commFeatureProcessFunction.DEFAULT_STATE_TTL);
        harness.open();
        harness.processElement(new StreamRecord<>(request("CA", 1000.0, 1)));
        harness.processElement(new StreamRecord<>(response("CA", 1000.1, 1)));
        List<KeyedS7commVector> side = scoring(harness);
        assertEquals(2, side.size());
        assertTrue(side.get(0).freshState(), "the connection's first event");
        assertFalse(side.get(1).freshState());
        for (KeyedS7commVector k : side) {
            assertEquals("CA", k.key().uid());
            assertEquals("10.0.0.5", k.clientIp(), "the request's sender is the response's receiver");
            assertEquals("10.0.0.9", k.serverIp());
        }
        assertEquals(harness.extractOutputValues().stream().map(FeatureVector::eventId).toList(),
            side.stream().map(k -> k.vector().eventId()).toList());
        harness.close();
    }

    // Review Focus 2: an expired feature state is a fresh connection to the scorer too.
    @Test
    void aConnectionWhoseStateExpiredIsFreshAgain() throws Exception {
        var harness = harness(Duration.ofMinutes(60));
        harness.setStateTtlProcessingTime(0L);
        harness.open();
        harness.processElement(new StreamRecord<>(request("CA", 1000.0, 1)));
        harness.setStateTtlProcessingTime(Duration.ofMinutes(61).toMillis());
        harness.processElement(new StreamRecord<>(request("CA", 1001.0, 2)));
        assertTrue(scoring(harness).get(1).freshState(), "the TTL expired the state: the scorer must re-warm");
        harness.close();
    }

    private static OneInputStreamOperatorTestHarness<S7commEvent, FeatureVector> harness(Duration ttl,
            long restartEvents) throws Exception {
        return new KeyedOneInputStreamOperatorTestHarness<>(
            new KeyedProcessOperator<>(new S7commFeatureProcessFunction(ttl, restartEvents)),
            new S7commConnectionKeySelector(), TypeInformation.of(S7commConnectionKey.class));
    }

    private static boolean restarted(FeatureVector vector) {
        return (vector.qualityFlags() & QualityFlags.S7COMM_RESTARTED) != 0;
    }

    // Amendment A1, item 4: a connection that has run restartEvents events starts
    // again from empty state, flagged, and the scorer is told it is fresh.
    @Test
    void aConnectionRestartsAfterItsRestartCount() throws Exception {
        var harness = harness(S7commFeatureProcessFunction.DEFAULT_STATE_TTL, 3);
        harness.open();
        for (int i = 0; i < 5; i++) {
            harness.processElement(new StreamRecord<>(request("CA", 1000.0 + i, i)));
        }
        List<FeatureVector> out = harness.extractOutputValues();
        List<KeyedS7commVector> side = scoring(harness);
        for (int i : new int[]{0, 1, 2, 4}) {
            assertFalse(restarted(out.get(i)), "event " + i);
        }
        assertTrue(restarted(out.get(3)), "the fourth event follows the restart");
        assertTrue(side.get(3).freshState(), "the scorer must re-warm after a restart");
        assertFalse(side.get(4).freshState());
        // s7_same_function_run_length: 1, 2, 3, then 1 again after the restart.
        assertEquals(List.of(1f, 2f, 3f, 1f, 2f), out.stream().map(v -> v.values()[3]).toList());
        assertTrue(restarted(side.get(3).vector()), "the side output carries the same flagged vector");
        harness.close();
    }

    // The feature operator as it was before amendment A1: its connection state only,
    // written the same way. A snapshot of it is what the deployed job restores.
    private static final class WithoutCounter
            extends KeyedProcessFunction<S7commConnectionKey, S7commEvent, FeatureVector> {
        private transient ValueState<S7commConnectionState> state;
        private transient S7commBuildFeaturesUseCase useCase;

        @Override
        public void open(OpenContext openContext) {
            ValueStateDescriptor<S7commConnectionState> descriptor = new ValueStateDescriptor<>(
                "s7comm-connection-state", TypeInformation.of(S7commConnectionState.class));
            // The deployed operator's TTL, so the snapshot has the deployed layout.
            descriptor.enableTimeToLive(StateTtlConfig.newBuilder(S7commFeatureProcessFunction.DEFAULT_STATE_TTL)
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .cleanupFullSnapshot()
                .build());
            state = getRuntimeContext().getState(descriptor);
            useCase = new S7commBuildFeaturesUseCase();
        }

        @Override
        public void processElement(S7commEvent event, Context ctx, Collector<FeatureVector> out) throws Exception {
            S7commConnectionState current = state.value();
            FeatureBuildResult<S7commConnectionState> result =
                useCase.build(event, current == null ? S7commConnectionState.empty() : current);
            state.update(result.newState());
            out.collect(result.vector());
        }
    }

    // A key restored from a snapshot taken before the counter existed restarts at
    // its first event (final review): its run length is unbounded and may already
    // be past what the detector flags, and its new score window warms up anyway.
    @Test
    void aKeyRestoredWithoutACounterRestartsAtItsFirstEvent() throws Exception {
        var before = new KeyedOneInputStreamOperatorTestHarness<>(
            new KeyedProcessOperator<>(new WithoutCounter()),
            new S7commConnectionKeySelector(), TypeInformation.of(S7commConnectionKey.class));
        before.open();
        for (int i = 0; i < 5; i++) {
            before.processElement(new StreamRecord<>(request("CA", 1000.0 + i, i)));
        }
        OperatorSubtaskState snapshot = before.snapshot(1L, 1L);
        before.close();

        var after = harness(S7commFeatureProcessFunction.DEFAULT_STATE_TTL, 3);
        after.initializeState(snapshot);
        after.open();
        for (int i = 5; i < 8; i++) {
            after.processElement(new StreamRecord<>(request("CA", 1000.0 + i, i)));
        }
        List<FeatureVector> out = after.extractOutputValues();
        List<KeyedS7commVector> side = scoring(after);
        assertTrue(restarted(out.get(0)), "no counter: the restored key restarts at once");
        assertTrue(side.get(0).freshState(), "and the scorer re-warms");
        assertEquals(1f, out.get(0).values()[3], "the run starts again");
        assertFalse(restarted(out.get(1)), "then it counts: 1 of 3");
        assertFalse(restarted(out.get(2)), "2 of 3");
        after.close();
    }

    @Test
    void theRestartCountMustBePositive() {
        assertEquals(16_384L, S7commFeatureProcessFunction.RESTART_EVENTS);
        assertThrows(IllegalArgumentException.class,
            () -> new S7commFeatureProcessFunction(S7commFeatureProcessFunction.DEFAULT_STATE_TTL, 0));
    }
}
