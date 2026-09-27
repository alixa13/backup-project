package io.netsecml.platform.application.usecase;

import io.netsecml.platform.domain.event.EventEnvelope;
import io.netsecml.platform.domain.event.EventId;
import io.netsecml.platform.domain.event.LogType;
import io.netsecml.platform.domain.event.ModbusEvent;
import io.netsecml.platform.domain.event.ModbusEvent.ModbusDirection;
import io.netsecml.platform.domain.event.SensorId;
import io.netsecml.platform.domain.feature.DnsFeatureSchemaV1;
import io.netsecml.platform.domain.feature.FeatureBuildResult;
import io.netsecml.platform.domain.feature.ModbusEntityState;
import io.netsecml.platform.domain.feature.ModbusFeatureSchemaV1;
import io.netsecml.platform.domain.feature.QualityFlags;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

// Pins Ruling 5 -- decide the segment, reset-then-extract-then-advance, in
// that exact order -- against ModbusBuildFeaturesUseCase.build. Companion to
// ModbusFeatureExtractorTest, which pins the same per-index rules one level
// down, against the extractor alone with hand-built before/after state pairs.
// This class instead drives the use case end to end, so a bug in the
// ORDERING of decide-reset-extract-advance itself -- not just in a single
// index's formula -- has somewhere to show up.
class ModbusBuildFeaturesUseCaseTest {
    private static final SensorId SENSOR = new SensorId("sensor-eu-1");

    // ts -> Instant for envelope().eventTime() only (nanosecond-rounded, for
    // realism); the causal engine itself reads tsSeconds -- the same `ts`
    // double passed to each helper below, unrounded -- not this Instant.
    // Mirrors ModbusFeatureExtractorTest's own helper.
    private static Instant instantOf(double ts) {
        long seconds = (long) Math.floor(ts);
        long nanos = Math.round((ts - seconds) * 1_000_000_000.0);
        return Instant.ofEpochSecond(seconds, nanos);
    }

    private EventEnvelope envelope(double ts) {
        String uid = "u-" + ts;
        return new EventEnvelope(EventId.derive(SENSOR, uid), instantOf(ts), SENSOR, LogType.MODBUS, uid);
    }

    private ModbusEvent request(double ts, int functionCode, String tid) {
        // tsSeconds is the same `ts` double the caller passed, not re-derived
        // from the Instant instantOf(ts) built above -- the whole point of F1
        // is that the causal engine reads the wire value directly, not a
        // value round-tripped through Instant.
        return new ModbusEvent(envelope(ts), ts, ModbusDirection.REQUEST, "10.0.0.5", "10.0.0.9",
            functionCode, tid, "1", null, null, false, new double[0], new double[0]);
    }

    // A state that has already seen one FC-3 request at `ts`.
    private static ModbusEntityState stateAfterOneRequestAt(double ts) {
        ModbusEntityState state = ModbusEntityState.empty();
        state.advance(ts, 3, "17", ModbusDirection.REQUEST, null, null);
        return state;
    }

    private ModbusBuildFeaturesUseCase useCase() {
        return new ModbusBuildFeaturesUseCase(Clock.systemUTC());
    }

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
        assertEquals(1, result.newState().outstandingRequests(), "but the state advanced");
    }

    @Test
    void aSixteenSecondGapResetsTheSegmentWithoutFlagging() {
        ModbusEntityState state = stateAfterOneRequestAt(1000.0);
        FeatureBuildResult<ModbusEntityState> result = useCase().build(request(1016.0, 3, "18"), state);
        assertEquals(0.0f, result.vector().values()[23], "prev_event_available is zeroed");
        assertEquals(QualityFlags.NONE, result.vector().qualityFlags());
    }

    @Test
    void anOutOfOrderRecordResetsTheSegmentAndFlagsIt() {
        // The upstream engine raises here. A streaming operator resets instead, and
        // says so in qualityFlags so the condition stays observable.
        ModbusEntityState state = stateAfterOneRequestAt(1000.0);
        FeatureBuildResult<ModbusEntityState> result = useCase().build(request(999.5, 3, "18"), state);
        assertEquals(QualityFlags.MODBUS_OUT_OF_ORDER, result.vector().qualityFlags());
        assertEquals(0.0f, result.vector().values()[23]);
    }

    @Test
    void inSegmentInterArrivalIsAlwaysWithinZeroAndFifteen() {
        // The invariant the upstream engine asserts. After the segment rule it holds
        // by construction, so this test is what keeps the rule honest.
        // A fresh state for each case: build() advances the state in place, so
        // reusing one would chain the three events instead of testing each
        // against the same prior event.
        for (double ts : new double[] {1000.0, 1007.5, 1015.0}) {
            ModbusEntityState state = stateAfterOneRequestAt(1000.0);
            float ia = useCase().build(request(ts, 3, "18"), state).vector().values()[24];
            assertTrue(ia >= 0.0f && ia <= 15.0f, "inter_arrival_s out of range: " + ia);
        }
    }

    @Test
    void buildAdvancesTheGivenStateInPlaceAndReturnsIt() {
        // The documented contract ModbusFeatureProcessFunction relies on: the
        // state passed in IS the new state, already advanced.
        ModbusEntityState state = ModbusEntityState.empty();
        FeatureBuildResult<ModbusEntityState> result = useCase().build(request(1000.0, 3, "17"), state);
        assertSame(state, result.newState());
        assertEquals(1000.0, state.lastTs());
        assertEquals(1, state.outstandingRequests());
    }

    @Test
    void aSaturatedWindowSetsItsQualityBitAndAnOutOfOrderResetClearsIt() {
        // Cap 1: the second event evicts the first from every window while it is
        // still inside them, so that vector's window features differ from the
        // uncapped engine's.
        ModbusEntityState state = ModbusEntityState.emptyWithWindowCap(1);
        assertEquals(QualityFlags.NONE, useCase().build(request(1000.0, 3, "17"), state).vector().qualityFlags());
        assertEquals(QualityFlags.MODBUS_WINDOW_SATURATED,
            useCase().build(request(1000.5, 3, "18"), state).vector().qualityFlags());

        // Time going backwards resets the segment and flags out-of-order. The
        // reset leaves one entry per window, so that vector cannot be saturated:
        // the two bits never appear on the same vector. The next in-segment
        // event saturates again.
        assertEquals(QualityFlags.MODBUS_OUT_OF_ORDER,
            useCase().build(request(999.0, 3, "19"), state).vector().qualityFlags());
        assertEquals(QualityFlags.MODBUS_WINDOW_SATURATED,
            useCase().build(request(999.5, 3, "20"), state).vector().qualityFlags());
    }

    @Test
    void constructorRejectsASchemaThatDisagreesWithTheExtractorsOwnStatic() {
        // DnsFeatureSchemaV1.SCHEMA is a convenient wrong-but-real schema: not
        // ModbusFeatureSchemaV1.SCHEMA, so it must be rejected regardless of
        // which of id/contentHash/featureCount differs. The package-private
        // (Clock, FeatureSchema) constructor is what lets this test drive that
        // mismatch directly, without touching the registry.
        assertThrows(IllegalArgumentException.class,
            () -> new ModbusBuildFeaturesUseCase(Clock.systemUTC(), DnsFeatureSchemaV1.SCHEMA));
    }

    @Test
    void producedAtComesFromTheInjectedClock() {
        Instant fixed = Instant.parse("2026-09-21T10:00:00Z");
        ModbusBuildFeaturesUseCase useCase =
            new ModbusBuildFeaturesUseCase(Clock.fixed(fixed, ZoneOffset.UTC));
        assertEquals(fixed, useCase.build(request(1000.0, 3, "17"), ModbusEntityState.empty())
            .vector().producedAt());
    }

    // An event on the fixed key (10.0.0.5 -> 10.0.0.9, unit 1) with its own
    // address and quantity, which may be null as Zeek v1.0.0 writes them.
    private static ModbusEvent v1Event(double ts, ModbusDirection direction, String tid, Double address,
                                       Double quantity) {
        String uid = "u-" + ts;
        EventEnvelope envelope = new EventEnvelope(EventId.derive(new SensorId("sensor-eu-1"), uid),
            Instant.ofEpochMilli((long) (ts * 1000)), new SensorId("sensor-eu-1"), LogType.MODBUS, uid);
        boolean request = direction == ModbusDirection.REQUEST;
        return new ModbusEvent(envelope, ts, direction, request ? "10.0.0.5" : "10.0.0.9",
            request ? "10.0.0.9" : "10.0.0.5", 3, tid, "1", address, quantity, null, new double[0], new double[0]);
    }

    // F2 (spec section 2.1): Zeek leaves a response's address off; it takes its request's.
    @Test
    void aResponseWithoutAddressTakesItsPendingRequestsAddressAndQuantity() {
        ModbusBuildFeaturesUseCase useCase = useCase();
        ModbusEntityState state = ModbusEntityState.empty();
        useCase.build(v1Event(1000.0, ModbusDirection.REQUEST, "7", 100.0, 2.0), state);
        float[] v = useCase.build(v1Event(1000.25, ModbusDirection.RESPONSE, "7", null, null), state)
            .vector().values();
        assertEquals(100f, v[8], "address_value");
        assertEquals(1f, v[9], "address_present");
        assertEquals(2f, v[10], "quantity_value");
        assertEquals(1f, v[11], "quantity_present");
    }

    // F2: a response that carries its own address keeps it.
    @Test
    void aResponsesOwnAddressWins() {
        ModbusBuildFeaturesUseCase useCase = useCase();
        ModbusEntityState state = ModbusEntityState.empty();
        useCase.build(v1Event(1000.0, ModbusDirection.REQUEST, "7", 100.0, 2.0), state);
        float[] v = useCase.build(v1Event(1000.25, ModbusDirection.RESPONSE, "7", 300.0, null), state)
            .vector().values();
        assertEquals(300f, v[8], "its own address");
        assertEquals(2f, v[10], "the request's quantity, since it had none");
    }

    // Review Focus 3: no pending request, no borrowing.
    @Test
    void anUnansweredResponseBorrowsNothing() {
        float[] v = useCase().build(v1Event(1000.0, ModbusDirection.RESPONSE, "7", null, null),
            ModbusEntityState.empty()).vector().values();
        assertEquals(0f, v[9], "address_present");
        assertEquals(0f, v[11], "quantity_present");
    }

    // Review Focus 3: a >15 s gap starts a new segment, which forgets the request.
    @Test
    void aResponseAcrossASegmentStartBorrowsNothing() {
        ModbusBuildFeaturesUseCase useCase = useCase();
        ModbusEntityState state = ModbusEntityState.empty();
        useCase.build(v1Event(1000.0, ModbusDirection.REQUEST, "7", 100.0, 2.0), state);
        float[] v = useCase.build(v1Event(1016.0, ModbusDirection.RESPONSE, "7", null, null), state)
            .vector().values();
        assertEquals(0f, v[9], "address_present");
    }

    // Review Focus 3: a request evicted by the 4096 cap is no longer pending.
    @Test
    void aResponseWhoseRequestWasEvictedBorrowsNothing() {
        ModbusBuildFeaturesUseCase useCase = useCase();
        ModbusEntityState state = ModbusEntityState.empty();
        useCase.build(v1Event(1000.0, ModbusDirection.REQUEST, "evicted", 100.0, 2.0), state);
        for (int i = 0; i < 4096; i++) {
            useCase.build(v1Event(1000.0 + (i + 1) * 0.001, ModbusDirection.REQUEST, "t" + i, 200.0, 1.0), state);
        }
        float[] v = useCase.build(v1Event(1005.0, ModbusDirection.RESPONSE, "evicted", null, null), state)
            .vector().values();
        assertEquals(0f, v[9], "address_present");
    }
}
