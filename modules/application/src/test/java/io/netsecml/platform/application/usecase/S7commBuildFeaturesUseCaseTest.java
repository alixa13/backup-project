package io.netsecml.platform.application.usecase;

import io.netsecml.platform.domain.event.EventEnvelope;
import io.netsecml.platform.domain.event.EventId;
import io.netsecml.platform.domain.event.LogType;
import io.netsecml.platform.domain.event.S7commEvent;
import io.netsecml.platform.domain.event.SensorId;
import io.netsecml.platform.domain.feature.DnsFeatureSchemaV1;
import io.netsecml.platform.domain.feature.FeatureBuildResult;
import io.netsecml.platform.domain.feature.QualityFlags;
import io.netsecml.platform.domain.feature.S7commConnectionState;
import io.netsecml.platform.domain.feature.S7commFeatureSchemaV1;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

// The use case end to end: advance, extract, flag. The oracle test (in
// adapter-flink) proves the values against upstream; these pin the wiring of
// state, categorical codes, flags and schema labels.
class S7commBuildFeaturesUseCaseTest {
    private static final SensorId SENSOR = new SensorId("sensor-eu-1");

    private final S7commBuildFeaturesUseCase useCase =
        new S7commBuildFeaturesUseCase(Clock.fixed(Instant.parse("2026-09-24T12:00:00Z"), ZoneOffset.UTC));

    // A request (to port 102) or a response (from it) on uid C1.
    private static S7commEvent event(double ts, boolean request, int pdu, Integer rosctr, Integer function,
                                     String name) {
        String uid = "C1";
        return new S7commEvent(
            new EventEnvelope(EventId.derive(SENSOR, uid + ":" + pdu + ":" + request + ":" + ts),
                Instant.ofEpochMilli(Math.round(ts * 1000)), SENSOR, LogType.S7COMM, uid),
            ts, request ? "10.0.0.5" : "10.0.0.9", request ? 50001 : 102,
            request ? "10.0.0.9" : "10.0.0.5", request ? 102 : 50001, pdu, rosctr, function, name);
    }

    @Test
    void theVectorTakesItsWidthIdAndHashFromTheRegisteredSchema() {
        FeatureBuildResult<S7commConnectionState> result =
            useCase.build(event(1000.0, true, 1, 1, 4, null), S7commConnectionState.empty());
        assertEquals(16, result.vector().values().length);
        assertEquals("s7comm-feature-v1", result.vector().schemaId());
        assertEquals(S7commFeatureSchemaV1.CONTENT_HASH, result.vector().schemaHash());
        assertEquals(LogType.S7COMM, result.vector().logType());
        assertEquals("C1", result.vector().connectionUid());
        assertEquals(Instant.parse("2026-09-24T12:00:00Z"), result.vector().producedAt());
    }

    @Test
    void buildAdvancesTheGivenStateInPlaceAndReturnsIt() {
        S7commConnectionState state = S7commConnectionState.empty();
        FeatureBuildResult<S7commConnectionState> result = useCase.build(event(1000.0, true, 1, 1, 4, null), state);
        assertSame(state, result.newState());
        assertEquals(1, state.outstandingRequests());
    }

    @Test
    void theNumericSlotsReadTheStateAfterTheEvent() {
        S7commConnectionState state = S7commConnectionState.empty();
        useCase.build(event(1000.0, true, 7, 1, 4, null), state);
        float[] response = useCase.build(event(1000.1, false, 7, 3, 4, null), state).vector().values();
        assertEquals(0f, response[0], "s7_outstanding_requests: the response cleared it");
        assertEquals(0.5f, response[1], "s7_outstanding_mean_16: counts 1, 0");
        assertEquals(1f, response[2], "s7_response_match_rate_16");
        assertEquals(1f, response[3], "s7_same_function_run_length");
        assertEquals(1f, response[4], "s7_same_direction_run_length");
        assertEquals(0.5f, response[5], "s7_request_ratio_16");
        assertEquals(0.5f, response[6], "s7_direction_change_rate_16");
        assertEquals(0f, response[12], "is_request_direction");
        assertEquals(0f, response[13], "s7_function_changed is 0 for a response");
    }

    @Test
    void theCategoricalSlotsCarryTheCodes() {
        float[] coded = useCase.build(event(1000.0, true, 1, 1, 0x29, null), S7commConnectionState.empty())
            .vector().values();
        assertEquals(1f, coded[14]);
        assertEquals(41f, coded[15]);

        float[] missing = useCase.build(event(1000.0, true, 1, null, null, null), S7commConnectionState.empty())
            .vector().values();
        assertEquals(-1f, missing[14]);
        assertEquals(-1f, missing[15]);

        float[] named = useCase.build(event(1000.0, true, 1, 1, null, "plc_stop"), S7commConnectionState.empty())
            .vector().values();
        assertEquals(41f, named[15], "a known name maps back to its code");

        float[] unseen = useCase.build(event(1000.0, true, 1, 1, null, "vendor_thing"), S7commConnectionState.empty())
            .vector().values();
        assertEquals(-2f, unseen[15]);
    }

    @Test
    void functionChangedMarksAChangedRequestFunction() {
        S7commConnectionState state = S7commConnectionState.empty();
        assertEquals(0f, useCase.build(event(1000.0, true, 1, 1, 4, null), state).vector().values()[13]);
        assertEquals(1f, useCase.build(event(1000.1, true, 2, 1, 5, null), state).vector().values()[13]);
    }

    @Test
    void anOutOfOrderRecordIsFlaggedAndStillProcessed() {
        S7commConnectionState state = S7commConnectionState.empty();
        assertEquals(QualityFlags.NONE,
            useCase.build(event(1000.0, true, 1, 1, 4, null), state).vector().qualityFlags());
        FeatureBuildResult<S7commConnectionState> late = useCase.build(event(999.0, true, 2, 1, 4, null), state);
        assertEquals(QualityFlags.S7COMM_OUT_OF_ORDER, late.vector().qualityFlags());
        assertEquals(2f, late.vector().values()[0], "the late request still counts as outstanding");
    }

    @Test
    void constructorRejectsASchemaThatDisagreesWithTheExtractorsOwnStatic() {
        assertThrows(IllegalArgumentException.class,
            () -> new S7commBuildFeaturesUseCase(Clock.systemUTC(), DnsFeatureSchemaV1.SCHEMA));
    }
}
