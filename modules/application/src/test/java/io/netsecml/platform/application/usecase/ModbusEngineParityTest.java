package io.netsecml.platform.application.usecase;

import io.netsecml.platform.application.feature.reference.ReferenceModbusEngine;
import io.netsecml.platform.application.feature.reference.ReferenceModbusEntityState;
import io.netsecml.platform.domain.event.ModbusEvent;
import io.netsecml.platform.domain.feature.FeatureBuildResult;
import io.netsecml.platform.domain.feature.ModbusEntityState;
import io.netsecml.platform.domain.feature.ModbusFeatureSchemaV1;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

// Differential test: the production engine (ModbusBuildFeaturesUseCase over
// ModbusEntityState) against the verbatim copy of the engine at 1d0878e
// (ReferenceModbusEngine), event by event, on the same reproducible streams.
// All 42 values must be bit-identical (Float.floatToRawIntBits, so -0.0f vs
// 0.0f and any last-ulp drift both fail) and the quality flags equal. The
// oracle is the engine that ModbusGoldenVectorTest proved against a hand
// derivation of the upstream 07b engine, so equality here carries that proof
// over to whatever the production engine becomes.
//
// Every stream stays far below any window cap: the oracle has none, so this
// test is only about the in-cap engine. The cap itself is
// ModbusWindowSaturationTest's subject.
class ModbusEngineParityTest {

    // Fixed seeds, so a failure names a stream that replays exactly.
    private static final long FIRST_SEED = 1L;
    private static final int SEEDS = 40;
    private static final int EVENTS_PER_SEED = 2_000;
    private static final int KEYS = 3;

    private final ModbusBuildFeaturesUseCase useCase =
        new ModbusBuildFeaturesUseCase(Clock.fixed(Instant.parse("2026-09-24T00:00:00Z"), ZoneOffset.UTC));
    private final ReferenceModbusEngine reference = new ReferenceModbusEngine();

    @Test
    void randomMixedStreamsMatchTheOracleBitForBit() {
        // How often each condition the streams are meant to reach actually
        // occurred, so a generator change that quietly stops exercising one
        // fails here instead of leaving a gap nobody sees.
        int outOfOrder = 0;
        int responseWithoutRequest = 0;
        int requestOverwrite = 0;
        int rttValid = 0;
        int addressDelta = 0;
        int quantityDelta = 0;
        int neitherReadNorWrite = 0;
        int segmentRestarts = 0;
        int busySixtySecondWindows = 0;

        for (long seed = FIRST_SEED; seed < FIRST_SEED + SEEDS; seed++) {
            List<ModbusEventStreams.Keyed> stream = ModbusEventStreams.randomMixed(seed, EVENTS_PER_SEED, KEYS);

            // One state per entity key, for each engine, threaded exactly as
            // ModbusFeatureProcessFunction threads Flink's ValueState.
            ModbusEntityState[] states = new ModbusEntityState[KEYS];
            ReferenceModbusEntityState[] referenceStates = new ReferenceModbusEntityState[KEYS];
            for (int k = 0; k < KEYS; k++) {
                states[k] = ModbusEntityState.empty();
                referenceStates[k] = ReferenceModbusEntityState.empty();
            }

            for (int i = 0; i < stream.size(); i++) {
                int key = stream.get(i).key();
                ModbusEvent event = stream.get(i).event();
                FeatureBuildResult<ModbusEntityState> actual = useCase.build(event, states[key]);
                ReferenceModbusEngine.Step expected = reference.step(event, referenceStates[key]);
                assertSameVector("seed " + seed + ", event " + i, expected, actual);

                float[] v = expected.values();
                outOfOrder += expected.qualityFlags() != 0 ? 1 : 0;
                responseWithoutRequest += v[31] == 1f ? 1 : 0;
                requestOverwrite += v[32] == 1f ? 1 : 0;
                rttValid += v[33] == 1f ? 1 : 0;
                addressDelta += v[26] == 1f ? 1 : 0;
                quantityDelta += v[28] == 1f ? 1 : 0;
                neitherReadNorWrite += v[40] == 0f && v[41] == 0f ? 1 : 0;
                segmentRestarts += v[23] == 0f && referenceStates[key].lastTs() != null ? 1 : 0;
                busySixtySecondWindows += v[37] * 60f >= 100f ? 1 : 0;

                states[key] = actual.newState();
                referenceStates[key] = expected.newState();
            }
        }

        assertCovered("out-of-order records", outOfOrder);
        assertCovered("responses with no pending request", responseWithoutRequest);
        assertCovered("requests reusing a pending tid", requestOverwrite);
        assertCovered("matched responses with an rtt", rttValid);
        assertCovered("address deltas", addressDelta);
        assertCovered("quantity deltas", quantityDelta);
        assertCovered("10 s windows with neither a read nor a write", neitherReadNorWrite);
        assertCovered("segment restarts after a gap", segmentRestarts);
        assertCovered("60 s windows holding 100+ events", busySixtySecondWindows);
    }

    private static void assertCovered(String condition, int count) {
        if (count == 0) {
            fail("the random streams never produced " + condition);
        }
    }

    @Test
    void anUnansweredRequestFloodPastThePendingCapMatchesTheOracle() {
        // 6000 requests 1 ms apart: past the 4096-entry pending cap after about
        // 4.1 s, with a late response every 50th event.
        List<ModbusEvent> stream = ModbusEventStreams.unansweredFlood(6_000, 0.001);
        ModbusEntityState state = ModbusEntityState.empty();
        ReferenceModbusEntityState referenceState = ReferenceModbusEntityState.empty();
        for (int i = 0; i < stream.size(); i++) {
            FeatureBuildResult<ModbusEntityState> actual = useCase.build(stream.get(i), state);
            ReferenceModbusEngine.Step expected = reference.step(stream.get(i), referenceState);
            assertSameVector("flood event " + i, expected, actual);
            state = actual.newState();
            referenceState = expected.newState();
        }
        assertEquals(4096, state.outstandingRequests(), "the stream must actually reach the pending cap");
    }

    private static void assertSameVector(String where, ReferenceModbusEngine.Step expected,
                                         FeatureBuildResult<ModbusEntityState> actual) {
        float[] expectedValues = expected.values();
        float[] actualValues = actual.vector().values();
        assertEquals(expectedValues.length, actualValues.length, where + ": vector width");
        for (int f = 0; f < expectedValues.length; f++) {
            if (Float.floatToRawIntBits(expectedValues[f]) != Float.floatToRawIntBits(actualValues[f])) {
                fail(where + ": feature " + f + " ("
                    + ModbusFeatureSchemaV1.SCHEMA.definitions().get(f).name() + ") expected "
                    + expectedValues[f] + " but was " + actualValues[f]);
            }
        }
        assertEquals(expected.qualityFlags(), actual.vector().qualityFlags(), where + ": quality flags");
    }
}
