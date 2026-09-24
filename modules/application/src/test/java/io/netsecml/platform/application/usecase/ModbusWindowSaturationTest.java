package io.netsecml.platform.application.usecase;

import io.netsecml.platform.application.feature.reference.ReferenceModbusEngine;
import io.netsecml.platform.application.feature.reference.ReferenceModbusEntityState;
import io.netsecml.platform.domain.event.ModbusEvent;
import io.netsecml.platform.domain.feature.FeatureBuildResult;
import io.netsecml.platform.domain.feature.ModbusEntityState;
import io.netsecml.platform.domain.feature.QualityFlags;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

// The per-window cap against the uncapped 1d0878e oracle, on bursty streams
// with caps small enough to bite. MODBUS_WINDOW_SATURATED must be exact in
// both directions: a vector without it is bit-identical to the oracle's, and
// a vector with it differs from the oracle's in at least one window rate --
// while indices 0-34 (groups A-C, which never read a window) match the oracle
// either way.
class ModbusWindowSaturationTest {

    // Indices 35-41 are group D, the window features; 35-37 are the three
    // event rates, one per window.
    private static final int FIRST_WINDOW_FEATURE = 35;
    private static final int LAST_EVENT_RATE = 37;

    private static final int[] CAPS = {5, 20, 60};
    private static final long FIRST_SEED = 101L;
    private static final int SEEDS = 12;
    private static final int EVENTS_PER_SEED = 2_000;
    private static final int KEYS = 3;

    private final ModbusBuildFeaturesUseCase useCase =
        new ModbusBuildFeaturesUseCase(Clock.fixed(Instant.parse("2026-09-24T00:00:00Z"), ZoneOffset.UTC));
    private final ReferenceModbusEngine reference = new ReferenceModbusEngine();

    @Test
    void theSaturationFlagMarksExactlyTheVectorsWhoseWindowsDifferFromTheUncappedEngine() {
        int saturated = 0;
        int exact = 0;
        int recoveredWithinASegment = 0;

        for (int cap : CAPS) {
            for (long seed = FIRST_SEED; seed < FIRST_SEED + SEEDS; seed++) {
                List<ModbusEventStreams.Keyed> stream = ModbusEventStreams.randomMixed(seed, EVENTS_PER_SEED, KEYS);
                ModbusEntityState[] states = new ModbusEntityState[KEYS];
                ReferenceModbusEntityState[] referenceStates = new ReferenceModbusEntityState[KEYS];
                boolean[] wasSaturated = new boolean[KEYS];
                for (int k = 0; k < KEYS; k++) {
                    states[k] = ModbusEntityState.emptyWithWindowCap(cap);
                    referenceStates[k] = ReferenceModbusEntityState.empty();
                }

                for (int i = 0; i < stream.size(); i++) {
                    String where = "cap " + cap + ", seed " + seed + ", event " + i;
                    int key = stream.get(i).key();
                    ModbusEvent event = stream.get(i).event();
                    FeatureBuildResult<ModbusEntityState> actual = useCase.build(event, states[key]);
                    ReferenceModbusEngine.Step expected = reference.step(event, referenceStates[key]);
                    float[] actualValues = actual.vector().values();
                    float[] expectedValues = expected.values();
                    boolean flagged = (actual.vector().qualityFlags() & QualityFlags.MODBUS_WINDOW_SATURATED) != 0;

                    // The flag on the vector is the state's own verdict, and the
                    // other bits are exactly the oracle's.
                    assertEquals(states[key].windowSaturated(), flagged, where + ": flag vs state");
                    assertEquals(expected.qualityFlags(),
                        actual.vector().qualityFlags() & ~QualityFlags.MODBUS_WINDOW_SATURATED,
                        where + ": the other quality bits");

                    // Groups A-C never read a window: exact regardless.
                    assertBitsEqual(where, expectedValues, actualValues, 0, FIRST_WINDOW_FEATURE - 1);

                    if (flagged) {
                        saturated++;
                        if (!anyBitsDiffer(expectedValues, actualValues, FIRST_WINDOW_FEATURE, LAST_EVENT_RATE)) {
                            fail(where + ": flagged saturated, yet every window rate equals the uncapped engine's");
                        }
                    } else {
                        exact++;
                        assertBitsEqual(where, expectedValues, actualValues, FIRST_WINDOW_FEATURE,
                            expectedValues.length - 1);
                        // Unflagged right after a flagged vector, with no segment
                        // restart between (prev_event_available = 1): the flag
                        // cleared by ageing out, not by a reset.
                        if (wasSaturated[key] && actualValues[23] == 1f) {
                            recoveredWithinASegment++;
                        }
                    }
                    wasSaturated[key] = flagged;
                    states[key] = actual.newState();
                    referenceStates[key] = expected.newState();
                }
            }
        }

        // The streams must reach every case, or the assertions above prove little.
        assertTrue(saturated > 0, "no vector was ever saturated");
        assertTrue(exact > 0, "no vector was ever exact");
        assertTrue(recoveredWithinASegment > 0, "saturation never cleared by ageing out within a segment");
    }

    private static void assertBitsEqual(String where, float[] expected, float[] actual, int from, int to) {
        for (int f = from; f <= to; f++) {
            if (Float.floatToRawIntBits(expected[f]) != Float.floatToRawIntBits(actual[f])) {
                fail(where + ": feature " + f + " expected " + expected[f] + " but was " + actual[f]);
            }
        }
    }

    private static boolean anyBitsDiffer(float[] expected, float[] actual, int from, int to) {
        for (int f = from; f <= to; f++) {
            if (Float.floatToRawIntBits(expected[f]) != Float.floatToRawIntBits(actual[f])) {
                return true;
            }
        }
        return false;
    }
}
