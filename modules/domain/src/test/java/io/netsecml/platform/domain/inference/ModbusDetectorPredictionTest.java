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
