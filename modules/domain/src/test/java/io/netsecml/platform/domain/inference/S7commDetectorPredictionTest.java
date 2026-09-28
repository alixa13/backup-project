package io.netsecml.platform.domain.inference;

import io.netsecml.platform.domain.event.SensorId;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

// A score and a p-value exist exactly when the detector judged the event.
class S7commDetectorPredictionTest {

    private static S7commDetectorPrediction prediction(DetectorVerdict verdict, Float score, Double p, double alpha,
                                                       long events) {
        return new S7commDetectorPrediction("a".repeat(64), "s:C1:1:REQUEST:1", Instant.EPOCH, new SensorId("s"),
            "C1", "10.0.0.5", "10.0.0.9", "s7comm-stage1-detector", "v1", "b".repeat(64), "s7comm-feature-v1",
            "c".repeat(64), verdict, score, p, S7commScoreGroup.READ_REQUEST, alpha, events, 0, 0L, Instant.EPOCH);
    }

    @Test
    void judgedVerdictsCarryAScoreAndAPValue() {
        assertDoesNotThrow(() -> prediction(DetectorVerdict.NORMAL, 0.1f, 0.5, 0.001, 16));
        assertThrows(IllegalArgumentException.class, () -> prediction(DetectorVerdict.ANOMALY, null, 0.5, 0.001, 16));
        assertThrows(IllegalArgumentException.class, () -> prediction(DetectorVerdict.ANOMALY, 0.1f, null, 0.001, 16));
    }

    @Test
    void otherVerdictsCarryNeither() {
        assertDoesNotThrow(() -> prediction(DetectorVerdict.WARMUP, null, null, 0.001, 3));
        assertThrows(IllegalArgumentException.class, () -> prediction(DetectorVerdict.UNSCORABLE, 0.1f, null, 0.001, 16));
    }

    @Test
    void theEventCountStartsAtOneAndAlphaIsAProbability() {
        assertThrows(IllegalArgumentException.class, () -> prediction(DetectorVerdict.WARMUP, null, null, 0.001, 0));
        assertThrows(IllegalArgumentException.class, () -> prediction(DetectorVerdict.WARMUP, null, null, 0.0, 1));
    }
}
