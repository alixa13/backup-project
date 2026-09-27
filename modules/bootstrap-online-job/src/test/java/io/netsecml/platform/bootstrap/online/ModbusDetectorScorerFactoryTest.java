package io.netsecml.platform.bootstrap.online;

import io.netsecml.platform.domain.inference.DetectorScores;
import io.netsecml.platform.port.out.SequenceScorer;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

// The composition root's factory: the real bundle, loaded and opened, scores
// the all-zero window as Python does (OnnxSequenceScorerTest's numbers).
class ModbusDetectorScorerFactoryTest {

    private static final String FIXTURE = Path.of("..", "..", "tests", "fixtures", "models",
        "modbus-stage1-detector", "v1").toString();

    @Test
    void itLoadsTheBundleAndScores() {
        try (SequenceScorer scorer = new ModbusDetectorScorerFactory(FIXTURE).create()) {
            DetectorScores s = scorer.score(new float[20][42]);
            assertEquals(0.3135979175567627, s.dense(), 1e-5);
            assertEquals("modbus-stage1-detector/v1", scorer.bundle().bundleId());
        }
    }

    @Test
    void aMissingBundleFailsLoudly() {
        assertThrows(IllegalStateException.class, () -> new ModbusDetectorScorerFactory("/no/such/bundle").create());
    }
}
