package io.netsecml.platform.bootstrap.online;

import io.netsecml.platform.port.out.ReconstructionScorer;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

// The composition root's factory: the real bundle (detector v2), loaded and opened,
// reconstructs the all-zero window as Python does (OnnxReconstructionScorerTest's numbers).
class S7commDetectorScorerFactoryTest {

    private static final String FIXTURE = Path.of("..", "..", "tests", "fixtures", "models",
        "s7comm-stage1-detector", "v2").toString();

    @Test
    void itLoadsTheBundleAndReconstructs() {
        try (ReconstructionScorer scorer = new S7commDetectorScorerFactory(FIXTURE).create()) {
            assertEquals(-0.04589303955435753f, scorer.reconstructLast(new float[16][22])[0], 1e-5f);
            assertEquals("s7comm-stage1-detector/v2", scorer.bundle().bundleId());
        }
    }

    @Test
    void aMissingBundleFailsLoudly() {
        assertThrows(IllegalStateException.class, () -> new S7commDetectorScorerFactory("/no/such/bundle").create());
    }
}
