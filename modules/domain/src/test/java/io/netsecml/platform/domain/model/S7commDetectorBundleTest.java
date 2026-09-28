package io.netsecml.platform.domain.model;

import io.netsecml.platform.domain.inference.S7commScoreGroup;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

// A bundle's parts must agree: the graph's width is the preprocessing's, and
// the score has one non-negative weight per column, not all zero.
class S7commDetectorBundleTest {

    private static S7commPreprocessing preprocessing() {
        List<S7commPreprocessing.Continuous> cs = new ArrayList<>();
        for (int i = 0; i < 12; i++) {
            cs.add(new S7commPreprocessing.Continuous("c" + i, 0.0, 0.0, 1.0, Double.NaN, Double.NaN));
        }
        return new S7commPreprocessing(cs, List.of("b0", "b1"), 20.0, List.of("1", "3", "7"),
            List.of("FUNCTION_0X00", "READ_VAR", "SETUP_COMMUNICATION", "WRITE_VAR"));
    }

    private static S7commConformalPolicy policy() {
        Map<S7commScoreGroup, double[]> scores = new EnumMap<>(S7commScoreGroup.class);
        for (S7commScoreGroup g : S7commScoreGroup.values()) {
            scores.put(g, new double[]{0.1});
        }
        return new S7commConformalPolicy(scores, Map.of(), 0.001);
    }

    private static float[] filled(int n, float value) {
        float[] w = new float[n];
        Arrays.fill(w, value);
        return w;
    }

    private static S7commDetectorBundle bundle(int sequenceLength, int featureCount, float[] weights) {
        return new S7commDetectorBundle("s7comm-stage1-detector", "v1", "b".repeat(64), "s7comm-feature-v1",
            "c".repeat(64), sequenceLength, featureCount, "input", "reconstruction", preprocessing(), weights,
            policy());
    }

    @Test
    void theBundleIdIsNameSlashVersion() {
        assertEquals("s7comm-stage1-detector/v1", bundle(16, 21, filled(21, 1f)).bundleId());
    }

    @Test
    void theWeightsAreCopiedInAndOut() {
        float[] weights = filled(21, 1f);
        S7commDetectorBundle b = bundle(16, 21, weights);
        weights[0] = 5f;
        assertEquals(1f, b.scoreWeights()[0]);
        b.scoreWeights()[1] = 5f;
        assertEquals(1f, b.scoreWeights()[1]);
    }

    @Test
    void thePartsMustAgree() {
        assertThrows(IllegalArgumentException.class, () -> bundle(16, 20, filled(20, 1f)), "21 columns");
        assertThrows(IllegalArgumentException.class, () -> bundle(16, 21, filled(20, 1f)), "one weight per column");
        assertThrows(IllegalArgumentException.class, () -> bundle(16, 21, filled(21, 0f)), "not all zero");
        assertThrows(IllegalArgumentException.class, () -> bundle(16, 21, filled(21, -1f)), "not negative");
        assertThrows(IllegalArgumentException.class, () -> bundle(0, 21, filled(21, 1f)), "a window of events");
    }
}
