package io.netsecml.platform.domain.model;

import io.netsecml.platform.domain.inference.S7commScoreGroup;
import org.junit.jupiter.api.Test;

import java.util.EnumMap;
import java.util.Map;
import java.util.stream.IntStream;

import static io.netsecml.platform.domain.inference.S7commScoreGroup.OTHER_REQUEST;
import static io.netsecml.platform.domain.inference.S7commScoreGroup.READ_REQUEST;
import static io.netsecml.platform.domain.inference.S7commScoreGroup.RESPONSE;
import static io.netsecml.platform.domain.inference.S7commScoreGroup.WRITE_REQUEST;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

// upstream's conformal_pvalues and apply_group_conformal_policy
// (debiased.py, causal_shadow.py) on a hand-built policy.
class S7commConformalPolicyTest {

    // RESPONSE: four scores (unsorted on purpose), alpha 0.2. READ_REQUEST: one
    // score, no alpha of its own. WRITE_REQUEST: 0.01..0.63, alpha 1/64.
    // OTHER_REQUEST: no scores, a configured alpha that must not apply.
    private static S7commConformalPolicy policy() {
        Map<S7commScoreGroup, double[]> scores = new EnumMap<>(S7commScoreGroup.class);
        scores.put(RESPONSE, new double[]{0.4, 0.1, 0.3, 0.2});
        scores.put(READ_REQUEST, new double[]{0.05});
        scores.put(WRITE_REQUEST, IntStream.rangeClosed(1, 63).mapToDouble(i -> i / 100.0).toArray());
        scores.put(OTHER_REQUEST, new double[0]);
        Map<S7commScoreGroup, Double> alphas = new EnumMap<>(S7commScoreGroup.class);
        alphas.put(RESPONSE, 0.2);
        alphas.put(WRITE_REQUEST, 0.015625);
        alphas.put(OTHER_REQUEST, 0.01);
        return new S7commConformalPolicy(scores, alphas, 0.001);
    }

    @Test
    void thePValueCountsTheCalibrationScoresAtLeastTheScore() {
        S7commConformalPolicy p = policy();
        assertEquals(0.4, p.pValue(RESPONSE, 0.35), 1e-12, "0.4 only: (1 + 1) / 5");
        assertEquals(0.6, p.pValue(RESPONSE, 0.3), 1e-12, "0.3 counts itself: (2 + 1) / 5");
        assertEquals(0.2, p.pValue(RESPONSE, 0.5), 1e-12, "none: 1 / 5");
        assertEquals(1.0, p.pValue(RESPONSE, 0.0), 1e-12, "all four: 5 / 5");
    }

    @Test
    void anAnomalyIsAPValueAtMostTheAlpha() {
        assertTrue(policy().anomalous(RESPONSE, 0.5), "p 0.2 <= 0.2");
        assertFalse(policy().anomalous(RESPONSE, 0.4), "p 0.4");
    }

    // 63 calibration scores: 1/64 is the smallest p-value there is, so a write
    // is an anomaly only above every one of them.
    @Test
    void aWriteIsAnAnomalyOnlyAboveEveryCalibrationScore() {
        assertTrue(policy().anomalous(WRITE_REQUEST, 0.64));
        assertEquals(1.0 / 64, policy().pValue(WRITE_REQUEST, 0.64), 1e-12);
        assertFalse(policy().anomalous(WRITE_REQUEST, 0.63), "equal to the maximum: p = 2/64");
    }

    @Test
    void aGroupWithoutAnAlphaUsesTheFallback() {
        assertEquals(0.001, policy().alpha(READ_REQUEST));
    }

    // apply_group_conformal_policy: a group with no calibration scores is judged
    // against every group's scores pooled, with the fallback alpha -- its own
    // configured alpha never applies.
    @Test
    void aGroupWithoutCalibrationUsesEveryScorePooledAndTheFallbackAlpha() {
        assertEquals(4 + 1 + 63, policy().calibrationSize(OTHER_REQUEST));
        assertEquals(0.001, policy().alpha(OTHER_REQUEST));
        assertEquals(1.0 / 69, policy().pValue(OTHER_REQUEST, 1.0), 1e-12);
    }

    @Test
    void aNonFiniteScoreIsNeverAnAnomaly() {
        assertTrue(Double.isNaN(policy().pValue(RESPONSE, Double.NaN)));
        assertFalse(policy().anomalous(RESPONSE, Double.NaN));
    }

    @Test
    void anUnusablePolicyIsRefused() {
        Map<S7commScoreGroup, double[]> missing = new EnumMap<>(S7commScoreGroup.class);
        missing.put(RESPONSE, new double[]{0.1});
        assertThrows(IllegalArgumentException.class, () -> new S7commConformalPolicy(missing, Map.of(), 0.001),
            "every group must be given, even empty");
        Map<S7commScoreGroup, double[]> nan = new EnumMap<>(S7commScoreGroup.class);
        for (S7commScoreGroup g : S7commScoreGroup.values()) {
            nan.put(g, new double[]{Double.NaN});
        }
        assertThrows(IllegalArgumentException.class, () -> new S7commConformalPolicy(nan, Map.of(), 0.001));
        Map<S7commScoreGroup, double[]> none = new EnumMap<>(S7commScoreGroup.class);
        for (S7commScoreGroup g : S7commScoreGroup.values()) {
            none.put(g, new double[0]);
        }
        assertThrows(IllegalArgumentException.class, () -> new S7commConformalPolicy(none, Map.of(), 0.001));
        Map<S7commScoreGroup, double[]> fine = new EnumMap<>(S7commScoreGroup.class);
        for (S7commScoreGroup g : S7commScoreGroup.values()) {
            fine.put(g, new double[]{0.1});
        }
        assertThrows(IllegalArgumentException.class, () -> new S7commConformalPolicy(fine, Map.of(), 0.0));
    }
}
