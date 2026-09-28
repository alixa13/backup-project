package io.netsecml.platform.domain.model;

import io.netsecml.platform.domain.inference.S7commScoreGroup;

import java.util.Arrays;
import java.util.EnumMap;
import java.util.Map;
import java.util.stream.DoubleStream;

// The S7comm Stage 1 detector's decision (scoring design section 3.4):
// upstream's one-sided conformal p-value against each group's normal-validation
// scores, p = (#{calibration >= score} + 1) / (n + 1), and ANOMALY iff
// p <= that group's alpha (conformal_pvalues, apply_group_conformal_policy).
// A group with no scores is judged against every group's scores pooled, with
// the fallback alpha. Immutable; built once per subtask.
public final class S7commConformalPolicy {

    private final EnumMap<S7commScoreGroup, double[]> calibration = new EnumMap<>(S7commScoreGroup.class);
    private final EnumMap<S7commScoreGroup, Double> alpha = new EnumMap<>(S7commScoreGroup.class);

    public S7commConformalPolicy(Map<S7commScoreGroup, double[]> scores, Map<S7commScoreGroup, Double> alphaByGroup,
                                 double fallbackAlpha) {
        requireAlpha(fallbackAlpha, "fallback");
        // Every group must be given, possibly empty, and every score finite.
        for (S7commScoreGroup g : S7commScoreGroup.values()) {
            double[] s = scores.get(g);
            if (s == null) {
                throw new IllegalArgumentException("no calibration scores given for " + g);
            }
            if (DoubleStream.of(s).anyMatch(v -> !Double.isFinite(v))) {
                throw new IllegalArgumentException(g + "'s calibration scores must all be finite");
            }
        }
        // The fallback reference: every non-empty group's scores, together.
        double[] pooled = Arrays.stream(S7commScoreGroup.values()).flatMapToDouble(g -> DoubleStream.of(scores.get(g)))
            .sorted().toArray();
        if (pooled.length == 0) {
            throw new IllegalArgumentException("a policy needs calibration scores");
        }
        for (S7commScoreGroup g : S7commScoreGroup.values()) {
            double[] own = scores.get(g).clone();
            Arrays.sort(own);
            if (own.length == 0) {
                calibration.put(g, pooled);
                alpha.put(g, fallbackAlpha);
            } else {
                calibration.put(g, own);
                double a = alphaByGroup.getOrDefault(g, fallbackAlpha);
                requireAlpha(a, g.name());
                alpha.put(g, a);
            }
        }
    }

    private static void requireAlpha(double a, String name) {
        if (!(a > 0 && a <= 1)) {
            throw new IllegalArgumentException(name + "'s alpha must be in (0, 1], was " + a);
        }
    }

    // The alpha this group's decision uses: its own, or the fallback when it
    // had no alpha or no calibration scores.
    public double alpha(S7commScoreGroup group) {
        return alpha.get(group);
    }

    // How many calibration scores judge this group (the pooled count for a
    // group that had none).
    public int calibrationSize(S7commScoreGroup group) {
        return calibration.get(group).length;
    }

    // conformal_pvalues: NaN for a non-finite score.
    public double pValue(S7commScoreGroup group, double score) {
        if (!Double.isFinite(score)) {
            return Double.NaN;
        }
        double[] ordered = calibration.get(group);
        int atLeast = ordered.length - firstAtLeast(ordered, score);
        return (atLeast + 1.0) / (ordered.length + 1.0);
    }

    // apply_group_conformal_policy: isfinite(p) & (p <= alpha).
    public boolean anomalous(S7commScoreGroup group, double score) {
        double p = pValue(group, score);
        return !Double.isNaN(p) && p <= alpha(group);
    }

    // np.searchsorted(side="left"): the first index whose value is >= key.
    private static int firstAtLeast(double[] ordered, double key) {
        int low = 0;
        int high = ordered.length;
        while (low < high) {
            int mid = (low + high) >>> 1;
            if (ordered[mid] < key) {
                low = mid + 1;
            } else {
                high = mid;
            }
        }
        return low;
    }
}
