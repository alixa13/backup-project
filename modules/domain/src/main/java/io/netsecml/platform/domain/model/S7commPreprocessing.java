package io.netsecml.platform.domain.model;

import io.netsecml.platform.domain.feature.S7commCategories;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;

// The S7comm Stage 1 detector's frozen preprocessing (preprocessor_contract.json;
// docs/superpowers/specs/2026-09-28-s7comm-stage1-scoring-design.md section 3.2):
// the 16 raw s7comm-feature-v1 values in, one column per contract output out
// (22 for detector v2). Twelve continuous values are imputed, then clipped to a
// physical range or robust-scaled and clipped; two binary values are imputed;
// the two categorical codes are decoded to their category strings and one-hot
// encoded, an unknown category as all zeros. Computed in double and carried as
// float32, as the Python recipe does. The output is always finite: NaN is
// imputed and every continuous value is clipped.
public final class S7commPreprocessing {

    // s7comm-feature-v1's layout: 12 continuous, 2 binary, then the two codes.
    public static final int RAW_WIDTH = 16;
    public static final String ROSCTR_FEATURE = "s7_rosctr";
    public static final String OPERATION_FEATURE = "s7_operation";
    private static final int CONTINUOUS_COUNT = 12;
    private static final int BINARY_END = 14;
    private static final int ROSCTR = 14;
    private static final int OPERATION = 15;

    // One continuous feature: the imputer's median, and either a physical range
    // (bounded: clip only) or a robust center and scale (unbounded).
    public record Continuous(String name, double median, double center, double scale, double low, double high) {

        public Continuous {
            Objects.requireNonNull(name, "name");
            // A bounded feature has both ends of its range; an unbounded one neither.
            if (Double.isNaN(low) != Double.isNaN(high)) {
                throw new IllegalArgumentException(name + ": a range needs both ends");
            }
            if (!Double.isNaN(low) && !(low < high)) {
                throw new IllegalArgumentException(name + ": range [" + low + ", " + high + "] is empty");
            }
            if (Double.isNaN(low) && !(scale > 0 && Double.isFinite(scale) && Double.isFinite(center))) {
                throw new IllegalArgumentException(name + ": an unbounded feature needs a finite center and a "
                    + "positive scale");
            }
            if (!Double.isFinite(median)) {
                throw new IllegalArgumentException(name + ": the imputer's median must be finite");
            }
        }

        public boolean bounded() {
            return !Double.isNaN(low);
        }
    }

    private final List<Continuous> continuous;
    private final List<String> binary;
    private final double transformedClip;
    private final List<String> rosctrCategories;
    private final List<String> operationCategories;

    public S7commPreprocessing(List<Continuous> continuous, List<String> binary, double transformedClip,
                               List<String> rosctrCategories, List<String> operationCategories) {
        this.continuous = List.copyOf(continuous);
        this.binary = List.copyOf(binary);
        this.transformedClip = transformedClip;
        this.rosctrCategories = List.copyOf(rosctrCategories);
        this.operationCategories = List.copyOf(operationCategories);
        // The contract's shape is s7comm-feature-v1's: 12 continuous, 2 binary.
        if (this.continuous.size() != CONTINUOUS_COUNT || this.binary.size() != BINARY_END - CONTINUOUS_COUNT) {
            throw new IllegalArgumentException("expected 12 continuous and 2 binary features, got "
                + this.continuous.size() + " and " + this.binary.size());
        }
        if (!(transformedClip > 0 && Double.isFinite(transformedClip))) {
            throw new IllegalArgumentException("transformedClip must be positive and finite, was " + transformedClip);
        }
        // A category listed twice would make its one-hot column ambiguous.
        requireDistinct(this.rosctrCategories, ROSCTR_FEATURE);
        requireDistinct(this.operationCategories, OPERATION_FEATURE);
    }

    private static void requireDistinct(List<String> categories, String feature) {
        if (categories.isEmpty() || new HashSet<>(categories).size() != categories.size()) {
            throw new IllegalArgumentException(feature + " needs distinct categories, got " + categories);
        }
    }

    public int width() {
        return BINARY_END + rosctrCategories.size() + operationCategories.size();
    }

    // The raw features, in the contract's raw_feature_order.
    public List<String> rawFeatureOrder() {
        List<String> names = new ArrayList<>();
        continuous.forEach(c -> names.add(c.name()));
        names.addAll(binary);
        names.add(ROSCTR_FEATURE);
        names.add(OPERATION_FEATURE);
        return List.copyOf(names);
    }

    // The names scikit-learn's ColumnTransformer gives its outputs, in order; the
    // loader checks the contract's transformed_feature_order against them.
    public List<String> transformedFeatureOrder() {
        List<String> names = new ArrayList<>();
        continuous.forEach(c -> names.add("continuous__" + c.name()));
        binary.forEach(b -> names.add("binary__" + b));
        rosctrCategories.forEach(c -> names.add("categorical__" + ROSCTR_FEATURE + "_" + c));
        operationCategories.forEach(c -> names.add("categorical__" + OPERATION_FEATURE + "_" + c));
        return List.copyOf(names);
    }

    // The score's per-column weights: 0 for every column a listed raw feature
    // produces, 1 elsewhere -- upstream's transformed_columns_for_raw_features
    // rule (reliability.py). A name that is not a raw feature is refused.
    public float[] scoreWeights(Collection<String> zeroWeightFeatures) {
        List<String> raw = rawFeatureOrder();
        for (String feature : zeroWeightFeatures) {
            if (!raw.contains(feature)) {
                throw new IllegalArgumentException("not a raw feature of this contract: " + feature);
            }
        }
        List<String> names = transformedFeatureOrder();
        float[] weights = new float[names.size()];
        for (int i = 0; i < names.size(); i++) {
            String name = names.get(i);
            boolean zero = zeroWeightFeatures.stream().anyMatch(f -> name.equals("continuous__" + f)
                || name.equals("binary__" + f) || name.startsWith("categorical__" + f + "_"));
            weights[i] = zero ? 0f : 1f;
        }
        return weights;
    }

    // The transformed vector, per the contract's continuous, binary and
    // categorical rules.
    public float[] apply(float[] raw) {
        if (raw.length != RAW_WIDTH) {
            throw new IllegalArgumentException("expected " + RAW_WIDTH + " values, got " + raw.length);
        }
        float[] out = new float[width()];
        // Continuous: impute NaN, then clip to the range, or robust-scale and clip.
        for (int i = 0; i < CONTINUOUS_COUNT; i++) {
            Continuous c = continuous.get(i);
            double x = Float.isNaN(raw[i]) ? c.median() : raw[i];
            double y = c.bounded()
                ? clip(x, c.low(), c.high())
                : clip((x - c.center()) / c.scale(), -transformedClip, transformedClip);
            out[i] = (float) y;
        }
        // Binary: NaN is the imputer's 0; anything else passes through.
        for (int i = CONTINUOUS_COUNT; i < BINARY_END; i++) {
            out[i] = Float.isNaN(raw[i]) ? 0f : raw[i];
        }
        // Categorical: one-hot of the decoded string; an unknown one is all zeros.
        oneHot(out, BINARY_END, rosctrCategories, category(raw[ROSCTR], false));
        oneHot(out, BINARY_END + rosctrCategories.size(), operationCategories, category(raw[OPERATION], true));
        return out;
    }

    // A code's category string, exactly as S7commCategories decodes it -- the
    // strings detector v2 was trained on (scoring spec amendment A1). A code
    // that is not a whole number is no category at all (all zeros).
    private static String category(float code, boolean operation) {
        if (!Float.isFinite(code) || code != Math.rint(code)) {
            return null;
        }
        int c = (int) code;
        return operation ? S7commCategories.decodeOperation(c) : S7commCategories.decodeRosctr(c);
    }

    private static void oneHot(float[] out, int at, List<String> categories, String value) {
        for (int j = 0; j < categories.size(); j++) {
            out[at + j] = categories.get(j).equals(value) ? 1f : 0f;
        }
    }

    // np.clip: an infinite input lands on the bound it passed.
    private static double clip(double x, double low, double high) {
        return Math.max(low, Math.min(high, x));
    }
}
