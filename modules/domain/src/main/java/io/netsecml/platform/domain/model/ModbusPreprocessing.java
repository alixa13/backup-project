package io.netsecml.platform.domain.model;

import java.util.List;

// modbus_preprocessing_contract_v1's 42 -> 42 transform: each feature's
// frozen policy, applied in double precision and carried as float32, the
// model's input type. No clipping, as the contract says; a non-finite result
// is left for the caller to reject.
public final class ModbusPreprocessing {

    private final List<FeaturePreprocessing> features;

    public ModbusPreprocessing(List<FeaturePreprocessing> features) {
        this.features = List.copyOf(features);
        // Every conditional feature's mask must be a feature of this vector.
        for (FeaturePreprocessing f : this.features) {
            if (f.policy().conditional() && (f.maskIndex() < 0 || f.maskIndex() >= this.features.size())) {
                throw new IllegalArgumentException(f.feature() + ": mask index " + f.maskIndex()
                    + " is outside the " + this.features.size() + "-feature vector");
            }
        }
    }

    public int width() {
        return features.size();
    }

    public List<String> featureOrder() {
        return features.stream().map(FeaturePreprocessing::feature).toList();
    }

    // The transformed vector, feature by feature, per the contract's
    // transform_definitions.
    public float[] apply(float[] raw) {
        if (raw.length != features.size()) {
            throw new IllegalArgumentException("expected " + features.size() + " values, got " + raw.length);
        }
        float[] out = new float[raw.length];
        for (int i = 0; i < raw.length; i++) {
            FeaturePreprocessing f = features.get(i);
            double x = raw[i];
            // A conditional feature is exactly 0.0 where its mask is 0.
            if (f.policy().conditional() && raw[f.maskIndex()] != 1f) {
                out[i] = 0f;
                continue;
            }
            double y = switch (f.policy()) {
                case PASSTHROUGH_BINARY, PASSTHROUGH_BOUNDED_OR_CONSTANT -> x;
                case GLOBAL_STANDARD, CONDITIONAL_STANDARD -> (x - f.mean()) / f.std();
                case GLOBAL_LOG1P_ONLY, CONDITIONAL_LOG1P_ONLY -> Math.log1p(x);
                case CONDITIONAL_LOG1P_THEN_STANDARD -> (Math.log1p(x) - f.mean()) / f.std();
            };
            out[i] = (float) y;
        }
        return out;
    }
}
