package io.netsecml.platform.adapter.registry;

import java.util.Objects;

// One golden vector from bundle.json's sampleVectors: a real feature vector
// paired with the probability scikit-learn produced for it at training time
// (contracts/model/model-bundle-v1.json). A later test feeds values through
// the loaded ONNX model and asserts its score agrees with expectedScore
// within a small tolerance -- the guard against a converter (e.g. skl2onnx)
// changing semantics between training and serving, which no other check in
// this pipeline would notice.
public record SampleVector(float[] values, double expectedScore) {

    public SampleVector {
        // values must exist -- a golden vector with no values cannot be scored.
        Objects.requireNonNull(values, "values must not be null");
        // Defensive copy in: the caller keeps no handle on our internal array,
        // so it cannot mutate this instance's state after construction.
        values = values.clone();
    }

    // Defensive copy out: callers cannot mutate our internal array either.
    @Override
    public float[] values() {
        return values.clone();
    }
}
