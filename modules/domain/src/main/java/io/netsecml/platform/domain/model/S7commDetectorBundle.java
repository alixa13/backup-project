package io.netsecml.platform.domain.model;

import java.util.Arrays;
import java.util.Objects;

// A loaded, verified S7comm Stage 1 detector: its identity, the feature schema
// it was trained on, its graph's input and output names, its frozen
// preprocessing, the score's per-column weights and its conformal policy.
// Built only by the bundle loader, after every SHA-256 and every contract
// check has passed.
public record S7commDetectorBundle(String name, String version, String modelSha, String schemaId,
                                   String schemaHash, int sequenceLength, int featureCount, String inputName,
                                   String outputName, S7commPreprocessing preprocessing, float[] scoreWeights,
                                   S7commConformalPolicy policy) {

    public S7commDetectorBundle {
        // Identity and graph names are looked up by string; none may be missing.
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(version, "version");
        Objects.requireNonNull(modelSha, "modelSha");
        Objects.requireNonNull(schemaId, "schemaId");
        Objects.requireNonNull(schemaHash, "schemaHash");
        Objects.requireNonNull(inputName, "inputName");
        Objects.requireNonNull(outputName, "outputName");
        Objects.requireNonNull(preprocessing, "preprocessing");
        Objects.requireNonNull(policy, "policy");
        if (sequenceLength < 1) {
            throw new IllegalArgumentException("sequenceLength must be at least 1, was " + sequenceLength);
        }
        // The preprocessing produces exactly the vector the graph reads.
        if (featureCount != preprocessing.width()) {
            throw new IllegalArgumentException("featureCount " + featureCount + " but preprocessing produces "
                + preprocessing.width());
        }
        // One finite, non-negative weight per column, not all zero (debiased.py's checks).
        scoreWeights = Arrays.copyOf(Objects.requireNonNull(scoreWeights, "scoreWeights"), scoreWeights.length);
        if (scoreWeights.length != featureCount) {
            throw new IllegalArgumentException(scoreWeights.length + " weights for " + featureCount + " columns");
        }
        float sum = 0f;
        for (float w : scoreWeights) {
            if (!(w >= 0 && Float.isFinite(w))) {
                throw new IllegalArgumentException("weights must be finite and >= 0, found " + w);
            }
            sum += w;
        }
        if (sum <= 0f) {
            throw new IllegalArgumentException("at least one weight must be > 0");
        }
    }

    @Override
    public float[] scoreWeights() {
        return Arrays.copyOf(scoreWeights, scoreWeights.length);
    }

    // How a window records which bundle filled it: name/version.
    public String bundleId() {
        return name + "/" + version;
    }
}
