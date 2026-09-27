package io.netsecml.platform.domain.model;

import java.util.Objects;

// One feature's frozen transform: its policy, the index of its mask feature
// (-1 for none), and its TRAIN mean and std (NaN where the policy uses none).
public record FeaturePreprocessing(String feature, PreprocessingPolicy policy, int maskIndex, double mean,
                                   double std) {

    public FeaturePreprocessing {
        // A feature is looked up by name and by policy; neither may be missing.
        Objects.requireNonNull(feature, "feature");
        Objects.requireNonNull(policy, "policy");
        // A standardized policy divides by std, so it must be a positive number.
        if (policy.standardized() && !(Double.isFinite(mean) && Double.isFinite(std) && std > 0)) {
            throw new IllegalArgumentException(feature + ": " + policy + " needs a finite mean and a positive std");
        }
    }
}
