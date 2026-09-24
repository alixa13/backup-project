package io.netsecml.platform.domain.feature;

public record FeatureDefinition(int index, String name, String unit, MissingPolicy missingPolicy, String formula) {
    // REQUIRED: the feature's source must be present or the record is rejected.
    // DEFAULT_ZERO: absent or not-yet-computable means 0.
    // SENTINEL: absence is itself a category, carried as a documented negative
    // code (s7comm-feature-v1's two categorical features; see
    // contracts/features/s7comm-feature-schema-v1.json's categoricalCodes).
    public enum MissingPolicy { REQUIRED, DEFAULT_ZERO, SENTINEL }
}
