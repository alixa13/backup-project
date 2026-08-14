package io.netsecml.platform.domain.feature;

public record FeatureDefinition(int index, String name, String unit, MissingPolicy missingPolicy, String formula) {
    public enum MissingPolicy { REQUIRED, DEFAULT_ZERO }
}
