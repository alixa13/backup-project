package io.netsecml.platform.domain.feature;

public record FeatureBuildResult(FeatureVector vector, SourceWindowState newState) {
}
