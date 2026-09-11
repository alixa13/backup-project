package io.netsecml.platform.domain.feature;

// The vector built from one event, plus the window state to store for that key.
//
// Generic in the state so it carries whatever state its log type uses, rather
// than naming one log type's state in a type every log type returns.
public record FeatureBuildResult<S>(FeatureVector vector, S newState) {
}
