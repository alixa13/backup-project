package io.netsecml.platform.domain.inference;

// The detector's two scores for one window: the dense head's reconstruction
// MAE and the temporal head's endpoint MAE.
public record DetectorScores(double dense, double temporal) {
}
