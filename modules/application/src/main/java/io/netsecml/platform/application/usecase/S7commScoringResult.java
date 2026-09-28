package io.netsecml.platform.application.usecase;

import io.netsecml.platform.domain.inference.S7commDetectorPrediction;
import io.netsecml.platform.domain.inference.S7commScoreWindow;

// One event's prediction and its connection's window after it.
public record S7commScoringResult(S7commDetectorPrediction prediction, S7commScoreWindow window) {
}
