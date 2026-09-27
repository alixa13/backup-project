package io.netsecml.platform.application.usecase;

import io.netsecml.platform.domain.inference.ModbusDetectorPrediction;
import io.netsecml.platform.domain.inference.ModbusScoreWindow;

// One event's prediction and the stream's window after it.
public record ModbusScoringResult(ModbusDetectorPrediction prediction, ModbusScoreWindow window) {
}
