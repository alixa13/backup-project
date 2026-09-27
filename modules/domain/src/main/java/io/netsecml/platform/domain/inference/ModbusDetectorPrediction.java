package io.netsecml.platform.domain.inference;

import io.netsecml.platform.domain.event.SensorId;

import java.time.Instant;
import java.util.Objects;

// One Modbus event's prediction (contracts/stream/modbus-detector-prediction-v1.json):
// which device, which model, the verdict and, when the detector ran, both scores.
public record ModbusDetectorPrediction(String predictionId, String eventId, Instant eventTime, SensorId sensor,
                                       String connectionUid, String clientIp, String serverIp, String unitId,
                                       String modelName, String modelVersion, String modelSha, String schemaId,
                                       String schemaHash, DetectorVerdict verdict, Float denseScore,
                                       Float temporalScore, float denseThreshold, float temporalThreshold,
                                       DetectorTrigger trigger, int windowEvents, int qualityFlags,
                                       long inferenceMicros, Instant producedAt) {

    public ModbusDetectorPrediction {
        // The join keys and the model identity are required on every prediction.
        Objects.requireNonNull(predictionId, "predictionId");
        Objects.requireNonNull(eventId, "eventId");
        Objects.requireNonNull(eventTime, "eventTime");
        Objects.requireNonNull(sensor, "sensor");
        Objects.requireNonNull(verdict, "verdict");
        Objects.requireNonNull(trigger, "trigger");
        Objects.requireNonNull(producedAt, "producedAt");
        // Scores exist exactly when the detector ran.
        boolean hasScores = denseScore != null && temporalScore != null;
        boolean hasNoScores = denseScore == null && temporalScore == null;
        if (verdict.scored() ? !hasScores : !hasNoScores) {
            throw new IllegalArgumentException(verdict + " must " + (verdict.scored() ? "" : "not ")
                + "carry both scores");
        }
    }
}
