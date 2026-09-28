package io.netsecml.platform.domain.inference;

import io.netsecml.platform.domain.event.SensorId;

import java.time.Instant;
import java.util.Objects;

// One S7comm event's prediction (contracts/stream/s7comm-detector-prediction-v1.json):
// which connection, which model, the verdict, the group and alpha that judged
// it, and -- when the detector judged it -- its score and p-value.
public record S7commDetectorPrediction(String predictionId, String eventId, Instant eventTime, SensorId sensor,
                                       String connectionUid, String clientIp, String serverIp, String modelName,
                                       String modelVersion, String modelSha, String schemaId, String schemaHash,
                                       DetectorVerdict verdict, Float score, Double pValue,
                                       S7commScoreGroup scoreGroup, double alpha, long eventsSinceReset,
                                       int qualityFlags, long inferenceMicros, Instant producedAt) {

    public S7commDetectorPrediction {
        // The join keys and the model identity are required on every prediction.
        Objects.requireNonNull(predictionId, "predictionId");
        Objects.requireNonNull(eventId, "eventId");
        Objects.requireNonNull(eventTime, "eventTime");
        Objects.requireNonNull(sensor, "sensor");
        Objects.requireNonNull(verdict, "verdict");
        Objects.requireNonNull(scoreGroup, "scoreGroup");
        Objects.requireNonNull(producedAt, "producedAt");
        // A score and a p-value exist exactly when the detector judged the event.
        boolean judged = score != null && pValue != null;
        boolean unjudged = score == null && pValue == null;
        if (verdict.scored() ? !judged : !unjudged) {
            throw new IllegalArgumentException(verdict + " must " + (verdict.scored() ? "" : "not ")
                + "carry a score and a p-value");
        }
        // The count includes this event; alpha is a probability.
        if (eventsSinceReset < 1) {
            throw new IllegalArgumentException("eventsSinceReset counts this event, so is at least 1");
        }
        if (!(alpha > 0 && alpha <= 1)) {
            throw new IllegalArgumentException("alpha must be in (0, 1], was " + alpha);
        }
    }
}
