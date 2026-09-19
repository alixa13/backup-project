package io.netsecml.platform.domain.inference;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;

// One scored event: the score and decision a model produced for it, plus the
// identity (model + schema + event) needed to trace that decision back to the
// bundle and feature vector that produced it, and re-derive predictionId on replay.
public record Prediction(String predictionId, String eventId, Instant eventTime, String modelName,
                          String modelVersion, String modelSha, String schemaId, String schemaHash,
                          float score, boolean decision, float threshold, long inferenceMicros,
                          int qualityFlags, Instant producedAt) {

    public Prediction {
        // Without an eventId this prediction cannot be joined back to the feature
        // vector that produced it, which is its only purpose.
        if (eventId == null || eventId.isBlank()) {
            throw new IllegalArgumentException("eventId must not be blank");
        }
        // predictions.prediction_id is FixedString(64) lowercase hex in ClickHouse;
        // anything else cannot be stored under that column's declared width.
        if (predictionId == null || !predictionId.matches("[0-9a-f]{64}")) {
            throw new IllegalArgumentException("predictionId must be 64 lowercase hex characters");
        }
    }

    public static String deriveId(String eventId, String modelName, String modelVersion) {
        // Deterministic so a replay rewrites the same row rather than adding one:
        // predictions is a ReplacingMergeTree keyed by (model, version, time, event).
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest((eventId + "|" + modelName + "|" + modelVersion)
                .getBytes(StandardCharsets.UTF_8));
            StringBuilder hex = new StringBuilder(64);
            for (byte b : hash) {
                hex.append(Character.forDigit((b >> 4) & 0xF, 16)).append(Character.forDigit(b & 0xF, 16));
            }
            return hex.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is required by every JVM", e);
        }
    }
}
