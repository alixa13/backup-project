package io.netsecml.platform.domain.inference;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.HexFormat;
import java.util.List;

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

        // score is a probability read off the model's positive-class output
        // column. NaN and infinity both pass a naive `< 0 || > 1` range check (a
        // NaN comparison is always false), so finiteness is checked explicitly.
        // A value outside 0..1 means the wrong output column was read -- exactly
        // the misconfiguration positiveClassColumn and threshold already guard
        // against elsewhere in this unit -- so it fails loudly at construction
        // rather than becoming a silent garbage row an operator has to notice later.
        if (!Float.isFinite(score) || score < 0.0f || score > 1.0f) {
            throw new IllegalArgumentException("score must be finite and within 0.0..1.0, got " + score);
        }

        // threshold is the decision cutoff this prediction was scored against,
        // recorded here for the archived row's own audit trail. Same rule as
        // ModelRef.threshold and for the same reason -- a NaN or out-of-range
        // value would pass a naive range check silently -- but this is not mere
        // defence in depth: PredictionDeserializer (archive side, reading the
        // prediction topic) builds a Prediction straight from wire JSON without
        // ever touching a ModelRef, so this constructor is the ONLY guard a
        // malformed or truncated message meets before becoming a row in
        // ClickHouse.
        if (!Float.isFinite(threshold) || threshold < 0.0f || threshold > 1.0f) {
            throw new IllegalArgumentException("threshold must be finite and within 0.0..1.0, got " + threshold);
        }

        // decision is deliberately NOT checked against score >= threshold here.
        // That comparison lives in exactly one place -- the scoring use case --
        // on purpose, so it cannot drift between two homes; a future model that
        // decides by something other than a single threshold would otherwise
        // force this record to encode a rule it no longer represents. This is a
        // decision, not an oversight.

        // inferenceMicros is a measured duration; a clock cannot produce a
        // negative one.
        if (inferenceMicros < 0) {
            throw new IllegalArgumentException("inferenceMicros must not be negative, got " + inferenceMicros);
        }
    }

    public static String deriveId(String eventId, String modelName, String modelVersion) {
        // Deterministic so the same (event, model, version) always recomputes the
        // same id on replay instead of minting a new one. prediction_id is NOT
        // part of predictions' ReplacingMergeTree key (ORDER BY (model_name,
        // model_version, event_time, event_id)), so a collision here would not
        // silently overwrite a row -- it would break anything that instead treats
        // prediction_id as unique: a Kafka message key, a join, a dedup query.
        //
        // Each part is length-prefixed before hashing so the encoding is
        // injective: a plain "|"-joined string would let ("x|y", "z", "v1") and
        // ("x", "y|z", "v1") hash identically, and half of eventId is an
        // operator-supplied sensor name that nothing here constrains.
        try {
            StringBuilder canonical = new StringBuilder();
            for (String part : List.of(eventId, modelName, modelVersion)) {
                canonical.append(part.length()).append(':').append(part);
            }
            byte[] hash = MessageDigest.getInstance("SHA-256")
                .digest(canonical.toString().getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(hash);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is required by every JVM", e);
        }
    }
}
