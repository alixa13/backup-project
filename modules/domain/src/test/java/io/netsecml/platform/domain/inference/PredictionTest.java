package io.netsecml.platform.domain.inference;

import org.junit.jupiter.api.Test;
import java.time.Instant;
import static org.junit.jupiter.api.Assertions.*;

// deriveId gives each scored event a stable, deterministic id -- it is NOT the
// row identity ReplacingMergeTree dedups on (that is ORDER BY (model_name,
// model_version, event_time, event_id), and prediction_id is not part of it).
// What determinism protects is anything that instead treats prediction_id as
// unique: a Kafka message key, a join, a dedup query. The encoding is also
// injective (length-prefixed, not "|"-joined) so two different inputs cannot
// collide on the same id.
class PredictionTest {

    private static final Instant EVENT_TIME = Instant.parse("2026-09-19T10:00:00Z");
    private static final String VALID_PREDICTION_ID = "a".repeat(64);
    private static final String VALID_SHA = "b".repeat(64);
    private static final String VALID_SCHEMA_HASH = "c".repeat(64);

    @Test
    void theIdIsDeterministicAndSixtyFourHexCharacters() {
        String first = Prediction.deriveId("sensor-eu-1:Cabc123XYZ", "conn-demo", "v1");
        String second = Prediction.deriveId("sensor-eu-1:Cabc123XYZ", "conn-demo", "v1");
        assertEquals(first, second, "the same event and model must always produce the same id");
        assertTrue(first.matches("[0-9a-f]{64}"),
            "predictions.prediction_id is FixedString(64) lowercase hex");
    }

    @Test
    void adifferentModelVersionGivesADifferentId() {
        // Rescoring the same event with a new model must not overwrite the old row
        // under ReplacingMergeTree -- the version is part of the identity.
        assertNotEquals(Prediction.deriveId("e", "m", "v1"), Prediction.deriveId("e", "m", "v2"));
    }

    // A plain "|"-joined encoding would let a delimiter character inside one part
    // shift where the next part appears to start, so two different (eventId,
    // modelName) pairs could hash to the same id. eventId is half operator-supplied
    // (a sensor name), so nothing upstream rules this out -- the encoding itself
    // must prevent it.
    @Test
    void deriveIdDoesNotCollideWhenADelimiterCharacterCrossesPartBoundaries() {
        assertNotEquals(Prediction.deriveId("x|y", "z", "v1"), Prediction.deriveId("x", "y|z", "v1"));
    }

    // Without an eventId a prediction cannot be joined back to the feature vector
    // that produced it, which is its only purpose.
    @Test
    void rejectsABlankEventId() {
        assertThrows(IllegalArgumentException.class, () -> new Prediction(
            VALID_PREDICTION_ID, "  ", EVENT_TIME, "conn-demo", "v1", VALID_SHA, "conn-feature-v1",
            VALID_SCHEMA_HASH, 0.9f, true, 0.5f, 1200L, 0, EVENT_TIME));
    }

    // predictions.prediction_id is FixedString(64) in ClickHouse; anything else
    // cannot be stored under that column's declared width.
    @Test
    void rejectsAPredictionIdThatIsNotSixtyFourHexCharacters() {
        assertThrows(IllegalArgumentException.class, () -> new Prediction(
            "not-a-hash", "sensor-eu-1:Cabc123XYZ", EVENT_TIME, "conn-demo", "v1", VALID_SHA,
            "conn-feature-v1", VALID_SCHEMA_HASH, 0.9f, true, 0.5f, 1200L, 0, EVENT_TIME));
    }

    // score is a probability read off the model's positive-class output column.
    // A value outside 0..1 means the wrong column was read -- the same class of
    // misconfiguration positiveClassColumn and threshold already guard against
    // elsewhere in this unit -- so it fails loudly here instead of archiving a
    // garbage row.
    @Test
    void rejectsAScoreOutsideZeroToOne() {
        assertThrows(IllegalArgumentException.class, () -> new Prediction(
            VALID_PREDICTION_ID, "sensor-eu-1:Cabc123XYZ", EVENT_TIME, "conn-demo", "v1", VALID_SHA,
            "conn-feature-v1", VALID_SCHEMA_HASH, 1.5f, true, 0.5f, 1200L, 0, EVENT_TIME));
    }

    // NaN and infinity both pass a naive `< 0 || > 1` range check (a NaN
    // comparison is always false), so finiteness is checked separately, not
    // folded into the range test above.
    @Test
    void rejectsANonFiniteScore() {
        assertThrows(IllegalArgumentException.class, () -> new Prediction(
            VALID_PREDICTION_ID, "sensor-eu-1:Cabc123XYZ", EVENT_TIME, "conn-demo", "v1", VALID_SHA,
            "conn-feature-v1", VALID_SCHEMA_HASH, Float.NaN, true, 0.5f, 1200L, 0, EVENT_TIME));
    }

    // inferenceMicros is a measured duration; negative is not a value a clock can produce.
    @Test
    void rejectsANegativeInferenceMicros() {
        assertThrows(IllegalArgumentException.class, () -> new Prediction(
            VALID_PREDICTION_ID, "sensor-eu-1:Cabc123XYZ", EVENT_TIME, "conn-demo", "v1", VALID_SHA,
            "conn-feature-v1", VALID_SCHEMA_HASH, 0.9f, true, 0.5f, -1L, 0, EVENT_TIME));
    }

    // Same rule as ModelRef.threshold, but this is not defence in depth: the
    // archive side's PredictionDeserializer builds a Prediction straight from
    // wire JSON without ever going through a ModelRef, so this constructor is
    // the only thing standing between a malformed message and a garbage row.
    @Test
    void rejectsAThresholdOutsideZeroToOne() {
        assertThrows(IllegalArgumentException.class, () -> new Prediction(
            VALID_PREDICTION_ID, "sensor-eu-1:Cabc123XYZ", EVENT_TIME, "conn-demo", "v1", VALID_SHA,
            "conn-feature-v1", VALID_SCHEMA_HASH, 0.9f, true, 1.5f, 1200L, 0, EVENT_TIME));
    }

    // NaN passes a naive `< 0 || > 1` range check (every NaN comparison is
    // false), same as score above -- checked separately for the same reason.
    @Test
    void rejectsANonFiniteThreshold() {
        assertThrows(IllegalArgumentException.class, () -> new Prediction(
            VALID_PREDICTION_ID, "sensor-eu-1:Cabc123XYZ", EVENT_TIME, "conn-demo", "v1", VALID_SHA,
            "conn-feature-v1", VALID_SCHEMA_HASH, 0.9f, true, Float.NaN, 1200L, 0, EVENT_TIME));
    }
}
