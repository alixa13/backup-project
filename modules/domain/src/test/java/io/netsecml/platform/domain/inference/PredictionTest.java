package io.netsecml.platform.domain.inference;

import org.junit.jupiter.api.Test;
import java.time.Instant;
import static org.junit.jupiter.api.Assertions.*;

// deriveId is the identity Prediction rows use in ClickHouse's ReplacingMergeTree:
// deterministic per (event, model, version) so a replay overwrites rather than
// duplicates, and versioned so rescoring under a new model adds a row instead.
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
}
