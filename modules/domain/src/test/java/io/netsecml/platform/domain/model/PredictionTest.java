package io.netsecml.platform.domain.model;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

// deriveId is the identity Prediction rows use in ClickHouse's ReplacingMergeTree:
// deterministic per (event, model, version) so a replay overwrites rather than
// duplicates, and versioned so rescoring under a new model adds a row instead.
class PredictionTest {

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
}
