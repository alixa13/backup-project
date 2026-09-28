package io.netsecml.platform.adapter.kafka.sink;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netsecml.platform.domain.event.SensorId;
import io.netsecml.platform.domain.inference.DetectorVerdict;
import io.netsecml.platform.domain.inference.S7commDetectorPrediction;
import io.netsecml.platform.domain.inference.S7commScoreGroup;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

// Round trips, and a WARMUP prediction's score and p-value are JSON nulls, not absent keys.
class S7commDetectorPredictionSerializerTest {

    static S7commDetectorPrediction prediction(DetectorVerdict verdict, Float score, Double p) {
        return new S7commDetectorPrediction("a".repeat(64), "s:C1:25:REQUEST:1790000000123",
            Instant.parse("2026-09-28T12:00:00.123Z"), new SensorId("s"), "C1", "10.0.0.5", "10.0.0.9",
            "s7comm-stage1-detector", "v1", "b".repeat(64), "s7comm-feature-v1", "c".repeat(64), verdict, score, p,
            S7commScoreGroup.READ_REQUEST, 0.001, 40, 16, 150L, Instant.parse("2026-09-28T12:00:00.456Z"));
    }

    @Test
    void aWarmupsScoreAndPValueAreJsonNulls() throws Exception {
        JsonNode node = new ObjectMapper().readTree(new S7commDetectorPredictionSerializer()
            .serialize("t", prediction(DetectorVerdict.WARMUP, null, null)));
        assertTrue(node.has("score") && node.get("score").isNull());
        assertTrue(node.has("pValue") && node.get("pValue").isNull());
        assertEquals("READ_REQUEST", node.get("scoreGroup").asText());
    }

    @Test
    void everyFieldSurvivesARoundTrip() {
        for (S7commDetectorPrediction original : new S7commDetectorPrediction[]{
            prediction(DetectorVerdict.WARMUP, null, null), prediction(DetectorVerdict.ANOMALY, 0.53f, 0.2)}) {
            S7commDetectorPrediction restored = new S7commDetectorPredictionDeserializer()
                .deserialize("t", new S7commDetectorPredictionSerializer().serialize("t", original));
            assertEquals(original, restored);
        }
    }
}
