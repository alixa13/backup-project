package io.netsecml.platform.adapter.kafka.sink;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netsecml.platform.domain.event.SensorId;
import io.netsecml.platform.domain.inference.DetectorTrigger;
import io.netsecml.platform.domain.inference.DetectorVerdict;
import io.netsecml.platform.domain.inference.ModbusDetectorPrediction;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

// Round trips, and a WARMUP prediction's scores are JSON nulls, not absent keys.
class ModbusDetectorPredictionSerializerTest {

    private static ModbusDetectorPrediction warmup() {
        return new ModbusDetectorPrediction("a".repeat(64), "s:u:7:REQUEST:1", Instant.parse("2026-09-26T12:00:00Z"),
            new SensorId("s"), "u", "10.0.0.5", "10.0.0.9", "1", "modbus-stage1-detector", "v1", "b".repeat(64),
            "modbus-feature-v1", "c".repeat(64), DetectorVerdict.WARMUP, null, null, 0.2483385f, 0.4121148f,
            DetectorTrigger.NONE, 3, 0, 0L, Instant.parse("2026-09-26T12:00:00.001Z"));
    }

    @Test
    void aWarmupsScoresAreJsonNulls() throws Exception {
        JsonNode node = new ObjectMapper().readTree(
            new ModbusDetectorPredictionSerializer().serialize("t", warmup()));
        assertTrue(node.has("denseScore") && node.get("denseScore").isNull());
        assertTrue(node.has("temporalScore") && node.get("temporalScore").isNull());
        assertEquals("WARMUP", node.get("verdict").asText());
    }

    @Test
    void everyFieldSurvivesARoundTrip() {
        ModbusDetectorPrediction original = warmup();
        ModbusDetectorPrediction restored = new ModbusDetectorPredictionDeserializer()
            .deserialize("t", new ModbusDetectorPredictionSerializer().serialize("t", original));
        assertEquals(original, restored);
        assertNull(restored.denseScore());
    }
}
