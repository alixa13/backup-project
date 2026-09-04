package io.netsecml.platform.adapter.kafka.sink;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netsecml.platform.domain.event.SensorId;
import io.netsecml.platform.domain.feature.ConnFeatureSchemaV1;
import io.netsecml.platform.domain.feature.FeatureVector;
import org.junit.jupiter.api.Test;
import java.time.Instant;
import static org.junit.jupiter.api.Assertions.*;

class FeatureVectorSerializerTest {
    // One representative vector reused by every assertion below.
    private FeatureVector vector() {
        return new FeatureVector(
            "sensor-eu-1:abc",
            Instant.parse("2026-08-13T10:00:00Z"),
            new SensorId("sensor-eu-1"),
            ConnFeatureSchemaV1.SCHEMA.id(),
            ConnFeatureSchemaV1.CONTENT_HASH,
            new float[]{1f, 2f, 3f},
            0,
            Instant.parse("2026-08-13T10:00:00.402Z"));
    }

    // Baseline smoke test for the fields that predate this task. sensor and
    // producedAt get their own dedicated tests below, so this one deliberately
    // stays focused on what was already on the wire.
    @Test
    void serializesAllFieldsAsJson() throws Exception {
        byte[] bytes = new FeatureVectorSerializer().serialize("netsec.conn.feature-vector.v1", vector());

        JsonNode json = new ObjectMapper().readTree(bytes);
        assertEquals("sensor-eu-1:abc", json.get("eventId").asText());
        assertEquals("conn-feature-v1", json.get("schemaId").asText());
        assertEquals(ConnFeatureSchemaV1.CONTENT_HASH, json.get("schemaHash").asText());
        assertEquals(3, json.get("values").size());
        assertEquals(1.0, json.get("values").get(0).asDouble(), 0.0001);
    }

    // sensor is what lets a training query slice by sensor without splitting the
    // composite eventId on its internal delimiter.
    @Test
    void emitsSensorAsAPlainString() throws Exception {
        JsonNode json = new ObjectMapper().readTree(
            new FeatureVectorSerializer().serialize("netsec.conn.feature-vector.v1", vector()));

        assertEquals("sensor-eu-1", json.get("sensor").asText());
    }

    // producedAt becomes the ClickHouse row_version, so it must survive the wire
    // exactly, milliseconds included.
    @Test
    void emitsProducedAtAsIso8601WithMilliseconds() throws Exception {
        JsonNode json = new ObjectMapper().readTree(
            new FeatureVectorSerializer().serialize("netsec.conn.feature-vector.v1", vector()));

        assertEquals("2026-08-13T10:00:00.402Z", json.get("producedAt").asText());
        assertEquals(Instant.parse("2026-08-13T10:00:00.402Z"), Instant.parse(json.get("producedAt").asText()));
    }
}
