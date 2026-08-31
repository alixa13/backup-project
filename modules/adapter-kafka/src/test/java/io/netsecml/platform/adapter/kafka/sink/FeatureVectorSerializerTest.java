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
    @Test
    void serializesAllFieldsAsJson() throws Exception {
        // sensor and producedAt are structural on FeatureVector now; this test only
        // exercises serialization of the pre-existing fields, so fixed values suffice.
        FeatureVector vector = new FeatureVector(
            "sensor-eu-1:abc", Instant.parse("2026-08-13T10:00:00Z"), new SensorId("sensor-eu-1"),
            ConnFeatureSchemaV1.SCHEMA.id(), ConnFeatureSchemaV1.CONTENT_HASH,
            new float[]{1f, 2f, 3f}, 0, Instant.parse("2026-08-13T10:00:00Z"));

        FeatureVectorSerializer serializer = new FeatureVectorSerializer();
        byte[] bytes = serializer.serialize("netsec.conn.feature-vector.v1", vector);

        JsonNode json = new ObjectMapper().readTree(bytes);
        assertEquals("sensor-eu-1:abc", json.get("eventId").asText());
        assertEquals("conn-feature-v1", json.get("schemaId").asText());
        assertEquals(ConnFeatureSchemaV1.CONTENT_HASH, json.get("schemaHash").asText());
        assertEquals(3, json.get("values").size());
        assertEquals(1.0, json.get("values").get(0).asDouble(), 0.0001);
    }
}
