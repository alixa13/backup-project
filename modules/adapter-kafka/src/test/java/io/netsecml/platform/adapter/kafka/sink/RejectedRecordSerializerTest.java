package io.netsecml.platform.adapter.kafka.sink;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import java.nio.charset.StandardCharsets;
import static org.junit.jupiter.api.Assertions.*;

class RejectedRecordSerializerTest {
    @Test
    void serializesReasonCodeDetailAndRawPayloadHash() throws Exception {
        RejectedRecordPayload payload = new RejectedRecordPayload(
            "{ broken".getBytes(StandardCharsets.UTF_8), "MALFORMED_JSON", "unexpected end of input");

        RejectedRecordSerializer serializer = new RejectedRecordSerializer();
        byte[] bytes = serializer.serialize("netsec.conn.dlq.v1", payload);

        JsonNode json = new ObjectMapper().readTree(bytes);
        assertEquals("MALFORMED_JSON", json.get("reasonCode").asText());
        assertEquals("unexpected end of input", json.get("detail").asText());
        assertTrue(json.has("rawPayloadHash"));
        assertFalse(json.has("rawPayload"), "raw payload bytes must never be stored, only their hash");
    }
}
