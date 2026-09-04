package io.netsecml.platform.adapter.kafka.sink;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import static org.junit.jupiter.api.Assertions.*;

class RejectedRecordSerializerTest {
    // Fixed instant reused by every test below so assertions compare against a
    // known literal instead of a moving Instant.now().
    private static final Instant RECEIVED_AT = Instant.parse("2026-08-27T10:03:11.250Z");

    // One representative PARSE-stage payload reused by most assertions below;
    // emitsStageAndEventId builds its own MAP-stage payload where that distinction matters.
    private RejectedRecordPayload payload() {
        return new RejectedRecordPayload(
            "{ broken".getBytes(StandardCharsets.UTF_8),
            "",
            "PARSE",
            "MALFORMED_JSON",
            "unexpected end of input",
            RECEIVED_AT);
    }

    // Baseline smoke test carried over from before this task: reasonCode and
    // detail must still round-trip, and raw bytes must never appear on the wire.
    @Test
    void serializesReasonCodeDetailAndRawPayloadHash() throws Exception {
        JsonNode json = new ObjectMapper().readTree(
            new RejectedRecordSerializer().serialize("netsec.conn.dlq.v1", payload()));

        assertEquals("MALFORMED_JSON", json.get("reasonCode").asText());
        assertEquals("unexpected end of input", json.get("detail").asText());
        assertTrue(json.has("rawPayloadHash"));
        assertFalse(json.has("rawPayload"), "raw payload bytes must never be stored, only their hash");
    }

    // The hash lands in a FixedString(64) column.
    @Test
    void emitsASixtyFourCharacterHexHash() throws Exception {
        JsonNode json = new ObjectMapper().readTree(
            new RejectedRecordSerializer().serialize("netsec.conn.dlq.v1", payload()));

        assertEquals(64, json.get("rawPayloadHash").asText().length());
        assertTrue(json.get("rawPayloadHash").asText().matches("[0-9a-f]{64}"));
    }

    // stage is the PARSE/MAP discriminator that invalid_events.stage stores.
    @Test
    void emitsStageAndEventId() throws Exception {
        RejectedRecordPayload mapStage = new RejectedRecordPayload(
            "{}".getBytes(StandardCharsets.UTF_8), "sensor-eu-1:Cabc", "MAP",
            "INVALID_PORT", "port 70000 out of range", RECEIVED_AT);

        JsonNode json = new ObjectMapper().readTree(
            new RejectedRecordSerializer().serialize("netsec.conn.dlq.v1", mapStage));

        assertEquals("MAP", json.get("stage").asText());
        assertEquals("sensor-eu-1:Cabc", json.get("eventId").asText());
    }

    // The serializer must not mint data. Serializing the same payload twice has to
    // produce byte-identical output, which it cannot if it calls Instant.now().
    @Test
    void isDeterministicAndTakesReceivedAtFromThePayload() throws Exception {
        RejectedRecordSerializer serializer = new RejectedRecordSerializer();
        byte[] first = serializer.serialize("netsec.conn.dlq.v1", payload());
        byte[] second = serializer.serialize("netsec.conn.dlq.v1", payload());

        assertArrayEquals(first, second, "serializing the same payload twice must produce identical bytes");
        assertEquals("2026-08-27T10:03:11.250Z",
            new ObjectMapper().readTree(first).get("receivedAt").asText());
    }
}
