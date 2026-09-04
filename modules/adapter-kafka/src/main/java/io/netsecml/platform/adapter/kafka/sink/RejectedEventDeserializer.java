package io.netsecml.platform.adapter.kafka.sink;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netsecml.platform.domain.event.ReasonCode;
import io.netsecml.platform.domain.event.RejectedEvent;
import org.apache.kafka.common.serialization.Deserializer;
import java.time.Instant;

// Reads the DLQ topic into the neutral domain RejectedEvent.
//
// Note the asymmetry with RejectedRecordPayload on the producer side: that type
// carries raw bytes because the serializer hashes them. By the time a message is
// on the wire only the hash exists, so the consumer-side type carries no payload.
//
// stage is deliberately NOT read back — it is derivable from reason.stage(), and
// re-reading it would let a hand-edited message disagree with the domain.
public final class RejectedEventDeserializer implements Deserializer<RejectedEvent> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    // Reads one wire message back into a RejectedEvent, field by field. reasonCode
    // is parsed back into the enum via valueOf(); an unrecognized name (produced by
    // a version mismatch, say) surfaces as an IllegalArgumentException below rather
    // than being silently accepted.
    @Override
    public RejectedEvent deserialize(String topic, byte[] data) {
        try {
            JsonNode node = objectMapper.readTree(data);
            return new RejectedEvent(
                node.get("eventId").asText(),
                node.get("rawPayloadHash").asText(),
                ReasonCode.valueOf(node.get("reasonCode").asText()),
                node.get("detail").asText(),
                Instant.parse(node.get("receivedAt").asText()));
        } catch (Exception e) {
            // A malformed internal record is a bug, not an expected condition —
            // unlike the external conn topic, this data was written by us.
            throw new IllegalArgumentException("failed to deserialize RejectedEvent from topic " + topic, e);
        }
    }
}
