package io.netsecml.platform.adapter.kafka.sink;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.common.serialization.Serializer;
import java.security.MessageDigest;
import java.time.Instant;

public final class RejectedRecordSerializer implements Serializer<RejectedRecordPayload> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public byte[] serialize(String topic, RejectedRecordPayload payload) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(payload.rawPayload());
            StringBuilder hex = new StringBuilder();
            for (byte b : digest) {
                hex.append(String.format("%02x", b));
            }

            ObjectNode node = objectMapper.createObjectNode();
            node.put("reasonCode", payload.reasonCode());
            node.put("detail", payload.detail());
            node.put("rawPayloadHash", hex.toString());
            node.put("receivedAt", Instant.now().toString());
            return objectMapper.writeValueAsBytes(node);
        } catch (Exception e) {
            throw new RuntimeException("failed to serialize RejectedRecordPayload for topic " + topic, e);
        }
    }
}
