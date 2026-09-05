package io.netsecml.platform.adapter.kafka.sink;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.common.serialization.Serializer;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.io.Serializable;

// Serializable because the online job captures an instance of this class inside
// the lambda it hands to KafkaRecordSerializationSchema, and Flink serializes
// that lambda to ship it to the TaskManagers. Kafka's Serializer interface does
// not extend Serializable, so without this the job cannot be submitted at all.
// The only field is an ObjectMapper, which Jackson already makes Serializable.
public final class RejectedRecordSerializer implements Serializer<RejectedRecordPayload>, Serializable {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public byte[] serialize(String topic, RejectedRecordPayload payload) {
        try {
            // Only the hash of the rejected payload is published. The bytes may
            // contain customer network data and must not leave the pipeline.
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(payload.rawPayload());
            String rawPayloadHash = HexFormat.of().formatHex(digest);

            // Field order mirrors contracts/stream/dlq-v1.json.
            //
            // Every value comes from the payload. This serializer mints nothing:
            // receivedAt used to be Instant.now() here, which recorded when the
            // sink ran rather than when the record was rejected, and re-stamped on
            // every serialization attempt.
            ObjectNode node = objectMapper.createObjectNode();
            node.put("eventId", payload.eventId());
            node.put("stage", payload.stage());
            node.put("reasonCode", payload.reasonCode());
            node.put("detail", payload.detail());
            node.put("rawPayloadHash", rawPayloadHash);
            node.put("receivedAt", payload.receivedAt().toString());

            return objectMapper.writeValueAsBytes(node);
        } catch (Exception e) {
            throw new RuntimeException("failed to serialize RejectedRecordPayload for topic " + topic, e);
        }
    }
}
