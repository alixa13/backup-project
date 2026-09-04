package io.netsecml.platform.adapter.kafka.sink;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.netsecml.platform.domain.feature.FeatureVector;
import org.apache.kafka.common.serialization.Serializer;

public final class FeatureVectorSerializer implements Serializer<FeatureVector> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public byte[] serialize(String topic, FeatureVector vector) {
        try {
            // Field order mirrors contracts/stream/feature-vector-v1.json so a
            // human diffing a Kafka message against the contract reads top to bottom.
            ObjectNode node = objectMapper.createObjectNode();
            node.put("eventId", vector.eventId());
            node.put("eventTime", vector.eventTime().toString());
            node.put("sensor", vector.sensor().value());
            node.put("schemaId", vector.schemaId());
            node.put("schemaHash", vector.schemaHash());

            // The 20 float32 values, in frozen schema order.
            ArrayNode values = node.putArray("values");
            for (float v : vector.values()) {
                values.add(v);
            }

            node.put("qualityFlags", vector.qualityFlags());

            // Emission time. The archive job writes this as row_version, which is
            // what makes "last emission wins" deduplication deterministic.
            node.put("producedAt", vector.producedAt().toString());

            return objectMapper.writeValueAsBytes(node);
        } catch (Exception e) {
            throw new RuntimeException("failed to serialize FeatureVector for topic " + topic, e);
        }
    }
}
