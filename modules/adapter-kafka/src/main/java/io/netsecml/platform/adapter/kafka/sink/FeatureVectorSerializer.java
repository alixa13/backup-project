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
            ObjectNode node = objectMapper.createObjectNode();
            node.put("eventId", vector.eventId());
            node.put("eventTime", vector.eventTime().toString());
            node.put("schemaId", vector.schemaId());
            node.put("schemaHash", vector.schemaHash());
            node.put("qualityFlags", vector.qualityFlags());
            ArrayNode values = node.putArray("values");
            for (float v : vector.values()) {
                values.add(v);
            }
            return objectMapper.writeValueAsBytes(node);
        } catch (Exception e) {
            throw new RuntimeException("failed to serialize FeatureVector for topic " + topic, e);
        }
    }
}
