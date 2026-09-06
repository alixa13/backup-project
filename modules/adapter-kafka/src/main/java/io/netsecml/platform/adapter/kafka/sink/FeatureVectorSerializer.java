package io.netsecml.platform.adapter.kafka.sink;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.netsecml.platform.domain.feature.FeatureVector;
import org.apache.kafka.common.serialization.Serializer;
import java.io.Serializable;

// Serializable because the online job captures an instance of this class inside
// the lambda it hands to KafkaRecordSerializationSchema, and Flink serializes
// that lambda to ship it to the TaskManagers. Kafka's Serializer interface does
// not extend Serializable, so without this the job cannot be submitted at all.
// The only field is an ObjectMapper, which Jackson already makes Serializable.
public final class FeatureVectorSerializer implements Serializer<FeatureVector>, Serializable {
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
