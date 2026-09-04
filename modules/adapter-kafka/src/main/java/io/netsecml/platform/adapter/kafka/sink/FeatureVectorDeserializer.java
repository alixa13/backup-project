package io.netsecml.platform.adapter.kafka.sink;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netsecml.platform.domain.event.LogType;
import io.netsecml.platform.domain.event.SensorId;
import io.netsecml.platform.domain.feature.FeatureVector;
import org.apache.kafka.common.serialization.Deserializer;
import java.time.Instant;

// Exact inverse of FeatureVectorSerializer. The archive job reads the
// feature-vector topic through this, which is why it lives beside the
// serializer rather than in adapter-clickhouse — keeping JSON knowledge in the
// module that owns Kafka wire formats is what stops the two adapters from
// importing each other.
public final class FeatureVectorDeserializer implements Deserializer<FeatureVector> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    // Reads one wire message back into a FeatureVector, field by field, in the
    // same order the serializer wrote them.
    @Override
    public FeatureVector deserialize(String topic, byte[] data) {
        try {
            JsonNode node = objectMapper.readTree(data);

            // values arrives as a JSON array of numbers; narrow each to float32,
            // which is the dtype the model contract fixes.
            JsonNode valuesNode = node.get("values");
            float[] values = new float[valuesNode.size()];
            for (int i = 0; i < values.length; i++) {
                values[i] = (float) valuesNode.get(i).asDouble();
            }

            return new FeatureVector(
                node.get("eventId").asText(),
                Instant.parse(node.get("eventTime").asText()),
                new SensorId(node.get("sensor").asText()),
                // logType was written lowercase by wireName(); valueOf() needs the
                // enum constant's exact case, so upper-case it back on the way in.
                LogType.valueOf(node.get("logType").asText().toUpperCase()),
                node.get("connectionUid").asText(),
                node.get("schemaId").asText(),
                node.get("schemaHash").asText(),
                values,
                node.get("qualityFlags").asInt(),
                Instant.parse(node.get("producedAt").asText()));
        } catch (Exception e) {
            // A malformed internal record is a bug, not an expected condition —
            // unlike the external conn topic, this data was written by us.
            throw new IllegalArgumentException("failed to deserialize FeatureVector from topic " + topic, e);
        }
    }
}
