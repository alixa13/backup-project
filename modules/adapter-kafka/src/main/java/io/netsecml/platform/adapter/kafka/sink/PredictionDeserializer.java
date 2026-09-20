package io.netsecml.platform.adapter.kafka.sink;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netsecml.platform.domain.inference.Prediction;
import org.apache.kafka.common.serialization.Deserializer;
import java.time.Instant;

// Exact inverse of PredictionSerializer. The archive job reads the prediction
// topic through this, which is why it lives beside the serializer rather than
// in adapter-clickhouse -- keeping JSON knowledge in the module that owns
// Kafka wire formats is what stops the two adapters from importing each other.
public final class PredictionDeserializer implements Deserializer<Prediction> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    // Reads one wire message back into a Prediction, field by field, in the
    // same order the serializer wrote them.
    @Override
    public Prediction deserialize(String topic, byte[] data) {
        try {
            JsonNode node = objectMapper.readTree(data);

            return new Prediction(
                node.get("predictionId").asText(),
                node.get("eventId").asText(),
                Instant.parse(node.get("eventTime").asText()),
                node.get("modelName").asText(),
                node.get("modelVersion").asText(),
                node.get("modelSha").asText(),
                node.get("schemaId").asText(),
                node.get("schemaHash").asText(),
                // score/threshold are float32 per the contract; asDouble()
                // narrowed back to float matches how the serializer wrote them.
                (float) node.get("score").asDouble(),
                node.get("decision").asBoolean(),
                (float) node.get("threshold").asDouble(),
                node.get("inferenceMicros").asLong(),
                node.get("qualityFlags").asInt(),
                Instant.parse(node.get("producedAt").asText()));
        } catch (Exception e) {
            // A malformed internal record is a bug, not an expected condition --
            // unlike an external source topic, this data was written by us.
            throw new IllegalArgumentException("failed to deserialize Prediction from topic " + topic, e);
        }
    }
}
