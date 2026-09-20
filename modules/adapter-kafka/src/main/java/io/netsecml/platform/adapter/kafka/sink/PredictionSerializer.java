package io.netsecml.platform.adapter.kafka.sink;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.netsecml.platform.domain.inference.Prediction;
import org.apache.kafka.common.serialization.Serializer;
import java.io.Serializable;

// Serializable for the same reason as FeatureVectorSerializer: a job that scores
// online captures an instance of this class inside the lambda it hands to
// KafkaRecordSerializationSchema, and Flink serializes that lambda to ship it
// to the TaskManagers. Kafka's Serializer interface does not extend
// Serializable, so without this the job cannot be submitted at all. The only
// field is an ObjectMapper, which Jackson already makes Serializable.
public final class PredictionSerializer implements Serializer<Prediction>, Serializable {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public byte[] serialize(String topic, Prediction prediction) {
        try {
            // Field order mirrors contracts/stream/prediction-v1.json so a human
            // diffing a Kafka message against the contract reads top to bottom.
            ObjectNode node = objectMapper.createObjectNode();
            node.put("predictionId", prediction.predictionId());
            node.put("eventId", prediction.eventId());
            node.put("eventTime", prediction.eventTime().toString());
            node.put("modelName", prediction.modelName());
            node.put("modelVersion", prediction.modelVersion());
            node.put("modelSha", prediction.modelSha());
            node.put("schemaId", prediction.schemaId());
            node.put("schemaHash", prediction.schemaHash());

            // score/threshold are float32 per the contract; put(float) keeps
            // them as a Jackson FloatNode instead of widening to double.
            node.put("score", prediction.score());
            node.put("decision", prediction.decision());
            node.put("threshold", prediction.threshold());

            node.put("inferenceMicros", prediction.inferenceMicros());
            node.put("qualityFlags", prediction.qualityFlags());

            // Emission time of this prediction, ISO-8601 UTC.
            node.put("producedAt", prediction.producedAt().toString());

            return objectMapper.writeValueAsBytes(node);
        } catch (Exception e) {
            throw new RuntimeException("failed to serialize Prediction for topic " + topic, e);
        }
    }
}
