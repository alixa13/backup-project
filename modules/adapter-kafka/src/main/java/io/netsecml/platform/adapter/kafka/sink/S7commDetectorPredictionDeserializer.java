package io.netsecml.platform.adapter.kafka.sink;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netsecml.platform.domain.event.SensorId;
import io.netsecml.platform.domain.inference.DetectorVerdict;
import io.netsecml.platform.domain.inference.S7commDetectorPrediction;
import io.netsecml.platform.domain.inference.S7commScoreGroup;
import org.apache.kafka.common.serialization.Deserializer;

import java.time.Instant;

// The archive job's side of contracts/stream/s7comm-detector-prediction-v1.json.
public final class S7commDetectorPredictionDeserializer implements Deserializer<S7commDetectorPrediction> {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public S7commDetectorPrediction deserialize(String topic, byte[] data) {
        try {
            JsonNode n = objectMapper.readTree(data);
            return new S7commDetectorPrediction(
                n.get("predictionId").asText(), n.get("eventId").asText(),
                Instant.parse(n.get("eventTime").asText()), new SensorId(n.get("sensor").asText()),
                n.get("connectionUid").asText(), n.get("clientIp").asText(), n.get("serverIp").asText(),
                n.get("modelName").asText(), n.get("modelVersion").asText(), n.get("modelSha").asText(),
                n.get("schemaId").asText(), n.get("schemaHash").asText(),
                DetectorVerdict.valueOf(n.get("verdict").asText()),
                nullableFloat(n.get("score")), nullableDouble(n.get("pValue")),
                S7commScoreGroup.valueOf(n.get("scoreGroup").asText()), n.get("alpha").asDouble(),
                n.get("eventsSinceReset").asLong(), n.get("qualityFlags").asInt(),
                n.get("inferenceMicros").asLong(), Instant.parse(n.get("producedAt").asText()));
        } catch (Exception e) {
            throw new IllegalArgumentException("failed to deserialize S7commDetectorPrediction from topic " + topic, e);
        }
    }

    // A score or p-value is null when the detector did not judge the event.
    private static Float nullableFloat(JsonNode node) {
        return node == null || node.isNull() ? null : (float) node.asDouble();
    }

    private static Double nullableDouble(JsonNode node) {
        return node == null || node.isNull() ? null : node.asDouble();
    }
}
