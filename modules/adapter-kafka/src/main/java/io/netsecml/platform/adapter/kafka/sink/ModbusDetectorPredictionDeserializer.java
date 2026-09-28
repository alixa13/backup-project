package io.netsecml.platform.adapter.kafka.sink;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netsecml.platform.domain.event.SensorId;
import io.netsecml.platform.domain.inference.DetectorTrigger;
import io.netsecml.platform.domain.inference.DetectorVerdict;
import io.netsecml.platform.domain.inference.ModbusDetectorPrediction;
import org.apache.kafka.common.serialization.Deserializer;

import java.time.Instant;

// The archive job's side of contracts/stream/modbus-detector-prediction-v1.json.
public final class ModbusDetectorPredictionDeserializer implements Deserializer<ModbusDetectorPrediction> {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public ModbusDetectorPrediction deserialize(String topic, byte[] data) {
        try {
            JsonNode n = objectMapper.readTree(data);
            return new ModbusDetectorPrediction(
                n.get("predictionId").asText(), n.get("eventId").asText(),
                Instant.parse(n.get("eventTime").asText()), new SensorId(n.get("sensor").asText()),
                n.get("connectionUid").asText(), n.get("clientIp").asText(), n.get("serverIp").asText(),
                n.get("unitId").asText(), n.get("modelName").asText(), n.get("modelVersion").asText(),
                n.get("modelSha").asText(), n.get("schemaId").asText(), n.get("schemaHash").asText(),
                DetectorVerdict.valueOf(n.get("verdict").asText()),
                nullableFloat(n.get("denseScore")), nullableFloat(n.get("temporalScore")),
                (float) n.get("denseThreshold").asDouble(), (float) n.get("temporalThreshold").asDouble(),
                DetectorTrigger.valueOf(n.get("trigger").asText()), n.get("windowEvents").asInt(),
                n.get("qualityFlags").asInt(), n.get("inferenceMicros").asLong(),
                Instant.parse(n.get("producedAt").asText()));
        } catch (Exception e) {
            throw new IllegalArgumentException("failed to deserialize ModbusDetectorPrediction from topic " + topic, e);
        }
    }

    // A score is null when the detector did not run.
    private static Float nullableFloat(JsonNode node) {
        return node == null || node.isNull() ? null : (float) node.asDouble();
    }
}
