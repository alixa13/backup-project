package io.netsecml.platform.adapter.kafka.sink;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.netsecml.platform.domain.inference.ModbusDetectorPrediction;
import org.apache.kafka.common.serialization.Serializer;

import java.io.Serializable;

// contracts/stream/modbus-detector-prediction-v1.json, field for field. The
// two scores are always written -- as JSON null when the detector did not run
// -- so every message carries the same keys.
public final class ModbusDetectorPredictionSerializer implements Serializer<ModbusDetectorPrediction>, Serializable {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public byte[] serialize(String topic, ModbusDetectorPrediction p) {
        try {
            ObjectNode node = objectMapper.createObjectNode();
            // Which event this prediction is for, and where it came from.
            node.put("predictionId", p.predictionId());
            node.put("eventId", p.eventId());
            node.put("eventTime", p.eventTime().toString());
            node.put("sensor", p.sensor().value());
            node.put("connectionUid", p.connectionUid());
            node.put("clientIp", p.clientIp());
            node.put("serverIp", p.serverIp());
            node.put("unitId", p.unitId());
            // The bundle and the feature schema that produced it.
            node.put("modelName", p.modelName());
            node.put("modelVersion", p.modelVersion());
            node.put("modelSha", p.modelSha());
            node.put("schemaId", p.schemaId());
            node.put("schemaHash", p.schemaHash());
            // The verdict and the evidence behind it; a score is null when the detector did not run.
            node.put("verdict", p.verdict().name());
            node.put("denseScore", p.denseScore());
            node.put("temporalScore", p.temporalScore());
            node.put("denseThreshold", p.denseThreshold());
            node.put("temporalThreshold", p.temporalThreshold());
            node.put("trigger", p.trigger().name());
            node.put("windowEvents", p.windowEvents());
            node.put("qualityFlags", p.qualityFlags());
            // Cost and provenance.
            node.put("inferenceMicros", p.inferenceMicros());
            node.put("producedAt", p.producedAt().toString());
            return objectMapper.writeValueAsBytes(node);
        } catch (Exception e) {
            throw new RuntimeException("failed to serialize ModbusDetectorPrediction for topic " + topic, e);
        }
    }
}
