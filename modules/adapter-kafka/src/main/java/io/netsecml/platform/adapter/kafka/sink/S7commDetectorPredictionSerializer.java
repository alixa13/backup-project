package io.netsecml.platform.adapter.kafka.sink;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.netsecml.platform.domain.inference.S7commDetectorPrediction;
import org.apache.kafka.common.serialization.Serializer;

import java.io.Serializable;

// contracts/stream/s7comm-detector-prediction-v1.json, field for field. The
// score and the p-value are always written -- as JSON null when the detector
// did not judge the event -- so every message carries the same keys.
public final class S7commDetectorPredictionSerializer implements Serializer<S7commDetectorPrediction>, Serializable {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public byte[] serialize(String topic, S7commDetectorPrediction p) {
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
            // The bundle and the feature schema that produced it.
            node.put("modelName", p.modelName());
            node.put("modelVersion", p.modelVersion());
            node.put("modelSha", p.modelSha());
            node.put("schemaId", p.schemaId());
            node.put("schemaHash", p.schemaHash());
            // The verdict and the evidence behind it; null when the detector did not judge.
            node.put("verdict", p.verdict().name());
            node.put("score", p.score());
            node.put("pValue", p.pValue());
            node.put("scoreGroup", p.scoreGroup().name());
            node.put("alpha", p.alpha());
            node.put("eventsSinceReset", p.eventsSinceReset());
            node.put("qualityFlags", p.qualityFlags());
            // Cost and provenance.
            node.put("inferenceMicros", p.inferenceMicros());
            node.put("producedAt", p.producedAt().toString());
            return objectMapper.writeValueAsBytes(node);
        } catch (Exception e) {
            throw new RuntimeException("failed to serialize S7commDetectorPrediction for topic " + topic, e);
        }
    }
}
