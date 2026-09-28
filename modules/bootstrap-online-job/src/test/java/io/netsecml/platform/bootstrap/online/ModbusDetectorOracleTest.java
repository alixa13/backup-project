package io.netsecml.platform.bootstrap.online;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netsecml.platform.adapter.kafka.mapper.ModbusEventMapper;
import io.netsecml.platform.adapter.kafka.parser.JsonZeekModbusParser;
import io.netsecml.platform.application.usecase.ModbusBuildFeaturesUseCase;
import io.netsecml.platform.application.usecase.ModbusScoringResult;
import io.netsecml.platform.application.usecase.ScoreModbusSequenceUseCase;
import io.netsecml.platform.domain.event.ModbusEvent;
import io.netsecml.platform.domain.event.SensorId;
import io.netsecml.platform.domain.feature.FeatureBuildResult;
import io.netsecml.platform.domain.feature.FeatureVector;
import io.netsecml.platform.domain.feature.ModbusEntityKey;
import io.netsecml.platform.domain.feature.ModbusEntityState;
import io.netsecml.platform.domain.inference.ModbusDetectorPrediction;
import io.netsecml.platform.domain.inference.ModbusScoreWindow;
import io.netsecml.platform.port.out.SequenceScorer;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

// The Modbus scoring proof (spec section 10): every event of the upstream-
// generated oracle, as Zeek v1.0.0 writes it, through this platform's real
// parser, mapper, feature engine, preprocessing and Java ONNX scorer. The
// vectors and preprocessed values equal upstream's exactly, and both scores
// to 1e-5.
class ModbusDetectorOracleTest {

    private static final Path ORACLE = Path.of("..", "..", "tests", "fixtures", "modbus", "detector_oracle_v1.jsonl");
    private static final String BUNDLE = Path.of("..", "..", "tests", "fixtures", "models",
        "modbus-stage1-detector", "v1").toString();

    @Test
    void everyEventMatchesUpstream() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        List<String> lines = Files.readAllLines(ORACLE);
        JsonZeekModbusParser parser = new JsonZeekModbusParser();
        ModbusEventMapper eventMapper = new ModbusEventMapper();
        SensorId sensor = new SensorId("sensor-oracle");
        ModbusBuildFeaturesUseCase features = new ModbusBuildFeaturesUseCase();
        Map<ModbusEntityKey, ModbusEntityState> states = new HashMap<>();
        Map<ModbusEntityKey, ModbusScoreWindow> windows = new HashMap<>();
        int scored = 0;
        try (SequenceScorer scorer = new ModbusDetectorScorerFactory(BUNDLE).create()) {
            ScoreModbusSequenceUseCase scoring = new ScoreModbusSequenceUseCase(scorer, Clock.systemUTC());
            for (int i = 0; i < lines.size(); i++) {
                JsonNode expected = mapper.readTree(lines.get(i));
                // The real parse -> map -> features path, per stream.
                ModbusEvent event = (ModbusEvent) eventMapper.map(parser.parse(
                    expected.get("raw").toString().getBytes(java.nio.charset.StandardCharsets.UTF_8)).value(),
                    sensor).value();
                ModbusEntityKey key = ModbusEntityKey.of(event);
                ModbusEntityState state = states.computeIfAbsent(key, k -> ModbusEntityState.empty());
                FeatureBuildResult<ModbusEntityState> built = features.build(event, state);
                // Kept as the process function keeps it: the state the build returns.
                states.put(key, built.newState());
                FeatureVector vector = built.vector();
                for (int j = 0; j < 42; j++) {
                    assertEquals((float) expected.get("vector").get(j).asDouble(), vector.values()[j],
                        "event " + i + " feature " + j);
                }
                // The preprocessing, against upstream's own arithmetic.
                float[] preprocessed = scorer.bundle().preprocessing().apply(vector.values());
                for (int j = 0; j < 42; j++) {
                    assertEquals((float) expected.get("preprocessed").get(j).asDouble(), preprocessed[j], 1e-6f,
                        "event " + i + " preprocessed " + j);
                }
                // Then scoring, per stream.
                ModbusScoringResult result = scoring.score(key, vector, windows.get(key));
                windows.put(key, result.window());
                ModbusDetectorPrediction p = result.prediction();
                assertEquals(expected.get("verdict").asText(), p.verdict().name(), "event " + i);
                if (!expected.get("dense").isNull()) {
                    scored++;
                    assertEquals(expected.get("dense").asDouble(), p.denseScore(), 1e-5, "event " + i + " dense");
                    assertEquals(expected.get("temporal").asDouble(), p.temporalScore(), 1e-5,
                        "event " + i + " temporal");
                }
            }
        }
        assertEquals(lines.stream().filter(l -> !l.contains("\"dense\":null")).count(), scored);
    }
}
