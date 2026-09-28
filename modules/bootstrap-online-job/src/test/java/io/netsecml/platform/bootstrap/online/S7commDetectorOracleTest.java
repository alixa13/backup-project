package io.netsecml.platform.bootstrap.online;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netsecml.platform.adapter.kafka.dto.ZeekS7commRecord;
import io.netsecml.platform.adapter.kafka.mapper.S7commEventMapper;
import io.netsecml.platform.adapter.kafka.parser.JsonZeekS7commParser;
import io.netsecml.platform.application.usecase.S7commBuildFeaturesUseCase;
import io.netsecml.platform.application.usecase.S7commScoringResult;
import io.netsecml.platform.application.usecase.ScoreS7commSequenceUseCase;
import io.netsecml.platform.domain.event.MappingResult;
import io.netsecml.platform.domain.event.NetworkEvent;
import io.netsecml.platform.domain.event.S7commEvent;
import io.netsecml.platform.domain.event.SensorId;
import io.netsecml.platform.domain.feature.FeatureBuildResult;
import io.netsecml.platform.domain.feature.S7commCategories;
import io.netsecml.platform.domain.feature.S7commConnectionState;
import io.netsecml.platform.domain.inference.S7commDetectorPrediction;
import io.netsecml.platform.domain.inference.S7commScoreWindow;
import io.netsecml.platform.port.out.ReconstructionScorer;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

// The S7comm scoring proof (docs/superpowers/specs/2026-09-28-s7comm-stage1-scoring-design.md
// section 10, amendment A1): every event of the oracle -- upstream's feature
// builder and detector v2's released files in Python -- as ICSNPP writes it, goes
// through this platform's real parser, mapper, feature use case, preprocessing,
// Java ONNX scorer and 64-event warm-up, with state and windows per uid threaded
// as the Flink operators thread them. Vectors must equal upstream's bit for
// bit, and preprocessed values to 1e-6. Verdicts, groups, alphas and counts
// must match exactly; scores to 1e-9 + 1e-4 relative, and p-values within the
// band that tolerance allows (plan ruling P3).
class S7commDetectorOracleTest {

    private static final Path ORACLE = Path.of("..", "..", "tests", "fixtures", "s7comm", "detector_oracle_v1.jsonl");
    private static final String BUNDLE = Path.of("..", "..", "tests", "fixtures", "models",
        "s7comm-stage1-detector", "v2").toString();
    private static final SensorId SENSOR = new SensorId("sensor-oracle");

    @Test
    void everyEventMatchesUpstream() throws Exception {
        ObjectMapper json = new ObjectMapper();
        List<String> lines = Files.readAllLines(ORACLE, StandardCharsets.UTF_8);
        JsonZeekS7commParser parser = new JsonZeekS7commParser();
        S7commEventMapper mapper = new S7commEventMapper();
        S7commBuildFeaturesUseCase features = new S7commBuildFeaturesUseCase(Clock.systemUTC());
        Map<String, S7commConnectionState> states = new HashMap<>();
        Map<String, S7commScoreWindow> windows = new HashMap<>();
        Map<String, Integer> verdicts = new HashMap<>();
        try (ReconstructionScorer scorer = new S7commDetectorScorerFactory(BUNDLE).create()) {
            ScoreS7commSequenceUseCase scoring = new ScoreS7commSequenceUseCase(scorer, Clock.systemUTC());
            // Line 0 is the generator's meta.
            for (int i = 1; i < lines.size(); i++) {
                JsonNode expected = json.readTree(lines.get(i));
                String at = "event " + i + " (" + expected.get("stream").asText() + ")";
                S7commEvent event = parseAndMap(parser, mapper, expected.get("raw").asText());
                String uid = event.connectionUid();
                // A TTL expiry: the feature state and the window start empty.
                if (expected.get("reset_before").asBoolean()) {
                    states.remove(uid);
                    windows.remove(uid);
                }
                // As S7commFeatureProcessFunction does: absent state is a fresh connection.
                S7commConnectionState state = states.get(uid);
                boolean fresh = state == null;
                FeatureBuildResult<S7commConnectionState> built =
                    features.build(event, fresh ? S7commConnectionState.empty() : state);
                states.put(uid, built.newState());
                float[] v = built.vector().values();

                // The 14 numeric features bit for bit; the two codes decoded.
                for (int f = 0; f < 14; f++) {
                    assertEquals((float) expected.get("values").get(f).asDouble(), v[f], 0f, at + " feature " + f);
                }
                assertEquals(expected.get("rosctr").asText(), S7commCategories.decodeRosctr((int) v[14]), at);
                assertEquals(expected.get("operation").asText(), S7commCategories.decodeOperation((int) v[15]), at);

                // The preprocessing, against v2's fitted preprocessor.
                float[] x = scorer.bundle().preprocessing().apply(v);
                for (int f = 0; f < x.length; f++) {
                    assertEquals((float) expected.get("preprocessed").get(f).asDouble(), x[f], 1e-6f,
                        at + " column " + f);
                }

                // Scoring, per uid, with the endpoints s7comm-features hands over.
                String client = event.isRequest() ? event.sourceIp() : event.destinationIp();
                String server = event.isRequest() ? event.destinationIp() : event.sourceIp();
                S7commScoringResult result = scoring.score(built.vector(), fresh, client, server, windows.get(uid));
                windows.put(uid, result.window());
                S7commDetectorPrediction p = result.prediction();
                assertEquals(expected.get("verdict").asText(), p.verdict().name(), at);
                assertEquals(expected.get("group").asText(), p.scoreGroup().name(), at);
                assertEquals(expected.get("alpha").asDouble(), p.alpha(), 0.0, at);
                assertEquals(expected.get("events_since_reset").asLong(), p.eventsSinceReset(), at);
                if (p.verdict().scored()) {
                    double want = expected.get("score").asDouble();
                    assertEquals(want, p.score(), 1e-9 + 1e-4 * Math.abs(want), at + " score");
                    double low = expected.get("p_low").asDouble();
                    double high = expected.get("p_high").asDouble();
                    assertTrue(p.pValue() >= low && p.pValue() <= high,
                        at + " p " + p.pValue() + " outside [" + low + ", " + high + "]");
                }
                verdicts.merge(p.verdict().name(), 1, Integer::sum);
            }
        }
        // The generator enforced per-group coverage; this pins that the fixture still has it.
        assertTrue(verdicts.getOrDefault("NORMAL", 0) > 0 && verdicts.getOrDefault("ANOMALY", 0) > 0
            && verdicts.getOrDefault("WARMUP", 0) > 0, "verdicts " + verdicts);
    }

    // The real parser, then the real mapper; the oracle holds only S7comm records.
    private static S7commEvent parseAndMap(JsonZeekS7commParser parser, S7commEventMapper mapper, String raw) {
        MappingResult<ZeekS7commRecord> parsed = parser.parse(raw.getBytes(StandardCharsets.UTF_8));
        assertTrue(parsed.isValid(), () -> "parser rejected " + raw);
        MappingResult<NetworkEvent> mapped = mapper.map(parsed.value(), SENSOR);
        assertTrue(mapped.isValid(), () -> "mapper rejected " + raw);
        return (S7commEvent) mapped.value();
    }
}
