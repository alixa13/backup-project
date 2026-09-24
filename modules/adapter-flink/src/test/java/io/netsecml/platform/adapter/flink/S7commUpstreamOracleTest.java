package io.netsecml.platform.adapter.flink;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netsecml.platform.adapter.kafka.dto.ZeekS7commRecord;
import io.netsecml.platform.adapter.kafka.mapper.S7commEventMapper;
import io.netsecml.platform.adapter.kafka.parser.JsonZeekS7commParser;
import io.netsecml.platform.application.usecase.S7commBuildFeaturesUseCase;
import io.netsecml.platform.domain.event.ConnEvent;
import io.netsecml.platform.domain.event.DnsEvent;
import io.netsecml.platform.domain.event.MappingResult;
import io.netsecml.platform.domain.event.ModbusEvent;
import io.netsecml.platform.domain.event.NetworkEvent;
import io.netsecml.platform.domain.event.S7commEvent;
import io.netsecml.platform.domain.event.SensorId;
import io.netsecml.platform.domain.feature.FeatureDefinition;
import io.netsecml.platform.domain.feature.S7commCategories;
import io.netsecml.platform.domain.feature.S7commConnectionState;
import io.netsecml.platform.domain.feature.S7commFeatureSchemaV1;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

// The S7comm parity proof. tests/fixtures/s7comm/upstream_oracle_v1.jsonl was
// produced by running the model team's own frozen builders
// (tests/fixtures/s7comm/generate_upstream_oracle.py) over seeded synthetic
// streams. Every raw record goes through the REAL parser, mapper and use
// case, with one connection state per uid threaded exactly as
// S7commFeatureProcessFunction threads it; all 14 numeric values must equal
// upstream's value rounded to float32 bit for bit, and the two categorical
// codes must decode to upstream's exact strings -- except a code-less
// function_name that is not one of FUNCTION_NAMES, which must be the unseen
// code (docs/superpowers/specs/2026-09-24-s7comm-stage1-design.md, ruling R6).
class S7commUpstreamOracleTest {
    private static final Path FIXTURE =
        Path.of("..", "..", "tests", "fixtures", "s7comm", "upstream_oracle_v1.jsonl");

    // The upstream files the spec was written against (its section 1): the
    // fixture must have been generated from exactly these.
    private static final Map<String, String> UPSTREAM_SHA256 = Map.of(
        "events.py", "60413956423e0ffa770e0fdaea8c3529d58068e6e9b45cdf9a18d3abbb7c72ae",
        "s7_parser.py", "8ef6bbdead97a4207d8cc74b98494fa4dd87dd85b3d572514cbe0aa6cda377fe",
        "customer_icsnpp_enriched_builder.py", "3aa48d969095e45c0084f713f8a34c9a8092674dc5f0cad9cec2998c3ffb3c99",
        "customer_icsnpp_time_normalized_builder.py",
        "65a03719c1986705d2819ebbd27d352cd66f7fb6ba8dc06115cbcc06180d7fb4",
        "kafka_source.py", "bedae0d6427a86dac97b1a0af1c3bbcfbc3301ac8741575590dc28a331d39aca");

    private static final SensorId SENSOR = new SensorId("sensor-eu-1");
    private static final ObjectMapper JSON = new ObjectMapper();

    private final JsonZeekS7commParser parser = new JsonZeekS7commParser();
    private final S7commEventMapper mapper = new S7commEventMapper();
    private final S7commBuildFeaturesUseCase useCase =
        new S7commBuildFeaturesUseCase(Clock.fixed(Instant.parse("2026-09-24T00:00:00Z"), ZoneOffset.UTC));

    private static List<String> lines() throws Exception {
        return Files.readAllLines(FIXTURE, StandardCharsets.UTF_8);
    }

    @Test
    void theFixtureWasGeneratedFromTheSpecifiedUpstreamCodeInTheSchemasOrder() throws Exception {
        JsonNode meta = JSON.readTree(lines().get(0)).get("meta");
        Map<String, String> hashes = new HashMap<>();
        meta.get("upstream_sha256").fields().forEachRemaining(e -> hashes.put(e.getKey(), e.getValue().asText()));
        assertEquals(UPSTREAM_SHA256, hashes);

        List<String> features = new ArrayList<>();
        meta.get("features").forEach(f -> features.add(f.asText()));
        assertEquals(S7commFeatureSchemaV1.SCHEMA.definitions().stream().map(FeatureDefinition::name).toList(),
            features);
    }

    @Test
    void everyRecordMatchesUpstreamBitForBit() throws Exception {
        List<String> lines = lines();
        Map<String, S7commConnectionState> states = new HashMap<>();
        List<String> failures = new ArrayList<>();
        int records = 0;
        for (String line : lines.subList(1, lines.size())) {
            records++;
            JsonNode expected = JSON.readTree(line);
            S7commEvent event = parseAndMap(expected.get("raw").asText());

            // One state per uid, threaded exactly as the Flink operator does.
            S7commConnectionState state = states.computeIfAbsent(event.connectionUid(),
                uid -> S7commConnectionState.empty());
            float[] actual = useCase.build(event, state).vector().values();

            // The 14 numeric features: upstream's float64 rounded to float32.
            for (int f = 0; f < 14; f++) {
                float want = (float) expected.get("values").get(f).doubleValue();
                if (Float.floatToRawIntBits(want) != Float.floatToRawIntBits(actual[f])) {
                    failures.add("record " + records + " (" + expected.get("stream").asText() + "), "
                        + S7commFeatureSchemaV1.SCHEMA.definitions().get(f).name() + ": upstream " + want
                        + ", ours " + actual[f]);
                }
            }

            // The two categorical codes, decoded.
            String rosctr = S7commCategories.decodeRosctr((int) actual[14]);
            if (!rosctr.equals(expected.get("rosctr").asText())) {
                failures.add("record " + records + ", s7_rosctr: upstream " + expected.get("rosctr").asText()
                    + ", ours " + rosctr);
            }
            String upstreamOperation = expected.get("operation").asText();
            int operationCode = (int) actual[15];
            boolean carriable = upstreamOperation.equals(S7commCategories.MISSING_CATEGORY)
                || S7commCategories.functionNames().containsValue(upstreamOperation)
                || upstreamOperation.matches("FUNCTION_0x[0-9A-F]{2,}");
            boolean operationMatches = carriable
                ? S7commCategories.decodeOperation(operationCode).equals(upstreamOperation)
                : operationCode == S7commCategories.UNSEEN_NAME;
            if (!operationMatches) {
                failures.add("record " + records + ", s7_operation: upstream " + upstreamOperation + ", ours code "
                    + operationCode);
            }
        }
        assertEquals(2121, records, "the fixture's record count");
        assertTrue(failures.isEmpty(), failures.size() + " mismatches, first: "
            + failures.subList(0, Math.min(20, failures.size())));
    }

    // The real parser, then the real mapper, narrowed with one explicit arm
    // per permitted event type.
    private S7commEvent parseAndMap(String raw) {
        MappingResult<ZeekS7commRecord> parsed = parser.parse(raw.getBytes(StandardCharsets.UTF_8));
        assertTrue(parsed.isValid(), () -> "parser rejected " + raw + ": " + parsed);
        MappingResult<NetworkEvent> mapped = mapper.map(parsed.value(), SENSOR);
        assertTrue(mapped.isValid(), () -> "mapper rejected " + raw + ": " + mapped);
        return switch (mapped.value()) {
            case S7commEvent s7 -> s7;
            case ConnEvent c -> throw new AssertionError("S7commEventMapper produced a ConnEvent");
            case DnsEvent d -> throw new AssertionError("S7commEventMapper produced a DnsEvent");
            case ModbusEvent m -> throw new AssertionError("S7commEventMapper produced a ModbusEvent");
        };
    }
}
