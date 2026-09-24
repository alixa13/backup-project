package io.netsecml.platform.domain.feature;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;

// Pins s7comm-feature-v1 against upstream's frozen feature order and against
// its own committed contract file -- two statements of one fact that must not
// drift, exactly as ModbusFeatureSchemaV1Test does for modbus.
class S7commFeatureSchemaV1Test {

    // Upstream's STAGE1_RAW_FEATURES, transcribed with provenance.
    private static final Path UPSTREAM =
        Path.of("..", "..", "tests", "fixtures", "contracts", "s7comm_stage1_raw_features_v1.json");

    private static final Path OURS =
        Path.of("..", "..", "contracts", "features", "s7comm-feature-schema-v1.json");

    private static List<String> schemaNames() {
        return S7commFeatureSchemaV1.SCHEMA.definitions().stream().map(FeatureDefinition::name).toList();
    }

    @Test
    void theSchemaIsExactlyUpstreamsFrozenFeatureOrder() throws Exception {
        // A vector in any other order cannot be scored by either frozen model.
        JsonNode upstream = new ObjectMapper().readTree(UPSTREAM.toFile());
        List<String> expected = new ArrayList<>();
        upstream.get("feature_order").forEach(n -> expected.add(n.asText()));
        assertEquals(16, expected.size(), "upstream freezes 16 raw features");
        assertEquals(expected, schemaNames());
    }

    @Test
    void theCommittedContractFileMatchesTheSchema() throws Exception {
        // Names, order and missing policies: the JSON is what a model author
        // reads, the Java is what the pipeline emits.
        JsonNode ours = new ObjectMapper().readTree(OURS.toFile());
        assertEquals(16, ours.get("featureCount").asInt());
        List<String> names = new ArrayList<>();
        List<String> policies = new ArrayList<>();
        ours.get("features").forEach(f -> {
            names.add(f.get("name").asText());
            policies.add(f.get("missingPolicy").asText());
        });
        assertEquals(schemaNames(), names);
        assertEquals(S7commFeatureSchemaV1.SCHEMA.definitions().stream()
            .map(d -> d.missingPolicy().name()).toList(), policies);
    }

    @Test
    void theContractFilesFunctionNamesAreTheCodecsOwn() throws Exception {
        // The decode table a scorer's author reads must be the one the code uses.
        JsonNode names = new ObjectMapper().readTree(OURS.toFile())
            .get("categoricalCodes").get("s7_operation").get("functionNames");
        Map<Integer, String> inFile = new TreeMap<>();
        names.fields().forEachRemaining(e -> inFile.put(Integer.parseInt(e.getKey()), e.getValue().asText()));
        assertEquals(new TreeMap<>(S7commCategories.functionNames()), inFile);
    }

    @Test
    void contentHashMatchesTheCommittedContractFile() throws Exception {
        byte[] digest = MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(OURS));
        StringBuilder hex = new StringBuilder();
        for (byte b : digest) {
            hex.append(String.format("%02x", b));
        }
        assertEquals(S7commFeatureSchemaV1.CONTENT_HASH, hex.toString(),
            "CONTENT_HASH must be the SHA-256 of contracts/features/s7comm-feature-schema-v1.json");
    }

    @Test
    void indicesAreZeroBasedAndDenseAndNamesUnique() {
        for (int i = 0; i < 16; i++) {
            assertEquals(i, S7commFeatureSchemaV1.SCHEMA.definitions().get(i).index());
        }
        assertEquals(16, schemaNames().stream().distinct().count());
    }

    @Test
    void missingPoliciesFollowTheContractRule() {
        // Direction is required to score a record at all; the two categorical
        // codes carry absence as a sentinel; everything else is 0 until there
        // is history to compute it from.
        for (FeatureDefinition d : S7commFeatureSchemaV1.SCHEMA.definitions()) {
            FeatureDefinition.MissingPolicy expected = switch (d.name()) {
                case "is_request_direction" -> FeatureDefinition.MissingPolicy.REQUIRED;
                case "s7_rosctr", "s7_operation" -> FeatureDefinition.MissingPolicy.SENTINEL;
                default -> FeatureDefinition.MissingPolicy.DEFAULT_ZERO;
            };
            assertEquals(expected, d.missingPolicy(), d.name());
        }
    }
}
