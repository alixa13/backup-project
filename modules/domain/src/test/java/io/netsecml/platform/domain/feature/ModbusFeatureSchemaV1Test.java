package io.netsecml.platform.domain.feature;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import static org.junit.jupiter.api.Assertions.assertEquals;

class ModbusFeatureSchemaV1Test {

    // The upstream frozen contract, committed to this repo so the parity claim
    // survives the working copy of the model project going away.
    private static final Path UPSTREAM =
        Path.of("..", "..", "tests", "fixtures", "contracts", "modbus_feature_contract_v1.json");

    private static final Path OURS =
        Path.of("..", "..", "contracts", "features", "modbus-feature-schema-v1.json");

    @Test
    void theSchemaIsExactlyTheUpstreamContractsFeatureOrder() throws Exception {
        // This is the whole point of the unit: a vector whose order differs from
        // the frozen contract cannot be scored by the frozen model at all.
        JsonNode upstream = new ObjectMapper().readTree(UPSTREAM.toFile());
        List<String> expected = new ArrayList<>();
        upstream.get("feature_order").forEach(n -> expected.add(n.asText()));

        assertEquals(42, expected.size(), "the upstream contract must carry 42 features");
        assertEquals(expected, ModbusFeatureSchemaV1.SCHEMA.definitions().stream()
            .map(FeatureDefinition::name).toList());
    }

    @Test
    void theCommittedContractFileMatchesTheRegisteredSchema() throws Exception {
        // The JSON is what a model author reads; the Java is what the pipeline
        // emits. They are two statements of one fact and must not drift.
        JsonNode ours = new ObjectMapper().readTree(OURS.toFile());
        List<String> inFile = new ArrayList<>();
        ours.get("features").forEach(f -> inFile.add(f.get("name").asText()));

        assertEquals(42, ours.get("featureCount").asInt());
        assertEquals(inFile, ModbusFeatureSchemaV1.SCHEMA.definitions().stream()
            .map(FeatureDefinition::name).toList());
    }

    @Test
    void theSchemaCarriesNoCommonTier() throws Exception {
        // modbus-feature-v1 deliberately does NOT lead with the common tier, unlike
        // dns-feature-v1: it mirrors an externally frozen contract, so it carries
        // exactly what that contract specifies. See CLAUDE.md's invariant.
        List<String> names = ModbusFeatureSchemaV1.SCHEMA.definitions().stream()
            .map(FeatureDefinition::name).toList();
        assertEquals("is_response", names.get(0));
        assertEquals("write_ratio_10s", names.get(41));
    }

    @Test
    void indicesAreZeroBasedAndDense() {
        for (int i = 0; i < 42; i++) {
            assertEquals(i, ModbusFeatureSchemaV1.SCHEMA.definitions().get(i).index());
        }
    }

    @Test
    void everyFeatureIsDefaultZero() {
        // The brief's stated ruling: none of the upstream contract's missing_rule
        // values describe a rejection, so REQUIRED must never appear here -- a
        // single REQUIRED entry would silently reintroduce a throw path the
        // upstream contract does not have.
        for (FeatureDefinition definition : ModbusFeatureSchemaV1.SCHEMA.definitions()) {
            assertEquals(FeatureDefinition.MissingPolicy.DEFAULT_ZERO, definition.missingPolicy(),
                definition.name() + " must be DEFAULT_ZERO");
        }
    }

    @Test
    void featureNamesAreUnique() {
        List<String> names = ModbusFeatureSchemaV1.SCHEMA.definitions().stream()
            .map(FeatureDefinition::name).toList();
        assertEquals(42, names.stream().distinct().count(), "feature names must be unique");
    }

    @Test
    void contentHashMatchesCommittedContractFile() throws Exception {
        // Mirrors DnsFeatureSchemaV1Test's own hash guard: the constant must track
        // the file it was computed from, or a silent edit to the JSON goes unnoticed.
        byte[] bytes = java.nio.file.Files.readAllBytes(OURS);
        byte[] digest = java.security.MessageDigest.getInstance("SHA-256").digest(bytes);
        StringBuilder hex = new StringBuilder();
        for (byte b : digest) {
            hex.append(String.format("%02x", b));
        }
        assertEquals(ModbusFeatureSchemaV1.CONTENT_HASH, hex.toString(),
            "ModbusFeatureSchemaV1.CONTENT_HASH must match the SHA-256 of "
            + "contracts/features/modbus-feature-schema-v1.json -- if you edited the JSON, "
            + "recompute the hash and update the constant");
    }
}
