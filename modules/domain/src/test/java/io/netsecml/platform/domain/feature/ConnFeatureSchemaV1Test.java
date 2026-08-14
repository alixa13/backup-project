package io.netsecml.platform.domain.feature;

import org.junit.jupiter.api.Test;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import static org.junit.jupiter.api.Assertions.*;

class ConnFeatureSchemaV1Test {
    @Test
    void hasTwentyContiguousUniquelyNamedFeatures() {
        FeatureSchema schema = ConnFeatureSchemaV1.SCHEMA;
        assertEquals(20, schema.featureCount());
        for (int i = 0; i < 20; i++) {
            assertEquals(i, schema.definitions().get(i).index(), "index " + i + " out of order");
        }
        long uniqueNames = schema.definitions().stream().map(FeatureDefinition::name).distinct().count();
        assertEquals(20, uniqueNames, "feature names must be unique");
    }

    @Test
    void contentHashMatchesCommittedContractFile() throws IOException, NoSuchAlgorithmException {
        Path contractPath = Paths.get("..", "..", "contracts", "features", "conn-feature-schema-v1.json");
        byte[] bytes = Files.readAllBytes(contractPath);
        byte[] digest = MessageDigest.getInstance("SHA-256").digest(bytes);
        StringBuilder hex = new StringBuilder();
        for (byte b : digest) {
            hex.append(String.format("%02x", b));
        }
        assertEquals(ConnFeatureSchemaV1.CONTENT_HASH, hex.toString(),
            "ConnFeatureSchemaV1.CONTENT_HASH must match the SHA-256 of contracts/features/conn-feature-schema-v1.json — "
            + "if you edited the JSON, recompute the hash and update the constant");
    }
}
