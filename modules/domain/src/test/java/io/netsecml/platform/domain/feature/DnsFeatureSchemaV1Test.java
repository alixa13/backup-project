package io.netsecml.platform.domain.feature;

import org.junit.jupiter.api.Test;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import static org.junit.jupiter.api.Assertions.*;

class DnsFeatureSchemaV1Test {
    @Test
    void hasTwentyFourContiguousUniquelyNamedFeatures() {
        FeatureSchema schema = DnsFeatureSchemaV1.SCHEMA;
        assertEquals(24, schema.featureCount());
        for (int i = 0; i < 24; i++) {
            assertEquals(i, schema.definitions().get(i).index(), "index " + i + " out of order");
        }
        long uniqueNames = schema.definitions().stream().map(FeatureDefinition::name).distinct().count();
        assertEquals(24, uniqueNames, "feature names must be unique");
    }

    @Test
    void contentHashMatchesCommittedContractFile() throws IOException, NoSuchAlgorithmException {
        Path contractPath = Paths.get("..", "..", "contracts", "features", "dns-feature-schema-v1.json");
        byte[] bytes = Files.readAllBytes(contractPath);
        byte[] digest = MessageDigest.getInstance("SHA-256").digest(bytes);
        StringBuilder hex = new StringBuilder();
        for (byte b : digest) {
            hex.append(String.format("%02x", b));
        }
        assertEquals(DnsFeatureSchemaV1.CONTENT_HASH, hex.toString(),
            "DnsFeatureSchemaV1.CONTENT_HASH must match the SHA-256 of contracts/features/dns-feature-schema-v1.json — "
            + "if you edited the JSON, recompute the hash and update the constant");
    }

    // The common tier leads every per-protocol schema, so DNS's first twelve
    // features must be the tier's twelve, in the tier's order. A schema that
    // reorders them would still hash consistently against its own file and still
    // load -- this is the only check that catches it.
    @Test
    void theFirstTwelveFeaturesAreTheCommonTierInOrder() {
        for (int i = 0; i < CommonFeatureTierV1.FEATURE_COUNT; i++) {
            assertEquals(CommonFeatureTierV1.FEATURE_NAMES.get(i),
                DnsFeatureSchemaV1.SCHEMA.definitions().get(i).name(),
                "common tier index " + i + " must lead the DNS schema unchanged");
        }
    }
}
