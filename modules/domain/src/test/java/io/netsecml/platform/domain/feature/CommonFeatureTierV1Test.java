package io.netsecml.platform.domain.feature;

import org.junit.jupiter.api.Test;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

// The common tier is shared by every per-protocol schema, so drift between the
// Java constant and the JSON contract would silently misalign the streaming side
// and the training side.
class CommonFeatureTierV1Test {

    // Order IS the contract: a feature's index is its position in the vector, so
    // reordering is a breaking change even though the set is unchanged.
    @Test
    void featureOrderIsFrozen() {
        assertEquals(List.of(
            "record_count_5m",
            "byte_sum_5m",
            "failed_count_5m",
            "inter_arrival_mean_ms",
            "inter_arrival_stddev_ms",
            "is_orig",
            "conn_orig_bytes",
            "conn_resp_bytes",
            "conn_orig_pkts",
            "conn_resp_pkts",
            "conn_age_seconds",
            "conn_enrichment_present"
        ), CommonFeatureTierV1.FEATURE_NAMES);
    }

    @Test
    void featureCountMatchesTheNameList() {
        assertEquals(12, CommonFeatureTierV1.FEATURE_COUNT);
        assertEquals(CommonFeatureTierV1.FEATURE_NAMES.size(), CommonFeatureTierV1.FEATURE_COUNT);
    }

    // The list is handed to callers that build vectors; an accidental mutation
    // would corrupt every schema that embeds this tier.
    @Test
    void featureNamesAreImmutable() {
        assertThrows(UnsupportedOperationException.class,
            () -> CommonFeatureTierV1.FEATURE_NAMES.add("injected"));
    }

    // Mirrors ConnFeatureSchemaV1Test's contentHashMatchesCommittedContractFile.
    // Only id, feature names and indices are checked by
    // CommonFeatureTierContractDriftTest in adapter-kafka -- description,
    // encoding and semanticVersion could previously drift silently. This closes
    // that gap without pulling Jackson into domain's test classpath.
    @Test
    void contentHashMatchesCommittedContractFile() throws IOException, NoSuchAlgorithmException {
        Path contractPath = Paths.get("..", "..", "contracts", "features", "common-feature-tier-v1.json");
        byte[] bytes = Files.readAllBytes(contractPath);
        byte[] digest = MessageDigest.getInstance("SHA-256").digest(bytes);
        StringBuilder hex = new StringBuilder();
        for (byte b : digest) {
            hex.append(String.format("%02x", b));
        }
        assertEquals(CommonFeatureTierV1.CONTENT_HASH, hex.toString(),
            "CommonFeatureTierV1.CONTENT_HASH must match the SHA-256 of contracts/features/common-feature-tier-v1.json — "
            + "if you edited the JSON, recompute the hash and update the constant");
    }
}
