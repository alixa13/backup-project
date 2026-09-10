package io.netsecml.platform.adapter.kafka.sink;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netsecml.platform.domain.feature.CommonFeatureTierV1;
import org.junit.jupiter.api.Test;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

// The Java constant and the shipped contract must not drift. Training reads the
// JSON; the online job reads the constant. If they disagree, feature i means two
// different things on the two sides and nothing errors anywhere.
//
// This lives here rather than in domain because domain's test classpath carries
// only junit-jupiter -- Jackson was deliberately removed from it -- and because
// StreamContractDriftTest in this same package already does exactly this job for
// the stream contracts.
class CommonFeatureTierContractDriftTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    // This module lives at modules/adapter-kafka, so the repo root is two up.
    private Path contract() {
        return Paths.get("..", "..", "contracts", "features", "common-feature-tier-v1.json");
    }

    @Test
    void javaConstantMatchesTheShippedContract() throws Exception {
        JsonNode root = MAPPER.readTree(contract().toFile());

        assertEquals(CommonFeatureTierV1.SCHEMA_ID, root.get("id").asText());

        List<String> fromContract = new ArrayList<>();
        root.get("features").forEach(feature -> fromContract.add(feature.get("name").asText()));

        assertEquals(CommonFeatureTierV1.FEATURE_NAMES, fromContract,
            "the contract file and the Java constant must list the same features in the same order");
    }

    // Index is the vector position, so a contract whose declared indices do not
    // ascend from zero would misplace every feature after the gap.
    @Test
    void contractIndicesAscendFromZero() throws Exception {
        JsonNode features = MAPPER.readTree(contract().toFile()).get("features");

        for (int i = 0; i < features.size(); i++) {
            assertEquals(i, features.get(i).get("index").asInt(),
                "feature at position " + i + " must declare index " + i);
        }
    }
}
