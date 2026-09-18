package io.netsecml.platform.adapter.kafka.sink;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netsecml.platform.domain.feature.CommonFeatureTierV1;
import org.junit.jupiter.api.Test;
import java.nio.file.Path;
import java.nio.file.Paths;
import static org.junit.jupiter.api.Assertions.*;

// DnsFeatureSchemaV1Test.theFirstTwelveFeaturesAreTheCommonTierInOrder (domain
// module) already pins that dns-feature-schema-v1.json's first twelve NAMES
// match common-feature-tier-v1.json's, in order -- but a JSON contract also
// carries unit, missingPolicy and formula per feature, and nothing previously
// compared those between the two files. A schema that kept the twelve names
// but drifted one of those other fields (say, dns's own conn_age_seconds
// declared REQUIRED instead of DEFAULT_ZERO) would pass every existing test
// and still describe a different feature to training than the common tier
// declares to the online job.
//
// Lives here rather than in domain because domain's test classpath carries
// only junit-jupiter (Jackson was deliberately removed from it -- see
// CommonFeatureTierV1Test's own contentHashMatchesCommittedContractFile
// comment), and CommonFeatureTierContractDriftTest in this same package
// already reads these JSON contracts with Jackson for the same reason.
class DnsCommonTierAlignmentContractDriftTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    // This module lives at modules/adapter-kafka, so the repo root is two up.
    private JsonNode features(String fileName) throws Exception {
        Path contract = Paths.get("..", "..", "contracts", "features", fileName);
        return MAPPER.readTree(contract.toFile()).get("features");
    }

    @Test
    void dnsSchemasFirstTwelveFeaturesMatchTheCommonTierFieldByField() throws Exception {
        JsonNode commonTier = features("common-feature-tier-v1.json");
        JsonNode dnsSchema = features("dns-feature-schema-v1.json");

        assertEquals(CommonFeatureTierV1.FEATURE_COUNT, commonTier.size(),
            "common-feature-tier-v1.json itself must declare exactly FEATURE_COUNT features");
        assertTrue(dnsSchema.size() >= CommonFeatureTierV1.FEATURE_COUNT,
            "dns-feature-schema-v1.json must be at least as wide as the common tier it leads with");

        for (int i = 0; i < CommonFeatureTierV1.FEATURE_COUNT; i++) {
            JsonNode commonFeature = commonTier.get(i);
            JsonNode dnsFeature = dnsSchema.get(i);

            assertEquals(commonFeature.get("name").asText(), dnsFeature.get("name").asText(),
                "index " + i + " name must match between the common tier and the dns schema");
            assertEquals(commonFeature.get("unit").asText(), dnsFeature.get("unit").asText(),
                "index " + i + " (" + commonFeature.get("name").asText() + ") unit must match");
            assertEquals(commonFeature.get("missingPolicy").asText(), dnsFeature.get("missingPolicy").asText(),
                "index " + i + " (" + commonFeature.get("name").asText() + ") missingPolicy must match");
            assertEquals(commonFeature.get("formula").asText(), dnsFeature.get("formula").asText(),
                "index " + i + " (" + commonFeature.get("name").asText() + ") formula must match");
        }
    }
}
