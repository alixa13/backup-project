package io.netsecml.platform.domain.feature;

import org.junit.jupiter.api.Test;
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
}
