package io.netsecml.platform.domain.feature;

import java.util.List;

// The frozen names and order of the protocol-agnostic feature tier that leads
// every per-protocol feature schema.
//
// Order IS the contract: a feature's index is its position in the emitted
// vector, so reordering breaks every model trained against it even though the
// set of names is unchanged. A change creates common-feature-tier-v2; this file
// is never edited.
//
// The same list is published language-neutrally at
// contracts/features/common-feature-tier-v1.json, which the Python training
// project reads. CommonFeatureTierV1Test pins the two together.
public final class CommonFeatureTierV1 {

    public static final String SCHEMA_ID = "common-feature-tier-v1";

    // SHA-256 of contracts/features/common-feature-tier-v1.json. Only id, feature
    // names and indices are checked elsewhere (CommonFeatureTierContractDriftTest
    // in adapter-kafka); nothing previously caught a silent edit to description,
    // encoding or semanticVersion text. CommonFeatureTierV1Test pins this hash
    // against the committed file the same way ConnFeatureSchemaV1 already does.
    public static final String CONTENT_HASH =
        "77dfc12409792a9e1438b6c366d2d46a8168752dc206688702f99534ca78d1cb";

    // Indices 0-5 come from ml-platform's own keyed state and are always
    // populated. Indices 6-11 come from conn.log enrichment, which is a
    // non-blocking left join, so they are zero when no snapshot has arrived --
    // index 11 is what makes that case distinguishable from genuine zeroes.
    public static final List<String> FEATURE_NAMES = List.of(
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
        "conn_enrichment_present");

    public static final int FEATURE_COUNT = FEATURE_NAMES.size();

    // Non-instantiable: every member is static.
    private CommonFeatureTierV1() {
    }
}
