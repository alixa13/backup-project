package io.netsecml.platform.domain.feature;

import java.util.List;

import static io.netsecml.platform.domain.feature.FeatureDefinition.MissingPolicy.DEFAULT_ZERO;
import static io.netsecml.platform.domain.feature.FeatureDefinition.MissingPolicy.REQUIRED;
import static io.netsecml.platform.domain.feature.FeatureDefinition.MissingPolicy.SENTINEL;

// s7comm-feature-v1: the 16 raw causal features the upstream model team froze
// for both S7comm models (STAGE1_RAW_FEATURES, transcribed with provenance as
// tests/fixtures/contracts/s7comm_stage1_raw_features_v1.json), in their frozen
// order and with no common tier -- it mirrors an externally frozen contract,
// like modbus-feature-v1. Every value is read after the current event has been
// applied to the connection state. contracts/features/s7comm-feature-schema-v1.json
// is the same fact in JSON; CONTENT_HASH is that file's SHA-256.
public final class S7commFeatureSchemaV1 {
    public static final String CONTENT_HASH =
        "e481236554f4a28869e5d1fc1a5b000e66bd0dcbbb985a19352170cd141129ac";

    public static final FeatureSchema SCHEMA = new FeatureSchema(
        "s7comm-feature-v1", "1.0.0", CONTENT_HASH, List.of(
            new FeatureDefinition(0, "s7_outstanding_requests", "count", DEFAULT_ZERO,
                "unmatched request PDU references after this event"),
            new FeatureDefinition(1, "s7_outstanding_mean_16", "count", DEFAULT_ZERO,
                "mean of the last <=16 values of s7_outstanding_requests, this event's included"),
            new FeatureDefinition(2, "s7_response_match_rate_16", "ratio", DEFAULT_ZERO,
                "matched / count over the last <=16 responses; 0 before any response"),
            new FeatureDefinition(3, "s7_same_function_run_length", "count", DEFAULT_ZERO,
                "current run of requests carrying the same function code; 0 before any"),
            new FeatureDefinition(4, "s7_same_direction_run_length", "count", DEFAULT_ZERO,
                "current run of events in the same direction"),
            new FeatureDefinition(5, "s7_request_ratio_16", "ratio", DEFAULT_ZERO,
                "requests / count over the last <=16 events"),
            new FeatureDefinition(6, "s7_direction_change_rate_16", "ratio", DEFAULT_ZERO,
                "mean of the last <=16 direction-change flags; the first event contributes 0"),
            new FeatureDefinition(7, "s7_function_change_rate_16", "ratio", DEFAULT_ZERO,
                "mean of the last <=16 function-change flags of requests carrying a function code; 0 before any"),
            new FeatureDefinition(8, "s7_function_entropy_16", "ratio", DEFAULT_ZERO,
                "Shannon entropy of the last <=16 request function codes / log2(max(2, min(16, n))); 0 for n <= 1"),
            new FeatureDefinition(9, "s7_function_transition_entropy_16", "ratio", DEFAULT_ZERO,
                "Shannon entropy of the last <=16 (previous, current) request function transitions / "
                    + "log2(max(2, min(16, n))); 0 for n <= 1"),
            new FeatureDefinition(10, "s7_rosctr_change_rate_16", "ratio", DEFAULT_ZERO,
                "mean of the last <=16 ROSCTR-change flags of events carrying a ROSCTR code; 0 before any"),
            new FeatureDefinition(11, "s7_pdu_reference_unique_ratio_32", "ratio", DEFAULT_ZERO,
                "distinct / count over the last <=32 request PDU references; 0 before any request"),
            new FeatureDefinition(12, "is_request_direction", "boolean", REQUIRED,
                "1 iff the destination port of this record is 102"),
            new FeatureDefinition(13, "s7_function_changed", "boolean", DEFAULT_ZERO,
                "1 iff this request's function code differs from the previous request's; 0 for responses"),
            new FeatureDefinition(14, "s7_rosctr", "code", SENTINEL,
                "ROSCTR code; -1 = __MISSING__ (see categoricalCodes)"),
            new FeatureDefinition(15, "s7_operation", "code", SENTINEL,
                "S7 function code; -1 = __MISSING__, -2 = unseen function_name (see categoricalCodes)")
        ));

    // Non-instantiable: every member is a constant.
    private S7commFeatureSchemaV1() {
    }
}
