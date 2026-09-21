package io.netsecml.platform.domain.feature;

import java.util.List;
import static io.netsecml.platform.domain.feature.FeatureDefinition.MissingPolicy.DEFAULT_ZERO;

// The frozen modbus.log feature schema: 42 values in the exact name/order of the
// externally frozen upstream contract at tests/fixtures/contracts/modbus_feature_contract_v1.json
// (feature_order, indices 1-42 there; 0-based here -- the numbering differs by one
// on purpose, the names and their order are what must match). Every feature is
// DEFAULT_ZERO: the upstream contract's missing_rule values ("required", "never
// missing", "0 if absent", "use address_present") all describe a value that is
// present or defaulted to zero, never one this schema rejects as missing, so
// REQUIRED is never used here.
//
// Unlike dns-feature-v1, this schema carries NO common tier: it mirrors an
// externally frozen contract, so it carries exactly what that contract specifies
// (docs/superpowers/specs/2026-09-21-modbus-stage1-design.md section 9, DECIDED 1).
public final class ModbusFeatureSchemaV1 {
    public static final String CONTENT_HASH =
        "0284907ec63d911e1275c5a343a0b82fbddb52b6e0dc55bd550a55ac349de00b";

    public static final FeatureSchema SCHEMA = new FeatureSchema(
        "modbus-feature-v1", "1.0.0", CONTENT_HASH, List.of(
            new FeatureDefinition(0, "is_response", "boolean", DEFAULT_ZERO, "1=response; 0=request"),
            new FeatureDefinition(1, "fc_1", "boolean", DEFAULT_ZERO, "1 iff FC=1"),
            new FeatureDefinition(2, "fc_2", "boolean", DEFAULT_ZERO, "1 iff FC=2"),
            new FeatureDefinition(3, "fc_3", "boolean", DEFAULT_ZERO, "1 iff FC=3"),
            new FeatureDefinition(4, "fc_4", "boolean", DEFAULT_ZERO, "1 iff FC=4"),
            new FeatureDefinition(5, "fc_5", "boolean", DEFAULT_ZERO, "1 iff FC=5"),
            new FeatureDefinition(6, "fc_6", "boolean", DEFAULT_ZERO, "1 iff FC=6"),
            new FeatureDefinition(7, "fc_other", "boolean", DEFAULT_ZERO, "1 iff FC not in {1..6}"),
            new FeatureDefinition(8, "address_value", "address", DEFAULT_ZERO, "numeric address; 0 if absent"),
            new FeatureDefinition(9, "address_present", "boolean", DEFAULT_ZERO, "1 iff address present"),
            new FeatureDefinition(10, "quantity_value", "count", DEFAULT_ZERO, "numeric quantity; 0 if absent"),
            new FeatureDefinition(11, "quantity_present", "boolean", DEFAULT_ZERO, "1 iff quantity present"),
            new FeatureDefinition(12, "response_matched", "boolean", DEFAULT_ZERO, "response current causal matched flag; request=0"),
            new FeatureDefinition(13, "request_values_present", "boolean", DEFAULT_ZERO, "1 iff numeric request values present"),
            new FeatureDefinition(14, "request_value_count", "count", DEFAULT_ZERO, "count numeric request values"),
            new FeatureDefinition(15, "request_value_min", "value", DEFAULT_ZERO, "min numeric request value"),
            new FeatureDefinition(16, "request_value_max", "value", DEFAULT_ZERO, "max numeric request value"),
            new FeatureDefinition(17, "request_value_mean", "value", DEFAULT_ZERO, "mean numeric request value"),
            new FeatureDefinition(18, "response_values_present", "boolean", DEFAULT_ZERO, "1 iff numeric response values present"),
            new FeatureDefinition(19, "response_value_count", "count", DEFAULT_ZERO, "count numeric response values"),
            new FeatureDefinition(20, "response_value_min", "value", DEFAULT_ZERO, "min numeric response value"),
            new FeatureDefinition(21, "response_value_max", "value", DEFAULT_ZERO, "max numeric response value"),
            new FeatureDefinition(22, "response_value_mean", "value", DEFAULT_ZERO, "mean numeric response value"),
            new FeatureDefinition(23, "prev_event_available", "boolean", DEFAULT_ZERO, "1 iff prior event exists in current C3 segment"),
            new FeatureDefinition(24, "inter_arrival_s", "seconds", DEFAULT_ZERO, "current ts - previous ts in segment"),
            new FeatureDefinition(25, "function_changed", "boolean", DEFAULT_ZERO, "1 iff FC differs from previous event"),
            new FeatureDefinition(26, "address_delta_valid", "boolean", DEFAULT_ZERO, "1 iff current and previous applicable address exist"),
            new FeatureDefinition(27, "address_delta", "address", DEFAULT_ZERO, "current address - previous applicable address"),
            new FeatureDefinition(28, "quantity_delta_valid", "boolean", DEFAULT_ZERO, "1 iff current and previous applicable quantity exist"),
            new FeatureDefinition(29, "quantity_delta", "count", DEFAULT_ZERO, "current quantity - previous applicable quantity"),
            new FeatureDefinition(30, "outstanding_requests_before_event", "count", DEFAULT_ZERO, "pending TID count before current event"),
            new FeatureDefinition(31, "response_without_request", "boolean", DEFAULT_ZERO, "response with no pending matching TID"),
            new FeatureDefinition(32, "request_overwrite_same_tid", "boolean", DEFAULT_ZERO, "request reuses already-pending TID"),
            new FeatureDefinition(33, "rtt_valid", "boolean", DEFAULT_ZERO, "1 iff response matched prior request causally"),
            new FeatureDefinition(34, "rtt_s", "seconds", DEFAULT_ZERO, "response ts - matched request ts"),
            new FeatureDefinition(35, "event_rate_1s", "count/s", DEFAULT_ZERO, "count in (t-1,t] / 1s"),
            new FeatureDefinition(36, "event_rate_10s", "count/s", DEFAULT_ZERO, "count in (t-10,t] / 10s"),
            new FeatureDefinition(37, "event_rate_60s", "count/s", DEFAULT_ZERO, "count in (t-60,t] / 60s"),
            new FeatureDefinition(38, "unique_function_count_10s", "count", DEFAULT_ZERO, "distinct FCs in (t-10,t]"),
            new FeatureDefinition(39, "unique_address_count_10s", "count", DEFAULT_ZERO, "distinct present addresses in (t-10,t]"),
            new FeatureDefinition(40, "read_ratio_10s", "ratio", DEFAULT_ZERO, "READ/READ_WRITE fraction in (t-10,t]"),
            new FeatureDefinition(41, "write_ratio_10s", "ratio", DEFAULT_ZERO, "WRITE/READ_WRITE fraction in (t-10,t]")
        ));

    private ModbusFeatureSchemaV1() {
    }
}
