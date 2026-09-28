package io.netsecml.platform.domain.model;

// modbus_preprocessing_contract_v1's seven transform families, by the names
// the contract itself uses, so a loader maps them with valueOf.
public enum PreprocessingPolicy {
    PASSTHROUGH_BINARY(false, false),
    PASSTHROUGH_BOUNDED_OR_CONSTANT(false, false),
    GLOBAL_STANDARD(false, true),
    GLOBAL_LOG1P_ONLY(false, false),
    CONDITIONAL_STANDARD(true, true),
    CONDITIONAL_LOG1P_ONLY(true, false),
    CONDITIONAL_LOG1P_THEN_STANDARD(true, true);

    private final boolean conditional;
    private final boolean standardized;

    PreprocessingPolicy(boolean conditional, boolean standardized) {
        this.conditional = conditional;
        this.standardized = standardized;
    }

    // Applied only where the feature's mask is 1; exactly 0.0 elsewhere.
    public boolean conditional() {
        return conditional;
    }

    // Uses the fitted mean and std.
    public boolean standardized() {
        return standardized;
    }
}
