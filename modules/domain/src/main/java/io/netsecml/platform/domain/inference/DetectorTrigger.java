package io.netsecml.platform.domain.inference;

// Which head exceeded its threshold.
public enum DetectorTrigger {
    NONE, DENSE, TEMPORAL, BOTH;

    public static DetectorTrigger of(boolean dense, boolean temporal) {
        if (dense && temporal) {
            return BOTH;
        }
        if (dense) {
            return DENSE;
        }
        return temporal ? TEMPORAL : NONE;
    }
}
