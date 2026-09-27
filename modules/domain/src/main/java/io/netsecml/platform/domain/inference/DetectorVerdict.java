package io.netsecml.platform.domain.inference;

// One Modbus event's outcome. WARMUP: fewer than a full window in its stream's
// segment. UNSCORABLE: its preprocessed vector was not finite. NORMAL and
// ANOMALY: the detector ran.
public enum DetectorVerdict {
    WARMUP, NORMAL, ANOMALY, UNSCORABLE;

    // The detector ran, so both scores exist.
    public boolean scored() {
        return this == NORMAL || this == ANOMALY;
    }
}
