package io.netsecml.platform.domain.inference;

// One scored event's outcome, for either detector. WARMUP: fewer than a full
// window in the stream's (Modbus) history, or (S7comm) still inside the warm-up
// after the connection's last reset (the first 64 events; scoring spec
// amendment A1). UNSCORABLE: the detector could not judge it -- non-finite
// preprocessed input (Modbus) or a non-finite score (S7comm). NORMAL and
// ANOMALY: the detector ran, so its scores exist.
public enum DetectorVerdict {
    WARMUP, NORMAL, ANOMALY, UNSCORABLE;

    // The detector ran, so both scores exist.
    public boolean scored() {
        return this == NORMAL || this == ANOMALY;
    }
}
