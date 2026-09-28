package io.netsecml.platform.domain.feature;

// One pending request: when it was sent and the address and quantity it
// asked for (either may be null, as Zeek writes them). The address and
// quantity are kept so its response, which icsnpp-modbus v1.0.0 mostly writes
// without them, can take them
// (docs/superpowers/specs/2026-09-26-modbus-stage1-scoring-design.md section 2.1, F2).
public record PendingRequest(double ts, Double address, Double quantity) {
}
