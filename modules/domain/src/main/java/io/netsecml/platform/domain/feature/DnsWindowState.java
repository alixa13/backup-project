package io.netsecml.platform.domain.feature;

import java.util.Objects;

// The bounded per-(sensor, sourceIp) state DnsBuildFeaturesUseCase folds each
// dns.log record into: the shared rolling counters (record/byte/failure counts)
// and DNS's own inter-arrival timing. Two independent pieces rather than one
// merged type because RollingCounters' serialized shape is already shared with
// conn's checkpoint state (see RecordTimingState's own javadoc) -- widening it
// to also carry timing would change what conn already has on disk.
//
// This record is held directly in Flink's ValueState<DnsWindowState> by
// DnsFeatureProcessFunction, so its shape is checkpoint-affecting: adding,
// removing or reordering components here changes what a running job has
// serialized under its uid, the same way changing RollingCounters' own fields
// would. DnsFeatureProcessFunctionTest.windowStateSurvivesASnapshotRestoreRoundTrip
// -- a Flink KeyedOneInputStreamOperatorTestHarness snapshot/restore test, not
// a Testcontainers-backed one -- is what actually proves a DnsWindowState
// instance survives a real serialize/deserialize round trip; this record's own
// compact constructor only proves the Java-level behavior above that.
public record DnsWindowState(RollingCounters counters, RecordTimingState timing) {

    public DnsWindowState {
        Objects.requireNonNull(counters, "counters must not be null");
        Objects.requireNonNull(timing, "timing must not be null");
    }

    // The zero state a fresh (sensor, sourceIp) key starts from -- mirrors
    // RollingCounters.empty() and RecordTimingState.empty() individually, since
    // this type is only ever the pairing of those two, never more.
    public static DnsWindowState empty() {
        return new DnsWindowState(RollingCounters.empty(), RecordTimingState.empty());
    }
}
