package io.netsecml.platform.domain.feature;

// The per-interval difference between two consecutive ConnSnapshots.
//
// A distinct type from ConnSnapshot on purpose. conn.log counters are cumulative
// since the connection opened, so a delta and an absolute observation carry the
// same four numbers with completely different meanings. Returning a ConnSnapshot
// from deltaFrom made those two interchangeable at compile time, which is how a
// cumulative total silently ends up in a rate feature.
//
// ageSeconds is carried rather than differenced: the age at the later of the two
// observations is the meaningful value, and the gap between snapshots is already
// implied by the counters.
public record ConnSnapshotDelta(long origBytes, long respBytes, long origPkts, long respPkts,
                                long ageSeconds) {
}
