package io.netsecml.platform.adapter.flink.process;

import io.netsecml.platform.domain.feature.ConnSnapshot;
import io.netsecml.platform.domain.feature.ConnSnapshotDelta;

// ConnSnapshotJoinFunction's own keyed ValueState shape: the latest accepted
// conn.log snapshot for this uid, paired with the delta that was computed at
// the moment it was accepted (so processElement1 never has to recompute a
// delta on the read path -- it just reads what processElement2 already
// resolved). Package-private: an operator-private implementation detail of the
// join, never referenced outside this package, so unlike ConnSnapshot and
// ConnSnapshotDelta themselves (domain types, part of the frozen contract
// surface) it carries no public API to keep stable.
record ConnEnrichment(ConnSnapshot latest, ConnSnapshotDelta delta) {
}
