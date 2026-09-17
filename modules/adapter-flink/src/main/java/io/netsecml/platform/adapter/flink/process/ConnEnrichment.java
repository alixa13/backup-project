package io.netsecml.platform.adapter.flink.process;

import io.netsecml.platform.domain.feature.ConnSnapshot;
import io.netsecml.platform.domain.feature.ConnSnapshotDelta;

// ConnSnapshotJoinFunction's own keyed ValueState shape: the latest accepted
// conn.log snapshot for this uid, paired with the delta that was computed at
// the moment it was accepted (so processElement1 never has to recompute a
// delta on the read path -- it just reads what processElement2 already
// resolved).
//
// PUBLIC for a reason that has nothing to do with API surface: it is Flink
// keyed state. Flink's TypeExtractor checks Modifier.isPublic before its record
// branch, so a non-public record silently falls back to GenericTypeInfo and is
// serialized by Kryo -- which gives Flink no state schema evolution, so adding
// a field here, or to ConnSnapshot or ConnSnapshotDelta, would break restore
// from an older savepoint. Public, with components that are records of basic
// types, it gets the POJO/record serializer instead.
// ConnSnapshotJoinFunctionTest.enrichmentStateUsesThePojoSerializerNotKryo
// fails if that ever regresses. Nothing outside this package references it.
public record ConnEnrichment(ConnSnapshot latest, ConnSnapshotDelta delta) {
}
