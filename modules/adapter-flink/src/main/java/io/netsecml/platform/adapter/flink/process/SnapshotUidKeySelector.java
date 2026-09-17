package io.netsecml.platform.adapter.flink.process;

import io.netsecml.platform.domain.feature.ConnSnapshot;
import org.apache.flink.api.java.functions.KeySelector;

// Keys the conn.log side of the enrichment join (input 2 of
// ConnSnapshotJoinFunction) by connection uid -- the same key EventUidKeySelector
// derives for input 1, so ConnectedStreams.keyBy partitions both inputs onto the
// identical keyed-state slot for one connection.
public final class SnapshotUidKeySelector implements KeySelector<ConnSnapshot, String> {
    @Override
    public String getKey(ConnSnapshot snapshot) {
        return snapshot.connectionUid();
    }
}
