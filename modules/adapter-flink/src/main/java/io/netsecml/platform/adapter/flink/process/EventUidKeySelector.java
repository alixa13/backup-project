package io.netsecml.platform.adapter.flink.process;

import io.netsecml.platform.domain.event.NetworkEvent;
import org.apache.flink.api.java.functions.KeySelector;

// Keys the DNS side of the conn.log enrichment join (input 1 of
// ConnSnapshotJoinFunction) by connection uid, so a record lands on the same
// keyed-state partition as the ConnSnapshots for its own connection.
// connectionUid() is a NetworkEvent default method (delegates to the shared
// envelope), so this needs no narrowing switch the way SourceKeySelector's
// sourceIp lookup does -- every log type already carries it identically.
public final class EventUidKeySelector implements KeySelector<NetworkEvent, String> {
    @Override
    public String getKey(NetworkEvent event) {
        return event.connectionUid();
    }
}
