package io.netsecml.platform.adapter.flink.process;

import io.netsecml.platform.domain.event.ConnEvent;
import io.netsecml.platform.domain.event.NetworkEvent;
import io.netsecml.platform.domain.feature.SourceKey;
import org.apache.flink.api.java.functions.KeySelector;

public final class SourceKeySelector implements KeySelector<NetworkEvent, SourceKey> {
    @Override
    public SourceKey getKey(NetworkEvent event) {
        // KeySelector's type parameter is fixed to NetworkEvent by the
        // DataStream<NetworkEvent> being keyed, so this cannot narrow its
        // parameter the way EventFeatureExtractor does. It switches internally
        // instead, for the one conn-specific value it needs.
        String sourceIp = switch (event) {
            case ConnEvent conn -> conn.connection().sourceIp();
        };
        return new SourceKey(event.sensor(), sourceIp);
    }
}
