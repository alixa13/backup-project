package io.netsecml.platform.adapter.flink.process;

import io.netsecml.platform.domain.event.NetworkEvent;
import io.netsecml.platform.domain.feature.SourceKey;
import org.apache.flink.api.java.functions.KeySelector;

public final class SourceKeySelector implements KeySelector<NetworkEvent, SourceKey> {
    @Override
    public SourceKey getKey(NetworkEvent event) {
        return new SourceKey(event.sensor(), event.connection().sourceIp());
    }
}
