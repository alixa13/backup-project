package io.netsecml.platform.adapter.flink.process;

import io.netsecml.platform.domain.event.S7commEvent;
import io.netsecml.platform.domain.feature.S7commConnectionKey;
import org.apache.flink.api.java.functions.KeySelector;

// Keys the s7comm chain by (sensor, uid): a request and its response share
// the uid, so they share one S7commConnectionState.
public final class S7commConnectionKeySelector implements KeySelector<S7commEvent, S7commConnectionKey> {
    @Override
    public S7commConnectionKey getKey(S7commEvent event) {
        return S7commConnectionKey.of(event);
    }
}
