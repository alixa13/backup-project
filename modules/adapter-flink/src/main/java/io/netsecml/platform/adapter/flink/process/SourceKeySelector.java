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
        //
        // This switch is exhaustive over NetworkEvent's sealed permits, so adding
        // DnsEvent (or any future log type) to the hierarchy breaks this at
        // compile time -- that is the intended alarm. Resolve it with an explicit
        // case DnsEvent -> ... arm, NEVER with a `default ->` catch-all: the
        // moment a default arm exists here, the exhaustiveness guarantee is gone
        // for every other protocol too (HTTP, SSH, Modbus, S7comm would then
        // compile clean with no warning at all). The mechanism protects exactly
        // one protocol addition unless every future editor is told this.
        String sourceIp = switch (event) {
            case ConnEvent conn -> conn.connection().sourceIp();
        };
        return new SourceKey(event.sensor(), sourceIp);
    }
}
