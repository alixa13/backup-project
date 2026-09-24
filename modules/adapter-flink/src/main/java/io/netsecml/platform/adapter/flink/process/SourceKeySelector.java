package io.netsecml.platform.adapter.flink.process;

import io.netsecml.platform.domain.event.ConnEvent;
import io.netsecml.platform.domain.event.DnsEvent;
import io.netsecml.platform.domain.event.ModbusEvent;
import io.netsecml.platform.domain.event.S7commEvent;
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
            // Resolved with an explicit arm, per the comment above -- never a
            // default. DnsEvent carries sourceIp directly (no ConnectionTuple to
            // narrow through), so this arm is a plain accessor call rather than
            // the narrowing conn's arm needs.
            case DnsEvent dns -> dns.sourceIp();
            // A ModbusEvent reaching this selector is a wiring error, not a
            // value to compute: conn and dns are keyed by SourceKey, but modbus
            // is keyed by its own ModbusEntityKey (client/server roles,
            // normalized from direction, not a plain sourceIp -- see
            // ModbusEntityKey's own javadoc) and never flows through this
            // selector at all. Throwing is more honest than returning
            // modbus.sourceIp() from a path that cannot execute: it does not
            // silently imply modbus uses SourceKey.
            case ModbusEvent ignored -> throw new IllegalStateException(
                "SourceKeySelector received a ModbusEvent; the modbus chain is separate by design "
                + "(docs/superpowers/specs/2026-09-21-modbus-stage1-design.md section 7, "
                + "\"Keying, state and segments\") and this is a wiring error, not a runtime condition");
            case S7commEvent ignored -> throw new IllegalStateException(
                "SourceKeySelector received an S7commEvent; the s7comm chain is separate by design "
                + "(docs/superpowers/specs/2026-09-24-s7comm-stage1-design.md section 7, "
                + "\"Keying, state and lifetime\") and this is a wiring error, not a runtime condition");
        };
        // logType is the new middle component
        // (docs/superpowers/specs/2026-09-04-multi-protocol-feature-schema-design.md
        // section 5.5, "SourceKey gains the log type"): it needs no arm of its
        // own in the switch above because every NetworkEvent already carries it
        // on the shared envelope, conn and dns alike.
        return new SourceKey(event.sensor(), event.logType(), sourceIp);
    }
}
