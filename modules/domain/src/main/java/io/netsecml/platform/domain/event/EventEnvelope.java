package io.netsecml.platform.domain.event;

import java.time.Instant;
import java.util.Objects;

// The identity block every log type shares, whatever the shape of its payload.
//
// Extracted so a protocol record carries only its own fields: a dns.log record
// has a uid and a timestamp like a conn record does, but none of a connection's
// duration or byte counts. Before this existed, NetworkEvent required all three
// conn components non-null, so a non-conn protocol could not be represented at
// all without fabricating them.

// DEFERRED DESIGN: The binding spec (docs/superpowers/specs/2026-09-04-multi-protocol-feature-schema-design.md
// §5.1) specifies a sixth component, Endpoints, carrying sourceIp, sourcePort, destinationIp, and
// destinationPort—the four endpoint fields every Zeek log has. It is deliberately omitted because
// extracting it requires splitting the existing ConnectionTuple into Endpoints and ConnClassification, a
// refactor affecting 13 files across 4 modules. Until that work completes, SourceKeySelector
// pattern-switches to reach sourceIp, and each new protocol will require an additional case in that
// switch—costs that would vanish once Endpoints is properly defined on the envelope.
//
// There are now TWO such switch sites, not one, and they would not both go away
// the same way: SourceKeySelector's switch exists only to reach sourceIp, so an
// Endpoints component would delete it outright (envelope().endpoints().sourceIp()
// needs no narrowing). ConnFeatureProcessFunction.processElement's switch narrows
// NetworkEvent to ConnEvent for a structurally different reason -- it must call a
// ConnEvent-typed BuildFeaturesUseCase -- and would still need to narrow even with
// Endpoints on the envelope. Endpoints removes one switch, not both.
public record EventEnvelope(EventId eventId, Instant eventTime, SensorId sensor,
                            LogType logType, String connectionUid) {

    public EventEnvelope {
        // Identity, time, and sensor are structural: eventId is the ClickHouse ORDER BY key
        // tail, eventTime is the partition key, and sensor attributes an event to a
        // deployment. A row cannot be archived without any of these.
        Objects.requireNonNull(eventId, "eventId must not be null");
        Objects.requireNonNull(eventTime, "eventTime must not be null");
        Objects.requireNonNull(sensor, "sensor must not be null");

        // Which Zeek log produced this. Without it the archive job cannot route the
        // event and the feature schema cannot be chosen.
        Objects.requireNonNull(logType, "logType must not be null");

        // connectionUid is a CORRELATION key, never an identity -- several dns or
        // http records legitimately share one uid. Some log types have no uid at
        // all, so an absent one normalises to "" rather than being rejected.
        connectionUid = connectionUid == null ? "" : connectionUid;
    }
}
