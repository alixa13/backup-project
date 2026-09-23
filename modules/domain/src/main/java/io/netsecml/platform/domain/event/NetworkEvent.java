package io.netsecml.platform.domain.event;

import java.time.Instant;

// One parsed, validated Zeek record, whatever log produced it.
//
// A sealed interface rather than a record because the log types genuinely differ
// in shape: a conn record carries a connection's duration and byte counts, and a
// dns record carries none of those. Modelling that as one record with nullable
// conn fields is the "generic event" PILOT_ARCHITECTURE rejects -- every consumer
// would have to know which fields are meaningful for which log type, and the
// compiler could not help.
//
// permits lists ONLY implemented log types. A record is added when a protocol has
// a parser, a mapper and a feature schema behind it, never before -- the same
// rule LogType states about its own constants. Sealing then makes the compiler
// flag every non-exhaustive switch the moment a real second protocol arrives.
//
// That happened when DnsEvent joined the permits clause below. It flagged four
// pattern-switch sites -- SourceKeySelector.getKey, ConnFeatureProcessFunction.
// processElement, and one exhaustiveness-proving switch each in NetworkEventTest
// and EventMapperTest -- and all four were resolved with an explicit case
// DnsEvent arm, never a default. NetworkEventSurfaceTest's permittedSubclasses
// assertion also had to change, though it is a reflective assertion rather than
// a switch, so it did not fail until run rather than at compile time.
//
// ModbusEvent joining permits repeated the same mechanism at a larger scale:
// nine files failed to compile -- five pattern-switch sites in adapter-flink's
// process package (ConnFeatureProcessFunction, ConnSnapshotExtractFunction,
// ConnSnapshotJoinFunction, DnsFeatureProcessFunction, SourceKeySelector) plus
// four test files whose own switches exist to narrow or prove exhaustiveness
// (ConnSnapshotJoinFunctionTest, DnsEventMapperTest, EventMapperTest,
// NetworkEventTest) -- every one resolved with an explicit case ModbusEvent
// arm, never a default, and NetworkEventSurfaceTest's permittedSubclasses
// assertion needed the same reflective-not-compile-time update DnsEvent's
// arrival required.
//
// ModbusEvent's admission here is fully discharged: it has a parser
// (JsonZeekModbusParser), a mapper (ModbusEventMapper) and a feature schema
// (ModbusFeatureSchemaV1) -- the admission rule stated above this paragraph --
// so, unlike DnsEvent's own arrival, nothing about its permits membership was
// ever ahead of what it depends on.
public sealed interface NetworkEvent permits ConnEvent, DnsEvent, ModbusEvent {

    // Every log type carries the same identity block; only the payload differs.
    EventEnvelope envelope();

    // The shared fields are exposed directly, delegating to the envelope, so the
    // many call sites that read only identity and timing neither know nor care
    // that the hierarchy exists.
    default EventId eventId() {
        return envelope().eventId();
    }

    default Instant eventTime() {
        return envelope().eventTime();
    }

    default SensorId sensor() {
        return envelope().sensor();
    }

    default LogType logType() {
        return envelope().logType();
    }

    default String connectionUid() {
        return envelope().connectionUid();
    }
}
