package io.netsecml.platform.adapter.kafka.mapper;

import io.netsecml.platform.adapter.kafka.dto.ZeekModbusRecord;
import io.netsecml.platform.domain.event.*;

import java.time.Instant;
import java.util.List;
import java.util.Locale;
import java.util.OptionalInt;

// Turns a parsed modbus_detailed.log record into a validated ModbusEvent.
// Mirrors DnsEventMapper's idiom deliberately: this runs inside a Flink
// processElement too, so no exception may escape map() -- an escaping
// exception fails the subtask, the restart strategy replays the same
// record, and one malformed record crash-loops the whole job. Every field a
// domain constructor would reject is therefore validated here BEFORE that
// constructor is called, and returned as MappingResult.invalid(...) instead
// of thrown.
public final class ModbusEventMapper {
    // Same bound as EventMapper.MAX_VALID_TS_SECONDS and DnsEventMapper's own
    // copy, and for the same reason: ClickHouse's event_time column is
    // DateTime64(3, 'UTC'), valid for years 1900-2299, so
    // 2300-01-01T00:00:00Z -- expressed here in ts's own unit, Zeek's UNIX
    // epoch-seconds -- is the first instant outside that range. Not shared as
    // a single constant for the same reason DnsEventMapper's copy is not:
    // these mappers do not share a common base type to hang one on;
    // duplicated deliberately rather than re-derived, so it cannot drift from
    // the other two copies without all three mappers' own pinning tests
    // catching it.
    static final double MAX_VALID_TS_SECONDS = Instant.parse("2300-01-01T00:00:00Z").getEpochSecond();

    // "NA" is the literal sentinel the upstream engine falls back to for an
    // absent unit id rather than dropping the record
    // (docs/superpowers/specs/2026-09-21-modbus-stage1-design.md section 7,
    // and ModbusEvent's own javadoc). unitId is required non-null on
    // ModbusEvent, so an absent/blank wire value is defaulted to this
    // sentinel here rather than rejected.
    private static final String ABSENT_UNIT_ID = "NA";

    public MappingResult<NetworkEvent> map(ZeekModbusRecord dto, SensorId sensor) {
        // uid must be checked blank BEFORE it is used to derive the event id
        // below, for the same reason DnsEventMapper checks dto.id() first:
        // a blank uid concatenated into the identity string would still
        // produce a non-blank result, so EventId.derive's own guard would
        // never fire and a garbage identity would be silently accepted.
        if (dto.uid() == null || dto.uid().isBlank()) {
            return MappingResult.invalid(ReasonCode.MISSING_REQUIRED_FIELD, "uid is required");
        }

        // Same timestamp check as EventMapper/DnsEventMapper -- see
        // MAX_VALID_TS_SECONDS's own comment above for why it is duplicated
        // rather than shared. Past this mapper, ts only ever feeds
        // Math.round(ts * 1000.0), which silently saturates rather than
        // throws on infinity or a huge finite value.
        if (Double.isNaN(dto.ts()) || Double.isInfinite(dto.ts()) || dto.ts() < 0 || dto.ts() >= MAX_VALID_TS_SECONDS) {
            return MappingResult.invalid(ReasonCode.INVALID_TIMESTAMP, "ts must be a non-negative number, was " + dto.ts());
        }

        // sourceHost/destinationHost become ModbusEvent.sourceIp/
        // destinationIp below; ModbusEvent's compact constructor throws
        // IllegalArgumentException on either being blank -- unlike DnsEvent,
        // which carries only one endpoint, ModbusEvent needs both (the
        // causal engine's state key normalizes them into client/server
        // roles -- see ModbusEvent's own javadoc), so both are validated
        // ahead of that call instead of behind a catch.
        if (dto.sourceHost() == null || dto.sourceHost().isBlank()
                || dto.destinationHost() == null || dto.destinationHost().isBlank()) {
            return MappingResult.invalid(ReasonCode.MISSING_REQUIRED_FIELD,
                "id_orig_h and id_resp_h are required");
        }

        // Direction is validated, never guessed (ModbusEvent's own javadoc,
        // and docs/superpowers/specs/2026-09-21-modbus-stage1-design.md
        // section 5: "Direction normalization is a validation, not a
        // coercion"). request_response is the PRIMARY source, checked
        // first; is_orig is read only when request_response is ABSENT
        // (null) -- a request_response that IS present but does not resolve
        // to request/response is a hard rejection here, not a fall-through
        // to is_orig, mirroring the design doc's "hard-fails on anything
        // that is not request/response". Both entity-key components and the
        // frozen is_response feature depend on orientation, so a guess here
        // would corrupt the key and a required feature at once.
        ModbusEvent.ModbusDirection direction;
        if (dto.requestResponse() != null) {
            String normalized = dto.requestResponse().strip().toLowerCase(Locale.ROOT);
            if (normalized.equals("request")) {
                direction = ModbusEvent.ModbusDirection.REQUEST;
            } else if (normalized.equals("response")) {
                direction = ModbusEvent.ModbusDirection.RESPONSE;
            } else {
                return MappingResult.invalid(ReasonCode.MISSING_REQUIRED_FIELD,
                    "request_response must be \"request\" or \"response\" (case-insensitive), was \""
                        + dto.requestResponse() + "\"");
            }
        } else if (dto.isOrig() != null) {
            direction = dto.isOrig() ? ModbusEvent.ModbusDirection.REQUEST : ModbusEvent.ModbusDirection.RESPONSE;
        } else {
            return MappingResult.invalid(ReasonCode.MISSING_REQUIRED_FIELD,
                "direction is required: neither request_response nor is_orig resolved");
        }

        // function_code is required; the upstream engine hard-fails on a
        // missing or unparseable func too. ModbusFunctionCode.codeOf runs the
        // frozen name/number normalizer -- see that class's javadoc for why
        // it is transcribed rather than derived by hand.
        OptionalInt functionCode = ModbusFunctionCode.codeOf(dto.func());
        if (functionCode.isEmpty()) {
            return MappingResult.invalid(ReasonCode.MISSING_REQUIRED_FIELD,
                "func did not resolve to a known function code, was \"" + dto.func() + "\"");
        }

        // Every field this event needs is now known valid, so none of the
        // domain constructors below can throw.
        //
        // Event identity is sensor:uid:tid:direction:ts_millis, NOT
        // sensor:uid alone: ICSNPP emits a request and its matching response
        // as two separate records sharing one tid
        // (docs/superpowers/specs/2026-09-21-modbus-stage1-design.md section
        // 6), and tid itself is a client-chosen 16-bit counter that a 10 Hz
        // SCADA poll loop exhausts in under two hours on connections that
        // live far longer -- so BOTH direction and a millisecond timestamp
        // are folded in to keep same-tid, same-direction records from
        // colliding on the same identity.
        Instant eventTime = Instant.ofEpochMilli(Math.round(dto.ts() * 1000.0));
        String upstreamId = dto.uid() + ":" + dto.tid() + ":" + direction.name() + ":" + eventTime.toEpochMilli();
        EventId eventId = EventId.derive(sensor, upstreamId);

        // This mapper handles modbus_detailed.log exclusively, so
        // LogType.MODBUS is a fixed constant, mirroring how DnsEventMapper
        // fixes LogType.DNS and EventMapper fixes LogType.CONN.
        LogType logType = LogType.MODBUS;

        // dto.uid() is Zeek's connection uid -- the CORRELATION key, not the
        // event identity. ICSNPP legitimately logs many request/response
        // pairs on one connection, which is exactly why eventId above folds
        // in tid, direction and the millisecond timestamp instead of using
        // this value alone.
        String connectionUid = dto.uid();

        // unit_id is required upstream, but the design doc records that the
        // engine falls back to the "NA" sentinel for an absent one rather
        // than dropping the record -- see ABSENT_UNIT_ID's own comment
        // above.
        String unitId = (dto.unitId() == null || dto.unitId().isBlank()) ? ABSENT_UNIT_ID : dto.unitId();

        // matched is meaningful only on a response record and null on most
        // requests; ModbusEvent.matched is a primitive boolean, so an absent
        // wire value defaults to false rather than being rejected -- it is
        // not one of the fields whose absence corrupts a required feature.
        boolean matched = dto.matched() != null && dto.matched();

        NetworkEvent event = new ModbusEvent(
            new EventEnvelope(eventId, eventTime, sensor, logType, connectionUid),
            direction, dto.sourceHost(), dto.destinationHost(), functionCode.getAsInt(),
            String.valueOf(dto.tid()), unitId, dto.address(), dto.quantity(), matched,
            toArray(dto.requestValues()), toArray(dto.responseValues()));
        return MappingResult.valid(event);
    }

    // request_values/response_values are strict JSON arrays of numbers
    // (ZeekModbusRecord's own comment), but a JSON array element can still be
    // an explicit null (e.g. "[7, null, 9]") -- Jackson accepts that into a
    // List<Double> without complaint. ModbusEvent.requestValues/
    // responseValues are primitive double[] arrays, so a null element is
    // defaulted to 0.0 here rather than throwing a NullPointerException out
    // of map() when unboxed -- the same crash-loop reasoning as every other
    // check in this mapper, applied to an element rather than the whole
    // field. An absent (null) list itself becomes an empty array, per
    // ModbusEvent's compact constructor javadoc ("pass an empty array... not
    // null").
    private static double[] toArray(List<Double> values) {
        if (values == null || values.isEmpty()) {
            return new double[0];
        }
        double[] array = new double[values.size()];
        for (int i = 0; i < array.length; i++) {
            Double value = values.get(i);
            array[i] = value == null ? 0.0 : value;
        }
        return array;
    }
}
