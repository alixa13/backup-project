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

        // Endpoints are resolved only now, AFTER direction, because turning
        // connection-level input into ModbusEvent's per-packet form needs to
        // know which way this record travelled -- see
        // resolvePerPacketEndpoints below. ModbusEvent's compact constructor
        // throws IllegalArgumentException on a blank sourceIp/destinationIp,
        // and unlike DnsEvent (one endpoint) ModbusEvent needs both, so a
        // record with no usable endpoint pair is rejected here instead of
        // reaching that constructor.
        MappingResult<PerPacketEndpoints> endpoints = resolvePerPacketEndpoints(dto, direction);
        if (!endpoints.isValid()) {
            return MappingResult.invalid(endpoints.reason(), endpoints.detail());
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

        // request_values/response_values feed request_value_min/max/mean and
        // their response-side counterparts -- the same "numeric measurement
        // data this record carries" role EventMapper's ConnectionMeasurements
        // fields (including durationMillis, itself not literally a count)
        // already play under INVALID_COUNTER. The authoritative engine's
        // parse_numeric_vector (two-models-info/modbus_/
        // 07b_materialize_feature_engine_v1.py, ~lines 90-144) raises on a
        // null member (float(None) is a TypeError) and on a non-finite one
        // (an explicit math.isfinite check), and process_capture lets that
        // exception abort the whole record -- no vector is produced. The
        // streaming equivalent of that abort is this rejection, not a
        // default: 0.0 is a plausible real register value, so defaulting a
        // null member to it would have silently corrupted those features
        // instead of failing, and letting Infinity through would have
        // written a non-finite value into the vector.
        MappingResult<double[]> requestValues = toValidatedArray(dto.requestValues(), "request_values");
        if (!requestValues.isValid()) {
            return MappingResult.invalid(requestValues.reason(), requestValues.detail());
        }
        MappingResult<double[]> responseValues = toValidatedArray(dto.responseValues(), "response_values");
        if (!responseValues.isValid()) {
            return MappingResult.invalid(responseValues.reason(), responseValues.detail());
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
            direction, endpoints.value().sourceIp(), endpoints.value().destinationIp(), functionCode.getAsInt(),
            String.valueOf(dto.tid()), unitId, dto.address(), dto.quantity(), matched,
            requestValues.value(), responseValues.value());
        return MappingResult.valid(event);
    }

    // A resolved endpoint pair in PER-PACKET form: the sender and receiver of
    // this one record -- which is what ModbusEvent.sourceIp/destinationIp
    // mean -- as opposed to the connection's originator and responder.
    private record PerPacketEndpoints(String sourceIp, String destinationIp) {
    }

    // This mapper is the orientation stage in front of the entity key: it
    // turns whichever endpoint pair the record carries into PER-PACKET form,
    // because ModbusEntityKey.of expects per-packet sourceIp/destinationIp
    // and itself swaps a RESPONSE's pair back into client/server order.
    // Handing it a connection-level pair un-oriented would swap an
    // already-client/server pair on every response and put a request and its
    // own response into different keys.
    //
    // Precedence follows the upstream research code, which looks for the
    // connection-level pair first ("Preferred: canonical initiator/
    // responder") and falls back to the per-packet pair only without it. The
    // upstream makes that choice once per capture file, by which columns
    // exist; this mapper makes it per record, which agrees with it whenever a
    // record's pair is fully present.
    //
    //  1. id_orig_h AND id_resp_h both present (CONNECTION-level, identical
    //     on a request and its response): oriented by direction --
    //        REQUEST:  source = orig, destination = resp
    //        RESPONSE: source = resp, destination = orig
    //     i.e. a request travels originator -> responder and its response
    //     travels back. When direction came from is_orig this is exact by
    //     definition (is_orig marks a packet sent by the originator); when it
    //     came from request_response it rests on Modbus/TCP's client/server
    //     model, where the client opens the connection and sends the
    //     requests. Either way ModbusEntityKey.of then keys BOTH directions
    //     as client = orig, server = resp -- the key the upstream builds from
    //     a connection-level pair, which it uses as client/server directly.
    //  2. Otherwise, source_h AND destination_h both present (already
    //     PER-PACKET): passed through unchanged.
    //  3. Otherwise: rejected as MISSING_REQUIRED_FIELD, never defaulted -- a
    //     record that cannot be keyed cannot be scored.
    //
    // "Present" means non-null and non-blank, and a pair counts only when
    // BOTH of its halves are present: a record carrying id_orig_h without
    // id_resp_h falls through to the per-packet pair rather than mixing one
    // half of each kind. When both pairs are complete the connection-level
    // pair wins, per the same "Preferred" ordering.
    private static MappingResult<PerPacketEndpoints> resolvePerPacketEndpoints(
            ZeekModbusRecord dto, ModbusEvent.ModbusDirection direction) {
        // 1. Connection-level pair: orient it into per-packet form.
        if (isPresent(dto.origHost()) && isPresent(dto.respHost())) {
            return MappingResult.valid(switch (direction) {
                case REQUEST -> new PerPacketEndpoints(dto.origHost(), dto.respHost());
                case RESPONSE -> new PerPacketEndpoints(dto.respHost(), dto.origHost());
            });
        }
        // 2. Per-packet pair: already the form ModbusEvent carries.
        if (isPresent(dto.sourceHost()) && isPresent(dto.destinationHost())) {
            return MappingResult.valid(new PerPacketEndpoints(dto.sourceHost(), dto.destinationHost()));
        }
        // 3. Neither pair complete: nothing to key this record by.
        return MappingResult.invalid(ReasonCode.MISSING_REQUIRED_FIELD,
            "endpoints are required: neither id_orig_h+id_resp_h nor source_h+destination_h is fully present");
    }

    // Non-null and non-blank -- the same test ModbusEvent's compact
    // constructor applies to sourceIp/destinationIp, so an endpoint that
    // passes here cannot make that constructor throw.
    private static boolean isPresent(String host) {
        return host != null && !host.isBlank();
    }

    // request_values/response_values are strict JSON arrays of numbers
    // (ZeekModbusRecord's own comment), but a JSON array element can still be
    // an explicit null (e.g. "[7, null, 9]") or a value that overflows to a
    // non-finite double (e.g. "[1e400]" binds to Double.POSITIVE_INFINITY --
    // ModbusEventMapperTest.aNonFiniteRequestValueFromTheRealParserIsRejected
    // proves this by routing that exact literal through JsonZeekModbusParser,
    // the real wire path, rather than assuming Jackson's behavior) -- both
    // bind cleanly with no Jackson-level error. Rejected here by hand, by
    // index, rather than left
    // to reach ModbusEvent's compact constructor (which only clones the
    // array; it has no per-element validity check to throw from) or
    // defaulted -- see this method's call site above for why defaulting was
    // the wrong repair. An absent (null) list, or an empty one, becomes an
    // empty array, per ModbusEvent's compact constructor javadoc ("pass an
    // empty array... not null").
    private static MappingResult<double[]> toValidatedArray(List<Double> values, String fieldName) {
        if (values == null || values.isEmpty()) {
            return MappingResult.valid(new double[0]);
        }
        double[] array = new double[values.size()];
        for (int i = 0; i < array.length; i++) {
            Double value = values.get(i);
            if (value == null) {
                return MappingResult.invalid(ReasonCode.INVALID_COUNTER,
                    fieldName + "[" + i + "] must not be null");
            }
            if (!Double.isFinite(value)) {
                return MappingResult.invalid(ReasonCode.INVALID_COUNTER,
                    fieldName + "[" + i + "] must be finite, was " + value);
            }
            array[i] = value;
        }
        return MappingResult.valid(array);
    }
}
