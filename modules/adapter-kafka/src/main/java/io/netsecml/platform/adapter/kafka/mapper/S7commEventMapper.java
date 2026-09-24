package io.netsecml.platform.adapter.kafka.mapper;

import com.fasterxml.jackson.databind.JsonNode;
import io.netsecml.platform.adapter.kafka.dto.ZeekS7commRecord;
import io.netsecml.platform.domain.event.EventEnvelope;
import io.netsecml.platform.domain.event.EventId;
import io.netsecml.platform.domain.event.LogType;
import io.netsecml.platform.domain.event.MappingResult;
import io.netsecml.platform.domain.event.NetworkEvent;
import io.netsecml.platform.domain.event.ReasonCode;
import io.netsecml.platform.domain.event.S7commEvent;
import io.netsecml.platform.domain.event.SensorId;

import java.time.Instant;
import java.util.OptionalInt;

import static io.netsecml.platform.adapter.kafka.mapper.S7commFieldValues.first;

// One ICSNPP s7comm.log JSON object -> S7commEvent. A port of upstream's
// production adapter, normalize_zeek_message in
// two-models-info/S7___/kafka_source.py: the same aliases in the same order,
// the same two endpoint shapes with the same precedence, the same value
// parsing (S7commFieldValues). Where it deliberately differs -- uid and
// pdu_reference required, codes range-checked -- the spec's rulings R1-R3
// (docs/superpowers/specs/2026-09-24-s7comm-stage1-design.md section 9) say why.
public final class S7commEventMapper {

    // Same bound ModbusEventMapper applies: a year-2300 timestamp is a unit or
    // encoding error, not a record to score.
    static final double MAX_VALID_TS_SECONDS = Instant.parse("2300-01-01T00:00:00Z").getEpochSecond();

    // The resolved per-packet endpoints of one record.
    private record Endpoints(String sourceIp, int sourcePort, String destinationIp, int destinationPort) {
    }

    // An optional integer code that parsed and passed its range check (MappingResult
    // cannot carry a null value, so absence is a Code holding null).
    private record Code(Integer value) {
    }

    public MappingResult<NetworkEvent> map(ZeekS7commRecord dto, SensorId sensor) {
        JsonNode json = dto.json();

        // uid: required, unlike upstream's endpoint-key fallback (R1) -- Zeek
        // always writes it, and the event id and the state key need it.
        String uid = dto.uid();
        if (uid == null || uid.isBlank()) {
            return MappingResult.invalid(ReasonCode.MISSING_REQUIRED_FIELD, "uid is required");
        }

        // ts: float(message["ts"]), then this platform's sanity bounds.
        JsonNode tsNode = json.get("ts");
        if (tsNode == null || tsNode.isNull()) {
            return MappingResult.invalid(ReasonCode.MISSING_REQUIRED_FIELD, "ts is required");
        }
        double ts;
        try {
            ts = S7commFieldValues.seconds(tsNode);
        } catch (IllegalArgumentException e) {
            return MappingResult.invalid(ReasonCode.INVALID_TIMESTAMP, "ts " + e.getMessage());
        }
        if (!Double.isFinite(ts) || ts < 0 || ts >= MAX_VALID_TS_SECONDS) {
            return MappingResult.invalid(ReasonCode.INVALID_TIMESTAMP,
                "ts must be a non-negative number before 2300-01-01, was " + ts);
        }

        // is_orig: parsed before the endpoints, as upstream does, so an
        // unreadable value rejects the record even when it is not needed.
        Boolean isOrig;
        try {
            isOrig = S7commFieldValues.boolOrNone(json.get("is_orig"));
        } catch (IllegalArgumentException e) {
            return MappingResult.invalid(ReasonCode.MISSING_REQUIRED_FIELD, "is_orig " + e.getMessage());
        }

        // Endpoints: per-packet if complete, else the connection pair by is_orig.
        MappingResult<Endpoints> endpoints = resolveEndpoints(json, isOrig);
        if (!endpoints.isValid()) {
            return MappingResult.invalid(endpoints.reason(), endpoints.detail());
        }

        // pdu_reference: required and 16-bit (R2).
        MappingResult<Code> pdu = code(first(json.get("pdu_reference"), json.get("pdu_ref"), json.get("pdu_ref_num")),
            "pdu_reference", S7commEvent.MAX_PDU_REFERENCE + 1);
        if (!pdu.isValid()) {
            return MappingResult.invalid(pdu.reason(), pdu.detail());
        }
        if (pdu.value().value() == null) {
            return MappingResult.invalid(ReasonCode.MISSING_REQUIRED_FIELD, "pdu_reference is required");
        }

        // rosctr_code and function_code: optional, each under upstream's aliases (R3 bounds).
        MappingResult<Code> rosctr = code(first(json.get("rosctr_code"), json.get("rosctr")),
            "rosctr_code", S7commEvent.MAX_CODE_EXCLUSIVE);
        if (!rosctr.isValid()) {
            return MappingResult.invalid(rosctr.reason(), rosctr.detail());
        }
        MappingResult<Code> function = code(first(json.get("function_code"), json.get("function")),
            "function_code", S7commEvent.MAX_CODE_EXCLUSIVE);
        if (!function.isValid()) {
            return MappingResult.invalid(function.reason(), function.detail());
        }

        // function_name: message.get("function_name"), no aliases; "" means absent.
        JsonNode nameNode = json.get("function_name");
        String functionName = nameNode == null || nameNode.isNull() || nameNode.asText().isEmpty()
            ? null : nameNode.asText();

        // Identity: sensor:uid:pdu_reference:direction:ts_millis (spec section 6).
        // Direction because a request and its response share the reference;
        // milliseconds because the 16-bit reference wraps within hours.
        Endpoints e = endpoints.value();
        boolean isRequest = e.destinationPort() == S7commEvent.S7_PORT;
        Instant eventTime = Instant.ofEpochMilli(Math.round(ts * 1000.0));
        String upstreamId = uid + ":" + pdu.value().value() + ":" + (isRequest ? "REQUEST" : "RESPONSE")
            + ":" + eventTime.toEpochMilli();
        EventId eventId = EventId.derive(sensor, upstreamId);

        NetworkEvent event = new S7commEvent(
            new EventEnvelope(eventId, eventTime, sensor, LogType.S7COMM, uid), ts,
            e.sourceIp(), e.sourcePort(), e.destinationIp(), e.destinationPort(),
            pdu.value().value(), rosctr.value().value(), function.value().value(), functionName);
        return MappingResult.valid(event);
    }

    // The PDU reference for a DLQ record's correlation id: empty when it is
    // absent, unreadable or out of range, since a rejection may be about
    // exactly that. Never throws.
    public static OptionalInt pduReferenceOf(ZeekS7commRecord dto) {
        JsonNode json = dto.json();
        try {
            Long value = S7commFieldValues.intOrNone(
                first(json.get("pdu_reference"), json.get("pdu_ref"), json.get("pdu_ref_num")));
            if (value == null || value < 0 || value > S7commEvent.MAX_PDU_REFERENCE) {
                return OptionalInt.empty();
            }
            return OptionalInt.of(value.intValue());
        } catch (IllegalArgumentException e) {
            return OptionalInt.empty();
        }
    }

    // upstream _resolve_endpoints. Shape A: all four per-packet fields, used as
    // they are. Shape B: the connection pair -- this platform's underscored
    // id_orig_h, Zeek's dotted id.orig_h, a nested id object, or bare orig_h --
    // oriented by is_orig.
    private static MappingResult<Endpoints> resolveEndpoints(JsonNode json, Boolean isOrig) {
        JsonNode sourceHost = first(json.get("source_h"), json.get("src_h"), json.get("src"));
        JsonNode sourcePort = first(json.get("source_p"), json.get("src_p"));
        JsonNode destinationHost = first(json.get("destination_h"), json.get("dst_h"), json.get("dst"));
        JsonNode destinationPort = first(json.get("destination_p"), json.get("dst_p"));
        if (sourceHost != null && sourcePort != null && destinationHost != null && destinationPort != null) {
            return endpoints(sourceHost, sourcePort, destinationHost, destinationPort);
        }

        JsonNode id = json.path("id");
        JsonNode origHost = first(json.get("id_orig_h"), json.get("id.orig_h"), id.get("orig_h"), json.get("orig_h"));
        JsonNode origPort = first(json.get("id_orig_p"), json.get("id.orig_p"), id.get("orig_p"), json.get("orig_p"));
        JsonNode respHost = first(json.get("id_resp_h"), json.get("id.resp_h"), id.get("resp_h"), json.get("resp_h"));
        JsonNode respPort = first(json.get("id_resp_p"), json.get("id.resp_p"), id.get("resp_p"), json.get("resp_p"));
        if (origHost == null || origPort == null || respHost == null || respPort == null) {
            return MappingResult.invalid(ReasonCode.MISSING_REQUIRED_FIELD,
                "endpoints are required: neither source/destination nor id orig/resp is complete");
        }
        if (isOrig == null) {
            return MappingResult.invalid(ReasonCode.MISSING_REQUIRED_FIELD,
                "the connection pair id orig/resp requires is_orig to orient it");
        }
        // The originator sent this record -> it is the source; else the responder is.
        return isOrig
            ? endpoints(origHost, origPort, respHost, respPort)
            : endpoints(respHost, respPort, origHost, origPort);
    }

    // Reads and range-checks one resolved endpoint pair.
    private static MappingResult<Endpoints> endpoints(JsonNode sourceHost, JsonNode sourcePort,
                                                      JsonNode destinationHost, JsonNode destinationPort) {
        String source;
        String destination;
        try {
            source = S7commFieldValues.host(sourceHost);
            destination = S7commFieldValues.host(destinationHost);
        } catch (IllegalArgumentException e) {
            return MappingResult.invalid(ReasonCode.MISSING_REQUIRED_FIELD, "host " + e.getMessage());
        }
        if (source.isBlank() || destination.isBlank()) {
            return MappingResult.invalid(ReasonCode.MISSING_REQUIRED_FIELD, "endpoint hosts must not be blank");
        }
        long from;
        long to;
        try {
            from = S7commFieldValues.port(sourcePort);
            to = S7commFieldValues.port(destinationPort);
        } catch (IllegalArgumentException e) {
            return MappingResult.invalid(ReasonCode.INVALID_PORT, e.getMessage());
        }
        if (from < 0 || from > 0xFFFF || to < 0 || to > 0xFFFF) {
            return MappingResult.invalid(ReasonCode.INVALID_PORT, "ports must be 0-65535, were " + from + ", " + to);
        }
        return MappingResult.valid(new Endpoints(source, (int) from, destination, (int) to));
    }

    // One optional integer field: absent -> Code(null); unreadable or outside
    // 0 <= n < exclusiveMax -> INVALID_COUNTER.
    private static MappingResult<Code> code(JsonNode node, String name, int exclusiveMax) {
        Long value;
        try {
            value = S7commFieldValues.intOrNone(node);
        } catch (IllegalArgumentException e) {
            return MappingResult.invalid(ReasonCode.INVALID_COUNTER, name + " " + e.getMessage());
        }
        if (value == null) {
            return MappingResult.valid(new Code(null));
        }
        if (value < 0 || value >= exclusiveMax) {
            return MappingResult.invalid(ReasonCode.INVALID_COUNTER,
                name + " must be 0 <= n < " + exclusiveMax + ", was " + value);
        }
        return MappingResult.valid(new Code(value.intValue()));
    }
}
