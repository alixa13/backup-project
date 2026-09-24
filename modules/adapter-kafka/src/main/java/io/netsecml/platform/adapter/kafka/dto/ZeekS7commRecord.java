package io.netsecml.platform.adapter.kafka.dto;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.Objects;

// One ICSNPP s7comm.log line, kept as the parsed JSON object rather than bound
// field by field. Unlike conn, dns and modbus, whose fields have one spelling
// each, upstream's production adapter (two-models-info/S7___/kafka_source.py)
// accepts several aliases per field (pdu_reference / pdu_ref / pdu_ref_num,
// source_h / src_h / src, a flat or nested id, ...) and several JSON types per
// value (a number, "0x04", "4"). S7commEventMapper resolves each canonical
// field from this tree in upstream's own order, so the alias lists sit in one
// place, beside the rule that reads them.
public record ZeekS7commRecord(JsonNode json) {

    // Only a JSON object is a record; JsonZeekS7commParser rejects anything
    // else before constructing one.
    public ZeekS7commRecord {
        Objects.requireNonNull(json, "json must not be null");
        if (!json.isObject()) {
            throw new IllegalArgumentException("an s7comm record must be a JSON object");
        }
    }

    // The uid's text, or null when it is absent or JSON null -- read directly
    // because the DLQ event id needs it even when mapping fails.
    public String uid() {
        JsonNode uid = json.get("uid");
        return uid == null || uid.isNull() ? null : uid.asText();
    }
}
