package io.netsecml.platform.adapter.kafka.dto;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

// A single ICSNPP modbus_detailed.log record (icsnpp-modbus package), NOT
// base Zeek's own modbus.log -- see contracts/source/zeek-modbus-source-v1.json
// and ModbusFeatureSchemaV1's javadoc for why only the detailed log carries
// address, quantity, request_values and response_values, without which 15 of
// the 42 frozen modbus-feature-v1 features could not be computed.
//
// This is a pure binding step: it stops at "did the JSON bind to these
// fields", never at domain validation. Resolving direction (isOrig),
// defaulting an absent unit to "NA", resolving func to a numeric code, and
// deciding whether address/quantity/matched/the value arrays are usable all
// belong to the mapper that consumes this DTO -- ModbusEvent's own javadoc
// says as much for direction and func.
//
// Unknown fields are IGNORED, not rejected: modbus_detailed.log carries
// several columns this DTO never reads (exception_code, request_data,
// response_data, request_subfunction_code, response_subfunction_code,
// mei_type, modbus_detailed_link_id -- see the source contract's
// excludedRawFields), and without ignoreUnknown every real record would
// throw UnrecognizedPropertyException and route every record to the DLQ,
// the same production-shaped gap ZeekDnsEvent's own comment describes for
// dns.log's always-present "rejected"/"rtt"/"qclass"/"Z" columns.
@JsonIgnoreProperties(ignoreUnknown = true)
public record ZeekModbusRecord(
    @JsonProperty(value = "ts", required = true) double ts,
    @JsonProperty(value = "uid", required = true) String uid,

    // Endpoint hosts: some Zeek/ICSNPP configurations emit the raw
    // connection-id fields (id.orig_h/id.resp_h, matching conn/dns's own
    // wire shape), others emit the plugin's normalized source_h/destination_h
    // pair instead. Both spellings are accepted so this parser does not
    // depend on which one a given sensor build produces.
    @JsonProperty("id.orig_h") @JsonAlias("source_h") String sourceHost,
    @JsonProperty("id.resp_h") @JsonAlias("destination_h") String destinationHost,

    // Direction of THIS record (request vs. response): the source contract
    // marks is_orig required, but that is a MAP-stage (direction-resolution)
    // obligation per ModbusEvent's javadoc, not a PARSE-stage one -- an
    // unresolvable direction is a DLQ rejection the mapper makes deliberately,
    // never a guess this parser should make by treating absence as malformed.
    @JsonProperty("is_orig") Boolean isOrig,

    // Modbus transaction id: keys the pending-TID state the mapper/causal
    // engine build on top of this record. required=true only enforces that
    // the "tid" KEY is present; explicit null is still stopped, because this
    // parser's ObjectMapper enables FAIL_ON_NULL_FOR_PRIMITIVES (see
    // JsonZeekModbusParser) -- without it, "tid": null would silently bind to
    // 0, a plausible-looking transaction id, rather than fail to parse.
    @JsonProperty(value = "tid", required = true) int tid,

    // Modbus unit id: some builds emit "unit", others "uint" -- both are
    // accepted and treated identically. Bound as a String (not int) because
    // ModbusEvent.unitId is itself a String: the upstream engine falls back
    // to a literal "NA" sentinel for an absent unit rather than dropping the
    // record, and that fallback -- like everything else domain-validation
    // shaped -- is the next task's mapper's job, not this parser's.
    @JsonProperty("unit") @JsonAlias("uint") String unitId,

    // Modbus function code. Bound as a String, deliberately: ICSNPP emits it
    // as a name ("READ_HOLDING_REGISTERS") in some configurations and as a
    // bare number in others, and Jackson coerces a JSON number into a String
    // target automatically, so one field handles both shapes without this
    // parser having to sniff which configuration produced the record.
    // Resolving the name/number to a numeric code against the frozen
    // func-name table belongs to the next task's mapper.
    //
    // required=true only enforces that the "func" KEY is present, not that
    // its value is non-null -- and because func is a String (a reference
    // type), FAIL_ON_NULL_FOR_PRIMITIVES does NOT cover it (that flag is
    // primitives-only). An explicit "func": null would therefore still bind
    // cleanly as a null String if nothing else stopped it. JsonZeekModbusParser
    // checks for that by hand after binding, for the same reason
    // FAIL_ON_NULL_FOR_PRIMITIVES exists for ts/tid above: a defaulted or
    // silently-absent function code is a plausible-looking one and must be a
    // parse failure, not a record forwarded for scoring with func == null.
    @JsonProperty(value = "func", required = true) String func,

    // Register/coil address and count. Both optional and boxed (not
    // primitive): their absence is itself meaningful downstream
    // (ModbusFeatureSchemaV1's address_present/quantity_present mask
    // features), so this DTO must be able to represent "field absent" as
    // null rather than coercing it to a primitive default.
    @JsonProperty("address") Double address,
    @JsonProperty("quantity") Double quantity,

    // Response causal-match flag; meaningful only on a response record,
    // ignored on a request -- which of those this record is isOrig decides,
    // not this DTO.
    @JsonProperty("matched") Boolean matched,

    // Strict JSON arrays of numbers only. The offline research engine falls
    // back to ast.literal_eval to tolerate a Python-repr string
    // (e.g. "[7, 9]") read back from a research file; that fallback must NOT
    // be reproduced here, because on a production Kafka wire it would accept
    // a malformed payload -- a JSON string where an array belongs -- instead
    // of rejecting it. Jackson's default (no ACCEPT_SINGLE_VALUE_AS_ARRAY)
    // already refuses to bind a scalar string into a List, which is exactly
    // the strictness this parser wants.
    @JsonProperty("request_values") List<Double> requestValues,
    @JsonProperty("response_values") List<Double> responseValues
) {
}
