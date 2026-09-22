package io.netsecml.platform.adapter.kafka.dto;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

// A single ICSNPP modbus_detailed.log record (icsnpp-modbus package), NOT
// base Zeek's own modbus.log -- see contracts/source/zeek-modbus-source-v1.json
// and ModbusFeatureSchemaV1's javadoc for why only the detailed log carries
// address, quantity, request_values and response_values, none of which base
// modbus.log has at all, and without which the frozen modbus-feature-v1
// features that read them could not be computed.
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

    // Endpoint hosts: underscored id_orig_h/id_resp_h is PRIMARY because it is
    // what this platform's sensor actually emits and what the working
    // end-to-end fixtures carry -- ZeekConnEvent and ZeekDnsEvent both bind
    // exactly these underscored names as required=true, and both protocols
    // consume real Kafka records through passing Testcontainers E2E tests.
    // The dotted id.orig_h/id.resp_h form is Zeek's own native JSON naming
    // (and the design spec's own table lists it first), and source_h/
    // destination_h is the plugin's alternate normalized pair; both are
    // accepted as aliases so this parser does not depend on which spelling a
    // given sensor build produces, but underscored is what to expect from
    // THIS deployment's wire.
    @JsonProperty("id_orig_h") @JsonAlias({"id.orig_h", "source_h"}) String sourceHost,
    @JsonProperty("id_resp_h") @JsonAlias({"id.resp_h", "destination_h"}) String destinationHost,

    // Direction of THIS record (request vs. response), FALLBACK source.
    // requestResponse (below) is the PRIMARY direction source per the design
    // spec's §5 table; is_orig is read only when requestResponse is absent.
    // Neither is required=true here: resolving that either/or, and rejecting
    // a record where NEITHER resolves, is a MAP-stage obligation per
    // ModbusEvent's javadoc ("an unresolvable direction is a DLQ rejection,
    // never a guess") -- a Jackson-level required=true cannot express an
    // either/or across two independently named keys, so this parser leaves
    // both nullable and lets the mapper enforce the real rule.
    @JsonProperty("is_orig") Boolean isOrig,

    // Direction of THIS record, PRIMARY source: the design spec's §5 table
    // names request_response (or its network_direction alias) as the field
    // to check BEFORE falling back to is_orig above. Nullable for the same
    // either/or reason isOrig is nullable: a record may legitimately carry
    // only is_orig, and the mapper -- not this parser -- decides whether the
    // two together resolve to a usable direction.
    @JsonProperty("request_response") @JsonAlias("network_direction") String requestResponse,

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
