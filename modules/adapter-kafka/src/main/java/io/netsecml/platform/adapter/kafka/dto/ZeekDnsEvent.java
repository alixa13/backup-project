package io.netsecml.platform.adapter.kafka.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

// Zeek emits more dns.log columns than this platform reads -- rejected, rtt,
// qclass, Z and others -- and emits them on every record. Unknown fields are
// IGNORED, not rejected, matching the promise
// contracts/source/zeek-conn-source-v1.json makes for conn: "unknown additive
// fields are tolerated and ignored". Without this a bare ObjectMapper throws
// UnrecognizedPropertyException on the first real record and every one of them
// lands in the DLQ as MALFORMED_JSON.
@JsonIgnoreProperties(ignoreUnknown = true)
public record ZeekDnsEvent(
    @JsonProperty(value = "id", required = true) String id,
    @JsonProperty(value = "ts", required = true) double ts,
    @JsonProperty(value = "id_orig_h", required = true) String idOrigH,
    @JsonProperty(value = "id_orig_p", required = true) int idOrigP,
    @JsonProperty(value = "id_resp_h", required = true) String idRespH,
    @JsonProperty(value = "id_resp_p", required = true) int idRespP,
    // trans_id is REQUIRED because it is half of this log type's event identity
    // (sensor:uid:trans_id, spec section 5). A resolver reuses one connection for
    // many queries, so several dns.log records legitimately share a uid; without
    // trans_id a record cannot be given a unique id, so its absence is a
    // MAP-stage rejection, not a defaulted field.
    @JsonProperty(value = "trans_id", required = true) int transId,
    @JsonProperty(value = "query", required = true) String query,
    @JsonProperty("qtype") Integer qtype,
    @JsonProperty("rcode") Integer rcode,
    @JsonProperty("AA") Boolean aa,
    @JsonProperty("RA") Boolean ra,
    @JsonProperty("TC") Boolean tc,
    @JsonProperty("answers") List<String> answers,
    @JsonProperty("TTLs") List<Double> ttls
) {
}
