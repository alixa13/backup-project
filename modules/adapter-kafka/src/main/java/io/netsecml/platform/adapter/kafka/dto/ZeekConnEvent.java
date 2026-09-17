package io.netsecml.platform.adapter.kafka.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

// Zeek emits more conn.log columns than this platform reads -- history,
// tunnel_parents, orig_ip_bytes and others -- and a Zeek upgrade or a new script
// can add more at any time. Unknown fields are IGNORED, not rejected, because
// contracts/source/zeek-conn-source-v1.json promises exactly that: "unknown
// additive fields are tolerated and ignored". Without this a bare ObjectMapper
// throws UnrecognizedPropertyException on the first real record and every one
// of them lands in the DLQ as MALFORMED_JSON.
@JsonIgnoreProperties(ignoreUnknown = true)
public record ZeekConnEvent(
    @JsonProperty(value = "id", required = true) String id,
    @JsonProperty(value = "ts", required = true) double ts,
    @JsonProperty(value = "id_orig_h", required = true) String idOrigH,
    @JsonProperty(value = "id_orig_p", required = true) int idOrigP,
    @JsonProperty(value = "id_resp_h", required = true) String idRespH,
    @JsonProperty(value = "id_resp_p", required = true) int idRespP,
    @JsonProperty(value = "proto", required = true) String proto,
    @JsonProperty("service") String service,
    @JsonProperty(value = "conn_state", required = true) String connState,
    @JsonProperty("duration") Double duration,
    @JsonProperty("orig_bytes") Long origBytes,
    @JsonProperty("resp_bytes") Long respBytes,
    @JsonProperty("orig_pkts") Long origPkts,
    @JsonProperty("resp_pkts") Long respPkts,
    @JsonProperty("missed_bytes") Long missedBytes,
    @JsonProperty("local_orig") Boolean localOrig,
    @JsonProperty("local_resp") Boolean localResp
) {
}
