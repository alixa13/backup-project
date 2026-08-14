package io.netsecml.platform.domain.feature;

import java.util.List;
import static io.netsecml.platform.domain.feature.FeatureDefinition.MissingPolicy.DEFAULT_ZERO;
import static io.netsecml.platform.domain.feature.FeatureDefinition.MissingPolicy.REQUIRED;

public final class ConnFeatureSchemaV1 {
    public static final String CONTENT_HASH =
        "f42fb1bebb2efe3acc5de634c6a7bb3d6f97fc021207f75d77652533b1c01e1b";

    public static final FeatureSchema SCHEMA = new FeatureSchema(
        "conn-feature-v1", "1.0.0", CONTENT_HASH, List.of(
            new FeatureDefinition(0, "duration_ms", "milliseconds", DEFAULT_ZERO, "zeek duration seconds * 1000, non-negative"),
            new FeatureDefinition(1, "origin_bytes", "bytes", DEFAULT_ZERO, "orig_bytes, non-negative"),
            new FeatureDefinition(2, "response_bytes", "bytes", DEFAULT_ZERO, "resp_bytes, non-negative"),
            new FeatureDefinition(3, "origin_packets", "count", DEFAULT_ZERO, "orig_pkts, non-negative"),
            new FeatureDefinition(4, "response_packets", "count", DEFAULT_ZERO, "resp_pkts, non-negative"),
            new FeatureDefinition(5, "total_bytes", "bytes", DEFAULT_ZERO, "origin_bytes + response_bytes"),
            new FeatureDefinition(6, "total_packets", "count", DEFAULT_ZERO, "origin_packets + response_packets"),
            new FeatureDefinition(7, "bytes_per_packet", "bytes", DEFAULT_ZERO, "total_bytes / max(1, total_packets)"),
            new FeatureDefinition(8, "response_origin_byte_ratio", "ratio", DEFAULT_ZERO, "response_bytes / max(1, origin_bytes)"),
            new FeatureDefinition(9, "destination_port", "port", REQUIRED, "id_resp_p, exact integer as float32"),
            new FeatureDefinition(10, "destination_is_well_known", "boolean", REQUIRED, "1 if destination_port in [1,1023] else 0"),
            new FeatureDefinition(11, "protocol_tcp", "boolean", REQUIRED, "1 if proto == tcp else 0"),
            new FeatureDefinition(12, "protocol_udp", "boolean", REQUIRED, "1 if proto == udp else 0"),
            new FeatureDefinition(13, "service_dns", "boolean", REQUIRED, "1 if service == dns else 0"),
            new FeatureDefinition(14, "service_http", "boolean", REQUIRED, "1 if service == http else 0"),
            new FeatureDefinition(15, "service_ssl", "boolean", REQUIRED, "1 if service == ssl else 0"),
            new FeatureDefinition(16, "connection_failed", "boolean", REQUIRED, "1 if conn_state in [S0,REJ,RSTO,RSTR] else 0"),
            new FeatureDefinition(17, "source_connections_5m", "count", DEFAULT_ZERO, "count of prior connections for (sensor,sourceIp) in trailing 5 one-minute buckets"),
            new FeatureDefinition(18, "source_bytes_5m", "bytes", DEFAULT_ZERO, "total_bytes sum for (sensor,sourceIp) in trailing 5 one-minute buckets"),
            new FeatureDefinition(19, "source_failed_connections_5m", "count", DEFAULT_ZERO, "connection_failed count for (sensor,sourceIp) in trailing 5 one-minute buckets")
        ));

    private ConnFeatureSchemaV1() {
    }
}
