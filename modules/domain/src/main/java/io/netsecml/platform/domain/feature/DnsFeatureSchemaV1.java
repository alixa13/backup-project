package io.netsecml.platform.domain.feature;

import java.util.List;
import static io.netsecml.platform.domain.feature.FeatureDefinition.MissingPolicy.DEFAULT_ZERO;
import static io.netsecml.platform.domain.feature.FeatureDefinition.MissingPolicy.REQUIRED;

// The frozen dns.log feature schema: the common tier (indices 0-11) followed by
// twelve DNS-specific values (12-23). 24, not the 25 spec section 4.1 lists --
// dns_ngram_score is excluded because it needs a data-derived reference corpus
// that does not exist yet, unlike dns_rcode/dns_qtype which map to fixed IANA
// registry numbers. See the plan's ruling 1 for the full reasoning; a v2 schema
// can add it once a frequency table is committed to contracts/.
public final class DnsFeatureSchemaV1 {
    public static final String CONTENT_HASH =
        "41bde5d99886f8343766d6f593ab33bf2ebfc1bcfebcf41ad1da775856c2439f";

    public static final FeatureSchema SCHEMA = new FeatureSchema(
        "dns-feature-v1", "1.0.0", CONTENT_HASH, List.of(
            // Indices 0-11: the common tier, copied verbatim (same names, order,
            // units, missingPolicy, formulas) from common-feature-tier-v1.json --
            // DnsFeatureSchemaV1Test.theFirstTwelveFeaturesAreTheCommonTierInOrder
            // is the guard against these two ever drifting apart.
            new FeatureDefinition(0, "record_count_5m", "count", DEFAULT_ZERO, "records for (sensor, sourceIp) key in trailing 5-minute rolling window"),
            new FeatureDefinition(1, "byte_sum_5m", "bytes", DEFAULT_ZERO, "byte total for (sensor, sourceIp) key in trailing 5-minute rolling window"),
            new FeatureDefinition(2, "failed_count_5m", "count", DEFAULT_ZERO, "failed records for (sensor, sourceIp) key in trailing 5-minute rolling window"),
            new FeatureDefinition(3, "inter_arrival_mean_ms", "milliseconds", DEFAULT_ZERO, "running mean interval between records for (sensor, sourceIp) key"),
            new FeatureDefinition(4, "inter_arrival_stddev_ms", "milliseconds", DEFAULT_ZERO, "running population stddev of the inter-arrival interval for (sensor, sourceIp) key"),
            new FeatureDefinition(5, "is_orig", "boolean", REQUIRED, "1 if the record was sent by the connection originator else 0"),
            new FeatureDefinition(6, "conn_orig_bytes", "bytes", DEFAULT_ZERO, "orig_bytes delta since the previous conn.log snapshot"),
            new FeatureDefinition(7, "conn_resp_bytes", "bytes", DEFAULT_ZERO, "resp_bytes delta since the previous conn.log snapshot"),
            new FeatureDefinition(8, "conn_orig_pkts", "count", DEFAULT_ZERO, "orig_pkts delta since the previous conn.log snapshot"),
            new FeatureDefinition(9, "conn_resp_pkts", "count", DEFAULT_ZERO, "resp_pkts delta since the previous conn.log snapshot"),
            new FeatureDefinition(10, "conn_age_seconds", "seconds", DEFAULT_ZERO, "connection age at the time of the latest conn.log snapshot"),
            new FeatureDefinition(11, "conn_enrichment_present", "boolean", REQUIRED, "1 if a conn.log snapshot was available for this record else 0"),

            // Indices 12-23: DNS's own twelve. The missingPolicy split is
            // deliberate -- 19-23 are REQUIRED because they derive from `query`,
            // which DnsEvent requires and can therefore always compute; 12 and
            // 14-18 are DEFAULT_ZERO because `response` is nullable and a query
            // with no answer is a real observation, not a missing value. 13
            // (qtype) is REQUIRED because qtype is part of the query, not the
            // response.
            new FeatureDefinition(12, "dns_rcode", "code", DEFAULT_ZERO, "IANA RCODE number; 0 (NOERROR) when no response"),
            new FeatureDefinition(13, "dns_qtype", "code", REQUIRED, "IANA QTYPE number from qtype"),
            new FeatureDefinition(14, "dns_authoritative", "boolean", DEFAULT_ZERO, "1 if AA else 0"),
            new FeatureDefinition(15, "dns_recursion_available", "boolean", DEFAULT_ZERO, "1 if RA else 0"),
            new FeatureDefinition(16, "dns_truncated", "boolean", DEFAULT_ZERO, "1 if TC else 0"),
            new FeatureDefinition(17, "dns_answer_count", "count", DEFAULT_ZERO, "length of answers"),
            new FeatureDefinition(18, "dns_ttl", "seconds", DEFAULT_ZERO, "first element of TTLs"),
            new FeatureDefinition(19, "dns_qname_length", "count", REQUIRED, "character count of query"),
            new FeatureDefinition(20, "dns_qname_entropy", "bits", REQUIRED, "Shannon entropy over query characters"),
            new FeatureDefinition(21, "dns_label_count", "count", REQUIRED, "dot-separated label count of query"),
            new FeatureDefinition(22, "dns_digit_ratio", "ratio", REQUIRED, "digits / length of query"),
            new FeatureDefinition(23, "dns_hyphen_ratio", "ratio", REQUIRED, "hyphens / length of query")
        ));

    private DnsFeatureSchemaV1() {
    }
}
