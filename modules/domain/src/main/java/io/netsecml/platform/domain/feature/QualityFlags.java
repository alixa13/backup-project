package io.netsecml.platform.domain.feature;

// Bit flags recording how a feature vector was produced, not what it observed.
//
// FeatureVector.qualityFlags has been hard-coded 0 since it was defined. This is
// its first real use:
// docs/superpowers/specs/2026-09-10-per-protocol-feature-schemas-design.md
// section 6.2 requires that a vector built without a conn.log snapshot be
// distinguishable from one where the connection genuinely moved no bytes. The
// common tier's conn_enrichment_present (index 11) already records it inside
// the vector for the model; this flag records it OUTSIDE the values, so a query
// over archived rows can filter on provenance without knowing any schema's
// index layout.
public final class QualityFlags {

    public static final int NONE = 0;

    // BIT LAYOUT CONVENTION for protocols 3-6 (HTTP, SSH, Modbus, S7comm): bit
    // 0 (CONN_ENRICHMENT_ABSENT) is PROTOCOL-AGNOSTIC -- every protocol that
    // consumes the common tier's conn.log enrichment shares this exact bit,
    // rather than each protocol defining its own copy of it. Bit 1 onward is
    // where each protocol's OWN bits start; DNS claims only bit 1
    // (DNS_RESPONSE_ABSENT) here, so bit 1 remains free for the next
    // protocol's own first flag -- a future protocol must not reuse bit 1 for
    // an unrelated meaning while DNS vectors carrying it are still archived,
    // since a consumer reads this word relative to a row's own `log_type` and
    // has no other way to know which protocol's bit convention applied when
    // the row was written.
    public static final int CONN_ENRICHMENT_ABSENT = 1;

    // No dns.log answer had arrived for this query at build time. The frozen
    // dns-feature-v1 schema cannot record this itself: dns_rcode (index 12) is
    // DEFAULT_ZERO, and NOERROR's own IANA code is also 0, so a genuinely
    // successful lookup and a query nothing ever answered write the identical
    // value at that index. DnsFeatureExtractor's extractProtocolTier already
    // promised, in its own comment, that "a later task records the unanswered
    // case in qualityFlags" -- this bit is that promise kept. Distinct from
    // CONN_ENRICHMENT_ABSENT above: that bit is about the common tier's conn.log
    // join, this one is about the DNS protocol tier's own response, and either
    // can be set independently of the other for the same record.
    public static final int DNS_RESPONSE_ABSENT = 2;

    // Non-instantiable: every member is a constant.
    private QualityFlags() {
    }
}
