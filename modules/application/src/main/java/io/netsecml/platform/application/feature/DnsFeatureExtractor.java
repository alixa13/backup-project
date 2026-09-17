package io.netsecml.platform.application.feature;

import io.netsecml.platform.domain.event.DnsEvent;
import io.netsecml.platform.domain.event.DnsQuery;
import io.netsecml.platform.domain.event.DnsResponse;

// Narrowed to DnsEvent rather than the sealed NetworkEvent interface, same
// rationale as EventFeatureExtractor: every line below reads query() or
// response(), so this class IS the dns extractor, not a generic one wearing a
// switch. A later unit's join operator populates DnsEvent.enrichment, which
// belongs to the common tier (CommonFeatureExtractor), not here.
//
// Local index i here becomes schema index i + 12: the 12-value common tier
// leads every per-protocol vector, and dns-feature-v1 is frozen at that offset.
// See contracts/features/dns-feature-schema-v1.json -- indices and missingPolicy
// below are transcribed from that file, not from the order fields appear in
// DnsQuery/DnsResponse, which is a different order.
public final class DnsFeatureExtractor {
    // The width of the array extractProtocolTier returns. DnsBuildFeaturesUseCase
    // checks the registered schema against CommonFeatureTierV1.FEATURE_COUNT +
    // this constant, once, in its own constructor -- so this number and the
    // twelve entries in the array literal below must never drift apart.
    // DnsFeatureExtractorTest.featureCountConstantMatchesTheLengthOfTheArrayItDescribes
    // is the guard.
    public static final int FEATURE_COUNT = 12;

    public float[] extractProtocolTier(DnsEvent event) {
        DnsQuery query = event.query();
        DnsResponse response = event.response();

        // Indices 0, 2-6: response-derived, DEFAULT_ZERO. response is nullable
        // by design -- dns.log records a query that received no answer, and
        // that is a real observation, not a parse failure -- so a null response
        // leaves every local below at its Java default (0) instead of aborting
        // the vector. dns_rcode defaulting to 0 is not the same as NOERROR being
        // observed; a later task records the unanswered case in qualityFlags.
        float rcode = 0f;
        float authoritative = 0f;
        float recursionAvailable = 0f;
        float truncated = 0f;
        float answerCount = 0f;
        float ttl = 0f;
        if (response != null) {
            rcode = response.rcode().code();
            authoritative = response.authoritative() ? 1f : 0f;
            recursionAvailable = response.recursionAvailable() ? 1f : 0f;
            truncated = response.truncated() ? 1f : 0f;
            answerCount = response.answerCount();
            ttl = response.firstTtlSeconds();
        }

        // Indices 1, 7-11: query-derived, REQUIRED. DnsEvent's compact
        // constructor rejects a null query, so these are always computable --
        // an unanswered query still carries the name that was asked for, which
        // is the signal a DGA detector needs regardless of whether anything
        // answered it. Must NOT be gated behind the response != null check
        // above: doing so would make every unanswered query look identical
        // regardless of the name queried, destroying that signal.
        String qname = query.name();

        return new float[]{
            rcode,                                        // 0  dns_rcode
            query.qtype().code(),                          // 1  dns_qtype
            authoritative,                                  // 2  dns_authoritative
            recursionAvailable,                             // 3  dns_recursion_available
            truncated,                                      // 4  dns_truncated
            answerCount,                                    // 5  dns_answer_count
            ttl,                                            // 6  dns_ttl
            QnameFeatures.length(qname),                    // 7  dns_qname_length
            (float) QnameFeatures.shannonEntropy(qname),     // 8  dns_qname_entropy
            QnameFeatures.labelCount(qname),                 // 9  dns_label_count
            (float) QnameFeatures.digitRatio(qname),         // 10 dns_digit_ratio
            (float) QnameFeatures.hyphenRatio(qname)         // 11 dns_hyphen_ratio
        };
    }
}
