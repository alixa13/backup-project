package io.netsecml.platform.domain.event;

import java.util.Objects;

// The response block of a dns.log record. Distinct from DnsQuery because it is
// the part that can be genuinely absent -- see DnsEvent.response's javadoc --
// so this record itself is never null-checked into existence by a caller that
// has no answer to report; DnsEvent simply omits it.
//
// rcodeCode carries the wire rcode exactly as Zeek sent it, for the same reason
// DnsQuery carries qtypeCode: dns-feature-schema-v1 promises index 12 (dns_rcode)
// is "IANA RCODE number", but DnsRcode enumerates only six of the registry's
// values plus OTHER(-1), so response.rcode() alone would collapse every
// unenumerated code (6-10, 16+) to -1, contradicting the frozen contract's own
// text. rcode stays: DnsRcode.isFailure() and the rest of the domain still
// switch on it. rcodeCode is what the feature extractor now reads.
public record DnsResponse(DnsRcode rcode, boolean authoritative, boolean recursionAvailable,
                          boolean truncated, int answerCount, long firstTtlSeconds, int rcodeCode) {

    public DnsResponse {
        // rcode is the one field every response has by construction (even a
        // resolver failure carries a code); the four flags and two counters
        // default to their Java zero values when Zeek omits them, which matches
        // this schema's DEFAULT_ZERO policy for indices 12, 14-18.
        Objects.requireNonNull(rcode, "rcode must not be null");
        // Same drift guard as DnsQuery.qtypeCode: OTHER is the legitimate
        // exception (fromCode's catch-all for a code the enum does not list),
        // but for every enumerated value rcodeCode must be exactly rcode.code()
        // -- both come from the same wire value, so disagreement means a caller
        // bug, not a real dns.log record.
        if (rcode != DnsRcode.OTHER && rcodeCode != rcode.code()) {
            throw new IllegalArgumentException(
                "rcodeCode " + rcodeCode + " does not match rcode " + rcode + "'s code " + rcode.code());
        }
    }
}
