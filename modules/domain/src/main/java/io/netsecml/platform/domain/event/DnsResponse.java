package io.netsecml.platform.domain.event;

import java.util.Objects;

// The response block of a dns.log record. Distinct from DnsQuery because it is
// the part that can be genuinely absent -- see DnsEvent.response's javadoc --
// so this record itself is never null-checked into existence by a caller that
// has no answer to report; DnsEvent simply omits it.
public record DnsResponse(DnsRcode rcode, boolean authoritative, boolean recursionAvailable,
                          boolean truncated, int answerCount, long firstTtlSeconds) {

    public DnsResponse {
        // rcode is the one field every response has by construction (even a
        // resolver failure carries a code); the four flags and two counters
        // default to their Java zero values when Zeek omits them, which matches
        // this schema's DEFAULT_ZERO policy for indices 12, 14-18.
        Objects.requireNonNull(rcode, "rcode must not be null");
    }
}
