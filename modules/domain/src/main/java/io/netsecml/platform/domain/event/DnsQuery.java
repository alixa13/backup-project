package io.netsecml.platform.domain.event;

import java.util.Objects;

// The query block of a dns.log record. Eight of the twelve protocol features
// (indices 19-23 directly, plus qtype at 13) derive from these three fields, so
// unlike response, query is required -- a dns record without one is not a real
// observation, it is a parse failure.
public record DnsQuery(String name, DnsQType qtype, int transId) {

    public DnsQuery {
        // name feeds qname_length, qname_entropy, label_count, digit_ratio and
        // hyphen_ratio -- a blank name makes every one of those meaningless
        // rather than merely zero, so it is rejected here instead of producing
        // five silently-wrong feature values downstream.
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("name must not be blank");
        }
        Objects.requireNonNull(qtype, "qtype must not be null");
    }
}
