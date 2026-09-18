package io.netsecml.platform.domain.event;

import java.util.Objects;

// The query block of a dns.log record. Eight of the twelve protocol features
// (indices 19-23 directly, plus qtype at 13) derive from these three fields, so
// unlike response, query is required -- a dns record without one is not a real
// observation, it is a parse failure.
//
// qtypeCode carries the wire qtype exactly as Zeek sent it. dns-feature-schema-v1
// promises index 13 (dns_qtype) is "the IANA QTYPE number from qtype", but DnsQType
// enumerates only ten of the registry's values plus OTHER(-1) -- so query.qtype()
// alone would collapse every unenumerated type (65/HTTPS-SVCB among them, a large
// and growing share of live traffic) to the same -1, contradicting the frozen
// contract's own text. qtype stays: it is still the named semantics the rest of
// the domain switches on. qtypeCode is what the feature extractor now reads.
public record DnsQuery(String name, DnsQType qtype, int transId, int qtypeCode) {

    public DnsQuery {
        // name feeds qname_length, qname_entropy, label_count, digit_ratio and
        // hyphen_ratio -- a blank name makes every one of those meaningless
        // rather than merely zero, so it is rejected here instead of producing
        // five silently-wrong feature values downstream.
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("name must not be blank");
        }
        Objects.requireNonNull(qtype, "qtype must not be null");
        // OTHER is DnsQType.fromCode's catch-all for a code the enum does not
        // list, so qtypeCode legitimately disagrees with qtype.code() (-1) in
        // that one case. For every enumerated value the two must never drift
        // apart -- the enum names a specific IANA number, and qtypeCode is
        // that same number by construction (DnsEventMapper passes both through
        // from the one wire value), so a mismatch here is a caller bug, not a
        // real dns.log record.
        if (qtype != DnsQType.OTHER && qtypeCode != qtype.code()) {
            throw new IllegalArgumentException(
                "qtypeCode " + qtypeCode + " does not match qtype " + qtype + "'s code " + qtype.code());
        }
    }
}
