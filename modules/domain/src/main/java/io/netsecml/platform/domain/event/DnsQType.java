package io.netsecml.platform.domain.event;

// DNS query types, numbered by the IANA DNS RRTYPEs registry. Same rationale as
// DnsRcode: the number is the feature value (index 13), fixed by the registry,
// not by the order these constants happen to be declared in.
public enum DnsQType {
    A(1), NS(2), CNAME(5), SOA(6), PTR(12), MX(15), TXT(16), AAAA(28), SRV(33),
    ANY(255), OTHER(-1);

    private final int code;

    DnsQType(int code) {
        this.code = code;
    }

    public int code() {
        return code;
    }

    // A qtype outside this set is real traffic -- the registry defines far more
    // types than this schema enumerates -- so it maps to OTHER rather than
    // throwing. Rejecting the record here would lose a legitimate query.
    public static DnsQType fromCode(int code) {
        for (DnsQType t : values()) {
            if (t.code == code) {
                return t;
            }
        }
        return OTHER;
    }
}
