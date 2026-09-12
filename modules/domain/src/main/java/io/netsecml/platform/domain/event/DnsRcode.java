package io.netsecml.platform.domain.event;

// DNS response codes, numbered by the IANA DNS RCODEs registry. The numeric
// value IS the feature value (index 12), so these numbers are contract, not
// implementation detail -- reordering the constants would not change them, and
// that is the point of writing them explicitly.
public enum DnsRcode {
    NOERROR(0), FORMERR(1), SERVFAIL(2), NXDOMAIN(3), NOTIMP(4), REFUSED(5),
    OTHER(-1);

    private final int code;

    DnsRcode(int code) {
        this.code = code;
    }

    public int code() {
        return code;
    }

    // An rcode outside the set above is real -- the registry has more values than
    // any schema should enumerate -- so it maps to OTHER rather than throwing. A
    // rejected record would lose a legitimate observation.
    public static DnsRcode fromCode(int code) {
        for (DnsRcode r : values()) {
            if (r.code == code) {
                return r;
            }
        }
        return OTHER;
    }

    // NOERROR is the only non-failure. The rolling window's failedCount5m for DNS
    // is "responses that did not succeed", which is this predicate.
    public boolean isFailure() {
        return this != NOERROR;
    }
}
