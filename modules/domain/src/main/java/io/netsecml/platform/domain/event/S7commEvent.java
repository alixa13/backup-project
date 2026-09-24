package io.netsecml.platform.domain.event;

import java.util.Objects;

// One ICSNPP s7comm.log record: a single S7comm PDU, a request or a response.
//
// sourceIp/sourcePort and destinationIp/destinationPort are the sender and the
// receiver of THIS record (per-packet), never the connection's originator and
// responder. S7commEventMapper resolves them: it takes a record's per-packet
// source_*/destination_* fields as they are, and otherwise orients Zeek's
// connection-level id_orig_*/id_resp_* pair -- identical on a request and on
// its response -- by is_orig. Direction is then upstream's own rule,
// isRequest(): a record sent TO port 102 is a request.
//
// rosctrCode and functionCode are null when the record carried none.
// functionName is kept only for the categorical fallback S7commCategories
// applies when there is no function code, and is null when absent or empty.
public record S7commEvent(EventEnvelope envelope, double tsSeconds, String sourceIp, int sourcePort,
                          String destinationIp, int destinationPort, int pduReference, Integer rosctrCode,
                          Integer functionCode, String functionName) implements NetworkEvent {

    // The S7comm (ISO-on-TCP) service port. Upstream: is_request = destination_p == 102.
    public static final int S7_PORT = 102;

    // The S7 header's PDU reference is a 16-bit field.
    public static final int MAX_PDU_REFERENCE = 0xFFFF;

    // Every integer below 2^24 is exact in a float32 vector slot, which is where
    // the two codes end up (s7comm-feature-v1 indices 14 and 15).
    public static final int MAX_CODE_EXCLUSIVE = 1 << 24;

    public S7commEvent {
        Objects.requireNonNull(envelope, "envelope must not be null");

        // tsSeconds only orders records (and flags an out-of-order one); no
        // s7comm-feature-v1 feature reads time. It must still be a real number.
        if (!Double.isFinite(tsSeconds)) {
            throw new IllegalArgumentException("tsSeconds must be finite, was " + tsSeconds);
        }

        // Both endpoints are structural: direction and the event id depend on them.
        requireHost(sourceIp, "sourceIp");
        requireHost(destinationIp, "destinationIp");
        requirePort(sourcePort, "sourcePort");
        requirePort(destinationPort, "destinationPort");

        // The mapper range-checks all three before constructing, so these
        // throws are defended invariants, not reachable DLQ gaps.
        if (pduReference < 0 || pduReference > MAX_PDU_REFERENCE) {
            throw new IllegalArgumentException("pduReference must be 0-65535, was " + pduReference);
        }
        requireCode(rosctrCode, "rosctrCode");
        requireCode(functionCode, "functionCode");

        // "absent" has one representation, so S7commCategories need not test two.
        if (functionName != null && functionName.isEmpty()) {
            throw new IllegalArgumentException("functionName must be null rather than empty");
        }
    }

    // Upstream's direction rule, verbatim: destination port 102 means request.
    public boolean isRequest() {
        return destinationPort == S7_PORT;
    }

    private static void requireHost(String host, String name) {
        if (host == null || host.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
    }

    private static void requirePort(int port, String name) {
        if (port < 0 || port > 0xFFFF) {
            throw new IllegalArgumentException(name + " must be 0-65535, was " + port);
        }
    }

    private static void requireCode(Integer code, String name) {
        if (code != null && (code < 0 || code >= MAX_CODE_EXCLUSIVE)) {
            throw new IllegalArgumentException(name + " must be null or 0 <= n < 2^24, was " + code);
        }
    }
}
