package io.netsecml.platform.domain.feature;

import java.time.Duration;
import java.time.Instant;

// One observation of a Zeek conn.log record for a connection that may still be
// open. The sensors are configured to emit conn.log every 5 minutes rather than
// only at connection close, so a long-lived connection produces a series of
// these.
//
// The counters are CUMULATIVE totals since the connection began, not per-interval
// values. Any rate feature must therefore be computed as the difference between
// two consecutive snapshots -- see deltaFrom.
public record ConnSnapshot(String connectionUid, Instant connectionStart, Instant observedAt,
                           long origBytes, long respBytes, long origPkts, long respPkts) {

    public ConnSnapshot {
        // Without a uid this snapshot cannot be joined to a protocol record, which
        // is its only purpose.
        if (connectionUid == null || connectionUid.isBlank()) {
            throw new IllegalArgumentException("connectionUid must not be blank");
        }
        if (connectionStart == null || observedAt == null) {
            throw new IllegalArgumentException("connectionStart and observedAt must not be null");
        }
    }

    // The per-interval difference against the previous snapshot of the SAME
    // connection. Returned as a ConnSnapshotDelta to keep delta counters
    // type-distinct from cumulative observations.
    public ConnSnapshotDelta deltaFrom(ConnSnapshot previous) {
        // Subtracting across connections would blend unrelated traffic into one
        // rate, so it is a programming error rather than a recoverable case.
        if (previous == null || !previous.connectionUid.equals(connectionUid)) {
            throw new IllegalArgumentException(
                "deltaFrom requires a previous snapshot of the same connection: " + connectionUid);
        }
        return new ConnSnapshotDelta(
            nonNegativeDifference(origBytes, previous.origBytes),
            nonNegativeDifference(respBytes, previous.respBytes),
            nonNegativeDifference(origPkts, previous.origPkts),
            nonNegativeDifference(respPkts, previous.respPkts),
            ageSeconds());
    }

    // A counter that went backwards means Zeek restarted or the connection was
    // re-keyed. Zero is the honest answer; a negative rate is not a signal the
    // model can use.
    private static long nonNegativeDifference(long current, long previous) {
        return Math.max(0L, current - previous);
    }

    // How long the connection had been alive when this snapshot was taken. This
    // is the feature that separates a persistent SCADA poll loop from a short
    // burst of traffic. A sensor with clock skew or out-of-order record can put
    // observedAt before connectionStart; a negative age is not a signal the model
    // can use, so it clamps to zero matching the philosophy used for counters.
    public long ageSeconds() {
        long age = Duration.between(connectionStart, observedAt).getSeconds();
        return Math.max(0L, age);
    }
}
