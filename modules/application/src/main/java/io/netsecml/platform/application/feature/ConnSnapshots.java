package io.netsecml.platform.application.feature;

import io.netsecml.platform.domain.event.ConnEvent;
import io.netsecml.platform.domain.feature.ConnSnapshot;
import java.time.Instant;
import java.util.Optional;

// The producer side of the conn.log enrichment left join
// (docs/superpowers/specs/2026-09-10-per-protocol-feature-schemas-design.md
// section 6.2): turns one parsed conn.log record into the cumulative snapshot
// ConnSnapshotJoinFunction (adapter-flink) keys its state on. Pure and
// side-effect-free so it is unit-testable here, in application, rather than
// only reachable through a Flink harness.
public final class ConnSnapshots {

    // Non-instantiable: every member is static, same convention as
    // CommonFeatureExtractor and DnsFeatureExtractor in this package.
    private ConnSnapshots() {
    }

    // Returns Optional.empty() rather than throwing on a blank connectionUid.
    // ConnSnapshot's own compact constructor throws IllegalArgumentException on
    // exactly that input, and this method runs inside a Flink FlatMapFunction
    // (ConnSnapshotExtractFunction) where an escaping exception crash-loops the
    // whole job on one bad record -- so the blank check happens here, first,
    // ahead of the throwing constructor, turning "reject" into "skip".
    //
    // Unreachable for conn today: EventMapper (adapter-kafka) derives every
    // ConnEvent's uid via EventId.derive(sensor, dto.id()), and derive itself
    // throws IllegalArgumentException on a blank upstream id, so no ConnEvent
    // with a blank connectionUid can reach this method from production wiring.
    // The check stays anyway -- ConnEvent's own type does not forbid a blank
    // uid (EventEnvelope's compact constructor normalises only null, not
    // blank, to "") -- so this method must not silently assume an invariant
    // that only EventMapper happens to enforce today.
    public static Optional<ConnSnapshot> fromConnEvent(ConnEvent event) {
        String connectionUid = event.connectionUid();
        if (connectionUid == null || connectionUid.isBlank()) {
            return Optional.empty();
        }

        // connectionStart is the connection's FIRST packet. Zeek's conn.log ts
        // is that on every record it writes for a connection -- interim
        // five-minute flushes and the final record alike -- so eventTime maps
        // straight across, never adjusted by duration.
        Instant connectionStart = event.eventTime();

        // observedAt has no field of its own on conn.log: it is how far past
        // connectionStart this particular snapshot's cumulative counters run,
        // i.e. connectionStart + the duration this record reports.
        Instant observedAt = connectionStart.plusMillis(event.measurements().durationMillis());

        // The four counters are cumulative totals since the connection began
        // (ConnSnapshot's own javadoc), copied straight from measurements() --
        // packet counts widen int -> long implicitly on the constructor call.
        return Optional.of(new ConnSnapshot(
            connectionUid,
            connectionStart,
            observedAt,
            event.measurements().originBytes(),
            event.measurements().responseBytes(),
            event.measurements().originPackets(),
            event.measurements().responsePackets()));
    }
}
