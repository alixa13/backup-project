package io.netsecml.platform.domain.event;

import io.netsecml.platform.domain.feature.ConnSnapshotDelta;
import java.util.Objects;

// A dns.log record: the shared envelope plus the query and response blocks.
//
// sourceIp and isOrig are carried directly rather than through a ConnectionTuple
// because dns.log has no connection state, service or conn_state to put in one --
// the four endpoint fields are all it shares with conn. When EventEnvelope gains
// the Endpoints component its spec section 5.1 already specifies, sourceIp moves
// there and this record loses it; until then it lives here rather than forcing
// DNS to fabricate a ConnectionTuple.
//
// enrichment is a left-join RESULT carried on the record, not a field dns.log
// ever logs. A later unit adds a Flink operator that joins dns.log records
// against conn.log snapshots by uid and re-emits the event with this populated
// via withEnrichment. It is nullable and null by default: null means no conn.log
// snapshot has arrived yet for this uid, which is the ordinary state for a
// connection's first five minutes -- conn.log itself is only written when a
// connection ends or is periodically flushed, so most dns records will be
// scored before any snapshot exists. That is never an error condition; the
// common tier's conn_enrichment_present feature (index 11) exists specifically
// to let a model distinguish "no snapshot yet" from a genuine zero.
//
// This component is populated by no production code yet -- the conn.log join
// operator arrives later in this same unit. That is a deliberate, narrow
// exception to this codebase's rule that a field is added only once something
// stands behind it (the rule LogType and NetworkEvent.permits state about
// themselves). The rule exists so the compiler cannot advertise support that is
// absent; a nullable field whose null case is already the designed-for path
// advertises nothing. The alternative was to carry the join's result in a
// separate stream type, which would break DataStream<NetworkEvent> and force a
// third parameter onto BuildFeaturesUseCase for conn's sake as well.
public record DnsEvent(EventEnvelope envelope, DnsQuery query, DnsResponse response,
                       String sourceIp, boolean isOrig, ConnSnapshotDelta enrichment)
        implements NetworkEvent {

    public DnsEvent {
        Objects.requireNonNull(envelope, "envelope must not be null");

        // A dns record without a query is malformed -- the query is what the
        // record is about, and eight of the twelve protocol features derive from
        // its name.
        Objects.requireNonNull(query, "query must not be null");

        // response is NULLABLE: dns.log records a query that received no answer,
        // and that is a real observation, not a parse failure. The extractor
        // defaults the response features and the caller sets the quality flag.
        if (sourceIp == null || sourceIp.isBlank()) {
            throw new IllegalArgumentException("sourceIp must not be blank");
        }

        // enrichment is intentionally NOT null-checked -- null is its expected
        // default, not an error state. See the class javadoc.
    }

    // Returns a copy carrying the given enrichment. The enrichment operator reads
    // an event off the stream, looks up a conn.log snapshot by uid, and needs to
    // hand a populated event downstream without mutating the one it received --
    // records have no setters, so this is the only way to do that.
    public DnsEvent withEnrichment(ConnSnapshotDelta enrichment) {
        return new DnsEvent(envelope, query, response, sourceIp, isOrig, enrichment);
    }
}
