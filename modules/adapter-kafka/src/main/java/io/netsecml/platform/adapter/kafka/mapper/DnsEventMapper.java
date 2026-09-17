package io.netsecml.platform.adapter.kafka.mapper;

import io.netsecml.platform.adapter.kafka.dto.ZeekDnsEvent;
import io.netsecml.platform.domain.event.*;
import java.time.Instant;

// Turns a parsed dns.log record into a validated DnsEvent. Mirrors EventMapper's
// idiom deliberately: this runs inside a Flink processElement too, so no
// exception may escape map() -- an escaping exception fails the subtask, the
// restart strategy replays the same record, and one malformed record
// crash-loops the whole job. Every field a domain constructor would reject is
// therefore validated here BEFORE that constructor is called, and returned as
// MappingResult.invalid(...) instead of thrown. Jackson's required=true on
// ZeekDnsEvent only proves a field was PRESENT -- "query": "" parses cleanly --
// so PRESENT is not the same guarantee as the domain layer needs.
public final class DnsEventMapper {
    public MappingResult<NetworkEvent> map(ZeekDnsEvent dto, SensorId sensor) {
        // id must be checked blank BEFORE it is used to derive the event id
        // below. dto.id() + ":" + dto.transId() turns a blank id into ":4242"
        // -- which is NOT blank -- so EventId.derive's own guard would never
        // fire and a garbage identity would be silently accepted. Same check,
        // same order, as EventMapper's first line.
        if (dto.id() == null || dto.id().isBlank()) {
            return MappingResult.invalid(ReasonCode.MISSING_REQUIRED_FIELD, "id is required");
        }

        // Same timestamp check and conversion as EventMapper -- reused rather
        // than re-derived, since both mappers turn the same Zeek "ts" shape
        // into the same Instant.
        if (Double.isNaN(dto.ts()) || dto.ts() < 0) {
            return MappingResult.invalid(ReasonCode.INVALID_TIMESTAMP, "ts must be a non-negative number, was " + dto.ts());
        }

        // id_orig_h becomes DnsEvent.sourceIp below; DnsEvent's compact
        // constructor throws IllegalArgumentException on a blank sourceIp, so
        // this is validated ahead of that call instead of behind a catch.
        //
        // Deliberately NOT validated: id_resp_h, id_orig_p, id_resp_p. DnsEvent
        // carries none of them -- unlike ConnEvent, which holds all four
        // endpoint fields in its ConnectionTuple, dns.log's record is just
        // query + response + the originator's own address. Rejecting a real
        // dns.log observation over a field this path never reads would drop
        // data for no benefit, so the asymmetry with EventMapper's port/
        // id_resp_h checks is intentional, not an oversight.
        if (dto.idOrigH() == null || dto.idOrigH().isBlank()) {
            return MappingResult.invalid(ReasonCode.MISSING_REQUIRED_FIELD, "id_orig_h is required");
        }

        // query becomes DnsQuery.name below; DnsQuery's compact constructor
        // throws IllegalArgumentException on a blank name, so this is
        // validated ahead of that call for the same crash-loop reason as
        // every check in this method.
        if (dto.query() == null || dto.query().isBlank()) {
            return MappingResult.invalid(ReasonCode.MISSING_REQUIRED_FIELD, "query is required");
        }

        // qtype is a nullable Integer on the wire. ABSENT means "not observed"
        // and must be rejected here, NOT defaulted to DnsQType.OTHER: OTHER
        // means a real query type outside this schema's enumerated set, and
        // dns-feature-v1 marks schema index 13 (dns_qtype) REQUIRED, so
        // defaulting absence to OTHER's numeric code (-1) would silently
        // corrupt a frozen, required feature rather than merely default it. A
        // PRESENT qtype still goes through DnsQType.fromCode below, which DOES
        // map an unrecognised code to OTHER correctly -- that is a genuinely
        // different case from absence.
        if (dto.qtype() == null) {
            return MappingResult.invalid(ReasonCode.MISSING_REQUIRED_FIELD, "qtype is required");
        }
        DnsQType qtype = DnsQType.fromCode(dto.qtype());

        // Every field this event needs is now known valid, so none of the
        // domain constructors below can throw.
        EventId eventId = EventId.derive(sensor, dto.id() + ":" + dto.transId());
        Instant eventTime = Instant.ofEpochMilli(Math.round(dto.ts() * 1000.0));

        // This mapper handles dns.log exclusively, so LogType.DNS is a fixed
        // constant, mirroring how EventMapper fixes LogType.CONN.
        LogType logType = LogType.DNS;

        // dto.id() is Zeek's uid -- the CORRELATION key linking this query back
        // to its connection and to every other query on that same connection.
        // It is deliberately NOT the event id: a resolver reuses one connection
        // for many queries, so several dns.log records legitimately share this
        // value (docs/superpowers/specs/2026-09-04-multi-protocol-feature-schema-design.md
        // §5.4). eventId above folds in trans_id precisely so those records
        // still get distinct identities.
        String connectionUid = dto.id();

        DnsQuery query = new DnsQuery(dto.query(), qtype, dto.transId());

        // Only rcode's presence indicates a response was actually observed.
        // Zeek writes AA, RA and TC as false on every record, reply or not, so
        // their truthiness proves nothing on its own -- checking them instead
        // of rcode would fabricate a response for a query that never got one.
        DnsResponse response = null;
        if (dto.rcode() != null) {
            boolean authoritative = dto.aa() != null && dto.aa();
            boolean recursionAvailable = dto.ra() != null && dto.ra();
            boolean truncated = dto.tc() != null && dto.tc();
            int answerCount = dto.answers() == null ? 0 : dto.answers().size();

            // firstTtlSeconds is 0 in THREE wire states, each guarded separately
            // because each fails differently: TTLs absent (null), TTLs present
            // but empty (a naive get(0) throws IndexOutOfBoundsException), and
            // TTLs whose first element is itself null. Jackson accepts
            // "TTLs": [null] by default, and unboxing that element to double
            // throws NullPointerException -- which would escape map() and
            // crash-loop the Flink subtask on a single record. dns_ttl is
            // DEFAULT_ZERO in the frozen contract, so an unusable value is
            // "not observed", not a rejection.
            Double firstTtl = (dto.ttls() == null || dto.ttls().isEmpty()) ? null : dto.ttls().get(0);
            long firstTtlSeconds = firstTtl == null ? 0L : Math.round(firstTtl);

            response = new DnsResponse(DnsRcode.fromCode(dto.rcode()), authoritative, recursionAvailable,
                truncated, answerCount, firstTtlSeconds);
        }

        // dns.log has no is_orig column (unlike the ICSNPP logs --
        // docs/superpowers/specs/2026-09-10-per-protocol-feature-schemas-design.md
        // §5.1): every record IS a transaction the originator initiated, so
        // isOrig is always true and sourceIp is always id_orig_h. enrichment
        // stays null here -- a later operator in this unit populates it via
        // DnsEvent.withEnrichment once a conn.log snapshot exists for this uid;
        // null is that operator's ordinary "no snapshot yet" state, not an
        // error this mapper needs to report.
        NetworkEvent event = new DnsEvent(
            new EventEnvelope(eventId, eventTime, sensor, logType, connectionUid),
            query, response, dto.idOrigH(), true, null);
        return MappingResult.valid(event);
    }
}
