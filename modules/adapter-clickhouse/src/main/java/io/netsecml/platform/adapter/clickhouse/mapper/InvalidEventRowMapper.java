package io.netsecml.platform.adapter.clickhouse.mapper;

import io.netsecml.platform.adapter.clickhouse.row.InvalidEventRow;
import io.netsecml.platform.domain.event.LogType;
import io.netsecml.platform.domain.event.RejectedEvent;
import java.io.Serializable;

// Turns a domain RejectedEvent into an invalid_events row.
public final class InvalidEventRowMapper implements Serializable {

    // Supplied at construction from the topic binding, never read from the event:
    // dlq-v1 is frozen and carries no protocol field. The archive job knows the
    // log type because it knows which topic it read the rejection from, bound at
    // wiring time. See the design's section 9.
    private final LogType logType;

    public InvalidEventRowMapper(LogType logType) {
        if (logType == null) {
            throw new IllegalArgumentException("logType must not be null");
        }
        this.logType = logType;
    }

    // The source contract the rejected bytes claimed to satisfy, derived from the
    // constructed log type so it stays correct as more Zeek log types are added.
    public String sourceVersion() {
        return "zeek-" + logType.wireName() + "-source-v1";
    }

    // Copies every RejectedEvent field across. eventId and detail arrive already
    // normalized to "" by RejectedEvent's own compact constructor, so no null
    // handling is needed here beyond the timestamp and the always-null eventTime.
    public InvalidEventRow toRow(RejectedEvent event) {
        return new InvalidEventRow(
            event.eventId(),
            // No rejection path recovers a source timestamp today. The column is
            // Nullable for exactly this reason.
            null,
            ClickHouseTimestamps.format(event.receivedAt()),
            // stage comes from the domain, never re-derived from the reason name.
            event.reason().stage().name(),
            event.reason().name(),
            event.detail(),
            sourceVersion(),
            event.rawPayloadHash(),
            // The wire form, not the enum name: the column is LowCardinality(String)
            // and would silently accept "CONN", which would then fail to join
            // against feature_vectors.log_type.
            logType.wireName());
    }
}
