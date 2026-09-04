package io.netsecml.platform.adapter.clickhouse.mapper;

import io.netsecml.platform.adapter.clickhouse.row.InvalidEventRow;
import io.netsecml.platform.domain.event.RejectedEvent;
import java.io.Serializable;

// Turns a domain RejectedEvent into an invalid_events row.
public final class InvalidEventRowMapper implements Serializable {

    // The source contract the rejected bytes claimed to satisfy. Constant while
    // the platform ingests only Zeek conn logs; it becomes a parameter the day a
    // second source type appears.
    public static final String SOURCE_VERSION = "zeek-conn-source-v1";

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
            SOURCE_VERSION,
            event.rawPayloadHash());
    }
}
