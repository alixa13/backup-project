package io.netsecml.platform.adapter.clickhouse.mapper;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

// ClickHouse parses DateTime64(3) from 'yyyy-MM-dd HH:mm:ss.SSS'. ISO-8601 with
// 'T' and 'Z' — what Instant.toString() produces — is not reliably accepted, so
// every Instant crossing into a row goes through here.
final class ClickHouseTimestamps {
    // Fixed pattern ClickHouse's DateTime64(3) column accepts, forced to UTC so
    // the formatted string never drifts with the JVM's default zone.
    private static final DateTimeFormatter DATETIME64 =
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS").withZone(ZoneOffset.UTC);

    // Utility class: every member is static, so no instance is ever needed.
    private ClickHouseTimestamps() {
    }

    // Renders one Instant as the millisecond-precision UTC string ClickHouse
    // expects for a DateTime64(3) column.
    static String format(Instant instant) {
        return DATETIME64.format(instant);
    }
}
