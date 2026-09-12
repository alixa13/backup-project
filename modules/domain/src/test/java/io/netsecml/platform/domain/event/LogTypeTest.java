package io.netsecml.platform.domain.event;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class LogTypeTest {
    // Only implemented log types get a constant. Adding ssh/http/modbus/s7comm
    // here before their mapper and schema exist would advertise support that is not
    // there; this test records that intent so the omission is deliberate. DNS is
    // now implemented (mapper/event, and dns-feature-v1 schema), so it joins CONN
    // here -- this is a plain array-equality assertion, not a switch, so it did
    // not fail at compile time when LogType.DNS was added; it went red only when
    // this test actually ran.
    @Test
    void onlyImplementedLogTypesArePresent() {
        assertArrayEquals(new LogType[]{LogType.CONN, LogType.DNS}, LogType.values());
    }

    // wireName is what reaches Kafka and the ClickHouse log_type column. It is
    // lowercase so the wire form matches Zeek's own log naming (conn.log, dns.log).
    @Test
    void wireNameIsTheLowercaseZeekLogName() {
        assertEquals("conn", LogType.CONN.wireName());
        assertEquals("dns", LogType.DNS.wireName());
    }
}
