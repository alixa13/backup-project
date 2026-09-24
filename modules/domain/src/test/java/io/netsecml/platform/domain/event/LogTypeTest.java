package io.netsecml.platform.domain.event;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class LogTypeTest {
    // Only implemented log types get a constant. Adding ssh/http here before
    // their mapper and schema exist would advertise support that is not
    // there; this test records that intent so the omission is deliberate. DNS
    // joined CONN here once its mapper/event and dns-feature-v1 schema existed --
    // this is a plain array-equality assertion, not a switch, so it did not fail
    // at compile time when LogType.DNS was added; it went red only when this
    // test actually ran. MODBUS joined the same way. S7COMM joined with its
    // event, mapper and schema in one change.
    @Test
    void onlyImplementedLogTypesArePresent() {
        assertArrayEquals(new LogType[]{LogType.CONN, LogType.DNS, LogType.MODBUS, LogType.S7COMM}, LogType.values());
    }

    // wireName is what reaches Kafka and the ClickHouse log_type column. It is
    // lowercase so the wire form matches Zeek's own log naming (conn.log,
    // dns.log, modbus_detailed.log).
    @Test
    void wireNameIsTheLowercaseZeekLogName() {
        assertEquals("conn", LogType.CONN.wireName());
        assertEquals("dns", LogType.DNS.wireName());
        assertEquals("modbus", LogType.MODBUS.wireName());
        assertEquals("s7comm", LogType.S7COMM.wireName());
    }
}
