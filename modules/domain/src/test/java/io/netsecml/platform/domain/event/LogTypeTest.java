package io.netsecml.platform.domain.event;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class LogTypeTest {
    // Only implemented log types get a constant. Adding ssh/dns/http/modbus/s7comm
    // here before their mapper and schema exist would advertise support that is not
    // there; this test records that intent so the omission reads as deliberate.
    @Test
    void onlyImplementedLogTypesArePresent() {
        assertArrayEquals(new LogType[]{LogType.CONN}, LogType.values());
    }

    // wireName is what reaches Kafka and the ClickHouse log_type column. It is
    // lowercase so the wire form matches Zeek's own log naming (conn.log).
    @Test
    void wireNameIsTheLowercaseZeekLogName() {
        assertEquals("conn", LogType.CONN.wireName());
    }
}
