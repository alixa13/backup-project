package io.netsecml.platform.domain.event;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class ConnectionTupleTest {
    @Test
    void acceptsValidTuple() {
        ConnectionTuple tuple = new ConnectionTuple(
            "10.0.0.5", 51820, "93.184.216.34", 443,
            Protocol.TCP, ServiceCode.SSL, ConnectionState.SF);
        assertEquals("10.0.0.5", tuple.sourceIp());
        assertEquals(51820, tuple.sourcePort());
        assertEquals(443, tuple.destinationPort());
        assertEquals(Protocol.TCP, tuple.protocol());
    }

    @Test
    void rejectsPortOutOfRange() {
        assertThrows(IllegalArgumentException.class, () -> new ConnectionTuple(
            "10.0.0.5", -1, "93.184.216.34", 443,
            Protocol.TCP, ServiceCode.SSL, ConnectionState.SF));
        assertThrows(IllegalArgumentException.class, () -> new ConnectionTuple(
            "10.0.0.5", 51820, "93.184.216.34", 70000,
            Protocol.TCP, ServiceCode.SSL, ConnectionState.SF));
    }

    @Test
    void rejectsBlankIp() {
        assertThrows(IllegalArgumentException.class, () -> new ConnectionTuple(
            "", 51820, "93.184.216.34", 443,
            Protocol.TCP, ServiceCode.SSL, ConnectionState.SF));
    }
}
