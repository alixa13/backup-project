package io.netsecml.platform.domain.event;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;

class ProtocolTest {
    @Test
    void mapsKnownValuesCaseInsensitively() {
        assertEquals(Protocol.TCP, Protocol.fromZeekValue("tcp"));
        assertEquals(Protocol.TCP, Protocol.fromZeekValue("TCP"));
        assertEquals(Protocol.UDP, Protocol.fromZeekValue("udp"));
        assertEquals(Protocol.ICMP, Protocol.fromZeekValue("icmp"));
    }

    @Test
    void mapsUnknownOrNullToOther() {
        assertEquals(Protocol.OTHER, Protocol.fromZeekValue("sctp"));
        assertEquals(Protocol.OTHER, Protocol.fromZeekValue(null));
        assertEquals(Protocol.OTHER, Protocol.fromZeekValue(""));
    }
}
