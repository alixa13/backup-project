package io.netsecml.platform.domain.event;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;

class ConnectionStateTest {
    @Test
    void mapsKnownStates() {
        assertEquals(ConnectionState.SF, ConnectionState.fromZeekValue("SF"));
        assertEquals(ConnectionState.S0, ConnectionState.fromZeekValue("S0"));
        assertEquals(ConnectionState.REJ, ConnectionState.fromZeekValue("REJ"));
        assertEquals(ConnectionState.OTHER, ConnectionState.fromZeekValue("XYZ"));
    }

    @Test
    void identifiesFailedStates() {
        assertTrue(ConnectionState.S0.isFailed());
        assertTrue(ConnectionState.REJ.isFailed());
        assertTrue(ConnectionState.RSTO.isFailed());
        assertTrue(ConnectionState.RSTR.isFailed());
        assertFalse(ConnectionState.SF.isFailed());
        assertFalse(ConnectionState.OTHER.isFailed());
    }
}
