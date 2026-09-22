package io.netsecml.platform.domain.event;

import org.junit.jupiter.api.Test;
import java.time.Instant;
import java.util.Set;
import static org.junit.jupiter.api.Assertions.*;

// Pins the two properties ModbusEvent's own javadoc argues for but nothing
// enforces at compile time: the array components are defensively copied both
// ways, and an absent address is represented as null rather than a 0
// sentinel. The sealed hierarchy's own admission list is proven once, in
// NetworkEventSurfaceTest.permitsListsOnlyImplementedLogTypes -- not
// duplicated here.
class ModbusEventTest {
    private static final SensorId SENSOR = new SensorId("sensor-eu-1");
    private static final Instant EVENT_TIME = Instant.parse("2026-09-21T10:00:00Z");

    // Builds a minimal, valid modbus event with the given request/response
    // value arrays; every other field is fixed because no test in this class
    // cares about their shape.
    private ModbusEvent event(double[] requestValues, double[] responseValues) {
        EventEnvelope envelope = new EventEnvelope(EventId.derive(SENSOR, "Mabc"), EVENT_TIME, SENSOR,
            LogType.MODBUS, "Mabc");
        return new ModbusEvent(envelope, ModbusEvent.ModbusDirection.REQUEST, "10.0.0.5", "10.0.0.6",
            3, "1", "1", null, null, false, requestValues, responseValues);
    }

    // Builds a minimal, valid modbus event with the given address; every other
    // field is fixed for the same reason event() above fixes its own.
    private ModbusEvent eventWithAddress(Double address) {
        EventEnvelope envelope = new EventEnvelope(EventId.derive(SENSOR, "Mabc"), EVENT_TIME, SENSOR,
            LogType.MODBUS, "Mabc");
        return new ModbusEvent(envelope, ModbusEvent.ModbusDirection.REQUEST, "10.0.0.5", "10.0.0.6",
            3, "1", "1", address, null, false, new double[0], new double[0]);
    }

    @Test
    void theValueArraysAreDefensivelyCopiedBothWays() {
        double[] request = {1.0, 2.0};
        ModbusEvent event = event(request, new double[] {3.0});

        request[0] = 99.0;
        assertEquals(1.0, event.requestValues()[0], "the constructor must copy");

        event.requestValues()[1] = 99.0;
        assertEquals(2.0, event.requestValues()[1], "the accessor must copy too");
    }

    @Test
    void anAbsentAddressIsNullRatherThanZero() {
        // address_present is feature index 9 and address_value index 8; a sentinel
        // zero would make a genuine address of 0 indistinguishable from an absent one.
        ModbusEvent event = eventWithAddress(null);
        assertNull(event.address());
    }
}
