package io.netsecml.platform.domain.feature;

import io.netsecml.platform.domain.event.EventEnvelope;
import io.netsecml.platform.domain.event.EventId;
import io.netsecml.platform.domain.event.LogType;
import io.netsecml.platform.domain.event.ModbusEvent;
import io.netsecml.platform.domain.event.ModbusEvent.ModbusDirection;
import io.netsecml.platform.domain.event.SensorId;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

// Pins ModbusEntityKey's two structural properties: the orientation
// normalization that lets a request and its response share one key (the
// whole reason this key exists), and the JVM-stable hash formula Flink's
// key-group assignment depends on -- the same defect class SourceKey shipped
// once already (see SourceKey's own hashCode() javadoc).
class ModbusEntityKeyTest {
    private static final SensorId SENSOR = new SensorId("sensor-eu-1");
    private static final Instant EVENT_TIME = Instant.parse("2026-09-21T10:00:00Z");

    // Builds a minimal, valid modbus event for the given direction, endpoint
    // pair and unit. A null unitId stands in for "absent on the wire": the
    // real mapper already substitutes the literal "NA" before a ModbusEvent
    // can exist at all (ModbusEvent's compact constructor rejects a null
    // unitId), so this helper performs that same substitution rather than
    // bypassing the invariant it is testing against.
    private ModbusEvent event(ModbusDirection direction, String sourceIp, String destinationIp, String unitId) {
        EventEnvelope envelope = new EventEnvelope(EventId.derive(SENSOR, "Mabc"), EVENT_TIME, SENSOR,
            LogType.MODBUS, "Mabc");
        return new ModbusEvent(envelope, direction, sourceIp, destinationIp, 3, "1",
            unitId == null ? "NA" : unitId, null, null, false, new double[0], new double[0]);
    }

    @Test
    void theHashIsTheSpecifiedFormulaOverStringsOnly() {
        // Pinning the formula, not a magic number: Flink derives key groups from
        // hashCode(), so any component whose hash is JVM-dependent breaks state
        // restore. A single-JVM test cannot observe that directly.
        ModbusEntityKey key = new ModbusEntityKey(new SensorId("sensor-eu-1"), "10.0.0.5", "10.0.0.9", "1");
        assertEquals(Objects.hash("sensor-eu-1", "10.0.0.5", "10.0.0.9", "1"), key.hashCode());
    }

    @Test
    void aRequestAndItsResponseShareOneKey() {
        // Orientation normalization is the whole reason this key exists. This
        // test constructs PER-PACKET events directly -- the form ModbusEvent's
        // sourceIp/destinationIp are defined to carry, in which a response's
        // source/destination are its request's destination/source -- and pins
        // that of(...) folds the two back into one key. It says nothing about
        // what the wire sends: the platform's sensor emits CONNECTION-level
        // id_orig_h/id_resp_h, identical on both records, and the mapper's
        // orientation of that into this per-packet form is proven end to end
        // (real parser, real mapper, this real key) in
        // ModbusEventMapperTest.aRequestAndItsResponseFromTheRealWireShareOneEntityKey.
        ModbusEvent request = event(ModbusDirection.REQUEST, "10.0.0.5", "10.0.0.9", "1");
        ModbusEvent response = event(ModbusDirection.RESPONSE, "10.0.0.9", "10.0.0.5", "1");
        assertEquals(ModbusEntityKey.of(request), ModbusEntityKey.of(response));
    }

    @Test
    void adifferentUnitIsADifferentKey() {
        ModbusEvent unitOne = event(ModbusDirection.REQUEST, "10.0.0.5", "10.0.0.9", "1");
        ModbusEvent unitTwo = event(ModbusDirection.REQUEST, "10.0.0.5", "10.0.0.9", "2");
        assertNotEquals(ModbusEntityKey.of(unitOne), ModbusEntityKey.of(unitTwo));
    }

    @Test
    void anAbsentUnitBecomesTheLiteralNaRatherThanDroppingTheRecord() {
        ModbusEvent noUnit = event(ModbusDirection.REQUEST, "10.0.0.5", "10.0.0.9", null);
        assertEquals("NA", ModbusEntityKey.of(noUnit).unitId());
    }
}
