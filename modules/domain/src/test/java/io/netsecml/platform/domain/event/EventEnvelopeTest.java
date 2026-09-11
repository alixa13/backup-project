package io.netsecml.platform.domain.event;

import org.junit.jupiter.api.Test;
import java.time.Instant;
import static org.junit.jupiter.api.Assertions.*;

// The identity block every log type carries, whatever its payload shape.
// These tests pin which fields are structural and which are optional, because
// every protocol record added later inherits exactly this contract.
class EventEnvelopeTest {

    private static final Instant WHEN = Instant.parse("2026-09-11T10:00:00Z");
    private static final SensorId SENSOR = new SensorId("sensor-eu-1");

    @Test
    void carriesIdentityTimingSensorAndLogType() {
        EventEnvelope envelope = new EventEnvelope(
            EventId.derive(SENSOR, "Cabc123XYZ"), WHEN, SENSOR, LogType.CONN, "Cabc123XYZ");

        assertEquals("sensor-eu-1:Cabc123XYZ", envelope.eventId().value());
        assertEquals(WHEN, envelope.eventTime());
        assertEquals(SENSOR, envelope.sensor());
        assertEquals(LogType.CONN, envelope.logType());
        assertEquals("Cabc123XYZ", envelope.connectionUid());
    }

    // eventId is the ClickHouse ORDER BY key tail, eventTime is the partition key,
    // and sensor is what makes an event attributable to a deployment. All three
    // are structural: a row cannot be archived without them. Each is varied
    // independently so dropping any single check fails this test.
    @Test
    void rejectsAMissingEventIdEventTimeOrSensor() {
        assertThrows(NullPointerException.class, () -> new EventEnvelope(
            null, WHEN, SENSOR, LogType.CONN, "Cabc"));
        assertThrows(NullPointerException.class, () -> new EventEnvelope(
            EventId.derive(SENSOR, "Cabc"), null, SENSOR, LogType.CONN, "Cabc"));
        assertThrows(NullPointerException.class, () -> new EventEnvelope(
            EventId.derive(SENSOR, "Cabc"), WHEN, null, LogType.CONN, "Cabc"));
    }

    // logType is structural: every event must say which Zeek log produced it, or
    // the archive job cannot route it and the feature schema cannot be chosen.
    @Test
    void rejectsAMissingLogType() {
        assertThrows(NullPointerException.class, () -> new EventEnvelope(
            EventId.derive(SENSOR, "Cabc"), WHEN, SENSOR, null, "Cabc"));
    }

    // connectionUid is a CORRELATION key, not an identity, and some log types
    // legitimately have none. It normalises to "" rather than rejecting null, so
    // a protocol without a Zeek uid can still produce an envelope.
    @Test
    void normalisesAnAbsentConnectionUidToEmpty() {
        EventEnvelope envelope = new EventEnvelope(
            EventId.derive(SENSOR, "x"), WHEN, SENSOR, LogType.CONN, null);

        assertEquals("", envelope.connectionUid());
    }
}
