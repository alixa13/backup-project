package io.netsecml.platform.domain.event;

import org.junit.jupiter.api.Test;
import java.time.Instant;
import static org.junit.jupiter.api.Assertions.*;

class NetworkEventTest {
    // Shared conn-only components, built once so every test in this class
    // compares against the same values instead of re-deriving them.
    private static final ConnectionTuple TUPLE = new ConnectionTuple("10.0.0.5", 51820, "93.184.216.34", 443,
        Protocol.TCP, ServiceCode.SSL, ConnectionState.SF);
    private static final ConnectionMeasurements MEASUREMENTS = new ConnectionMeasurements(1500, 2048, 4096, 10, 12, 0);
    private static final ConnectionLocality LOCALITY = new ConnectionLocality(null, null);

    // Shared event envelope used to test that ConnEvent requires it non-null, and by tests
    // that build ConnEvent instances needing a valid envelope.
    private static final SensorId SENSOR = new SensorId("sensor-eu-1");
    private static final EventEnvelope ENVELOPE = new EventEnvelope(
        EventId.derive(SENSOR, "Cabc123XYZ"),
        Instant.parse("2026-08-13T10:00:00Z"),
        SENSOR,
        LogType.CONN,
        "Cabc123XYZ");

    @Test
    void buildsValidEvent() {
        SensorId sensor = new SensorId("sensor-eu-1");
        EventId id = EventId.derive(sensor, "Cabc123XYZ");
        EventEnvelope envelope = new EventEnvelope(id, Instant.parse("2026-08-13T10:00:00Z"), sensor,
            LogType.CONN, "Cabc123XYZ");
        ConnEvent event = new ConnEvent(envelope, TUPLE, MEASUREMENTS, LOCALITY);
        assertEquals(id, event.eventId());
        assertEquals(sensor, event.sensor());
        assertEquals(443, event.connection().destinationPort());
        assertEquals(2048, event.measurements().originBytes());
    }

    @Test
    void rejectsNullRequiredFields() {
        // A null eventTime is rejected by the envelope, not by ConnEvent -- the
        // envelope owns identity and timing, so it is the one that must throw.
        assertThrows(NullPointerException.class, () -> new EventEnvelope(
            EventId.derive(SENSOR, "Cabc123XYZ"), null, SENSOR, LogType.CONN, "Cabc123XYZ"));

        // Each of ConnEvent's four null checks, varied one at a time so that deleting
        // any single requireNonNull fails this test. envelope's check is new in this
        // branch and was previously unguarded; the other three carry over from the
        // record NetworkEvent used to be.
        assertThrows(NullPointerException.class,
            () -> new ConnEvent(null, TUPLE, MEASUREMENTS, LOCALITY));
        assertThrows(NullPointerException.class,
            () -> new ConnEvent(ENVELOPE, null, MEASUREMENTS, LOCALITY));
        assertThrows(NullPointerException.class,
            () -> new ConnEvent(ENVELOPE, TUPLE, null, LOCALITY));
        assertThrows(NullPointerException.class,
            () -> new ConnEvent(ENVELOPE, TUPLE, MEASUREMENTS, null));
    }

    // logType and connectionUid are this task's two additions; downstream code
    // (the serializer, later the archive job) reads them straight off the
    // record, so a plain round-trip is what matters here.
    @Test
    void logTypeAndConnectionUidRoundTrip() {
        SensorId sensor = new SensorId("sensor-eu-1");
        EventId id = EventId.derive(sensor, "Cabc123XYZ");
        EventEnvelope envelope = new EventEnvelope(id, Instant.parse("2026-08-13T10:00:00Z"), sensor,
            LogType.CONN, "Cabc123XYZ");
        ConnEvent event = new ConnEvent(envelope, TUPLE, MEASUREMENTS, LOCALITY);

        assertEquals(LogType.CONN, event.logType());
        assertEquals("Cabc123XYZ", event.connectionUid());
    }

    // logType is structural (every event must say which log produced it), so a
    // null is rejected outright. connectionUid is not always available upstream
    // (Modbus/S7comm may carry no uid), so a null is normalized to "" instead of
    // rejected, the same convention RejectedEvent.eventId already uses. Both
    // behaviours now live on EventEnvelope, since it owns both fields.
    @Test
    void nullLogTypeThrowsButNullConnectionUidNormalizesToEmpty() {
        SensorId sensor = new SensorId("sensor-eu-1");
        EventId id = EventId.derive(sensor, "Cabc123XYZ");

        assertThrows(NullPointerException.class, () -> new EventEnvelope(
            id, Instant.now(), sensor, null, "Cabc123XYZ"));

        EventEnvelope envelope = new EventEnvelope(id, Instant.now(), sensor, LogType.CONN, null);
        ConnEvent event = new ConnEvent(envelope, TUPLE, MEASUREMENTS, LOCALITY);
        assertEquals("", event.connectionUid());
    }

    // The default accessors are what let the fourteen files that read only shared
    // fields keep compiling across this refactor. If they stopped delegating, every
    // one of those call sites would have to learn about ConnEvent.
    @Test
    void sharedAccessorsDelegateToTheEnvelope() {
        EventEnvelope envelope = new EventEnvelope(
            EventId.derive(new SensorId("sensor-eu-1"), "Cabc"),
            Instant.parse("2026-09-11T10:00:00Z"),
            new SensorId("sensor-eu-1"), LogType.CONN, "Cabc");

        NetworkEvent event = new ConnEvent(envelope, TUPLE, MEASUREMENTS, LOCALITY);

        assertEquals(envelope.eventId(), event.eventId());
        assertEquals(envelope.eventTime(), event.eventTime());
        assertEquals(envelope.sensor(), event.sensor());
        assertEquals(LogType.CONN, event.logType());
        assertEquals("Cabc", event.connectionUid());
    }

    // permits used to list only ConnEvent, so a switch over NetworkEvent was
    // exhaustive with a single case. DnsEvent joining permits broke this exact
    // switch at compile time -- one of the four sites the sealed hierarchy's
    // exhaustiveness alarm flagged (see NetworkEvent's javadoc) -- and it is
    // fixed here with an explicit case DnsEvent arm, never a default. ModbusEvent
    // joining permits later broke it again -- one of the nine sites that round's
    // javadoc update names -- fixed the same way with an explicit case
    // ModbusEvent arm. The test now exercises all three branches so it proves
    // the switch handles each shape, not merely that it compiles.
    @Test
    void aSwitchOverTheHierarchyIsExhaustiveWithoutADefaultBranch() {
        NetworkEvent connEvent = new ConnEvent(
            new EventEnvelope(EventId.derive(new SensorId("s"), "C"),
                Instant.parse("2026-09-11T10:00:00Z"), new SensorId("s"), LogType.CONN, "C"),
            TUPLE, MEASUREMENTS, LOCALITY);

        NetworkEvent dnsEvent = new DnsEvent(
            new EventEnvelope(EventId.derive(new SensorId("s"), "D"),
                Instant.parse("2026-09-11T10:00:00Z"), new SensorId("s"), LogType.DNS, "D"),
            new DnsQuery("example.com", DnsQType.A, 1, DnsQType.A.code()), null, "10.0.0.5", true, null);

        Instant modbusEventTime = Instant.parse("2026-09-11T10:00:00Z");
        NetworkEvent modbusEvent = new ModbusEvent(
            new EventEnvelope(EventId.derive(new SensorId("s"), "M"),
                modbusEventTime, new SensorId("s"), LogType.MODBUS, "M"),
            modbusEventTime.getEpochSecond() + modbusEventTime.getNano() / 1_000_000_000.0,
            ModbusEvent.ModbusDirection.REQUEST, "10.0.0.5", "10.0.0.6", 3, "1", "1",
            null, null, false, new double[0], new double[0]);

        Instant s7EventTime = Instant.parse("2026-09-24T10:00:00Z");
        NetworkEvent s7Event = new S7commEvent(
            new EventEnvelope(EventId.derive(new SensorId("s"), "S"),
                s7EventTime, new SensorId("s"), LogType.S7COMM, "S"),
            s7EventTime.getEpochSecond(), "10.0.0.5", 50001, "10.0.0.6", 102, 7, 1, 4, null);

        String describedConn = switch (connEvent) {
            case ConnEvent conn -> "conn:" + conn.connection().sourceIp();
            case DnsEvent dns -> "dns:" + dns.sourceIp();
            case ModbusEvent modbus -> "modbus:" + modbus.sourceIp();
            case S7commEvent s7 -> "s7comm:" + s7.sourceIp();
        };
        String describedDns = switch (dnsEvent) {
            case ConnEvent conn -> "conn:" + conn.connection().sourceIp();
            case DnsEvent dns -> "dns:" + dns.sourceIp();
            case ModbusEvent modbus -> "modbus:" + modbus.sourceIp();
            case S7commEvent s7 -> "s7comm:" + s7.sourceIp();
        };
        String describedModbus = switch (modbusEvent) {
            case ConnEvent conn -> "conn:" + conn.connection().sourceIp();
            case DnsEvent dns -> "dns:" + dns.sourceIp();
            case ModbusEvent modbus -> "modbus:" + modbus.sourceIp();
            case S7commEvent s7 -> "s7comm:" + s7.sourceIp();
        };

        assertTrue(describedConn.startsWith("conn:"));
        assertTrue(describedDns.startsWith("dns:"));
        String describedS7 = switch (s7Event) {
            case ConnEvent conn -> "conn:" + conn.connection().sourceIp();
            case DnsEvent dns -> "dns:" + dns.sourceIp();
            case ModbusEvent modbus -> "modbus:" + modbus.sourceIp();
            case S7commEvent s7 -> "s7comm:" + s7.sourceIp();
        };

        assertTrue(describedModbus.startsWith("modbus:"));
        assertTrue(describedS7.startsWith("s7comm:"));
    }
}
