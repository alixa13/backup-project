package io.netsecml.platform.adapter.kafka.mapper;

import io.netsecml.platform.adapter.kafka.dto.ZeekConnEvent;
import io.netsecml.platform.adapter.kafka.parser.JsonZeekConnParser;
import io.netsecml.platform.domain.event.*;
import org.junit.jupiter.api.Test;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import static org.junit.jupiter.api.Assertions.*;

class EventMapperTest {
    private final JsonZeekConnParser parser = new JsonZeekConnParser();
    private final EventMapper mapper = new EventMapper();
    private final SensorId sensor = new SensorId("sensor-eu-1");

    private ZeekConnEvent fixture(String name) throws IOException {
        byte[] bytes = Files.readAllBytes(Paths.get("..", "..", "tests", "fixtures", "zeek_conn", name));
        return parser.parse(bytes).value();
    }

    @Test
    void mapsValidFixtureToNetworkEvent() throws IOException {
        MappingResult<NetworkEvent> result = mapper.map(fixture("valid-tcp-ssl.json"), sensor);
        assertTrue(result.isValid());
        NetworkEvent event = result.value();
        assertEquals("sensor-eu-1:Cabc123XYZ", event.eventId().value());
        assertEquals(Instant.ofEpochMilli(1786608000123L), event.eventTime());

        // EventMapper handles conn.log exclusively, so its output is always a
        // ConnEvent; this switch reaches the conn-specific fields to assert on.
        ConnEvent conn = switch (event) {
            case ConnEvent c -> c;
        };
        assertEquals(Protocol.TCP, conn.connection().protocol());
        assertEquals(ServiceCode.SSL, conn.connection().service());
        assertEquals(ConnectionState.SF, conn.connection().connectionState());
        assertEquals(443, conn.connection().destinationPort());
        assertEquals(1500L, conn.measurements().durationMillis());
        assertEquals(2048L, conn.measurements().originBytes());
    }

    // EventMapper handles conn.log exclusively, so LogType.CONN should show up as
    // a constant on every mapped event, and connectionUid must be the fixture's
    // raw "id" (Zeek's uid), not something re-derived or altered.
    @Test
    void mapsLogTypeAndConnectionUidFromTheFixture() throws IOException {
        MappingResult<NetworkEvent> result = mapper.map(fixture("valid-tcp-ssl.json"), sensor);
        assertTrue(result.isValid());
        NetworkEvent event = result.value();
        assertEquals(LogType.CONN, event.logType());
        assertEquals("Cabc123XYZ", event.connectionUid());
    }

    @Test
    void defaultsMissingOptionalNumericFieldsToZero() throws IOException {
        MappingResult<NetworkEvent> result = mapper.map(fixture("valid-udp-dns.json"), sensor);
        assertTrue(result.isValid());
        ConnEvent conn = switch (result.value()) {
            case ConnEvent c -> c;
        };
        assertEquals(0L, conn.measurements().durationMillis(), "duration was absent in this fixture");
        assertEquals(0L, conn.measurements().missedBytes(), "missed_bytes was absent in this fixture");
    }

    @Test
    void rejectsInvalidPortWithReasonCode() throws IOException {
        MappingResult<NetworkEvent> result = mapper.map(fixture("invalid-port.json"), sensor);
        assertFalse(result.isValid());
        assertEquals(ReasonCode.INVALID_PORT, result.reason());
    }
}
