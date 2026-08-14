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
        assertEquals(Protocol.TCP, event.connection().protocol());
        assertEquals(ServiceCode.SSL, event.connection().service());
        assertEquals(ConnectionState.SF, event.connection().connectionState());
        assertEquals(443, event.connection().destinationPort());
        assertEquals(1500L, event.measurements().durationMillis());
        assertEquals(2048L, event.measurements().originBytes());
    }

    @Test
    void defaultsMissingOptionalNumericFieldsToZero() throws IOException {
        MappingResult<NetworkEvent> result = mapper.map(fixture("valid-udp-dns.json"), sensor);
        assertTrue(result.isValid());
        assertEquals(0L, result.value().measurements().durationMillis(), "duration was absent in this fixture");
        assertEquals(0L, result.value().measurements().missedBytes(), "missed_bytes was absent in this fixture");
    }

    @Test
    void rejectsInvalidPortWithReasonCode() throws IOException {
        MappingResult<NetworkEvent> result = mapper.map(fixture("invalid-port.json"), sensor);
        assertFalse(result.isValid());
        assertEquals(ReasonCode.INVALID_PORT, result.reason());
    }
}
