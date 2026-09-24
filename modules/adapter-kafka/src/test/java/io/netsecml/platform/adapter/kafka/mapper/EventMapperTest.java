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
        // DnsEvent joining NetworkEvent's permits broke this switch at compile
        // time -- it is resolved with an explicit arm, never a default, that
        // fails loudly rather than silently narrowing to null: EventMapper
        // producing a DnsEvent would be a wiring bug this test should catch, not
        // paper over.
        ConnEvent conn = switch (event) {
            case ConnEvent c -> c;
            case DnsEvent d -> throw new AssertionError("EventMapper maps conn.log exclusively; got a DnsEvent");
            case ModbusEvent m -> throw new AssertionError("EventMapper maps conn.log exclusively; got a ModbusEvent");
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
        // Same compile break, same fix, as mapsValidFixtureToNetworkEvent above:
        // this fixture is named for the UDP/DNS *service* the conn record
        // observed, not a dns.log record -- EventMapper still only ever produces
        // a ConnEvent here.
        ConnEvent conn = switch (result.value()) {
            case ConnEvent c -> c;
            case DnsEvent d -> throw new AssertionError("EventMapper maps conn.log exclusively; got a DnsEvent");
            case ModbusEvent m -> throw new AssertionError("EventMapper maps conn.log exclusively; got a ModbusEvent");
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

    // Pins the ceiling itself, not just behavior around it: if a later edit
    // widened MAX_VALID_TS_SECONDS (e.g. moving the year forward), this fails
    // even though every accept/reject test below would still pass against the
    // new, wider bound.
    @Test
    void maxValidTsSecondsIsTheEpochSecondOfTheFirstInstantOutsideClickHousesDateTime64Range() {
        assertEquals(10_413_792_000.0, EventMapper.MAX_VALID_TS_SECONDS,
            "must equal Instant.parse(\"2300-01-01T00:00:00Z\").getEpochSecond()");
    }

    // A JSON literal like 1e400 overflows double and parses to
    // Double.POSITIVE_INFINITY -- NOT NaN, so the pre-existing NaN/negative
    // guard never caught it. Driven through the real parser, not a hand-built
    // DTO, so this proves a Kafka record actually carrying that literal reaches
    // this rejection rather than merely assuming Jackson would produce infinity.
    @Test
    void aPositiveInfiniteTimestampFromTheRealParserIsRejectedAsInvalidTimestamp() {
        String json = "{\"id\":\"Cabc123XYZ\",\"ts\":1e400,\"id_orig_h\":\"10.0.0.5\","
            + "\"id_orig_p\":53421,\"id_resp_h\":\"93.184.216.34\",\"id_resp_p\":443,"
            + "\"proto\":\"tcp\",\"conn_state\":\"SF\"}";
        ZeekConnEvent parsed = parser.parse(json.getBytes(java.nio.charset.StandardCharsets.UTF_8)).value();
        assertTrue(Double.isInfinite(parsed.ts()), "1e400 must overflow to POSITIVE_INFINITY, not NaN");

        MappingResult<NetworkEvent> result = mapper.map(parsed, sensor);

        assertFalse(result.isValid());
        assertEquals(ReasonCode.INVALID_TIMESTAMP, result.reason());
    }

    // A merely huge FINITE value overflows the same way once multiplied by 1000
    // and rounded to a long in Instant.ofEpochMilli -- this is the ceiling
    // catching a value that is neither NaN nor infinite, at or beyond
    // 2300-01-01T00:00:00Z.
    @Test
    void aTimestampAtTheClickHouseCeilingIsRejectedAsInvalidTimestamp() {
        ZeekConnEvent atCeiling = new ZeekConnEvent("Cabc123XYZ", EventMapper.MAX_VALID_TS_SECONDS,
            "10.0.0.5", 53421, "93.184.216.34", 443, "tcp", null, "SF",
            null, null, null, null, null, null, null, null);

        MappingResult<NetworkEvent> result = mapper.map(atCeiling, sensor);

        assertFalse(result.isValid());
        assertEquals(ReasonCode.INVALID_TIMESTAMP, result.reason());
    }

    // The boundary's other side: one second BEFORE the ceiling must still be
    // accepted, so the fix does not silently narrow the valid range. Together
    // with the at-the-ceiling test above, this pins the ">=" comparison exactly
    // -- a drift to ">" would flip only the previous test, and a drift to "<="
    // would flip only this one.
    @Test
    void aTimestampOneSecondBeforeTheClickHouseCeilingIsAccepted() {
        ZeekConnEvent justBelowCeiling = new ZeekConnEvent("Cabc123XYZ", EventMapper.MAX_VALID_TS_SECONDS - 1,
            "10.0.0.5", 53421, "93.184.216.34", 443, "tcp", null, "SF",
            null, null, null, null, null, null, null, null);

        MappingResult<NetworkEvent> result = mapper.map(justBelowCeiling, sensor);

        assertTrue(result.isValid());
    }
}
