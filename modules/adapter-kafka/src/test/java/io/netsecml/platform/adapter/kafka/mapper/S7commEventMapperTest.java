package io.netsecml.platform.adapter.kafka.mapper;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.netsecml.platform.adapter.kafka.dto.ZeekS7commRecord;
import io.netsecml.platform.domain.event.ConnEvent;
import io.netsecml.platform.domain.event.DnsEvent;
import io.netsecml.platform.domain.event.LogType;
import io.netsecml.platform.domain.event.MappingResult;
import io.netsecml.platform.domain.event.ModbusEvent;
import io.netsecml.platform.domain.event.NetworkEvent;
import io.netsecml.platform.domain.event.ReasonCode;
import io.netsecml.platform.domain.event.S7commEvent;
import io.netsecml.platform.domain.event.SensorId;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.OptionalInt;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

// Pins S7commEventMapper against upstream's normalize_zeek_message
// (two-models-info/S7___/kafka_source.py): aliases, the two endpoint shapes
// and their precedence, direction by destination port, identity, and every
// rejection -- including the modbus lesson, a request and its response that
// carry the SAME connection-level pair and differ only in is_orig.
class S7commEventMapperTest {
    private static final SensorId SENSOR = new SensorId("sensor-eu-1");
    private static final ObjectMapper JSON = new ObjectMapper();
    private final S7commEventMapper mapper = new S7commEventMapper();

    // A well-formed per-packet request: client 10.0.0.5:50001 -> PLC 10.0.0.9:102.
    private static ObjectNode request() {
        ObjectNode json = JSON.createObjectNode();
        json.put("ts", 1790000000.123456);
        json.put("uid", "CS7A1");
        json.put("source_h", "10.0.0.5");
        json.put("source_p", 50001);
        json.put("destination_h", "10.0.0.9");
        json.put("destination_p", 102);
        json.put("rosctr_code", 1);
        json.put("pdu_reference", 7);
        json.put("function_code", "0x04");
        return json;
    }

    // The same record with only the connection-level pair, oriented by is_orig.
    private static ObjectNode connectionShaped(boolean isOrig) {
        ObjectNode json = request();
        json.remove(List.of("source_h", "source_p", "destination_h", "destination_p"));
        json.put("id_orig_h", "10.0.0.5");
        json.put("id_orig_p", 50001);
        json.put("id_resp_h", "10.0.0.9");
        json.put("id_resp_p", 102);
        json.put("is_orig", isOrig);
        return json;
    }

    private MappingResult<NetworkEvent> map(ObjectNode json) {
        return mapper.map(new ZeekS7commRecord(json), SENSOR);
    }

    // Narrows a successful mapping to S7commEvent, one explicit arm per permit.
    private S7commEvent mapped(ObjectNode json) {
        MappingResult<NetworkEvent> result = map(json);
        assertTrue(result.isValid(), () -> "rejected: " + result);
        return switch (result.value()) {
            case S7commEvent s7 -> s7;
            case ConnEvent c -> throw new AssertionError("S7commEventMapper maps s7comm.log exclusively; got a ConnEvent");
            case DnsEvent d -> throw new AssertionError("S7commEventMapper maps s7comm.log exclusively; got a DnsEvent");
            case ModbusEvent m -> throw new AssertionError("S7commEventMapper maps s7comm.log exclusively; got a ModbusEvent");
        };
    }

    private void assertRejected(ReasonCode expected, Consumer<ObjectNode> change) {
        ObjectNode json = request();
        change.accept(json);
        MappingResult<NetworkEvent> result = map(json);
        assertFalse(result.isValid(), () -> "expected " + expected + " for " + json);
        assertEquals(expected, result.reason(), () -> "for " + json + ": " + result);
    }

    @Test
    void aPerPacketRequestMapsEveryField() {
        S7commEvent event = mapped(request());
        assertEquals("10.0.0.5", event.sourceIp());
        assertEquals(50001, event.sourcePort());
        assertEquals("10.0.0.9", event.destinationIp());
        assertEquals(102, event.destinationPort());
        assertTrue(event.isRequest(), "sent to port 102");
        assertEquals(7, event.pduReference());
        assertEquals(1, event.rosctrCode());
        assertEquals(4, event.functionCode(), "\"0x04\" reads as upstream's int(text, 0)");
        assertNull(event.functionName());
        assertEquals(1790000000.123456, event.tsSeconds(), "the wire ts, unrounded");
        assertEquals(Instant.ofEpochMilli(1790000000123L), event.eventTime());
        assertEquals("sensor-eu-1:CS7A1:7:REQUEST:1790000000123", event.eventId().value());
        assertEquals(LogType.S7COMM, event.logType());
        assertEquals("CS7A1", event.connectionUid());
    }

    @Test
    void aRequestAndItsResponseWithIdenticalConnectionEndpointsAreOrientedByIsOrig() {
        // Zeek's id_orig_*/id_resp_* describe the CONNECTION, so a request and
        // its response carry the same pair; only is_orig tells them apart. If
        // the mapper did not orient the pair, both records would look like
        // requests to port 102 and no response could ever match.
        S7commEvent request = mapped(connectionShaped(true));
        S7commEvent response = mapped(connectionShaped(false));

        assertEquals("10.0.0.5", request.sourceIp());
        assertEquals(102, request.destinationPort());
        assertTrue(request.isRequest());

        assertEquals("10.0.0.9", response.sourceIp());
        assertEquals(102, response.sourcePort());
        assertEquals("10.0.0.5", response.destinationIp());
        assertEquals(50001, response.destinationPort());
        assertFalse(response.isRequest(), "the PLC's reply goes to the client's port, not to 102");
        assertEquals("sensor-eu-1:CS7A1:7:RESPONSE:1790000000123", response.eventId().value());
    }

    @Test
    void theUnderscoredDottedAndNestedIdSpellingsMeanTheSame() {
        ObjectNode dotted = connectionShaped(false);
        for (String side : List.of("orig_h", "orig_p", "resp_h", "resp_p")) {
            dotted.set("id." + side, dotted.remove("id_" + side));
        }
        ObjectNode nested = connectionShaped(false);
        ObjectNode id = nested.putObject("id");
        for (String side : List.of("orig_h", "orig_p", "resp_h", "resp_p")) {
            id.set(side, nested.remove("id_" + side));
        }
        S7commEvent underscored = mapped(connectionShaped(false));
        for (S7commEvent other : List.of(mapped(dotted), mapped(nested))) {
            assertEquals(underscored.sourceIp(), other.sourceIp());
            assertEquals(underscored.sourcePort(), other.sourcePort());
            assertEquals(underscored.destinationIp(), other.destinationIp());
            assertEquals(underscored.destinationPort(), other.destinationPort());
        }
    }

    @Test
    void perPacketFieldsWinOverTheConnectionPair() {
        // upstream uses the per-packet four whenever all are present. Here they
        // say "PLC -> client" while the connection pair with is_orig=true would
        // say "client -> PLC": the per-packet reading must win.
        ObjectNode json = connectionShaped(true);
        json.put("source_h", "10.0.0.9");
        json.put("source_p", 102);
        json.put("destination_h", "10.0.0.5");
        json.put("destination_p", 50001);
        S7commEvent event = mapped(json);
        assertEquals("10.0.0.9", event.sourceIp());
        assertFalse(event.isRequest());
    }

    @Test
    void upstreamsAliasesResolve() {
        ObjectNode json = JSON.createObjectNode();
        json.put("ts", 1790000000.5);
        json.put("uid", "CS7A2");
        json.put("src", "10.0.0.5");
        json.put("src_p", "50001");
        json.put("dst_h", "10.0.0.9");
        json.put("dst_p", 102);
        json.put("rosctr", "3");
        json.put("pdu_ref_num", 9);
        json.put("function", "26");
        S7commEvent event = mapped(json);
        assertEquals(50001, event.sourcePort(), "a decimal-string port reads as int(text)");
        assertEquals(3, event.rosctrCode());
        assertEquals(9, event.pduReference());
        assertEquals(26, event.functionCode());
    }

    @Test
    void aStringTimestampIsAccepted() {
        // upstream: float(message["ts"]) accepts a decimal string.
        ObjectNode json = request();
        json.put("ts", "1790000000.25");
        assertEquals(1790000000.25, mapped(json).tsSeconds());
    }

    @Test
    void aFunctionNameIsKeptOnlyWhenNonEmpty() {
        ObjectNode named = request();
        named.remove("function_code");
        named.put("function_name", "read_var");
        S7commEvent event = mapped(named);
        assertNull(event.functionCode());
        assertEquals("read_var", event.functionName(), "kept as written; S7commCategories upper-cases it");

        ObjectNode empty = request();
        empty.remove("function_code");
        empty.put("function_name", "");
        assertNull(mapped(empty).functionName(), "\"\" is falsy in upstream's `if e.function_name:`");
    }

    @Test
    void aBlankHostIsRejectedNotThrown() {
        assertRejected(ReasonCode.MISSING_REQUIRED_FIELD, json -> json.put("source_h", " "));
    }

    @Test
    void everyRejectionCarriesItsReason() {
        assertRejected(ReasonCode.MISSING_REQUIRED_FIELD, json -> json.remove("uid"));
        assertRejected(ReasonCode.MISSING_REQUIRED_FIELD, json -> json.put("uid", " "));
        assertRejected(ReasonCode.MISSING_REQUIRED_FIELD, json -> json.remove("ts"));
        assertRejected(ReasonCode.INVALID_TIMESTAMP, json -> json.put("ts", -1.0));
        assertRejected(ReasonCode.INVALID_TIMESTAMP, json -> json.put("ts", "soon"));
        assertRejected(ReasonCode.INVALID_TIMESTAMP, json -> json.put("ts", 1.0e13));
        assertRejected(ReasonCode.MISSING_REQUIRED_FIELD, json -> json.put("is_orig", "maybe"));
        assertRejected(ReasonCode.MISSING_REQUIRED_FIELD, json -> json.remove("destination_h"));
        assertRejected(ReasonCode.INVALID_PORT, json -> json.put("source_p", "abc"));
        assertRejected(ReasonCode.INVALID_PORT, json -> json.put("destination_p", 70000));
        assertRejected(ReasonCode.MISSING_REQUIRED_FIELD, json -> json.remove("pdu_reference"));
        assertRejected(ReasonCode.INVALID_COUNTER, json -> json.put("pdu_reference", 70000));
        assertRejected(ReasonCode.INVALID_COUNTER, json -> json.put("pdu_reference", -1));
        assertRejected(ReasonCode.INVALID_COUNTER, json -> json.put("function_code", "zz"));
        assertRejected(ReasonCode.INVALID_COUNTER, json -> json.put("function_code", 1 << 24));
        assertRejected(ReasonCode.INVALID_COUNTER, json -> json.put("rosctr_code", -1));
    }

    @Test
    void theConnectionPairWithoutIsOrigIsRejected() {
        ObjectNode json = connectionShaped(true);
        json.remove("is_orig");
        MappingResult<NetworkEvent> result = map(json);
        assertEquals(ReasonCode.MISSING_REQUIRED_FIELD, result.reason());
    }

    @Test
    void pduReferenceOfIsLenientForDlqIds() {
        assertEquals(OptionalInt.of(7), S7commEventMapper.pduReferenceOf(new ZeekS7commRecord(request())));
        ObjectNode missing = request();
        missing.remove("pdu_reference");
        assertEquals(OptionalInt.empty(), S7commEventMapper.pduReferenceOf(new ZeekS7commRecord(missing)));
        ObjectNode outOfRange = request();
        outOfRange.put("pdu_reference", 70000);
        assertEquals(OptionalInt.empty(), S7commEventMapper.pduReferenceOf(new ZeekS7commRecord(outOfRange)));
    }
}
