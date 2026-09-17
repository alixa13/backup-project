package io.netsecml.platform.adapter.kafka.parser;

import io.netsecml.platform.adapter.kafka.dto.ZeekConnEvent;
import io.netsecml.platform.domain.event.MappingResult;
import io.netsecml.platform.domain.event.ReasonCode;
import org.junit.jupiter.api.Test;
import java.nio.charset.StandardCharsets;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import static org.junit.jupiter.api.Assertions.*;

class JsonZeekConnParserTest {
    private final JsonZeekConnParser parser = new JsonZeekConnParser();

    private byte[] fixture(String name) throws IOException {
        return Files.readAllBytes(Paths.get("..", "..", "tests", "fixtures", "zeek_conn", name));
    }

    @Test
    void parsesValidTcpSslFixture() throws IOException {
        MappingResult<ZeekConnEvent> result = parser.parse(fixture("valid-tcp-ssl.json"));
        assertTrue(result.isValid());
        ZeekConnEvent dto = result.value();
        assertEquals("Cabc123XYZ", dto.id());
        assertEquals("10.0.0.5", dto.idOrigH());
        assertEquals(443, dto.idRespP());
        assertEquals("ssl", dto.service());
        assertEquals(2048L, dto.origBytes());
    }

    @Test
    void missingRequiredFieldFailsToParse() throws IOException {
        MappingResult<ZeekConnEvent> result = parser.parse(fixture("missing-required-field.json"));
        assertFalse(result.isValid(), "this fixture omits id_resp_p, a Jackson-required int component of the record, "
            + "so it fails to deserialize; EventMapper's own required-field checks (Task 8) are a separate, "
            + "additional layer for fields Jackson cannot enforce structurally");
        assertEquals(ReasonCode.MALFORMED_JSON, result.reason());
    }

    @Test
    void rejectsMalformedJson() throws IOException {
        MappingResult<ZeekConnEvent> result = parser.parse(fixture("malformed.json"));
        assertFalse(result.isValid());
        assertEquals(ReasonCode.MALFORMED_JSON, result.reason());
    }

    // The frozen source contract (contracts/source/zeek-conn-source-v1.json)
    // promises "unknown additive fields are tolerated and ignored", and a real
    // conn.log record carries columns this DTO does not declare -- history and
    // tunnel_parents are emitted by default. Until this test existed every
    // fixture in the suite was synthesised to match the DTO exactly, so nothing
    // ever fed the parser a record shaped like the ones production would send,
    // and a bare ObjectMapper rejected all of them as MALFORMED_JSON.
    @Test
    void toleratesUnknownAdditiveFieldsARealConnLogCarries() {
        String json = "{\"id\":\"CXWv6p\",\"ts\":1756290191.402,\"id_orig_h\":\"10.0.0.5\","
            + "\"id_orig_p\":51820,\"id_resp_h\":\"93.184.216.34\",\"id_resp_p\":443,"
            + "\"proto\":\"tcp\",\"conn_state\":\"SF\","
            + "\"history\":\"ShADadFf\",\"tunnel_parents\":[],\"orig_ip_bytes\":1420}";

        MappingResult<ZeekConnEvent> result =
            new JsonZeekConnParser().parse(json.getBytes(StandardCharsets.UTF_8));

        assertTrue(result.isValid(), () -> "rejected a real conn.log record: " + result.detail());
        assertEquals("CXWv6p", result.value().id());
        assertEquals("SF", result.value().connState());
    }
}
