package io.netsecml.platform.adapter.kafka.parser;

import io.netsecml.platform.adapter.kafka.dto.ZeekConnEvent;
import io.netsecml.platform.domain.event.MappingResult;
import io.netsecml.platform.domain.event.ReasonCode;
import org.junit.jupiter.api.Test;
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
}
