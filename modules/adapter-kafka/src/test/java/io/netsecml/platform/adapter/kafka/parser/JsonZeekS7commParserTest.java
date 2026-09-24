package io.netsecml.platform.adapter.kafka.parser;

import io.netsecml.platform.adapter.kafka.dto.ZeekS7commRecord;
import io.netsecml.platform.domain.event.MappingResult;
import io.netsecml.platform.domain.event.ReasonCode;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

// The parse stage accepts any JSON object and rejects everything else as
// MALFORMED_JSON; field-level rules belong to S7commEventMapper, as upstream's
// normalize_zeek_message applies them to an already-decoded dict.
class JsonZeekS7commParserTest {
    private final JsonZeekS7commParser parser = new JsonZeekS7commParser();

    private MappingResult<ZeekS7commRecord> parse(String json) {
        return parser.parse(json.getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void aJsonObjectParsesAndKeepsEveryField() {
        MappingResult<ZeekS7commRecord> result = parse(
            "{\"ts\":1.5,\"uid\":\"C1\",\"id\":{\"orig_h\":\"10.0.0.5\"},\"future_field\":[1,2]}");
        assertTrue(result.isValid());
        assertEquals("C1", result.value().uid());
        assertEquals("10.0.0.5", result.value().json().get("id").get("orig_h").asText());
        assertTrue(result.value().json().has("future_field"), "unknown fields are kept, and ignored later");
    }

    @Test
    void aMissingUidReadsAsNullForTheMapperToReject() {
        assertNull(parse("{\"ts\":1.5}").value().uid());
    }

    @Test
    void truncatedJsonIsMalformed() {
        MappingResult<ZeekS7commRecord> result = parse("{\"ts\":1.5,\"uid\":\"C1\"");
        assertFalse(result.isValid());
        assertEquals(ReasonCode.MALFORMED_JSON, result.reason());
    }

    @Test
    void aJsonValueThatIsNotAnObjectIsMalformed() {
        assertEquals(ReasonCode.MALFORMED_JSON, parse("[1,2,3]").reason());
        assertEquals(ReasonCode.MALFORMED_JSON, parse("\"text\"").reason());
        assertEquals(ReasonCode.MALFORMED_JSON, parse("").reason());
    }

    @Test
    void trailingContentAfterTheObjectIsMalformed() {
        assertEquals(ReasonCode.MALFORMED_JSON, parse("{\"ts\":1.5} {\"ts\":2.5}").reason());
    }
}
