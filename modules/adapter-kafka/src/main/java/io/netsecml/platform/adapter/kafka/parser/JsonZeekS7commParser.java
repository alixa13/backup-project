package io.netsecml.platform.adapter.kafka.parser;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netsecml.platform.adapter.kafka.dto.ZeekS7commRecord;
import io.netsecml.platform.domain.event.MappingResult;
import io.netsecml.platform.domain.event.ReasonCode;

// Bytes -> one JSON object. The parse stage only establishes that the payload
// is a single JSON object; which fields it must carry, and how their values
// read, is S7commEventMapper's job, exactly as upstream's normalize_zeek_message
// works on an already-decoded dict.
public final class JsonZeekS7commParser {

    // FAIL_ON_TRAILING_TOKENS: a second JSON value after the object is a
    // malformed record, not something to silently drop.
    private final ObjectMapper objectMapper = new ObjectMapper()
        .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);

    public MappingResult<ZeekS7commRecord> parse(byte[] json) {
        try {
            // readTree returns a missing node for empty input and a non-object
            // node for arrays or scalars; both are malformed records.
            JsonNode tree = objectMapper.readTree(json);
            if (tree == null || !tree.isObject()) {
                return MappingResult.invalid(ReasonCode.MALFORMED_JSON, "an s7comm record must be a JSON object");
            }
            return MappingResult.valid(new ZeekS7commRecord(tree));
        } catch (Exception e) {
            return MappingResult.invalid(ReasonCode.MALFORMED_JSON, e.getMessage());
        }
    }
}
