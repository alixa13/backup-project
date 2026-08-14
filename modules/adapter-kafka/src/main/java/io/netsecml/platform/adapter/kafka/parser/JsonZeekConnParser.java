package io.netsecml.platform.adapter.kafka.parser;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.netsecml.platform.adapter.kafka.dto.ZeekConnEvent;
import io.netsecml.platform.domain.event.MappingResult;
import io.netsecml.platform.domain.event.ReasonCode;

public final class JsonZeekConnParser {
    private final ObjectMapper objectMapper = new ObjectMapper();

    public MappingResult<ZeekConnEvent> parse(byte[] json) {
        try {
            ZeekConnEvent dto = objectMapper.readValue(json, ZeekConnEvent.class);
            return MappingResult.valid(dto);
        } catch (Exception e) {
            return MappingResult.invalid(ReasonCode.MALFORMED_JSON, e.getMessage());
        }
    }
}
