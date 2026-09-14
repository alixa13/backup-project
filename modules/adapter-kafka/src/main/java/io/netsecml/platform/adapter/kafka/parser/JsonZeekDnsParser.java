package io.netsecml.platform.adapter.kafka.parser;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.netsecml.platform.adapter.kafka.dto.ZeekDnsEvent;
import io.netsecml.platform.domain.event.MappingResult;
import io.netsecml.platform.domain.event.ReasonCode;

public final class JsonZeekDnsParser {
    private final ObjectMapper objectMapper = new ObjectMapper();

    public MappingResult<ZeekDnsEvent> parse(byte[] json) {
        try {
            ZeekDnsEvent dto = objectMapper.readValue(json, ZeekDnsEvent.class);
            return MappingResult.valid(dto);
        } catch (Exception e) {
            return MappingResult.invalid(ReasonCode.MALFORMED_JSON, e.getMessage());
        }
    }
}
