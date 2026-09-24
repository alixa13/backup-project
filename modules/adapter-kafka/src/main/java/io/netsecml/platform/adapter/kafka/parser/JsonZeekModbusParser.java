package io.netsecml.platform.adapter.kafka.parser;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netsecml.platform.adapter.kafka.dto.ZeekModbusRecord;
import io.netsecml.platform.domain.event.MappingResult;
import io.netsecml.platform.domain.event.ReasonCode;

public final class JsonZeekModbusParser {

    // FAIL_ON_NULL_FOR_PRIMITIVES, not "null safety" or some other generic
    // name: that is the exact failure mode this guards against.
    // @JsonProperty(required = true) enforces only that a KEY is present, not
    // that its value is non-null, and this feature is OFF by default. Without
    // it, an explicit "tid": null (or "ts": null) would silently bind to 0
    // (or epoch 0) -- both plausible-looking values for a required primitive
    // field -- rather than fail to parse. This does NOT cover func: func is
    // bound as a String (see ZeekModbusRecord), a reference type, so this
    // flag has no effect on it; parse() below rejects an explicit null func
    // by hand for the identical reason.
    private final ObjectMapper objectMapper = new ObjectMapper()
        .enable(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES);

    public MappingResult<ZeekModbusRecord> parse(byte[] json) {
        try {
            ZeekModbusRecord dto = objectMapper.readValue(json, ZeekModbusRecord.class);

            // See this class's FAIL_ON_NULL_FOR_PRIMITIVES comment above: func
            // is a String, so the flag does not stop an explicit "func": null
            // from binding. Checked by hand here instead.
            if (dto.func() == null) {
                return MappingResult.invalid(ReasonCode.MALFORMED_JSON, "func must not be null");
            }

            return MappingResult.valid(dto);
        } catch (Exception e) {
            return MappingResult.invalid(ReasonCode.MALFORMED_JSON, e.getMessage());
        }
    }
}
