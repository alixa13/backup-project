package io.netsecml.platform.adapter.flink.process;

import io.netsecml.platform.adapter.kafka.dto.ZeekConnEvent;
import io.netsecml.platform.adapter.kafka.mapper.EventMapper;
import io.netsecml.platform.adapter.kafka.parser.JsonZeekConnParser;
import io.netsecml.platform.domain.event.EventId;
import io.netsecml.platform.domain.event.MappingResult;
import io.netsecml.platform.domain.event.NetworkEvent;
import io.netsecml.platform.domain.event.SensorId;
import java.time.Clock;

// conn.log's half of the shared parse-map-validate body. Behaviourally identical
// to the pre-generification ParseMapValidateFunction -- this class only supplies
// the conn-specific parser, mapper and event-id derivation the abstract base now
// delegates to.
public final class ConnParseMapValidateFunction extends ParseMapValidateFunction<ZeekConnEvent> {
    private transient JsonZeekConnParser parser;
    private transient EventMapper mapper;

    // Convenience entry point for production wiring, where nothing needs to
    // control the clock: defaults to the real system clock.
    public ConnParseMapValidateFunction(SensorId sensor) {
        super(sensor);
    }

    // Primary constructor: tests call this overload with a fixed Clock so
    // receivedAt becomes an assertable, known value instead of "now".
    public ConnParseMapValidateFunction(SensorId sensor, Clock clock) {
        super(sensor, clock);
    }

    @Override
    public void open(org.apache.flink.api.common.functions.OpenContext openContext) throws Exception {
        super.open(openContext);
        // Both are stateless and cheap to hold; building them once per subtask
        // keeps them out of the per-record path.
        parser = new JsonZeekConnParser();
        mapper = new EventMapper();
    }

    @Override
    protected MappingResult<ZeekConnEvent> parse(byte[] rawPayload) {
        return parser.parse(rawPayload);
    }

    @Override
    protected MappingResult<NetworkEvent> map(ZeekConnEvent dto, SensorId sensor) {
        return mapper.map(dto, sensor);
    }

    // EventId.derive refuses a blank upstream id. A DTO that parsed but carries a
    // blank id would otherwise throw here and kill the subtask, so fall back to
    // no identity rather than failing the whole job over a cosmetic field.
    @Override
    protected String deriveEventId(ZeekConnEvent dto) {
        String upstreamId = dto.id();
        if (upstreamId == null || upstreamId.isBlank()) {
            return null;
        }
        return EventId.derive(sensor(), upstreamId).value();
    }
}
