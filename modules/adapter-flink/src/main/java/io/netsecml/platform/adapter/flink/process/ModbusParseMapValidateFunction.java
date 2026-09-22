package io.netsecml.platform.adapter.flink.process;

import io.netsecml.platform.adapter.kafka.dto.ZeekModbusRecord;
import io.netsecml.platform.adapter.kafka.mapper.ModbusEventMapper;
import io.netsecml.platform.adapter.kafka.parser.JsonZeekModbusParser;
import io.netsecml.platform.domain.event.EventId;
import io.netsecml.platform.domain.event.MappingResult;
import io.netsecml.platform.domain.event.NetworkEvent;
import io.netsecml.platform.domain.event.SensorId;
import java.time.Clock;

// modbus_detailed.log's half of the shared parse-map-validate body, mirroring
// DnsParseMapValidateFunction exactly: extend the generic base, supply the
// per-protocol parser/mapper in open(), and reuse the base's REJECTED_TAG for
// the DLQ side output -- no second OutputTag is declared here, because two
// tags would split modbus rejections across two side outputs and only one of
// them would be wired to the DLQ sink.
public final class ModbusParseMapValidateFunction extends ParseMapValidateFunction<ZeekModbusRecord> {
    private transient JsonZeekModbusParser parser;
    private transient ModbusEventMapper mapper;

    // Convenience entry point for production wiring, where nothing needs to
    // control the clock: defaults to the real system clock.
    public ModbusParseMapValidateFunction(SensorId sensor) {
        super(sensor);
    }

    // Primary constructor: tests call this overload with a fixed Clock so
    // receivedAt becomes an assertable, known value instead of "now".
    public ModbusParseMapValidateFunction(SensorId sensor, Clock clock) {
        super(sensor, clock);
    }

    @Override
    public void open(org.apache.flink.api.common.functions.OpenContext openContext) throws Exception {
        super.open(openContext);
        // Both are stateless and cheap to hold; building them once per subtask
        // keeps them out of the per-record path.
        parser = new JsonZeekModbusParser();
        mapper = new ModbusEventMapper();
    }

    @Override
    protected MappingResult<ZeekModbusRecord> parse(byte[] rawPayload) {
        return parser.parse(rawPayload);
    }

    @Override
    protected MappingResult<NetworkEvent> map(ZeekModbusRecord dto, SensorId sensor) {
        return mapper.map(dto, sensor);
    }

    // uid must be checked blank BEFORE it is combined with tid, for the same
    // reason DnsParseMapValidateFunction checks dto.id() first: a blank uid
    // concatenated with tid would still produce a non-blank result, so
    // EventId.derive's own guard would never fire and "sensor::17" would be
    // written to the DLQ as if it were a real identity. ModbusEventMapper
    // guards the same trap on the success path.
    //
    // This id is deliberately narrower than the success path's own
    // (sensor:uid:tid:direction:ts_millis, per ModbusEventMapper) -- it omits
    // direction and the resolved event time because those are exactly the
    // things a map-stage rejection can be ABOUT (an unresolvable direction, an
    // out-of-range ts): a rejected record cannot be assumed to have either
    // one resolvable. uid and tid are both required=true at the JSON-binding
    // level (ZeekModbusRecord), so both are guaranteed present on any dto
    // that reached the map stage at all, which is why they are the only two
    // fields this derivation is safe to combine.
    @Override
    protected String deriveEventId(ZeekModbusRecord dto) {
        String upstreamId = dto.uid();
        if (upstreamId == null || upstreamId.isBlank()) {
            return null;
        }
        return EventId.derive(sensor(), upstreamId + ":" + dto.tid()).value();
    }
}
