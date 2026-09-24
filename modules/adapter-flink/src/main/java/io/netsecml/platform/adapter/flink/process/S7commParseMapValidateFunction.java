package io.netsecml.platform.adapter.flink.process;

import io.netsecml.platform.adapter.kafka.dto.ZeekS7commRecord;
import io.netsecml.platform.adapter.kafka.mapper.S7commEventMapper;
import io.netsecml.platform.adapter.kafka.parser.JsonZeekS7commParser;
import io.netsecml.platform.domain.event.EventId;
import io.netsecml.platform.domain.event.MappingResult;
import io.netsecml.platform.domain.event.NetworkEvent;
import io.netsecml.platform.domain.event.SensorId;
import org.apache.flink.api.common.functions.OpenContext;

import java.time.Clock;
import java.util.OptionalInt;

// s7comm's parse/map/validate stage: JsonZeekS7commParser then
// S7commEventMapper, with every rejection routed to the side output by
// ParseMapValidateFunction, exactly as the conn, dns and modbus subclasses do.
public final class S7commParseMapValidateFunction extends ParseMapValidateFunction<ZeekS7commRecord> {

    // Built in open(), not serialized with the function.
    private transient JsonZeekS7commParser parser;
    private transient S7commEventMapper mapper;

    public S7commParseMapValidateFunction(SensorId sensor) {
        super(sensor);
    }

    public S7commParseMapValidateFunction(SensorId sensor, Clock clock) {
        super(sensor, clock);
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        super.open(openContext);
        parser = new JsonZeekS7commParser();
        mapper = new S7commEventMapper();
    }

    @Override
    protected MappingResult<ZeekS7commRecord> parse(byte[] rawPayload) {
        return parser.parse(rawPayload);
    }

    @Override
    protected MappingResult<NetworkEvent> map(ZeekS7commRecord dto, SensorId sensor) {
        return mapper.map(dto, sensor);
    }

    // A map-stage rejection's correlation id: sensor:uid:pdu_reference, or
    // sensor:uid when the reference is what could not be read -- never the
    // full sensor:uid:pdu_reference:direction:ts_millis id, since a rejection
    // may be about exactly the direction or the timestamp. No uid, no id.
    @Override
    protected String deriveEventId(ZeekS7commRecord dto) {
        String uid = dto.uid();
        if (uid == null || uid.isBlank()) {
            return null;
        }
        OptionalInt pdu = S7commEventMapper.pduReferenceOf(dto);
        String upstreamId = pdu.isPresent() ? uid + ":" + pdu.getAsInt() : uid;
        return EventId.derive(sensor(), upstreamId).value();
    }
}
