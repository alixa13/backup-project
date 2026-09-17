package io.netsecml.platform.adapter.flink.process;

import io.netsecml.platform.adapter.kafka.dto.ZeekDnsEvent;
import io.netsecml.platform.adapter.kafka.mapper.DnsEventMapper;
import io.netsecml.platform.adapter.kafka.parser.JsonZeekDnsParser;
import io.netsecml.platform.domain.event.EventId;
import io.netsecml.platform.domain.event.MappingResult;
import io.netsecml.platform.domain.event.NetworkEvent;
import io.netsecml.platform.domain.event.SensorId;
import java.time.Clock;

// dns.log's half of the shared parse-map-validate body. Unlike conn, dns's event
// id needs BOTH the upstream uid and trans_id: a resolver reuses one connection
// for many queries, so several dns.log records legitimately share a uid, and
// trans_id is what keeps their derived ids distinct (spec section 5).
public final class DnsParseMapValidateFunction extends ParseMapValidateFunction<ZeekDnsEvent> {
    private transient JsonZeekDnsParser parser;
    private transient DnsEventMapper mapper;

    // Convenience entry point for production wiring, where nothing needs to
    // control the clock: defaults to the real system clock.
    public DnsParseMapValidateFunction(SensorId sensor) {
        super(sensor);
    }

    // Primary constructor: tests call this overload with a fixed Clock so
    // receivedAt becomes an assertable, known value instead of "now".
    public DnsParseMapValidateFunction(SensorId sensor, Clock clock) {
        super(sensor, clock);
    }

    @Override
    public void open(org.apache.flink.api.common.functions.OpenContext openContext) throws Exception {
        super.open(openContext);
        // Both are stateless and cheap to hold; building them once per subtask
        // keeps them out of the per-record path.
        parser = new JsonZeekDnsParser();
        mapper = new DnsEventMapper();
    }

    @Override
    protected MappingResult<ZeekDnsEvent> parse(byte[] rawPayload) {
        return parser.parse(rawPayload);
    }

    @Override
    protected MappingResult<NetworkEvent> map(ZeekDnsEvent dto, SensorId sensor) {
        return mapper.map(dto, sensor);
    }

    // id must be checked blank BEFORE it is combined with trans_id: a blank id
    // produces ":4242", which is NOT blank, so EventId.derive's own guard would
    // never fire and "sensor::4242" would be written to the DLQ as if it were a
    // real identity. DnsEventMapper guards the same trap on the success path;
    // this is the rejected path's copy of that guard.
    @Override
    protected String deriveEventId(ZeekDnsEvent dto) {
        String upstreamId = dto.id();
        if (upstreamId == null || upstreamId.isBlank()) {
            return null;
        }
        return EventId.derive(sensor(), upstreamId + ":" + dto.transId()).value();
    }
}
