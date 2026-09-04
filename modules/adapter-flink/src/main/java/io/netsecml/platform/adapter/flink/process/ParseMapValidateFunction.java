package io.netsecml.platform.adapter.flink.process;

import io.netsecml.platform.adapter.kafka.dto.ZeekConnEvent;
import io.netsecml.platform.adapter.kafka.mapper.EventMapper;
import io.netsecml.platform.adapter.kafka.parser.JsonZeekConnParser;
import io.netsecml.platform.domain.event.EventId;
import io.netsecml.platform.domain.event.MappingResult;
import io.netsecml.platform.domain.event.NetworkEvent;
import io.netsecml.platform.domain.event.SensorId;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

public final class ParseMapValidateFunction extends ProcessFunction<byte[], NetworkEvent> {
    public static final OutputTag<RejectedRecord> REJECTED_TAG =
        new OutputTag<RejectedRecord>("rejected") {};

    private final SensorId sensor;

    // Injected so receivedAt is assertable in tests. Clock's JDK implementations
    // are Serializable, which they must be to ride along in a Flink function.
    private final Clock clock;

    private transient JsonZeekConnParser parser;
    private transient EventMapper mapper;

    // Convenience entry point for production wiring, where nothing needs to
    // control the clock: defaults to the real system clock.
    public ParseMapValidateFunction(SensorId sensor) {
        this(sensor, Clock.systemUTC());
    }

    // Primary constructor: assigns both fields directly. Tests call this overload
    // with a fixed Clock so receivedAt becomes an assertable, known value instead
    // of "now".
    public ParseMapValidateFunction(SensorId sensor, Clock clock) {
        this.sensor = sensor;
        this.clock = clock;
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
    public void processElement(byte[] rawPayload, Context ctx, Collector<NetworkEvent> out) {
        // Truncated to milliseconds because it lands in a DateTime64(3) column.
        Instant receivedAt = clock.instant().truncatedTo(ChronoUnit.MILLIS);

        // Stage 1 — parse. A failure here produced no DTO, so there is no event
        // identity to attach.
        MappingResult<ZeekConnEvent> parsed = parser.parse(rawPayload);
        if (!parsed.isValid()) {
            ctx.output(REJECTED_TAG,
                new RejectedRecord(rawPayload, parsed.reason(), parsed.detail(), receivedAt, null));
            return;
        }

        // Stage 2 — map and validate. A failure here DID parse, so the record can
        // be named using the same composite ID the feature path derives. That is
        // what makes an invalid_events row joinable to feature_vectors.event_id.
        MappingResult<NetworkEvent> mapped = mapper.map(parsed.value(), sensor);
        if (!mapped.isValid()) {
            ctx.output(REJECTED_TAG,
                new RejectedRecord(rawPayload, mapped.reason(), mapped.detail(), receivedAt,
                    deriveEventId(parsed.value())));
            return;
        }

        out.collect(mapped.value());
    }

    // EventId.derive refuses a blank upstream id. A DTO that parsed but carries a
    // blank id would otherwise throw here and kill the subtask, so fall back to
    // no identity rather than failing the whole job over a cosmetic field.
    private String deriveEventId(ZeekConnEvent dto) {
        String upstreamId = dto.id();
        if (upstreamId == null || upstreamId.isBlank()) {
            return null;
        }
        return EventId.derive(sensor, upstreamId).value();
    }
}
