package io.netsecml.platform.adapter.flink.process;

import io.netsecml.platform.domain.event.MappingResult;
import io.netsecml.platform.domain.event.NetworkEvent;
import io.netsecml.platform.domain.event.SensorId;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

// Generic in the parsed DTO type D so every log type's parse-map-validate body
// -- parse, side-output on failure; map, side-output with a derived event id on
// failure; collect -- lives in exactly one place instead of being copied per
// protocol. ConnParseMapValidateFunction and DnsParseMapValidateFunction supply
// the per-protocol parser/mapper/event-id-derivation below; nothing about the
// control flow itself differs between them.
public abstract class ParseMapValidateFunction<D> extends ProcessFunction<byte[], NetworkEvent> {
    // One DLQ side output shared by every subclass: a rejection is a rejection
    // regardless of which log type produced it, so there is exactly one tag for
    // OnlineFeatureJob to wire up rather than one per protocol.
    public static final OutputTag<RejectedRecord> REJECTED_TAG =
        new OutputTag<RejectedRecord>("rejected") {};

    private final SensorId sensor;

    // Injected so receivedAt is assertable in tests. Clock's JDK implementations
    // are Serializable, which they must be to ride along in a Flink function.
    private final Clock clock;

    // Convenience entry point for production wiring, where nothing needs to
    // control the clock: defaults to the real system clock.
    protected ParseMapValidateFunction(SensorId sensor) {
        this(sensor, Clock.systemUTC());
    }

    // Primary constructor: assigns both fields directly. Tests call this overload
    // with a fixed Clock so receivedAt becomes an assertable, known value instead
    // of "now".
    protected ParseMapValidateFunction(SensorId sensor, Clock clock) {
        this.sensor = sensor;
        this.clock = clock;
    }

    // deriveEventId implementations need the sensor to build a composite id, but
    // the field itself stays private -- this accessor is the narrower way to
    // share it with subclasses than widening the field's own visibility.
    protected final SensorId sensor() {
        return sensor;
    }

    @Override
    public void processElement(byte[] rawPayload, Context ctx, Collector<NetworkEvent> out) {
        // Truncated to milliseconds because it lands in a DateTime64(3) column.
        Instant receivedAt = clock.instant().truncatedTo(ChronoUnit.MILLIS);

        // Stage 1 — parse. A failure here produced no DTO, so there is no event
        // identity to attach.
        MappingResult<D> parsed = parse(rawPayload);
        if (!parsed.isValid()) {
            ctx.output(REJECTED_TAG,
                new RejectedRecord(rawPayload, parsed.reason(), parsed.detail(), receivedAt, null));
            return;
        }

        // Stage 2 — map and validate. A failure here DID parse, so the record can
        // be named using the same composite ID the feature path derives. That is
        // what makes an invalid_events row joinable to feature_vectors.event_id.
        MappingResult<NetworkEvent> mapped = map(parsed.value(), sensor);
        if (!mapped.isValid()) {
            ctx.output(REJECTED_TAG,
                new RejectedRecord(rawPayload, mapped.reason(), mapped.detail(), receivedAt,
                    deriveEventId(parsed.value())));
            return;
        }

        out.collect(mapped.value());
    }

    // Built in each subclass's open() into a transient field and delegated to
    // here -- never a constructor-injected or lambda parser. A Flink function's
    // fields must be Serializable or built in open(); a lambda/Supplier field is
    // exactly how this project's three serialization defects reached main.
    protected abstract MappingResult<D> parse(byte[] rawPayload);

    // Same open()-built-transient-field rule as parse() above, delegated to each
    // subclass's own mapper.
    protected abstract MappingResult<NetworkEvent> map(D dto, SensorId sensor);

    // Each protocol derives its own event id shape (conn: uid alone; dns: uid +
    // trans_id), but every implementation shares the same reason for existing:
    // EventId.derive refuses a blank upstream id, and a DTO that parsed but
    // carries a blank identity would otherwise throw here and kill the subtask.
    protected abstract String deriveEventId(D dto);
}
