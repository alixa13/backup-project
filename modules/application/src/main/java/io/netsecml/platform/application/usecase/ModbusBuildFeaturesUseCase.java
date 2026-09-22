package io.netsecml.platform.application.usecase;

import io.netsecml.platform.application.feature.ModbusFeatureExtractor;
import io.netsecml.platform.domain.event.LogType;
import io.netsecml.platform.domain.event.ModbusEvent;
import io.netsecml.platform.domain.feature.FeatureBuildResult;
import io.netsecml.platform.domain.feature.FeatureSchema;
import io.netsecml.platform.domain.feature.FeatureSchemaRegistry;
import io.netsecml.platform.domain.feature.FeatureVector;
import io.netsecml.platform.domain.feature.ModbusEntityState;
import io.netsecml.platform.domain.feature.QualityFlags;
import io.netsecml.platform.port.in.BuildFeaturesUseCase;
import java.time.Clock;
import java.time.temporal.ChronoUnit;
import java.util.Objects;

// Owns Ruling 5 end to end: decide whether this event starts a new causal
// segment, reset state if so, extract every one of modbus-feature-v1's 42
// values from the state as it stood BEFORE this event, and only then advance
// the state. Mirrors ConnBuildFeaturesUseCase / DnsBuildFeaturesUseCase's
// shape throughout -- same Clock injection, same constructor overloads, same
// resolved-once-in-the-constructor schema -- because all three classes are
// the same pattern applied to a different log type, not three independently
// invented ones.
public final class ModbusBuildFeaturesUseCase implements BuildFeaturesUseCase<ModbusEvent, ModbusEntityState> {
    private final ModbusFeatureExtractor modbusFeatureExtractor = new ModbusFeatureExtractor();

    // Injected so producedAt is deterministic under test, same rationale as
    // ConnBuildFeaturesUseCase's field of the same name.
    private final Clock clock;

    // Resolved once, in the constructor, rather than per record inside
    // build() -- see ConnBuildFeaturesUseCase's field of the same name for
    // the full rationale (fail-fast at wiring time rather than on the first
    // record).
    private final FeatureSchema schema;

    // Kept so a future ModbusFeatureProcessFunction.open()'s no-arg
    // construction compiles unchanged, same as Conn/DnsBuildFeaturesUseCase's
    // no-arg constructor.
    public ModbusBuildFeaturesUseCase() {
        this(Clock.systemUTC());
    }

    // Overload used by callers (tests, and a later task) that need a fixed or
    // fake Clock.
    public ModbusBuildFeaturesUseCase(Clock clock) {
        this(clock, FeatureSchemaRegistry.byLogType(LogType.MODBUS));
    }

    // Package-private: lets a test drive this against a schema other than the
    // registered modbus-feature-v1. NOT public -- production has exactly one
    // correct schema for this class, and it is the registered one the public
    // constructors resolve.
    ModbusBuildFeaturesUseCase(Clock clock, FeatureSchema schema) {
        this.clock = Objects.requireNonNull(clock, "clock must not be null");
        this.schema = Objects.requireNonNull(schema, "schema must not be null");
    }

    @Override
    public FeatureBuildResult<ModbusEntityState> build(ModbusEvent event, ModbusEntityState currentState) {
        // Shared, not duplicated: both this class and ModbusFeatureExtractor
        // need the event's timestamp as the same fractional-second double, and
        // ModbusFeatureExtractor.epochSeconds is the ONE place that conversion
        // is defined (see its own comment). Calling it here, rather than
        // reimplementing getEpochSecond()+getNano()/1e9 a second time, is what
        // keeps `after` built against the exact ts the extractor itself used
        // for inter_arrival_s, rtt_s and every window boundary.
        double ts = ModbusFeatureExtractor.epochSeconds(event.envelope().eventTime());

        // Step 1: decide, from the state as it stood BEFORE this event,
        // whether this event starts a new causal segment -- a first event for
        // this key, a gap over 15s, or time going backwards (see
        // ModbusEntityState.startsNewSegment).
        boolean newSegment = currentState.startsNewSegment(ts);

        // Of those three causes, only time going backwards is a data-quality
        // event worth flagging. currentState.lastTs() is null for a key's
        // first-ever event -- that must NOT be flagged, so the null check
        // comes first and short-circuits before the subtraction.
        Double lastTs = currentState.lastTs();
        boolean outOfOrder = lastTs != null && ts - lastTs < 0.0;

        // Step 2: reset to empty across a segment boundary BEFORE extracting.
        // ModbusFeatureExtractor.extract asserts that `before` is
        // ModbusEntityState.empty() whenever newSegment is true, so `before`
        // must already be the reset state here, never currentState itself.
        ModbusEntityState before = newSegment ? currentState.resetForNewSegment() : currentState;

        // Step 3: advance state from `before` using this event's own fields,
        // at the same ts the extractor computes below.
        ModbusEntityState after = before.afterEvent(ts, event.functionCode(), event.transactionId(),
            event.direction(), event.address(), event.quantity());

        // Step 4: every current-event feature (groups A-C) reads `before` --
        // the state as it stood when this event arrived, so a request does
        // not count itself in outstanding_requests_before_event (index 30).
        // The trailing-window rates/ratios (group D) read `after`, so the
        // current event is counted in its own windows. See
        // ModbusFeatureExtractor's own javadoc for the group-by-group
        // rationale.
        float[] values = modbusFeatureExtractor.extract(event, before, after, newSegment);

        // Step 5: width, id and hash all come from the schema resolved in the
        // constructor -- FeatureSchemaRegistry.byLogType(LogType.MODBUS) in
        // production -- never from a literal.
        //
        // Step 6: MODBUS_OUT_OF_ORDER is set only for the negative-gap case
        // decided above, never for an ordinary >15s gap or a key's first
        // event; modbus-feature-v1 carries no common tier, so this vector
        // never sets CONN_ENRICHMENT_ABSENT (bit 0).
        int qualityFlags = outOfOrder ? QualityFlags.MODBUS_OUT_OF_ORDER : QualityFlags.NONE;

        // logType and connectionUid pass straight through from the event,
        // unchanged. producedAt is truncated to milliseconds because it lands
        // in a DateTime64(3) row_version; finer precision would not
        // round-trip.
        FeatureVector vector = new FeatureVector(
            event.eventId().value(),
            event.eventTime(),
            event.sensor(),
            event.logType(),
            event.connectionUid(),
            schema.id(),
            schema.contentHash(),
            values,
            qualityFlags,
            clock.instant().truncatedTo(ChronoUnit.MILLIS));

        return new FeatureBuildResult<>(vector, after);
    }
}
