package io.netsecml.platform.application.usecase;

import io.netsecml.platform.application.feature.EventFeatureExtractor;
import io.netsecml.platform.domain.event.ConnEvent;
import io.netsecml.platform.domain.event.LogType;
import io.netsecml.platform.domain.feature.ConnWindowState;
import io.netsecml.platform.domain.feature.FeatureBuildResult;
import io.netsecml.platform.domain.feature.FeatureSchema;
import io.netsecml.platform.domain.feature.FeatureSchemaRegistry;
import io.netsecml.platform.domain.feature.FeatureVector;
import io.netsecml.platform.port.in.BuildFeaturesUseCase;
import java.time.Clock;
import java.time.temporal.ChronoUnit;
import java.util.Objects;

public final class ConnBuildFeaturesUseCase implements BuildFeaturesUseCase<ConnEvent, ConnWindowState> {
    private final EventFeatureExtractor eventFeatureExtractor = new EventFeatureExtractor();

    // Injected so producedAt is deterministic under test. Clock's JDK
    // implementations are Serializable, which matters if this ever moves into a
    // Flink function field rather than being built in open().
    private final Clock clock;

    // Kept so ConnFeatureProcessFunction.open()'s existing no-arg construction
    // compiles unchanged.
    public ConnBuildFeaturesUseCase() {
        this(Clock.systemUTC());
    }

    // Overload used by callers (tests, and later tasks) that need a fixed or
    // fake Clock.
    public ConnBuildFeaturesUseCase(Clock clock) {
        this.clock = Objects.requireNonNull(clock, "clock must not be null");
    }

    @Override
    public FeatureBuildResult<ConnWindowState> build(ConnEvent event, ConnWindowState currentState) {
        // Indices 0-16: deterministic, event-local features.
        float[] eventLevel = eventFeatureExtractor.extractEventLevel(event);

        // Fold this event into the bounded 5-bucket rolling window. record()
        // returns a NEW state; the caller is responsible for storing it.
        long totalBytes = event.measurements().originBytes() + event.measurements().responseBytes();
        boolean failed = event.connection().connectionState().isFailed();
        long bucketMinute = event.eventTime().getEpochSecond() / 60;
        ConnWindowState newState = currentState.record(bucketMinute, totalBytes, failed);

        // The width comes from the schema rather than a literal, which is what
        // lets a log type with a different feature count use this same shape. For
        // conn the schema reports 20, so this produces exactly what the old
        // new float[20] produced.
        FeatureSchema schema = FeatureSchemaRegistry.byLogType(LogType.CONN);
        float[] values = new float[schema.featureCount()];

        // Indices 17-19 come from the window AFTER this event is folded in.
        // These three indices stay hardcoded: this class is conn's own
        // implementation and is entitled to know conn's frozen 20-wide layout.
        // Only the array's width and the schema identity below become dynamic.
        System.arraycopy(eventLevel, 0, values, 0, 17);
        values[17] = newState.connectionCount5m();
        values[18] = newState.byteSum5m();
        values[19] = newState.failedCount5m();

        // logType and connectionUid pass straight through from the event, unchanged.
        // producedAt is truncated to milliseconds because it lands in a
        // DateTime64(3) row_version; finer precision would not round-trip.
        FeatureVector vector = new FeatureVector(
            event.eventId().value(),
            event.eventTime(),
            event.sensor(),
            event.logType(),
            event.connectionUid(),
            schema.id(),
            schema.contentHash(),
            values,
            0,
            clock.instant().truncatedTo(ChronoUnit.MILLIS));

        return new FeatureBuildResult<>(vector, newState);
    }
}
