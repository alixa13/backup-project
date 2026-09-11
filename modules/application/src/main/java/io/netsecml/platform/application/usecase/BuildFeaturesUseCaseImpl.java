package io.netsecml.platform.application.usecase;

import io.netsecml.platform.application.feature.EventFeatureExtractor;
import io.netsecml.platform.domain.event.ConnEvent;
import io.netsecml.platform.domain.event.NetworkEvent;
import io.netsecml.platform.domain.feature.ConnFeatureSchemaV1;
import io.netsecml.platform.domain.feature.FeatureBuildResult;
import io.netsecml.platform.domain.feature.FeatureVector;
import io.netsecml.platform.domain.feature.SourceWindowState;
import io.netsecml.platform.port.in.BuildFeaturesUseCase;
import java.time.Clock;
import java.time.temporal.ChronoUnit;
import java.util.Objects;

public final class BuildFeaturesUseCaseImpl implements BuildFeaturesUseCase {
    private final EventFeatureExtractor eventFeatureExtractor = new EventFeatureExtractor();

    // Injected so producedAt is deterministic under test. Clock's JDK
    // implementations are Serializable, which matters if this ever moves into a
    // Flink function field rather than being built in open().
    private final Clock clock;

    // Kept so ConnFeatureProcessFunction.open()'s existing no-arg construction
    // compiles unchanged.
    public BuildFeaturesUseCaseImpl() {
        this(Clock.systemUTC());
    }

    // Overload used by callers (tests, and later tasks) that need a fixed or
    // fake Clock.
    public BuildFeaturesUseCaseImpl(Clock clock) {
        this.clock = Objects.requireNonNull(clock, "clock must not be null");
    }

    @Override
    public FeatureBuildResult build(NetworkEvent event, SourceWindowState currentState) {
        // The single switch that reaches ConnEvent for this whole use case. A
        // pattern switch rather than a cast, so the compiler flags this site when
        // a second log type joins the hierarchy and this code has to decide what
        // building features means for that type.
        ConnEvent conn = switch (event) {
            case ConnEvent c -> c;
        };

        // Indices 0-16: deterministic, event-local features.
        float[] eventLevel = eventFeatureExtractor.extractEventLevel(conn);

        // Fold this event into the bounded 5-bucket rolling window. record()
        // returns a NEW state; the caller is responsible for storing it.
        long totalBytes = conn.measurements().originBytes() + conn.measurements().responseBytes();
        boolean failed = conn.connection().connectionState().isFailed();
        long bucketMinute = event.eventTime().getEpochSecond() / 60;
        SourceWindowState newState = currentState.record(bucketMinute, totalBytes, failed);

        // Indices 17-19 come from the window AFTER this event is folded in.
        float[] values = new float[20];
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
            ConnFeatureSchemaV1.SCHEMA.id(),
            ConnFeatureSchemaV1.CONTENT_HASH,
            values,
            0,
            clock.instant().truncatedTo(ChronoUnit.MILLIS));

        return new FeatureBuildResult(vector, newState);
    }
}
