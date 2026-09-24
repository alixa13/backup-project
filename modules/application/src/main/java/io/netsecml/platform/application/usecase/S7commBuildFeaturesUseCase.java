package io.netsecml.platform.application.usecase;

import io.netsecml.platform.application.feature.S7commFeatureExtractor;
import io.netsecml.platform.domain.event.LogType;
import io.netsecml.platform.domain.event.S7commEvent;
import io.netsecml.platform.domain.feature.FeatureBuildResult;
import io.netsecml.platform.domain.feature.FeatureSchema;
import io.netsecml.platform.domain.feature.FeatureSchemaRegistry;
import io.netsecml.platform.domain.feature.FeatureVector;
import io.netsecml.platform.domain.feature.QualityFlags;
import io.netsecml.platform.domain.feature.S7commConnectionState;
import io.netsecml.platform.domain.feature.S7commFeatureSchemaV1;
import io.netsecml.platform.port.in.BuildFeaturesUseCase;

import java.time.Clock;
import java.time.temporal.ChronoUnit;
import java.util.Objects;

// One S7comm record -> one s7comm-feature-v1 vector: advance the connection
// state in place with the event, extract the 16 values from the advanced
// state, flag an out-of-order record. The same shape, Clock injection and
// constructor overloads as ModbusBuildFeaturesUseCase.
public final class S7commBuildFeaturesUseCase
        implements BuildFeaturesUseCase<S7commEvent, S7commConnectionState> {

    private final S7commFeatureExtractor extractor = new S7commFeatureExtractor();

    // Injected so producedAt is deterministic under test.
    private final Clock clock;

    // Resolved once, in the constructor: an unresolvable schema fails at
    // wiring time, not on the first record.
    private final FeatureSchema schema;

    // Used by S7commFeatureProcessFunction.open().
    public S7commBuildFeaturesUseCase() {
        this(Clock.systemUTC());
    }

    // For callers (tests) that need a fixed Clock.
    public S7commBuildFeaturesUseCase(Clock clock) {
        this(clock, FeatureSchemaRegistry.byLogType(LogType.S7COMM));
    }

    // Package-private: lets a test drive a schema other than the registered one.
    S7commBuildFeaturesUseCase(Clock clock, FeatureSchema schema) {
        this.clock = Objects.requireNonNull(clock, "clock must not be null");
        this.schema = Objects.requireNonNull(schema, "schema must not be null");

        // The vector is labelled from `schema` but sized and ordered by
        // S7commFeatureExtractor from S7commFeatureSchemaV1.SCHEMA: two sources
        // that agree only while the registry returns that same static. Checked
        // here, once, so a divergence fails at startup instead of mislabelling
        // every vector (ModbusBuildFeaturesUseCase's own check, for the same reason).
        FeatureSchema extractorSchema = S7commFeatureSchemaV1.SCHEMA;
        if (!schema.id().equals(extractorSchema.id())
            || !schema.contentHash().equals(extractorSchema.contentHash())
            || schema.featureCount() != extractorSchema.featureCount()) {
            throw new IllegalArgumentException("schema '" + schema.id() + "' does not match "
                + "S7commFeatureExtractor's own S7commFeatureSchemaV1.SCHEMA ('" + extractorSchema.id() + "')");
        }
    }

    // Advances `currentState` IN PLACE and returns that same instance as the
    // result's newState, as ModbusBuildFeaturesUseCase does. Nothing after
    // advance(...) throws on a valid S7commEvent, and the Flink operator
    // catches nothing: a throw fails the task and Flink restores the state
    // from the last checkpoint.
    @Override
    public FeatureBuildResult<S7commConnectionState> build(S7commEvent event, S7commConnectionState currentState) {
        // Apply the event; every feature is read after it, as upstream does.
        S7commConnectionState.Step step = currentState.advance(event.tsSeconds(), event.isRequest(),
            event.pduReference(), event.rosctrCode(), event.functionCode());
        float[] values = extractor.extract(event, currentState, step);

        // Out-of-order is observable, never corrective: nothing was reset.
        int qualityFlags = step.outOfOrder() ? QualityFlags.S7COMM_OUT_OF_ORDER : QualityFlags.NONE;

        // Identity and timing pass straight through from the event; producedAt
        // is truncated to milliseconds for its DateTime64(3) column.
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
        return new FeatureBuildResult<>(vector, currentState);
    }
}
