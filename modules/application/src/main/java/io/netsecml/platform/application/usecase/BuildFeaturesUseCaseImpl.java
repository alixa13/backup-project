package io.netsecml.platform.application.usecase;

import io.netsecml.platform.application.feature.EventFeatureExtractor;
import io.netsecml.platform.domain.event.NetworkEvent;
import io.netsecml.platform.domain.feature.ConnFeatureSchemaV1;
import io.netsecml.platform.domain.feature.FeatureBuildResult;
import io.netsecml.platform.domain.feature.FeatureVector;
import io.netsecml.platform.domain.feature.SourceWindowState;
import io.netsecml.platform.port.in.BuildFeaturesUseCase;

public final class BuildFeaturesUseCaseImpl implements BuildFeaturesUseCase {
    private final EventFeatureExtractor eventFeatureExtractor = new EventFeatureExtractor();

    @Override
    public FeatureBuildResult build(NetworkEvent event, SourceWindowState currentState) {
        float[] eventLevel = eventFeatureExtractor.extractEventLevel(event);

        long totalBytes = event.measurements().originBytes() + event.measurements().responseBytes();
        boolean failed = event.connection().connectionState().isFailed();
        long bucketMinute = event.eventTime().getEpochSecond() / 60;
        SourceWindowState newState = currentState.record(bucketMinute, totalBytes, failed);

        float[] values = new float[20];
        System.arraycopy(eventLevel, 0, values, 0, 17);
        values[17] = newState.connectionCount5m();
        values[18] = newState.byteSum5m();
        values[19] = newState.failedCount5m();

        FeatureVector vector = new FeatureVector(
            event.eventId().value(),
            event.eventTime(),
            ConnFeatureSchemaV1.SCHEMA.id(),
            ConnFeatureSchemaV1.CONTENT_HASH,
            values,
            0);

        return new FeatureBuildResult(vector, newState);
    }
}
