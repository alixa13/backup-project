package io.netsecml.platform.port.in;

import io.netsecml.platform.domain.event.NetworkEvent;
import io.netsecml.platform.domain.feature.FeatureBuildResult;
import io.netsecml.platform.domain.feature.SourceWindowState;

public interface BuildFeaturesUseCase {
    FeatureBuildResult build(NetworkEvent event, SourceWindowState currentState);
}
