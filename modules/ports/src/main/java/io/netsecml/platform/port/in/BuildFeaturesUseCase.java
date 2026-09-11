package io.netsecml.platform.port.in;

import io.netsecml.platform.domain.event.NetworkEvent;
import io.netsecml.platform.domain.feature.ConnWindowState;
import io.netsecml.platform.domain.feature.FeatureBuildResult;

public interface BuildFeaturesUseCase {
    FeatureBuildResult build(NetworkEvent event, ConnWindowState currentState);
}
