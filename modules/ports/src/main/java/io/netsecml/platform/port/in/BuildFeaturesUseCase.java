package io.netsecml.platform.port.in;

import io.netsecml.platform.domain.event.NetworkEvent;
import io.netsecml.platform.domain.feature.FeatureBuildResult;

// Building features from one event of one log type, given that key's current
// window state.
//
// Generic in BOTH the event and the state, because they vary together: a DNS
// implementation takes a DnsEvent and whatever window state DNS needs, and
// neither is a ConnEvent or a ConnWindowState. One implementation per log type,
// so no implementation ever casts or switches to discover what it was given.
public interface BuildFeaturesUseCase<E extends NetworkEvent, S> {
    FeatureBuildResult<S> build(E event, S currentState);
}
