package io.netsecml.platform.adapter.flink.process;

import io.netsecml.platform.domain.feature.FeatureVector;
import io.netsecml.platform.domain.feature.S7commConnectionKey;

// An S7comm feature vector as s7comm-features hands it to s7comm-score
// (scoring design section 4): its connection key, whether the connection
// started from empty state on this event, and the connection's endpoints --
// none of which the vector itself carries.
public record KeyedS7commVector(S7commConnectionKey key, FeatureVector vector, boolean freshState, String clientIp,
                                String serverIp) {
}
