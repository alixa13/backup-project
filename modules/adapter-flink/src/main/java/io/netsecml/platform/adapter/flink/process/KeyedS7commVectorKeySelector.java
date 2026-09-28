package io.netsecml.platform.adapter.flink.process;

import io.netsecml.platform.domain.feature.S7commConnectionKey;
import org.apache.flink.api.java.functions.KeySelector;

// s7comm-score is keyed by the same connection key as s7comm-features.
public final class KeyedS7commVectorKeySelector implements KeySelector<KeyedS7commVector, S7commConnectionKey> {
    @Override
    public S7commConnectionKey getKey(KeyedS7commVector value) {
        return value.key();
    }
}
