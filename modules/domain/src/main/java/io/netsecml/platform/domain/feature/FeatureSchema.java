package io.netsecml.platform.domain.feature;

import java.util.Collections;
import java.util.List;

public record FeatureSchema(String id, String semanticVersion, String contentHash, List<FeatureDefinition> definitions) {
    public FeatureSchema {
        definitions = Collections.unmodifiableList(definitions);
    }

    public int featureCount() {
        return definitions.size();
    }
}
