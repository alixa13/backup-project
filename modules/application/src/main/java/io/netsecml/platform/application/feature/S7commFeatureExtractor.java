package io.netsecml.platform.application.feature;

import io.netsecml.platform.domain.event.S7commEvent;
import io.netsecml.platform.domain.feature.FeatureDefinition;
import io.netsecml.platform.domain.feature.S7commCategories;
import io.netsecml.platform.domain.feature.S7commConnectionState;
import io.netsecml.platform.domain.feature.S7commFeatureSchemaV1;

import java.util.HashMap;
import java.util.Map;

// All 16 values of s7comm-feature-v1 for one S7commEvent, read from the
// connection state AFTER the event was applied (upstream computes every
// frozen feature after its own updates), plus the two per-event values the
// state cannot hold: is_request_direction from the event, s7_function_changed
// from the advance step. The two categorical slots carry S7commCategories'
// codes. A pure function: it never mutates the state.
public final class S7commFeatureExtractor {

    // Every index resolved by name against the frozen schema, as
    // ModbusFeatureExtractor does, so a reordered schema fails at class-init
    // time instead of silently misaligning a literal.
    private static final Map<String, Integer> INDEX = buildIndex();

    private static final int OUTSTANDING = indexOf("s7_outstanding_requests");
    private static final int OUTSTANDING_MEAN = indexOf("s7_outstanding_mean_16");
    private static final int RESPONSE_MATCH_RATE = indexOf("s7_response_match_rate_16");
    private static final int SAME_FUNCTION_RUN = indexOf("s7_same_function_run_length");
    private static final int SAME_DIRECTION_RUN = indexOf("s7_same_direction_run_length");
    private static final int REQUEST_RATIO = indexOf("s7_request_ratio_16");
    private static final int DIRECTION_CHANGE_RATE = indexOf("s7_direction_change_rate_16");
    private static final int FUNCTION_CHANGE_RATE = indexOf("s7_function_change_rate_16");
    private static final int FUNCTION_ENTROPY = indexOf("s7_function_entropy_16");
    private static final int TRANSITION_ENTROPY = indexOf("s7_function_transition_entropy_16");
    private static final int ROSCTR_CHANGE_RATE = indexOf("s7_rosctr_change_rate_16");
    private static final int PDU_UNIQUE_RATIO = indexOf("s7_pdu_reference_unique_ratio_32");
    private static final int IS_REQUEST = indexOf("is_request_direction");
    private static final int FUNCTION_CHANGED = indexOf("s7_function_changed");
    private static final int ROSCTR = indexOf("s7_rosctr");
    private static final int OPERATION = indexOf("s7_operation");

    private static Map<String, Integer> buildIndex() {
        Map<String, Integer> map = new HashMap<>();
        for (FeatureDefinition definition : S7commFeatureSchemaV1.SCHEMA.definitions()) {
            map.put(definition.name(), definition.index());
        }
        return map;
    }

    private static int indexOf(String name) {
        Integer index = INDEX.get(name);
        if (index == null) {
            throw new IllegalStateException("s7comm-feature-v1 schema has no feature named \"" + name + "\"");
        }
        return index;
    }

    public float[] extract(S7commEvent event, S7commConnectionState after, S7commConnectionState.Step step) {
        float[] vector = new float[S7commFeatureSchemaV1.SCHEMA.featureCount()];

        // The 12 continuous features, each a read of the advanced state; each
        // double is rounded to float32 once, here.
        vector[OUTSTANDING] = after.outstandingRequests();
        vector[OUTSTANDING_MEAN] = (float) after.outstandingMean16();
        vector[RESPONSE_MATCH_RATE] = (float) after.responseMatchRate16();
        vector[SAME_FUNCTION_RUN] = after.sameFunctionRunLength();
        vector[SAME_DIRECTION_RUN] = after.sameDirectionRunLength();
        vector[REQUEST_RATIO] = (float) after.requestRatio16();
        vector[DIRECTION_CHANGE_RATE] = (float) after.directionChangeRate16();
        vector[FUNCTION_CHANGE_RATE] = (float) after.functionChangeRate16();
        vector[FUNCTION_ENTROPY] = (float) after.functionEntropy16();
        vector[TRANSITION_ENTROPY] = (float) after.functionTransitionEntropy16();
        vector[ROSCTR_CHANGE_RATE] = (float) after.rosctrChangeRate16();
        vector[PDU_UNIQUE_RATIO] = (float) after.pduReferenceUniqueRatio32();

        // The 2 binary features: direction from the event, the change flag
        // from this event's own advance step (always 0 for a response).
        vector[IS_REQUEST] = event.isRequest() ? 1f : 0f;
        vector[FUNCTION_CHANGED] = step.functionChanged() ? 1f : 0f;

        // The 2 categorical codes; a scorer decodes them with S7commCategories.
        vector[ROSCTR] = S7commCategories.encodeRosctr(event.rosctrCode());
        vector[OPERATION] = S7commCategories.encodeOperation(event.functionCode(), event.functionName());

        return vector;
    }
}
