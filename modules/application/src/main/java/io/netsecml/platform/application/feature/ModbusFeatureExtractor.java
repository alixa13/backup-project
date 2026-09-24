package io.netsecml.platform.application.feature;

import io.netsecml.platform.domain.event.ModbusEvent;
import io.netsecml.platform.domain.event.ModbusEvent.ModbusDirection;
import io.netsecml.platform.domain.feature.FeatureDefinition;
import io.netsecml.platform.domain.feature.ModbusEntityState;
import io.netsecml.platform.domain.feature.ModbusFeatureSchemaV1;

import java.util.HashMap;
import java.util.Map;

// Produces all 42 values of modbus-feature-v1 for a single ModbusEvent. A
// direct, deliberately literal port of the upstream offline feature engine's
// per-event body inside process_capture
// (two-models-info/modbus_/07b_materialize_feature_engine_v1.py, roughly
// lines 344-559) -- that file is the authority for every rule here, not this
// class's own comments. Where the two ever disagree, the Python wins.
//
// extract is a pure function of (event, before, after, newSegment) -> vector:
// it never mutates ModbusEntityState itself. Group C (causal deltas /
// pending-TID bookkeeping) reads `before` -- the BeforeEvent snapshot
// ModbusEntityState.advance(...) captured as the state stood when this event
// arrived, matching process_capture's own ordering, which extracts every
// current-event feature BEFORE mutating `state.pending` / `state.prev_fc` /
// `state.last_ts` / `state.last_address` / `state.last_quantity` for this
// event. Groups A (current-event Modbus semantics) and B (numeric value
// summaries) read only the event. Group D (the trailing-window rates and
// ratios) reads `after` -- the state advance(...) left behind -- because
// process_capture purges and appends the current event to its window
// deques/counters (w1_ts, w10_events, w60_ts, fc_counter_10,
// addr_counter_10, read_count_10, write_count_10) BEFORE reading their sizes
// for event_rate_1s/10s/60s, unique_function_count_10s,
// unique_address_count_10s and read/write_ratio_10s -- so the current event
// is always counted in its own windows. The caller (ModbusBuildFeaturesUseCase)
// is responsible for calling advance(...) with this same event's own fields
// -- so `before.pendingTsForTid()` is this event's tid's -- and for
// resetting the state across a segment boundary before calling advance at
// all.
//
// newSegment carries no branch of its own: when true, the caller has already
// reset the state (ModbusEntityState.reset) before advancing, so `before` is
// all-empty and prev_event_available,
// inter_arrival_s, function_changed, address_delta_valid,
// quantity_delta_valid, outstanding_requests_before_event, rtt_valid and
// rtt_s all fall out at zero by ordinary computation -- exactly
// process_capture's own `required_zero` sanity check (lines ~525-543), which
// validates the same fact rather than causing it. The throw below (never a
// Java `assert`; see its own comment) checks that same invariant instead of
// special-casing newSegment in the body.
public final class ModbusFeatureExtractor {

    // Resolved once, by name, against the frozen schema -- so a schema edit
    // (a reordered or renamed feature) fails loudly at class-init time
    // instead of silently misaligning every index literal below. Mirrors
    // process_capture's own `idx = {name: i for i, name in
    // enumerate(feature_names)}` (line 244 of the authority file above).
    private static final Map<String, Integer> INDEX = buildIndex();

    private static final int IS_RESPONSE = indexOf("is_response");
    private static final int FC_1 = indexOf("fc_1");
    private static final int FC_2 = indexOf("fc_2");
    private static final int FC_3 = indexOf("fc_3");
    private static final int FC_4 = indexOf("fc_4");
    private static final int FC_5 = indexOf("fc_5");
    private static final int FC_6 = indexOf("fc_6");
    private static final int FC_OTHER = indexOf("fc_other");
    private static final int ADDRESS_VALUE = indexOf("address_value");
    private static final int ADDRESS_PRESENT = indexOf("address_present");
    private static final int QUANTITY_VALUE = indexOf("quantity_value");
    private static final int QUANTITY_PRESENT = indexOf("quantity_present");
    private static final int RESPONSE_MATCHED = indexOf("response_matched");
    private static final int REQUEST_VALUES_PRESENT = indexOf("request_values_present");
    private static final int REQUEST_VALUE_COUNT = indexOf("request_value_count");
    private static final int REQUEST_VALUE_MIN = indexOf("request_value_min");
    private static final int REQUEST_VALUE_MAX = indexOf("request_value_max");
    private static final int REQUEST_VALUE_MEAN = indexOf("request_value_mean");
    private static final int RESPONSE_VALUES_PRESENT = indexOf("response_values_present");
    private static final int RESPONSE_VALUE_COUNT = indexOf("response_value_count");
    private static final int RESPONSE_VALUE_MIN = indexOf("response_value_min");
    private static final int RESPONSE_VALUE_MAX = indexOf("response_value_max");
    private static final int RESPONSE_VALUE_MEAN = indexOf("response_value_mean");
    private static final int PREV_EVENT_AVAILABLE = indexOf("prev_event_available");
    private static final int INTER_ARRIVAL_S = indexOf("inter_arrival_s");
    private static final int FUNCTION_CHANGED = indexOf("function_changed");
    private static final int ADDRESS_DELTA_VALID = indexOf("address_delta_valid");
    private static final int ADDRESS_DELTA = indexOf("address_delta");
    private static final int QUANTITY_DELTA_VALID = indexOf("quantity_delta_valid");
    private static final int QUANTITY_DELTA = indexOf("quantity_delta");
    private static final int OUTSTANDING_REQUESTS_BEFORE_EVENT = indexOf("outstanding_requests_before_event");
    private static final int RESPONSE_WITHOUT_REQUEST = indexOf("response_without_request");
    private static final int REQUEST_OVERWRITE_SAME_TID = indexOf("request_overwrite_same_tid");
    private static final int RTT_VALID = indexOf("rtt_valid");
    private static final int RTT_S = indexOf("rtt_s");
    private static final int EVENT_RATE_1S = indexOf("event_rate_1s");
    private static final int EVENT_RATE_10S = indexOf("event_rate_10s");
    private static final int EVENT_RATE_60S = indexOf("event_rate_60s");
    private static final int UNIQUE_FUNCTION_COUNT_10S = indexOf("unique_function_count_10s");
    private static final int UNIQUE_ADDRESS_COUNT_10S = indexOf("unique_address_count_10s");
    private static final int READ_RATIO_10S = indexOf("read_ratio_10s");
    private static final int WRITE_RATIO_10S = indexOf("write_ratio_10s");

    private static Map<String, Integer> buildIndex() {
        Map<String, Integer> map = new HashMap<>();
        for (FeatureDefinition definition : ModbusFeatureSchemaV1.SCHEMA.definitions()) {
            map.put(definition.name(), definition.index());
        }
        return map;
    }

    private static int indexOf(String name) {
        Integer index = INDEX.get(name);
        if (index == null) {
            throw new IllegalStateException(
                "modbus-feature-v1 schema has no feature named \"" + name + "\"");
        }
        return index;
    }

    public float[] extract(ModbusEvent event, ModbusEntityState.BeforeEvent before, ModbusEntityState after,
                            boolean newSegment) {
        // newSegment implies the caller already reset the state before
        // advancing it, so `before` is all-empty -- see this class's
        // javadoc. If that ever stops holding, indices 23-34 would silently
        // compute non-zero, wrong values with no signal anywhere: a Java
        // `assert` would not do it, since neither bootstrap module runs its
        // JVM with -ea, so an `assert` here would be inert in the one place
        // (a real Flink job) a caller bug would actually matter. An explicit
        // throw is a runtime check that fires the same way in tests and in
        // production, and matches how every other invariant in this codebase
        // is enforced (throw new Illegal...Exception, never `assert`).
        if (newSegment && !(before.lastTs() == null && before.lastAddress() == null
            && before.lastQuantity() == null && before.outstandingRequests() == 0)) {
            throw new IllegalArgumentException(
                "newSegment=true requires the caller to have reset the state before advancing it; "
                + "a non-empty before-event snapshot here would silently break the "
                + "zeroed-at-segment-start guarantee for indices 23-34.");
        }

        float[] vector = new float[ModbusFeatureSchemaV1.SCHEMA.featureCount()];

        // event.tsSeconds() -- the wire ts exactly as the mapper bound it,
        // never re-derived from envelope().eventTime() -- is the causal
        // engine's clock. See ModbusEvent's own javadoc for why the two are
        // kept deliberately separate: envelope().eventTime() is
        // millisecond-rounded (for the event id and the ClickHouse column
        // only), and the upstream engine's `ts` (07b:259,303) is this exact
        // float64, unrounded, so reading it here is what makes this class's
        // float32 output bit-identical to 07b's.
        double ts = event.tsSeconds();
        boolean isResponse = event.direction() == ModbusDirection.RESPONSE;
        int functionCode = event.functionCode();

        // Group A: current-event Modbus semantics (process_capture lines
        // 344-379). Exactly one of fc_1..fc_6/fc_other is ever 1, whatever
        // the wire code -- an unenumerated code (including 23, deliberately:
        // see ModbusFunctionCode's own javadoc for why 23 is NOT one-hot
        // here) folds into fc_other, never left all-zero.
        vector[IS_RESPONSE] = isResponse ? 1f : 0f;
        boolean enumeratedFunctionCode = functionCode >= 1 && functionCode <= 6;
        vector[FC_1] = functionCode == 1 ? 1f : 0f;
        vector[FC_2] = functionCode == 2 ? 1f : 0f;
        vector[FC_3] = functionCode == 3 ? 1f : 0f;
        vector[FC_4] = functionCode == 4 ? 1f : 0f;
        vector[FC_5] = functionCode == 5 ? 1f : 0f;
        vector[FC_6] = functionCode == 6 ? 1f : 0f;
        vector[FC_OTHER] = enumeratedFunctionCode ? 0f : 1f;

        Double address = event.address();
        boolean addressPresent = address != null;
        vector[ADDRESS_PRESENT] = addressPresent ? 1f : 0f;
        vector[ADDRESS_VALUE] = addressPresent ? address.floatValue() : 0f;

        Double quantity = event.quantity();
        boolean quantityPresent = quantity != null;
        vector[QUANTITY_PRESENT] = quantityPresent ? 1f : 0f;
        vector[QUANTITY_VALUE] = quantityPresent ? quantity.floatValue() : 0f;

        // response_matched reads the record's own `matched` field (already
        // resolved by the upstream mapper), never recomputed causally here;
        // a request is unconditionally 0, matching process_capture's own
        // `d == "response" and truthy(matched_raw[i])`.
        vector[RESPONSE_MATCHED] = (isResponse && event.matched()) ? 1f : 0f;

        // Group B: numeric value summaries (process_capture lines 381-403).
        // The five request/response summary features are written ONLY under
        // `if req_values:` / `if rsp_values:` -- an empty array leaves every
        // one of the five, including *_values_present, at the Java float[]
        // default of 0. That default is exactly "absent", never "present,
        // count 0".
        double[] requestValues = event.requestValues();
        if (requestValues.length > 0) {
            double min = requestValues[0];
            double max = requestValues[0];
            double sum = 0.0;
            for (double value : requestValues) {
                min = Math.min(min, value);
                max = Math.max(max, value);
                sum += value;
            }
            vector[REQUEST_VALUES_PRESENT] = 1f;
            vector[REQUEST_VALUE_COUNT] = requestValues.length;
            vector[REQUEST_VALUE_MIN] = (float) min;
            vector[REQUEST_VALUE_MAX] = (float) max;
            vector[REQUEST_VALUE_MEAN] = (float) (sum / requestValues.length);
        }

        double[] responseValues = event.responseValues();
        if (responseValues.length > 0) {
            double min = responseValues[0];
            double max = responseValues[0];
            double sum = 0.0;
            for (double value : responseValues) {
                min = Math.min(min, value);
                max = Math.max(max, value);
                sum += value;
            }
            vector[RESPONSE_VALUES_PRESENT] = 1f;
            vector[RESPONSE_VALUE_COUNT] = responseValues.length;
            vector[RESPONSE_VALUE_MIN] = (float) min;
            vector[RESPONSE_VALUE_MAX] = (float) max;
            vector[RESPONSE_VALUE_MEAN] = (float) (sum / responseValues.length);
        }

        // Group C: causal state read from `before` (process_capture lines
        // 405-455), strictly before this event's own mutation of pending /
        // prev_fc / last_ts / last_address / last_quantity.
        Double beforeLastTs = before.lastTs();
        boolean prevAvailable = beforeLastTs != null;
        vector[PREV_EVENT_AVAILABLE] = prevAvailable ? 1f : 0f;
        if (prevAvailable) {
            vector[INTER_ARRIVAL_S] = (float) (ts - beforeLastTs);
            // prevFunctionCode is always non-null here: ModbusEntityState.advance
            // sets prevFunctionCode and lastTs together, unconditionally, on every
            // call, so lastTs != null implies prevFunctionCode != null too.
            int prevFunctionCode = before.prevFunctionCode();
            vector[FUNCTION_CHANGED] = functionCode != prevFunctionCode ? 1f : 0f;
        }

        Double lastAddress = before.lastAddress();
        if (addressPresent && lastAddress != null) {
            vector[ADDRESS_DELTA_VALID] = 1f;
            vector[ADDRESS_DELTA] = (float) (address - lastAddress);
        }

        Double lastQuantity = before.lastQuantity();
        if (quantityPresent && lastQuantity != null) {
            vector[QUANTITY_DELTA_VALID] = 1f;
            vector[QUANTITY_DELTA] = (float) (quantity - lastQuantity);
        }

        vector[OUTSTANDING_REQUESTS_BEFORE_EVENT] = before.outstandingRequests();

        // pendingTsForTid is this event's own tid's pending request timestamp
        // (advance was given event.transactionId()), or null if none was
        // pending.
        boolean hasPending = before.tidWasPending();
        if (isResponse) {
            vector[RESPONSE_WITHOUT_REQUEST] = hasPending ? 0f : 1f;
            if (hasPending) {
                vector[RTT_VALID] = 1f;
                vector[RTT_S] = (float) (ts - before.pendingTsForTid());
            }
        } else {
            vector[REQUEST_OVERWRITE_SAME_TID] = hasPending ? 1f : 0f;
        }

        // Group D: trailing-window rates/ratios read from `after`
        // (process_capture lines 457-523) -- the current event is already
        // purged-and-appended into every window, and counted in the running
        // 10 s counts, by the time advance returned, so it is always counted
        // in its own windows. Each read is a stored size or count, O(1).
        vector[EVENT_RATE_1S] = after.eventCount1s();
        vector[EVENT_RATE_10S] = (float) (after.eventCount10s() / 10.0);
        vector[EVENT_RATE_60S] = (float) (after.eventCount60s() / 60.0);
        vector[UNIQUE_FUNCTION_COUNT_10S] = after.uniqueFunctions10s();
        vector[UNIQUE_ADDRESS_COUNT_10S] = after.uniqueAddresses10s();

        // window10Count can never be 0 here: `after` always has this event's
        // own entry inside its 10-second window (an entry timestamped `ts`
        // is never purged by a cutoff of `ts - 10`), so this matches
        // process_capture's own unguarded `float(state.read_count_10) /
        // window10_count` division exactly -- no defensive zero-check either
        // side of this port.
        int window10Count = after.eventCount10s();
        vector[READ_RATIO_10S] = (float) (after.readCount10s() / (double) window10Count);
        vector[WRITE_RATIO_10S] = (float) (after.writeCount10s() / (double) window10Count);

        return vector;
    }
}
