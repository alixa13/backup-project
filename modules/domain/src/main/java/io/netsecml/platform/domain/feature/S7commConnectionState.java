package io.netsecml.platform.domain.feature;

import java.util.BitSet;

// The S7comm causal state for one (sensor, uid) connection: exactly what the
// 16 frozen s7comm-feature-v1 features read, and nothing else. A literal port
// of the frozen feature path of the upstream builders --
// CustomerICSNPPEnrichedFeatureBuilder.process_event and the V4 wrapper
// CustomerICSNPPTimeNormalizedFeatureBuilder.process_event in
// two-models-info/S7___/ -- which are the authority: where this class and
// they disagree, the Python wins. Their candidate features outside the frozen
// 16 (rates, latencies, errors, subfunctions) are not ported.
//
// Mutable and updated in place, like ModbusEntityState: advance() applies one
// event and every accessor then reads the state AFTER that event, as upstream
// computes every frozen feature after its own updates. Bounded by
// construction: nine fixed rings of 16 or 32 entries, and an outstanding set
// that can never exceed the 16-bit PDU reference space (65,536 bits, 8 KB)
// and grows only to the highest reference seen. No cap and no saturation flag
// are needed, and each event costs O(1).
//
// Kryo, not the POJO serializer, holds this in Flink (no public no-arg
// constructor, no bean accessors), as for ModbusEntityState: its field layout
// is free to change only until the first savepoint exists.
public final class S7commConnectionState {

    // Upstream's history_events and pdu_history.
    static final int HISTORY = 16;
    static final int PDU_HISTORY = 32;

    // upstream's `outstanding` dict of request PDU references awaiting a
    // response. The frozen features read only its membership and its size,
    // never the (ts, function) upstream stores per entry, so a set suffices.
    private final BitSet outstanding = new BitSet();
    private int outstandingCount;

    // The nine histories, each named for the upstream deque it ports.
    private final LongRing responseMatches = new LongRing(HISTORY);      // response_match_history
    private final LongRing requests = new LongRing(HISTORY);             // direction_history (enriched)
    private final LongRing outstandingCounts = new LongRing(HISTORY);    // outstanding_history
    private final LongRing requestFunctions = new LongRing(HISTORY);     // request_function_history
    private final LongRing functionChanges = new LongRing(HISTORY);      // function_change_history
    private final LongRing functionTransitions = new LongRing(HISTORY);  // function_transition_history (V4)
    private final LongRing rosctrChanges = new LongRing(HISTORY);        // rosctr_change_history
    private final LongRing directionChanges = new LongRing(HISTORY);     // direction_history (V4)
    private final LongRing requestPduReferences = new LongRing(PDU_HISTORY); // request_pdu_recent

    // Scalars: the previous request function (both builders keep one; they
    // are updated under the same condition, so one field serves both), the
    // runs, the previous direction and ROSCTR, and the last timestamp (for
    // the out-of-order flag only). The runs are long because upstream's are
    // unbounded Python ints: a connection polled with one function for months
    // would wrap an int negative.
    private Integer lastRequestFunction;
    private long sameFunctionRun;
    private Boolean previousWasRequest;
    private long sameDirectionRun;
    private Integer lastRosctr;
    private Double lastTs;

    private S7commConnectionState() {
    }

    // The state a new connection starts from.
    public static S7commConnectionState empty() {
        return new S7commConnectionState();
    }

    // What one event changed that the accessors cannot read back afterwards:
    // s7_function_changed is a per-event flag, and whether this record arrived
    // with a timestamp earlier than the connection's last.
    public record Step(boolean functionChanged, boolean outOfOrder) {
    }

    // Applies one event, in upstream's order. The caller has already decided
    // isRequest (destination port 102) and validated pduReference to 0-65535.
    public Step advance(double ts, boolean isRequest, int pduReference, Integer rosctrCode, Integer functionCode) {
        // This platform's addition: an earlier timestamp than the connection's
        // last is flagged. Nothing resets -- no frozen feature reads time.
        boolean outOfOrder = lastTs != null && ts < lastTs;
        lastTs = ts;

        // PDU matching (enriched builder): a request enters the outstanding
        // set; a response leaves it, and every response records whether its
        // reference was outstanding.
        if (isRequest) {
            if (!outstanding.get(pduReference)) {
                outstanding.set(pduReference);
                outstandingCount++;
            }
        } else {
            boolean matched = outstanding.get(pduReference);
            if (matched) {
                outstanding.clear(pduReference);
                outstandingCount--;
            }
            responseMatches.add(matched ? 1 : 0);
        }

        // Request function (enriched builder), requests carrying a code only:
        // the run, the change flag and the code history. The V4 wrapper's
        // transition history is updated under the same condition, from the
        // same previous function, so it is recorded here too.
        boolean functionChanged = false;
        if (isRequest && functionCode != null) {
            if (lastRequestFunction == null) {
                sameFunctionRun = 1;
            } else if (functionCode.equals(lastRequestFunction)) {
                sameFunctionRun++;
            } else {
                functionChanged = true;
                sameFunctionRun = 1;
            }
            if (lastRequestFunction != null) {
                // A function code is below 2^24, so (previous, current) packs
                // into one long without collisions.
                functionTransitions.add(((long) lastRequestFunction << 24) | functionCode);
            }
            lastRequestFunction = functionCode;
            requestFunctions.add(functionCode);
            functionChanges.add(functionChanged ? 1 : 0);
        }

        // ROSCTR (enriched builder), events carrying a ROSCTR code only.
        if (rosctrCode != null) {
            boolean changed = lastRosctr != null && !rosctrCode.equals(lastRosctr);
            rosctrChanges.add(changed ? 1 : 0);
            lastRosctr = rosctrCode;
        }

        // Direction and outstanding histories (enriched builder): every event,
        // the outstanding count as it stands after the matching above.
        requests.add(isRequest ? 1 : 0);
        outstandingCounts.add(outstandingCount);

        // PDU uniqueness (enriched builder): request references, masked to 16 bits.
        if (isRequest) {
            requestPduReferences.add(pduReference & 0xFFFF);
        }

        // Direction run (V4 wrapper): the first event starts a run of 1 and
        // records "no change".
        boolean directionChanged;
        if (previousWasRequest == null) {
            sameDirectionRun = 1;
            directionChanged = false;
        } else if (previousWasRequest == isRequest) {
            sameDirectionRun++;
            directionChanged = false;
        } else {
            sameDirectionRun = 1;
            directionChanged = true;
        }
        directionChanges.add(directionChanged ? 1 : 0);
        previousWasRequest = isRequest;

        return new Step(functionChanged, outOfOrder);
    }

    // s7_outstanding_requests: len(outstanding) after this event.
    public int outstandingRequests() {
        return outstandingCount;
    }

    // s7_outstanding_mean_16: _mean(outstanding_history).
    public double outstandingMean16() {
        return outstandingCounts.meanOrZero();
    }

    // s7_response_match_rate_16: _ratio_true(response_match_history).
    public double responseMatchRate16() {
        return responseMatches.meanOrZero();
    }

    // s7_same_function_run_length.
    public long sameFunctionRunLength() {
        return sameFunctionRun;
    }

    // s7_same_direction_run_length.
    public long sameDirectionRunLength() {
        return sameDirectionRun;
    }

    // s7_request_ratio_16: _ratio_true(direction_history).
    public double requestRatio16() {
        return requests.meanOrZero();
    }

    // s7_direction_change_rate_16: sum / len of the V4 wrapper's change flags.
    public double directionChangeRate16() {
        return directionChanges.meanOrZero();
    }

    // s7_function_change_rate_16: _ratio_true(function_change_history).
    public double functionChangeRate16() {
        return functionChanges.meanOrZero();
    }

    // s7_function_entropy_16: _normalized_entropy(request_function_history, 16).
    public double functionEntropy16() {
        return requestFunctions.normalizedEntropy(HISTORY);
    }

    // s7_function_transition_entropy_16: _normalized_entropy(transitions, 16).
    public double functionTransitionEntropy16() {
        return functionTransitions.normalizedEntropy(HISTORY);
    }

    // s7_rosctr_change_rate_16: _ratio_true(rosctr_change_history).
    public double rosctrChangeRate16() {
        return rosctrChanges.meanOrZero();
    }

    // s7_pdu_reference_unique_ratio_32: _unique_ratio(request_pdu_recent).
    public double pduReferenceUniqueRatio32() {
        return requestPduReferences.distinctRatioOrZero();
    }

    // The outstanding set's allocated bits, for the test that a one-packet
    // connection does not cost a full 65,536-bit set.
    int outstandingBitCapacity() {
        return outstanding.size();
    }
}
