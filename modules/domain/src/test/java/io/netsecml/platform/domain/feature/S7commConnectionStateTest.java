package io.netsecml.platform.domain.feature;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

// Pins each of the 16 features' state rules against upstream's builders
// (two-models-info/S7___/customer_icsnpp_enriched_builder.py and
// customer_icsnpp_time_normalized_builder.py), one behaviour per test. The
// Task 4 oracle proves the composition; these name the rule a regression broke.
class S7commConnectionStateTest {

    private static final boolean REQUEST = true;
    private static final boolean RESPONSE = false;

    private static S7commConnectionState.Step request(S7commConnectionState state, int pdu, Integer function) {
        return state.advance(1000.0, REQUEST, pdu, 1, function);
    }

    private static S7commConnectionState.Step response(S7commConnectionState state, int pdu) {
        return state.advance(1000.0, RESPONSE, pdu, 3, 4);
    }

    @Test
    void aRequestCountsItselfAsOutstanding() {
        // Read AFTER the event, unlike modbus's outstanding_requests_before_event.
        S7commConnectionState state = S7commConnectionState.empty();
        request(state, 7, 4);
        assertEquals(1, state.outstandingRequests());
    }

    @Test
    void aMatchedResponseClearsItsReferenceAndCountsAsMatched() {
        S7commConnectionState state = S7commConnectionState.empty();
        request(state, 7, 4);
        response(state, 7);
        assertEquals(0, state.outstandingRequests());
        assertEquals(1.0, state.responseMatchRate16());
    }

    @Test
    void anUnmatchedResponseCountsAsUnmatched() {
        S7commConnectionState state = S7commConnectionState.empty();
        response(state, 9);
        assertEquals(0.0, state.responseMatchRate16());
        assertEquals(0, state.outstandingRequests());
    }

    @Test
    void aReusedOutstandingReferenceStaysOneEntry() {
        // upstream: outstanding[pdu] = ... -- a dict assignment, not an append.
        S7commConnectionState state = S7commConnectionState.empty();
        request(state, 7, 4);
        request(state, 7, 4);
        assertEquals(1, state.outstandingRequests());
    }

    @Test
    void aPduReferenceWrapIsANewReference() {
        // 65535 then 0: two different references to the set and to uniqueness.
        S7commConnectionState state = S7commConnectionState.empty();
        request(state, 0xFFFF, 4);
        request(state, 0, 4);
        assertEquals(2, state.outstandingRequests());
        assertEquals(1.0, state.pduReferenceUniqueRatio32());
        response(state, 0);
        assertEquals(1, state.outstandingRequests(), "only 0 was answered");
    }

    @Test
    void aOneEventConnectionHoldsAtMostOneWordOfOutstandingBits() {
        // A port scan opens thousands of one-packet connections: a fresh key
        // must not carry a full 65,536-bit set.
        S7commConnectionState state = S7commConnectionState.empty();
        request(state, 7, 4);
        assertTrue(state.outstandingBitCapacity() <= 64, "was " + state.outstandingBitCapacity());
    }

    @Test
    void outstandingMeanAveragesTheLastSixteenCounts() {
        // 17 unanswered requests: counts 1..17; the last 16 are 2..17, mean 9.5.
        S7commConnectionState state = S7commConnectionState.empty();
        for (int pdu = 0; pdu < 17; pdu++) {
            request(state, pdu, 4);
        }
        assertEquals(9.5, state.outstandingMean16());
    }

    @Test
    void theFunctionRunCountsRepeatsAndRestartsOnAChange() {
        S7commConnectionState state = S7commConnectionState.empty();
        assertFalse(request(state, 1, 4).functionChanged(), "the first request function is not a change");
        request(state, 2, 4);
        assertEquals(2, state.sameFunctionRunLength());
        assertTrue(request(state, 3, 5).functionChanged());
        assertEquals(1, state.sameFunctionRunLength());
        assertEquals(1.0 / 3.0, state.functionChangeRate16(), "flags 0, 0, 1");
    }

    @Test
    void responsesAndCodeLessRequestsLeaveTheFunctionRunAlone() {
        S7commConnectionState state = S7commConnectionState.empty();
        request(state, 1, 4);
        request(state, 2, 4);
        response(state, 1);
        request(state, 3, null);
        assertEquals(2, state.sameFunctionRunLength());
        assertEquals(0.0, state.functionChangeRate16(), "only the two coded requests added flags");
    }

    @Test
    void theRunLengthsDoNotOverflowOnALongLivedConnection() throws Exception {
        // A PLC connection polled with one function lives for months: at 100
        // requests/s an int run would wrap negative after about 248 days, where
        // upstream's Python ints never do. Seed both runs at Integer.MAX_VALUE
        // (two billion events are too many to replay) and take one more step.
        S7commConnectionState state = S7commConnectionState.empty();
        request(state, 1, 4);
        for (String run : new String[] {"sameFunctionRun", "sameDirectionRun"}) {
            java.lang.reflect.Field field = S7commConnectionState.class.getDeclaredField(run);
            field.setAccessible(true);
            field.set(state, Integer.MAX_VALUE);
        }
        request(state, 2, 4);
        assertEquals(Integer.MAX_VALUE + 1L, state.sameFunctionRunLength());
        assertEquals(Integer.MAX_VALUE + 1L, state.sameDirectionRunLength());
    }

    @Test
    void theFunctionRunIsZeroBeforeAnyRequestFunction() {
        S7commConnectionState state = S7commConnectionState.empty();
        response(state, 1);
        assertEquals(0, state.sameFunctionRunLength());
    }

    @Test
    void theDirectionRunAndChangeRateCountTheFirstEventAsNoChange() {
        // V4 wrapper: the first event appends changed=0.
        S7commConnectionState state = S7commConnectionState.empty();
        request(state, 1, 4);
        response(state, 1);
        response(state, 2);
        assertEquals(2, state.sameDirectionRunLength());
        assertEquals(1.0 / 3.0, state.directionChangeRate16(), "flags 0, 1, 0");
    }

    @Test
    void theRequestRatioCoversOnlyTheLastSixteenEvents() {
        S7commConnectionState state = S7commConnectionState.empty();
        for (int pdu = 0; pdu < 16; pdu++) {
            request(state, pdu, 4);
        }
        for (int pdu = 0; pdu < 4; pdu++) {
            response(state, pdu);
        }
        assertEquals(0.75, state.requestRatio16(), "12 requests and 4 responses in the last 16");
    }

    @Test
    void functionEntropyIsOverRequestFunctionCodes() {
        S7commConnectionState state = S7commConnectionState.empty();
        request(state, 1, 4);
        assertEquals(0.0, state.functionEntropy16(), "one value");
        request(state, 2, 5);
        assertEquals(1.0, state.functionEntropy16(), "two equally likely codes over log2(2)");
        response(state, 1);
        assertEquals(1.0, state.functionEntropy16(), "a response adds no code");
    }

    @Test
    void transitionEntropyIsOverConsecutiveRequestFunctionPairs() {
        S7commConnectionState state = S7commConnectionState.empty();
        request(state, 1, 4);
        request(state, 2, 4);
        request(state, 3, 4);
        assertEquals(-0.0, state.functionTransitionEntropy16(), "(4,4) twice: one distinct pair, -0.0 as upstream");
        request(state, 4, 5);
        request(state, 5, 4);
        // transitions (4,4), (4,4), (4,5), (5,4): counts 2, 1, 1 over n = 4.
        double expected = -(0.5 * log2(0.5) + 0.25 * log2(0.25) + 0.25 * log2(0.25)) / log2(4);
        assertEquals(expected, state.functionTransitionEntropy16());
    }

    @Test
    void theRosctrChangeRateSkipsEventsWithoutARosctr() {
        S7commConnectionState state = S7commConnectionState.empty();
        state.advance(1000.0, REQUEST, 1, 1, 4);
        state.advance(1000.0, RESPONSE, 1, 3, 4);
        state.advance(1000.0, REQUEST, 2, null, 4);
        state.advance(1000.0, REQUEST, 3, 1, 4);
        assertEquals(2.0 / 3.0, state.rosctrChangeRate16(), "flags 0, 1, 1; the ROSCTR-less event adds none");
    }

    @Test
    void pduUniquenessCoversTheLastThirtyTwoRequestReferences() {
        S7commConnectionState state = S7commConnectionState.empty();
        request(state, 7, 4);
        request(state, 7, 4);
        assertEquals(0.5, state.pduReferenceUniqueRatio32());
        for (int pdu = 100; pdu < 132; pdu++) {
            request(state, pdu, 4);
        }
        assertEquals(1.0, state.pduReferenceUniqueRatio32(), "the two 7s have left the window of 32");
    }

    @Test
    void anEarlierTimestampIsFlaggedButStillProcessed() {
        S7commConnectionState state = S7commConnectionState.empty();
        assertFalse(state.advance(1000.0, REQUEST, 1, 1, 4).outOfOrder(), "a key's first event is never out of order");
        assertTrue(state.advance(999.5, REQUEST, 2, 1, 4).outOfOrder());
        assertEquals(2, state.outstandingRequests(), "no feature reads time, so nothing is reset");
        assertFalse(state.advance(999.5, REQUEST, 3, 1, 4).outOfOrder(), "an equal timestamp is in order");
    }

    private static double log2(double x) {
        return Math.log(x) / Math.log(2.0);
    }
}
