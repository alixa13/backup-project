package io.netsecml.platform.domain.feature;

import io.netsecml.platform.domain.event.ModbusEvent.ModbusDirection;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

// Pins the modbus causal state machine against the upstream offline engine
// (two-models-info/modbus_/07b_materialize_feature_engine_v1.py's EntityState
// dataclass and process_capture's window/state handling), which is the
// authority this class must match bit-for-bit: 19 of the 42 frozen modbus
// features read this state, and a semantic drift here is invisible to the
// compiler. The state is mutable: advance(...) changes it in place and
// returns the before-event snapshot.
class ModbusEntityStateTest {

    // A state that has seen exactly the one given request.
    private static ModbusEntityState afterOneRequest(double ts, Double address, Double quantity) {
        ModbusEntityState state = ModbusEntityState.empty();
        state.advance(ts, 3, "17", ModbusDirection.REQUEST, address, quantity);
        return state;
    }

    @Test
    void aGapLongerThanFifteenSecondsStartsANewSegment() {
        ModbusEntityState state = afterOneRequest(1000.0, 40001.0, 2.0);
        assertTrue(state.startsNewSegment(1015.01));
        assertFalse(state.startsNewSegment(1015.0), "exactly 15s is still the same segment");
    }

    @Test
    void aNegativeGapAlsoStartsANewSegment() {
        // The upstream engine raises here, because offline it has already asserted a
        // strictly increasing event index. A streaming operator must not fail the job
        // on one out-of-order record, and resetting keeps in-segment inter-arrival
        // within [0, 15] by construction.
        ModbusEntityState state = afterOneRequest(1000.0, 40001.0, 2.0);
        assertTrue(state.startsNewSegment(999.9));
    }

    @Test
    void anEmptyStateAlwaysStartsANewSegment() {
        assertTrue(ModbusEntityState.empty().startsNewSegment(1000.0));
    }

    @Test
    void aRequestDoesNotCountItselfAsOutstanding() {
        // The contract's transaction_rule: compute before-event features, mutate
        // pending state afterwards. outstanding_requests_before_event is named for it.
        ModbusEntityState state = ModbusEntityState.empty();
        ModbusEntityState.BeforeEvent before = state.advance(1000.0, 3, "17", ModbusDirection.REQUEST, null, null);
        assertEquals(0, before.outstandingRequests());
        assertEquals(1, state.outstandingRequests());
    }

    @Test
    void advanceReturnsEveryValueAsItStoodBeforeTheEvent() {
        ModbusEntityState state = afterOneRequest(1000.0, 40001.0, 2.0);
        ModbusEntityState.BeforeEvent before =
            state.advance(1000.5, 6, "17", ModbusDirection.RESPONSE, 40005.0, 4.0);
        assertEquals(new ModbusEntityState.BeforeEvent(1000.0, 3, 40001.0, 2.0, 1, 1000.0), before);
        assertTrue(before.tidWasPending());

        // ...while the state itself has moved on.
        assertEquals(1000.5, state.lastTs());
        assertEquals(6, state.prevFunctionCode());
        assertEquals(40005.0, state.lastAddress());
        assertEquals(4.0, state.lastQuantity());
        assertEquals(0, state.outstandingRequests());
    }

    @Test
    void aTidThatWasNotPendingSnapshotsAsAbsent() {
        ModbusEntityState state = afterOneRequest(1000.0, null, null);
        ModbusEntityState.BeforeEvent before = state.advance(1000.5, 3, "99", ModbusDirection.RESPONSE, null, null);
        assertNull(before.pendingTsForTid());
        assertFalse(before.tidWasPending());
    }

    @Test
    void aMatchedResponseClearsItsPendingTid() {
        ModbusEntityState state = afterOneRequest(1000.0, null, null);
        state.advance(1000.5, 3, "17", ModbusDirection.RESPONSE, null, null);
        assertEquals(0, state.outstandingRequests());
        assertFalse(state.hasPending("17"));
    }

    @Test
    void theOneSecondWindowExcludesAnEventExactlyOneSecondOld() {
        // Half-open (t-w, t]: the upstream purge is `stored <= ts - w`.
        ModbusEntityState state = afterOneRequest(1000.0, null, null);
        state.advance(1000.5, 3, "18", ModbusDirection.REQUEST, null, null);
        assertEquals(2, state.eventCount1s());
        state.advance(1001.0, 3, "19", ModbusDirection.REQUEST, null, null);
        assertEquals(2, state.eventCount1s(), "the event at 1000.0 is exactly 1s old and has left");
    }

    @Test
    void theTenSecondWindowExcludesAnEventExactlyTenSecondsOld() {
        ModbusEntityState justInside = afterOneRequest(1000.0, null, null);
        justInside.advance(1009.99, 3, "18", ModbusDirection.REQUEST, null, null);
        assertEquals(2, justInside.eventCount10s());

        ModbusEntityState exactlyOnTheEdge = afterOneRequest(1000.0, null, null);
        exactlyOnTheEdge.advance(1010.0, 3, "18", ModbusDirection.REQUEST, null, null);
        assertEquals(1, exactlyOnTheEdge.eventCount10s());
    }

    @Test
    void theSixtySecondWindowExcludesAnEventExactlySixtySecondsOld() {
        // Four 15 s gaps stay inside one segment (only a gap OVER 15 s starts a new
        // one) and reach exactly 60 s back.
        ModbusEntityState state = afterOneRequest(1000.0, null, null);
        for (double ts : new double[] {1015.0, 1030.0, 1045.0}) {
            state.advance(ts, 3, "18", ModbusDirection.REQUEST, null, null);
        }
        assertEquals(4, state.eventCount60s());
        state.advance(1060.0, 3, "18", ModbusDirection.REQUEST, null, null);
        assertEquals(4, state.eventCount60s(), "the event at 1000.0 is exactly 60s old and has left");
    }

    @Test
    void purgingTheTenSecondWindowDecrementsItsFunctionAndAddressCounters() {
        ModbusEntityState state = afterOneRequest(1000.0, 40001.0, null);
        state.advance(1001.0, 6, "18", ModbusDirection.REQUEST, 40002.0, null);
        assertEquals(2, state.uniqueFunctions10s());
        assertEquals(2, state.uniqueAddresses10s());
        assertEquals(1, state.readCount10s());
        assertEquals(1, state.writeCount10s());

        // At 1010.5 the FC-3 read at address 40001 (ts 1000.0) leaves the window,
        // taking its function, its address and its read with it.
        state.advance(1010.5, 6, "19", ModbusDirection.REQUEST, 40002.0, null);
        assertEquals(1, state.uniqueFunctions10s(), "the FC-3 event has aged out");
        assertEquals(1, state.uniqueAddresses10s());
        assertEquals(0, state.readCount10s());
        assertEquals(2, state.writeCount10s());
    }

    @Test
    void aValueSeenTwiceStaysCountedUntilItsLastOccurrenceLeaves() {
        // fc_counter_10 counts occurrences, not presence: purging ONE of two FC-3
        // events must not drop FC 3 from unique_function_count_10s.
        ModbusEntityState state = afterOneRequest(1000.0, 40001.0, null);
        state.advance(1005.0, 3, "18", ModbusDirection.REQUEST, 40001.0, null);
        state.advance(1010.0, 6, "19", ModbusDirection.REQUEST, null, null);
        assertEquals(2, state.uniqueFunctions10s(), "FC 3 is still in the window at 1005.0");
        assertEquals(1, state.uniqueAddresses10s(), "40001 is still in the window at 1005.0");
    }

    @Test
    void functionCode23CountsInBothTheReadAndWriteTallies() {
        ModbusEntityState state = ModbusEntityState.empty();
        state.advance(1000.0, 23, "17", ModbusDirection.REQUEST, null, null);
        assertEquals(1, state.readCount10s());
        assertEquals(1, state.writeCount10s());
    }

    @Test
    void aFunctionCodeThatIsNeitherReadNorWriteCountsInNeitherTally() {
        ModbusEntityState state = ModbusEntityState.empty();
        state.advance(1000.0, 43, "17", ModbusDirection.REQUEST, null, null);
        assertEquals(0, state.readCount10s());
        assertEquals(0, state.writeCount10s());
        assertEquals(1, state.uniqueFunctions10s());
    }

    @Test
    void anAbsentAddressIsNotCountedAsAUniqueAddress() {
        ModbusEntityState state = afterOneRequest(1000.0, null, null);
        assertEquals(0, state.uniqueAddresses10s());
    }

    @Test
    void resetClearsEverythingInPlace() {
        ModbusEntityState state = afterOneRequest(1000.0, 40001.0, 2.0);
        state.advance(1000.5, 16, "18", ModbusDirection.REQUEST, 40002.0, 3.0);
        state.reset();

        assertNull(state.lastTs());
        assertNull(state.prevFunctionCode());
        assertNull(state.lastAddress());
        assertNull(state.lastQuantity());
        assertEquals(0, state.outstandingRequests());
        assertEquals(0, state.eventCount1s());
        assertEquals(0, state.eventCount10s());
        assertEquals(0, state.eventCount60s());
        assertEquals(0, state.uniqueFunctions10s());
        assertEquals(0, state.uniqueAddresses10s());
        assertEquals(0, state.readCount10s());
        assertEquals(0, state.writeCount10s());

        // The next event sees an all-empty before-state, and only itself in the windows.
        ModbusEntityState.BeforeEvent before = state.advance(1000.6, 3, "18", ModbusDirection.REQUEST, 7.0, null);
        assertEquals(new ModbusEntityState.BeforeEvent(null, null, null, null, 0, null), before);
        assertEquals(1, state.eventCount60s());
        assertEquals(1, state.uniqueAddresses10s());
    }

    @Test
    void theLastAddressSurvivesAnEventWithNoAddress() {
        // "previous APPLICABLE address" -- the delta reaches back past events that
        // carried none, which is why lastAddress is only updated when one is present.
        ModbusEntityState state = afterOneRequest(1000.0, 40001.0, null);
        state.advance(1001.0, 3, "18", ModbusDirection.REQUEST, null, null);
        assertEquals(40001.0, state.lastAddress());
    }

    @Test
    void theLastQuantitySurvivesAnEventWithNoQuantity() {
        // Same rule as the address: 07b sets last_quantity only `if quantity_present`.
        ModbusEntityState state = afterOneRequest(1000.0, null, 2.0);
        state.advance(1001.0, 3, "18", ModbusDirection.REQUEST, null, null);
        assertEquals(2.0, state.lastQuantity());
    }

    @Test
    void thePendingMapIsCappedSoARequestFloodCannotGrowItWithoutLimit() {
        ModbusEntityState state = ModbusEntityState.empty();
        for (int i = 0; i < 5000; i++) {
            state.advance(1000.0 + i * 0.001, 3, "tid-" + i, ModbusDirection.REQUEST, null, null);
        }
        assertEquals(4096, state.outstandingRequests());
    }

    @Test
    void anEventOlderThanEveryPendingRequestDoesNotEvictItself() {
        // Fix round 1, F2: an eviction keyed on the MINIMUM recorded
        // timestamp across the whole map (including the entry just
        // inserted) could evict the entry this very call just added, if
        // its own timestamp happened to be older than everything already
        // pending. Insertion-order eviction only ever drops the map's
        // current head, never the entry just appended at the tail, so this
        // must survive regardless of how old its own timestamp is.
        ModbusEntityState state = ModbusEntityState.empty();
        for (int i = 0; i < 4096; i++) {
            state.advance(2000.0 + i, 3, "tid-" + i, ModbusDirection.REQUEST, null, null);
        }
        state.advance(1.0, 3, "tid-ancient", ModbusDirection.REQUEST, null, null);
        assertEquals(4096, state.outstandingRequests());
        assertTrue(state.hasPending("tid-ancient"),
            "the entry just added must not be the one evicted, regardless of its own timestamp");
    }

    @Test
    void reInsertingAStillPendingTidMovesItToTheEndOfEvictionOrder() {
        // Fix round 1's pinned decision: a request that reuses a
        // still-pending tid (request_overwrite_same_tid) is removed and
        // re-put, so it moves to the tail of insertion order instead of
        // keeping its original (now stale) position. Without that move,
        // the just-renewed tid-0 below would still look like the eldest
        // entry despite carrying the newest timestamp of all -- exactly
        // backwards for a cap meant to drop the genuinely oldest request
        // first.
        ModbusEntityState state = ModbusEntityState.empty();
        for (int i = 0; i < 4096; i++) {
            state.advance(2000.0 + i, 3, "tid-" + i, ModbusDirection.REQUEST, null, null);
        }
        state.advance(2000.0 + 4096, 3, "tid-0", ModbusDirection.REQUEST, null, null);
        assertTrue(state.hasPending("tid-0"), "tid-0 was just re-requested and must still be outstanding");

        state.advance(2000.0 + 4097, 3, "tid-new", ModbusDirection.REQUEST, null, null);
        assertEquals(4096, state.outstandingRequests());
        assertTrue(state.hasPending("tid-0"), "the just-renewed tid-0 must not be the one evicted");
        assertFalse(state.hasPending("tid-1"), "tid-1 is now the genuinely oldest outstanding request");
    }

    @Test
    void theProductionWindowCapIsOneHundredThousandEntries() {
        assertEquals(100_000, ModbusEntityState.MAX_WINDOW_ENTRIES);
    }

    @Test
    void aWindowCapBelowOneIsRejected() {
        // A cap of 0 would evict the entry advance just appended.
        assertThrows(IllegalArgumentException.class, () -> ModbusEntityState.emptyWithWindowCap(0));
    }

    @Test
    void eachWindowHoldsAtMostItsCap() {
        ModbusEntityState state = ModbusEntityState.emptyWithWindowCap(3);
        for (int i = 0; i < 5; i++) {
            state.advance(1000.0 + i * 0.1, 3, "t" + i, ModbusDirection.REQUEST, null, null);
        }
        assertEquals(3, state.eventCount1s());
        assertEquals(3, state.eventCount10s());
        assertEquals(3, state.eventCount60s());
    }

    @Test
    void theCapEvictsTheOldestEntryNeverTheOneJustAppended() {
        // Cap 1: each window keeps only the newest entry, so the FC-6 write just
        // appended survives and the earlier FC-3 read is the one uncounted.
        ModbusEntityState state = ModbusEntityState.emptyWithWindowCap(1);
        state.advance(1000.0, 3, "1", ModbusDirection.REQUEST, 40001.0, null);
        state.advance(1000.1, 6, "2", ModbusDirection.REQUEST, 40002.0, null);
        assertEquals(1, state.eventCount10s());
        assertEquals(1, state.uniqueFunctions10s());
        assertEquals(0, state.readCount10s(), "the FC-3 read was evicted");
        assertEquals(1, state.writeCount10s(), "the FC-6 write just appended stays");
    }

    @Test
    void aCapEvictionFromTheTenSecondWindowUncountsItsEntryLikeAPurge() {
        ModbusEntityState state = ModbusEntityState.emptyWithWindowCap(2);
        state.advance(1000.0, 3, "1", ModbusDirection.REQUEST, 40001.0, null);
        state.advance(1000.1, 6, "2", ModbusDirection.REQUEST, 40002.0, null);
        state.advance(1000.2, 16, "3", ModbusDirection.REQUEST, 40003.0, null);
        assertEquals(2, state.uniqueFunctions10s());
        assertEquals(2, state.uniqueAddresses10s());
        assertEquals(0, state.readCount10s());
        assertEquals(2, state.writeCount10s());
    }

    @Test
    void theSaturationFlagTurnsOnAtTheFirstCapEviction() {
        // An evicted entry is always one the purge left, i.e. still inside the
        // window, so the very first eviction already makes the window differ
        // from the uncapped one.
        ModbusEntityState state = ModbusEntityState.emptyWithWindowCap(10);
        for (int i = 0; i < 10; i++) {
            state.advance(1000.0 + i * 0.01, 3, "t" + i, ModbusDirection.REQUEST, null, null);
            assertFalse(state.windowSaturated(), "no eviction yet after event " + i);
        }
        state.advance(1000.10, 3, "t10", ModbusDirection.REQUEST, null, null);
        assertTrue(state.windowSaturated());
    }

    @Test
    void theSaturationFlagClearsOnceEveryEvictedEntryWouldHaveAgedOutOfItsWindow() {
        // A burst of 11 events at cap 10 evicts the first from every window.
        ModbusEntityState state = ModbusEntityState.emptyWithWindowCap(10);
        for (int i = 0; i <= 10; i++) {
            state.advance(1000.0 + i * 0.01, 3, "t" + i, ModbusDirection.REQUEST, null, null);
        }
        assertTrue(state.windowSaturated());

        // Events 14 s apart then keep the 60 s window full of burst entries,
        // so each one evicts the next burst entry (1000.01, .02, .03, .04):
        // still saturated, although the 1 s and 10 s windows long recovered.
        for (double ts : new double[] {1014.1, 1028.1, 1042.1, 1056.1}) {
            state.advance(ts, 3, "s" + ts, ModbusDirection.REQUEST, null, null);
            assertTrue(state.windowSaturated(), "the 60 s window still lacks an entry 07b holds at " + ts);
        }

        // At 1060.5 an uncapped 60 s window would have purged everything up to
        // 1000.5, the last evicted entry (1000.04) included: every window value
        // is exact again.
        state.advance(1060.5, 3, "last", ModbusDirection.REQUEST, null, null);
        assertFalse(state.windowSaturated());
        assertEquals(5, state.eventCount60s());
    }

    @Test
    void resetClearsSaturationButKeepsTheCap() {
        ModbusEntityState state = ModbusEntityState.emptyWithWindowCap(2);
        for (int i = 0; i < 3; i++) {
            state.advance(1000.0 + i * 0.1, 3, "t" + i, ModbusDirection.REQUEST, null, null);
        }
        assertTrue(state.windowSaturated());

        state.reset();
        assertFalse(state.windowSaturated());
        for (int i = 0; i < 3; i++) {
            state.advance(2000.0 + i * 0.1, 3, "u" + i, ModbusDirection.REQUEST, null, null);
        }
        assertEquals(2, state.eventCount1s(), "the cap of 2 survived the reset");
        assertTrue(state.windowSaturated());
    }

    @Test
    void anEmptyStateIsNeverSaturated() {
        assertFalse(ModbusEntityState.empty().windowSaturated());
    }
}
