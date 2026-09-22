package io.netsecml.platform.domain.feature;

import io.netsecml.platform.domain.event.ModbusEvent.ModbusDirection;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

// Pins the modbus causal state machine against the upstream offline engine
// (two-models-info/modbus_/07b_materialize_feature_engine_v1.py's EntityState
// dataclass and process_capture's window/state handling), which is the
// authority this class must match bit-for-bit: 19 of the 42 frozen modbus
// features read this state, and a semantic drift here is invisible to the
// compiler.
class ModbusEntityStateTest {

    @Test
    void aGapLongerThanFifteenSecondsStartsANewSegment() {
        ModbusEntityState state = ModbusEntityState.empty()
            .afterEvent(1000.0, 3, "17", ModbusDirection.REQUEST, 40001.0, 2.0);
        assertTrue(state.startsNewSegment(1015.01));
        assertFalse(state.startsNewSegment(1015.0), "exactly 15s is still the same segment");
    }

    @Test
    void aNegativeGapAlsoStartsANewSegment() {
        // The upstream engine raises here, because offline it has already asserted a
        // strictly increasing event index. A streaming operator must not fail the job
        // on one out-of-order record, and resetting keeps in-segment inter-arrival
        // within [0, 15] by construction.
        ModbusEntityState state = ModbusEntityState.empty()
            .afterEvent(1000.0, 3, "17", ModbusDirection.REQUEST, 40001.0, 2.0);
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
        ModbusEntityState before = ModbusEntityState.empty();
        assertEquals(0, before.outstandingRequests());
        ModbusEntityState after = before.afterEvent(1000.0, 3, "17", ModbusDirection.REQUEST, null, null);
        assertEquals(1, after.outstandingRequests());
    }

    @Test
    void aMatchedResponseClearsItsPendingTid() {
        ModbusEntityState state = ModbusEntityState.empty()
            .afterEvent(1000.0, 3, "17", ModbusDirection.REQUEST, null, null)
            .afterEvent(1000.5, 3, "17", ModbusDirection.RESPONSE, null, null);
        assertEquals(0, state.outstandingRequests());
        assertFalse(state.hasPending("17"));
    }

    @Test
    void theTenSecondWindowExcludesAnEventExactlyTenSecondsOld() {
        // Half-open (t-w, t]: the upstream purge is `stored <= ts - w`.
        ModbusEntityState state = ModbusEntityState.empty()
            .afterEvent(1000.0, 3, "17", ModbusDirection.REQUEST, null, null);
        assertEquals(1, state.windowCount10s(1009.99));
        assertEquals(0, state.windowCount10s(1010.0));
    }

    @Test
    void purgingTheTenSecondWindowDecrementsItsFunctionAndAddressCounters() {
        ModbusEntityState state = ModbusEntityState.empty()
            .afterEvent(1000.0, 3, "17", ModbusDirection.REQUEST, 40001.0, null)
            .afterEvent(1001.0, 6, "18", ModbusDirection.REQUEST, 40002.0, null);
        assertEquals(2, state.uniqueFunctions10s(1001.0));
        assertEquals(2, state.uniqueAddresses10s(1001.0));
        assertEquals(1, state.uniqueFunctions10s(1010.5), "the FC-3 event has aged out");
        assertEquals(1, state.uniqueAddresses10s(1010.5));
    }

    @Test
    void functionCode23CountsInBothTheReadAndWriteTallies() {
        ModbusEntityState state = ModbusEntityState.empty()
            .afterEvent(1000.0, 23, "17", ModbusDirection.REQUEST, null, null);
        assertEquals(1, state.readCount10s(1000.0));
        assertEquals(1, state.writeCount10s(1000.0));
    }

    @Test
    void anAbsentAddressIsNotCountedAsAUniqueAddress() {
        ModbusEntityState state = ModbusEntityState.empty()
            .afterEvent(1000.0, 3, "17", ModbusDirection.REQUEST, null, null);
        assertEquals(0, state.uniqueAddresses10s(1000.0));
    }

    @Test
    void resetForNewSegmentClearsEverything() {
        ModbusEntityState state = ModbusEntityState.empty()
            .afterEvent(1000.0, 3, "17", ModbusDirection.REQUEST, 40001.0, 2.0)
            .resetForNewSegment();
        assertNull(state.lastTs());
        assertNull(state.prevFunctionCode());
        assertNull(state.lastAddress());
        assertNull(state.lastQuantity());
        assertEquals(0, state.outstandingRequests());
        assertEquals(0, state.windowCount60s(1000.0));
    }

    @Test
    void theLastAddressSurvivesAnEventWithNoAddress() {
        // "previous APPLICABLE address" -- the delta reaches back past events that
        // carried none, which is why lastAddress is only updated when one is present.
        ModbusEntityState state = ModbusEntityState.empty()
            .afterEvent(1000.0, 3, "17", ModbusDirection.REQUEST, 40001.0, null)
            .afterEvent(1001.0, 3, "18", ModbusDirection.REQUEST, null, null);
        assertEquals(40001.0, state.lastAddress());
    }

    @Test
    void thePendingMapIsCappedSoARequestFloodCannotGrowItWithoutLimit() {
        ModbusEntityState state = ModbusEntityState.empty();
        for (int i = 0; i < 5000; i++) {
            state = state.afterEvent(1000.0 + i * 0.001, 3, "tid-" + i, ModbusDirection.REQUEST, null, null);
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
            state = state.afterEvent(2000.0 + i, 3, "tid-" + i, ModbusDirection.REQUEST, null, null);
        }
        state = state.afterEvent(1.0, 3, "tid-ancient", ModbusDirection.REQUEST, null, null);
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
            state = state.afterEvent(2000.0 + i, 3, "tid-" + i, ModbusDirection.REQUEST, null, null);
        }
        state = state.afterEvent(2000.0 + 4096, 3, "tid-0", ModbusDirection.REQUEST, null, null);
        assertTrue(state.hasPending("tid-0"), "tid-0 was just re-requested and must still be outstanding");

        state = state.afterEvent(2000.0 + 4097, 3, "tid-new", ModbusDirection.REQUEST, null, null);
        assertEquals(4096, state.outstandingRequests());
        assertTrue(state.hasPending("tid-0"), "the just-renewed tid-0 must not be the one evicted");
        assertFalse(state.hasPending("tid-1"), "tid-1 is now the genuinely oldest outstanding request");
    }
}
