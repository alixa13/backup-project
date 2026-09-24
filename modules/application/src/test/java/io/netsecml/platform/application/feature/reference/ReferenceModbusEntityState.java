package io.netsecml.platform.application.feature.reference;

// Verbatim copy of the engine at commit 1d0878e, kept as a parity oracle.
// Only the package, the class names and the imports those renames require
// differ from the original; every other line is untouched, including the
// comments, which describe the engine as it stood at that commit and are not
// maintained. Never edit this file to follow production -- its whole value is
// that it does not.


import io.netsecml.platform.domain.event.ModbusEvent.ModbusDirection;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

// The modbus causal state machine: the bounded per-(client_ip, server_ip,
// unit) state 19 of the 42 frozen modbus features read
// (ModbusFeatureSchemaV1). This is a direct, deliberately literal port of the
// upstream offline feature engine's EntityState dataclass and the window/
// state handling inside process_capture
// (two-models-info/modbus_/07b_materialize_feature_engine_v1.py) -- that file
// is the authority for every constant and every purge/update rule here, not
// this class's own javadoc. Where the two ever disagree, the Python wins.
//
// Immutable-returning, following RollingCounters' established pattern for
// keyed state in this codebase: afterEvent(...) never mutates this instance,
// it copies the deques and the pending-TID map structurally (new
// ArrayDeque<>(old), new LinkedHashMap<>(old) -- see the `pending` field's own
// comment for why LinkedHashMap specifically, not HashMap) and returns a new
// ReferenceModbusEntityState built from the copies. That is a real cost, not a free
// choice: RollingCounters copies four
// fixed five-element arrays (trivial), while this class copies whatever its
// trailing windows currently hold -- at a 10 Hz poll rate the 60-second
// deque holds roughly 600 entries. Still cheap at realistic Modbus TCP
// rates, and matching the codebase's one existing pattern beats optimising
// an allocation nobody has profiled. If a future profiler disagrees, the fix
// is a mutable-buffer variant behind the same immutable-returning API, not a
// wholesale redesign of the extractor that depends on this class staying a
// pure function of (state, event) -> state.
//
// MEASURED, not assumed: this class is held directly in Flink
// ValueState<ReferenceModbusEntityState> (the same shape DnsFeatureProcessFunction
// uses for DnsWindowState), and
// TypeInformation.of(ReferenceModbusEntityState.class) resolves to
// org.apache.flink.api.java.typeutils.GenericTypeInfo -- i.e. Kryo, not the
// POJO/record serializer, exactly like RollingCounters and RecordTimingState
// (DnsWindowState's own two components) already do. The reason is the same
// one that sends those two to Kryo: Flink's POJO analysis requires (1) a
// public no-arg constructor, which this class does not have (its constructor
// is private, taking every field); and (2) bean-style get/is-prefixed,
// no-argument accessors for every field, which this class also does not
// have -- lastTs()/prevFunctionCode()/lastAddress()/lastQuantity() are
// argument-less but not get/is-prefixed, and windowCount1s(ts) and its
// siblings are neither prefixed nor argument-less, since they answer for a
// caller-supplied instant rather than exposing a stored field directly (see
// windowCount1s's own comment for why). Practical consequence: state
// evolution here is Kryo's problem, not the POJO serializer's -- adding,
// removing or reordering a field changes what a running job has serialized
// under this operator's uid, and Kryo's own compatibility rules (not
// Flink's POJO schema migration) govern whether an old savepoint can still
// restore into a new field layout.
public final class ReferenceModbusEntityState {

    // A gap strictly greater than this starts a new causal segment. Matches
    // the upstream engine's GAP_SECONDS constant exactly.
    private static final double SEGMENT_GAP_SECONDS = 15.0;

    private static final double WINDOW_1S = 1.0;
    private static final double WINDOW_10S = 10.0;
    private static final double WINDOW_60S = 60.0;

    // Function codes the upstream engine's READ_FUNCTIONS / WRITE_FUNCTIONS
    // sets fold into the 10-second read/write tallies. 23 (Read/Write
    // Multiple Registers) appears in both: one PDU performs both operations,
    // so it counts in both tallies, never neither -- pinned by
    // functionCode23CountsInBothTheReadAndWriteTallies.
    private static final Set<Integer> READ_FUNCTIONS = Set.of(1, 2, 3, 4, 20, 24, 23);
    private static final Set<Integer> WRITE_FUNCTIONS = Set.of(5, 6, 15, 16, 21, 22, 23);

    // Pending-TID cap: absent from the upstream offline engine, whose
    // `pending` dict is unbounded because it processes one finite capture
    // file at a time and never runs longer than that. A streaming operator
    // has no such natural bound -- a flood of requests that never receive a
    // response (itself one of the attack shapes this detector exists to
    // catch) would otherwise grow this map without limit, which breaks the
    // bounded-state invariant every other keyed state in this codebase
    // holds to. Capped at 4096 entries, evicted oldest-first BY INSERTION
    // ORDER (see the `pending` field's own comment for why that equals
    // timestamp order here, and evictOldestIfOverCap for the O(1) mechanics):
    // a genuine long-lived outstanding request is dropped before a
    // recently-issued one is. Recorded as a known limit in CLAUDE.md's
    // "Modbus limits and decisions".
    private static final int MAX_PENDING = 4096;

    private final Double lastTs;
    private final Integer prevFunctionCode;
    private final Double lastAddress;
    private final Double lastQuantity;

    // transaction_id -> the REQUEST's own timestamp. Mirrors the upstream
    // engine's `pending: dict[Any, float]`, but held as a LinkedHashMap
    // (insertion-order iteration, NOT access-order -- access-order would
    // reorder on a plain get() inside pendingTs(), mutating this supposedly
    // read-only accessor's target map) rather than a HashMap, specifically
    // so evictOldestIfOverCap can find the entry to drop in O(1) instead of
    // scanning every entry for the minimum timestamp.
    //
    // That only gives the right answer if insertion order tracks timestamp
    // order, which requires two things this class enforces itself:
    //   - a REQUEST that reuses a still-pending tid is removed and
    //     re-inserted (see afterEvent's pending mutation), so it moves to
    //     the tail instead of keeping the stale position from its first,
    //     now-superseded, timestamp;
    //   - see evictOldestIfOverCap's own comment for the deployment
    //     assumption that makes "insertion order" and "timestamp order"
    //     the same order for a well-behaved caller.
    private final Map<String, Double> pending;

    // Trailing-window deques. w1/w60 need only the timestamp (upstream's
    // w1_ts / w60_ts are used purely as counts: event_rate_1s = len(w1_ts),
    // event_rate_60s = len(w60_ts) / 60.0 -- both ModbusFeatureExtractor's
    // concern, read via this class's own windowCount1s/windowCount60s below,
    // not this state's). w10 needs the fuller upstream w10_events tuple
    // (ts, function_code, address_present, address, is_read, is_write)
    // because the 10-second accessors below (uniqueFunctions10s,
    // uniqueAddresses10s, readCount10s, writeCount10s) must be able to
    // recompute their answer for it.
    private final Deque<Double> window1s;
    private final Deque<Double> window60s;
    private final Deque<Window10Entry> window10s;

    private ReferenceModbusEntityState(Double lastTs, Integer prevFunctionCode, Double lastAddress, Double lastQuantity,
                               Map<String, Double> pending, Deque<Double> window1s, Deque<Double> window60s,
                               Deque<Window10Entry> window10s) {
        this.lastTs = lastTs;
        this.prevFunctionCode = prevFunctionCode;
        this.lastAddress = lastAddress;
        this.lastQuantity = lastQuantity;
        this.pending = pending;
        this.window1s = window1s;
        this.window60s = window60s;
        this.window10s = window10s;
    }

    // One entry per event still (as of the last afterEvent purge) inside the
    // trailing 10-second window -- exactly the upstream engine's own
    // w10_events tuple. A private record: this never crosses this class's
    // boundary, so it does not need the read accessors a public domain
    // record would.
    private record Window10Entry(double ts, int functionCode, boolean addressPresent, Double address,
                                  boolean isRead, boolean isWrite) {
    }

    // The zero state a fresh (client_ip, server_ip, unit) key starts from.
    public static ReferenceModbusEntityState empty() {
        return new ReferenceModbusEntityState(null, null, null, null,
            new LinkedHashMap<>(), new ArrayDeque<>(), new ArrayDeque<>(), new ArrayDeque<>());
    }

    // True when the given timestamp is far enough past this state's last
    // event to start a new causal segment. An empty state (no prior event at
    // all) always does. A gap strictly greater than 15s does. A NEGATIVE gap
    // also does, deliberately differing from the upstream offline engine,
    // which raises on a negative gap because by the time it runs it has
    // already asserted the whole capture is in strictly increasing
    // capture_event_index order. A streaming operator receives events in
    // arrival order, not a pre-validated total order, and must not fail the
    // whole job over one out-of-order record; treating a negative gap as
    // "start a new segment" keeps every in-segment inter-arrival within
    // [0, 15] by construction, which is the invariant ModbusFeatureExtractor's
    // inter_arrival_s computation depends on.
    public boolean startsNewSegment(double ts) {
        if (lastTs == null) {
            return true;
        }
        double gap = ts - lastTs;
        return gap < 0.0 || gap > SEGMENT_GAP_SECONDS;
    }

    // Mirrors the upstream engine's reset_for_new_segment(): every field this
    // class carries goes back to its zero value. (The upstream method also
    // resets segment_local_id / position_in_segment, but those are the
    // upstream engine's own audit columns, outside modbus-feature-v1's 42
    // frozen features -- not part of this class's interface, and not planned
    // to become part of it.)
    public ReferenceModbusEntityState resetForNewSegment() {
        return empty();
    }

    public Double lastTs() {
        return lastTs;
    }

    public Integer prevFunctionCode() {
        return prevFunctionCode;
    }

    public Double lastAddress() {
        return lastAddress;
    }

    public Double lastQuantity() {
        return lastQuantity;
    }

    public int outstandingRequests() {
        return pending.size();
    }

    public boolean hasPending(String tid) {
        return pending.containsKey(tid);
    }

    public Double pendingTs(String tid) {
        return pending.get(tid);
    }

    // Half-open window: an entry stored at exactly `ts - window` seconds old
    // is EXCLUDED (upstream purges with `stored <= ts - window`), so this
    // filter must be strictly greater-than for the same entries to be
    // included here.
    //
    // Deliberately filters the deque fresh on every call rather than reading
    // a maintained running count: afterEvent purges (and so the deque's
    // *contents*) only ever advance to the timestamp of the event that was
    // just folded in, but an accessor can be asked about ANY later instant
    // (that is exactly what this class's own boundary tests do -- see
    // ModbusEntityStateTest.purgingTheTenSecondWindowDecrementsItsFunctionAndAddressCounters,
    // which queries a state whose last afterEvent call was at ts=1001.0
    // for both ts=1001.0 and ts=1010.5). A running counter frozen at the
    // last afterEvent's purge point would answer the second query wrong. In
    // production the extractor always queries with the same ts afterEvent
    // just purged at, so this and a maintained counter would agree there --
    // the two diverge only when a caller asks about a later instant, which
    // is exactly the case these accessors are built to answer correctly.
    public int windowCount1s(double ts) {
        return countAfter(window1s, ts, WINDOW_1S);
    }

    public int windowCount60s(double ts) {
        return countAfter(window60s, ts, WINDOW_60S);
    }

    public int windowCount10s(double ts) {
        double cutoff = ts - WINDOW_10S;
        int count = 0;
        for (Window10Entry entry : window10s) {
            if (entry.ts() > cutoff) {
                count++;
            }
        }
        return count;
    }

    public int uniqueFunctions10s(double ts) {
        double cutoff = ts - WINDOW_10S;
        Set<Integer> functions = new HashSet<>();
        for (Window10Entry entry : window10s) {
            if (entry.ts() > cutoff) {
                functions.add(entry.functionCode());
            }
        }
        return functions.size();
    }

    public int uniqueAddresses10s(double ts) {
        double cutoff = ts - WINDOW_10S;
        Set<Double> addresses = new HashSet<>();
        for (Window10Entry entry : window10s) {
            if (entry.ts() > cutoff && entry.addressPresent()) {
                addresses.add(entry.address());
            }
        }
        return addresses.size();
    }

    public int readCount10s(double ts) {
        double cutoff = ts - WINDOW_10S;
        int count = 0;
        for (Window10Entry entry : window10s) {
            if (entry.ts() > cutoff && entry.isRead()) {
                count++;
            }
        }
        return count;
    }

    public int writeCount10s(double ts) {
        double cutoff = ts - WINDOW_10S;
        int count = 0;
        for (Window10Entry entry : window10s) {
            if (entry.ts() > cutoff && entry.isWrite()) {
                count++;
            }
        }
        return count;
    }

    private static int countAfter(Deque<Double> deque, double ts, double window) {
        double cutoff = ts - window;
        int count = 0;
        for (double stored : deque) {
            if (stored > cutoff) {
                count++;
            }
        }
        return count;
    }

    // The state transition. Mirrors process_capture's per-event handling
    // exactly, section by section:
    //
    //   1. Purge every trailing window using the INCOMING event's own
    //      timestamp (purge_time_deque + the w10 cutoff loop), THEN append
    //      the current event to each. Purging first is what keeps this
    //      state bounded -- every event calls afterEvent, so stale entries
    //      are always dropped before new ones arrive.
    //   2. Mutate the pending-TID map: a request records itself as pending
    //      (overwriting any existing entry for the same tid, matching the
    //      upstream engine's unconditional `pending[tid] = ts`); a response
    //      clears its tid if one was pending. This happens AFTER window
    //      bookkeeping, matching upstream's ordering, though the two are
    //      independent and this class does not rely on the ordering itself.
    //   3. Update prevFunctionCode and lastTs unconditionally, and
    //      lastAddress / lastQuantity only when this event actually carried
    //      one -- "previous APPLICABLE address", per the upstream engine's
    //      own `if address_present: state.last_address = ...` guard.
    //
    // The caller is expected to have already read whatever BEFORE-mutation
    // features it needs (outstanding_requests_before_event, hasPending,
    // lastAddress, etc.) from the state this is called on, since this
    // returns a DIFFERENT (new) state reflecting the event having happened.
    public ReferenceModbusEntityState afterEvent(double ts, int functionCode, String tid, ModbusDirection direction,
                                         Double address, Double quantity) {
        Deque<Double> newWindow1s = new ArrayDeque<>(window1s);
        Deque<Double> newWindow60s = new ArrayDeque<>(window60s);
        Deque<Window10Entry> newWindow10s = new ArrayDeque<>(window10s);
        // LinkedHashMap's copy constructor iterates its source in that
        // source's own order, so copying a LinkedHashMap here preserves
        // insertion order rather than falling back to hash-bucket order.
        Map<String, Double> newPending = new LinkedHashMap<>(pending);

        // 1a. Purge (strict upstream semantics: stored <= cutoff is dropped).
        purgeTimeDeque(newWindow1s, ts - WINDOW_1S);
        purgeTimeDeque(newWindow60s, ts - WINDOW_60S);
        double cutoff10 = ts - WINDOW_10S;
        while (!newWindow10s.isEmpty() && newWindow10s.peekFirst().ts() <= cutoff10) {
            newWindow10s.pollFirst();
        }

        // 1b. Append the current event.
        newWindow1s.addLast(ts);
        newWindow60s.addLast(ts);
        boolean addressPresent = address != null;
        boolean isRead = READ_FUNCTIONS.contains(functionCode);
        boolean isWrite = WRITE_FUNCTIONS.contains(functionCode);
        newWindow10s.addLast(new Window10Entry(ts, functionCode, addressPresent, address, isRead, isWrite));

        // 2. Pending-TID mutation. A request that reuses a still-pending
        // tid (request_overwrite_same_tid, feature index 32) is removed
        // before being re-put, DELIBERATELY, so it moves to the tail of
        // insertion order instead of keeping its original position:
        // LinkedHashMap's plain put() on an already-present key overwrites
        // the value in place WITHOUT moving it, and leaving a renewed
        // request at its stale position would make it look like the
        // eldest outstanding request when it is in fact the newest --
        // exactly backwards for a cap whose purpose is dropping the
        // genuinely oldest request first (see evictOldestIfOverCap and
        // the `pending` field's own comment; pinned by
        // ModbusEntityStateTest.reInsertingAStillPendingTidMovesItToTheEndOfEvictionOrder).
        if (direction == ModbusDirection.REQUEST) {
            newPending.remove(tid);
            newPending.put(tid, ts);
            evictOldestIfOverCap(newPending);
        } else {
            newPending.remove(tid);
        }

        // 3. Previous-event state.
        Integer newPrevFunctionCode = functionCode;
        Double newLastTs = ts;
        Double newLastAddress = addressPresent ? address : lastAddress;
        Double newLastQuantity = quantity != null ? quantity : lastQuantity;

        return new ReferenceModbusEntityState(newLastTs, newPrevFunctionCode, newLastAddress, newLastQuantity,
            newPending, newWindow1s, newWindow60s, newWindow10s);
    }

    // `while q and q[0] <= cutoff: popleft()`, verbatim.
    private static void purgeTimeDeque(Deque<Double> deque, double cutoff) {
        while (!deque.isEmpty() && deque.peekFirst() <= cutoff) {
            deque.pollFirst();
        }
    }

    // Cap enforcement: evicts the single eldest-BY-INSERTION entry if the
    // map is now over MAX_PENDING -- O(1) via LinkedHashMap's own iteration
    // order (removing its first key), not an O(map size) scan for the
    // minimum timestamp. A single afterEvent call adds, or moves, at most
    // one entry to the tail (see the pending mutation above), so that
    // entry is always the newest in iteration order and can never be the
    // one this method evicts -- an event whose own timestamp is older than
    // everything already pending still cannot evict itself, because
    // eviction only ever looks at the head.
    //
    // "Eldest by insertion" only equals "eldest by timestamp" under this
    // deployment's required per-key arrival ordering -- the same
    // requirement RollingCounters' own comment records for conn/dns: the
    // sensor's Kafka producer must partition by the record's key (here,
    // (client_ip, server_ip, unit)) so that one key's events arrive with
    // non-decreasing timestamps. Under that ordering, this map's insertion
    // order and timestamp order are the same order, so evicting the head is
    // evicting the genuinely oldest outstanding request. An out-of-order
    // arrival for this key does not silently break that assumption: a
    // negative gap makes startsNewSegment(ts) report a new segment, and a
    // new segment resets pending to empty before any further event reaches
    // this method with a stale ordering to exploit.
    private static void evictOldestIfOverCap(Map<String, Double> pending) {
        if (pending.size() <= MAX_PENDING) {
            return;
        }
        Iterator<String> insertionOrder = pending.keySet().iterator();
        insertionOrder.next();
        insertionOrder.remove();
    }
}
