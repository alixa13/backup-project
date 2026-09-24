package io.netsecml.platform.domain.feature;

import io.netsecml.platform.domain.event.ModbusEvent.ModbusDirection;
import io.netsecml.platform.domain.event.ModbusFunctionCode;

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

// The modbus causal state machine: the bounded per-(client_ip, server_ip,
// unit) state 19 of the 42 frozen modbus features read
// (ModbusFeatureSchemaV1). This is a direct, deliberately literal port of the
// upstream offline feature engine's EntityState dataclass and the window/
// state handling inside process_capture
// (two-models-info/modbus_/07b_materialize_feature_engine_v1.py) -- that file
// is the authority for every constant and every purge/update rule here, not
// this class's own comments. Where the two ever disagree, the Python wins.
//
// MUTABLE, updated in place, like the upstream EntityState itself. advance()
// changes this instance and returns an immutable BeforeEvent snapshot of the
// values the extractor must read from before the event. The trailing windows
// have to be bounded by TIME, not by a count -- exact counts over (t-w, t]
// are the frozen contract -- so under a flood of r events/s on one key the
// 60 s window holds about 60r entries. An earlier, immutable version copied
// all three windows and the pending-TID map on every event and rescanned the
// 10 s window six times, about 192r element operations per event: measured
// single-threaded on one key, it managed ~820 events/s against a 1,000
// events/s answered flood and ~510-560 events/s against 10,000 events/s,
// falling behind real time either way. In place, with 07b's own running 10 s
// counts (fc_counter_10, addr_counter_10, read_count_10, write_count_10)
// maintained on append and on purge, every event costs amortized O(1): each
// entry is appended once and purged once.
//
// Why in-place mutation is safe in Flink: this object lives in
// ValueState<ModbusEntityState> on the default heap state backend
// (HashMapStateBackend; the job configures none in code), whose
// CopyOnWriteStateMap.get(key, namespace) -- which ValueState.value() calls --
// hands out a serializer COPY of the state object, stored back in place of
// the original, whenever a running checkpoint snapshot still holds the
// original. A checkpoint therefore never sees a half-mutated object, provided
// the operator reads this state through value() on every call and never
// caches the reference in a field across calls (ModbusFeatureProcessFunction
// does exactly that). That copy costs O(state size), once per key per
// checkpoint, not per event. On RocksDB/ForSt the picture differs: every
// value()/update() (de)serializes the whole object, so per-event cost returns
// to O(window size) there -- this class is fast on the heap backend only.
//
// Capped, and flagged when the cap bites. Bounded by time alone, a window's
// memory still grows with the flood rate, so each of the three windows also
// holds at most MAX_WINDOW_ENTRIES entries: over the cap, the oldest entry is
// evicted (after the purge, so it is always an entry 07b would still hold,
// and never the entry just appended). A capped window then holds fewer
// entries than 07b's would for as long as an evicted entry would still be
// inside it, and exactly then windowSaturated() is true and
// ModbusBuildFeaturesUseCase sets QualityFlags.MODBUS_WINDOW_SATURATED on the
// vector. Every vector without that bit carries exactly the window values an
// uncapped engine would; the causal features (group C) never read a window
// and are exact either way. All three windows share one cap, so the 60 s
// window, which holds the most entries, reaches it first -- above ~1,667
// events/s on one key -- and in that common case only event_rate_60s differs
// from 07b. The 10 s window saturates above 10,000/s and the 1 s window above
// 100,000/s. A saturated window's event rate is lower than 07b's, its unique
// counts can be, and its read/write ratios can move either way; the bit does
// not say which window.
//
// Memory note: ArrayDeque and HashMap never shrink their backing arrays, so a
// key whose windows once grew large keeps that capacity (a few MB at the cap)
// until its segment ends; reset() therefore allocates fresh collections
// rather than clearing the grown ones. A key that goes idle keeps whatever
// its windows held at its last event -- nothing purges without an event --
// and with no TTL on the key set, that is for as long as the job runs.
//
// Still Kryo, not the POJO serializer: TypeInformation.of(ModbusEntityState
// .class) resolves to GenericTypeInfo (Kryo), because Flink's POJO analysis
// requires a public no-arg constructor and get/is-prefixed accessors for every
// field, and this class has neither. Kryo's own compatibility rules govern
// whether an old savepoint restores into a new field layout -- and this
// version's layout differs from the immutable one's (running counts, cap and
// cap-eviction timestamps added).
// That change was free only because the job had never been deployed, so no
// savepoint of the old layout exists; any later layout change is not free.
public final class ModbusEntityState {

    // A gap strictly greater than this starts a new causal segment. Matches
    // the upstream engine's GAP_SECONDS constant exactly.
    private static final double SEGMENT_GAP_SECONDS = 15.0;

    private static final double WINDOW_1S = 1.0;
    private static final double WINDOW_10S = 10.0;
    private static final double WINDOW_60S = 60.0;

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

    // Per-window entry cap -- see the class comment. Absent from 07b for the
    // same reason the pending cap is: 07b processes one finite capture.
    public static final int MAX_WINDOW_ENTRIES = 100_000;

    private Double lastTs;
    private Integer prevFunctionCode;
    private Double lastAddress;
    private Double lastQuantity;

    // transaction_id -> the REQUEST's own timestamp. Mirrors the upstream
    // engine's `pending: dict[Any, float]`, but held as a LinkedHashMap
    // (insertion-order iteration, NOT access-order -- access-order would
    // reorder on a plain get(), so reading a pending timestamp would mutate
    // the eviction order) rather than a HashMap, specifically so
    // evictOldestIfOverCap can find the entry to drop in O(1) instead of
    // scanning every entry for the minimum timestamp.
    //
    // That only gives the right answer if insertion order tracks timestamp
    // order, which requires two things this class enforces itself:
    //   - a REQUEST that reuses a still-pending tid is removed and
    //     re-inserted (see advance's pending mutation), so it moves to the
    //     tail instead of keeping the stale position from its first,
    //     now-superseded, timestamp;
    //   - see evictOldestIfOverCap's own comment for the deployment
    //     assumption that makes "insertion order" and "timestamp order"
    //     the same order for a well-behaved caller.
    private LinkedHashMap<String, Double> pending = new LinkedHashMap<>();

    // Trailing-window deques, upstream's w1_ts, w60_ts and w10_events. w1/w60
    // are used purely as counts (event_rate_1s = len(w1_ts), event_rate_60s =
    // len(w60_ts) / 60.0), so they hold only timestamps; w10 holds the fuller
    // upstream tuple because a purge must know which running counts the
    // leaving entry had incremented.
    private ArrayDeque<Double> window1s = new ArrayDeque<>();
    private ArrayDeque<Double> window60s = new ArrayDeque<>();
    private ArrayDeque<Window10Entry> window10s = new ArrayDeque<>();

    // The running 10 s counts, exactly upstream's fc_counter_10 (function
    // code -> events in the window), addr_counter_10 (address -> events in
    // the window, for events that carried one), read_count_10 and
    // write_count_10. Incremented on append, decremented on purge, and a map
    // key is removed when its count reaches zero (07b: `if <= 0: del`), so
    // each map's size is the number of DISTINCT values in the window.
    //
    // Address keys compare with Double.equals, as the HashSet<Double> the
    // immutable version rescanned into did: -0.0 and 0.0 are two keys, and
    // NaN equals NaN. Python's float keys differ on both (-0.0 == 0.0; NaN
    // != NaN). Zeek's modbus `address` is an unsigned register number, so
    // neither value is expected, but ModbusEventMapper does not reject them:
    // a record carrying one would count differently here than upstream.
    // Kept as it was, deliberately -- this class changed how it counts, not
    // what it counts.
    private HashMap<Integer, Integer> functionCounts10s = new HashMap<>();
    private HashMap<Double, Integer> addressCounts10s = new HashMap<>();
    private int readCount10s;
    private int writeCount10s;

    // This instance's per-window cap: MAX_WINDOW_ENTRIES for empty(), smaller
    // only through emptyWithWindowCap. Kept on the instance, so reset() keeps
    // it and a restored state brings it back.
    private final int windowCap;

    // The timestamp of the most recent entry each window's cap evicted, or
    // -infinity if none has been since the last reset. The windows are
    // sorted by timestamp, so this is also the NEWEST entry the cap has
    // taken from that window -- the one that stays inside 07b's window
    // longest.
    private double lastCapEvicted1s = Double.NEGATIVE_INFINITY;
    private double lastCapEvicted10s = Double.NEGATIVE_INFINITY;
    private double lastCapEvicted60s = Double.NEGATIVE_INFINITY;

    private ModbusEntityState(int windowCap) {
        this.windowCap = windowCap;
    }

    // One entry per event still (as of the last advance's purge) inside the
    // trailing 10-second window -- exactly the upstream engine's own
    // w10_events tuple. A private record: this never crosses this class's
    // boundary, so it does not need the read accessors a public domain
    // record would.
    private record Window10Entry(double ts, int functionCode, boolean addressPresent, Double address,
                                  boolean isRead, boolean isWrite) {
    }

    // The values of this state that the extractor reads as they stood
    // BEFORE an event -- process_capture's group C, which reads state.last_ts,
    // state.prev_fc, state.last_address, state.last_quantity,
    // len(state.pending) and state.pending[current_tid] before mutating any
    // of them. Captured by advance() before it changes anything.
    // pendingTsForTid is the pending REQUEST timestamp for the tid advance()
    // was given, or null if that tid was not pending (a pending timestamp is
    // never null, so null means absent).
    public record BeforeEvent(Double lastTs, Integer prevFunctionCode, Double lastAddress, Double lastQuantity,
                              int outstandingRequests, Double pendingTsForTid) {

        public boolean tidWasPending() {
            return pendingTsForTid != null;
        }
    }

    // The zero state a fresh (client_ip, server_ip, unit) key starts from.
    public static ModbusEntityState empty() {
        return new ModbusEntityState(MAX_WINDOW_ENTRIES);
    }

    // The zero state with a different per-window cap. For tests (a small cap
    // makes saturation cheap to reach) and tuning; production uses empty().
    // A cap below 1 would let the just-appended entry be evicted.
    public static ModbusEntityState emptyWithWindowCap(int windowCap) {
        if (windowCap < 1) {
            throw new IllegalArgumentException("windowCap must be at least 1, was " + windowCap);
        }
        return new ModbusEntityState(windowCap);
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
    // inter_arrival_s computation depends on -- and keeps each window deque
    // sorted by timestamp, which is what lets a purge stop at the first entry
    // still inside the window.
    public boolean startsNewSegment(double ts) {
        if (lastTs == null) {
            return true;
        }
        double gap = ts - lastTs;
        return gap < 0.0 || gap > SEGMENT_GAP_SECONDS;
    }

    // Mirrors the upstream engine's reset_for_new_segment(): every field this
    // class carries goes back to its zero value, in place. The collections are
    // replaced rather than cleared, so a segment that grew them to flood size
    // does not leave its large, now-empty backing arrays behind (see the class
    // comment's memory note); a reset happens at most once per segment, so
    // the allocation is cheap next to the events it separates. (The upstream
    // method also resets segment_local_id / position_in_segment, but those
    // are the upstream engine's own audit columns, outside
    // modbus-feature-v1's 42 frozen features -- not part of this class's
    // interface, and not planned to become part of it.)
    public void reset() {
        lastTs = null;
        prevFunctionCode = null;
        lastAddress = null;
        lastQuantity = null;
        pending = new LinkedHashMap<>();
        window1s = new ArrayDeque<>();
        window60s = new ArrayDeque<>();
        window10s = new ArrayDeque<>();
        functionCounts10s = new HashMap<>();
        addressCounts10s = new HashMap<>();
        readCount10s = 0;
        writeCount10s = 0;
        lastCapEvicted1s = Double.NEGATIVE_INFINITY;
        lastCapEvicted10s = Double.NEGATIVE_INFINITY;
        lastCapEvicted60s = Double.NEGATIVE_INFINITY;
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

    // The window reads. Each answers as of the last advance() -- whose purge
    // ran at that event's own timestamp, so every entry still held lies in
    // (lastTs - w, lastTs] -- which is the only instant the extractor asks
    // about. None takes a timestamp: a later instant's answer would need a
    // purge, and a purge is a mutation that belongs to advance().
    public int eventCount1s() {
        return window1s.size();
    }

    public int eventCount10s() {
        return window10s.size();
    }

    public int eventCount60s() {
        return window60s.size();
    }

    public int uniqueFunctions10s() {
        return functionCounts10s.size();
    }

    public int uniqueAddresses10s() {
        return addressCounts10s.size();
    }

    public int readCount10s() {
        return readCount10s;
    }

    public int writeCount10s() {
        return writeCount10s;
    }

    // True iff some window's values, as of the last advance, differ from what
    // 07b's uncapped window would hold: an entry the cap evicted from window w
    // is still inside (lastTs - w, lastTs]. The comparison is the exact
    // negation of the purge rule (`stored <= ts - w` leaves), so it turns
    // false at the same event 07b would have purged that entry itself.
    public boolean windowSaturated() {
        if (lastTs == null) {
            return false;
        }
        return lastCapEvicted1s > lastTs - WINDOW_1S
            || lastCapEvicted10s > lastTs - WINDOW_10S
            || lastCapEvicted60s > lastTs - WINDOW_60S;
    }

    // The state transition, in place. Captures the before-event snapshot
    // first, then mirrors process_capture's per-event handling exactly,
    // section by section (1c, the cap, is this class's own addition):
    //
    //   1. Purge every trailing window using the INCOMING event's own
    //      timestamp (purge_time_deque + the w10 cutoff loop, which also
    //      decrements the running 10 s counts for each entry it drops), THEN
    //      append the current event to each window and increment the counts.
    //      Purging first is what keeps this state bounded -- every event
    //      calls advance, so stale entries are always dropped before new ones
    //      arrive.
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
    // The caller decides segment boundaries BEFORE calling this (reset() on a
    // new segment), and reads group C from the returned snapshot and group D
    // from this state afterwards.
    public BeforeEvent advance(double ts, int functionCode, String tid, ModbusDirection direction,
                               Double address, Double quantity) {
        // 0. The before-event snapshot, before anything below changes.
        BeforeEvent before = new BeforeEvent(lastTs, prevFunctionCode, lastAddress, lastQuantity,
            pending.size(), pending.get(tid));

        // 1a. Purge (strict upstream semantics: stored <= cutoff is dropped).
        purgeTimeDeque(window1s, ts - WINDOW_1S);
        purgeTimeDeque(window60s, ts - WINDOW_60S);
        double cutoff10 = ts - WINDOW_10S;
        while (!window10s.isEmpty() && window10s.peekFirst().ts() <= cutoff10) {
            forgetInTenSecondCounts(window10s.pollFirst());
        }

        // 1b. Append the current event, and count it.
        window1s.addLast(ts);
        window60s.addLast(ts);
        boolean addressPresent = address != null;
        Window10Entry entry = new Window10Entry(ts, functionCode, addressPresent, address,
            ModbusFunctionCode.READ_FUNCTIONS.contains(functionCode),
            ModbusFunctionCode.WRITE_FUNCTIONS.contains(functionCode));
        window10s.addLast(entry);
        countInTenSecondCounts(entry);

        // 1c. Enforce the per-window cap: evict from the head (the oldest),
        // recording what left. A window is at most windowCap entries before
        // this event and one more after its append, and windowCap >= 1, so
        // the entry just appended at the tail is never the one evicted. An
        // entry leaving the 10 s window is uncounted exactly as a purge
        // uncounts it.
        while (window1s.size() > windowCap) {
            lastCapEvicted1s = window1s.pollFirst();
        }
        while (window60s.size() > windowCap) {
            lastCapEvicted60s = window60s.pollFirst();
        }
        while (window10s.size() > windowCap) {
            Window10Entry evicted = window10s.pollFirst();
            forgetInTenSecondCounts(evicted);
            lastCapEvicted10s = evicted.ts();
        }

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
            pending.remove(tid);
            pending.put(tid, ts);
            evictOldestIfOverCap(pending);
        } else {
            pending.remove(tid);
        }

        // 3. Previous-event state.
        prevFunctionCode = functionCode;
        lastTs = ts;
        if (addressPresent) {
            lastAddress = address;
        }
        if (quantity != null) {
            lastQuantity = quantity;
        }

        return before;
    }

    // An entry joining the 10 s window: `fc_counter_10[fc] += 1`,
    // `addr_counter_10[addr] += 1` when an address is present, and the
    // read/write tallies. Function code 23 is in both READ_FUNCTIONS and
    // WRITE_FUNCTIONS, so it counts in both tallies, never neither.
    private void countInTenSecondCounts(Window10Entry entry) {
        functionCounts10s.merge(entry.functionCode(), 1, Integer::sum);
        if (entry.addressPresent()) {
            addressCounts10s.merge(entry.address(), 1, Integer::sum);
        }
        readCount10s += entry.isRead() ? 1 : 0;
        writeCount10s += entry.isWrite() ? 1 : 0;
    }

    // An entry leaving the 10 s window: the exact reverse, and a key whose
    // count reaches zero is removed, so each map's size stays the number of
    // distinct values still in the window. (sumOrRemove returns null at zero,
    // which Map.merge treats as "remove the key".)
    private void forgetInTenSecondCounts(Window10Entry entry) {
        functionCounts10s.merge(entry.functionCode(), -1, ModbusEntityState::sumOrRemove);
        if (entry.addressPresent()) {
            addressCounts10s.merge(entry.address(), -1, ModbusEntityState::sumOrRemove);
        }
        readCount10s -= entry.isRead() ? 1 : 0;
        writeCount10s -= entry.isWrite() ? 1 : 0;
    }

    private static Integer sumOrRemove(Integer count, Integer delta) {
        int sum = count + delta;
        return sum <= 0 ? null : sum;
    }

    // `while q and q[0] <= cutoff: popleft()`, verbatim.
    private static void purgeTimeDeque(ArrayDeque<Double> deque, double cutoff) {
        while (!deque.isEmpty() && deque.peekFirst() <= cutoff) {
            deque.pollFirst();
        }
    }

    // Cap enforcement: evicts the single eldest-BY-INSERTION entry if the
    // map is now over MAX_PENDING -- O(1) via LinkedHashMap's own iteration
    // order (removing its first key), not an O(map size) scan for the
    // minimum timestamp. A single advance call adds, or moves, at most one
    // entry to the tail (see the pending mutation above), so that entry is
    // always the newest in iteration order and can never be the one this
    // method evicts -- an event whose own timestamp is older than everything
    // already pending still cannot evict itself, because eviction only ever
    // looks at the head.
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
