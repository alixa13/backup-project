package io.netsecml.platform.application.usecase;

import io.netsecml.platform.domain.event.EventEnvelope;
import io.netsecml.platform.domain.event.EventId;
import io.netsecml.platform.domain.event.LogType;
import io.netsecml.platform.domain.event.ModbusEvent;
import io.netsecml.platform.domain.event.ModbusEvent.ModbusDirection;
import io.netsecml.platform.domain.event.SensorId;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

// Reproducible modbus event streams for the tests that compare the production
// engine against the verbatim 1d0878e oracle (reference package). Every
// stream is a pure function of its arguments -- a failing seed replays
// exactly.
final class ModbusEventStreams {

    private static final SensorId SENSOR = new SensorId("sensor-eu-1");

    // A Zeek-like epoch base: large enough that a microsecond is near the
    // double's resolution, exactly as on the wire.
    private static final double BASE_TS = 1_700_000_000.0;

    // Gaps straddling every edge the engine has: the 1 s, 10 s and 60 s window
    // cutoffs (purged at `stored <= ts - w`) and the 15 s segment gap (a new
    // segment at `gap > 15`). The whole-second ones are added to the clock
    // WITHOUT rounding, so `ts - w` lands exactly on an earlier timestamp and
    // the equality edge itself is exercised, not just its neighbours.
    private static final double[] EDGE_GAPS = {
        0.999999, 1.0, 1.000001, 9.999999, 10.0, 10.000001,
        14.999999, 15.0, 15.000001, 59.999999, 60.0, 60.000001};

    // Read, write, both (23) and neither (7, 8, 43, 90).
    private static final int[] FUNCTION_CODES = {1, 2, 3, 4, 5, 6, 7, 8, 15, 16, 20, 21, 22, 23, 24, 43, 90};

    // -0.0 is included on purpose: it equals 0.0 as a primitive but not under
    // Double.equals, the address-key equality both engines use.
    private static final double[] ADDRESSES = {0.0, -0.0, 1.0, 7.5, 40001.0, 40002.0, 40010.0, 65535.0};

    private static final double[] QUANTITIES = {1.0, 2.0, 10.0, 125.0};

    // A small tid pool, so requests reuse still-pending tids and responses
    // arrive both for pending tids and for tids nothing requested.
    private static final int TID_POOL = 12;

    private ModbusEventStreams() {
    }

    // One event and the index of the entity key it belongs to.
    record Keyed(int key, ModbusEvent event) {
    }

    // A mixed stream over `keys` interleaved entity keys. Each key keeps its
    // own clock, so interleaving never makes one key's time go backwards --
    // only the deliberate negative gaps below do.
    static List<Keyed> randomMixed(long seed, int length, int keys) {
        Random random = new Random(seed);
        double[] clocks = new double[keys];
        for (int k = 0; k < keys; k++) {
            clocks[k] = BASE_TS + k * 1000.0;
        }
        List<Keyed> stream = new ArrayList<>(length);
        while (stream.size() < length) {
            int key = random.nextInt(keys);

            // Now and then, a ladder of four exact 15 s gaps: each stays inside
            // the segment, and together they reach exactly 60 s back, which is
            // the 60 s window's own equality edge.
            if (random.nextInt(50) == 0) {
                for (int rung = 0; rung < 4 && stream.size() < length; rung++) {
                    clocks[key] += 15.0;
                    stream.add(new Keyed(key, randomEvent(random, key, clocks[key], stream.size())));
                }
                continue;
            }

            // Now and then, a burst of 50-249 sub-half-second gaps on one key:
            // nothing in it can restart the segment, so it runs past 60 s of
            // event time and fills the 60 s window with 100+ entries while
            // purging its oldest -- which the mixed gaps alone, restarting
            // every ten or so events, almost never reach.
            if (random.nextInt(40) == 0) {
                int burst = 50 + random.nextInt(200);
                for (int b = 0; b < burst && stream.size() < length; b++) {
                    clocks[key] = micros(clocks[key] + random.nextInt(500_000) / 1e6);
                    stream.add(new Keyed(key, randomEvent(random, key, clocks[key], stream.size())));
                }
                continue;
            }

            clocks[key] = nextTs(random, clocks[key]);
            stream.add(new Keyed(key, randomEvent(random, key, clocks[key], stream.size())));
        }
        return stream;
    }

    // An unanswered request flood on one key, with a response to an old tid
    // every 50th event: the pending map passes its 4096 cap, and those late
    // responses find their request evicted or still pending depending on age.
    static List<ModbusEvent> unansweredFlood(int length, double gapSeconds) {
        List<ModbusEvent> stream = new ArrayList<>(length);
        double ts = BASE_TS;
        for (int i = 0; i < length; i++) {
            ts += gapSeconds;
            if (i % 50 == 49) {
                String oldTid = "flood-" + (i - 4500);
                stream.add(event(0, ts, i, ModbusDirection.RESPONSE, 3, oldTid, 40001.0, 1.0, true));
            } else {
                stream.add(event(0, ts, i, ModbusDirection.REQUEST, 3, "flood-" + i, 40001.0 + (i % 64), 1.0,
                    false));
            }
        }
        return stream;
    }

    // `length` events at `ratePerSecond` of event time on one key, in pairs:
    // an even pair is a request and its response, an odd pair is two requests
    // on one tid that nobody answers -- a flood that is half answered, half
    // not.
    static List<ModbusEvent> steadyFlood(int length, double ratePerSecond) {
        List<ModbusEvent> stream = new ArrayList<>(length);
        for (int i = 0; i < length; i++) {
            stream.add(steadyFloodEvent(i, ratePerSecond));
        }
        return stream;
    }

    // The i-th event of steadyFlood, for callers that stream a flood too long
    // to hold in memory at once.
    static ModbusEvent steadyFloodEvent(int i, double ratePerSecond) {
        double ts = BASE_TS + i / ratePerSecond;
        int pair = i / 2;
        boolean response = i % 2 == 1 && pair % 2 == 0;
        String tid = Integer.toString(pair & 0xFFFF);
        return event(0, ts, i, response ? ModbusDirection.RESPONSE : ModbusDirection.REQUEST,
            (pair % 3) + 3, tid, 40001.0 + (pair % 32), 2.0, response);
    }

    // A gap from a mix of shapes: mostly sub-second microsecond gaps, plus
    // edge gaps, mid-range in-segment gaps, repeated timestamps, and the odd
    // negative (out-of-order) gap.
    private static double nextTs(Random random, double ts) {
        int shape = random.nextInt(100);
        if (shape < 55) {
            return micros(ts + random.nextInt(300_000) / 1e6);
        }
        if (shape < 75) {
            double gap = EDGE_GAPS[random.nextInt(EDGE_GAPS.length)];
            return gap == Math.rint(gap) ? ts + gap : micros(ts + gap);
        }
        if (shape < 85) {
            return micros(ts + 1.0 + random.nextInt(13_000_000) / 1e6);
        }
        if (shape < 88) {
            return micros(ts - (1 + random.nextInt(2_000_000)) / 1e6);
        }
        if (shape < 93) {
            return ts;
        }
        return micros(ts + 1e-6);
    }

    private static ModbusEvent randomEvent(Random random, int key, double ts, int index) {
        boolean response = random.nextBoolean();
        String tid = Integer.toString(random.nextInt(TID_POOL));
        int functionCode = FUNCTION_CODES[random.nextInt(FUNCTION_CODES.length)];
        Double address = random.nextInt(4) == 0 ? null : ADDRESSES[random.nextInt(ADDRESSES.length)];
        Double quantity = random.nextInt(3) == 0 ? null : QUANTITIES[random.nextInt(QUANTITIES.length)];
        double[] requestValues = values(random);
        double[] responseValues = response ? values(random) : new double[0];
        return new ModbusEvent(envelope(ts, index), ts,
            response ? ModbusDirection.RESPONSE : ModbusDirection.REQUEST,
            "10.0.0.5", "10.0.0.9", functionCode, tid, Integer.toString(key), address, quantity,
            response && random.nextBoolean(), requestValues, responseValues);
    }

    private static double[] values(Random random) {
        if (random.nextInt(3) != 0) {
            return new double[0];
        }
        double[] values = new double[1 + random.nextInt(4)];
        for (int i = 0; i < values.length; i++) {
            values[i] = random.nextInt(1000);
        }
        return values;
    }

    static ModbusEvent event(int key, double ts, int index, ModbusDirection direction, int functionCode,
                             String tid, Double address, Double quantity, boolean matched) {
        return new ModbusEvent(envelope(ts, index), ts, direction, "10.0.0.5", "10.0.0.9", functionCode, tid,
            Integer.toString(key), address, quantity, matched, new double[0], new double[0]);
    }

    // Rounds to whole microseconds, the resolution Zeek's JSON writer emits.
    private static double micros(double ts) {
        return Math.round(ts * 1e6) / 1e6;
    }

    private static EventEnvelope envelope(double ts, int index) {
        String uid = "u-" + index;
        long seconds = (long) Math.floor(ts);
        long nanos = Math.round((ts - seconds) * 1_000_000_000.0);
        if (nanos >= 1_000_000_000L) {
            seconds += 1;
            nanos -= 1_000_000_000L;
        }
        return new EventEnvelope(EventId.derive(SENSOR, uid), Instant.ofEpochSecond(seconds, nanos), SENSOR,
            LogType.MODBUS, uid);
    }
}
