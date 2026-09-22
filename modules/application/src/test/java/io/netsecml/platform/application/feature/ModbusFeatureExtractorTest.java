package io.netsecml.platform.application.feature;

import io.netsecml.platform.domain.event.EventEnvelope;
import io.netsecml.platform.domain.event.EventId;
import io.netsecml.platform.domain.event.LogType;
import io.netsecml.platform.domain.event.ModbusEvent;
import io.netsecml.platform.domain.event.ModbusEvent.ModbusDirection;
import io.netsecml.platform.domain.event.SensorId;
import io.netsecml.platform.domain.feature.ModbusEntityState;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

// Pins process_capture's per-index rules (two-models-info/modbus_/
// 07b_materialize_feature_engine_v1.py) against ModbusFeatureExtractor.extract,
// one behaviour per test rather than one giant golden-vector assertion, so a
// future regression names the exact rule it broke instead of just "the vector
// changed".
class ModbusFeatureExtractorTest {
    private static final SensorId SENSOR = new SensorId("sensor-eu-1");
    private final ModbusFeatureExtractor extractor = new ModbusFeatureExtractor();

    // ts -> Instant, matched by ModbusFeatureExtractor's own epochSeconds(Instant)
    // so a fractional ts (e.g. 1000.25) round-trips exactly through nanos rather
    // than losing precision the way a millis-only conversion would.
    private static Instant instantOf(double ts) {
        long seconds = (long) Math.floor(ts);
        long nanos = Math.round((ts - seconds) * 1_000_000_000.0);
        return Instant.ofEpochSecond(seconds, nanos);
    }

    private EventEnvelope envelope(double ts) {
        String uid = "u-" + ts;
        return new EventEnvelope(EventId.derive(SENSOR, uid), instantOf(ts), SENSOR, LogType.MODBUS, uid);
    }

    private ModbusEvent request(double ts, int functionCode, String tid) {
        return new ModbusEvent(envelope(ts), ModbusDirection.REQUEST, "10.0.0.5", "10.0.0.9",
            functionCode, tid, "1", null, null, false, new double[0], new double[0]);
    }

    private ModbusEvent requestAgainst(double ts, int functionCode, String tid, ModbusEntityState before) {
        return request(ts, functionCode, tid);
    }

    private ModbusEvent requestWithValues(double[] values) {
        return new ModbusEvent(envelope(1000.0), ModbusDirection.REQUEST, "10.0.0.5", "10.0.0.9",
            3, "17", "1", null, null, false, values, new double[0]);
    }

    private ModbusEvent requestWithAddress(double ts, double address, ModbusEntityState before) {
        return new ModbusEvent(envelope(ts), ModbusDirection.REQUEST, "10.0.0.5", "10.0.0.9",
            3, "99", "1", address, null, false, new double[0], new double[0]);
    }

    private ModbusEvent response(double ts, int functionCode, String tid, ModbusEntityState before) {
        boolean matched = before.hasPending(tid);
        return new ModbusEvent(envelope(ts), ModbusDirection.RESPONSE, "10.0.0.9", "10.0.0.5",
            functionCode, tid, "1", null, null, matched, new double[0], new double[0]);
    }

    private float[] extractFor(ModbusEvent event) {
        ModbusEntityState before = ModbusEntityState.empty();
        return extractFor(event, before);
    }

    private float[] extractFor(ModbusEvent event, ModbusEntityState before) {
        double ts = event.envelope().eventTime().getEpochSecond()
            + event.envelope().eventTime().getNano() / 1_000_000_000.0;
        ModbusEntityState after = before.afterEvent(ts, event.functionCode(), event.transactionId(),
            event.direction(), event.address(), event.quantity());
        boolean newSegment = before.lastTs() == null;
        return extractor.extract(event, before, after, newSegment);
    }

    private float[] extractAtSegmentStart(ModbusEvent event) {
        return extractFor(event, ModbusEntityState.empty());
    }

    @Test
    void theVectorIsExactlyFortyTwoValues() {
        assertEquals(42, extractFor(request(1000.0, 3, "17")).length);
    }

    @Test
    void exactlyOneFunctionIndicatorIsSetForEveryFunctionCode() {
        for (int code : new int[] {1, 2, 3, 4, 5, 6, 7, 23, 43, 99}) {
            float[] v = extractFor(request(1000.0, code, "17"));
            float sum = 0f;
            for (int i = 1; i <= 7; i++) {
                sum += v[i];
            }
            assertEquals(1.0f, sum, "exactly one of fc_1..fc_other must be set for code " + code);
        }
    }

    @Test
    void anUnenumeratedFunctionCodeSetsFcOther() {
        float[] v = extractFor(request(1000.0, 23, "17"));
        assertEquals(1.0f, v[7]);
    }

    @Test
    void aRequestNeverCarriesResponseMatchedOrRtt() {
        float[] v = extractFor(request(1000.0, 3, "17"));
        assertEquals(0.0f, v[12], "response_matched is response-only");
        assertEquals(0.0f, v[33], "rtt_valid is response-only");
        assertEquals(0.0f, v[34], "rtt_s is response-only");
    }

    @Test
    void aResponseWithoutAPendingRequestIsFlagged() {
        float[] v = extractFor(response(1000.0, 3, "17", ModbusEntityState.empty()));
        assertEquals(1.0f, v[31]);
        assertEquals(0.0f, v[33], "an unmatched response has no valid rtt");
    }

    @Test
    void aMatchedResponseCarriesItsRoundTripTime() {
        ModbusEntityState before = ModbusEntityState.empty()
            .afterEvent(1000.0, 3, "17", ModbusDirection.REQUEST, null, null);
        float[] v = extractFor(response(1000.25, 3, "17", before), before);
        assertEquals(0.0f, v[31], "it had a pending request");
        assertEquals(1.0f, v[33]);
        assertEquals(0.25f, v[34], 1e-6f);
    }

    @Test
    void aRequestReusingAPendingTidIsFlagged() {
        ModbusEntityState before = ModbusEntityState.empty()
            .afterEvent(1000.0, 3, "17", ModbusDirection.REQUEST, null, null);
        float[] v = extractFor(requestAgainst(1000.5, 3, "17", before), before);
        assertEquals(1.0f, v[32]);
    }

    @Test
    void emptyValueArraysLeaveAllFiveSummariesAtZero() {
        // The upstream engine only writes the summaries when the list is non-empty,
        // so *_present stays 0 too -- an empty array is "absent", not "present, count 0".
        float[] v = extractFor(request(1000.0, 3, "17"));
        for (int i = 13; i <= 22; i++) {
            assertEquals(0.0f, v[i], "index " + i + " must stay zero for absent values");
        }
    }

    @Test
    void requestValueSummariesAreCountMinMaxMean() {
        float[] v = extractFor(requestWithValues(new double[] {4.0, 8.0, 6.0}));
        assertEquals(1.0f, v[13]);
        assertEquals(3.0f, v[14]);
        assertEquals(4.0f, v[15]);
        assertEquals(8.0f, v[16]);
        assertEquals(6.0f, v[17], 1e-6f);
    }

    @Test
    void aSegmentStartZeroesEveryCausalFeature() {
        float[] v = extractAtSegmentStart(request(1000.0, 3, "17"));
        for (int i : new int[] {23, 24, 25, 26, 28, 30, 33, 34}) {
            assertEquals(0.0f, v[i], "index " + i + " must be zero at a segment start");
        }
    }

    @Test
    void theReadAndWriteRatiosDivideByTheTenSecondWindowCountNotByTen() {
        // Two events in the window, one of them a read: the ratio is 1/2, not 1/10.
        ModbusEntityState before = ModbusEntityState.empty()
            .afterEvent(1000.0, 5, "16", ModbusDirection.REQUEST, null, null);
        float[] v = extractFor(requestAgainst(1000.5, 3, "17", before), before);
        assertEquals(0.5f, v[40], 1e-6f);
        assertEquals(0.5f, v[41], 1e-6f);
    }

    @Test
    void theCurrentEventIsCountedInItsOwnWindows() {
        float[] v = extractFor(request(1000.0, 3, "17"));
        assertEquals(1.0f, v[35], "event_rate_1s is a raw count including this event");
        assertEquals(0.1f, v[36], 1e-6f, "one event in the 10s window is 1/10");
        assertEquals(1.0f / 60.0f, v[37], 1e-6f);
    }

    @Test
    void theAddressDeltaReachesPastAnEventThatCarriedNoAddress() {
        ModbusEntityState before = ModbusEntityState.empty()
            .afterEvent(1000.0, 3, "16", ModbusDirection.REQUEST, 40001.0, null)
            .afterEvent(1000.5, 3, "17", ModbusDirection.REQUEST, null, null);
        float[] v = extractFor(requestWithAddress(1001.0, 40005.0, before), before);
        assertEquals(1.0f, v[26]);
        assertEquals(4.0f, v[27], 1e-6f);
    }

    // Guards fix-round-1's F1: newSegment=true must reject a non-empty
    // `before` with a real runtime check, not a Java `assert` -- Surefire
    // enables assertions for this module's own tests, but nothing in either
    // bootstrap module runs its JVM with -ea, so an `assert` would be inert
    // in production. This is the one test in the unit whose whole point is
    // behaviour the default JVM configuration (assertions off) would hide;
    // see this class's own javadoc-adjacent note in the task's fix report
    // for the two-run proof (assertions disabled: fails; assertions'
    // irrelevant, throw restored: passes).
    @Test
    void newSegmentWithANonEmptyBeforeStateIsRejected() {
        ModbusEntityState nonEmptyBefore = ModbusEntityState.empty()
            .afterEvent(1000.0, 3, "17", ModbusDirection.REQUEST, null, null);
        ModbusEvent event = request(1000.5, 3, "18");
        ModbusEntityState after = nonEmptyBefore.afterEvent(1000.5, 3, "18", ModbusDirection.REQUEST, null, null);

        assertThrows(IllegalArgumentException.class,
            () -> extractor.extract(event, nonEmptyBefore, after, true));
    }
}
