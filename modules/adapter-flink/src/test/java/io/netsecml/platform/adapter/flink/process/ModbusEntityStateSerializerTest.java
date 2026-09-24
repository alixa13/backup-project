package io.netsecml.platform.adapter.flink.process;

import io.netsecml.platform.application.usecase.ModbusBuildFeaturesUseCase;
import io.netsecml.platform.domain.event.EventEnvelope;
import io.netsecml.platform.domain.event.EventId;
import io.netsecml.platform.domain.event.LogType;
import io.netsecml.platform.domain.event.ModbusEvent;
import io.netsecml.platform.domain.event.ModbusEvent.ModbusDirection;
import io.netsecml.platform.domain.event.SensorId;
import io.netsecml.platform.domain.feature.ModbusEntityState;
import org.apache.flink.api.common.serialization.SerializerConfigImpl;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

// ModbusEntityState is mutable and updated in place, which is safe in the
// heap state backend only because Flink hands out a serializer COPY of a
// state object a running checkpoint still holds (see ModbusEntityState's own
// comment). This pins the two serializer properties that argument rests on,
// against the exact serializer Flink resolves for the ValueState: a copy
// shares nothing mutable with its original, and a serialized round trip
// (checkpoint, then restore) loses nothing the next event reads -- including
// the running 10 s counts, which, unlike the windows, are not recomputed
// from anything.
//
// The serializer comes from a default SerializerConfigImpl, which is what the
// online job actually runs with: it configures no serialization in code. A
// cluster-level serialization config registering a shallow-copying Kryo
// serializer for these collections would void this test's premise -- and
// with it the copy-on-write safety argument -- without failing it.
class ModbusEntityStateSerializerTest {

    private static final SensorId SENSOR = new SensorId("sensor-eu-1");

    private final TypeSerializer<ModbusEntityState> serializer =
        TypeInformation.of(ModbusEntityState.class).createSerializer(new SerializerConfigImpl());
    private final ModbusBuildFeaturesUseCase useCase =
        new ModbusBuildFeaturesUseCase(Clock.fixed(Instant.parse("2026-09-24T00:00:00Z"), ZoneOffset.UTC));

    private static ModbusEvent event(double ts, ModbusDirection direction, int functionCode, String tid,
                                     Double address, Double quantity) {
        String uid = "u-" + ts;
        long seconds = (long) Math.floor(ts);
        Instant instant = Instant.ofEpochSecond(seconds, Math.round((ts - seconds) * 1_000_000_000.0));
        return new ModbusEvent(new EventEnvelope(EventId.derive(SENSOR, uid), instant, SENSOR, LogType.MODBUS, uid),
            ts, direction, "10.0.0.5", "10.0.0.9", functionCode, tid, "1", address, quantity,
            direction == ModbusDirection.RESPONSE, new double[0], new double[0]);
    }

    // Events whose state touches every field: all three windows, both running
    // count maps with repeated and distinct keys, both tallies (FC 23 counts
    // in both), last address/quantity, and two tids still pending.
    private static final ModbusEvent[] HISTORY = {
        event(1000.0, ModbusDirection.REQUEST, 3, "1", 40001.0, 2.0),
        event(1000.4, ModbusDirection.RESPONSE, 3, "1", 40001.0, 2.0),
        event(1003.0, ModbusDirection.REQUEST, 23, "2", 40002.0, 4.0),
        event(1006.0, ModbusDirection.REQUEST, 6, "3", 40001.0, null),
        event(1009.5, ModbusDirection.REQUEST, 16, "4", null, 8.0),
    };

    // What follows HISTORY: responses to the pending tids, and events far
    // enough on that the 10 s window purges HISTORY's entries one by one, so
    // every running count is decremented from its restored value.
    private static final ModbusEvent[] CONTINUATION = {
        event(1010.2, ModbusDirection.RESPONSE, 23, "2", 40002.0, 4.0),
        event(1012.0, ModbusDirection.REQUEST, 3, "5", 40003.0, 1.0),
        event(1014.1, ModbusDirection.RESPONSE, 16, "4", null, null),
        event(1016.5, ModbusDirection.REQUEST, 3, "6", 40001.0, 2.0),
        event(1019.9, ModbusDirection.RESPONSE, 6, "3", 40001.0, null),
    };

    private ModbusEntityState stateAfterHistory() {
        return stateAfterHistory(ModbusEntityState.empty());
    }

    private ModbusEntityState stateAfterHistory(ModbusEntityState state) {
        for (ModbusEvent event : HISTORY) {
            state = useCase.build(event, state).newState();
        }
        return state;
    }

    // Every value and quality flag the continuation produces from `state`, in
    // order, one row per event: its 42 values, then its flags as a 43rd float
    // (small integers are exact in a float).
    private float[][] continuationVectors(ModbusEntityState state) {
        float[][] vectors = new float[CONTINUATION.length][];
        for (int i = 0; i < CONTINUATION.length; i++) {
            var result = useCase.build(CONTINUATION[i], state);
            float[] values = result.vector().values();
            vectors[i] = Arrays.copyOf(values, values.length + 1);
            vectors[i][values.length] = result.vector().qualityFlags();
            state = result.newState();
        }
        return vectors;
    }

    @Test
    void theStateResolvesToKryo() {
        // The class comment's claim, and the reason these two properties need
        // their own test: Kryo, not the POJO serializer, does the copying.
        assertInstanceOf(GenericTypeInfo.class, TypeInformation.of(ModbusEntityState.class));
    }

    @Test
    void advancingASerializerCopyLeavesTheOriginalUntouched() {
        ModbusEntityState original = stateAfterHistory();
        ModbusEntityState copy = serializer.copy(original);

        // Mutate the copy exactly as an operator would after a copy-on-write
        // hand-out: windows purged, counts decremented, pending entries removed.
        continuationVectors(copy);

        // The original must still produce what an untouched state produces.
        assertArrayEquals(continuationVectors(stateAfterHistory()), continuationVectors(original));
    }

    @Test
    void aSerializedRoundTripPreservesEverythingTheNextEventsRead() throws IOException {
        ModbusEntityState restored = roundTrip(stateAfterHistory());
        assertArrayEquals(continuationVectors(stateAfterHistory()), continuationVectors(restored));
    }

    @Test
    void aCappedSaturatedStateKeepsItsCapAndSaturationThroughARoundTrip() throws IOException {
        // Cap 3 against HISTORY's five events inside 10 s: the windows have
        // evicted entries still inside them, so the state is saturated. A
        // restore that lost the cap or the eviction timestamps would change the
        // continuation's window values or its MODBUS_WINDOW_SATURATED bits.
        ModbusEntityState original = stateAfterHistory(ModbusEntityState.emptyWithWindowCap(3));
        assertTrue(original.windowSaturated());

        ModbusEntityState restored = roundTrip(original);
        assertTrue(restored.windowSaturated());
        assertArrayEquals(continuationVectors(stateAfterHistory(ModbusEntityState.emptyWithWindowCap(3))),
            continuationVectors(restored));
    }

    private ModbusEntityState roundTrip(ModbusEntityState state) throws IOException {
        DataOutputSerializer out = new DataOutputSerializer(1024);
        serializer.serialize(state, out);
        return serializer.deserialize(new DataInputDeserializer(out.getCopyOfBuffer()));
    }
}
