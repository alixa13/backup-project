package io.netsecml.platform.adapter.flink.process;

import io.netsecml.platform.domain.feature.S7commConnectionState;
import org.apache.flink.api.common.serialization.SerializerConfigImpl;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

// The S7 state is mutable and updated in place, which is safe on Flink's heap
// backend only because a running checkpoint gets a serializer COPY (see
// ModbusEntityState's comment and ModbusEntityStateSerializerTest). This pins
// the two properties that argument needs, against the serializer Flink
// resolves from the online job's default serialization config: a copy shares
// nothing mutable with its original, and a round trip keeps everything the
// next events read -- the BitSet, the nine rings and the scalars.
class S7commConnectionStateSerializerTest {

    private final TypeSerializer<S7commConnectionState> serializer =
        TypeInformation.of(S7commConnectionState.class).createSerializer(new SerializerConfigImpl());

    // (isRequest, pdu, rosctr, function) for a history and a continuation that
    // matches, re-uses and wraps references and changes function and ROSCTR.
    private static final int[][] HISTORY = {
        {1, 1, 1, 4}, {1, 2, 1, 4}, {0, 1, 3, 4}, {1, 65535, 1, 5}, {1, 3, 7, 0x29}, {0, 9, 3, 4}};
    private static final int[][] CONTINUATION = {
        {0, 2, 3, 4}, {1, 0, 1, 4}, {0, 65535, 3, 5}, {1, 4, 1, 0x1A}, {0, 3, 3, 0x29}, {1, 5, 1, 4}};

    private static S7commConnectionState afterHistory() {
        S7commConnectionState state = S7commConnectionState.empty();
        apply(state, HISTORY);
        return state;
    }

    private static void apply(S7commConnectionState state, int[][] events) {
        for (int i = 0; i < events.length; i++) {
            int[] e = events[i];
            state.advance(1000.0 + i, e[0] == 1, e[1], e[2], e[3]);
        }
    }

    // Every accessor after each continuation event, as one flat row per event.
    private static double[][] continuationReads(S7commConnectionState state) {
        double[][] rows = new double[CONTINUATION.length][];
        for (int i = 0; i < CONTINUATION.length; i++) {
            int[] e = CONTINUATION[i];
            S7commConnectionState.Step step = state.advance(2000.0 + i, e[0] == 1, e[1], e[2], e[3]);
            rows[i] = new double[] {state.outstandingRequests(), state.outstandingMean16(),
                state.responseMatchRate16(), state.sameFunctionRunLength(), state.sameDirectionRunLength(),
                state.requestRatio16(), state.directionChangeRate16(), state.functionChangeRate16(),
                state.functionEntropy16(), state.functionTransitionEntropy16(), state.rosctrChangeRate16(),
                state.pduReferenceUniqueRatio32(), step.functionChanged() ? 1 : 0, step.outOfOrder() ? 1 : 0};
        }
        return rows;
    }

    @Test
    void theStateResolvesToKryo() {
        assertInstanceOf(GenericTypeInfo.class, TypeInformation.of(S7commConnectionState.class));
    }

    @Test
    void advancingASerializerCopyLeavesTheOriginalUntouched() {
        S7commConnectionState original = afterHistory();
        S7commConnectionState copy = serializer.copy(original);
        continuationReads(copy);
        assertArrayEquals(continuationReads(afterHistory()), continuationReads(original));
    }

    @Test
    void aSerializedRoundTripPreservesEverythingTheNextEventsRead() throws IOException {
        DataOutputSerializer out = new DataOutputSerializer(1024);
        serializer.serialize(afterHistory(), out);
        S7commConnectionState restored = serializer.deserialize(new DataInputDeserializer(out.getCopyOfBuffer()));
        assertArrayEquals(continuationReads(afterHistory()), continuationReads(restored));
    }
}
