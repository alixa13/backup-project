package io.netsecml.platform.domain.inference;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

// upstream's operation_groups, read from the vector: is_request_direction
// (index 12) and the s7_operation code (index 15).
class S7commScoreGroupTest {

    private static float[] vector(boolean request, float operation) {
        float[] v = new float[16];
        v[12] = request ? 1f : 0f;
        v[15] = operation;
        return v;
    }

    @Test
    void aResponseIsResponseWhateverItsFunction() {
        assertEquals(S7commScoreGroup.RESPONSE, S7commScoreGroup.of(vector(false, 4f)));
        assertEquals(S7commScoreGroup.RESPONSE, S7commScoreGroup.of(vector(false, 5f)));
    }

    @Test
    void aRequestIsGroupedByItsFunction() {
        assertEquals(S7commScoreGroup.READ_REQUEST, S7commScoreGroup.of(vector(true, 4f)));
        assertEquals(S7commScoreGroup.WRITE_REQUEST, S7commScoreGroup.of(vector(true, 5f)));
        assertEquals(S7commScoreGroup.OTHER_REQUEST, S7commScoreGroup.of(vector(true, 0xF0)));
        assertEquals(S7commScoreGroup.OTHER_REQUEST, S7commScoreGroup.of(vector(true, 0f)), "user data (S2)");
        assertEquals(S7commScoreGroup.OTHER_REQUEST, S7commScoreGroup.of(vector(true, -1f)), "no function");
    }
}
