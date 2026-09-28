package io.netsecml.platform.domain.inference;

// The S7comm Stage 1 detector's calibration groups (upstream's operation_groups,
// debiased.py): a response, or a request by its function -- READ_VAR (4),
// WRITE_VAR (5) or anything else. Read from the vector: is_request_direction
// (index 12) and the s7_operation code (index 15), which equals the record's
// function code whenever it carried one (scoring design section 9, D5).
public enum S7commScoreGroup {
    RESPONSE, READ_REQUEST, WRITE_REQUEST, OTHER_REQUEST;

    private static final int IS_REQUEST = 12;
    private static final int OPERATION = 15;

    public static S7commScoreGroup of(float[] vector) {
        // upstream: is_request_direction.astype(int64) == 1.
        if (vector[IS_REQUEST] != 1f) {
            return RESPONSE;
        }
        float operation = vector[OPERATION];
        if (operation == 4f) {
            return READ_REQUEST;
        }
        if (operation == 5f) {
            return WRITE_REQUEST;
        }
        return OTHER_REQUEST;
    }
}
