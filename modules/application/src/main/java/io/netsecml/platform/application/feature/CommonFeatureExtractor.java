package io.netsecml.platform.application.feature;

import io.netsecml.platform.domain.feature.CommonFeatureTierV1;
import io.netsecml.platform.domain.feature.ConnSnapshotDelta;
import io.netsecml.platform.domain.feature.ConnWindowState;
import io.netsecml.platform.domain.feature.RecordTimingState;

// Builds the protocol-agnostic feature tier that leads every per-protocol
// feature vector.
//
// Indices here are the frozen positions declared in CommonFeatureTierV1, and
// every per-protocol schema appends its own features after FEATURE_COUNT. An
// index error in this class therefore misaligns every protocol at once, which
// is why each position is asserted individually in the tests.
public final class CommonFeatureExtractor {

    // Non-instantiable: every member is static.
    private CommonFeatureExtractor() {
    }

    // enrichmentDelta is NULLABLE and null means "no conn.log snapshot available
    // for this connection yet". The join is deliberately non-blocking: a
    // connection's first snapshot does not exist until it has been alive five
    // minutes, and waiting for it would stall every record from a new connection.
    public static float[] extract(ConnWindowState window, RecordTimingState timing,
                                  boolean isOrig, ConnSnapshotDelta enrichmentDelta) {
        if (window == null || timing == null) {
            throw new IllegalArgumentException("window and timing must not be null");
        }

        float[] values = new float[CommonFeatureTierV1.FEATURE_COUNT];

        // Indices 0-2: the rolling window this platform already maintains per
        // (sensor, sourceIp). Always populated.
        values[0] = window.connectionCount5m();
        values[1] = window.byteSum5m();
        values[2] = window.failedCount5m();

        // Indices 3-4: inter-arrival shape. A steady poll loop and a burst have
        // the same record count but very different deviation.
        values[3] = (float) timing.meanIntervalMillis();
        values[4] = (float) timing.stddevIntervalMillis();

        // Index 5: direction, as a flag rather than a count.
        values[5] = isOrig ? 1.0f : 0.0f;

        // Indices 6-11: conn.log enrichment. Absent is not an error -- it is the
        // expected state for the first five minutes of every connection -- so the
        // features default to zero and index 11 records that they were not
        // measured. Without that flag an idle connection and a missing snapshot
        // would be indistinguishable to the model.
        if (enrichmentDelta == null) {
            values[11] = 0.0f;
            return values;
        }

        values[6] = enrichmentDelta.origBytes();
        values[7] = enrichmentDelta.respBytes();
        values[8] = enrichmentDelta.origPkts();
        values[9] = enrichmentDelta.respPkts();
        values[10] = enrichmentDelta.ageSeconds();
        values[11] = 1.0f;
        return values;
    }
}
