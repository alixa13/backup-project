package io.netsecml.platform.domain.feature;

import io.netsecml.platform.domain.event.LogType;
import io.netsecml.platform.domain.event.SensorId;
import java.time.Instant;
import java.util.Arrays;
import java.util.Objects;

// One scored-ready feature vector: the frozen 20 float32 values plus the
// envelope that identifies and dates them.
//
// sensor makes an archived row self-sufficient for training without joining to
// the optional network_events table. producedAt is the emission timestamp and
// becomes the ClickHouse row_version, so "last emission wins" on replay.
// logType records which Zeek log produced this vector, and connectionUid carries
// Zeek's cross-protocol correlation key (a correlation key only, never an
// identity — see LogType and NetworkEvent). None of these fields affects the
// 20 values, their order, or the frozen schema hash.
public record FeatureVector(String eventId, Instant eventTime, SensorId sensor, LogType logType,
                             String connectionUid, String schemaId, String schemaHash, float[] values,
                             int qualityFlags, Instant producedAt) {
    public FeatureVector {
        // Identity must be present — it is the ClickHouse ORDER BY key tail.
        if (eventId == null || eventId.isBlank()) {
            throw new IllegalArgumentException("eventId must not be blank");
        }
        if (values == null) {
            throw new IllegalArgumentException("values must not be null");
        }

        // Both new components are structural; a row cannot be archived without them.
        Objects.requireNonNull(sensor, "sensor must not be null");
        Objects.requireNonNull(producedAt, "producedAt must not be null");

        // logType is structural too: every archived row must say which Zeek log
        // produced it.
        Objects.requireNonNull(logType, "logType must not be null");

        // A log type carrying no correlation uid (Modbus/S7comm may not) yields ""
        // rather than null, matching the convention RejectedEvent.eventId already uses.
        connectionUid = connectionUid == null ? "" : connectionUid;

        // Defensive copy in: the caller keeps no handle on our internal array.
        values = Arrays.copyOf(values, values.length);
    }

    // Defensive copy out: callers cannot mutate our internal array either.
    @Override
    public float[] values() {
        return Arrays.copyOf(values, values.length);
    }
}
