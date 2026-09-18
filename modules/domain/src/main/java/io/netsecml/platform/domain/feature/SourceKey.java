package io.netsecml.platform.domain.feature;

import io.netsecml.platform.domain.event.LogType;
import io.netsecml.platform.domain.event.SensorId;
import java.util.Objects;

// The key every per-(sensor, source) rolling window is grouped by.
//
// logType is part of the KEY, not decoration: one host's conn records and its
// dns records must not accumulate into a shared window, or each protocol's
// record_count_5m would count the other's traffic.
//
// This record is also a Flink KEY TYPE -- SourceKeySelector hands it to
// keyBy(), so its shape feeds both key-group assignment and the bytes keyed
// state is serialized under. Adding, removing, or reordering a component
// changes those for every existing key, independent of any state name; unlike
// the state-name and Kryo-class-name breaks elsewhere in this unit, there is
// no name to keep here that would soften it. This change was accepted for the
// same reason as those: no checkpoint plausibly exists yet. A future editor
// adding a fourth component should know it carries that same cost again.
public record SourceKey(SensorId sensor, LogType logType, String sourceIp) {
    public SourceKey {
        if (sensor == null) {
            throw new IllegalArgumentException("sensor must not be null");
        }
        Objects.requireNonNull(logType, "logType must not be null");
        if (sourceIp == null || sourceIp.isBlank()) {
            throw new IllegalArgumentException("sourceIp must not be blank");
        }
    }

    // Flink assigns this key to a key group with key.hashCode()
    // (KeyGroupRangeAssignment.assignToKeyGroup, reached from both the sending
    // partitioner and the receiving setCurrentKey), so the hash must be the
    // same number for the same logical key in every JVM that computes it. The
    // record's generated hashCode() would fold in logType's own hashCode(),
    // but Enum.hashCode() is final and returns Object's IDENTITY hash -- a
    // number a JVM assigns per instance from its own allocation history, with
    // no guarantee of repeating across JVMs or even across restarts of the
    // same one. Hashing logType.name() instead sidesteps that: like
    // sensor.value() and sourceIp, it is a String, whose hashCode() the JLS
    // fixes to one formula everywhere, so the combined result here
    // (Objects.hash, itself specified by Arrays.hashCode(Object[])'s javadoc)
    // is identical on every JVM. equals() is left as the generated one: it
    // already compares logType by enum value, not identity, so it was already
    // JVM-consistent and needs no change to stay consistent with this hash.
    @Override
    public int hashCode() {
        return Objects.hash(sensor.value(), logType.name(), sourceIp);
    }
}
