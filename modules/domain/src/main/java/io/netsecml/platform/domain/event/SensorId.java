package io.netsecml.platform.domain.event;

import java.io.Serializable;

// Serializable because Flink captures a SensorId inside the job's operators and
// serializes them to ship to the TaskManagers. Without it the online job cannot
// be submitted at all: env.execute() fails with NotSerializableException before
// a single record is read. java.io is the JDK, not a framework, so this does not
// breach the domain module's no-framework-imports rule.
public record SensorId(String value) implements Serializable {
    public SensorId {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("SensorId must not be blank");
        }
    }

    @Override
    public String toString() {
        return value;
    }
}
