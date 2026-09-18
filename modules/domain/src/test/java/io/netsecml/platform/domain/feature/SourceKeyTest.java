package io.netsecml.platform.domain.feature;

import io.netsecml.platform.domain.event.LogType;
import io.netsecml.platform.domain.event.SensorId;
import org.junit.jupiter.api.Test;
import java.util.Objects;
import static org.junit.jupiter.api.Assertions.*;

class SourceKeyTest {
    // The reason logType is in the key rather than merely alongside it: a host
    // that both browses and resolves names produces conn and dns records with the
    // same sensor and the same source IP. Without logType they would share one
    // rolling window, and DNS's record_count_5m would silently include the host's
    // connections. Same sensor, same IP, different log -- different key.
    //
    // The name says "keys differ", not "windows are isolated", because key
    // inequality is all this asserts. Key inequality is the MECHANISM by which
    // the windows stay separate; the isolation itself is structural, following
    // from logType being one of the record's components (equals() compares
    // every component), not something a particular test harness exercises.
    @Test
    void keysDifferByLogTypeAlone() {
        SensorId sensor = new SensorId("sensor-eu-1");
        SourceKey connKey = new SourceKey(sensor, LogType.CONN, "10.0.0.5");
        SourceKey dnsKey = new SourceKey(sensor, LogType.DNS, "10.0.0.5");

        assertNotEquals(connKey, dnsKey);
    }

    // Pins the FORMULA hashCode() must compute, not a magic int:
    // Objects.hash(sensor.value(), logType.name(), sourceIp) -- see the
    // reasoning on SourceKey.hashCode() itself. A test running in a single JVM
    // cannot observe the cross-JVM instability the override fixes directly (the
    // whole defect is that Enum.hashCode() differs BETWEEN JVMs, and a test only
    // ever runs in one), so pinning the formula -- rather than merely asserting
    // two calls agree with each other -- is what keeps the override honest:
    // changing any of the three inputs, or the formula itself, fails this test
    // even though both would still pass a same-JVM self-equality check.
    @Test
    void hashCodeMatchesTheDeclaredFormula() {
        SourceKey key = new SourceKey(new SensorId("sensor-eu-1"), LogType.DNS, "10.0.0.5");

        assertEquals(Objects.hash("sensor-eu-1", "DNS", "10.0.0.5"), key.hashCode());
    }

    // "Build the same key twice, assert equal hashes" would pass even with the
    // ORIGINAL defect -- Enum.hashCode()'s identity hash is stable WITHIN one
    // JVM run, so same-JVM equality proves nothing about this fix. What DOES
    // distinguish the fix from the defect, still inside a single JVM, is which
    // formula produced the number: this checks the real hashCode() against
    // Objects.hash(sensor, logType.NAME, ip) for BOTH enum constants, so a
    // regression back to hashing logType itself (its identity hash, not its
    // name) fails here even though it would not fail a bare
    // equals-itself check.
    @Test
    void hashCodeDoesNotUseTheLogTypeEnumsIdentityHash() {
        SensorId sensor = new SensorId("sensor-eu-1");
        SourceKey connKey = new SourceKey(sensor, LogType.CONN, "10.0.0.5");
        SourceKey dnsKey = new SourceKey(sensor, LogType.DNS, "10.0.0.5");

        assertEquals(Objects.hash("sensor-eu-1", "CONN", "10.0.0.5"), connKey.hashCode());
        assertEquals(Objects.hash("sensor-eu-1", "DNS", "10.0.0.5"), dnsKey.hashCode());
    }
}
