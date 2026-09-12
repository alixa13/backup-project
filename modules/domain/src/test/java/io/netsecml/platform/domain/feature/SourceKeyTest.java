package io.netsecml.platform.domain.feature;

import io.netsecml.platform.domain.event.LogType;
import io.netsecml.platform.domain.event.SensorId;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class SourceKeyTest {
    // The reason logType is in the key rather than merely alongside it: a host
    // that both browses and resolves names produces conn and dns records with the
    // same sensor and the same source IP. Without logType they would share one
    // rolling window, and DNS's record_count_5m would silently include the host's
    // connections. Same sensor, same IP, different log -- different key.
    @Test
    void keysDifferByLogTypeAloneSoProtocolsDoNotShareAWindow() {
        SensorId sensor = new SensorId("sensor-eu-1");
        SourceKey connKey = new SourceKey(sensor, LogType.CONN, "10.0.0.5");
        SourceKey dnsKey = new SourceKey(sensor, LogType.DNS, "10.0.0.5");

        assertNotEquals(connKey, dnsKey);
        assertNotEquals(connKey.hashCode(), dnsKey.hashCode(),
            "a hash collision here would not be wrong, but these must not be equal");
    }
}
