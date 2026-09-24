package io.netsecml.platform.domain.feature;

import io.netsecml.platform.domain.event.S7commEvent;
import io.netsecml.platform.domain.event.SensorId;

import java.util.Objects;

// The S7 connection state's key: (sensor, uid). Upstream keys its state by uid
// alone (one builder per capture); the sensor keeps two sensors that happen to
// report the same uid string apart. A request and its response share the uid,
// so they share one state -- which the endpoints could not guarantee, since
// they swap between the two.
public record S7commConnectionKey(SensorId sensor, String uid) {

    public S7commConnectionKey {
        Objects.requireNonNull(sensor, "sensor must not be null");
        if (uid == null || uid.isBlank()) {
            throw new IllegalArgumentException("uid must not be blank");
        }
    }

    // The key an event's state lives under.
    public static S7commConnectionKey of(S7commEvent event) {
        return new S7commConnectionKey(event.sensor(), event.connectionUid());
    }

    // Written out, as ModbusEntityKey's is: Flink picks a key's group from its
    // hashCode, and this one visibly depends only on two Strings' hashes,
    // which are the same on every JVM.
    @Override
    public int hashCode() {
        return Objects.hash(sensor.value(), uid);
    }
}
