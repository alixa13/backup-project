package io.netsecml.platform.domain.feature;

import io.netsecml.platform.domain.event.ModbusEvent;
import io.netsecml.platform.domain.event.SensorId;
import java.util.Objects;

// The key every per-modbus-entity causal state (the request/response
// tracking the upstream engine keys as (client_ip, server_ip, unit)) is
// grouped by.
//
// clientIp and serverIp are an ORIENTATION-NORMALIZED pair, not the raw
// sourceIp/destinationIp a ModbusEvent carries: ICSNPP logs a request and its
// matching response as two separate records, and the response's src/dst are
// the request's dst/src, swapped. Without normalization the two halves of one
// transaction would hash to different keys and never share state. The
// formula matches the upstream feature engine exactly
// (two-models-info/modbus_/07b_materialize_feature_engine_v1.py):
//
//   client_ip = (direction == request) ? sourceIp : destinationIp
//   server_ip = (direction == request) ? destinationIp : sourceIp
public record ModbusEntityKey(SensorId sensor, String clientIp, String serverIp, String unitId) {
    public ModbusEntityKey {
        Objects.requireNonNull(sensor, "sensor must not be null");
        if (clientIp == null || clientIp.isBlank()) {
            throw new IllegalArgumentException("clientIp must not be blank");
        }
        if (serverIp == null || serverIp.isBlank()) {
            throw new IllegalArgumentException("serverIp must not be blank");
        }
        Objects.requireNonNull(unitId, "unitId must not be null");
    }

    // Builds the key from a raw event, applying the same client/server
    // orientation normalization the upstream engine applies before keying:
    // a REQUEST's own source/destination are already client/server order, a
    // RESPONSE's are swapped back into it.
    public static ModbusEntityKey of(ModbusEvent event) {
        boolean isRequest = event.direction() == ModbusEvent.ModbusDirection.REQUEST;
        String clientIp = isRequest ? event.sourceIp() : event.destinationIp();
        String serverIp = isRequest ? event.destinationIp() : event.sourceIp();
        return new ModbusEntityKey(event.envelope().sensor(), clientIp, serverIp, event.unitId());
    }

    // Flink assigns this key to a key group with key.hashCode()
    // (KeyGroupRangeAssignment.assignToKeyGroup), so the hash must be the
    // same number for the same logical key in every JVM that computes it.
    // Every component here is already a String, so the record's generated
    // hashCode() would already be JVM-stable today -- but SourceKey shipped
    // this exact defect once via an enum component, because
    // Enum.hashCode() is final and returns Object's IDENTITY hash, not a
    // value the JLS pins down. Pinning the formula here, over sensor.value()
    // and the three String components, means a future edit that adds a
    // non-String component (an enum direction, say) fails loudly at the
    // hashCode() call site instead of silently reintroducing the bug.
    // equals() is left as the generated one: it is already
    // component-by-component and JVM-consistent, so it needs no change to
    // stay consistent with this hash.
    @Override
    public int hashCode() {
        return Objects.hash(sensor.value(), clientIp, serverIp, unitId);
    }
}
