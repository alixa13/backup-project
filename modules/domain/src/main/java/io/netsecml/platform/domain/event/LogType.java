package io.netsecml.platform.domain.event;

import java.util.Locale;

// Which Zeek log produced an event: conn.log, ssh.log, dns.log and so on.
//
// This is NOT the transport protocol. `Protocol` (TCP/UDP/ICMP/OTHER) is a
// different concept in this same package, and it is frozen into conn-feature-v1
// as features 11-12 — the two must never be conflated.
//
// CONN, DNS and MODBUS exist because each has a DTO (or event record), a
// mapper and a feature schema. A constant is added per log type as each one
// lands; Java should not advertise support that has nothing behind it. DNS
// was the second constant this enum ever had, MODBUS the third -- see
// NetworkEvent's javadoc for what adding each one did to the sealed
// hierarchy's switches.
public enum LogType {
    CONN, DNS, MODBUS;

    // The lowercase form used on the Kafka wire and in ClickHouse's log_type
    // column. Matches Zeek's own log naming for CONN and DNS (conn.log,
    // dns.log); for MODBUS it is "modbus", the wire's own name for what is
    // actually ICSNPP's modbus_detailed.log, not base Zeek's own modbus.log
    // (see ModbusFeatureSchemaV1's javadoc for why only the detailed log has
    // the fields this platform's schema needs).
    public String wireName() {
        return name().toLowerCase(Locale.ROOT);
    }
}
