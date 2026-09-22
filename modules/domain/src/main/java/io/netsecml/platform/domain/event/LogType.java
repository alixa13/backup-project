package io.netsecml.platform.domain.event;

import java.util.Locale;

// Which Zeek log produced an event: conn.log, ssh.log, dns.log and so on.
//
// This is NOT the transport protocol. `Protocol` (TCP/UDP/ICMP/OTHER) is a
// different concept in this same package, and it is frozen into conn-feature-v1
// as features 11-12 — the two must never be conflated.
//
// CONN, DNS and MODBUS exist because each has a DTO (or event record), a
// mapper and a feature schema -- except MODBUS's mapper, which is a later
// task in this same unit still in progress; see NetworkEvent's javadoc for
// why the record still joins the sealed hierarchy ahead of it. A constant is
// added per log type as each one lands; Java should not advertise support
// that has nothing behind it. DNS was the second constant this enum ever
// had, MODBUS the third -- see NetworkEvent's javadoc for what adding each
// one did to the sealed hierarchy's switches.
public enum LogType {
    CONN, DNS, MODBUS;

    // The lowercase form used on the Kafka wire and in ClickHouse's log_type
    // column, matching Zeek's own log naming.
    public String wireName() {
        return name().toLowerCase(Locale.ROOT);
    }
}
