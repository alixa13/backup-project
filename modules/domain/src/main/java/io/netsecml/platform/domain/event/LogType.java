package io.netsecml.platform.domain.event;

import java.util.Locale;

// Which Zeek log produced an event: conn.log, ssh.log, dns.log and so on.
//
// This is NOT the transport protocol. `Protocol` (TCP/UDP/ICMP/OTHER) is a
// different concept in this same package, and it is frozen into conn-feature-v1
// as features 11-12 — the two must never be conflated.
//
// Only CONN exists because only conn has a DTO, a mapper and a feature schema.
// A constant is added per log type as each one lands; Java should not advertise
// support that has nothing behind it.
public enum LogType {
    CONN;

    // The lowercase form used on the Kafka wire and in ClickHouse's log_type
    // column, matching Zeek's own log naming.
    public String wireName() {
        return name().toLowerCase(Locale.ROOT);
    }
}
