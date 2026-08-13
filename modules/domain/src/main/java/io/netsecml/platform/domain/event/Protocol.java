package io.netsecml.platform.domain.event;

public enum Protocol {
    TCP, UDP, ICMP, OTHER;

    public static Protocol fromZeekValue(String raw) {
        if (raw == null || raw.isBlank()) {
            return OTHER;
        }
        return switch (raw.trim().toLowerCase()) {
            case "tcp" -> TCP;
            case "udp" -> UDP;
            case "icmp" -> ICMP;
            default -> OTHER;
        };
    }
}
