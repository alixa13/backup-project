package io.netsecml.platform.domain.event;

public enum ServiceCode {
    DNS, HTTP, SSL, UNKNOWN;

    public static ServiceCode fromZeekValue(String raw) {
        if (raw == null || raw.isBlank()) {
            return UNKNOWN;
        }
        return switch (raw.trim().toLowerCase()) {
            case "dns" -> DNS;
            case "http" -> HTTP;
            case "ssl" -> SSL;
            default -> UNKNOWN;
        };
    }
}
