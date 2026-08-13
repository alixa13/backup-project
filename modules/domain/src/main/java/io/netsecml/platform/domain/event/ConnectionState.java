package io.netsecml.platform.domain.event;

import java.util.Set;

public enum ConnectionState {
    SF, S0, REJ, RSTO, RSTR, SH, OTH, OTHER;

    private static final Set<ConnectionState> FAILED = Set.of(S0, REJ, RSTO, RSTR);

    public boolean isFailed() {
        return FAILED.contains(this);
    }

    public static ConnectionState fromZeekValue(String raw) {
        if (raw == null || raw.isBlank()) {
            return OTHER;
        }
        return switch (raw.trim().toUpperCase()) {
            case "SF" -> SF;
            case "S0" -> S0;
            case "REJ" -> REJ;
            case "RSTO" -> RSTO;
            case "RSTR" -> RSTR;
            case "SH" -> SH;
            case "OTH" -> OTH;
            default -> OTHER;
        };
    }
}
