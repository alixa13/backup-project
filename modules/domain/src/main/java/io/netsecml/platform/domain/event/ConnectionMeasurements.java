package io.netsecml.platform.domain.event;

public record ConnectionMeasurements(long durationMillis, long originBytes, long responseBytes,
                                      int originPackets, int responsePackets, long missedBytes) {
    public ConnectionMeasurements {
        durationMillis = requireNonNegative(durationMillis, "durationMillis");
        originBytes = requireNonNegative(originBytes, "originBytes");
        responseBytes = requireNonNegative(responseBytes, "responseBytes");
        originPackets = (int) requireNonNegative(originPackets, "originPackets");
        responsePackets = (int) requireNonNegative(responsePackets, "responsePackets");
        missedBytes = requireNonNegative(missedBytes, "missedBytes");
    }

    private static long requireNonNegative(long value, String field) {
        if (value < 0) {
            throw new IllegalArgumentException(field + " must be non-negative, was " + value);
        }
        return value;
    }
}
