package io.netsecml.platform.adapter.kafka.sink;

import java.util.Arrays;

public record RejectedRecordPayload(byte[] rawPayload, String reasonCode, String detail) {
    public RejectedRecordPayload {
        rawPayload = Arrays.copyOf(rawPayload, rawPayload.length);
    }

    @Override
    public byte[] rawPayload() {
        return Arrays.copyOf(rawPayload, rawPayload.length);
    }
}
