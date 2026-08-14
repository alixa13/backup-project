package io.netsecml.platform.adapter.flink.process;

import io.netsecml.platform.domain.event.ReasonCode;
import java.util.Arrays;

public record RejectedRecord(byte[] rawPayload, ReasonCode reason, String detail) {
    public RejectedRecord {
        rawPayload = Arrays.copyOf(rawPayload, rawPayload.length);
    }

    @Override
    public byte[] rawPayload() {
        return Arrays.copyOf(rawPayload, rawPayload.length);
    }
}
