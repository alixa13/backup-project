package io.netsecml.platform.application.feature;

import io.netsecml.platform.domain.event.ConnEvent;
import io.netsecml.platform.domain.event.Protocol;
import io.netsecml.platform.domain.event.ServiceCode;

// Narrowed to ConnEvent rather than the sealed NetworkEvent interface: every line
// below reads measurements() or connection(), so this class IS the conn
// extractor, not a generic one wearing a switch. A later unit adds per-log-type
// extractors beside this one rather than branches inside it.
public final class EventFeatureExtractor {
    public float[] extractEventLevel(ConnEvent event) {
        long durationMillis = event.measurements().durationMillis();
        long originBytes = event.measurements().originBytes();
        long responseBytes = event.measurements().responseBytes();
        int originPackets = event.measurements().originPackets();
        int responsePackets = event.measurements().responsePackets();
        long totalBytes = originBytes + responseBytes;
        int totalPackets = originPackets + responsePackets;
        int destinationPort = event.connection().destinationPort();
        Protocol protocol = event.connection().protocol();
        ServiceCode service = event.connection().service();

        return new float[]{
            durationMillis,
            originBytes,
            responseBytes,
            originPackets,
            responsePackets,
            totalBytes,
            totalPackets,
            (float) totalBytes / Math.max(1, totalPackets),
            (float) responseBytes / Math.max(1, originBytes),
            destinationPort,
            (destinationPort >= 1 && destinationPort <= 1023) ? 1f : 0f,
            protocol == Protocol.TCP ? 1f : 0f,
            protocol == Protocol.UDP ? 1f : 0f,
            service == ServiceCode.DNS ? 1f : 0f,
            service == ServiceCode.HTTP ? 1f : 0f,
            service == ServiceCode.SSL ? 1f : 0f,
            event.connection().connectionState().isFailed() ? 1f : 0f
        };
    }
}
