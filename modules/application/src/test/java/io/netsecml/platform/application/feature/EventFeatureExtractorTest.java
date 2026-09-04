package io.netsecml.platform.application.feature;

import io.netsecml.platform.domain.event.*;
import org.junit.jupiter.api.Test;
import java.time.Instant;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;

class EventFeatureExtractorTest {
    private final EventFeatureExtractor extractor = new EventFeatureExtractor();

    private NetworkEvent event(Protocol proto, ServiceCode service, ConnectionState state,
                                long durationMillis, long originBytes, long responseBytes,
                                int originPackets, int responsePackets, int destinationPort) {
        SensorId sensor = new SensorId("sensor-eu-1");
        ConnectionTuple tuple = new ConnectionTuple("10.0.0.5", 51820, "93.184.216.34", destinationPort,
            proto, service, state);
        ConnectionMeasurements measurements = new ConnectionMeasurements(
            durationMillis, originBytes, responseBytes, originPackets, responsePackets, 0);
        // LogType.CONN and a non-blank uid are required positional components now;
        // this extractor only cares about event-level feature math, not log type or
        // correlation, so a fixed constant and the same "abc" id used above are enough.
        return new NetworkEvent(EventId.derive(sensor, "abc"), Instant.now(), sensor, LogType.CONN, "abc",
            tuple, measurements, new ConnectionLocality(null, null));
    }

    @Test
    void extractsNormalTcpSslConnection() {
        NetworkEvent e = event(Protocol.TCP, ServiceCode.SSL, ConnectionState.SF, 1500, 2048, 4096, 10, 12, 443);
        float[] v = extractor.extractEventLevel(e);
        assertArrayEquals(new float[]{
            1500f,   // 0 duration_ms
            2048f,   // 1 origin_bytes
            4096f,   // 2 response_bytes
            10f,     // 3 origin_packets
            12f,     // 4 response_packets
            6144f,   // 5 total_bytes
            22f,     // 6 total_packets
            279.27273f, // 7 bytes_per_packet = 6144/22
            2f,      // 8 response_origin_byte_ratio = 4096/2048
            443f,    // 9 destination_port
            1f,      // 10 destination_is_well_known (443 in [1,1023])
            1f,      // 11 protocol_tcp
            0f,      // 12 protocol_udp
            0f,      // 13 service_dns
            0f,      // 14 service_http
            1f,      // 15 service_ssl
            0f       // 16 connection_failed (SF is not a failed state)
        }, v, 0.001f);
    }

    @Test
    void extractsZeroPacketConnectionWithoutDivideByZero() {
        NetworkEvent e = event(Protocol.UDP, ServiceCode.DNS, ConnectionState.S0, 0, 0, 0, 0, 0, 53);
        float[] v = extractor.extractEventLevel(e);
        assertArrayEquals(new float[]{
            0f, 0f, 0f, 0f, 0f,
            0f,      // 5 total_bytes
            0f,      // 6 total_packets
            0f,      // 7 bytes_per_packet = 0 / max(1,0) = 0
            0f,      // 8 response_origin_byte_ratio = 0 / max(1,0) = 0
            53f,     // 9 destination_port
            1f,      // 10 well-known
            0f,      // 11 protocol_tcp
            1f,      // 12 protocol_udp
            1f,      // 13 service_dns
            0f, 0f,
            1f       // 16 connection_failed (S0 is a failed state)
        }, v, 0.001f);
    }

    @Test
    void nonWellKnownHighPortIsZero() {
        NetworkEvent e = event(Protocol.TCP, ServiceCode.UNKNOWN, ConnectionState.SF, 100, 10, 10, 1, 1, 51820);
        float[] v = extractor.extractEventLevel(e);
        assertArrayEquals(new float[]{
            100f, 10f, 10f, 1f, 1f,
            20f,   // 5 total_bytes
            2f,    // 6 total_packets
            10f,   // 7 bytes_per_packet = 20/2
            1f,    // 8 response_origin_byte_ratio = 10/10
            51820f, // 9 destination_port
            0f,    // 10 well-known (51820 is not in [1,1023])
            1f,    // 11 protocol_tcp
            0f,    // 12 protocol_udp
            0f,    // 13 service_dns
            0f,    // 14 service_http
            0f,    // 15 service_ssl
            0f     // 16 connection_failed (SF is not a failed state)
        }, v, 0.001f);
    }
}
