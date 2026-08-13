package io.netsecml.platform.domain.event;

public record ConnectionTuple(String sourceIp, int sourcePort, String destinationIp, int destinationPort,
                               Protocol protocol, ServiceCode service, ConnectionState connectionState) {
    public ConnectionTuple {
        sourceIp = requireNonBlank(sourceIp, "sourceIp");
        sourcePort = requireValidPort(sourcePort, "sourcePort");
        destinationIp = requireNonBlank(destinationIp, "destinationIp");
        destinationPort = requireValidPort(destinationPort, "destinationPort");
    }

    private static String requireNonBlank(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(field + " must not be blank");
        }
        return value;
    }

    private static int requireValidPort(int port, String field) {
        if (port < 0 || port > 65535) {
            throw new IllegalArgumentException(field + " must be in [0,65535], was " + port);
        }
        return port;
    }
}
