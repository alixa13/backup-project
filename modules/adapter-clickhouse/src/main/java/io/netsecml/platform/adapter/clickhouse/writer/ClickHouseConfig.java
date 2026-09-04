package io.netsecml.platform.adapter.clickhouse.writer;

import java.io.Serializable;
import java.util.Objects;

// Connection settings for the archive sink.
//
// Serializable because the sink carries it from the job graph out to every
// subtask; the live client is built on the far side, in createWriter.
public record ClickHouseConfig(String endpoint, String database, String username, String password)
    implements Serializable {

    public ClickHouseConfig {
        Objects.requireNonNull(endpoint, "endpoint must not be null");
        Objects.requireNonNull(database, "database must not be null");
        Objects.requireNonNull(username, "username must not be null");

        // An empty password is normal for a local ClickHouse; null is not.
        password = password == null ? "" : password;
    }

    // Convenience for the host/port form the environment variables carry.
    public static ClickHouseConfig of(String host, int port, String database, String username, String password) {
        return new ClickHouseConfig("http://" + host + ":" + port, database, username, password);
    }
}
