package io.netsecml.platform.adapter.clickhouse.writer;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.insert.InsertResponse;
import com.clickhouse.data.ClickHouseFormat;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

// The real insert path: POST a JSONEachRow batch and block until the server
// acknowledges it.
public final class ClientV2Inserter implements ClickHouseInserter {
    private final Client client;

    // Eagerly builds the live HTTP client from the Serializable config. This
    // constructor runs inside createWriter, on the subtask — never on the job
    // graph — because Client itself cannot be serialized.
    public ClientV2Inserter(ClickHouseConfig config) {
        this.client = new Client.Builder()
            .addEndpoint(config.endpoint())
            .setUsername(config.username())
            .setPassword(config.password())
            .setDefaultDatabase(config.database())
            // Compressing the request is worthwhile: batches are up to 4 MiB of
            // highly repetitive JSON.
            .compressClientRequest(true)
            // input_format_skip_unknown_fields defaults to 1 (skip) on the server,
            // which would let a JSONEachRow key matching no column vanish silently
            // instead of failing the insert -- exactly the case (a renamed
            // @JsonProperty, or a DDL column drift) this adapter most needs to
            // surface loudly. Forcing it to 0 turns that into a thrown exception
            // here, which is what makes throwsWhenTheServerRejectsTheBatch's
            // assertion true.
            .serverSetting("input_format_skip_unknown_fields", "0")
            .build();
    }

    @Override
    public void insert(String table, List<String> jsonLines) throws Exception {
        // JSONEachRow is one JSON object per line. The trailing newline keeps the
        // last record well-formed.
        byte[] body = (String.join("\n", jsonLines) + "\n").getBytes(StandardCharsets.UTF_8);

        // get() is what makes this synchronous. The future completing normally IS
        // the server's acknowledgement; a server-side error arrives as an
        // ExecutionException and propagates to the caller's retry loop.
        try (InputStream in = new ByteArrayInputStream(body);
             InsertResponse response = client.insert(table, in, ClickHouseFormat.JSONEachRow).get()) {
            // The response is closed to release the connection back to the pool.
            // Row counts are deliberately not asserted here: getWrittenRows()
            // depends on server summary settings, and the integration test verifies
            // persistence by querying the table instead.
        }
    }

    // Releases the underlying HTTP client and its pooled connections. Client's
    // own close() declares no checked exception, so this never needs to wrap one.
    @Override
    public void close() {
        client.close();
    }
}
