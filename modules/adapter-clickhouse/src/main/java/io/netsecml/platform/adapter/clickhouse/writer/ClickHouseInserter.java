package io.netsecml.platform.adapter.clickhouse.writer;

import java.util.List;

// The narrow seam between "talk to ClickHouse" and "decide when to talk to it".
//
// One method, deliberately: it is what lets ClickHouseSinkWriter's retry and
// flush logic be tested against a fake with no server and no Docker.
public interface ClickHouseInserter extends AutoCloseable {

    // Inserts the given JSONEachRow lines into the table, blocking until the
    // server acknowledges. Throws on any failure — the caller owns retry policy.
    void insert(String table, List<String> jsonLines) throws Exception;

    // Narrowed from AutoCloseable's `throws Exception`: releasing the HTTP
    // client is not expected to fail in a way callers need to handle, so
    // try-with-resources callers of insert() only ever catch one checked type.
    @Override
    void close();
}
