package io.netsecml.platform.adapter.clickhouse.writer;

import java.io.Serializable;

// Builds an inserter on the subtask that will use it.
//
// A live HTTP client cannot travel through Flink's job graph, so the sink holds
// this Serializable factory plus the config and constructs the real inserter
// inside createWriter. Tests substitute a factory returning a fake.
@FunctionalInterface
public interface ClickHouseInserterFactory extends Serializable {
    // Called once per subtask, inside createWriter, after the config has
    // travelled through the job graph — so this is where the real (or fake,
    // in tests) inserter actually comes into existence.
    ClickHouseInserter create(ClickHouseConfig config);
}
