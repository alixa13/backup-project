/**
 * Connection configuration and the seam ({@code ClickHouseInserter}) that talks
 * to ClickHouse, plus the {@code client-v2}-backed implementation
 * ({@code ClientV2Inserter}) that does it over HTTP with JSONEachRow.
 *
 * <p>Retry and flush policy (when to call {@code insert}, and how often) live in
 * {@code ClickHouseSinkWriter}, in this same package. What makes that testable
 * without a server is not package boundaries but the {@code ClickHouseInserter}
 * seam: the sink writer is tested against a fake implementation of it, and only
 * {@code ClientV2Inserter} talks to a real ClickHouse over HTTP.
 */
package io.netsecml.platform.adapter.clickhouse.writer;
