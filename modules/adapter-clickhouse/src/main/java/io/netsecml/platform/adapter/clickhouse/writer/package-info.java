/**
 * Connection configuration and the seam ({@code ClickHouseInserter}) that talks
 * to ClickHouse, plus the {@code client-v2}-backed implementation
 * ({@code ClientV2Inserter}) that does it over HTTP with JSONEachRow.
 *
 * <p>Retry and flush policy (when to call {@code insert}, and how often)
 * deliberately live outside this package, in the sink writer, so they can be
 * tested against a fake {@code ClickHouseInserter} with no server involved.
 */
package io.netsecml.platform.adapter.clickhouse.writer;
