# infrastructure/clickhouse/

`ddl/001_mvp_tables.sql` defines the five MVP tables. Apply it with
`scripts/database/apply-ddl.sh`, which is idempotent — every statement is
`CREATE ... IF NOT EXISTS`.

| Table | Engine | Retention | First writer |
|---|---|---|---|
| `feature_vectors` | `ReplacingMergeTree(row_version)` | 90 days | archive job (today) |
| `invalid_events` | `MergeTree` | 30 days | archive job (today) |
| `predictions` | `ReplacingMergeTree(row_version)` | 180 days | Roadmap Day 9 |
| `network_events` | `ReplacingMergeTree(row_version)` | 14 days | optional; no producer exists |
| `model_releases` | `MergeTree` | none | Roadmap Day 11 |

The five-table set and the `model_releases` naming follow `FINAL_ARCHITECTURE.md`
Step 8 rather than the four-table list in `Roadmap.md`; see
`docs/adr/0001-clickhouse-table-set.md` for why. Column types, deduplication
semantics and the access requirements for `network_events` are documented in
`docs/clickhouse.md`.

`DdlMigrationTest` in `modules/adapter-clickhouse` applies this exact file against
a ClickHouse container, so there is no test-only copy of the schema.
