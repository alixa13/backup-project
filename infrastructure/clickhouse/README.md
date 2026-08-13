# infrastructure/clickhouse/

`ddl/001_mvp_tables.sql` will define exactly the four MVP tables: `network_events`,
`feature_vectors`, `predictions`, `model_metadata` — see Roadmap.md, "Minimum
ClickHouse scope," for engines, partitioning, ordering keys, and TTLs. Populated
starting Roadmap.md Day 6-7. Not yet committed — Step 1 scope is directory skeleton
only.
