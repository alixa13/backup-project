# config/flink/

`flink-conf.yaml`: cluster-level Flink configuration (checkpoint interval, state
backend, durable checkpoint/savepoint storage location — never TaskManager-local
disk, per `FINAL_ARCHITECTURE.md`, "Checkpoint storage is not mentioned"). Not yet
committed — Step 1 scope is directory skeleton only.
