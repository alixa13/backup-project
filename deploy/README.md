# netsec-ml on a server

One script runs the whole Modbus + S7comm pipeline on one Linux server:
Zeek sniffs the OT interface → Kafka → the online Flink job (42-value Modbus and
16-value S7comm feature vectors, bad records to a DLQ) → the archive job →
ClickHouse. It is its own Docker Compose project (`netsec-ml`) and never touches
any other container on the host. Design:
`docs/superpowers/specs/2026-09-24-server-deployment-design.md`.

No attack scoring yet: the Modbus and S7 models are not delivered. The
`predictions` table exists and stays empty until they are.

## First install

```sh
git clone <this repository> && cd ml-platform
./deploy/deploy.sh doctor                     # read-only host check; lists interfaces
./deploy/deploy.sh install --interface eth1   # Docker/curl/jq if missing, deploy/.env, sizing
./deploy/deploy.sh build                      # job JARs + Zeek image (~10 min the first time)
./deploy/deploy.sh up                         # everything, in order; ends with 'status'
./deploy/deploy.sh selftest                   # one Modbus + one S7 pair end to end
./deploy/deploy.sh zeek-check                 # our Zeek vs the parsers, on ICSNPP samples
```

Once real traffic flows: `./deploy/deploy.sh zeek-check --live 500` checks the
sensor's own newest records against the parsers, and `status` shows rows per
protocol. `RESULT: PASS` may list "known upstream parity" Modbus rejections —
function names the frozen model's own code cannot resolve (exception PDUs and
Zeek's `ENCAP_INTERFACE_TRANSPORT`); anything `UNEXPECTED` is a finding to report.

## Day to day

| Task | Command |
|---|---|
| Health, jobs, traffic, recent rows | `./deploy/deploy.sh status` |
| Logs | `./deploy/deploy.sh logs zeek` (or kafka, clickhouse, flink-jobmanager, flink-taskmanager, job-submitter) |
| Query | `./deploy/deploy.sh sql "SELECT log_type, count() FROM feature_vectors GROUP BY log_type"` |
| Stop (state saved) / start | `./deploy/deploy.sh down` / `./deploy/deploy.sh up` |
| Upgrade after `git pull` | `./deploy/deploy.sh build && ./deploy/deploy.sh restart` |
| Re-size after a hardware change | `./deploy/deploy.sh tune && ./deploy/deploy.sh restart` |
| Remove (data kept) / everything | `./deploy/deploy.sh uninstall` / `uninstall --purge` |

The Flink UI and ClickHouse listen on `127.0.0.1` only. From your desk:
`ssh -L 18081:127.0.0.1:18081 -L 18123:127.0.0.1:18123 you@server`, then open
http://localhost:18081.

## What to know

- **Sizing.** `tune` reads the CPU count and the RAM free right now (so what
  other containers use is already excluded), keeps a reserve for the host, and
  splits the rest: Flink TaskManager 40%, ClickHouse 25%, Kafka 15%, Zeek 10%,
  JobManager a fixed 1 GiB. It refuses below 4.25 GiB (`--force` overrides);
  `--memory-budget 12g` and `--cpus 4` set the budget yourself.
- **State survives restarts.** `down` stops each job with a savepoint; `up`, a
  reboot or a JobManager restart resumes each job from its newest savepoint or
  checkpoint (the `job-submitter` container does this every minute).
- **If a job keeps failing to start after an upgrade**, its saved state no longer
  fits the new code. `logs job-submitter` says so; move that job's folders under
  `deploy/data/flink/savepoints/` and `deploy/data/flink/checkpoints/` aside and
  it starts fresh.
- **Zeek packages are pinned.** icsnpp-modbus stays at v1.0.0: v2.0.0 changed
  `modbus_detailed` to one record per request/response pair, which the frozen
  Modbus model cannot read.
- **Every topic must exist**; `up` creates all twelve, including the empty conn
  and dns ones both jobs subscribe to.
- **Single node, no HA.** One Kafka broker, one ClickHouse, one TaskManager.
- Data lives in `deploy/data/`; settings and the generated ClickHouse password
  in `deploy/.env` (mode 600, never committed).
