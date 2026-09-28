# netsec-ml on a server

One script runs the whole Modbus + S7comm pipeline on one Linux server:
Zeek sniffs the OT interface → Kafka → the online Flink job (42-value Modbus and
16-value S7comm feature vectors, bad records to a DLQ, and a Modbus anomaly
score per event) → the archive job → ClickHouse. It is its own Docker Compose project (`netsec-ml`) and never touches
any other container on the host. Design:
`docs/superpowers/specs/2026-09-24-server-deployment-design.md`.

Modbus is scored by the model team's Stage 1 detector (see Scoring below). S7
is not scored yet, and the older `predictions` table stays empty.

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

## Scoring

The online job scores every Modbus event with the Stage 1 dual-head detector
and writes one prediction to `netsec.modbus.prediction.v1`, which the archive
job stores in ClickHouse `modbus_detector_predictions`. Design:
`docs/superpowers/specs/2026-09-26-modbus-stage1-scoring-design.md`.

The detector is a bundle under `models/` (not in Git), pinned by
`MODBUS_DETECTOR_BUNDLE` in `deploy/.env` (default `modbus-stage1-detector/v1`).
Package it once from the model team's delivery, before `up`:

```sh
./deploy/models/package-modbus-detector.sh models/modbus/stage1_anomaly_detector
```

It checks the model file against the delivery's own manifest and writes
`models/modbus-stage1-detector/v1/`. `up` and `restart` refuse to start while
the pinned bundle is missing or any of its files no longer matches the SHA-256
in its `bundle.json`; set `MODBUS_DETECTOR_BUNDLE=` (empty) to run features only.
Switching scoring off or back on is a `restart` either way: the scoring operator
stays in the job while off, so the saved state still fits. A new model version
is a new folder and a new pin, then `restart`.

S7comm is scored the same way by our S7comm Stage 1 detector v2 (an LSTM
autoencoder), into `netsec.s7comm.prediction.v1` and ClickHouse
`s7comm_detector_predictions`. The design is
`docs/superpowers/specs/2026-09-28-s7comm-stage1-scoring-design.md`; the model card
is `docs/models/s7comm-stage1-detector-v2.md`. Its bundle is pinned by
`S7COMM_DETECTOR_BUNDLE` (default `s7comm-stage1-detector/v2`). Package it once from
the release:

```sh
./deploy/models/package-s7comm-detector.sh models/S7v2/models/stage1_anomaly/v2_multisource_r1 v2
```

It checks every file against the release's own `FROZEN_MANIFEST.json`.
- **The warm-up.** A connection's first 64 events are `WARMUP`, and so are the first 64 after any
  reset; `events_since_reset` counts them.
- **The restart.** A connection restarts its S7 state every 16,384 events, because one feature
  grows without bound, and the restarted event carries the quality flag 32.
- **Turning it off.** Set `S7COMM_DETECTOR_BUNDLE=` (empty) to run S7 features only.

Each Modbus prediction's `verdict` (`modbus_detector_predictions`):

| Verdict | Meaning |
|---|---|
| `WARMUP` | Fewer than 20 events in this stream (client, server, unit) since it started, or since a gap over 15 s -- no scores. Every stream begins with 19 of these |
| `NORMAL` / `ANOMALY` | Scored: `ANOMALY` when either score is above its threshold; `trigger` says which (`DENSE`, `TEMPORAL`, `BOTH`) |
| `UNSCORABLE` | The vector could not be preprocessed (not expected; counted by the `unscorable` metric) |

`./deploy/deploy.sh sql "SELECT verdict, count() FROM modbus_detector_predictions GROUP BY verdict"`
shows the split. Upgrading from a deployment without scoring restarts every
Modbus stream's state once (its layout changed); nothing else is lost.

Each S7comm prediction's `verdict` (`s7comm_detector_predictions`):

| Verdict | Meaning |
|---|---|
| `WARMUP` | The first 64 events of this connection (uid) since it started or last reset: a TTL expiry, a restore without its state, or the periodic restart every 16,384 events -- no score. `events_since_reset` counts them |
| `NORMAL` / `ANOMALY` | Scored: `ANOMALY` when the event's conformal `p_value` is at most its `score_group`'s `alpha` |
| `UNSCORABLE` | The score was not finite (not expected) |

- **Some events are `ANOMALY` by design.** The detector flags every S7 operation it rarely saw
  mid-connection: setup, PLC stop, user-data (CPU function) requests and their responses, and bare
  ROSCTR 2 ACK responses. These are the operator actions it exists to surface; on a plain polling
  connection they do not occur.
- **Upgrading restarts every S7 connection once**, at its next event (restored state has no
  restart count yet); nothing else is lost.

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
  other containers use is already excluded) and keeps a reserve for the host.
  From the budget left it takes the JobManager (1 GiB) and the job supervisor
  (640 MiB), then splits the rest: Flink TaskManager 40%, ClickHouse 25%,
  Kafka 15%, Zeek 10%. Every limit together stays inside the budget, and the
  CPU limits inside the cores it leaves you (a quarter of them, at least one,
  stay with the host). It refuses a budget below 5000 MiB (`--force`
  overrides); `--memory-budget 12g` and `--cpus 4` set the budget yourself.
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
- **Every topic must exist**; `up` creates all thirteen, including the empty conn
  and dns ones both jobs subscribe to, and `netsec.modbus.prediction.v1` even when
  scoring is off (the archive job reads it).
- **Scoring keeps up with about 2,000 Modbus events per second per stream**
  (client, server, unit). Above that -- a flood -- both the Modbus feature vectors
  and the predictions fall behind real time and catch up afterwards; nothing is
  lost. Normal polling is far below it.
- **Single node, no HA.** One Kafka broker, one ClickHouse, one TaskManager.
- Data lives in `deploy/data/`; settings and the generated ClickHouse password
  in `deploy/.env` (mode 600, never committed).
