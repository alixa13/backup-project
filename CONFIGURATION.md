# Configuration Management

This project uses **one source of truth** for resource and service configuration: **`configure-resources.sh`**. It detects host hardware and generates all allocation-related config so the stack is consistent and predictable.

## Principle

- **Do not** hand-edit resource limits, Flink memory, or LSTM worker count in isolation. They are derived from the same allocation.
- **Do** run `./configure-resources.sh` (optionally with `--use-all` or `--minimal`) and then start the stack with `docker compose up -d`.

## What the script does

1. **Detects hardware**: CPU count (`nproc` / `/proc/cpuinfo`), total RAM (`/proc/meminfo`).
2. **Reserves** (unless `--use-all`): 2 CPUs and 4G for OS/Zeek (host network). The rest is “available” for containers.
3. **Allocates** by fixed percentages of available CPU/RAM:
   - Flink: 35% CPU, 40% memory
   - LSTM: 35% CPU, 28% memory
   - Kafka: 12% CPU, 8% memory
   - Postgres: 12% CPU, 18% memory
   - Zookeeper: 5% CPU, 3% memory
   - Supervised API: 2% CPU, 2% memory  
   (Minimums and maximums apply when not using `--use-all`.)
4. **Generates**:
   - **`docker-compose.override.yml`**: `deploy.resources` for each service; **LSTM** `GUNICORN_WORKERS` and TF/OMP thread counts (so inference scales beyond ~200% CPU when training is disabled).
   - **`flink/config/flink-conf.yaml`**: Only **memory** is derived from the Flink allocation (JobManager 4G, rest to TaskManager). **Slot count stays 128** and is not driven by hardware.

## Flink: why 128 slots and no “per-core” slots

Flink’s **task slots** are a **job-topology choice**, not a hardware setting:

- **`flink/submit-jobs.sh`** submits: 1 supervised job at **parallelism 8** and 4 unsupervised jobs at **parallelism 16** each (8 + 4×16 = 72 parallel tasks in total).
- **128 slots** gives headroom for all jobs and for future parallelism changes. That value is fixed in **`flink-conf.yaml.template`** and in the generated **`flink-conf.yaml`**.
- **Memory** is what scales with hardware: the script sets `jobmanager.memory.process.size` and `taskmanager.memory.process.size` so they fit inside the Flink container’s allocated memory (from the override). Slot count is **never** changed by the script.

So: **slots = 128** (topology); **memory = from allocation** (hardware).

## File roles

| File | Role |
|------|------|
| `configure-resources.sh` | Single entry point: detects HW, computes allocation, writes override + Flink config + LSTM workers. |
| `docker-compose.yml` | Base stack definition. Resource limits are overridden by the generated override. |
| `docker-compose.override.yml` | **Generated.** Container resources, LSTM `GUNICORN_WORKERS`, and LSTM TF/OMP thread env vars so inference can use all allocated CPUs (avoids ~200% CPU when training is disabled). |
| `flink/config/flink-conf.yaml.template` | **Template.** Contains 128 slots and placeholders `__FLINK_JM_MEM_MB__`, `__FLINK_TM_MEM_MB__`. Editable for non-memory Flink settings. |
| `flink/config/flink-conf.yaml` | **Generated.** Filled from template with memory from current Flink allocation. Slot count remains 128. |
| `flink/submit-jobs.sh` | Defines job parallelism (-p 8, -p 16). Change parallelism here if needed; slot count (128) should still cover total parallelism. |

## Usage

```bash
# Default: reserve 2C/4G for OS, allocate the rest
./configure-resources.sh

# Use all detected CPU/RAM for containers (no reserve)
./configure-resources.sh --use-all

# See allocation only (no files written)
./configure-resources.sh --dry-run

# Minimal host (4C/8G) for testing
./configure-resources.sh --minimal
```

Then:

```bash
docker compose up -d
```

## LSTM: why inference was only ~200% CPU when training disabled

Each Gunicorn worker runs TensorFlow/NumPy for inference. Those libraries use **TF_NUM_INTEROP_THREADS**, **TF_NUM_INTRAOP_THREADS**, **OMP_NUM_THREADS**, and **MKL_NUM_THREADS**. They were previously hardcoded to **2**, so each worker could use at most 2 cores (~200% CPU). With sync workers and one request per worker, total CPU stayed around 200%.

**Fix:** `configure-resources.sh` and the LSTM entrypoint set **TF_NUM_INTRAOP_THREADS** high (up to allocated LSTM CPU, cap 64) so that **training** (single worker) can use all allocated cores and finish faster. **TF_NUM_INTEROP_THREADS** is kept low (2–4). Inference also benefits from high intra_op.

## Changing behaviour

- **Resource shares or minimums**: Edit the allocation section in `configure-resources.sh` (percentages and min/max).
- **Flink memory split**: In the script, change `FLINK_JM_MEM_MB` (default 4096) and the formula for `FLINK_TM_MEM_MB`.
- **Flink topology (slots, checkpointing, etc.)**: Edit `flink/config/flink-conf.yaml.template`; the script only substitutes the two memory placeholders when generating `flink-conf.yaml`.
- **Job parallelism**: Edit `flink/submit-jobs.sh` (-p values). Keep total parallelism ≤ 128 (or update the slot count in the template if you increase it).

## Trusted IPs (skip anomaly check)

IPs listed in the trusted list are **not** checked for anomalies (unsupervised LSTM and supervised UNSW42).

**Single file:** `flink/config/trusted_ips.txt` is the only source of truth. It is mounted into both Flink and the supervised API.

- **Flink** reads `/opt/flink/config/trusted_ips.txt` (from `./flink/config`). One IP per line; lines starting with `#` are ignored. Set env `TRUSTED_IPS_FILE` or system property `trusted.ips.file` to use another path. Flink loads the list at job startup; restart Flink jobs to pick up file changes.
- **Supervised API** reads the same file at `/app/models/trusted_ips.txt` (bind-mounted from `flink/config/trusted_ips.txt`). Use `./trusted-ip.sh add <ip>`, `./trusted-ip.sh remove <ip>`, `./trusted-ip.sh list` to manage; edits are written to the shared file. The API also skips the model in `/predict_unsw42` when the request includes a trusted `id_orig_h` or `source_ip`.

This keeps the system understandable and consistent: one script, one model of the hardware, and no ad-hoc edits to slots or memory.
