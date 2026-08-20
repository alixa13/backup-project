#!/usr/bin/env bash
#
# configure-resources.sh  —  Anomaly Detection Stack Resource Manager
#
# Detects host hardware (including hyperthreading), allocates resources
# per service role, validates totals, and writes:
#   • docker-compose.override.yml
#   • flink/config/flink-conf.yaml
#
# Allocation model
# ─────────────────
# Resources are divided into two pools:
#
#   INFRA pool  (ZK, Kafka, Postgres, Supervised)
#     → infrastructure services; small, predictable, IO-bound
#
#   COMPUTE pool  (Flink, LSTM)
#     → data-plane workloads; large, burst, CPU-bound
#
# Within COMPUTE, Flink and LSTM split the pool.
# Flink has a hard-cap because its actual parallelism is fixed (submit-jobs.sh -p 16 supervised, -p 16 unsupervised).
# LSTM scales freely because training uses all its allocated cores (model.py dynamic threads).
#
# LSTM Gunicorn workers
# ─────────────────────
# Workers serve 4 log types (conn/http/dns/ssl). Optimal count = physical_cores / 4,
# capped to [4, 20]. Each worker uses a few inference threads; training boosts
# intra_op to all cores dynamically (handled inside model.py, not here).
#
# Usage
# ─────
#   ./configure-resources.sh               # auto-detect, write files
#   ./configure-resources.sh --use-all     # no OS/Zeek reserve
#   ./configure-resources.sh --dry-run     # print plan, no files written
#   ./configure-resources.sh --minimal     # 4C/8G test profile
#   ./configure-resources.sh --help
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OVERRIDE_FILE="${SCRIPT_DIR}/docker-compose.override.yml"
FLINK_CONF_TEMPLATE="${SCRIPT_DIR}/flink/config/flink-conf.yaml.template"
FLINK_CONF_OUT="${SCRIPT_DIR}/flink/config/flink-conf.yaml"

DRY_RUN=false
MINIMAL=false
USE_ALL=false

for arg in "$@"; do
  case "$arg" in
    --dry-run)   DRY_RUN=true ;;
    --minimal)   MINIMAL=true ;;
    --use-all)   USE_ALL=true ;;
    -h|--help)
      sed -n '/^# Usage/,/^#$/p' "$0" | sed 's/^# \{0,2\}//'
      exit 0 ;;
  esac
done

# ══════════════════════════════════════════════════════════════════════════════
# 1. HARDWARE DETECTION
# ══════════════════════════════════════════════════════════════════════════════

detect_logical_cpus() {
  nproc 2>/dev/null \
  || grep -c ^processor /proc/cpuinfo 2>/dev/null \
  || echo 4
}

detect_physical_cores() {
  # Method 1: lscpu
  if command -v lscpu &>/dev/null; then
    local sockets cores_per_socket
    sockets=$(lscpu 2>/dev/null | awk '/^Socket\(s\):/{print $2}')
    cores_per_socket=$(lscpu 2>/dev/null | awk '/^Core\(s\) per socket:/{print $4}')
    if [ -n "$sockets" ] && [ -n "$cores_per_socket" ] && \
       [ "$sockets" -gt 0 ] 2>/dev/null && [ "$cores_per_socket" -gt 0 ] 2>/dev/null; then
      echo $(( sockets * cores_per_socket ))
      return
    fi
  fi
  # Method 2: sysfs unique core_id per physical_package_id
  local cores
  cores=$(sort -u \
    /sys/devices/system/cpu/cpu*/topology/core_id \
    /sys/devices/system/cpu/cpu*/topology/physical_package_id \
    2>/dev/null | wc -l)
  [ "$cores" -gt 0 ] && echo "$cores" && return
  # Fallback: logical / 2 (assume HT)
  echo $(( $(detect_logical_cpus) / 2 ))
}

detect_mem_gb() {
  local kb
  kb=$(awk '/MemTotal:/{print $2}' /proc/meminfo 2>/dev/null)
  [ -n "$kb" ] && echo $(( kb / 1024 / 1024 )) && return
  free -g 2>/dev/null | awk '/^Mem:/{print $2}' && return
  echo 8
}

detect_mem_free_gb() {
  local kb
  kb=$(awk '/MemAvailable:/{print $2}' /proc/meminfo 2>/dev/null)
  [ -n "$kb" ] && echo $(( kb / 1024 / 1024 )) && return
  echo $(detect_mem_gb)
}

# ══════════════════════════════════════════════════════════════════════════════
# 2. GATHER HARDWARE
# ══════════════════════════════════════════════════════════════════════════════

if [ "$MINIMAL" = true ]; then
  TOTAL_LOGICAL=4; TOTAL_PHYSICAL=4; TOTAL_GB=8
  echo "Profile : MINIMAL (4 logical / 4 physical cores, 8G RAM)"
else
  TOTAL_LOGICAL=$(detect_logical_cpus)
  TOTAL_PHYSICAL=$(detect_physical_cores)
  TOTAL_GB=$(detect_mem_gb)
  FREE_GB=$(detect_mem_free_gb)
  HT=$(( TOTAL_LOGICAL / TOTAL_PHYSICAL ))
  [ "$HT" -lt 1 ] && HT=1

  echo "Hardware : ${TOTAL_LOGICAL} logical CPUs / ${TOTAL_PHYSICAL} physical cores (HT factor ${HT}×)"
  echo "Memory   : ${TOTAL_GB}G total  |  ${FREE_GB}G available"
fi

# Reserve for OS + Zeek (host network, not in Docker)
if [ "$USE_ALL" = true ]; then
  RESERVE_CPUS=0; RESERVE_GB=0
  echo "Reserve  : none (--use-all)"
else
  RESERVE_CPUS=2; RESERVE_GB=4
  echo "Reserve  : ${RESERVE_CPUS} CPUs + ${RESERVE_GB}G for OS/Zeek"
fi

AVAIL_CPUS=$(( TOTAL_LOGICAL - RESERVE_CPUS ))
AVAIL_PHYS=$(( TOTAL_PHYSICAL - RESERVE_CPUS / 2 ))
AVAIL_GB=$(( TOTAL_GB - RESERVE_GB ))
[ "$AVAIL_CPUS" -lt 2 ] && AVAIL_CPUS=2
[ "$AVAIL_PHYS" -lt 1 ] && AVAIL_PHYS=1
[ "$AVAIL_GB"   -lt 4 ] && AVAIL_GB=4

echo "Available: ${AVAIL_CPUS} CPUs (${AVAIL_PHYS} physical) / ${AVAIL_GB}G"
echo ""

# ══════════════════════════════════════════════════════════════════════════════
# 3. POOL BUDGETS
# ══════════════════════════════════════════════════════════════════════════════
# INFRA: 18% CPU (min 8, max 24), 22% RAM (min 8G, max 28G)
# COMPUTE = remaining

INFRA_CPU=$(( AVAIL_CPUS * 18 / 100 ))
[ "$INFRA_CPU" -lt 8  ] && INFRA_CPU=8
[ "$INFRA_CPU" -gt 24 ] && INFRA_CPU=24

INFRA_GB=$(( AVAIL_GB * 22 / 100 ))
[ "$INFRA_GB" -lt 8  ] && INFRA_GB=8
[ "$INFRA_GB" -gt 28 ] && INFRA_GB=28

COMPUTE_CPU=$(( AVAIL_CPUS - INFRA_CPU ))
[ "$COMPUTE_CPU" -lt 2 ] && COMPUTE_CPU=2

COMPUTE_GB=$(( AVAIL_GB - INFRA_GB ))
[ "$COMPUTE_GB" -lt 4 ] && COMPUTE_GB=4

# ══════════════════════════════════════════════════════════════════════════════
# 4. INFRA ALLOCATION  (ZK / Kafka / Postgres / Supervised)
# ══════════════════════════════════════════════════════════════════════════════
# Ratios within INFRA budget: Kafka 40%, Postgres 35%, Supervised 15%, ZK 10%
# Reservations = 70% of limit for infra (they run steadily, not bursty)

_ic() { # infra cpu: pct, min, max
  local v=$(( INFRA_CPU * $1 / 100 ))
  [ "$v" -lt "$2" ] && v=$2
  [ "$v" -gt "$3" ] && v=$3
  [ "$v" -lt 1 ] && v=1
  echo "$v"
}
_im() { # infra mem: pct, min, max
  local v=$(( INFRA_GB * $1 / 100 ))
  [ "$v" -lt "$2" ] && v=$2
  [ "$v" -gt "$3" ] && v=$3
  [ "$v" -lt 1 ] && v=1
  echo "$v"
}

ZK_CPU=$(_ic 10 1 4);   ZK_MEM=$(_im 10 1 3)
KAFKA_CPU=$(_ic 40 2 12); KAFKA_MEM=$(_im 35 2 8)
POSTGRES_CPU=$(_ic 35 2 8); POSTGRES_MEM=$(_im 40 4 16)
SUPERVISED_CPU=$(_ic 15 1 4); SUPERVISED_MEM=$(_im 15 1 6)

ZK_CPU_RES=$(( ZK_CPU * 7/10 ));   [ "$ZK_CPU_RES"   -lt 1 ] && ZK_CPU_RES=1
ZK_MEM_RES=$(( ZK_MEM * 7/10 ));   [ "$ZK_MEM_RES"   -lt 1 ] && ZK_MEM_RES=1
KAFKA_CPU_RES=$(( KAFKA_CPU * 7/10 )); [ "$KAFKA_CPU_RES" -lt 1 ] && KAFKA_CPU_RES=1
KAFKA_MEM_RES=$(( KAFKA_MEM * 7/10 )); [ "$KAFKA_MEM_RES" -lt 1 ] && KAFKA_MEM_RES=1
POSTGRES_CPU_RES=$(( POSTGRES_CPU * 7/10 )); [ "$POSTGRES_CPU_RES" -lt 1 ] && POSTGRES_CPU_RES=1
POSTGRES_MEM_RES=$(( POSTGRES_MEM * 7/10 )); [ "$POSTGRES_MEM_RES" -lt 2 ] && POSTGRES_MEM_RES=2
SUPERVISED_CPU_RES=1; SUPERVISED_MEM_RES=1

# Postgres shared_buffers = 25% of its memory allocation (standard DBA formula)
PG_SHARED_BUFFERS=$(( POSTGRES_MEM * 1024 / 4 ))  # MB
PG_EFF_CACHE=$(( POSTGRES_MEM * 3 * 1024 / 4 ))   # MB (~75%)
PG_WORK_MEM=$(( POSTGRES_MEM * 1024 / 50 ))       # MB (small, many parallel queries)
[ "$PG_SHARED_BUFFERS" -lt 128 ] && PG_SHARED_BUFFERS=128
[ "$PG_WORK_MEM" -lt 8 ] && PG_WORK_MEM=8

# ══════════════════════════════════════════════════════════════════════════════
# 5. COMPUTE ALLOCATION  (Flink / LSTM)
# ══════════════════════════════════════════════════════════════════════════════
# Flink: fixed parallelism (submit-jobs.sh: max -p 16). Cap at 28 CPUs.
#   More CPUs don't help beyond the job parallelism ceiling.
# LSTM: gets the rest — benefits from extra CPUs during training.

FLINK_CPU=$(( COMPUTE_CPU * 42 / 100 ))
[ "$FLINK_CPU" -lt 4  ] && FLINK_CPU=4
[ "$FLINK_CPU" -gt 28 ] && FLINK_CPU=28    # cap: -p 16 max job parallelism

FLINK_MEM=$(( COMPUTE_GB * 45 / 100 ))
[ "$FLINK_MEM" -lt 6  ] && FLINK_MEM=6
[ "$FLINK_MEM" -gt 52 ] && FLINK_MEM=52   # 128 slots × ~400MB ≈ 52G practical max

LSTM_CPU=$(( COMPUTE_CPU - FLINK_CPU ))
[ "$LSTM_CPU" -lt 2  ] && LSTM_CPU=2

LSTM_MEM=$(( COMPUTE_GB - FLINK_MEM ))
[ "$LSTM_MEM" -lt 4  ] && LSTM_MEM=4

# Reservations: compute services are bursty → reserve 60% of limit
FLINK_CPU_RES=$(( FLINK_CPU * 6 / 10 )); [ "$FLINK_CPU_RES" -lt 2 ] && FLINK_CPU_RES=2
FLINK_MEM_RES=$(( FLINK_MEM * 6 / 10 )); [ "$FLINK_MEM_RES" -lt 4 ] && FLINK_MEM_RES=4
LSTM_CPU_RES=$(( LSTM_CPU * 6 / 10 ));  [ "$LSTM_CPU_RES"  -lt 2 ] && LSTM_CPU_RES=2
LSTM_MEM_RES=$(( LSTM_MEM * 6 / 10 ));  [ "$LSTM_MEM_RES"  -lt 4 ] && LSTM_MEM_RES=4

# ══════════════════════════════════════════════════════════════════════════════
# 6. LSTM WORKER & THREAD TUNING
# ══════════════════════════════════════════════════════════════════════════════
# Workers: serve 4 log types (conn/http/dns/ssl) concurrently.
#   Formula: physical_cores / 4  → each log type gets ~1 physical core base.
#   Clamped to [4, 20]. More workers = more concurrency; too many = RAM waste.
#
# Inference intra_op: LSTM_CPU / workers (a few threads per worker for inference).
#   Training intra_op: all LSTM_CPU cores (handled dynamically in model.py).
#
# inter_op: 4 is enough for all cases.

LSTM_WORKERS=$(( AVAIL_PHYS / 4 ))
[ "$LSTM_WORKERS" -lt 4  ] && LSTM_WORKERS=4
[ "$LSTM_WORKERS" -gt 20 ] && LSTM_WORKERS=20

TF_INTRAOP_INFERENCE=$(( LSTM_CPU / LSTM_WORKERS ))
[ "$TF_INTRAOP_INFERENCE" -lt 2 ] && TF_INTRAOP_INFERENCE=2
[ "$TF_INTRAOP_INFERENCE" -gt 8 ] && TF_INTRAOP_INFERENCE=8
TF_INTEROP=4
[ "$TF_INTEROP" -gt "$LSTM_CPU" ] && TF_INTEROP=$LSTM_CPU

# Validate minimum LSTM memory per worker (each loads 4 models ~200MB each = ~800MB + overhead)
LSTM_MEM_PER_WORKER=$(( LSTM_MEM * 1024 / LSTM_WORKERS ))   # MB
LSTM_MEM_MIN_MB=$(( LSTM_WORKERS * 1500 ))                  # 1.5G/worker
if [ "${LSTM_MEM_PER_WORKER}" -lt 1500 ]; then
  LSTM_MEM=$(( (LSTM_MEM_MIN_MB + 1023) / 1024 ))
fi

# ══════════════════════════════════════════════════════════════════════════════
# 7. FLINK MEMORY SPLIT & SLOTS
# ══════════════════════════════════════════════════════════════════════════════
# JobManager: 4G fixed.
# TaskManager: container_limit - JM - 1G overhead headroom.
# TaskManager slots: set here; must be >= total job parallelism (16 supervised + 4×16 unsupervised = 80).

FLINK_TM_SLOTS=80
FLINK_JM_MEM_MB=4096
FLINK_TM_MEM_MB=$(( (FLINK_MEM - 5) * 1024 ))   # 4G JM + 1G headroom
[ "$FLINK_TM_MEM_MB" -lt 4096 ] && FLINK_TM_MEM_MB=4096
FLINK_TM_MIN_MB=$(( FLINK_TM_SLOTS * 200 ))   # ~200MB per slot minimum
FLINK_TM_WARN=""
if [ "$FLINK_TM_MEM_MB" -lt "$FLINK_TM_MIN_MB" ]; then
  FLINK_TM_WARN="  ⚠ TM ${FLINK_TM_MEM_MB}m < ${FLINK_TM_MIN_MB}m (${FLINK_TM_SLOTS} slots × 200MB min). Consider --use-all or reducing slots."
fi

# ══════════════════════════════════════════════════════════════════════════════
# 8. CASCADE VALIDATION
# ══════════════════════════════════════════════════════════════════════════════

TOTAL_ALLOC_CPU=$(( FLINK_CPU + LSTM_CPU + ZK_CPU + KAFKA_CPU + POSTGRES_CPU + SUPERVISED_CPU ))
TOTAL_ALLOC_GB=$(( FLINK_MEM + LSTM_MEM + ZK_MEM + KAFKA_MEM + POSTGRES_MEM + SUPERVISED_MEM ))
CPU_OK="✓"; MEM_OK="✓"
[ "$TOTAL_ALLOC_CPU" -gt "$AVAIL_CPUS" ] && CPU_OK="⚠ OVER"
[ "$TOTAL_ALLOC_GB"  -gt "$AVAIL_GB"   ] && MEM_OK="⚠ OVER"

# ══════════════════════════════════════════════════════════════════════════════
# 9. SUMMARY TABLE
# ══════════════════════════════════════════════════════════════════════════════

echo "┌──────────────────────────────────────────────────────────────────┐"
echo "│  COMPUTE POOL  (${COMPUTE_CPU} CPUs / ${COMPUTE_GB}G)  — all CPU soft sharing    │"
echo "├────────────────────────┬────────────────────┬────────────────────┤"
printf "│  %-22s │  CPU res (—/%-3s)     │  RAM lim/res %2sG/%-2sG │\n" \
  "flink" "$FLINK_CPU" "$FLINK_MEM" "$FLINK_MEM_RES"
printf "│  %-22s │  CPU res (—/%-3s)     │  RAM lim/res %2sG/%-2sG │\n" \
  "lstm-autoencoder" "$LSTM_CPU" "$LSTM_MEM" "$LSTM_MEM_RES"
echo "├──────────────────────────────────────────────────────────────────┤"
echo "│  INFRA POOL  (${INFRA_CPU} CPUs / ${INFRA_GB}G)  — all CPU soft sharing          │"
echo "├────────────────────────┬────────────────────┬────────────────────┤"
printf "│  %-22s │  CPU res (—/%-3s)     │  RAM lim/res %2sG/%-2sG │\n" \
  "kafka" "$KAFKA_CPU" "$KAFKA_MEM" "$KAFKA_MEM_RES"
printf "│  %-22s │  CPU res (—/%-3s)     │  RAM lim/res %2sG/%-2sG │\n" \
  "postgres" "$POSTGRES_CPU" "$POSTGRES_MEM" "$POSTGRES_MEM_RES"
printf "│  %-22s │  CPU res (—/%-3s)     │  RAM lim/res %2sG/%-2sG │\n" \
  "zookeeper" "$ZK_CPU" "$ZK_MEM" "$ZK_MEM_RES"
printf "│  %-22s │  CPU res (—/%-3s)     │  RAM lim/res %2sG/%-2sG │\n" \
  "supervised" "$SUPERVISED_CPU" "$SUPERVISED_MEM" "$SUPERVISED_MEM_RES"
echo "├──────────────────────────────────────────────────────────────────┤"
printf "│  %-22s │  %3s / %-3s avail  %s │  %3sG / %-3sG avail  %s │\n" \
  "TOTAL" "$TOTAL_ALLOC_CPU" "$AVAIL_CPUS" "$CPU_OK" \
  "$TOTAL_ALLOC_GB" "$AVAIL_GB" "$MEM_OK"
echo "└──────────────────────────────────────────────────────────────────┘"
echo ""
echo "LSTM tuning:"
printf "  Gunicorn workers : %d  (physical_cores/4 = %d/4)\n" \
  "$LSTM_WORKERS" "$AVAIL_PHYS"
printf "  TF intra_op (inf): %d threads/worker  (training→all cores via model.py)\n" \
  "$TF_INTRAOP_INFERENCE"
printf "  TF inter_op      : %d\n" "$TF_INTEROP"
echo ""
echo "Flink tuning:"
printf "  JobManager  : %dm\n" "$FLINK_JM_MEM_MB"
printf "  TaskManager : %dm  (%d slots)\n" "$FLINK_TM_MEM_MB" "$FLINK_TM_SLOTS"
[ -n "$FLINK_TM_WARN" ] && echo "$FLINK_TM_WARN"
echo ""
echo "Postgres auto-config:"
printf "  shared_buffers      : %dMB  (25%% of %dG)\n" "$PG_SHARED_BUFFERS" "$POSTGRES_MEM"
printf "  effective_cache_size: %dMB  (75%% of %dG)\n" "$PG_EFF_CACHE" "$POSTGRES_MEM"
printf "  work_mem            : %dMB\n" "$PG_WORK_MEM"
echo ""
echo "(zeek: host network, not in override)"
echo ""

if [ "$DRY_RUN" = true ]; then
  echo "Dry run: no files written."
  exit 0
fi

# Training subprocess uses all host cores (no cap in Python); after training subprocess exits, inference stays at default
LSTM_TRAINING_CPUS_VAL=$TOTAL_LOGICAL

# ══════════════════════════════════════════════════════════════════════════════
# 10. WRITE docker-compose.override.yml
# ══════════════════════════════════════════════════════════════════════════════

cat > "$OVERRIDE_FILE" <<OVERRIDE
# Generated by configure-resources.sh
# Host: ${TOTAL_LOGICAL} logical CPUs / ${TOTAL_PHYSICAL} physical cores / ${TOTAL_GB}G RAM
# Available: ${AVAIL_CPUS} CPUs / ${AVAIL_GB}G  |  reserved: ${RESERVE_CPUS}C / ${RESERVE_GB}G (OS+Zeek)
# Pools: compute ${COMPUTE_CPU}C/${COMPUTE_GB}G  |  infra ${INFRA_CPU}C/${INFRA_GB}G
# DO NOT EDIT — re-run ./configure-resources.sh to regenerate

version: '3.8'

services:

  # ── COMPUTE (all: CPU reservations only, soft sharing) ───────────────────

  flink:
    deploy:
      resources:
        limits:
          memory: ${FLINK_MEM}G
        reservations:
          cpus: '${FLINK_CPU}'
          memory: ${FLINK_MEM_RES}G

  lstm-autoencoder:
    environment:
      GUNICORN_WORKERS: '${LSTM_WORKERS}'
      TF_NUM_INTEROP_THREADS: '${TF_INTEROP}'
      TF_NUM_INTRAOP_THREADS: '${TF_INTRAOP_INFERENCE}'
      OMP_NUM_THREADS: '${TF_INTRAOP_INFERENCE}'
      MKL_NUM_THREADS: '${TF_INTRAOP_INFERENCE}'
      LSTM_TRAINING_CPUS: '${LSTM_TRAINING_CPUS_VAL}'
    deploy:
      resources:
        limits:
          memory: ${LSTM_MEM}G
        reservations:
          cpus: '${LSTM_CPU}'
          memory: ${LSTM_MEM_RES}G

  # ── INFRA (CPU reservations only, soft sharing) ─────────────────────────

  zookeeper:
    deploy:
      resources:
        limits:
          memory: ${ZK_MEM}G
        reservations:
          cpus: '${ZK_CPU}'
          memory: ${ZK_MEM_RES}G

  kafka:
    deploy:
      resources:
        limits:
          memory: ${KAFKA_MEM}G
        reservations:
          cpus: '${KAFKA_CPU}'
          memory: ${KAFKA_MEM_RES}G

  supervised:
    deploy:
      resources:
        limits:
          memory: ${SUPERVISED_MEM}G
        reservations:
          cpus: '${SUPERVISED_CPU}'
          memory: ${SUPERVISED_MEM_RES}G

  postgres:
    environment:
      POSTGRES_SHARED_BUFFERS: '${PG_SHARED_BUFFERS}MB'
      POSTGRES_EFFECTIVE_CACHE_SIZE: '${PG_EFF_CACHE}MB'
      POSTGRES_WORK_MEM: '${PG_WORK_MEM}MB'
    command:
      - postgres
      - -c
      - shared_buffers=${PG_SHARED_BUFFERS}MB
      - -c
      - effective_cache_size=${PG_EFF_CACHE}MB
      - -c
      - work_mem=${PG_WORK_MEM}MB
      - -c
      - maintenance_work_mem=256MB
      - -c
      - max_connections=200
      - -c
      - wal_buffers=32MB
      - -c
      - max_wal_size=4GB
      - -c
      - checkpoint_completion_target=0.9
      - -c
      - random_page_cost=1.1
      - -c
      - effective_io_concurrency=300
      - -c
      - synchronous_commit=off
      - -c
      - wal_writer_delay=10ms
      - -c
      - commit_delay=100
      - -c
      - commit_siblings=5
    deploy:
      resources:
        limits:
          memory: ${POSTGRES_MEM}G
        reservations:
          cpus: '${POSTGRES_CPU}'
          memory: ${POSTGRES_MEM_RES}G
OVERRIDE

echo "Wrote ${OVERRIDE_FILE}"

# ══════════════════════════════════════════════════════════════════════════════
# 11. WRITE flink-conf.yaml  (from template, memory only)
# ══════════════════════════════════════════════════════════════════════════════

if [ -f "$FLINK_CONF_TEMPLATE" ]; then
  sed -e "s/__FLINK_JM_MEM_MB__/${FLINK_JM_MEM_MB}/g" \
      -e "s/__FLINK_TM_MEM_MB__/${FLINK_TM_MEM_MB}/g" \
      -e "s/__FLINK_TM_SLOTS__/${FLINK_TM_SLOTS}/g" \
      "$FLINK_CONF_TEMPLATE" > "$FLINK_CONF_OUT"
  echo "Wrote ${FLINK_CONF_OUT} (JM ${FLINK_JM_MEM_MB}m / TM ${FLINK_TM_MEM_MB}m / ${FLINK_TM_SLOTS} slots)"
  [ -n "$FLINK_TM_WARN" ] && echo "$FLINK_TM_WARN"
else
  echo "Warning: ${FLINK_CONF_TEMPLATE} not found; Flink config not generated"
fi

echo ""
echo "Run:  docker compose up -d"
echo ""
