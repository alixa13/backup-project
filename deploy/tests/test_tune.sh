#!/usr/bin/env bash
# Pins tune_compute's arithmetic (design §7 as corrected 2026-09-24, rulings
# P4/P5/P10) on seven hosts,
# plus the size parsers, the .env block writer and own_usage_mib.
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
. "$HERE/lib.sh"
. "$HERE/../lib/common.sh"
. "$HERE/../lib/tune.sh"

# The value of KEY in tune_compute output $1.
v() { sed -n "s/^$2=//p" <<< "$1"; }

# assert_alloc NAME OUTPUT KEY=VALUE... -- every listed key has that value.
assert_alloc() {
  local name="$1" out="$2" pair; shift 2
  for pair in "$@"; do
    assert_eq "${pair#*=}" "$(v "$out" "${pair%%=*}")" "${name}: ${pair%%=*}"
  done
}

# A: 8 cores, 32 GiB, 24 GiB free -> budget = 24576 - 3276 = 21300; the split
# is what is left after the JobManager and the job supervisor: 19636.
out="$(tune_compute 8 32768 24576 0 0 0 0)"
assert_alloc A "$out" TUNE_BUDGET_MIB=21300 FLINK_TM_MEMORY_MIB=7854 FLINK_TM_PROCESS_MIB=7790 \
  FLINK_JM_MEMORY_MIB=1024 FLINK_JM_PROCESS_MIB=960 CLICKHOUSE_MEMORY_MIB=4909 KAFKA_MEMORY_MIB=2945 \
  KAFKA_HEAP_MIB=1472 ZEEK_MEMORY_MIB=1963 JOB_SUBMITTER_MEMORY_MIB=640 FLINK_PARALLELISM=2 FLINK_TASK_SLOTS=4 \
  FLINK_TM_CPUS=2.10 CLICKHOUSE_CPUS=1.50 KAFKA_CPUS=0.90 ZEEK_CPUS=0.90 FLINK_JM_CPUS=0.60 \
  TUNE_DETECTED_CORES=8 TUNE_DETECTED_MEM_TOTAL_MIB=32768

# B: 4 cores, 8 GiB, 6 GiB free -> budget 5120, split 3456; Kafka and Zeek
# land on their floors, and the five CPU shares fill the 3 usable cores.
out="$(tune_compute 4 8192 6144 0 0 0 0)"
assert_alloc B "$out" TUNE_BUDGET_MIB=5120 FLINK_TM_MEMORY_MIB=1382 CLICKHOUSE_MEMORY_MIB=864 \
  KAFKA_MEMORY_MIB=768 KAFKA_HEAP_MIB=384 ZEEK_MEMORY_MIB=384 FLINK_PARALLELISM=1 FLINK_TASK_SLOTS=2 \
  FLINK_TM_CPUS=1.05 CLICKHOUSE_CPUS=0.75 KAFKA_CPUS=0.45 ZEEK_CPUS=0.45 FLINK_JM_CPUS=0.30

# C: the development machine (5.7 GiB, 3 GiB free) -> refused unless forced.
err="$(tune_compute 2 5836 3000 0 0 0 0 2>&1 >/dev/null)"; status=$?
assert_eq 2 "$status" "C: refused with status 2"
assert_eq 1 "$(grep -c 'below the 5000 MiB minimum' <<< "$err")" "C: says why"
out="$(tune_compute 2 5836 3000 0 0 0 1)"
assert_alloc "C forced" "$out" TUNE_BUDGET_MIB=1976 FLINK_TM_MEMORY_MIB=1280 CLICKHOUSE_MEMORY_MIB=768 \
  KAFKA_MEMORY_MIB=768 ZEEK_MEMORY_MIB=384 FLINK_TM_CPUS=0.35 CLICKHOUSE_CPUS=0.25

# D: a budget override replaces the computed budget.
out="$(tune_compute 8 32768 24576 0 10240 0 0)"
assert_alloc D "$out" TUNE_BUDGET_MIB=10240 FLINK_TM_MEMORY_MIB=3430 CLICKHOUSE_MEMORY_MIB=2144 \
  KAFKA_MEMORY_MIB=1286 KAFKA_HEAP_MIB=643 ZEEK_MEMORY_MIB=857

# E: memory our own running stack holds counts as available (2100 free alone
# would be refused).
out="$(tune_compute 4 8192 2100 4000 0 0 0)"
assert_alloc E "$out" TUNE_BUDGET_MIB=5076 FLINK_TM_MEMORY_MIB=1364 CLICKHOUSE_MEMORY_MIB=853 \
  KAFKA_MEMORY_MIB=768 ZEEK_MEMORY_MIB=384

# F: a large host hits every ceiling; 80% of RAM caps the budget.
out="$(tune_compute 64 262144 250000 0 0 0 0)"
assert_alloc F "$out" TUNE_BUDGET_MIB=209715 FLINK_TM_MEMORY_MIB=16384 CLICKHOUSE_MEMORY_MIB=16384 \
  KAFKA_MEMORY_MIB=6144 KAFKA_HEAP_MIB=3072 ZEEK_MEMORY_MIB=4096 FLINK_PARALLELISM=4 FLINK_TASK_SLOTS=8 \
  FLINK_TM_CPUS=16.80 CLICKHOUSE_CPUS=12.00 KAFKA_CPUS=7.20 ZEEK_CPUS=7.20 FLINK_JM_CPUS=1.00

# G: --cpus overrides the usable cores, but never beyond the host's.
out="$(tune_compute 8 32768 24576 0 0 4 0)"
assert_alloc G "$out" FLINK_TM_CPUS=1.40 CLICKHOUSE_CPUS=1.00 KAFKA_CPUS=0.60 ZEEK_CPUS=0.60 FLINK_JM_CPUS=0.40
out="$(tune_compute 4 8192 6144 0 0 16 0)"
assert_alloc "G capped" "$out" FLINK_TM_CPUS=1.40 CLICKHOUSE_CPUS=1.00

# Final review, Important 3: everything tune hands out -- the four split
# services, the JobManager and the job supervisor -- fits inside the budget it
# reports, and the CPU limits inside the usable cores, on every host that
# passes the minimum; and at exactly the minimum budget.
sum_mib() {
  local out="$1" key value total=0
  shift
  for key in "$@"; do value="$(v "$out" "$key")"; total=$(( total + ${value:-0} )); done
  printf '%s\n' "$total"
}
cpu_hundredths() {
  local out="$1" key total=0
  shift
  for key in "$@"; do total=$(( total + $(awk -v c="$(v "$out" "$key")" 'BEGIN { printf "%d", c * 100 + 0.5 }') )); done
  printf '%s\n' "$total"
}
MEM_KEYS=(FLINK_TM_MEMORY_MIB FLINK_JM_MEMORY_MIB CLICKHOUSE_MEMORY_MIB KAFKA_MEMORY_MIB ZEEK_MEMORY_MIB JOB_SUBMITTER_MEMORY_MIB)
CPU_KEYS=(FLINK_TM_CPUS FLINK_JM_CPUS CLICKHOUSE_CPUS KAFKA_CPUS ZEEK_CPUS)
for total in 8192 16384 32768 65536 262144; do
  for cores in 4 8 16 64; do
    out="$(tune_compute "$cores" "$total" $(( total * 75 / 100 )) 0 0 0 0 2>/dev/null)" || continue
    mem="$(sum_mib "$out" "${MEM_KEYS[@]}")"
    budget="$(v "$out" TUNE_BUDGET_MIB)"
    assert_eq yes "$([ "$mem" -le "$budget" ] && echo yes || echo "no: ${mem} > ${budget}")" "memory fits the budget (${cores} cores, ${total} MiB)"
    keep=$(( cores * 25 / 100 )); [ "$keep" -lt 1 ] && keep=1
    cpu="$(cpu_hundredths "$out" "${CPU_KEYS[@]}")"
    assert_eq yes "$([ "$cpu" -le $(( (cores - keep) * 100 )) ] && echo yes || echo "no: ${cpu} > $(( (cores - keep) * 100 ))")" "CPU fits the usable cores (${cores} cores, ${total} MiB)"
  done
done
out="$(tune_compute 4 8192 6144 0 "$TUNE_MIN_BUDGET_MIB" 0 0)"
assert_eq yes "$([ "$(sum_mib "$out" "${MEM_KEYS[@]}")" -le "$TUNE_MIN_BUDGET_MIB" ] && echo yes || echo no)" "memory fits at exactly the minimum budget"
assert_eq 640 "$(v "$out" JOB_SUBMITTER_MEMORY_MIB)" "the job supervisor's memory is part of the allocation"

# docker stats units -> MiB.
assert_eq 1536 "$(mem_to_mib 1.5GiB)" "mem_to_mib GiB"
assert_eq 512 "$(mem_to_mib 512MiB)" "mem_to_mib MiB"
assert_eq 2 "$(mem_to_mib 2048KiB)" "mem_to_mib KiB"
assert_eq 0 "$(mem_to_mib 0B)" "mem_to_mib bytes"
assert_eq 953 "$(mem_to_mib 1GB)" "mem_to_mib decimal GB"

# --memory-budget sizes -> MiB.
assert_eq 12288 "$(size_to_mib 12g)" "size_to_mib g"
assert_eq 4096 "$(size_to_mib 4096m)" "size_to_mib m"
assert_eq 5000 "$(size_to_mib 5000)" "size_to_mib bare MiB"
out="$( (size_to_mib 1.5g) 2>&1; echo "exit=$?")"
assert_eq 1 "$(grep -c 'exit=1' <<< "$out")" "size_to_mib refuses a fraction"

# write_tune_block replaces the old block, keeps every other line, once.
tmp="$(mktemp)"
printf 'A=1\n%s 2026-01-01 ---\nOLD=1\n%s\nB=2\n' "$TUNE_BLOCK_BEGIN" "$TUNE_BLOCK_END" > "$tmp"
write_tune_block "$tmp" "NEW=2"
assert_eq 1 "$(grep -c '^A=1$' "$tmp")" "block writer keeps A"
assert_eq 1 "$(grep -c '^B=2$' "$tmp")" "block writer keeps B"
assert_eq 0 "$(grep -c '^OLD=' "$tmp")" "block writer drops the old block"
assert_eq 1 "$(grep -c '^NEW=2$' "$tmp")" "block writer adds the new block"
assert_eq 1 "$(grep -cF "$TUNE_BLOCK_BEGIN" "$tmp")" "exactly one block"
rm -f "$tmp"

# Review Focus 5: with no netsec-ml containers, `docker stats` must not be asked
# (given no ids it lists EVERY container on the host).
docker() {
  case "$1" in
    ps) printf '' ;;
    stats) printf '9GiB / 10GiB\n' ;;
  esac
}
assert_eq 0 "$(own_usage_mib)" "own_usage_mib is 0 with no netsec-ml containers"
docker() {
  case "$1" in
    ps) printf 'aaa\nbbb\n' ;;
    stats) printf '1GiB / 2GiB\n512MiB / 1GiB\n' ;;
  esac
}
assert_eq 1536 "$(own_usage_mib)" "own_usage_mib sums our containers"
unset -f docker

finish
