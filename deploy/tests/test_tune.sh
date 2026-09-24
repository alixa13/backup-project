#!/usr/bin/env bash
# Pins tune_compute's arithmetic (design §7, rulings P4/P5/P10) on seven hosts,
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

# A: 8 cores, 32 GiB, 24 GiB free -> budget = 24576 - 3276 = 21300.
out="$(tune_compute 8 32768 24576 0 0 0 0)"
assert_alloc A "$out" TUNE_BUDGET_MIB=21300 FLINK_TM_MEMORY_MIB=8520 FLINK_TM_PROCESS_MIB=8456 \
  FLINK_JM_MEMORY_MIB=1024 FLINK_JM_PROCESS_MIB=960 CLICKHOUSE_MEMORY_MIB=5325 KAFKA_MEMORY_MIB=3195 \
  KAFKA_HEAP_MIB=1597 ZEEK_MEMORY_MIB=2130 FLINK_PARALLELISM=2 FLINK_TASK_SLOTS=4 \
  FLINK_TM_CPUS=2.70 CLICKHOUSE_CPUS=1.50 KAFKA_CPUS=0.90 ZEEK_CPUS=0.90 FLINK_JM_CPUS=1.00 \
  TUNE_DETECTED_CORES=8 TUNE_DETECTED_MEM_TOTAL_MIB=32768

# B: 4 cores, 8 GiB, 6 GiB free -> budget 5120; Kafka lands on its floor.
out="$(tune_compute 4 8192 6144 0 0 0 0)"
assert_alloc B "$out" TUNE_BUDGET_MIB=5120 FLINK_TM_MEMORY_MIB=2048 CLICKHOUSE_MEMORY_MIB=1280 \
  KAFKA_MEMORY_MIB=768 KAFKA_HEAP_MIB=384 ZEEK_MEMORY_MIB=512 FLINK_PARALLELISM=1 FLINK_TASK_SLOTS=2 \
  FLINK_TM_CPUS=1.35 CLICKHOUSE_CPUS=0.75 KAFKA_CPUS=0.50 ZEEK_CPUS=0.50

# C: the development machine (5.7 GiB, 3 GiB free) -> refused unless forced.
err="$(tune_compute 2 5836 3000 0 0 0 0 2>&1 >/dev/null)"; status=$?
assert_eq 2 "$status" "C: refused with status 2"
assert_eq 1 "$(grep -c 'below the 4352 MiB minimum' <<< "$err")" "C: says why"
out="$(tune_compute 2 5836 3000 0 0 0 1)"
assert_alloc "C forced" "$out" TUNE_BUDGET_MIB=1976 FLINK_TM_MEMORY_MIB=1280 CLICKHOUSE_MEMORY_MIB=768 \
  KAFKA_MEMORY_MIB=768 ZEEK_MEMORY_MIB=384 FLINK_TM_CPUS=0.50 CLICKHOUSE_CPUS=0.50

# D: a budget override replaces the computed budget.
out="$(tune_compute 8 32768 24576 0 10240 0 0)"
assert_alloc D "$out" TUNE_BUDGET_MIB=10240 FLINK_TM_MEMORY_MIB=4096 CLICKHOUSE_MEMORY_MIB=2560 \
  KAFKA_MEMORY_MIB=1536 KAFKA_HEAP_MIB=768 ZEEK_MEMORY_MIB=1024

# E: memory our own running stack holds counts as available.
out="$(tune_compute 4 8192 2000 4000 0 0 0)"
assert_alloc E "$out" TUNE_BUDGET_MIB=4976 FLINK_TM_MEMORY_MIB=1990 CLICKHOUSE_MEMORY_MIB=1244 \
  KAFKA_MEMORY_MIB=768 ZEEK_MEMORY_MIB=497

# F: a large host hits every ceiling; 80% of RAM caps the budget.
out="$(tune_compute 64 262144 250000 0 0 0 0)"
assert_alloc F "$out" TUNE_BUDGET_MIB=209715 FLINK_TM_MEMORY_MIB=16384 CLICKHOUSE_MEMORY_MIB=16384 \
  KAFKA_MEMORY_MIB=6144 KAFKA_HEAP_MIB=3072 ZEEK_MEMORY_MIB=4096 FLINK_PARALLELISM=4 FLINK_TASK_SLOTS=8 \
  FLINK_TM_CPUS=21.60 CLICKHOUSE_CPUS=12.00 KAFKA_CPUS=7.20 ZEEK_CPUS=7.20

# G: --cpus overrides the usable cores, but never beyond the host's.
out="$(tune_compute 8 32768 24576 0 0 4 0)"
assert_alloc G "$out" FLINK_TM_CPUS=1.80 CLICKHOUSE_CPUS=1.00 KAFKA_CPUS=0.60 ZEEK_CPUS=0.60
out="$(tune_compute 4 8192 6144 0 0 16 0)"
assert_alloc "G capped" "$out" FLINK_TM_CPUS=1.80 CLICKHOUSE_CPUS=1.00

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
