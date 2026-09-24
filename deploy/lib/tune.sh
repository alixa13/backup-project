#!/usr/bin/env bash
# deploy.sh tune: size every service from this host's hardware (design §7,
# rulings P4, P5, P10). tune_compute is pure arithmetic so the tests can pin
# it; the rest detects the hardware and writes the result into deploy/.env.

TUNE_MIN_BUDGET_MIB=4352          # the floors below sum to 4224, plus slack
TUNE_BLOCK_BEGIN="# --- resources: written by deploy.sh tune"
TUNE_BLOCK_END="# --- end of resources ---"

# clamp VALUE FLOOR CEILING
clamp() {
  local value="$1"
  [ "$value" -lt "$2" ] && value="$2"
  [ "$value" -gt "$3" ] && value="$3"
  printf '%s\n' "$value"
}

# A CPU share in hundredths, printed as a Docker cpus value ("1.35"), never
# below 0.50: a limit is a ceiling, and half a core keeps a service responsive.
cpus_value() {
  local hundredths="$1"
  [ "$hundredths" -lt 50 ] && hundredths=50
  printf '%d.%02d\n' $((hundredths / 100)) $((hundredths % 100))
}

# tune_compute CORES MEM_TOTAL_MIB MEM_AVAILABLE_MIB OWN_USAGE_MIB BUDGET_OVERRIDE_MIB CPUS_OVERRIDE FORCE
# Prints KEY=VALUE lines. Returns 2, saying why on stderr, when the memory
# budget is below the minimum and FORCE is not 1. Overrides are 0 when unset.
tune_compute() {
  local cores="$1" total="$2" avail="$3" own="$4" budget_override="$5" cpus_override="$6" force="$7"

  # Memory budget: what is free now plus what our own stack already holds,
  # less a reserve for the OS and the host's other containers, and never more
  # than 80% of the machine.
  local reserve=$(( total * 10 / 100 ))
  [ "$reserve" -lt 1024 ] && reserve=1024
  local budget=$(( avail + own - reserve ))
  local cap=$(( total * 80 / 100 ))
  [ "$budget" -gt "$cap" ] && budget="$cap"
  [ "$budget_override" -gt 0 ] && budget="$budget_override"
  if [ "$budget" -lt "$TUNE_MIN_BUDGET_MIB" ] && [ "$force" != 1 ]; then
    printf 'memory budget %s MiB is below the %s MiB minimum the stack needs: free memory, pass --memory-budget, or --force to try anyway\n' \
      "$budget" "$TUNE_MIN_BUDGET_MIB" >&2
    return 2
  fi

  # Memory split (design §7), each clamped to its floor and ceiling. The
  # JobManager is fixed (P4); each Flink process is its container less 64 MiB (P5).
  local tm ch kafka zeek jm=1024
  tm="$(clamp $(( budget * 40 / 100 )) 1280 16384)"
  ch="$(clamp $(( budget * 25 / 100 )) 768 16384)"
  kafka="$(clamp $(( budget * 15 / 100 )) 768 6144)"
  zeek="$(clamp $(( budget * 10 / 100 )) 384 4096)"

  # CPU: keep a quarter of the cores (at least one) for the host, split the
  # rest; an override may not exceed the host.
  local keep=$(( cores * 25 / 100 ))
  [ "$keep" -lt 1 ] && keep=1
  local usable=$(( cores - keep ))
  [ "$usable" -lt 1 ] && usable=1
  [ "$cpus_override" -gt 0 ] && usable="$cpus_override"
  [ "$usable" -gt "$cores" ] && usable="$cores"

  # Parallelism by core count; two jobs share the TaskManager's slots.
  local parallelism=1
  [ "$cores" -ge 8 ] && parallelism=2
  [ "$cores" -ge 16 ] && parallelism=4

  cat <<EOF
TUNE_DETECTED_CORES=${cores}
TUNE_DETECTED_MEM_TOTAL_MIB=${total}
TUNE_BUDGET_MIB=${budget}
FLINK_TM_MEMORY_MIB=${tm}
FLINK_TM_PROCESS_MIB=$(( tm - 64 ))
FLINK_JM_MEMORY_MIB=${jm}
FLINK_JM_PROCESS_MIB=$(( jm - 64 ))
CLICKHOUSE_MEMORY_MIB=${ch}
KAFKA_MEMORY_MIB=${kafka}
KAFKA_HEAP_MIB=$(( kafka / 2 ))
ZEEK_MEMORY_MIB=${zeek}
FLINK_PARALLELISM=${parallelism}
FLINK_TASK_SLOTS=$(( parallelism * 2 ))
FLINK_TM_CPUS=$(cpus_value $(( usable * 45 )))
CLICKHOUSE_CPUS=$(cpus_value $(( usable * 25 )))
KAFKA_CPUS=$(cpus_value $(( usable * 15 )))
ZEEK_CPUS=$(cpus_value $(( usable * 15 )))
FLINK_JM_CPUS=1.00
EOF
}

# A docker stats memory figure ("1.5GiB", "512MiB", "1GB", "0B") in whole MiB.
mem_to_mib() {
  awk -v s="$1" 'BEGIN {
    n = s + 0; u = s; sub(/^[0-9.]+/, "", u)
    if (u ~ /^Ki/) f = 1 / 1024;          else if (u ~ /^Mi/) f = 1
    else if (u ~ /^Gi/) f = 1024;         else if (u ~ /^Ti/) f = 1048576
    else if (u ~ /^kB/) f = 1e3 / 1048576; else if (u ~ /^MB/) f = 1e6 / 1048576
    else if (u ~ /^GB/) f = 1e9 / 1048576; else f = 1 / 1048576
    printf "%d\n", n * f }'
}

# A --memory-budget size ("12g", "4096m", or bare MiB) in MiB.
size_to_mib() {
  [[ "$1" =~ ^[0-9]+[gGmM]?$ ]] || die "size '$1' must be whole MiB, or end in g or m (e.g. 12g)"
  case "$1" in
    *[gG]) printf '%s\n' $(( ${1%[gG]} * 1024 )) ;;
    *[mM]) printf '%s\n' "${1%[mM]}" ;;
    *) printf '%s\n' "$1" ;;
  esac
}

# The memory our own running containers hold now, in MiB, so re-running tune
# while the stack is up does not count its own usage as unavailable.
own_usage_mib() {
  local ids used total=0
  ids="$(docker ps -q --filter "label=com.docker.compose.project=${COMPOSE_PROJECT}" 2>/dev/null)"
  # No containers of ours: 0. `docker stats` given no ids would list EVERY
  # container on the host and count theirs as ours (Review Focus 5).
  if [ -z "$ids" ]; then
    printf '0\n'
    return 0
  fi
  # shellcheck disable=SC2086  # one container id per word, split on purpose
  while read -r used; do
    total=$(( total + $(mem_to_mib "$used") ))
  done < <(docker stats --no-stream --format '{{.MemUsage}}' $ids | cut -d/ -f1 | tr -d ' ')
  printf '%s\n' "$total"
}

# Replace the tune block in FILE (or append one) with CONTENT, keeping every
# other line and the file's mode (600).
write_tune_block() {
  local file="$1" content="$2" tmp
  tmp="$(mktemp)"
  awk -v b="$TUNE_BLOCK_BEGIN" -v e="$TUNE_BLOCK_END" '
    index($0, b) == 1 { skip = 1; next }
    skip && index($0, e) == 1 { skip = 0; next }
    !skip { print }' "$file" > "$tmp"
  {
    printf '%s %s ---\n' "$TUNE_BLOCK_BEGIN" "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    printf '%s\n' "$content"
    printf '%s\n' "$TUNE_BLOCK_END"
  } >> "$tmp"
  cat "$tmp" > "$file"
  rm -f "$tmp"
}

# The allocation as a table, from tune_compute output $1.
tune_print_table() {
  local r="$1" avail="$2" own="$3"
  _tv() { sed -n "s/^$1=//p" <<< "$r"; }
  log "hardware: $(_tv TUNE_DETECTED_CORES) cores, $(_tv TUNE_DETECTED_MEM_TOTAL_MIB) MiB RAM, ${avail} MiB available now (other containers already excluded; +${own} MiB held by netsec-ml)"
  log "memory budget for netsec-ml: $(_tv TUNE_BUDGET_MIB) MiB"
  printf '  %-18s %10s %7s\n' service memory cpus
  printf '  %-18s %6s MiB %7s\n' flink-taskmanager "$(_tv FLINK_TM_MEMORY_MIB)" "$(_tv FLINK_TM_CPUS)"
  printf '  %-18s %6s MiB %7s\n' flink-jobmanager "$(_tv FLINK_JM_MEMORY_MIB)" "$(_tv FLINK_JM_CPUS)"
  printf '  %-18s %6s MiB %7s\n' clickhouse "$(_tv CLICKHOUSE_MEMORY_MIB)" "$(_tv CLICKHOUSE_CPUS)"
  printf '  %-18s %6s MiB %7s\n' kafka "$(_tv KAFKA_MEMORY_MIB)" "$(_tv KAFKA_CPUS)"
  printf '  %-18s %6s MiB %7s\n' zeek "$(_tv ZEEK_MEMORY_MIB)" "$(_tv ZEEK_CPUS)"
  printf '  flink parallelism %s, task slots %s\n' "$(_tv FLINK_PARALLELISM)" "$(_tv FLINK_TASK_SLOTS)"
}

# deploy.sh tune [--dry-run] [--memory-budget SIZE] [--cpus N] [--force]
tune_run() {
  local dry_run=0 budget_override=0 cpus_override=0 force=0
  while [ $# -gt 0 ]; do
    case "$1" in
      --dry-run) dry_run=1; shift ;;
      --memory-budget) budget_override="$(size_to_mib "${2:?--memory-budget needs a size, e.g. 12g}")"; shift 2 ;;
      --cpus) cpus_override="${2:?--cpus needs a whole number}"; shift 2 ;;
      --force) force=1; shift ;;
      *) die "tune: unknown option $1" ;;
    esac
  done

  # Detect: cores, total RAM, RAM available now, and what our stack holds.
  local cores total avail own result
  cores="$(nproc)"
  total="$(awk '/^MemTotal:/ {print int($2 / 1024)}' /proc/meminfo)"
  avail="$(awk '/^MemAvailable:/ {print int($2 / 1024)}' /proc/meminfo)"
  own="$(own_usage_mib)"
  result="$(tune_compute "$cores" "$total" "$avail" "$own" "$budget_override" "$cpus_override" "$force")" \
    || die "tune refused (see the reason above); fix it, then run 'deploy.sh tune' (its options are in 'deploy.sh help')"

  tune_print_table "$result" "$avail" "$own"
  if [ "$dry_run" -eq 1 ]; then
    log "dry run: deploy/.env not changed"
  else
    write_tune_block "$ENV_FILE" "$result"
    log "resources written to deploy/.env; 'deploy.sh restart' applies them to a running stack"
  fi
}

# Warn when this host's cores or RAM differ from what the last tune recorded;
# fail when there is no resources block at all.
tune_check_drift() {
  local rec_cores rec_total total drift
  rec_cores="$(env_value "$ENV_FILE" TUNE_DETECTED_CORES)"
  rec_total="$(env_value "$ENV_FILE" TUNE_DETECTED_MEM_TOTAL_MIB)"
  if [ -z "$rec_cores" ] || [ -z "$rec_total" ]; then
    die "deploy/.env has no resources block: run 'deploy.sh tune'"
  fi
  total="$(awk '/^MemTotal:/ {print int($2 / 1024)}' /proc/meminfo)"
  drift=$(( (total - rec_total) * 100 / rec_total ))
  if [ "$rec_cores" != "$(nproc)" ] || [ "${drift#-}" -gt 5 ]; then
    warn "hardware changed since the last tune (${rec_cores} cores/${rec_total} MiB then, $(nproc)/${total} now): run 'deploy.sh tune' and 'deploy.sh restart'"
  fi
}
