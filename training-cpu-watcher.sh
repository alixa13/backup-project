#!/usr/bin/env bash
#
# DEPRECATED: The stack now uses CPU reservations (no hard limit) for lstm-autoencoder,
# so the container automatically uses idle cores and yields when others need CPU. This
# script is kept for reference or environments that cannot use the shares approach.
#
# When LSTM training starts, expand container CPUs to all host cores;
# when training finishes, restore to normal limit. Reads ./training-signals/.
#

set -e
SIGNALS_DIR="${1:-./training-signals}"
CONTAINER="${LSTM_CONTAINER:-lstm-autoencoder}"
RESTORE_CPUS_FILE="$SIGNALS_DIR/.restore_cpus"
POLL_INTERVAL="${POLL_INTERVAL:-2}"

mkdir -p "$SIGNALS_DIR"

get_restore_cpus() {
  if [ -f "$RESTORE_CPUS_FILE" ] && [ -r "$RESTORE_CPUS_FILE" ]; then
    local n
    n=$(cat "$RESTORE_CPUS_FILE" 2>/dev/null | tr -d ' \n' | head -c 8)
    if [ -n "$n" ] && [ "$n" -ge 1 ] 2>/dev/null; then
      echo "$n"
      return
    fi
  fi
  echo "16"
}

expand_cpus() {
  local all_cpus
  all_cpus=$(nproc 2>/dev/null || echo 64)
  if ! docker update --cpus="$all_cpus" "$CONTAINER" 2>/dev/null; then
    echo "training-cpu-watcher: docker update --cpus=$all_cpus $CONTAINER failed (container may not be running)" >&2
    return 1
  fi
  echo "training-cpu-watcher: expanded $CONTAINER to $all_cpus CPUs"
}

restore_cpus() {
  local cpus
  cpus=$(get_restore_cpus)
  if ! docker update --cpus="$cpus" "$CONTAINER" 2>/dev/null; then
    echo "training-cpu-watcher: docker update --cpus=$cpus $CONTAINER failed" >&2
    return 1
  fi
  echo "training-cpu-watcher: restored $CONTAINER to $cpus CPUs"
}

while true; do
  if [ -f "$SIGNALS_DIR/expand" ]; then
    rm -f "$SIGNALS_DIR/expand"
    expand_cpus || true
  fi
  if [ -f "$SIGNALS_DIR/restore" ]; then
    rm -f "$SIGNALS_DIR/restore"
    restore_cpus || true
  fi
  sleep "$POLL_INTERVAL"
done
