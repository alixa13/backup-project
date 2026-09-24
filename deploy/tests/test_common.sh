#!/usr/bin/env bash
# Pins common.sh's env-file editing, path resolution and address helpers.
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
. "$HERE/lib.sh"
. "$HERE/../lib/common.sh"

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

# env_set replaces an existing key in place and appends a new one.
printf 'A=1\nB=2\n' > "$tmp/e"
env_set "$tmp/e" A 9
env_set "$tmp/e" C 3
assert_eq "$(printf 'A=9\nB=2\nC=3')" "$(cat "$tmp/e")" "env_set replaces and appends"
assert_eq 2 "$(env_value "$tmp/e" B)" "env_value reads a key"
assert_eq "" "$(env_value "$tmp/e" MISSING)" "env_value of a missing key is empty"

# Relative data paths resolve against deploy/, like compose does.
assert_eq "${DEPLOY_DIR}/data" "$(data_dir_abs ./data)" "relative data dir"
assert_eq /srv/netsec "$(data_dir_abs /srv/netsec)" "absolute data dir"

# host_addr: loopback when bound everywhere, else the bind address itself.
assert_eq 127.0.0.1 "$(BIND_ADDRESS=0.0.0.0 host_addr)" "0.0.0.0 -> loopback"
assert_eq 10.1.2.3 "$(BIND_ADDRESS=10.1.2.3 host_addr)" "a specific bind address"

# load_env without a deploy/.env stops with the way forward.
out="$( (ENV_FILE="$tmp/none"; load_env) 2>&1; echo "exit=$?")"
assert_eq 1 "$(grep -c "run './deploy/deploy.sh install" <<< "$out")" "load_env names install"
assert_eq 1 "$(grep -c 'exit=1' <<< "$out")" "load_env exits 1"

finish
