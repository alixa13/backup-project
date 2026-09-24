#!/usr/bin/env bash
# Pins install's env-file creation and doctor's port and interface checks,
# with docker, ss and ip stubbed (nothing is installed, nothing starts).
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
. "$HERE/lib.sh"
. "$HERE/../lib/common.sh"
. "$HERE/../lib/tune.sh"
. "$HERE/../lib/doctor.sh"
. "$HERE/../lib/install.sh"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

# Generated secrets: 32 letters/digits; a 22-character base64url KRaft id.
assert_eq 1 "$(random_token 32 | grep -cE '^[A-Za-z0-9]{32}$')" "random_token shape"
assert_eq 1 "$(kafka_cluster_id | grep -cE '^[A-Za-z0-9_-]{22}$')" "kafka_cluster_id shape"

# create_env_file: mode 600, generated values, the interface; a second run keeps them.
ENV_FILE="$tmp/.env"
create_env_file eth7 >/dev/null 2>&1
assert_eq 600 "$(stat -c %a "$ENV_FILE")" ".env is mode 600"
assert_eq "$(hostname -s)" "$(env_value "$ENV_FILE" SENSOR_ID)" "SENSOR_ID is the host name"
assert_eq eth7 "$(env_value "$ENV_FILE" ZEEK_INTERFACE)" "interface recorded"
password="$(env_value "$ENV_FILE" CLICKHOUSE_PASSWORD)"
assert_eq 32 "${#password}" "ClickHouse password generated"
create_env_file "" >/dev/null 2>&1
assert_eq "$password" "$(env_value "$ENV_FILE" CLICKHOUSE_PASSWORD)" "a second install keeps the password"
assert_eq eth7 "$(env_value "$ENV_FILE" ZEEK_INTERFACE)" "and the interface"

# Review Focus 4: a port held by another process is reported; ours is not.
ss() { [ "$*" = "-Hltn sport = :18081" ] && printf 'LISTEN 0 4096 127.0.0.1:18081 0.0.0.0:*\n'; return 0; }
assert_eq yes "$(port_in_use 18081 && echo yes || echo no)" "port_in_use sees a listener"
assert_eq no "$(port_in_use 18123 && echo yes || echo no)" "port_in_use sees a free port"
docker() {
  case "$1" in
    ps) printf '' ;;
    version) printf '27.0.0\n' ;;
  esac
  return 0
}
curl() { printf '401'; }       # "Docker Hub reachable", without the network
ip() { printf '1: lo: <LOOPBACK>\n2: eth0: <UP>\n3: ens5@if9: <UP>\n'; }
out="$(doctor_run 2>&1)"
assert_eq 1 "$(grep -c 'FAIL  port 18081 (FLINK_UI_PORT) is taken by another process' <<< "$out")" "doctor names the taken port"
assert_eq "lo eth0 ens5" "$(list_interfaces | tr '\n' ' ' | sed 's/ $//')" "interfaces listed without @peer"
assert_eq 1 "$(grep -c 'FAIL  capture interface eth7 does not exist' <<< "$out")" "doctor flags a missing interface"
unset -f ss docker curl ip

finish
