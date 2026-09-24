#!/usr/bin/env bash
# Pins stack.sh's decisions without starting anything: the JSON/offset
# filters, the topic loop (Review Focus 3) and the preflight (Review Focus 2).
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
. "$HERE/lib.sh"
. "$HERE/../lib/common.sh"
. "$HERE/../lib/tune.sh"
. "$HERE/../lib/doctor.sh"
. "$HERE/../lib/install.sh"
. "$HERE/../lib/stack.sh"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

# /jobs/overview filters.
overview='{"jobs":[{"jid":"a1","name":"online-feature-job","state":"RUNNING"},
{"jid":"b2","name":"archive-job","state":"RESTARTING"},{"jid":"c3","name":"archive-job","state":"FINISHED"}]}'
assert_eq online-feature-job "$(running_job_names <<< "$overview")" "running job names"
assert_eq "a1 online-feature-job" "$(running_jobs <<< "$overview")" "running jobs with ids"

# kafka-get-offsets output -> total records.
assert_eq 22 "$(printf 't:0:15\nt:1:7\n' | sum_offsets)" "sum_offsets adds partitions"
assert_eq 0 "$(printf '' | sum_offsets)" "sum_offsets of nothing"

# Seconds since the newest completed checkpoint.
assert_eq 42 "$(checkpoint_age 1000042000 <<< '{"latest":{"completed":{"latest_ack_timestamp":1000000000}}}')" "checkpoint age"
assert_eq none "$(checkpoint_age 1 <<< '{"latest":{"completed":null}}')" "no checkpoint yet"

# Epoch milliseconds: exactly 13 digits and within a second of 'date +%s' --
# never a width modifier like %3N, which some date implementations ignore.
ms="$(now_millis)"
assert_eq 1 "$(grep -cE '^[0-9]{13}$' <<< "$ms")" "now_millis is 13 digits"
assert_eq yes "$([ $(( ${ms:0:10} - $(date +%s) )) -le 1 ] && echo yes || echo no)" "now_millis agrees with date +%s"

# Review Focus 3: 'compose exec' reads stdin; inside the topic loop it must not
# swallow topics.conf, or only the first topic is ever created.
set -a; . "${DEPLOY_DIR}/.env.template"; set +a
compose() {
  if [ "$1" = exec ]; then
    cat > /dev/null            # what the real 'docker compose exec -T' does to stdin
    printf '%s\n' "$*" >> "$tmp/calls"
  fi
}
create_topics >/dev/null
assert_eq 12 "$(grep -c -- '--create' "$tmp/calls")" "create_topics creates every topic"
assert_eq 1 "$(grep -c -- '--topic netsec.s7comm.raw.v1 --partitions 1 --replication-factor 1 --config retention.ms=604800000' "$tmp/calls")" "raw topic: 1 partition, 7 days"
unset -f compose

# Review Focus 2: preflight refuses an unknown interface and lists the real ones.
DEPLOY_DIR_SAVED="$DEPLOY_DIR"; DEPLOY_DIR="$tmp/deploy"; mkdir -p "$DEPLOY_DIR/jars"
touch "$DEPLOY_DIR/jars/online-feature-job.jar" "$DEPLOY_DIR/jars/archive-job.jar"
docker() { return 0; }                     # 'docker image inspect' succeeds
list_interfaces() { printf 'lo\neth0\n'; }
out="$( (ZEEK_INTERFACE=eth9; stack_preflight) 2>&1; echo "exit=$?")"
assert_eq 1 "$(grep -c 'capture interface eth9 does not exist (this host has: lo eth0 )' <<< "$out")" "preflight refuses an unknown interface"
assert_eq 1 "$(grep -c 'exit=1' <<< "$out")" "and stops"
out="$( (ZEEK_INTERFACE=""; stack_preflight) 2>&1; echo "exit=$?")"
assert_eq 1 "$(grep -c 'ZEEK_INTERFACE is not set' <<< "$out")" "preflight refuses an empty interface"
rm "$DEPLOY_DIR/jars/archive-job.jar"
out="$( (ZEEK_INTERFACE=eth0; stack_preflight) 2>&1; echo "exit=$?")"
assert_eq 1 "$(grep -c "job JARs missing: run 'deploy.sh build' first" <<< "$out")" "preflight wants the JARs"
DEPLOY_DIR="$DEPLOY_DIR_SAVED"
unset -f docker list_interfaces

finish
