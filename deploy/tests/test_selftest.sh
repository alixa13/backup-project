#!/usr/bin/env bash
# Pins the selftest's records: rendered, they are valid JSON with the run's
# uid and ordered timestamps, and the PRODUCTION parsers accept all four
# (ZeekRecordCheck from the built online JAR, on the host's Java 21) -- so a
# selftest failure on the server is never the test records' own fault.
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
. "$HERE/lib.sh"
. "$HERE/../lib/common.sh"
. "$HERE/../lib/stack.sh"
. "$HERE/../lib/traces.sh"
. "$HERE/../lib/selftest.sh"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

# Timestamps: microseconds, and the response 4 ms after the request.
assert_eq 1 "$(now_epoch | grep -cE '^[0-9]{10}\.[0-9]{6}$')" "now_epoch has microseconds"
assert_eq 1790000000.127456 "$(plus_ms 1790000000.123456 4)" "plus_ms"

# Rendering fills every placeholder.
render_selftest "${DEPLOY_DIR}/selftest/modbus.jsonl.template" SELFTEST-1-M 1790000000.123456 1790000000.127456 > "$tmp/modbus_detailed.jsonl"
render_selftest "${DEPLOY_DIR}/selftest/s7comm.jsonl.template" SELFTEST-1-S 1790000000.123456 1790000000.127456 > "$tmp/s7comm.jsonl"
assert_eq 0 "$(grep -c '__' "$tmp/modbus_detailed.jsonl" "$tmp/s7comm.jsonl" | awk -F: '{s += $2} END {print s}')" "no placeholder left"
assert_eq "SELFTEST-1-M SELFTEST-1-M" "$(jq -r .uid "$tmp/modbus_detailed.jsonl" | tr '\n' ' ' | sed 's/ $//')" "modbus uid"
assert_eq true "$(jq -s '.[1].ts > .[0].ts' "$tmp/s7comm.jsonl")" "response after request"

# The production parsers accept all four records.
jars=("${REPO_ROOT}"/modules/bootstrap-online-job/target/bootstrap-online-job-*-all.jar)
jar="${jars[0]}"
out="$(java -cp "$jar" io.netsecml.platform.bootstrap.online.ZeekRecordCheck \
  --modbus "$tmp/modbus_detailed.jsonl" --s7comm "$tmp/s7comm.jsonl")"; status=$?
assert_eq 0 "$status" "ZeekRecordCheck passes the selftest records"
assert_eq 1 "$(grep -c '2 records, 2 accepted, 0 rejected' <<< "$(grep '^modbus' <<< "$out")")" "both modbus accepted"
assert_eq 1 "$(grep -c '2 records, 2 accepted, 0 rejected' <<< "$(grep '^s7comm' <<< "$out")")" "both s7comm accepted"

# A trace that does not match its pin is refused.
mkdir -p "$tmp/traces"; printf 'not a pcap' > "$tmp/traces/modbus_example.pcap"
out="$( (fetch_traces "$tmp/traces") 2>&1; echo "exit=$?")"
assert_eq 1 "$(grep -c 'does not match its pinned SHA-256' <<< "$out")" "tampered trace refused"

finish
