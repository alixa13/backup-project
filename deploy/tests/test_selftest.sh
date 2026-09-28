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


# Review finding (selftest leftover): with scoring on, the selftest waits for
# its Modbus pair's two predictions -- which arrive through their own archive
# chain, possibly a checkpoint later than the vectors -- then deletes them with
# its other rows. With scoring off it waits for none.
ENV_FILE_SAVED="$ENV_FILE"; ENV_FILE="$tmp/selftest.env"
printf 'MODBUS_DETECTOR_BUNDLE=modbus-stage1-detector/v1\n' > "$ENV_FILE"
export MODBUS_RAW_TOPIC=netsec.modbus.raw.v1 S7COMM_RAW_TOPIC=netsec.s7comm.raw.v1
load_env() { :; }
sleep() { :; }
compose() { cat > /dev/null; }
ch_query() {
  printf '%s\n' "$1" >> "$tmp/queries"
  case "$1" in
    *"FROM modbus_detector_predictions"*)
      # None on the first poll, both on the next.
      if [ -f "$tmp/pred-ready" ]; then printf '2\n'; else touch "$tmp/pred-ready"; printf '0\n'; fi ;;
    *"FROM feature_vectors"*) printf '2\t2\n' ;;
    *"FROM invalid_events"*) printf '0\n' ;;
  esac
}
: > "$tmp/queries"
out="$(selftest_run 2>&1)"
assert_eq 1 "$(grep -c 'selftest PASSED: 2 Modbus and 2 S7comm feature vectors and 2 Modbus predictions reached ClickHouse' <<< "$out")" "scoring on: the predictions are waited for and reported"
assert_eq 2 "$(grep -c 'SELECT count() FROM modbus_detector_predictions' "$tmp/queries")" "polling until they arrive"
assert_eq 1 "$(grep -c "ALTER TABLE modbus_detector_predictions DELETE WHERE connection_uid = 'SELFTEST-[0-9]*-M'" "$tmp/queries")" "and deleted with the other test rows"
printf 'MODBUS_DETECTOR_BUNDLE=\n' > "$ENV_FILE"; rm -f "$tmp/pred-ready"; : > "$tmp/queries"
out="$(selftest_run 2>&1)"
assert_eq 1 "$(grep -c 'selftest PASSED: 2 Modbus and 2 S7comm feature vectors reached ClickHouse, no DLQ rows, Modbus scoring off' <<< "$out")" "scoring off: passes without predictions"
assert_eq 0 "$(grep -c 'SELECT count() FROM modbus_detector_predictions' "$tmp/queries")" "and never waits for them"
ENV_FILE="$ENV_FILE_SAVED"
unset -f load_env sleep compose ch_query

finish
