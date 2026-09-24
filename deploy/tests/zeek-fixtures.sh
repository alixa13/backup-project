#!/usr/bin/env bash
# Regenerates tests/fixtures/zeek/ -- what the netsec-ml Zeek image writes for
# ICSNPP's own sample traces -- so ZeekRecordCheckTest pins the parsers
# against real sensor output. Zeek uids are random per run; everything else is
# stable. usage: deploy/tests/zeek-fixtures.sh [IMAGE]  (default netsec-ml/zeek:1)
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
. "$HERE/../lib/common.sh"
. "$HERE/../lib/traces.sh"
image="${1:-netsec-ml/zeek:1}"

# Run Zeek offline over the pinned traces into a scratch directory.
work="$(mktemp -d)"
fetch_traces "${DEPLOY_DIR}/.cache/traces"
zeek_offline_records "$image" "${DEPLOY_DIR}/.cache/traces" "$work"

# Copy the two logs to their fixture names and show the line counts.
out="${REPO_ROOT}/tests/fixtures/zeek"
mkdir -p "$out"
cp "${work}/modbus_detailed.jsonl" "${out}/icsnpp-modbus-v1.0.0_modbus_detailed.jsonl"
cp "${work}/s7comm.jsonl" "${out}/icsnpp-s7comm-7ebeb03_s7comm.jsonl"
rm -rf "$work"
wc -l "${out}"/*.jsonl
