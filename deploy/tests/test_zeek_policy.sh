#!/usr/bin/env bash
# The production sensor policy, run offline (seconds; no stack) against an
# unreachable broker: every Modbus/S7 record must reach the Kafka writer, and
# nothing may be written to local disk. Also pins run-zeek.sh's input checks.
# usage: deploy/tests/test_zeek_policy.sh [IMAGE]   (default netsec-ml/zeek:1)
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
. "$HERE/lib.sh"
. "$HERE/../lib/common.sh"
. "$HERE/../lib/traces.sh"
IMAGE="${1:-netsec-ml/zeek:1}"
TRACES="${DEPLOY_DIR}/.cache/traces"
fetch_traces "$TRACES"

# trace, records the Kafka writer must receive, log path in Zeek's message.
for case in "modbus_example 48 modbus_detailed" "snap7 62 s7comm"; do
  read -r trace n path <<< "$case"
  out="$(mktemp -d)"
  # Port 9 on loopback with no network: the broker is unreachable, so the
  # writer reports exactly how many records it was handed.
  output="$(docker run --rm --network none --user "$(id -u):$(id -g)" \
    -e ZEEK_READ_FILE="/traces/${trace}.pcap" -e NETSEC_KAFKA_BROKERS=127.0.0.1:9 \
    -v "${TRACES}:/traces:ro" -v "${out}:/tmp" "$IMAGE" 2>&1)"
  assert_eq 1 "$(grep -cF "${path}/Log::WRITER_KAFKAWRITER: Unable to deliver ${n} message(s)" <<< "$output")" \
    "${trace}: all ${n} records reach the Kafka writer"
  assert_eq 0 "$(find "$out" -name '*.log' | wc -l)" "${trace}: no local log files"
  rm -rf "$out"
done

# A broker list that could break out of the Zeek string literal is refused.
bad="$(docker run --rm --network none -e 'NETSEC_KAFKA_BROKERS=x";evil' -e ZEEK_READ_FILE=/dev/null "$IMAGE" 2>&1; echo "exit=$?")"
assert_eq 1 "$(grep -c 'invalid NETSEC_KAFKA_BROKERS' <<< "$bad")" "unsafe broker list refused"
assert_eq 1 "$(grep -c 'exit=2' <<< "$bad")" "unsafe broker list exits 2"

finish
