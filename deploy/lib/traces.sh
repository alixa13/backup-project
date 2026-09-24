#!/usr/bin/env bash
# ICSNPP's own published sample traces, pinned by URL and SHA-256, and the
# offline Zeek run over them. Input for the Zeek checks only
# (tests/zeek-fixtures.sh, deploy.sh zeek-check): the platform never reads a pcap.

# name  url  sha256
TRACE_PINS=(
  "modbus_example.pcap https://raw.githubusercontent.com/cisagov/icsnpp-modbus/v1.0.0/tests/traces/modbus_example.pcap a84656f9af62b2c948200ec288d51b81f03037c277a31a40efee0cfb244f1e30"
  "snap7.pcap https://raw.githubusercontent.com/cisagov/icsnpp-s7comm/7ebeb03a0f954541369361651d1c27d09a64b5a3/testing/traces/snap7.pcap 2b91f6a8a203ec83e4f2dbb69d5e61602845f23da0baa31d95d029fb24cbd427"
  "s7ident.pcap https://raw.githubusercontent.com/cisagov/icsnpp-s7comm/7ebeb03a0f954541369361651d1c27d09a64b5a3/testing/traces/s7ident.pcap 7a2ae7f2992669a3eb36a878f0e4f124c86ecdf3fc3a284f78e2e6e1deeea680"
)

# Download each pinned trace into DIR (once) and verify its SHA-256.
fetch_traces() {
  local dir="$1" pin name url sha
  mkdir -p "$dir"
  for pin in "${TRACE_PINS[@]}"; do
    read -r name url sha <<< "$pin"
    [ -f "${dir}/${name}" ] || curl -fsSL -o "${dir}/${name}" "$url"
    printf '%s  %s\n' "$sha" "${dir}/${name}" | sha256sum -c --quiet - \
      || die "${name} does not match its pinned SHA-256; delete ${dir} and retry"
  done
}

# Run IMAGE's Zeek offline (check policy, JSON files) over the traces in
# TRACES_DIR, then gather OUT_DIR/modbus_detailed.jsonl (from modbus_example)
# and OUT_DIR/s7comm.jsonl (snap7, then s7ident). Runs as the invoking user so
# the output stays theirs.
zeek_offline_records() {
  local image="$1" traces="$2" out="$3" trace
  for trace in modbus_example snap7 s7ident; do
    mkdir -p "${out}/runs/${trace}"
    docker run --rm --network none --user "$(id -u):$(id -g)" --entrypoint zeek \
      -v "${traces}:/traces:ro" -v "${out}/runs/${trace}:/work" -w /work \
      "$image" -C -r "/traces/${trace}.pcap" /opt/netsec/offline-json.zeek
  done
  cat "${out}/runs/modbus_example/modbus_detailed.log" > "${out}/modbus_detailed.jsonl"
  cat "${out}/runs/snap7/s7comm.log" "${out}/runs/s7ident/s7comm.log" > "${out}/s7comm.jsonl"
}
