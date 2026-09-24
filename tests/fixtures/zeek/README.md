# Real Zeek output (ICSNPP)

What the netsec-ml Zeek image (`deploy/zeek/Dockerfile`: Zeek 7.0.9,
icsnpp-modbus v1.0.0, icsnpp-s7comm 7ebeb03) writes for ICSNPP's own published
sample traces, with the check policy `deploy/zeek/offline-json.zeek` — the same
JSON the sensor sends to Kafka.

| File | Trace | Records |
|---|---|---|
| `icsnpp-modbus-v1.0.0_modbus_detailed.jsonl` | icsnpp-modbus v1.0.0 `tests/traces/modbus_example.pcap` | 48 |
| `icsnpp-s7comm-7ebeb03_s7comm.jsonl` | icsnpp-s7comm 7ebeb03 `testing/traces/snap7.pcap`, then `s7ident.pcap` | 62 + 22 |

Regenerate with `deploy/tests/zeek-fixtures.sh` (uids change, nothing else).
`ZeekRecordCheckTest` reads them; the traces themselves are never committed.
