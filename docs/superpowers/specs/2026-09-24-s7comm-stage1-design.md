# S7comm Stage 1 — Design

**Status:** agreed in conversation 2026-09-24, section by section; written here for review.
**Branch:** `feat/s7comm-stage1`, cut from `feat/modbus-stage1` at `8770a08`; its PR stacks on PR #4.
**Scope in one line:** ICSNPP `s7comm.log` records from Kafka → `s7comm-feature-v1`, exactly the 16
raw causal features the upstream model team froze, keyed per `(sensor, uid)`, archived to
ClickHouse. No scoring.

## 1. The upstream authority

The model team's S7comm project (`s7comm-zeek-anomaly-v2`) froze two models on one raw feature
contract. Its material is in `two-models-info/` (untracked; never staged). This design was written
against these exact files:

| File | SHA-256 | Role |
|---|---|---|
| `S7Comm_Frozen_Models_Feature_Engineering_Specification.docx` | `a1bb7ae5…a037` | The frozen contract, in prose |
| `S7___/customer_icsnpp_time_normalized_builder.py` | `65a03719…7fb4` | Top-level V4 builder; computes 3 of the 16 |
| `S7___/customer_icsnpp_enriched_builder.py` | `3aa48d96…3c99` | Wrapped by the above; computes the other 13 |
| `S7___/kafka_source.py` | `bedae0d6…9aca` | Production Zeek/Kafka normalisation into `CanonicalS7Event` |
| `S7___/events.py` | `60413956…72ae` | `CanonicalS7Event` |
| `S7___/s7_parser.py` | `8ef6bbde…77fe` | `FUNCTION_NAMES` (the builders import it) |
| `S7___/90_attack_type_mode_router_v2.py` | `fa6308ca…9e01` | `STAGE1_RAW_FEATURES`, the frozen order |

Where this document and those files disagree, **the Python wins**, except for the deliberate
deviations listed in §9, each argued there.

Both frozen models consume the same 16 raw features: Stage 1 (`v4_causal_final_r1`, an LSTM
autoencoder over 16-event sequences, one-hot for the two categoricals, 21 transformed inputs) and
Stage 2 (`attack_mode_router_v1_r2`, a Random Forest COMMAND/FLOODING router, ordinal-encoded
categoricals). One vector per event therefore serves both. Upstream's runtime is
"raw 16-feature row → fitted preprocessor → float32 → ONNX" (`103_freeze_attack_mode_router_v1.py`,
the `runtime` field); the fitted preprocessors (`preprocessor.joblib`, the Stage 1 category
contract) were not supplied.

## 2. What is in scope, and what is not

In scope: the source contract, parser, mapper, event, schema, causal state, extractor, use case,
Flink operators, both jobs' wiring, archive to the existing ClickHouse tables, and the tests in §11.

Out of scope, each for a stated reason:
- **Scoring, and Stage 1's 16-event sequence assembly** — a scoring unit, as for Modbus. This unit
  does implement and test the categorical *decode* rule (§4.2) that the scorer will call.
- **The three-log merge** planned by `2026-09-10-per-protocol-feature-schemas-design.md` §4.5/§6.1
  (`s7comm` + `s7comm_upload_download` + `s7comm_plus`). The frozen contract reads only
  `s7comm.log` fields, so it supersedes that plan exactly as Modbus's frozen contract replaced the
  common tier. No merge window, no second topic.
- **Research fields** (`parameter_length`, `db_number`, `address_*`, `data_value_hex`, …) — excluded
  from both frozen models by the upstream spec's §6.1.
- **PCAP** — upstream's `pcap_source.py` and `06_build_full_canonical.py` are offline development
  helpers. This platform consumes Zeek only.

## 3. Preprocessing lives in the ONNX graph

As agreed for Modbus on 2026-09-21: the platform emits RAW features, and the model team re-exports
their preprocessing inside the ONNX graph. For S7 that includes their fitted one-hot (Stage 1) and
ordinal (Stage 2) encoders, which is why the two categoricals travel as codes (§4.2) rather than
encoded values.

## 4. The 16 features

`s7comm-feature-v1`, exactly `STAGE1_RAW_FEATURES` in order. No common tier (CLAUDE.md's scope
clause for schemas that mirror an externally frozen contract). Every feature is read AFTER the
current event has been applied to the state, as upstream does — so, unlike Modbus,
`s7_outstanding_requests` counts the current request.

| # | Feature | Kind | Value |
|---|---|---|---|
| 0 | `s7_outstanding_requests` | continuous | outstanding PDU references after this event |
| 1 | `s7_outstanding_mean_16` | continuous | mean of the last ≤16 values of feature 0 |
| 2 | `s7_response_match_rate_16` | continuous | matched / count over the last ≤16 responses; 0 if none |
| 3 | `s7_same_function_run_length` | continuous | current run of requests with the same function code; 0 before any |
| 4 | `s7_same_direction_run_length` | continuous | current run of events in the same direction |
| 5 | `s7_request_ratio_16` | continuous | requests / count over the last ≤16 events |
| 6 | `s7_direction_change_rate_16` | continuous | mean of the last ≤16 direction-change flags |
| 7 | `s7_function_change_rate_16` | continuous | mean of the last ≤16 function-change flags |
| 8 | `s7_function_entropy_16` | continuous | normalised entropy of the last ≤16 request function codes |
| 9 | `s7_function_transition_entropy_16` | continuous | normalised entropy of the last ≤16 request-to-request transitions |
| 10 | `s7_rosctr_change_rate_16` | continuous | mean of the last ≤16 ROSCTR-change flags |
| 11 | `s7_pdu_reference_unique_ratio_32` | continuous | distinct / count over the last ≤32 request PDU references |
| 12 | `is_request_direction` | binary | 1 iff destination port = 102 |
| 13 | `s7_function_changed` | binary | 1 iff this request's function differs from the previous request's |
| 14 | `s7_rosctr` | categorical | code (§4.2) |
| 15 | `s7_operation` | categorical | code (§4.2) |

### 4.1 Per-event update, in upstream's order

For one event on one key (`is_request` = destination port is 102):

1. **PDU matching** (enriched builder). A request inserts its PDU reference into `outstanding`
   (a re-used reference just stays in). A response removes its reference; `matched` = 1 iff it was
   there. Every response appends `matched` to the response-match history (16).
2. **Request function** (enriched builder), only for a request that carries a function code:
   `run` = 1 if there was no previous request function, `run + 1` if equal, else 1 with
   `function_changed` = 1; append the code to the function history (16) and `function_changed` to
   the function-change history (16); remember the code. Responses, and requests without a code,
   leave `run` unchanged and set `function_changed` = 0.
3. **ROSCTR** (enriched builder), only when the record carries a ROSCTR code: changed = 1 iff a
   previous ROSCTR exists and differs; append to the ROSCTR-change history (16); remember it.
4. **Direction history** (enriched builder): append `is_request` to the request history (16).
5. **Outstanding history** (enriched builder): append `|outstanding|` (after step 1) (16).
6. **PDU uniqueness** (enriched builder), requests only: append `pdu & 0xFFFF` to the PDU history (32).
7. **Direction run** (V4 wrapper): no previous direction → run = 1, changed = 0; same → run + 1,
   changed = 0; different → run = 1, changed = 1. Append `changed` to the direction-change history
   (16) — the first event appends 0.
8. **Transition** (V4 wrapper), requests with a function code: if a previous request function
   exists, append the pair `(previous, current)` to the transition history (16); remember current.
   (The wrapper keeps its own "previous request function"; it is updated under exactly the same
   condition as step 2's, so the two are always equal and the state keeps one field.)

Means and ratios are integer sums over a count and match Python exactly. **Entropy** is
`_normalized_entropy`: 0 for ≤1 value; otherwise `H = -Σ (c/n)·log2(c/n)` summed over the
`Counter` in first-occurrence order, divided by `log2(max(2, min(16, n)))`. Java sums in the same
order; see §9 R7 for `log2`.

### 4.2 The two categoricals travel as codes

Upstream's categories are strings: `s7_rosctr = _cat(rosctr_code)` (`"1"`, `"3"`, `"7"`,
`"__MISSING__"`) and `s7_operation = _operation(event)`. The vector is float32, so each carries a
code that decodes back to upstream's exact string:

| Index | Encode | Decode (upstream's rule) |
|---|---|---|
| 14 `s7_rosctr` | ROSCTR code; missing → `-1` | `-1` → `"__MISSING__"`; `n` → `str(n)` |
| 15 `s7_operation` | function code; missing code but `upper(function_name)` is one of the 11 `FUNCTION_NAMES` values → that code; missing code with any other non-empty name → `-2`; both missing → `-1` | `-1` → `"__MISSING__"`; `-2` → `"__UNSEEN__"`; `n` → `FUNCTION_NAMES[n]`, else `"FUNCTION_0x%02X"` |

`FUNCTION_NAMES` (from `s7_parser.py`): 0x04 READ_VAR, 0x05 WRITE_VAR, 0x1A REQUEST_DOWNLOAD,
0x1B DOWNLOAD_BLOCK, 0x1C DOWNLOAD_ENDED, 0x1D START_UPLOAD, 0x1E UPLOAD, 0x1F END_UPLOAD,
0x28 PI_SERVICE, 0x29 PLC_STOP, 0xF0 SETUP_COMMUNICATION.

The decode lives in `domain` as a pure function beside the encode, so the scorer and the tests
share one implementation. `"__UNSEEN__"` is not a trained category: both upstream encoders treat
it as unseen (all-zero one-hot; ordinal −1), which is exactly what they do with an upper-cased
name absent from training — see §9 R6 for when that is not exact.

## 5. Source contract

`contracts/source/zeek-s7comm-source-v1.json`. Field resolution mirrors `kafka_source.py`,
including its aliases (first present, non-empty value wins):

| Canonical | Accepted fields |
|---|---|
| `ts` | `ts` (required, finite) |
| `uid` | `uid` (required — §9 R1) |
| per-packet endpoints | `source_h`/`src_h`/`src`, `source_p`/`src_p`, `destination_h`/`dst_h`/`dst`, `destination_p`/`dst_p` |
| connection endpoints | `id_orig_h`, `id.orig_h`, nested `id.orig_h`, `orig_h` (likewise `_p`, `resp_h`, `resp_p`) |
| `is_orig` | bool, 0/1, or `true t 1 yes y orig originator` / `false f 0 no n resp responder` |
| `rosctr_code` | `rosctr_code`, `rosctr` — integer or string (`int(text, 0)`: `"3"`, `"0x03"`) |
| `pdu_reference` | `pdu_reference`, `pdu_ref`, `pdu_ref_num` (required — §9 R2) |
| `function_code` | `function_code`, `function` — integer or string as above |
| `function_name` | `function_name` (used only when no function code) |

Integer fields are parsed exactly as upstream's `_int_or_none`: a JSON integer as is; a string as
a Python integer literal (`int(text, 0)`: decimal, `0x`/`0o`/`0b` prefixes), falling back to
`int(float(text))` (truncation); empty means absent; anything else is a DLQ rejection. Ports are
integers by the same rule.

The underscored `id_orig_h` family is not in upstream's list; it is what this platform's sensor
emits (proven by the conn/dns/modbus E2E tests), with the same meaning.

**Endpoints and direction — the Modbus lesson applied.** If all four per-packet fields are
present they are the source and destination of THIS record. Otherwise the connection pair is
used, oriented by `is_orig` (then required): true → source = orig, destination = resp; false →
the reverse. The connection pair is identical on a request and its response; only `is_orig` tells
them apart. A record is a request iff its destination port is 102 (both 102, or neither: upstream's
rule applied as written).

## 6. Event identity

`S7COMM` is `sensor:uid:pdu_reference:direction:ts_millis`, as the 2026-09-10 spec §5.1 planned:
direction because a request and its response share `pdu_reference`; the millisecond timestamp
because `pdu_reference` is a 16-bit counter that a 10 Hz poll wraps in under two hours. Residual
risk, recorded as for Modbus: two same-direction records sharing uid and PDU reference inside one
millisecond get one id, and `ReplacingMergeTree` may collapse their archived rows.

A map-stage rejection's id is `sensor:uid:pdu_reference`, or `sensor:uid` when the reference is
what is missing — a correlation key, not a join key back to `feature_vectors` (as for Modbus).

## 7. Keying, state and lifetime

- **Key:** `S7commConnectionKey(sensor, uid)`. Upstream keys by `uid`.
- **State:** `S7commConnectionState` (domain), mutable and updated in place like the fixed
  `ModbusEntityState`, holding only what §4.1 reads: the outstanding set as a 65,536-bit bitset
  plus a running count; ring buffers for the nine histories of §4.1 (response match, function,
  function change, ROSCTR change, request, outstanding, direction change and transition at 16
  each; request PDU references at 32);
  last request function, function run, previous direction, direction run, last ROSCTR; last `ts`
  (for §8 only). Upstream stores `(ts, function)` per outstanding reference, but the frozen
  features read only membership and count.
- **Bounded by construction:** the bitset is at most 8 KB and every history is a fixed ring, so a
  key is ~10 KB at most and each event costs O(1). No cap and no saturation flag are needed.
- **TTL (upstream has none):** Flink state TTL on the S7 `ValueState` only — 1 hour of processing
  time (`S7COMM_STATE_TTL_MINUTES`, default 60, read by `main()` and passed to the operator), refreshed on every write (`OnCreateAndWrite`), expired state never returned,
  cleaned incrementally and on full snapshot. Parity holds while the job runs: Zeek assigns a new
  `uid` after its TCP inactivity timeout (5 minutes by default), so a `uid` idle for an hour
  normally never receives another record. It does NOT hold in two cases (corrected 2026-09-24
  after the final review): (a) downtime or a stall longer than the TTL -- the TTL counts
  processing time and each key's last-write time is restored with the checkpoint, so every
  connection is expired on its next record and restarts from empty state, silently; (b) a
  connection Zeek keeps open with keepalives but no S7 PDUs for longer than the TTL. Only during
  catch-up after a replay, which compresses event time, does the TTL fire late rather than early.
  `S7COMM_STATE_TTL_MINUTES` must therefore exceed the longest expected outage. Conn, dns and
  modbus state keep their recorded TTL gap.
- **Kryo:** the state resolves to `GenericTypeInfo`, like the Modbus state; its layout is free to
  change only until the first savepoint.
- **In-place safety:** as for Modbus — heap backend copy-on-write, `value()` on every event, and
  `update()` always called.

## 8. Arrival order is a deployment requirement

Upstream processes events in stored order (`38_build_v4_time_normalized_features.py` does no
sorting); every history and run depends on order. The sensor's producer must partition the S7
topic by `uid`, so a request and its response reach one partition in order. A record whose `ts`
is earlier than its key's last is processed where it arrives — no frozen feature reads time, so
there is nothing to reset — and carries a new quality bit, `S7COMM_OUT_OF_ORDER = 16`.

## 9. Rulings this design makes

Each is a deliberate deviation from, or addition to, upstream.

- **R1 — `uid` is required.** Upstream falls back to an endpoint flow key when `uid` is absent.
  Zeek always writes `uid`, and event identity needs it. A record without one goes to the DLQ.
  Cost if wrong: such records are lost rather than scored.
- **R2 — `pdu_reference` is required and must be 0–65535.** Upstream tolerates a missing one
  (no matching) and uses the unmasked integer for matching. The S7 header always carries a 16-bit
  reference; identity needs it, and the range bounds the outstanding set. Outside it: DLQ.
- **R3 — code ranges.** `rosctr_code` and `function_code`, when present, must be integers in
  0 ≤ n < 2^24 (exact in float32); otherwise DLQ. Upstream would format any integer.
- **R4 — TTL** (§7). Upstream never evicts.
- **R5 — out-of-order bit** (§8). Upstream has no notion of it.
- **R6 — `-2` for unseen names.** With no function code, upstream uses `upper(function_name)` as
  the category. This design maps the 11 known names back to their codes (identical categories) and
  every other name to `-2` (decoded `"__UNSEEN__"`). That equals upstream only when such a name
  never appeared in training; ICSNPP writes the code whenever it writes a name, so the case is not
  expected. Recorded as a limit.
- **R7 — `log2` and summation, settled by proof.** Java has no `log2`, and Python ≥ 3.12's `sum()` is
  compensated. Checked exhaustively on 2026-09-24 over every input the two entropy features can
  reach — all 65,534 ordered count sequences for n = 2..16 — computing Java's way (plain summation,
  `Math.log(x) / Math.log(2)`) and Python's (`sum`, glibc `math.log2`): the double results differ in
  14,436 cases, the float32 results in none, and plain summation on the Python side also differs in
  none. The vector is float32, so neither difference can reach it, whichever Python version the model
  team trained on. The Java uses the plain form.
- **R8 — float32.** Upstream's raw features are float64; the platform's vectors are float32 for
  every protocol. The oracle compares against upstream's value rounded to float32.
- **R9 — narrowing style.** The chain narrows `NetworkEvent` to `S7commEvent` once, at its
  boundary (`s7comm-event-narrow`), as Modbus does, so its key selector and process function are
  typed on `S7commEvent`.

## 10. Wiring

- **Online job:** a fourth `OnlineFeatureJob.build(env, servers, conn, dns, modbus, s7comm, sensor)`
  and a private `s7commChain(...)`: `S7commParseMapValidateFunction` (uid `s7comm-parse`) →
  `NarrowToS7commEvent` (`s7comm-event-narrow`) → `keyBy(S7commConnectionKeySelector)` →
  `S7commFeatureProcessFunction` (`s7comm-features`; state `s7comm-connection-state` with the TTL).
  Topics from `S7COMM_RAW_TOPIC`, `S7COMM_FEATURE_VECTOR_TOPIC`, `S7COMM_DLQ_TOPIC`, defaulting to
  `netsec.s7comm.raw.v1`, `netsec.s7comm.feature-vector.v1`, `netsec.s7comm.dlq.v1`. `main()` calls
  the new overload; the three-protocol one stays public and tested.
- **Archive job:** `ArchiveJob.connDnsModbusAndS7commChains(8 topics)` on the existing N-ary
  `build(List<LogTypeChain<?>>)`; `main()` calls it; the six-chain method stays public and tested.
- **Domain additions:** `LogType.S7COMM` (wire name `s7comm`), `S7commEvent` in `NetworkEvent`'s
  permits in the same change as its parser, mapper and schema, `S7commFeatureSchemaV1` registered
  in `FeatureSchemaRegistry`, `QualityFlags.S7COMM_OUT_OF_ORDER = 16`. Every exhaustive switch
  over `NetworkEvent` in conn's and dns's operators gains a throw arm for an event its chain never
  receives, as for Modbus.
- **ClickHouse:** no DDL change — `log_type` is a string column and `values` is `Array(Float32)`.
- **Recorded seam, kept:** a fourth protocol is a fourth overload and a fourth chains method.
  Replacing them with generic N-protocol wiring would touch every existing protocol's wiring and
  operator uids; it is out of scope here.

## 11. How parity is proven

1. **Upstream-generated oracle.** A generator script copies the four upstream feature-path files
   (`events.py`, `s7_parser.py`, both builders) into a throwaway `s7zeek` package in the session
   scratchpad and runs `CustomerICSNPPTimeNormalizedFeatureBuilder.process_event` over seeded
   synthetic streams. It writes JSON lines: the raw ICSNPP record, and upstream's 16 outputs
   (float64 values and the exact category strings). Committed: the fixture and the generator, with
   the SHA-256 of every upstream file it ran; never the upstream code. Streams cover: interleaved
   uids; both endpoint shapes and all three `id` spellings; matched, unmatched and re-used PDU
   references; unanswered request floods; all 11 named function codes, unknown codes, name-only
   (known and unknown) and neither; ROSCTR present, absent and changing; neither or both ports 102;
   and lengths that wrap every ring many times.
2. **`S7commUpstreamOracleTest`** (adapter-flink, beside `ModbusGoldenVectorTest`): each raw record
   through the real parser, mapper and use case; all 16 values bit-identical to upstream's value
   rounded to float32, and indices 14–15 decoding to upstream's exact strings.
3. **Unit tests**, one behaviour each: every formula and ring wrap; the bitset; run resets; the
   wrapper's first-event 0; the mapper's two endpoint shapes; **a request and its response carrying
   identical, unswapped `id_orig_*`/`id_resp_*` and told apart only by `is_orig` must match**;
   hex-string codes; every DLQ reason; encode/decode for all 256 function codes, every ROSCTR code
   and the name fallbacks; `QualityFlagsTest` for bit 4.
4. **Flink:** keying per uid; checkpoint/restore mid-stream equals an uninterrupted run; TTL drops
   a key idle past 1 hour and keeps it just before; Kryo copy independence and round trip; a
   one-uid flood at constant cost per event.
5. **End to end (real containers, one suite at a time):** online — an S7 request and its response
   with identical connection-level endpoints come out matched (outstanding 0, match rate 1), and a
   malformed S7 record reaches only `netsec.s7comm.dlq.v1`; archive — an S7 vector and an S7
   rejection reach ClickHouse under `log_type = 's7comm'` through the eight chains `main()` wires;
   both topology tests check all four protocols' uids are distinct.
6. **Verification discipline** as in CLAUDE.md: one module at a time with reports cleared, E2E one
   at a time, `ClickHouseOutageTest` never run, exact `Tests run:` counts recorded.

## 12. Records

CLAUDE.md gains: the S7 data flow and topics; `S7COMM`'s identity and its argument; the S7
endpoint rule beside Modbus's; the TTL in the bounded-state invariant; and the limits — R6's
`-2` approximation, R7, the partition-by-uid requirement, R1–R3's DLQ rules, and the event-id
residual collision.
