# Modbus Stage 1 — Design

**Status:** approved in outline 2026-09-21; supersedes the Modbus portions of
`2026-09-10-per-protocol-feature-schemas-design.md` §4.4.

**Goal.** Carry the user's **frozen** Modbus Stage 1 feature contract onto ml-platform with
exact semantic parity: Zeek/ICSNPP `modbus_detailed` records in, a 42-value raw feature
vector out, archived and later scored by their LSTM autoencoder.

## 1. The upstream authority

This design does not invent a feature set. It reproduces one that is already frozen and
verified, supplied in `two-models-info/` (untracked in this repo, never staged):

| Artifact | What it fixes |
|---|---|
| `modbus_/FEATURE_CONTRACT_V1.json` | `modbus_feature_contract_v1`, 42 features, their exact order, groups, definitions, dependencies and missing-rules |
| `modbus_/07b_materialize_feature_engine_v1.py` | The authoritative calculation, in `process_capture(...)` |
| `modbus_/modbus_preprocessing_contract_v1/` | The frozen 42→42 preprocessing, its fitted parameters and its TRAIN verification |
| `Modbus_Two_Stage_Feature_Specification.docx` | Per-feature Zeek/ICSNPP source fields, and §6 "Important Engineering Notes" |

**Where this design and the upstream artifacts disagree, the upstream artifacts win.**
A vector that does not match `modbus_feature_contract_v1` cannot be scored by the frozen
model, which is the entire point of the work.

The spec's §6 is binding and is quoted here so it travels with the plan:

- **Zeek parity** — every production feature must be reconstructable from Zeek/ICSNPP
  fields or from bounded causal state derived from them, "then carried through Kafka and
  Flink with the same semantics".
- **Identity fields** — `uid`, IP addresses, ports and packet indices may key state and
  joins but "must not silently become model inputs".
- **Causality** — current-event features may use only the current event and previously
  observed state. Future events are forbidden. The contract states this as
  `future_information_allowed: false`.
- **Stage separation** — Stage 1 answers Normal vs Anomaly; Stage 2 only ever receives
  Stage 1's anomalous candidates.

## 2. What is in scope, and what cannot be

**Stage 1 only.** Stage 2 (attack family) is explicitly NOT implementable today, on the
authors' own statement: selection is fold-local MI Top-48 inside each CV fold, and "there
is no single globally frozen 48-feature production list yet". Building against a subset
that changes per fold would produce a model input nothing can reproduce. Stage 2 waits
for a frozen list.

**No model binary exists yet.** `two-models-info/` carries contracts, extraction code and
fitted preprocessing parameters — no `.onnx`, no autoencoder weights. As in the conn
scoring unit, the serving path is proven against a hand-built fixture with golden vectors
and the real bundle is swapped in later without a Java change.

## 3. Preprocessing lives inside the ONNX graph

Decision, 2026-09-21: the frozen preprocessor and the model are re-exported by the model
team as ONE ONNX graph. ml-platform computes and emits **raw** 42-value vectors and stays
"raw in, score out".

This preserves the rule from `2026-09-10-per-protocol-feature-schemas-design.md` §7 and
its reason: no fitted state in Java means no Java reimplementation of `log1p`,
`StandardScaler` or a mask-conditional transform can drift away from the Python that was
fitted, and move every score silently. `PREPROCESSING_CONTRACT_V1.md`'s rule that "when
the associated mask is 0, the transformed feature MUST remain exactly 0.0" is exactly the
kind of conditional a hand-port gets subtly wrong.

Consequence: the archived `feature_vectors` rows are raw and stay re-fittable.

## 4. The 42 features

Order is frozen by `FEATURE_CONTRACT_V1.json`'s `feature_order` and is reproduced in
`contracts/features/modbus-feature-schema-v1.json`. Four groups:

| Group | Indices (1-based, as the contract numbers them) | Mode | Platform home |
|---|---|---|---|
| `A_MODBUS_SEMANTIC` | 1–13 | event | pure function of one record |
| `B_MODBUS_VALUE` | 14–23 | event | pure function of one record |
| `C_TRANSACTION_TEMPORAL` | 24–35 | stateful | keyed state, before-event |
| `D_WINDOW_BEHAVIOR` | 36–42 | windowed | keyed state, trailing windows |

Groups A and B are a pure per-record mapping and belong in the feature extractor. Groups
C and D require the causal keyed state described in §7 and belong in the Flink operator,
exactly as conn's rolling counters and dns's window state already do.

`function_registry` fixes the function-code semantics and must not be re-derived:
one-hot `{1,2,3,4,5,6}`, everything else `fc_other`; READ `{1,2,3,4,20,24}`, WRITE
`{5,6,15,16,21,22}`, READ_WRITE `{23}`. Note the engine counts FC 23 in BOTH the read and
write tallies, which is what makes `read_ratio_10s` and `write_ratio_10s` the
"READ/READ_WRITE" and "WRITE/READ_WRITE" fractions the contract names.

## 5. Source contract

New `contracts/source/zeek-modbus-source-v1.json`, over `icsnpp-modbus`'s
`modbus_detailed.log`. Canonical fields and their accepted Zeek spellings, per the
specification's §1.1:

| Canonical | Zeek / ICSNPP | Role |
|---|---|---|
| `ts` | `ts` | event time; inter-arrival, RTT, windows |
| `uid` | `uid` | state/join key only — never a feature |
| endpoints | `id.orig_h`/`id.orig_p`, `id.resp_h`/`id.resp_p` (or `source_h`/`source_p`, `destination_h`/`destination_p`) | orientation and state key |
| `direction` | `request_response`, or `network_direction`/`is_orig` semantics | normalized to request/response BEFORE any feature is computed |
| `transaction_id` | `tid` | pending-request state, matching, overwrite detection, RTT |
| `unit_id` | `uint` or `unit` | sequence and transaction-state key |
| `function_code` | `func` | one-hot indicators and behavioural groups |
| `address` | `address` | value + presence mask, deltas, unique counts |
| `quantity` | `quantity` | value + presence mask, deltas |
| `request_values` | `request_values` | numeric summaries only |
| `response_values` | `response_values` | numeric summaries only |
| `matched` | `matched` | `response_matched` |

Retained for audit but NOT Stage 1 inputs, per the contract's `excluded_raw_fields`:
`exception_code`, `request_data`, `response_data`, the subfunction codes, `mei_type`,
`modbus_detailed_link_id`.

**Direction normalization is a validation, not a coercion.** The engine lowercases and
trims, then hard-fails on anything that is not `request`/`response`. A record whose
direction cannot be resolved is a DLQ rejection with its own reason code, never a guess —
`is_response` is feature index 1 and every orientation-derived key depends on it.

**`function_code` is required.** The engine raises on a missing or unparseable `func`.
That becomes a `MISSING_REQUIRED_FIELD` rejection here.

## 6. Event identity

`sensor:uid:tid:direction:ts_millis`, as `2026-09-10-per-protocol-feature-schemas-design.md`
§5.1 already argued: both a direction and a timestamp are required because ICSNPP emits
request and response as separate records sharing a `tid`, and `tid` is a 16-bit
client-chosen counter that a 10 Hz SCADA poll loop exhausts in under two hours on
connections that live far longer.

Residual risk, unchanged and to be closed by fixture per §10 of that spec: two
same-direction records sharing a `tid` inside one millisecond.

## 7. Keying, state and segments

**State key — a new type, not `SourceKey`.** The frozen contract's `sequence_context.key`
is `(client_ip, server_ip, unit_id)`, with orientation normalized first:

```
client_ip = (direction == request) ? src_ip : dst_ip
server_ip = (direction == request) ? dst_ip : src_ip
```

The platform's `SourceKey(sensor, logType, sourceIp)` cannot express this. A new
`ModbusEntityKey(SensorId sensor, String clientIp, String serverIp, String unitId)`
carries the sensor because one platform serves many, and unit id as a **String** because
the engine's `int_key` falls back to `"NA"` for an absent unit rather than dropping the
record.

Its `hashCode()` must be built from `Objects.hash` over Strings only. This is not
defensive style: `SourceKey` shipped with an enum component whose `Enum.hashCode()` is the
JVM identity hash, which made Flink's key-group assignment differ between JVMs and broke
keyed-state restore. That defect was found, verified and fixed on this codebase — do not
reintroduce its shape.

**Per-key state**, mirroring `EntityState` in the authoritative engine: `lastTs`,
`prevFunctionCode`, `lastAddress`, `lastQuantity`, a pending-TID map (`tid -> requestTs`),
three trailing-timestamp windows (1 s, 10 s, 60 s), function and address counters for the
10 s window, and read/write tallies.

**Segments.** A gap greater than `GAP_SECONDS = 15.0` resets every one of those fields and
starts a new segment. In the offline engine a new capture also resets; the streaming
equivalent is the first record ever seen for a key. Within a segment `inter_arrival_s`
must lie in `[0, 15]`, and the engine treats a violation as an error rather than clamping.

**Before-event, then mutate.** The contract's `transaction_rule` is explicit: compute the
before-event state features, and mutate pending-TID state only after the current event's
features are extracted. `outstanding_requests_before_event` is named for this. Getting the
order backwards changes the emitted value for every request and is invisible without a
test that pins it, so the plan pins it.

## 8. Arrival order is a deployment requirement

The offline engine asserts `capture_event_index` is strictly increasing and refuses to run
otherwise. A Kafka topic offers no such guarantee, and order affects far more here than it
does for conn: the pending-TID machine, `rtt_s`, both deltas, `function_changed`,
`inter_arrival_s` and the segment boundary are all order-dependent.

**The sensor's Kafka producer must partition by `(client_ip, server_ip)`** so that per-key
arrival is ordered. This design states that requirement, records it in CLAUDE.md's Known
limits, and does NOT pretend the operator enforces it. This is the same class of exposure
already recorded for `RollingCounters`, which resets a bucket whenever the stored minute
merely differs from the incoming one — but it reaches more features.

## 9. Rulings this design makes

**Ruling 1 — `modbus-feature-v1` is exactly 42 values and carries NO common tier.**
This is a deliberate exception to the invariant that "a protocol's feature schema is the
common tier (12 values, frozen) followed by that protocol's own tier". Prepending the tier
would make the vector 54 wide and it would no longer match the graph the frozen
autoencoder expects, so the schema would be useless for its only purpose. The invariant
gains a recorded exception: *a schema that mirrors a frozen external contract leads with
that contract's own order.* Cost if wrong: modbus vectors carry no conn-derived context,
so a later model wanting it needs `modbus-feature-v2`.

**Ruling 2 — `ModelRef` gains `scoreKind`.** Stage 1 is an autoencoder, so its score is a
reconstruction error: unbounded and ≥ 0. `Prediction` and `ModelRef` currently require a
score and threshold that are finite AND within 0..1, and those bounds are load-bearing —
they were hardened across four fix rounds and one of them catches a null threshold that
would otherwise score every record as an attack. So the range rule becomes conditional on
a declared kind: `PROBABILITY` keeps 0..1, `RECONSTRUCTION_ERROR` requires finite and ≥ 0.
`decision = score >= threshold` is correct for both, and the threshold stays readable in
the model's own units. Cost if wrong: an extra component on a record several tasks already
consume.

**Ruling 3 — this work branches from `feat/conn-scoring-path` at `f15f9f8`, not `main`.**
Its foundations (`ModelRef`, `Prediction`, the `ModelScorer` port, the filesystem registry,
`OnnxModelScorer`, the prediction serializers) exist only there, and `scoreKind` modifies
one of them. `main` additionally still cannot run the online job at all. Cost if wrong: the
modbus branch inherits any unmerged defect from the conn branch, which the whole-branch
review is the net for.

## 10. Unit decomposition

**Unit M1 — ingest and the 42-value vector. No scoring.**
Source contract, `LogType.MODBUS`, `ModbusEvent`, the parser and mapper, event identity,
`modbus-feature-v1`, the event-level extractor (groups A and B), the Flink operator holding
the causal state (groups C and D), topics, DLQ, the archive chain, and end-to-end proof.
Deliverable: 42-value raw vectors in Kafka and ClickHouse that match the frozen contract.
**M1 has no dependency on the model at all**, so it can proceed while the ONNX re-export
happens.

**Unit M2 — sequence assembly and scoring.**
`scoreKind`, a sequence-assembly operator holding the last L=20 vectors per entity key, a
sequence-aware scorer taking `[1, 20, 42]`, the prediction sink, and end-to-end proof
against a fixture autoencoder. Depends on M1 and on the re-exported bundle.

M1 is the larger and the one that carries the parity risk. It is also the one that can be
verified without a model, which is why it goes first.

## 11. How parity is proven

Semantic parity with a Python engine cannot be asserted; it has to be measured.

1. **Contract pinning.** `modbus-feature-schema-v1.json`'s names and order are compared
   field-by-field against `FEATURE_CONTRACT_V1.json`'s `feature_order` by a test, not by
   eye. The count is pinned at 42 and the schema's own content hash frozen.
2. **Worked vectors.** A small set of hand-computed records with expected 42-value outputs,
   derived from the contract's definitions, covering: a request, a matched response, an
   unmatched response (`response_without_request`), a same-TID overwrite, a segment
   boundary at exactly 15 s, and an absent address/quantity.
3. **Ordering pin.** A test that a request's `outstanding_requests_before_event` excludes
   itself — the before-event/mutate-after rule from §7.
4. **The engine's own invariants**, which it checks at runtime and which become tests here:
   in-segment `inter_arrival_s` within `[0, 15]`, exactly one `fc_*` indicator set per
   event, `rtt_s` zero on a request, and `response_matched` zero on a request.

`TRAIN_TRANSFORM_VERIFICATION_V1.csv` and the contract's `acceptance_gates` record what the
authors verified on 2,789,268 TRAIN events. They are evidence about the Python engine, not
about this port, and are not cited as proof of the Java implementation.

## 12. Out of scope

- **Stage 2** — no frozen feature list exists (§2).
- **S7comm** — its own unit, after M1 proves the OT pattern. Its Stage 2 router is
  per-event and needs no sequence assembly, so it is the cheaper of its two stages.
- **Re-exporting the ONNX bundle** — the model team's, per §3.
- **Enforcing per-key arrival order** — a sensor-side deployment requirement (§8).
- **`modbus.log`, and the specialised ICSNPP logs** (`mask_write_register`,
  `read_write_multiple_registers`, `read_device_identification`) — `modbus_detailed.log` is
  the only source Stage 1 reads.
