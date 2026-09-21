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

**Wire format: JSON from Kafka** (confirmed by the user, 2026-09-21), so `request_values`
/ `response_values` arrive as JSON arrays. The Java parser requires strict JSON and sends
anything else to the DLQ. It deliberately does NOT reproduce the offline engine's
`ast.literal_eval` fallback, which exists there to tolerate Python-repr strings read back
from research files — a tolerance that on a production wire would silently accept a
malformed payload instead of rejecting it.

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

> **Revised 2026-09-21, on the user's instruction: change no existing rule, and if
> something would break the system, do not do it — raise it instead.** Rulings 1 and 2
> below were both rule changes. Both are now WITHDRAWN as decisions and recorded as OPEN
> QUESTIONS. Neither blocks the bulk of M1 (see §10).

**OPEN QUESTION 1 — does `modbus-feature-v1` carry the common tier?**

The invariant says "a protocol's feature schema is the common tier (12 values, frozen)
followed by that protocol's own tier", and every schema from `dns-feature-v1` onward leads
with it. Honouring it makes the modbus vector 54 wide; the frozen autoencoder expects 42.

This is NOT merely a width preference. The two tiers require **different keys**, verified
against the code: the common tier is computed from `RollingCounters`, `RecordTimingState`
and `ConnEnrichment`, which live in state keyed by `SourceKey(sensor, logType, sourceIp)`
(`SourceKeySelector.getKey`), while the frozen 42 require state keyed by
`(sensor, clientIp, serverIp, unitId)`. A Flink keyed operator has exactly one key, so the
two cannot be produced by the same operator.

The options, with their real costs:

- **A — 54 values, invariant honoured.** Needs two keyed operators and a join to reassemble
  one vector. That join is processing-order dependent, which is already a recorded known
  limit for the dns enrichment join. Most of the 12 would also be near-constant for modbus
  for the same reason they are for dns: Zeek writes a connection's `conn.log` line only
  after the connection ends, so enrichment is absent for nearly every OT record.
- **B — 42 values, invariant gains a second recorded exception.** (`conn-feature-v1` is
  already the first, being frozen without the tier.) Exact parity with the frozen graph,
  no join, no extra state. Cost: a rule change, and modbus vectors carry no conn-derived
  context, so a later model wanting it needs `modbus-feature-v2`.
- **C — defer.** Build every part of M1 that does not depend on the width, and freeze the
  schema last. This is what §10 now does.

A feature schema is frozen forever once registered, so this is the one decision in M1 that
cannot be cheaply reversed. It is parked for the user, not decided here.

**OPEN QUESTION 2 — how does an unbounded reconstruction error reach `Prediction`?**

Stage 1 is an autoencoder, so its score is unbounded and ≥ 0, while `Prediction` and
`ModelRef` require score and threshold finite AND within 0..1. Those bounds are
load-bearing: they were hardened across four fix rounds, and one of them catches a null
threshold that would otherwise score every record as an attack.

Adding a `scoreKind` component to `ModelRef` would resolve it, but it modifies a frozen
domain type and makes an existing validation conditional — a rule change. **It is also not
needed by M1, which never touches `ModelRef`.** Deferred to M2, by which point the
re-exported graph exists and may settle the question on its own: if the export calibrates
its error to 0..1, nothing in the domain needs to change at all.

**Ruling 3 — this work branches from `feat/conn-scoring-path` at `f15f9f8`, not `main`.**
Its foundations (`ModelRef`, `Prediction`, the `ModelScorer` port, the filesystem registry,
`OnnxModelScorer`, the prediction serializers) exist only there, and `scoreKind` modifies
one of them. `main` additionally still cannot run the online job at all. Cost if wrong: the
modbus branch inherits any unmerged defect from the conn branch, which the whole-branch
review is the net for.

## 10. Unit decomposition

**Unit M1 — the source contract, the parser, and the 42-value causal engine.**
Deliberately scoped to touch NO existing invariant, so that OPEN QUESTION 1 blocks none
of it:

- `contracts/source/zeek-modbus-source-v1.json`
- a `ZeekModbusRecord` DTO and `JsonZeekModbusParser` in `adapter-kafka`
- `ModbusEntityKey` in `domain` — a new type, colliding with nothing
- the causal engine: orientation normalization, the segment rule, the pending-TID machine,
  the trailing windows, and all 42 values, as a standalone component with no Flink and no
  schema registration

In particular M1 does **not** add `LogType.MODBUS` or a `ModbusEvent` record. The sealed
hierarchy's own rule is that `permits` lists only log types that have "a parser, a mapper
and a feature schema" behind them, and M1 has no schema by design — so adding the record
in M1 would break that invariant exactly as registering a schema would.

This is the right order on risk as well as on rules: reproducing the Python engine's causal
semantics exactly is the hard part of this whole design, and M1 makes it independently
verifiable against worked vectors before anything is wired or frozen. **M1 has no
dependency on the model**, so it proceeds while the ONNX re-export happens.

**Unit M1b — hierarchy, schema, operator and end-to-end.** Unblocked by an answer to
OPEN QUESTION 1. Adds `LogType.MODBUS`, `ModbusEvent` and its mapper, registers
`modbus-feature-v1`, hosts M1's engine in a `KeyedProcessFunction`, wires topics, the DLQ
and the archive chain, and proves it end to end.

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
