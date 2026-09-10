# Per-Protocol Feature Schemas from Zeek Logs

**Status:** design agreed, not yet planned
**Supersedes nothing.** Extends `2026-09-04-multi-protocol-feature-schema-design.md`, which
generalised the domain structure for multiple protocols but deliberately left per-protocol
feature schemas out of scope. This document fills exactly that gap.

## 1. Purpose

The multi-protocol redesign froze the *envelope* — `LogType`, `connectionUid`, variable-length
`values`, per-log-type event identity — and stopped there, because no per-protocol preprocessing
plan existed yet. That plan now exists as a separate project,
[`IT-OT_AnomalyDetection`](https://github.com/amirhosseinah2000/IT-OT_AnomalyDetection), and this
document adapts its feature definitions to ml-platform's streaming, Zeek-sourced pipeline.

Six log types are in scope: the existing `CONN`, plus `SSH`, `DNS`, `HTTP` (IT) and `MODBUS`,
`S7COMM` (OT).

## 2. Constraints

These are fixed inputs to the design, not conclusions drawn from it.

1. **Zeek only. No PCAP ingestion, ever.** The sensors emit Zeek logs; ml-platform consumes Zeek
   logs. `IT-OT_AnomalyDetection` is a *feature-definition reference* — we borrow what to compute
   and never how it ingests. Any feature reachable only by parsing raw packet bytes is out of
   reach by construction, not by choice.
2. **OT protocols arrive via the CISA ICSNPP Zeek plugins** —
   [`icsnpp-modbus`](https://github.com/cisagov/icsnpp-modbus) and
   [`icsnpp-s7comm`](https://github.com/cisagov/icsnpp-s7comm). Base Zeek is insufficient: its
   `modbus.log` carries only `tid`, `unit`, `func`, `pdu_type`, `exception`, and it has no S7comm
   analyzer at all.
3. **Layer 7 is the priority for OT.** Flow-level features look unremarkable when an attacker
   issues a legitimately-shaped Modbus write to the wrong register. The detection signal lives in
   application semantics — function codes, register addresses, values, PLC block transfers — so
   schemas favour those over framing statistics.
4. **`conn.log` is emitted periodically for open connections** (5-minute interval, configured on
   the sensors), not only at connection close.
5. **All preprocessing lives inside the exported ONNX model.** See §7.

## 3. The two-tier schema model

Each log type owns one frozen contract at
`contracts/features/<log-type>-feature-schema-v1.json`, built from two tiers:

```
<log-type>-feature-schema-v1
  ├── common tier      identical across all log types, defined once
  └── protocol tier    fields specific to this log type
```

**Vector length is fixed within a log type and varies across log types.** DNS, Modbus and S7comm
each freeze their own length and their own content hash. `conn-feature-v1` and its 20 values are
untouched.

Almost nothing in the existing implementation changes to support this:

| Component | Change needed |
|---|---|
| `FeatureVector` | none — already carries `logType`, `schemaId`, `schemaHash`, `float[] values` |
| `feature_vectors.values` | none — already `Array(Float32)` |
| `feature-vector-v1.json` | none — already carries `logType` |
| `ReplacingMergeTree` key | none — length-agnostic |
| `LogType` | add `SSH`, `DNS`, `HTTP`, `MODBUS`, `S7COMM` |

### 3.1 Common tier

Derived from ml-platform's existing bounded keyed state per `(sensor, sourceIp)` — enriched from
`conn.log` where available (§6.2).

**A correction against the code, recorded because this design depends on it.** `FINAL_ARCHITECTURE.md`
describes "six 1-minute buckets/key; 30-minute TTL". The implemented `SourceWindowState` has
**five** buckets over a **5-minute** window (`connectionCount5m`, `byteSum5m`, `failedCount5m`) and
**no TTL is configured anywhere**. The code is the authority here; the architecture document is
aspirational and predates it. Nothing in this design assumes six buckets or a TTL.

- record rate over the 5-minute window
- inter-arrival mean and standard deviation (needs new state; `SourceWindowState` holds no
  timestamps and must not be widened, since its serialized shape is shared with `conn`)
- direction (`is_orig`)
- `orig_bytes`, `resp_bytes`, `orig_pkts`, `resp_pkts` (conn.log, cumulative)
- byte and packet deltas between consecutive conn.log snapshots
- connection age at record time

`payload_entropy`, which the reference project computes per packet, **has no Zeek equivalent** and
is not in the common tier. Zeek logs do not carry payload bytes. The entropy features that survive
are those computed over strings Zeek *does* log — `dns_qname_entropy` over `query`,
`http_url_entropy` over `uri`.

## 4. Per-protocol schemas

Each table below maps the reference project's features onto Zeek fields. "Excluded" means the
value is unreachable under §2.1 and is not in the schema.

### 4.1 DNS — `dns.log`

| Feature | Zeek source |
|---|---|
| `dns_rcode` | `rcode` |
| `dns_qtype` | `qtype` |
| `dns_authoritative` | `AA` |
| `dns_recursion_available` | `RA` |
| `dns_truncated` | `TC` |
| `dns_answer_count` | length of `answers` |
| `dns_ttl` | first element of `TTLs` |
| `dns_qname_length` | derived from `query` |
| `dns_qname_entropy` | derived from `query` |
| `dns_label_count` | derived from `query` |
| `dns_digit_ratio` | derived from `query` |
| `dns_hyphen_ratio` | derived from `query` |
| `dns_ngram_score` | derived from `query` |
| ~~`dns_qname`~~ | excluded — raw text, see §8 |
| ~~`dns_response_size`~~ | excluded — not in `dns.log`; the common tier covers volume |

DNS is the best fit of the six: thirteen of fifteen features survive.

### 4.2 HTTP — `http.log`

| Feature | Zeek source |
|---|---|
| `http_method` | `method`, mapped to a frozen enum |
| `http_status_code` | `status_code` |
| `http_content_length` | `request_body_len` |
| `http_response_size` | `response_body_len` |
| `http_url_length` | derived from `uri` |
| `http_url_entropy` | derived from `uri` |
| `http_query_parameter_count` | derived from `uri` |
| `http_suspicious_token_count` | derived from `uri` |
| ~~`http_header_count`~~ | excluded — Zeek does not log header counts |
| ~~`http_cookie_count`~~ | excluded — cookies not logged by default |
| ~~`http_host`, `http_user_agent`, `http_url`~~ | excluded — raw text, see §8 |

### 4.3 SSH — `ssh.log`

**This is the weakest fit, and it is not the OT protocols.** Four of the reference project's seven
SSH features count the *offered* algorithm lists from the KEXINIT exchange. Zeek deliberately logs
the **negotiated** result instead, so those counts are unreachable.

| Feature | Zeek source |
|---|---|
| `ssh_kex_algorithm` | `kex_alg`, mapped to a frozen algorithm enum |
| ~~`ssh_cipher_count`~~ | excluded — Zeek logs `cipher_alg` (negotiated), not the offered list |
| ~~`ssh_mac_count`~~ | excluded — same |
| ~~`ssh_compression_count`~~ | excluded — same |
| ~~`hassh`~~ | excluded unless the `zeek-hassh` package is deployed |
| ~~`ssh_client_banner`, `ssh_server_banner`~~ | excluded — raw text, see §8 |

Rather than ship a lossy three-feature copy, SSH's schema should use what Zeek is genuinely better
at than a packet parse — connection-level authentication state that a per-packet view cannot see:

- `auth_success`
- `auth_attempts`
- `version`

`ssh_kex_algorithm` is the one categorical in the design whose vocabulary is **not** fixed by a
protocol specification (§7.1). It requires a frozen enum of algorithm names with an explicit
"unknown" bucket, and that enum is a schema-versioned artifact: adding an algorithm is a `-v2`.

### 4.4 Modbus — `modbus_detailed.log`

| Feature | Zeek source |
|---|---|
| `modbus_transaction_id` | `tid` |
| `modbus_unit_id` | `unit` |
| `modbus_function_code` | `func` name mapped back to its Modbus numeric code |
| `modbus_function_category` | derived from the function code — read / write / diagnostic / other |
| `modbus_nonstandard_function` | derived — code outside 1–24 and 43 |
| `modbus_is_exception` | `exception_code` present |
| `modbus_exception_code` | `exception_code` |
| `modbus_starting_address` | `address` |
| `modbus_quantity` | `quantity` |
| `modbus_value_*` | reduced from `request_values` / `response_values` (see below) |
| ~~`modbus_protocol_id_valid`~~ | excluded — MBAP header bytes not exposed by ICSNPP |
| ~~`modbus_length_valid`~~ | excluded — same |
| ~~`modbus_byte_count`~~ | excluded — same |

`request_values` and `response_values` are `vector of count` — variable length inside a
fixed-length feature vector. They are reduced to a fixed set of summary features (count, first,
min, max, mean). The exact reduction is frozen in the contract; the point is that a variable-length
Zeek field never becomes a variable-length region of the vector.

### 4.5 S7comm — merged across three logs

The S7comm feature set **cannot be sourced from one log**, which is what forces the merge design in
§6.1:

| Feature | Zeek source |
|---|---|
| `s7_rosctr` | `s7comm.log` → `rosctr_code` |
| `s7_function_code` | `s7comm.log` → `function_code` |
| `s7_subfunction_code` | `s7comm.log` → `subfunction_code` |
| `s7_error_class` | `s7comm.log` → `error_class` |
| `s7_error_code` | `s7comm.log` → `error_code` |
| `s7_control_command` | derived — function code in {0x28, 0x29} |
| `s7_is_plus` | `s7comm_plus.log` → presence |
| `s7_block_transfer` | `s7comm_upload_download.log` → presence, or function code in {0x1A, 0x1B} |
| `s7_transfer_size` | `s7comm_upload_download.log` → `blocklength` |
| `s7_block_type` | `s7comm_upload_download.log` → `block_type` |
| `s7_function_status` | `s7comm_upload_download.log` → `function_status` |
| ~~`s7_parameter_length`, `s7_data_length`~~ | excluded — raw header byte offsets |
| ~~`s7_item_count`, `s7_db_number`, `s7_area_code`~~ | excluded — raw parameter-block parses |

`s7comm_read_szl.log` (SZL reads — the reconnaissance counterpart to block transfer) and
`cotp.log` are **not** in v1. They are the most likely first extension and the schema-versioning
rules must accommodate them arriving as additional merge inputs under a `-v2` schema.

## 5. Event identity

§5.4 of the multi-protocol design makes event-ID uniqueness a per-log-type obligation, each with
its own stated argument. This extends that table.

| Log type | `eventId` | Uniqueness argument |
|---|---|---|
| `CONN` | `sensor:uid` | one conn record per connection — **unchanged** |
| `DNS` | `sensor:uid:<trans_id>` | `dns.log` carries `trans_id` per query |
| `HTTP` | `sensor:uid:<trans_depth>` | `http.log` carries `trans_depth` per transaction |
| `SSH` | `sensor:uid` | one `ssh.log` record per connection |
| `MODBUS` | `sensor:uid:<tid>:<dir>:<ts_millis>` | see below |
| `S7COMM` | `sensor:uid:<pdu_reference>:<dir>:<ts_millis>` | see below |

### 5.1 Why the OT identities carry direction and timestamp

Two independent collisions, both of which would silently collapse distinct events into one
`ReplacingMergeTree` row — the exact failure §5.4 exists to prevent.

**Direction.** Both plugins emit separate request and response records. `modbus_detailed` has a
`matched` flag, the other Modbus logs carry `request_response`, and every ICSNPP log carries
`is_orig`. Without the direction in the key, a request and its response share an event ID.

**Counter wrap.** `tid` and `pdu_reference` are 16-bit. A SCADA poll loop at 10 requests per second
exhausts 65,536 values in under two hours, and these connections routinely live far longer. After
a wrap, `sensor:uid:<tid>:REQ` repeats within the same connection. The record timestamp
disambiguates it and requires no additional state.

**Residual risk, to be closed by fixture:** two same-direction records sharing a counter value
inside a single millisecond. This must be checked against real captures **before the OT schemas
are frozen**. §5.4 already requires exactly this evidence for every new log type.

## 6. Assembly

### 6.1 Protocol-internal merge

A single S7 operation writes to several ICSNPP logs at effectively the same instant, sharing `uid`
and `pdu_reference`. These are joined into one feature vector:

- **Key:** `(uid, pdu_reference | tid, direction)`
- **Window:** 5 seconds by default, configurable; the records originate from one PDU, so this
  is a generous bound rather than a tuning parameter
- **On timeout:** emit anyway with the absent block defaulted, and set a `qualityFlags` bit

This gives `qualityFlags` its first real use; it is currently hard-coded `0`. A vector emitted
without its `upload_download` component is not the same as one where no transfer occurred, and the
model must be able to distinguish them.

**Input topics remain one-per-Zeek-log** (`netsec.s7comm.raw.v1`,
`netsec.s7comm-upload-download.raw.v1`, …), because that is how Zeek writes them, and the topic
binding continues to determine the parser at wiring time. **Output is one feature vector per
protocol.**

### 6.2 conn.log is enrichment, never a blocking join

The common tier's byte and packet counters exist only in `conn.log`. Given §2.4 they are reachable,
but the join must never block:

- bounded keyed state `uid → latest conn snapshot`, TTL'd
- every protocol record performs a **left join** against it
- snapshot present → enrich; absent → default those features and set a `qualityFlags` bit

**Blocking on `conn.log` would be a correctness failure, not merely a latency cost.** A
connection's first snapshot does not exist until it has been alive 5 minutes. Waiting would stall
every record from a new connection, hold unbounded state per open connection, and — for the
longest-lived connections, which carry the most OT traffic — is precisely where the design must not
degrade.

Snapshots are **cumulative**, not per-interval. Rate features use the delta between consecutive
snapshots, which the keyed state already holds.

### 6.3 Topic and chain fan-out

Per-protocol topics on both the success and failure paths multiply the archive job's chain count.
Today `ArchiveJob` wires two source-to-sink chains, one per internal topic. With six log types it
becomes six feature-vector chains plus six DLQ chains.

Twelve hand-written chains is not acceptable. `ArchiveJob.build` must take the set of log types as
a parameter and construct the chains in a loop, so adding a log type is a one-line registration
rather than a copy-pasted chain. The two-chain structure is otherwise unchanged: each chain is
still source → map → ClickHouse sink, and the topic → `LogType` binding is still fixed at wiring
time.

This also affects operator IDs. Every chain needs a distinct, stable `.uid()` derived from its log
type, or a topology change silently invalidates checkpoint state — the defect already recorded
against both jobs.

## 7. Preprocessing lives in the ONNX graph

The reference project's `prepare` stage is a *fitted* transformer: robust scaling needs medians and
IQRs, one-hot encoding needs a vocabulary, imputation needs fill values. It persists this ("the
exact fitted transformer is stored with the matrix").

**That entire chain is exported inside the ONNX model** via `skl2onnx`'s `Pipeline` conversion, so
scaler and encoder become graph nodes. ml-platform computes **raw** features and feeds them in;
the model returns a score.

Consequences:

- ml-platform's inference contract is unchanged: raw vector in, score out.
- No fitted state in Java, so no risk of a Java reimplementation of `RobustScaler` drifting away
  from sklearn's and moving scores silently.
- Fitted state and model are versioned as one artifact, which is what makes the frozen-schema
  guarantee meaningful.
- **The feature schemas can be frozen before any training data exists**, because the raw vector's
  width does not depend on a learned vocabulary.

### 7.1 Categoricals use protocol-defined codes

The previous point holds only if every categorical maps to a number fixed by a specification rather
than derived from data. All but one do:

| Feature | Source of the number |
|---|---|
| `modbus_function_code` | Modbus specification (1–24, 43) |
| `s7_rosctr`, `s7_function_code` | S7 protocol constants |
| `dns_qtype`, `dns_rcode` | IANA registries |
| `http_status_code` | HTTP specification |
| `http_method` | small closed set, frozen enum |

`ssh_kex_algorithm` (§4.3) is the sole exception and is handled as a schema-versioned enum.

## 8. Raw text is excluded

`dns_qname`, `http_host`, `http_user_agent`, `http_url`, the SSH banners and `hassh` are **not** in
any schema. Two independent reasons, either sufficient:

1. **They cannot be frozen without training data.** The reference project collapses high-cardinality
   text to the top-N most frequent categories, which is data-derived. Their derived numerics
   (length, entropy, label count, digit and hyphen ratios, query-parameter count, suspicious-token
   count) carry most of the detection signal and need no vocabulary.
2. **They are user-identifying.** `docs/clickhouse.md` states that `feature_vectors` carries no
   addresses and is therefore not subject to the access and retention controls `network_events`
   needs. Raw domains, URLs and hostnames would make that claim false and would attach a real
   retention question to the table. Excluding them preserves the guarantee at no analytic cost.

## 9. Failure paths

The success path is multi-protocol aware; the failure path is not. `feature-vector-v1` carries
`logType`; `dlq-v1` has six fields and none identify the protocol, and `invalid_events` has no
`log_type` column. With one protocol that was invisible. With six it means "is the S7comm parser
rejecting everything?" is unanswerable — every rejection lands in one undifferentiated pile.

The existing taxonomy needs no change. `ReasonCode` with its `Stage.PARSE` / `Stage.MAP` split
generalises as-is: malformed ICSNPP JSON is `MALFORMED_JSON` at PARSE, an absent required field is
`MISSING_REQUIRED_FIELD` at MAP.

**Resolution, requiring no contract change:** each protocol gets its own DLQ topic, and the archive
job learns the log type from the **topic binding at wiring time** — the same principle the design
already mandates for the input side, where topic → `LogType` is a compile-time binding and never a
runtime string parse. `dlq-v1` stays frozen.

`invalid_events` gains a `log_type LowCardinality(String)` column. Since
`infrastructure/clickhouse/ddl/` is immutable, this is a new
`002_add_invalid_events_log_type.sql`, not an edit. `apply-ddl.sh` already iterates the directory.

## 10. Fixture obligations

Per §5.4, no log type is ready to be added without evidence. Each requires:

1. A fixture test feeding **two records sharing a `uid`** and asserting **two distinct
   `eventId`s**.
2. For `MODBUS` and `S7COMM`, additionally the **counter-wrap case**: two records with the same
   counter value and the same direction, differing only in timestamp, asserting distinct event IDs.
3. Real captures confirming the sub-millisecond residual risk in §5.1 does not occur in practice.

A log type whose uniqueness argument cannot be evidenced from its own fields is not ready to be
added.

## 11. Out of scope

Named explicitly so their absence is a decision rather than an oversight:

- **Sequence models.** The reference project configures an LSTM autoencoder with
  `sequence_length: 10`. ml-platform emits one vector per event, not a window. Assembling
  sequences is an inference-side concern for Roadmap Day 9. This design only requires that vectors
  carry a stable key and ordering so sequences *can* be assembled later — which `eventId`,
  `connectionUid` and `producedAt` already provide.
- **Two-stage detection.** The reference project's stage two consumes stage one's score.
  ml-platform's inference contract assumes a single ONNX model. Day 9.
- **Training data.** None is available yet. §7 establishes that this does not block freezing the
  schemas.
- **`s7comm_read_szl.log`, `cotp.log`, `s7comm_plus.log` as independent log types**, and the
  specialised Modbus logs (`mask_write_register`, `read_write_multiple_registers`,
  `read_device_identification`). Deferred, but the merge and versioning rules are built to accept
  them.
- **`user_agent` length and entropy** as derived numerics, by analogy with the URL treatment.
  Reasonable, but absent from the reference project, so it is a candidate for a later schema
  version rather than something to invent here.
- **Zeek-side work to close the §4 exclusions** — HTTP header and cookie counts, SSH KEXINIT
  algorithm lists, Modbus MBAP validation. Each is a tracked plugin or script requirement, not a
  silent loss.

## 12. Implementation sequencing

**This spec is deliberately larger than one implementation plan.** Five new log types, each with a
parser, a mapper, a frozen contract and fixtures, plus merge infrastructure, conn.log enrichment
and a DDL migration, is too much to plan as a single unit. It decomposes into the
"protocol-addition unit" the multi-protocol design already anticipates.

Suggested order, chosen so each unit de-risks the next:

| # | Unit | Why here |
|---|---|---|
| 1 | Common tier + conn.log enrichment + `ArchiveJob` chain loop (§6.2, §6.3) | Protocol-agnostic foundation; every later unit depends on it, and it changes no existing schema |
| 2 | `invalid_events` migration + per-protocol DLQ topics (§9) | Small, independent, and makes every later unit's failures diagnosable |
| 3 | `DNS` | Best coverage of the six, single log, no merge — proves the whole per-protocol pattern at the lowest risk |
| 4 | `HTTP` | Same shape as DNS; confirms the pattern generalises before anything harder |
| 5 | `SSH` | First schema needing a curated enum (§4.3) and the first whose feature set departs from the reference project |
| 6 | `MODBUS` | First OT and first ICSNPP source; single log, but introduces function-name-to-code mapping and vector-value reduction |
| 7 | `S7COMM` | Last: the only unit requiring the multi-log merge (§6.1), and the one whose identity carries the most risk (§5.1) |

Units 3–7 are each a plan of their own. Unit 1 is a plan of its own. Unit 2 could fold into unit 1
if it proves small in practice.

Each of units 3–7 is complete only when it satisfies §10's fixture obligations. A protocol whose
uniqueness argument cannot be evidenced against real captures does not ship, and blocks nothing
else in the sequence.
