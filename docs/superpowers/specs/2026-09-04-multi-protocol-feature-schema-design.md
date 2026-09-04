# Multi-Protocol Dynamic-Length Feature Schema — Design

**Date:** 2026-09-04
**Supersedes nothing.** Extends `FINAL_ARCHITECTURE.md` and follows `PILOT_ARCHITECTURE.md`'s
per-protocol pattern. Deliberately overrides one `Roadmap.md` scope line — see §10.
**Predecessors:** `docs/conn-foundation-pipeline.md` (Steps 2-7),
`docs/superpowers/specs/2026-08-27-clickhouse-archive-job-design.md` (Step 8, in flight).

---

## 1. Context

The platform today is single-protocol by construction. Zeek `conn` records become a
`FeatureVector` of **exactly 20 float32 values in a frozen order**, and that number is an
invariant stated in `CLAUDE.md`, asserted by a content hash, and repeated across the
architecture documents.

The goal is a dynamic-length, multi-protocol configuration: `conn`, `ssh`, `dns`, `http`,
`modbus`, and eventually `s7comm`, each with its own feature schema of its own length.

The good news is that most of the system is already length-agnostic. `FeatureVector.values` is a
plain `float[]`; the Kafka wire `values` is a JSON array; the ClickHouse column is
`Array(Float32)`; and a `FeatureVector` already carries `schemaId` and `schemaHash`, so a record
self-identifies wherever it lands. The literal `20` appears in exactly three places:

| Where | What it is |
|---|---|
| `ConnFeatureSchemaV1.SCHEMA` | 20 `FeatureDefinition` entries — correct and frozen, stays |
| `BuildFeaturesUseCaseImpl` | `new float[20]` — the only real hard-coding |
| `contracts/features/conn-feature-schema-v1.json` | `"featureCount": 20` — correct for conn, stays |

What is *not* generic is the domain model. `NetworkEvent` is conn-shaped: it holds a
`ConnectionTuple` (ports, service, connection state) and `ConnectionMeasurements` (bytes,
packets, duration). A DNS record has query/qtype/rcode; HTTP has method/host/uri/status. So the
redesign is chiefly about what replaces `NetworkEvent`, not about array lengths.

---

## 2. Scope

**In scope — the generalization.** Everything needed so that adding a protocol is a mechanical,
self-contained unit of work with no further architectural decisions.

**Explicitly out of scope — the per-protocol feature schemas.** The feature list for `ssh`,
`dns`, `http`, `modbus` and `s7comm` comes from a preprocessing plan that is not ready. Guessing
them here would be worse than useless: `contracts/` is immutable, so a placeholder
`ssh-feature-schema-v1.json` becomes permanent and has to be versioned around rather than fixed.
This spec defines the slot each schema drops into and the recipe for adding one (§9).

**Consequence — how genericity is proven with only `conn` implemented.** A claim that the
machinery is log-type-agnostic, backed by a single instance, is usually false. Since no real
second protocol is available, the proof is:

1. Tests that drive the feature-building machinery at feature counts **other than 20**, using
   synthetic `FeatureSchema` instances built in test sources.
2. A synthetic second event type implementing the sealed hierarchy, defined in test sources
   only, exercising the registry and the generic port with a non-conn shape.

Neither freezes a contract. Both fail loudly if anything reintroduces a conn assumption.

---

## 3. Decisions

| # | Decision | Rationale |
|---|---|---|
| D1 | Per-protocol **fixed** schemas; length varies across protocols, never within one | Keeps the content-hash audit property (a hash pins exactly what a model was trained on). Matches `PILOT_ARCHITECTURE` §2. |
| D2 | `conn` stays, and the redesign is **additive** | `conn` is the base layer every other Zeek log joins to via `uid`, and it supplies byte/duration context the application-layer logs lack. Nothing already built is discarded. |
| D3 | One Kafka topic per log type; topic→log-type is a **wiring-time binding**, never a runtime string parse | Topic names are deployment config. The same log type already has two names in this repo (`conn` and `netsec.conn.raw.v1`), so any parsing rule is broken before a second protocol exists. |
| D4 | The new concept is named **`LogType`**, not `Protocol` | `Protocol` already means TCP/UDP/ICMP/OTHER and is frozen into `conn-feature-v1` features 11-12. `network_events` already has a `protocol` column meaning tcp/udp. `LogType` matches Zeek's own vocabulary and avoids this codebase's overloaded "source". |
| D5 | `NetworkEvent` becomes a **sealed interface** with a shared `EventEnvelope` and one record per log type | Not the "generic event" `PILOT_ARCHITECTURE` rejects — see §5. It is the idiom `CLAUDE.md` already prescribes and that `MappingResult` already uses. |
| D6 | `SourceWindowState` is **renamed, not generalized**, in this pass | Generalizing a ring buffer against one real consumer and five imagined ones produces an abstraction fitting none. The extraction trigger is named in §6. |
| D7 | `LogType` starts with `CONN` only and gains a constant per protocol | Java should not advertise support for a protocol with no mapper behind it. The wire contract documents the intended vocabulary as prose. |
| D8 | The **already-in-flight** archive-job tasks 5 and 6 are amended, not redone | Task 5 freezes the wire contract and Task 6 creates the table. Adding `logType`/`connectionUid` before those run costs ~30 lines; adding them after costs a `-v2` contract and an `ALTER TABLE`. |

---

## 4. Ingest binding and identity

Six input topics, six independent parse→map→validate→features chains inside the online job,
matching `PILOT_ARCHITECTURE`'s "independent adapter-kafka pipelines".

Configuration binds topic to log type:

```
CONN_INPUT_TOPIC=conn      -> LogType.CONN
SSH_INPUT_TOPIC=ssh        -> LogType.SSH        (when ssh lands)
DNS_INPUT_TOPIC=dns        -> LogType.DNS        (when dns lands)
```

The bootstrap constructs each chain with its `LogType` passed in as a constructor argument,
exactly as `SensorId` reaches `ParseMapValidateFunction` today. A chain never asks at runtime
what it is. Misconfiguration fails at **startup**, not per-record in production.

Downstream — archive job, training, inference — nothing reads topics. A `FeatureVector` carries
`schemaId` and `schemaHash`, so it self-identifies. The rule, in one line:

> **Config binds topic → LogType. The record binds itself → schema.**

`logType` is nonetheless carried as a first-class field on the wire and as a column, even though
it is derivable from `schemaId`. Same reasoning as `sensor`: a `LowCardinality(String)` column
you can filter and partition on beats string-parsing a schema id in every training query.

---

## 5. Domain restructuring

Every Zeek log carries `uid`, `ts`, and the four `id.orig_h / id.orig_p / id.resp_h / id.resp_p`
fields. Everything after that differs. So the shared core is larger than identity alone, and
today's `ConnectionTuple` fuses two unrelated things: the universal 4-tuple, and `protocol` /
`service` / `conn_state`, which only `conn` has.

### 5.1 Split the tuple

```java
// Shared by every log type: identity, time, origin, correlation key, and the four
// endpoint fields every Zeek log carries.
public record EventEnvelope(EventId eventId, Instant eventTime, SensorId sensor,
                            LogType logType, String connectionUid, Endpoints endpoints) { }

public record Endpoints(String sourceIp, int sourcePort,
                        String destinationIp, int destinationPort) { }

// conn.log-only classification, lifted out of the old ConnectionTuple.
public record ConnClassification(Protocol protocol, ServiceCode service,
                                 ConnectionState connectionState) { }
```

`Endpoints` keeps the port and blank-IP validation `ConnectionTuple` performs today.
`connectionUid` is Zeek's `uid`; it is `""` when a log type has none (Modbus/S7comm may not
carry one), by the same convention `invalid_events.event_id` already uses.

### 5.2 Sealed hierarchy

```java
public sealed interface NetworkEvent permits ConnEvent {   // grows per protocol
    EventEnvelope envelope();
}

public record ConnEvent(EventEnvelope envelope, ConnClassification classification,
                        ConnectionMeasurements measurements, ConnectionLocality locality)
        implements NetworkEvent { }
```

`permits` lists only implemented log types (D7). Each new protocol adds its record and extends
the `permits` clause — the compiler then flags every non-exhaustive `switch`.

### 5.3 Why this is not the rejected "generic event"

`PILOT_ARCHITECTURE` §2 rejects a shared generic event, and it is right to. What it rejects is
*one record with a bag of nullable fields* that every protocol squeezes into: type safety is
lost and every consumer null-checks. This design is the opposite — each log type gets a record
containing exactly its own fields, and `sealed` makes the compiler enforce exhaustive handling.
A shared **envelope** is not a shared **payload**.

### 5.4 Event identity is per-log-type — `uid` is NOT a record id

For `conn`, the source contract's `id` field *is* Zeek's `uid`, and one `conn` record exists per
connection, so `EventId.derive(sensor, uid)` yields `sensor:uid` and it is unique. That is why
`connectionUid` needs no new parsing for conn: it is exactly `dto.id()`.

**This does not generalise.** Zeek's `uid` identifies a *connection*, not a record. A single
connection produces many `dns` records (one per query), many `http` records (one per
transaction), and several `ssh` records. Deriving `eventId` from `sensor:uid` for those log types
would emit **colliding event IDs**, and `event_id` is part of `feature_vectors`' ORDER BY key —
so `ReplacingMergeTree` would silently collapse distinct events into one row. Loss of data with
no error anywhere.

Therefore **event-id derivation is a per-log-type responsibility**, declared by that log type's
mapper:

| Log type | `connectionUid` | `eventId` | Uniqueness argument |
|---|---|---|---|
| `conn` | `uid` | `sensor:uid` — **unchanged** | One conn record per connection |
| `dns` | `uid` | `sensor:uid:<trans_id>` | Zeek `dns.log` carries `trans_id` per query |
| `http` | `uid` | `sensor:uid:<trans_depth>` | Zeek `http.log` carries `trans_depth` per transaction |
| `ssh`, `modbus`, `s7comm` | `uid` (or `""`) | decided with that log type's preprocessing plan | Must be justified against real fixture data before the schema is frozen |

`conn`'s format is deliberately left alone: conn events are already archived as `sensor:uid`, and
changing the format would orphan every existing row.

Each protocol-addition unit (§9) must state its uniqueness argument and back it with a fixture
test that feeds two records sharing a `uid` and asserts two distinct `eventId`s. A log type whose
uniqueness argument cannot be made from its own fields is not ready to be added.

### 5.5 `SourceKey` gains the log type

`(sensor, sourceIp)` becomes `(sensor, logType, sourceIp)`. Windowed features are per-protocol —
"connections in 5m" and "SSH auth failures in 5m" are different counters with different
semantics — so keyed state must never be shared across log types.

This **cannot** change any conn feature value: every conn event carries `LogType.CONN`, so the
partitioning is identical and indices 17-19 are byte-for-byte unchanged.

### 5.6 The safety property

The 20 conn values, their order, and `ConnFeatureSchemaV1.CONTENT_HASH`
(`f42fb1bebb2efe3acc5de634c6a7bb3d6f97fc021207f75d77652533b1c01e1b`) **do not change**.
`EventFeatureExtractor` reads `destinationPort` from `envelope().endpoints()` instead of
`connection()`, and `protocol`/`service`/`connectionState` from `classification()` — different
accessors, identical arithmetic, identical output.

`ConnFeatureSchemaV1Test` is the tripwire. **If it goes red at any point during this work, the
refactor is wrong.** No `conn-feature-v2`, no migration, no retraining of anything already
trained.

---

## 6. Generalizing the feature machinery

### 6.1 Schema registry

Inference and training both need "what is schema X, and how many values does it have?" without
touching Kafka or Flink. A lookup over the static schema constants — no framework, no config:

```java
public final class FeatureSchemaRegistry {
    public static FeatureSchema byLogType(LogType logType);  // wiring, fail-fast startup checks
    public static FeatureSchema byId(String schemaId);       // consumers holding a record
}
```

`byId` is what makes an archived row self-describing: training reads `schemaHash` off the row and
resolves the definitions, which is what `FINAL_ARCHITECTURE`'s fail-fast model/schema validation
expects. Both methods throw on an unknown key rather than returning null — an unresolvable schema
is a deployment error, not a runtime condition.

### 6.2 Generic port

```java
public interface BuildFeaturesUseCase<E extends NetworkEvent, S> {
    FeatureBuildResult<S> build(E event, S currentState);
}

public record FeatureBuildResult<S>(FeatureVector vector, S newState) { }
```

`BuildFeaturesUseCaseImpl` becomes
`ConnBuildFeaturesUseCase implements BuildFeaturesUseCase<ConnEvent, ConnWindowState>`.
One port, one implementation per log type, no casting anywhere.

### 6.3 The dynamic length itself

```java
float[] values = new float[schema.featureCount()];   // was: new float[20]
```

That is the entire "dynamic length" change. Every other length-bearing thing is already dynamic.

### 6.4 Wiring stays explicit

Six chains in the bootstrap, one small private method each. A generic loop is not expressible in
Java's type system here — `ConnEvent` and `DnsEvent` chains have different types — and the
bootstrap is the one module allowed to know about every adapter. Repetitive but type-safe and
greppable, the right trade for a composition root.

### 6.5 The deliberate non-decision on window state

`SourceWindowState` tracks three conn-specific counters: connections, bytes, failures. SSH would
want auth failures; DNS would want NXDOMAIN counts. Three generalizations are possible — a
generic `float[]` of counters (loses type safety: the generic-bag problem one level down), a
state record per log type (duplicates the 5-bucket ring arithmetic N times), or extracting the
ring mechanics behind a typed payload.

**None of them yet.** Rename `SourceWindowState` → `ConnWindowState` so the name stops lying, and
extract the mechanics when there is a second real consumer.

> **Named trigger: the second log type that requires a rolling window is when `RollingCounters`
> gets extracted.** Until then the ring arithmetic stays where it is.

A first schema for a new protocol built purely from event-level fields is entirely plausible, in
which case no window state is needed at all for it.

---

## 7. Contracts

### 7.1 `conn-feature-v1` is untouched

Same 20 values, same order, same hash, same `featureCount: 20`. Nothing to migrate.

### 7.2 Per log type, when its preprocessing plan is ready

- `contracts/source/zeek-<type>-source-v1.json`
- `contracts/features/<type>-feature-schema-v1.json`

Each content-hashed and immutable, exactly the conn pattern. Adding a log type never edits an
existing contract.

### 7.3 One stream contract covers all log types

`contracts/stream/feature-vector-v1.json` describes the **envelope**: `eventId`, `eventTime`,
`sensor`, `logType`, `connectionUid`, `schemaId`, `schemaHash`, `values`, `qualityFlags`,
`producedAt`. Only the *content* of `schemaId`/`schemaHash`/`values` varies by log type; the
shape does not. There is no `ssh-feature-vector-v1` — one frozen wire contract, six populations.

This property exists only because the contract is frozen with `logType` and `connectionUid`
present from the start (D8).

### 7.4 Topics

Input: one per log type. Output: `netsec.<type>.feature-vector.v1` and `netsec.<type>.dlq.v1`,
which is already the established convention and matches `PILOT_ARCHITECTURE`.

Because rows self-identify, the archive job can subscribe by **topic pattern**
(`netsec\..*\.feature-vector\.v1`) and keep a single chain for all log types — the ClickHouse row
shape is identical across them. That is a one-line `setTopics` → `setTopicPattern` change made
when the second log type arrives, not now.

---

## 8. Landing on the in-flight archive-job plan

The ClickHouse archive job (`docs/superpowers/plans/2026-08-31-clickhouse-archive-job.md`) is
4/14 tasks complete. It finishes first; this redesign follows. Two of its remaining tasks are
amended before dispatch:

| Task | Amendment |
|---|---|
| 5 — freeze stream contracts | `feature-vector-v1.json` freezes with **10** fields, not 8: adds `logType` and `connectionUid`. `FeatureVector`, both serializers and both deserializers carry them. |
| 6 — ClickHouse DDL | `feature_vectors` gains `log_type LowCardinality(String)` and `connection_uid String`. |
| 7 — row mappers | Two extra field mappings, falling out of 5 and 6. No design change. |
| 8-14 | Unaffected. |

At this point `conn` is the only log type, so both new fields are populated with `LogType.CONN`
and the Zeek `uid` — real values, not placeholders. Every archived row is multi-protocol-ready
and cross-protocol-joinable from the first row written.

---

## 9. The protocol-addition recipe

Once this spec is implemented, adding a log type is mechanical and needs no new architectural
decision. Per protocol, given a ready preprocessing plan:

1. `contracts/source/zeek-<type>-source-v1.json` — the source contract, plus sanitized fixtures
   under `tests/fixtures/zeek_<type>/`. **State the event-id uniqueness argument (§5.4) and back
   it with a fixture test: two records sharing a `uid` must produce two distinct `eventId`s.**
2. `contracts/features/<type>-feature-schema-v1.json` — the feature schema, content-hashed.
3. `<Type>FeatureSchemaV1` in `domain`, registered in `FeatureSchemaRegistry`.
4. `LogType.<TYPE>` enum constant; `<Type>Event` record; extend `NetworkEvent`'s `permits`.
5. `Zeek<Type>Event` DTO + `Json Zeek<Type>Parser` + `<Type>EventMapper` in `adapter-kafka`.
6. `<Type>FeatureExtractor` in `application`; `<Type>BuildFeaturesUseCase` implementing the
   generic port. If it needs a rolling window, this is where §6.5's trigger fires.
7. One wiring method in the bootstrap, plus its `<TYPE>_INPUT_TOPIC` variable in `.env.example`.

Each protocol is an independently shippable unit. Ship `ssh` and stop, if that is what the
evidence supports.

---

## 10. Deviations from the source documents

| Document | Says | This design | Why |
|---|---|---|---|
| `Roadmap.md` §8 | Multi-protocol is POSTPONE — "a scope failure unless it replaces an existing task" | Proceeding anyway | An explicit, recorded product decision by the repository owner. The Roadmap's 20-day MVP framing no longer governs. |
| `PILOT_ARCHITECTURE.md` | Pilot protocols are `conn`, `dns`, `http`, `ssl` | `conn`, `ssh`, `dns`, `http`, `modbus`, `s7comm` | Owner's target set. Drops `ssl`, adds `ssh` and two ICS protocols. The per-protocol *pattern* is unchanged. |
| `PILOT_ARCHITECTURE.md` §2 | "No shared generic event" | Sealed interface with a shared envelope | Not a contradiction — see §5.3. The rejected thing is a nullable field-bag; this is a closed hierarchy with per-type payloads. |
| `CLAUDE.md` | "Feature vector: exactly 20 float32 values" | Per-log-type length | The invariant becomes "exactly `schema.featureCount()` values, frozen per schema". `CLAUDE.md` is updated as part of the work. |

---

## 11. Out of scope

- **Per-protocol feature schemas** — owner's preprocessing plan (§2).
- **S7comm ingestion** — Zeek has no native parser; the field set depends on which third-party
  plugin is deployed. Needs a spike ("what does our Zeek actually emit for s7comm?") before any
  schema can be written.
- **Cross-protocol correlation** — the join on `connectionUid` is Phase 2 per
  `PILOT_ARCHITECTURE` §9. This design only guarantees the key is carried.
- **The two-stage detection cascade** (autoencoder + classifier) — `PILOT_ARCHITECTURE` §6, a
  separate effort.
- **`RollingCounters` extraction** — deferred behind the named trigger in §6.5.
- **Retraining or reissuing any conn model** — the conn schema and hash are unchanged.

---

## 12. Definition of done

1. `./mvnw clean verify` passes from a clean tree.
2. `ConnFeatureSchemaV1Test` passes **untouched** — the 20 values, their order and the content
   hash are unchanged.
3. `grep -rn "new float\[20\]"` over `modules/` returns nothing; no production code asserts a
   feature count.
4. Feature-building is exercised by tests at feature counts other than 20, via synthetic schemas.
5. A synthetic second event type in test sources drives the sealed hierarchy, the registry and
   the generic port — proving no conn assumption remains.
6. `FeatureSchemaRegistry.byLogType` and `byId` resolve `conn` and throw on an unknown key.
7. `SourceKey` carries `logType`, and a test proves conn's indices 17-19 are unchanged by it.
8. `CLAUDE.md`'s "exactly 20" invariant is restated as per-schema.
9. `EventId` derivation is a per-log-type responsibility (§5.4), with conn's `sensor:uid` format
   unchanged and a test asserting it.
10. The protocol-addition recipe in §9 is committed as `docs/adding-a-log-type.md`.
