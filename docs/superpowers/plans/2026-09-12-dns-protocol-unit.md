# DNS Protocol Unit (3c) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `dns.log` as the platform's second protocol end to end — parser, mapper, event record, frozen 24-value feature schema, conn.log enrichment join, and wiring — proving the per-protocol pattern the previous three units built.

**Architecture:** DNS gets its own Kafka topic, its own chain in both jobs, and its own `BuildFeaturesUseCase<DnsEvent, DnsWindowState>` implementation. Nothing switches on log type at runtime; the topic → `LogType` binding is fixed at wiring time. The generic rolling window is extracted from `ConnWindowState` so conn and DNS share counter machinery without sharing a name that lies.

**Tech Stack:** Java 21, Flink 2.2.1 (Sink V2 / FLIP-27 only), `flink-connector-kafka:5.0.0-2.2`, Jackson 2.17, JUnit 5, Testcontainers 1.21.4.

**Spec:** `docs/superpowers/specs/2026-09-10-per-protocol-feature-schemas-design.md` — §3.1 (common tier), §4.1 (DNS feature table), §5 (event identity), §6.2 (enrichment as a left join), §6.3 (chain fan-out), §7.1 (categoricals from registries), §8 (raw text excluded), §10 (fixture obligations). Secondary: `docs/superpowers/specs/2026-09-04-multi-protocol-feature-schema-design.md` §5.4 (per-log-type identity), §6.5 (the `RollingCounters` extraction trigger).

## Global Constraints

- Java package root `io.netsecml.platform`. Java 21: `record` for immutable carriers, `sealed interface` + pattern-matching `switch` for closed hierarchies. Records with array components take defensive copies in the compact constructor **and** the accessor.
- **`conn-feature-v1` and its 20 values are frozen and must not change** — not the contract file, not its content hash, not indices 0–19, not `ConnFeatureSchemaV1.java`, not `EventFeatureExtractor.java`.
- `contracts/` and `infrastructure/clickhouse/ddl/` are **immutable**. A change creates a new version file; it never edits an existing one.
- `domain` imports no Kafka, Flink, ClickHouse, ONNX, Jackson or Docker. `application` imports only `domain` + `ports`. **Adapters never import each other.**
- Every Flink operator carries an explicit, stable `.uid()`. A changed uid silently discards checkpoint state.
- Narrowing a `NetworkEvent` uses a pattern `switch` with explicit `case` arms. **Never `default`** — a `default` arm permanently disables the exhaustiveness alarm for protocols 3 through 6.
- Comment every block with the reasoning, not the syntax. A comment that misdescribes its code is worse than no comment.
- Build offline: `./mvnw install -DskipTests -q -o`, then `./mvnw test -pl <module> -o`. **Never** combine `-Dtest=X` with `-am`. Confirm the `Tests run:` count — a filtered run can report BUILD SUCCESS having run zero tests.
- `ClickHouseOutageTest` is OOM-killed on this machine and must never be described as passing.

**Baseline test counts before Task 1** (a drop means something was deleted): domain 76, application 16, adapter-kafka 23, adapter-flink 8.

---

## Two rulings this plan makes up front

**1. `dns_ngram_score` is NOT in v1, deviating from spec §4.1.** Every other categorical in §7.1 maps to a number fixed by a specification — IANA registries for `dns_qtype` and `dns_rcode`, the HTTP spec for status codes. An n-gram score is different in kind: it needs a reference corpus, which is data-derived and does not exist. Freezing a schema around a value we cannot compute deterministically is precisely the "cannot be frozen without training data" problem §8 uses to exclude raw text. The other twelve derived numerics carry the qname signal without a vocabulary. If a frequency table is later committed to `contracts/`, `dns-feature-v2` can add it. **DNS is therefore 24 values: 12 common + 12 protocol.**

**2. Two checkpoint-state breaks are taken now, together, because they are currently free.** Task 1 replaces `ConnWindowState` and Task 2 widens `SourceKey` — both change Flink keyed state and both invalidate any existing checkpoint. This is acceptable *only* because it is being done now: `main` cannot run the online job, this whole branch chain is unmerged, and production is air-gapped and not yet deployed, so no checkpoint plausibly exists. Taking both in one unit costs one break instead of two. **The same changes after deployment would not be free**, and a later unit must not assume this precedent transfers.

---

## File Structure

**New — domain:**
- `feature/RollingCounters.java` — the generic five-bucket window, extracted from `ConnWindowState`
- `feature/DnsWindowState.java` — DNS's per-key state: counters + timing
- `feature/DnsFeatureSchemaV1.java` — the frozen 24-value schema
- `feature/QualityFlags.java` — bit constants; first real use of `FeatureVector.qualityFlags`
- `event/DnsEvent.java`, `event/DnsQuery.java`, `event/DnsResponse.java`, `event/DnsRcode.java`, `event/DnsQType.java`

**New — application:**
- `feature/QnameFeatures.java` — pure string derivations (length, entropy, labels, ratios)
- `feature/DnsFeatureExtractor.java` — the 12 protocol-tier values
- `usecase/DnsBuildFeaturesUseCase.java` — `BuildFeaturesUseCase<DnsEvent, DnsWindowState>`

**New — adapters:**
- `adapter-kafka`: `dto/ZeekDnsEvent.java`, `parser/JsonZeekDnsParser.java`, `mapper/DnsEventMapper.java`
- `adapter-flink`: `process/DnsFeatureProcessFunction.java`, `process/ConnParseMapValidateFunction.java`, `process/DnsParseMapValidateFunction.java`, `process/ConnSnapshotJoinFunction.java`, `process/DnsKeySelector.java`

**New — contracts:**
- `contracts/features/dns-feature-schema-v1.json`

**Modified:**
- `domain`: `LogType` (+`DNS`), `NetworkEvent` (`permits`), `SourceKey` (+`logType`), `FeatureSchemaRegistry` (+entry), `ConnWindowState` (deleted)
- `application`: `CommonFeatureExtractor` (takes `RollingCounters`), `ConnBuildFeaturesUseCase`
- `adapter-flink`: `SourceKeySelector`, `ConnFeatureProcessFunction`, `ParseMapValidateFunction` (becomes abstract)
- `bootstrap-online-job`: `OnlineFeatureJob`; `bootstrap-archive-job`: `ArchiveJob`
- `.env.example`, `CLAUDE.md`

---

### Task 1: Extract `RollingCounters` from `ConnWindowState`

Spec §6.5's named trigger. Unit 3b's whole-branch review found that `CommonFeatureExtractor` — declared protocol-agnostic — takes a `ConnWindowState`, whose own javadoc says the name is conn-specific on purpose. Two comments one dependency apart, contradicting each other. DNS is the second consumer that §6.5 said would settle it.

**Files:**
- Create: `modules/domain/src/main/java/io/netsecml/platform/domain/feature/RollingCounters.java`
- Delete: `modules/domain/src/main/java/io/netsecml/platform/domain/feature/ConnWindowState.java`
- Rename: `modules/domain/src/test/.../ConnWindowStateTest.java` → `RollingCountersTest.java`
- Modify: `modules/application/.../feature/CommonFeatureExtractor.java`, `modules/application/.../usecase/ConnBuildFeaturesUseCase.java`, `modules/adapter-flink/.../process/ConnFeatureProcessFunction.java`, `modules/ports/.../BuildFeaturesUseCase.java` (no signature change — verify only)

**Interfaces:**
- Produces: `RollingCounters` with `static RollingCounters empty()`, `RollingCounters record(long bucketEpochMinute, long bytes, boolean failed)`, `long recordCount5m()`, `long byteSum5m()`, `long failedCount5m()`.

- [ ] **Step 1: Create `RollingCounters` as a copy of `ConnWindowState` with generic names**

Copy `ConnWindowState.java` verbatim, then rename the class and rename **only** the accessor `connectionCount5m()` → `recordCount5m()`. The three `long[]` arrays, `record(...)`, the bucket arithmetic and the constructor stay byte-for-byte identical — this is an extraction, not a rewrite.

Replace the class javadoc with:

```java
// A bounded five-bucket, one-minute rolling window of three counters for a
// single key: how many records, how many bytes, how many failures.
//
// Extracted from ConnWindowState, which named conn in a type the protocol-
// agnostic common tier depends on. What "bytes" and "failed" MEAN is the
// caller's decision, and that is the whole point of the extraction: conn passes
// total_bytes and a failed conn_state; DNS passes 0 bytes (dns.log carries no
// byte counts) and rcode != NOERROR. The three frozen common-tier names --
// record_count_5m, byte_sum_5m, failed_count_5m -- are already generic, so this
// type now matches the contract it feeds.
//
// Deliberately holds no timestamps: RecordTimingState owns inter-arrival shape,
// and the bounded-state invariant forbids keeping a record history here.
```

- [ ] **Step 2: Update the three consumers, names only**

`CommonFeatureExtractor.extract`'s first parameter becomes `RollingCounters window`, and `values[0] = window.recordCount5m()`. `ConnBuildFeaturesUseCase` becomes `BuildFeaturesUseCase<ConnEvent, RollingCounters>`. `ConnFeatureProcessFunction`'s `ValueState<RollingCounters>` and `TypeInformation.of(RollingCounters.class)` follow.

**Delete the stale trigger note** from `CommonFeatureExtractor`'s javadoc (the paragraph beginning "rolling window, and DNS is this tier's first consumer") and replace it with one sentence recording that §6.5's trigger fired here and was resolved by this extraction.

- [ ] **Step 3: The state descriptor name changes this time — and say why**

In `ConnFeatureProcessFunction.open()`, the descriptor comment currently explains at length why the name string stays `"source-window-state"` across a type rename. That reasoning was correct for a rename that bought nothing. **This time change the string to `"rolling-counters"`**, and rewrite the comment to say:

```java
        // The state name changes to match the type this time, unlike the
        // ConnWindowState rename that kept "source-window-state". That rename
        // gained nothing by breaking the name, so it kept it. This extraction
        // breaks checkpoint compatibility anyway -- Kryo embeds the class name,
        // and RollingCounters is a different class -- and it is being taken while
        // the break is free: main cannot run this job, the branch chain is
        // unmerged, and production is air-gapped and not deployed, so no
        // checkpoint plausibly exists. Paying for a correct name now is cheaper
        // than carrying a wrong one past the first deployment.
        //
        // A restore across this change does NOT run cold -- it fails. The uid and
        // the old state name would both still match, so Flink locates the state,
        // finds a snapshot naming a class that no longer exists, and fails with
        // StateMigrationException. --allowNonRestoredState does not cover a
        // serializer incompatibility on matching state. Recovery is a fresh start,
        // which replays from OffsetsInitializer.earliest(). Reasoned from
        // documented restore semantics, not from an executed savepoint test.
```

- [ ] **Step 4: Run every affected module**

```bash
./mvnw install -DskipTests -q -o
./mvnw test -pl modules/domain -o
./mvnw test -pl modules/application -o
./mvnw test -pl modules/adapter-flink -o
```

Expected: domain 76, application 16, adapter-flink 8 — **identical**, since this is a rename plus one accessor rename. `grep -rn "ConnWindowState" --include=*.java modules/` must return nothing.

- [ ] **Step 5: Commit**

```bash
git add -A modules/
git commit -m "refactor(domain): extract RollingCounters from ConnWindowState"
```

---

### Task 2: `SourceKey` gains `logType`

Spec §5.5 specifies `(sensor, logType, sourceIp)`. Today it is `(sensor, sourceIp)`. With one protocol that was invisible; with DNS arriving it decides whether a conn record and a dns record from the same host share a rolling window.

**Files:**
- Modify: `modules/domain/src/main/java/io/netsecml/platform/domain/feature/SourceKey.java`
- Modify: `modules/adapter-flink/src/main/java/io/netsecml/platform/adapter/flink/process/SourceKeySelector.java`
- Test: `modules/domain/src/test/.../feature/SourceKeyTest.java`

**Interfaces:**
- Produces: `record SourceKey(SensorId sensor, LogType logType, String sourceIp)`.

- [ ] **Step 1: Write the failing test**

```java
    // The reason logType is in the key rather than merely alongside it: a host
    // that both browses and resolves names produces conn and dns records with the
    // same sensor and the same source IP. Without logType they would share one
    // rolling window, and DNS's record_count_5m would silently include the host's
    // connections. Same sensor, same IP, different log -- different key.
    @Test
    void keysDifferByLogTypeAloneSoProtocolsDoNotShareAWindow() {
        SensorId sensor = new SensorId("sensor-eu-1");
        SourceKey connKey = new SourceKey(sensor, LogType.CONN, "10.0.0.5");
        SourceKey dnsKey = new SourceKey(sensor, LogType.DNS, "10.0.0.5");

        assertNotEquals(connKey, dnsKey);
        assertNotEquals(connKey.hashCode(), dnsKey.hashCode(),
            "a hash collision here would not be wrong, but these must not be equal");
    }
```

**This test cannot compile until Task 3 adds `LogType.DNS`.** That is a genuine cross-task dependency, not a defect: write the test now, confirm it fails to compile for exactly that reason, and note it. If executing tasks strictly in order, do Task 3's `LogType` constant first — it is one line — and record that you did.

- [ ] **Step 2: Add the component**

```java
// The key every per-(sensor, source) rolling window is grouped by.
//
// logType is part of the KEY, not decoration: one host's conn records and its
// dns records must not accumulate into a shared window, or each protocol's
// record_count_5m would count the other's traffic.
public record SourceKey(SensorId sensor, LogType logType, String sourceIp) {
```

Keep whatever validation the record already performs and add `Objects.requireNonNull(logType, "logType must not be null")`.

- [ ] **Step 3: Update `SourceKeySelector`**

It already narrows with a pattern switch to reach `sourceIp`. Pass `event.logType()` as the new middle component. Do not add a `default` arm.

- [ ] **Step 4: Run and commit**

```bash
./mvnw install -DskipTests -q -o
./mvnw test -pl modules/domain -o          # 76 + 1 = 77
./mvnw test -pl modules/adapter-flink -o   # 8
git add -A modules/
git commit -m "feat(domain): put logType in SourceKey so protocols do not share a window"
```

---

### Task 3: `LogType.DNS`, `DnsEvent`, and the compile breaks that follow

This is the moment the sealed hierarchy's exhaustiveness alarm fires for the first time — exactly what Units 2 and 3b built it to do.

**Files:**
- Modify: `modules/domain/.../event/LogType.java`, `modules/domain/.../event/NetworkEvent.java`
- Create: `modules/domain/.../event/DnsEvent.java`, `DnsQuery.java`, `DnsResponse.java`, `DnsRcode.java`, `DnsQType.java`
- Test: `modules/domain/src/test/.../event/DnsEventTest.java`

**Interfaces:**
- Produces: `record DnsEvent(EventEnvelope envelope, DnsQuery query, DnsResponse response, String sourceIp, boolean isOrig) implements NetworkEvent`; `record DnsQuery(String name, DnsQType qtype, int transId)`; `record DnsResponse(DnsRcode rcode, boolean authoritative, boolean recursionAvailable, boolean truncated, int answerCount, long firstTtlSeconds)`.

- [ ] **Step 1: Add the enums**

`DnsRcode` and `DnsQType` map to **IANA registry numbers**, per §7.1 — the number comes from the registry, never from the data's ordering.

```java
// DNS response codes, numbered by the IANA DNS RCODEs registry. The numeric
// value IS the feature value (index 12), so these numbers are contract, not
// implementation detail -- reordering the constants would not change them, and
// that is the point of writing them explicitly.
public enum DnsRcode {
    NOERROR(0), FORMERR(1), SERVFAIL(2), NXDOMAIN(3), NOTIMP(4), REFUSED(5),
    OTHER(-1);

    private final int code;

    DnsRcode(int code) {
        this.code = code;
    }

    public int code() {
        return code;
    }

    // An rcode outside the set above is real -- the registry has more values than
    // any schema should enumerate -- so it maps to OTHER rather than throwing. A
    // rejected record would lose a legitimate observation.
    public static DnsRcode fromCode(int code) {
        for (DnsRcode r : values()) {
            if (r.code == code) {
                return r;
            }
        }
        return OTHER;
    }

    // NOERROR is the only non-failure. The rolling window's failedCount5m for DNS
    // is "responses that did not succeed", which is this predicate.
    public boolean isFailure() {
        return this != NOERROR;
    }
}
```

`DnsQType` follows the same shape with `A(1), NS(2), CNAME(5), SOA(6), PTR(12), MX(15), TXT(16), AAAA(28), SRV(33), ANY(255), OTHER(-1)`.

- [ ] **Step 2: Add the event record**

```java
// A dns.log record: the shared envelope plus the query and response blocks.
//
// sourceIp and isOrig are carried directly rather than through a ConnectionTuple
// because dns.log has no connection state, service or conn_state to put in one --
// the four endpoint fields are all it shares with conn. When EventEnvelope gains
// the Endpoints component its spec section 5.1 already specifies, sourceIp moves
// there and this record loses it; until then it lives here rather than forcing
// DNS to fabricate a ConnectionTuple.
public record DnsEvent(EventEnvelope envelope, DnsQuery query, DnsResponse response,
                       String sourceIp, boolean isOrig) implements NetworkEvent {

    public DnsEvent {
        Objects.requireNonNull(envelope, "envelope must not be null");

        // A dns record without a query is malformed -- the query is what the
        // record is about, and eight of the twelve protocol features derive from
        // its name.
        Objects.requireNonNull(query, "query must not be null");

        // response is NULLABLE: dns.log records a query that received no answer,
        // and that is a real observation, not a parse failure. The extractor
        // defaults the response features and the caller sets the quality flag.
        if (sourceIp == null || sourceIp.isBlank()) {
            throw new IllegalArgumentException("sourceIp must not be blank");
        }
    }
}
```

- [ ] **Step 3: Add `DnsEvent` to `permits` and fix every compile break**

```java
public sealed interface NetworkEvent permits ConnEvent, DnsEvent {
```

This breaks four pattern switches at compile time, by design. Fix each with an explicit `case DnsEvent` arm — **never `default`**:

- `SourceKeySelector.getKey` → `case DnsEvent dns -> dns.sourceIp()`
- `ConnFeatureProcessFunction.processElement` → this function is conn's. Add `case DnsEvent ignored -> throw new IllegalStateException("ConnFeatureProcessFunction received a DnsEvent; the DNS chain is separate by design (spec section 6.3) and this is a wiring error, not a runtime condition")`.
- `modules/adapter-kafka/src/test/.../EventMapperTest.java` and `modules/domain/src/test/.../NetworkEventTest.java` → add arms asserting the DNS case.

Update `NetworkEvent`'s javadoc: it currently says sealing "makes the compiler flag every non-exhaustive switch the moment a real second protocol arrives". Record that this happened, that it flagged exactly four sites, and that all four were resolved with explicit arms.

- [ ] **Step 4: Run and commit**

```bash
./mvnw install -DskipTests -q -o
./mvnw test -pl modules/domain -o          # 77 + new DnsEventTest cases
./mvnw test -pl modules/adapter-kafka -o   # 23
./mvnw test -pl modules/adapter-flink -o   # 8
git add -A modules/
git commit -m "feat(domain): add DNS to the sealed event hierarchy"
```

---

### Task 4: The frozen `dns-feature-v1` contract

**Files:**
- Create: `contracts/features/dns-feature-schema-v1.json`
- Create: `modules/domain/.../feature/DnsFeatureSchemaV1.java`
- Modify: `modules/domain/.../feature/FeatureSchemaRegistry.java`
- Test: `modules/domain/src/test/.../feature/DnsFeatureSchemaV1Test.java`

**Interfaces:**
- Produces: `DnsFeatureSchemaV1.SCHEMA` (id `dns-feature-v1`, 24 definitions), `DnsFeatureSchemaV1.CONTENT_HASH`.

- [ ] **Step 1: Write the contract JSON**

Indices 0–11 are the common tier, copied **verbatim** from `contracts/features/common-feature-tier-v1.json` — same names, same order, same units, same `missingPolicy`, same formulas. Indices 12–23 are DNS's:

| Index | Name | Unit | Missing policy | Formula |
|---|---|---|---|---|
| 12 | `dns_rcode` | code | DEFAULT_ZERO | IANA RCODE number; 0 (NOERROR) when no response |
| 13 | `dns_qtype` | code | REQUIRED | IANA QTYPE number from `qtype` |
| 14 | `dns_authoritative` | boolean | DEFAULT_ZERO | 1 if `AA` else 0 |
| 15 | `dns_recursion_available` | boolean | DEFAULT_ZERO | 1 if `RA` else 0 |
| 16 | `dns_truncated` | boolean | DEFAULT_ZERO | 1 if `TC` else 0 |
| 17 | `dns_answer_count` | count | DEFAULT_ZERO | length of `answers` |
| 18 | `dns_ttl` | seconds | DEFAULT_ZERO | first element of `TTLs` |
| 19 | `dns_qname_length` | count | REQUIRED | character count of `query` |
| 20 | `dns_qname_entropy` | bits | REQUIRED | Shannon entropy over `query` characters |
| 21 | `dns_label_count` | count | REQUIRED | dot-separated label count of `query` |
| 22 | `dns_digit_ratio` | ratio | REQUIRED | digits / length of `query` |
| 23 | `dns_hyphen_ratio` | ratio | REQUIRED | hyphens / length of `query` |

`"featureCount": 24`, `"inputDtype": "float32"`, `"id": "dns-feature-v1"`, `"semanticVersion": "1.0.0"`.

**Note the `missingPolicy` split deliberately.** Indices 19–23 are REQUIRED because they derive from `query`, which `DnsEvent` requires — they can always be computed. Indices 12, 14–18 are DEFAULT_ZERO because `response` is nullable and a query with no answer is a real observation. Index 13 is REQUIRED: `qtype` is part of the query.

- [ ] **Step 2: Compute and pin the content hash**

```bash
sha256sum contracts/features/dns-feature-schema-v1.json
```

Put that value in `DnsFeatureSchemaV1.CONTENT_HASH`, mirroring `ConnFeatureSchemaV1`'s structure exactly (a `public static final String CONTENT_HASH`, a `public static final FeatureSchema SCHEMA`, a private constructor).

- [ ] **Step 3: Write the drift test**

Mirror `ConnFeatureSchemaV1Test`: read the committed JSON with `MessageDigest`, assert the hash matches `CONTENT_HASH`, and assert every index/name pair in `SCHEMA` matches the file. This is the guard that a schema and its contract cannot drift apart.

Add one assertion the conn test cannot make:

```java
    // The common tier leads every per-protocol schema, so DNS's first twelve
    // features must be the tier's twelve, in the tier's order. A schema that
    // reorders them would still hash consistently against its own file and still
    // load -- this is the only check that catches it.
    @Test
    void theFirstTwelveFeaturesAreTheCommonTierInOrder() {
        for (int i = 0; i < CommonFeatureTierV1.FEATURE_COUNT; i++) {
            assertEquals(CommonFeatureTierV1.FEATURE_NAMES.get(i),
                DnsFeatureSchemaV1.SCHEMA.definitions().get(i).name(),
                "common tier index " + i + " must lead the DNS schema unchanged");
        }
    }
```

- [ ] **Step 4: Register it**

Add `LogType.DNS -> DnsFeatureSchemaV1.SCHEMA` to `FeatureSchemaRegistry.BY_LOG_TYPE`. `BY_ID` derives automatically. `everyLogTypeConstantHasARegisteredSchema` now walks two constants instead of one, which is the limitation its ledger entry said would dissolve here — note that in the commit message.

- [ ] **Step 5: Run and commit**

```bash
./mvnw test -pl modules/domain -o
git add contracts/features/dns-feature-schema-v1.json modules/domain/
git commit -m "feat(domain): freeze the 24-value dns-feature-v1 schema"
```

---

### Task 5: Qname derived features

Pure string math, no framework, no I/O — the most testable code in the unit and the place a subtle error would be least visible downstream.

**Files:**
- Create: `modules/application/src/main/java/io/netsecml/platform/application/feature/QnameFeatures.java`
- Test: `modules/application/src/test/.../feature/QnameFeaturesTest.java`

**Interfaces:**
- Produces: `static int length(String)`, `static double shannonEntropy(String)`, `static int labelCount(String)`, `static double digitRatio(String)`, `static double hyphenRatio(String)`.

- [ ] **Step 1: Write the failing tests first**

```java
    // Entropy is the feature most likely to be quietly wrong, because a plausible
    // implementation returns a plausible number for every input. These are hand-
    // computable cases: a single repeated character carries no information, and
    // four distinct characters in equal proportion carry exactly 2 bits.
    @Test
    void entropyIsZeroForAUniformStringAndTwoBitsForFourEqualSymbols() {
        assertEquals(0.0, QnameFeatures.shannonEntropy("aaaa"), 1e-9);
        assertEquals(2.0, QnameFeatures.shannonEntropy("abcd"), 1e-9);
    }

    // A DGA domain is exactly the case this feature exists to separate from a
    // human-chosen one, so assert the ordering rather than only the arithmetic.
    @Test
    void entropyRanksARandomLabelAboveAnEnglishOne() {
        assertTrue(QnameFeatures.shannonEntropy("x7q2m9v4z1kd.com")
            > QnameFeatures.shannonEntropy("www.example.com"));
    }

    @Test
    void labelCountCountsDotSeparatedLabels() {
        assertEquals(3, QnameFeatures.labelCount("www.example.com"));
        assertEquals(1, QnameFeatures.labelCount("localhost"));

        // A trailing dot is the DNS root and is legal in a qname. It is not a
        // fourth label, and counting it as one would shift every fully-qualified
        // name's feature by exactly 1 against every relative name's.
        assertEquals(3, QnameFeatures.labelCount("www.example.com."));
    }

    @Test
    void ratiosAreOverTotalLengthAndSafeOnEmptyInput() {
        assertEquals(0.5, QnameFeatures.digitRatio("ab12"), 1e-9);
        assertEquals(0.25, QnameFeatures.hyphenRatio("a-bc"), 1e-9);

        // Division by zero would produce NaN, which serializes into the vector and
        // poisons any model that sees it. Empty is not reachable today (DnsEvent
        // requires a query) but the guard is one line and the failure is silent.
        assertEquals(0.0, QnameFeatures.digitRatio(""), 1e-9);
    }
```

- [ ] **Step 2: Run to verify they fail**

`./mvnw test -pl modules/application -o` — expect compilation failure, `QnameFeatures` does not exist.

- [ ] **Step 3: Implement**

Shannon entropy over character frequencies, base 2. Guard every ratio against zero length. Treat a trailing dot as the root label and not a label.

- [ ] **Step 4: Run, then commit**

```bash
./mvnw test -pl modules/application -o     # 16 + the new cases
git add modules/application/
git commit -m "feat(application): derive qname shape features without a vocabulary"
```

---

### Task 6: `DnsFeatureExtractor` — the twelve protocol-tier values

**Files:**
- Create: `modules/application/src/main/java/io/netsecml/platform/application/feature/DnsFeatureExtractor.java`
- Test: `modules/application/src/test/.../feature/DnsFeatureExtractorTest.java`

**Interfaces:**
- Produces: `float[] extractProtocolTier(DnsEvent event)` returning exactly 12 values, in schema order for indices 12–23.

- [ ] **Step 1: Write the failing test, including the null-response case**

```java
    // response is nullable by design: dns.log records a query that got no answer.
    // Every response-derived feature defaults to zero, and every query-derived one
    // must still be computed -- if a missing response zeroed the qname features
    // too, the unanswered queries a DGA generates would look identical to each
    // other regardless of the name asked for, which is the signal.
    @Test
    void anUnansweredQueryStillCarriesEveryQnameFeature() {
        DnsEvent event = dnsEvent("x7q2m9v4z1kd.com", DnsQType.A, 4242, null);

        float[] values = new DnsFeatureExtractor().extractProtocolTier(event);

        assertEquals(12, values.length);
        assertEquals(0f, values[0], "dns_rcode defaults to 0 with no response");
        assertEquals(0f, values[5], "dns_answer_count defaults to 0");
        assertEquals(16f, values[7], "dns_qname_length is computed from the query regardless");
        assertTrue(values[8] > 0f, "dns_qname_entropy is computed from the query regardless");
    }
```

Mirror the existing `EventFeatureExtractor` in shape: narrowed to `DnsEvent`, no switch, one `float[]` built positionally with a comment per block naming the indices it fills.

- [ ] **Step 2–4: Run red, implement, run green, commit**

```bash
git commit -m "feat(application): extract the DNS protocol feature tier"
```

---

### Task 7: `ZeekDnsEvent` DTO and `JsonZeekDnsParser`

**Files:**
- Create: `modules/adapter-kafka/.../dto/ZeekDnsEvent.java`, `modules/adapter-kafka/.../parser/JsonZeekDnsParser.java`
- Test: `modules/adapter-kafka/src/test/.../parser/JsonZeekDnsParserTest.java`

**Interfaces:**
- Produces: `MappingResult<ZeekDnsEvent> parse(byte[] rawPayload)`.

- [ ] **Step 1: Write the DTO**

Mirror `ZeekConnEvent`'s Jackson style exactly. Required: `id`, `ts`, `id_orig_h`, `id_orig_p`, `id_resp_h`, `id_resp_p`, `trans_id`, `query`. Optional (boxed, nullable): `qtype`, `rcode`, `AA`, `RA`, `TC`, `answers` (`List<String>`), `TTLs` (`List<Double>`), `rejected`.

```java
public record ZeekDnsEvent(
    @JsonProperty(value = "id", required = true) String id,
    @JsonProperty(value = "ts", required = true) double ts,
    @JsonProperty(value = "id_orig_h", required = true) String idOrigH,
    @JsonProperty(value = "id_orig_p", required = true) int idOrigP,
    @JsonProperty(value = "id_resp_h", required = true) String idRespH,
    @JsonProperty(value = "id_resp_p", required = true) int idRespP,
    // trans_id is REQUIRED because it is half of this log type's event identity
    // (spec section 5: sensor:uid:trans_id). A dns record without it cannot be
    // given a unique ID, so it is a MAP-stage rejection, not a defaulted field.
    @JsonProperty(value = "trans_id", required = true) int transId,
    @JsonProperty(value = "query", required = true) String query,
    @JsonProperty("qtype") Integer qtype,
    @JsonProperty("rcode") Integer rcode,
    @JsonProperty("AA") Boolean aa,
    @JsonProperty("RA") Boolean ra,
    @JsonProperty("TC") Boolean tc,
    @JsonProperty("answers") List<String> answers,
    @JsonProperty("TTLs") List<Double> ttls
) {
}
```

- [ ] **Step 2: Write the parser as a copy of `JsonZeekConnParser`**

Same `ReasonCode.MALFORMED_JSON` at `Stage.PARSE`, same `ObjectMapper` configuration, same failure detail format. Read `JsonZeekConnParser` first and match it rather than inventing a second style.

- [ ] **Step 3: Test the cases that differ from conn**

A record with `answers` absent, a record with `TTLs` present but empty, and a record missing `trans_id` (must reject). Assert the rejection reason and stage, not merely that it failed.

- [ ] **Step 4: Run and commit**

```bash
./mvnw test -pl modules/adapter-kafka -o
git commit -m "feat(adapter-kafka): parse dns.log records"
```

---

### Task 8: `DnsEventMapper` and event identity — §10's fixture obligation

**Files:**
- Create: `modules/adapter-kafka/.../mapper/DnsEventMapper.java`
- Test: `modules/adapter-kafka/src/test/.../mapper/DnsEventMapperTest.java`

**Interfaces:**
- Produces: `MappingResult<NetworkEvent> map(ZeekDnsEvent dto, SensorId sensor)`.

- [ ] **Step 1: Write §10's required fixture test FIRST**

Spec §10 makes this non-negotiable: *"A fixture test feeding two records sharing a `uid` and asserting two distinct `eventId`s."* A log type whose uniqueness argument cannot be evidenced from its own fields is not ready to be added.

```java
    // Spec section 10's fixture obligation, and the reason DNS's identity is
    // sensor:uid:trans_id rather than sensor:uid. A resolver reuses one connection
    // for many queries, so several dns.log records legitimately share a uid. If
    // eventId were derived from uid alone they would collapse into ONE
    // ReplacingMergeTree row and every query but the last would vanish from the
    // archive -- silently, with no error anywhere.
    @Test
    void twoQueriesSharingAUidGetDistinctEventIds() {
        SensorId sensor = new SensorId("sensor-eu-1");
        ZeekDnsEvent first = dto("CXWv6p3arKYeMETxOg", 4242, "example.com");
        ZeekDnsEvent second = dto("CXWv6p3arKYeMETxOg", 4243, "example.org");

        NetworkEvent a = new DnsEventMapper().map(first, sensor).value();
        NetworkEvent b = new DnsEventMapper().map(second, sensor).value();

        assertNotEquals(a.eventId().value(), b.eventId().value());
        assertEquals("sensor-eu-1:CXWv6p3arKYeMETxOg:4242", a.eventId().value());

        // connectionUid is the CORRELATION key and is SUPPOSED to be shared -- it
        // is what joins these two queries back to their connection. Asserting it
        // stays equal is asserting the two concepts did not get conflated.
        assertEquals(a.connectionUid(), b.connectionUid());
    }
```

- [ ] **Step 2: Implement the mapper**

`EventId.derive(sensor, dto.id() + ":" + dto.transId())`. `LogType.DNS` on the envelope. `connectionUid` is `dto.id()`. `eventTime` from `ts` the same way `EventMapper` does it — read that code, do not re-derive the conversion.

`response` is `null` when `rcode` is absent; otherwise `DnsRcode.fromCode(rcode)` with the booleans defaulting to false and `answerCount` from `answers.size()` (0 when absent) and `firstTtlSeconds` from `TTLs.get(0)` (0 when absent or empty).

Rejections use the existing taxonomy unchanged: an absent required field is `MISSING_REQUIRED_FIELD` at `Stage.MAP`.

- [ ] **Step 3: Run and commit**

```bash
./mvnw test -pl modules/adapter-kafka -o
git commit -m "feat(adapter-kafka): map dns records with per-query event identity"
```

---

### Task 9: Make `ParseMapValidateFunction` abstract, with per-protocol subclasses

`ParseMapValidateFunction` wears a generic name over a conn-only body — it constructs `JsonZeekConnParser` and `EventMapper` in `open()`. The 3b review flagged this. The fix keeps the processElement logic in one place rather than copying it.

**Files:**
- Modify: `modules/adapter-flink/.../process/ParseMapValidateFunction.java` (becomes abstract, generic in the DTO)
- Create: `modules/adapter-flink/.../process/ConnParseMapValidateFunction.java`, `DnsParseMapValidateFunction.java`
- Modify: `modules/bootstrap-online-job/.../OnlineFeatureJob.java`
- Test: existing `ParseMapValidateFunctionTest` retargets to the conn subclass; add a DNS case

**Interfaces:**
- Produces: `abstract class ParseMapValidateFunction<D> extends ProcessFunction<byte[], NetworkEvent>` with `protected abstract MappingResult<D> parse(byte[])`, `protected abstract MappingResult<NetworkEvent> map(D, SensorId)`, `protected abstract String deriveEventId(D)`.

- [ ] **Step 1: Generify without changing behaviour**

The two-stage body (parse → side-output on failure; map → side-output with a derived ID on failure; collect) moves unchanged into the abstract class. `REJECTED_TAG` stays a single static on the abstract class — one DLQ tag, many subclasses.

**Abstract methods, not a constructor-injected parser.** A Flink function's fields must be `Serializable` or built in `open()`; the existing class builds them in `open()` for exactly that reason. Subclass `open()` methods keep doing so, and the abstract methods delegate to those transient fields. **Do not replace this with a lambda or a `Supplier` field** — that is how this project's three serialization defects got onto `main`.

- [ ] **Step 2: Write the two subclasses**

`ConnParseMapValidateFunction` holds `JsonZeekConnParser`/`EventMapper` and derives its event ID from `dto.id()` with the existing blank-id fallback. `DnsParseMapValidateFunction` holds `JsonZeekDnsParser`/`DnsEventMapper` and derives `dto.id() + ":" + dto.transId()`, with the same fallback when `id` is blank.

- [ ] **Step 3: Retarget the existing test and add a DNS case**

The existing tests must keep passing against `ConnParseMapValidateFunction` **with their assertions unchanged** — that is what proves the generification changed no behaviour. Add one DNS test asserting a malformed dns payload lands on `REJECTED_TAG` with `MALFORMED_JSON`/`PARSE`.

- [ ] **Step 4: Run and commit**

```bash
./mvnw test -pl modules/adapter-flink -o
git commit -m "refactor(adapter-flink): one parse-map-validate body, one subclass per log type"
```

---

### Task 10: `DnsWindowState`, `DnsBuildFeaturesUseCase`, `DnsFeatureProcessFunction`

Assembles the 24 values: common tier (Task 1's `RollingCounters` + `RecordTimingState` + enrichment) then the protocol tier (Task 6).

**Files:**
- Create: `modules/domain/.../feature/DnsWindowState.java`, `modules/domain/.../feature/QualityFlags.java`
- Create: `modules/application/.../usecase/DnsBuildFeaturesUseCase.java`
- Create: `modules/adapter-flink/.../process/DnsFeatureProcessFunction.java`
- Test: `modules/application/src/test/.../usecase/DnsBuildFeaturesUseCaseTest.java`

**Interfaces:**
- Produces: `record DnsWindowState(RollingCounters counters, RecordTimingState timing)` with `static DnsWindowState empty()`; `DnsBuildFeaturesUseCase implements BuildFeaturesUseCase<DnsEvent, DnsWindowState>`.

- [ ] **Step 1: `QualityFlags` — the first real use of a field hard-coded to 0**

```java
// Bit flags recording how a feature vector was produced, not what it observed.
//
// FeatureVector.qualityFlags has been hard-coded 0 since it was defined. This is
// its first real use: spec section 6.2 requires that a vector built without a
// conn.log snapshot be distinguishable from one where the connection genuinely
// moved no bytes. The common tier's conn_enrichment_present (index 11) already
// records it inside the vector for the model; this flag records it OUTSIDE the
// values, so a query over archived rows can filter on provenance without knowing
// any schema's index layout.
public final class QualityFlags {

    public static final int NONE = 0;

    // No conn.log snapshot was available for this record's uid at build time.
    // Expected, not exceptional: a connection's first snapshot does not exist
    // until it has been alive five minutes.
    public static final int CONN_ENRICHMENT_ABSENT = 1;

    private QualityFlags() {
    }
}
```

- [ ] **Step 2: Write the failing use-case test**

```java
    // The load-bearing assertion of the whole unit: DNS's vector is the common
    // tier followed by DNS's own features, at the width its registered schema
    // declares -- not conn's 20, and not a number written down in this class.
    @Test
    void buildsTwentyFourValuesWithTheCommonTierLeading() {
        FeatureBuildResult<DnsWindowState> result =
            new DnsBuildFeaturesUseCase(FIXED_CLOCK).build(DNS_EVENT, DnsWindowState.empty());

        assertEquals(24, result.vector().values().length);
        assertEquals(FeatureSchemaRegistry.byLogType(LogType.DNS).featureCount(),
            result.vector().values().length);
        assertEquals("dns-feature-v1", result.vector().schemaId());

        // Index 0 is the common tier's record_count_5m, and this is the first
        // record for the key, so the window contains exactly this one.
        assertEquals(1f, result.vector().values()[0]);

        // Index 12 is the first DNS-specific value. If the two tiers were
        // concatenated in the wrong order every model trained on this is wrong,
        // and nothing else in the suite would notice.
        assertEquals(DnsRcode.NXDOMAIN.code(), result.vector().values()[12], 1e-6);
    }

    // A failed lookup must reach the rolling window, or failed_count_5m stays 0
    // for a host doing nothing but NXDOMAIN lookups -- the exact pattern the
    // feature exists to surface.
    @Test
    void anNxdomainResponseCountsAsAFailureInTheWindow() {
        FeatureBuildResult<DnsWindowState> first =
            new DnsBuildFeaturesUseCase(FIXED_CLOCK).build(DNS_EVENT, DnsWindowState.empty());

        assertEquals(1f, first.vector().values()[2], "failed_count_5m after one NXDOMAIN");
    }
```

- [ ] **Step 3: Implement**

`build` folds the event into `RollingCounters.record(bucketMinute, 0L, rcodeIsFailure)` — **`0L` bytes, because `dns.log` carries no byte counts**; the common tier's `byte_sum_5m` for DNS comes from conn enrichment or stays zero. Fold the timestamp into `RecordTimingState`. Then `CommonFeatureExtractor.extract(...)` for 0–11 and `DnsFeatureExtractor.extractProtocolTier(...)` for 12–23, `System.arraycopy`'d into a `new float[schema.featureCount()]`. Resolve the schema **once in the constructor**, as `ConnBuildFeaturesUseCase` now does.

Set `qualityFlags` to `CONN_ENRICHMENT_ABSENT` when the enrichment delta is null.

- [ ] **Step 4: The Flink function**

Mirror `ConnFeatureProcessFunction`: `ValueState<DnsWindowState>` with descriptor name `"dns-window-state"`, use case built in `open()`, narrowing `case DnsEvent` arm with a `case ConnEvent` arm that throws the wiring-error message.

- [ ] **Step 5: Run and commit**

```bash
./mvnw test -pl modules/application -o
./mvnw test -pl modules/adapter-flink -o
git commit -m "feat: build DNS feature vectors from the common and protocol tiers"
```

---

### Task 11: The conn.log enrichment left join

Spec §6.2. Unit 1 built `ConnSnapshot` and `ConnSnapshotDelta` but **no protocol consumes them** — DNS is the first. The join is keyed by `uid`, while the feature window is keyed by `SourceKey`, so this is a separate keyed operator upstream of the window.

**Files:**
- Create: `modules/adapter-flink/.../process/ConnSnapshotJoinFunction.java`, `modules/adapter-flink/.../process/UidKeySelector.java`
- Test: `modules/adapter-flink/src/test/.../process/ConnSnapshotJoinFunctionTest.java`

- [ ] **Step 1: Write the failing test — the non-blocking property is the point**

```java
    // Spec section 6.2: blocking on conn.log would be a CORRECTNESS failure, not a
    // latency cost. A connection's first snapshot does not exist until it has been
    // alive five minutes, so waiting would stall every record from every new
    // connection and hold unbounded state per open connection. This test asserts
    // the record comes out immediately with the enrichment absent -- if someone
    // later "improves" this into a blocking join, this is what fails.
    @Test
    void emitsImmediatelyWhenNoSnapshotHasArrivedForTheUid() throws Exception {
        // harness setup omitted here -- mirror ConnFeatureProcessFunctionTest's
        // KeyedOneInputStreamOperatorTestHarness setup exactly
        harness.processElement(dnsEventWithUid("CXWv6p3arKYeMETxOg"), 1_000L);

        assertEquals(1, harness.extractOutputValues().size(),
            "the record must not be held waiting for a snapshot");
        assertNull(harness.extractOutputValues().get(0).enrichment(),
            "absent enrichment is null, not a fabricated zero snapshot");
    }
```

- [ ] **Step 2: Implement as a `KeyedProcessFunction` on `uid`**

State: `ValueState<ConnSnapshot>` named `"conn-snapshot"`. Every DNS record performs a left join against it and emits a carrier record `(DnsEvent, ConnSnapshotDelta)` — delta null when no snapshot exists. Snapshots are **cumulative**, so the delta is computed against the previously stored snapshot, which the state already holds.

**Configure a state TTL here.** Unlike the rolling window, this state is keyed by `uid` — one entry per connection observed, unbounded without a TTL. Use `StateTtlConfig` with a 30-minute idle expiry and `NeverReturnExpired`. Comment that this is the first TTL in the codebase and that `OnlineFeatureJob`'s `KNOWN GAP:` note about the window's unbounded key set is a **separate** and still-open issue.

- [ ] **Step 3: Run and commit**

```bash
./mvnw test -pl modules/adapter-flink -o
git commit -m "feat(adapter-flink): left-join conn.log snapshots without blocking"
```

---

### Task 12: Wiring — topics, chains, and operator uids

**Files:**
- Modify: `modules/bootstrap-online-job/.../OnlineFeatureJob.java`, `modules/bootstrap-archive-job/.../ArchiveJob.java`, `.env.example`

- [ ] **Step 1: Add the DNS environment variables**

```
DNS_INPUT_TOPIC=dns
DNS_FEATURE_VECTOR_TOPIC=netsec.dns.feature-vector.v1
DNS_DLQ_TOPIC=netsec.dns.dlq.v1
```

Rename nothing existing — `CONN_INPUT_TOPIC`, `FEATURE_VECTOR_TOPIC` and `DLQ_TOPIC` keep their names and meanings so no deployment breaks.

- [ ] **Step 2: Add the DNS chain to `OnlineFeatureJob`**

A second source → parse → keyBy → process → sink chain, structurally identical to conn's. **Every operator gets its own uid**, prefixed by log type: `dns-raw-source`, `dns-parse-map-validate`, `dns-feature-extraction`, `dns-feature-vector-sink`, `dns-dlq-sink`. Conn's five uids **must not change** — a changed uid silently discards state.

Extract the shared chain-building into a private method taking the log type, topics and process function, so the third protocol is a call rather than a copy. This mirrors what §6.3 already required of `ArchiveJob` and Unit 1 delivered there.

- [ ] **Step 3: Add the DNS chains to `ArchiveJob`**

`ArchiveJob.build` already takes `List<LogTypeChain<?>>` and loops. Add a feature-vector chain and `dlqChain(LogType.DNS, dnsDlqTopic)`. Verify the uid prefix logic (`logType == CONN ? "" : logType.wireName() + "-"`) produces distinct uids for DNS — this is the branch the Unit 2 ledger recorded as untestable with one log type, so **assert it now** in `ArchiveJobTest`.

- [ ] **Step 4: Run and commit**

```bash
./mvnw install -DskipTests -q -o
./mvnw test -pl modules/bootstrap-archive-job -o
# Scoped, never `git add -A`: the repo root holds ~139 MB of untracked model
# binaries (new-models/, modbus_rf_attack_type_v1/) that an unscoped add sweeps in.
git add modules/bootstrap-online-job modules/bootstrap-archive-job .env.example
git commit -m "feat(bootstrap): wire the DNS chains in both jobs"
```

---

### Task 13: End-to-end proof and documentation

- [ ] **Step 1: Extend the online E2E to DNS**

`OnlineFeatureJobE2ETest` currently proves conn. Add a DNS case to the same test class, reusing its Kafka container: publish one `dns.log` JSON record to the DNS input topic, consume from `netsec.dns.feature-vector.v1`, and assert the vector has **24 values**, `schemaId` `dns-feature-v1`, and `logType` `dns`.

**Do not run the two jobs in one container-heavy test if memory forbids it.** This machine has ~856 MiB free plus swap and the conn E2E alone takes ~80 s. If the DNS case cannot run alongside, report that verbatim as an environment limit and leave the test in place, `@Disabled` with a reason naming the memory constraint — **do not weaken the assertions to make it fit**, and never describe it as passing.

- [ ] **Step 2: Update `CLAUDE.md`**

Add to Key invariants:

```markdown
- A protocol's feature schema is the common tier (12 values, frozen) followed by that
  protocol's own tier. `conn-feature-v1` predates the tier and is frozen without it; every
  schema from `dns-feature-v1` onward leads with it.
- Event identity is a per-log-type obligation with its own stated argument: `CONN` is
  `sensor:uid`, `DNS` is `sensor:uid:trans_id`. A log type whose uniqueness cannot be
  evidenced from its own fields is not ready to be added.
```

Update **Implementation state** and **Verification state** — both have been stale since the `feat/clickhouse-archive-job` snapshot and Unit 3b deliberately deferred fixing them. Record what is now true, including `LogType` having two constants.

- [ ] **Step 3: Commit**

```bash
# Scoped, never `git add -A` -- see Task 11's note on the untracked model binaries.
git add CLAUDE.md modules/bootstrap-online-job
git commit -m "docs: record DNS as the second implemented protocol"
```

---

## Self-Review

**Spec coverage.** §3.1 common tier → Tasks 1, 10 (it leads the DNS schema). §4.1 DNS features → Tasks 4, 5, 6, with `dns_ngram_score` deviated from and the argument stated. §5 identity → Task 8, evidenced per §10. §6.2 enrichment → Task 11. §6.3 chain fan-out → Task 12. §7.1 categoricals from IANA → Task 3's enums. §8 raw text excluded → no qname, host or URL appears in any schema; only derived numerics. §10 fixture obligations → Task 8 Step 1; the counter-wrap case is Modbus/S7comm only and does not apply here.

**What this plan does not do, deliberately.** It does not split `Endpoints` onto `EventEnvelope` — `DnsEvent` carries `sourceIp` directly and says so in a comment, because that refactor touches `EventFeatureExtractor`'s reads of the frozen conn path and belongs in its own unit. It does not add the HTTP or SSH schemas. It does not close `OnlineFeatureJob`'s unbounded rolling-window key set; Task 11 adds a TTL only to the new uid-keyed enrichment state and says so explicitly.

**Type consistency.** `RollingCounters` (Task 1) is the state type in `BuildFeaturesUseCase<ConnEvent, RollingCounters>` and a component of `DnsWindowState` (Task 10). `SourceKey`'s three components (Task 2) match `SourceKeySelector`'s construction. `DnsRcode.code()` returns the `int` that Task 6 writes to index 12 and Task 10 asserts. `FeatureSchemaRegistry.byLogType(LogType.DNS)` (Task 4) is what Task 10's use case resolves in its constructor.

**The cross-task dependency I could not remove.** Task 2's test needs `LogType.DNS`, which Task 3 adds. Rather than hide it, Task 2 Step 1 names it and tells the implementer to add that one constant early and record having done so. Reordering to put `LogType` first would split Task 3's coherent "add DNS to the hierarchy and fix what breaks" into two.

**The risk that is not in any task.** Two checkpoint-state breaks land in Tasks 1 and 2. Both are free **today** and would not be after deployment, and the plan says so in both the ruling and the code comment. If this unit is executed after this branch chain merges and deploys, that reasoning expires and Tasks 1 and 2 need re-deciding — not re-running as written.

**A test that cannot be written here.** Nothing proves the enrichment join against a *real* `conn.log` stream, because the online job has no conn-snapshot source wired — Unit 1 built the carrier types and left the producer for the first consumer. Task 11 tests the join with a synthetic snapshot in a Flink harness, which proves the left-join semantics but not the end-to-end enrichment path. Stated rather than hidden; the first unit that wires a real snapshot source closes it.
