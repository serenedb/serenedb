# Testing, CI, Benchmarking & Fuzzing -- State of the Art and Improvement Plan

Status: draft for review. Compiled by surveying SereneDB's current CI/test tree,
ClickHouse, and the vendored DuckDB. Priorities below are a *proposal* -- the
ordering is open for review.

---

## Issue breakdown (detailed docs)

This plan is the overview; the actionable work is split into separate issues:

1. [PR trigger (change-aware defaults) + nightly](issues/1-pr-trigger-and-nightly.md)
   -- keep manual gate, auto-select suites by change, move sqlite->nightly,
   disable extensions/searchbench by default, parallelize, make coverage visible.
2. [Coverage in PRs](issues/2-coverage-in-pr.md) -- ClickHouse-style diff
   coverage as a (non-blocking) part of the default run.
3. [Performance tests](issues/3-perf-tests.md) -- manual-mark, dedicated cloud
   instance, dogfood SereneDB for history.
4. [Better fuzzing](issues/4-fuzzing.md) -- PG wire protocol + iresearch storage,
   crash->repro substrate.
5. [Other](issues/5-other.md) -- test-runner strategy (unittest/sqllogictest-rs),
   pg-protocol testing, scale/robustness, staged automation.

Supporting: [test classification + validated dependency graph](issues/test-classification.md).

---

## 0. TL;DR -- where the real lever is

The build/test matrix runs when a maintainer **checks a box** in the bot comment
(`pr-build-trigger.yml` -> `build-manual.yml`); `simple.yml` runs pre-commit
automatically. **This manual trigger is intentional and correct, and stays:**

- It is a **required merge gate** -- merge is impossible without a passing run.
- It deliberately **avoids auto-running on every commit**, which would saturate
  a 5-machine fleet (and waste it on WIP pushes). The author triggers when ready.

So the goal "fast PR CI (5-10 min)" is **not** about making CI automatic. It is
about making the *triggered* run **fast and selective**. The actual gap: the
triggered run is essentially monolithic -- a one-line or docs-only PR pays nearly
the same cost as a storage-engine rewrite, and there is no path-based tiering
(except the PG-test filter). The lever is **tiering and changed-path filtering
*inside* the manual-triggered pipeline**, not removing the trigger.

The heavy plumbing already exists and is good (Docker build image, ccache on
`/mnt/data`, reusable workflow, sanitizer presets, llvm-cov collection,
sqlsmith, recovery harness) -- this is assembly, not greenfield.

**Guiding principle (already correct in the brief):** DuckDB exhaustively tests
its own SQL engine (~4,190 `.test` files). Re-running isolated SQL semantics
per-PR is low marginal value for us. SereneDB's *novel* surface -- the only part
nobody upstream tests -- is four things, and that is where the PR budget goes:

1. **iresearch storage** (index, columnstore, WAL, recovery, compaction)
2. **the connector** (DuckDB <-> iresearch: vector builders, column serializers,
   search sink, key builders) -- *and its unit tests are mostly commented out today*
3. **PG wire protocol + server**
4. **whole-DB integration** (transactions, durability, recovery)

> **Out of scope -- being removed soon (do not invest in testing/fuzzing):**
> **RocksDB** and **vpack**. Both are still in the tree but slated for removal,
> so skip them entirely -- including the existing `vpack-tests` and
> `vpack_fuzzer`. Every "storage" item below means **iresearch** storage
> (segments / columnstore / WAL), never RocksDB layouts.

---

## 1. Hardware reality (shapes every decision)

CI fleet:

- **5x amd64**: AMD Ryzen 9 9950X, 16c/32t, 192 GiB RAM, ~1.8 TB disk.
  CPU frequency scaling observed at ~77% -- unpinned, so unsuitable for stable
  perf numbers without a governor change.
- **1x arm64**: ~128 threads, but *small* threads. Ideal for highly-parallel
  suites (gtest-parallel, sharded sqllogic) and as a second-arch correctness
  check; poor for single-thread-sensitive perf.

Implications:

- This is **not** ClickHouse's CI. We cannot run dozens of sharded sanitizer +
  perf + fuzz jobs per PR. We must be selective and lean on nightly.
- **Dedicated cloud perf instance** (AWS/GCS, paid from existing credits) +
  its own persistent disk, instead of reserving one of the 5 local machines.
  Pick a fixed instance type (prefer bare-metal / dedicated, not burstable) so
  numbers are comparable run-to-run; pin governor to `performance`, control
  turbo/SMT jitter, no co-scheduled jobs. It only runs when a perf job is
  **manually marked** (see P1.2), so it can be small/cheap and even
  stopped-when-idle to save credits.
- Honest PR latency target: see the **measured baseline** in §2.1 -- with warm
  ccache the build is ~1 min, so the compiler is *not* the long pole; the
  integration-test step is. Trimming it is what gets a simple PR toward ~5 min.

---

## 2. Current inventory

### 2.1 Measured baseline -- run #750 (`build & test`, `functional none`, warm cache)

Real numbers from a representative triggered PR run
(actions run 26664916294), not estimates:

| Job | Wall time |
|---|---|
| compile check (build only) | 1m34s |
| **unit none** (build 208s + gtest 195s) | 6m57s |
| **functional none** (build **56s** + integration **635s**) | 11m44s |
| validate pg (no PG-test changes -> skipped) | 14s |

Inside the **functional** job's 635s integration step:

| Sub-suite | Time | Note |
|---|---|---|
| **sqllogic** | **473s** | the long pole (~75% of the step) |
| driver | 95s | 11 languages x protocols |
| recovery | 40s | 100 tests, ~24 workers |
| extension | 27s | postgres_scanner etc. |

Key facts this establishes:

- **Warm ccache build ≈ 56s.** The compiler is *not* the bottleneck; sqllogic is.
- **sqllogic is 473s, and 396 of 756 sqllogic files (52%) are sqlite-derived**
  (`tests/sqllogic/any/pg/sqlite/{aggregates,select,expr,groupby}`) -- pure SQL
  semantics that exercise DuckDB's engine, not our novel surface. This is the
  single biggest, lowest-value chunk of PR time. (Exact wall-time share of the
  sqlite subset isn't separable from this log, but at half the files it is a
  large fraction of the 473s; worth measuring precisely as step 1 of P0.)
- sqllogic runs **two engines** (pg-wire-simple + pg-wire-extended), ~doubling
  its cost -- relevant when deciding what the PR tier must cover vs. nightly.
- A couple of individual tests are huge (single "Finished in" entries of ~242s
  and ~213s) -- a handful of outliers worth isolating/splitting (quick win).
- **Queue/wait:** in this run jobs started promptly (no visible runner-queue
  delay), but with 5 machines under concurrent PRs that won't always hold --
  **measure queue wait under load** before trusting wall-time alone. The manual
  trigger helps here: it naturally caps how many runs compete for the fleet.

**The opportunity (your example, quantified):** move the sqlite-derived sqllogic
to nightly and the PR sqllogic suite roughly halves in file count. The freed
budget makes it affordable to run *our own* (storage/connector/pg) sqllogic +
recovery **under ASAN/TSAN on every triggered PR** -- far more valuable per
second than re-running DuckDB's SQL semantics uninstrumented.



| Area | Status | Notes |
|---|---|---|
| Pre-commit lint/format | auto on PR | the only thing automatic on PR |
| Build (clang-21, ninja, ccache) | yes | Docker image, `/mnt/data` caches |
| gtest unit | yes | iresearch (~68), serenedb_basics (~34), connector (~6 active, **many commented out**), fuerte; ~~vpack (~12 variants)~~ *(being dropped)* |
| sqllogic | yes | ~340 `.test` + ~416 `.test_slow`; any/sdb/pg subtrees; sqllogictest-rs runner |
| recovery (fault injection + restart) | yes | ~100 tests, `SET sdb_faults`, ~24 workers |
| driver tests | yes | Python (asyncpg/psycopg2/3, COPY, psql mode) + multi-lang harness spec |
| sqlsmith (SQL fuzzer) | partial | 10k queries nightly, seeded schema; SQL-level only |
| DuckDB extension tests | opt-in | postgres_scanner/avro/httpfs via DuckDB `unittest`, `-DSDB_BUILD_DUCKDB_UNITTESTS` |
| sanitizers ASAN/MSAN/TSAN/UBSAN | nightly | one per weekday; suppressions in `resources/suppressions/` |
| coverage (llvm-cov) | collected, **not reported** | HTML artifact only; no PR comment, no codecov, no diff-cov, no gate |
| microbench (Google Benchmark) | **manual only** | ~20 benches in `tests/bench/micro`, never run in CI |
| searchbench | **default-on in PR, but not a tracked benchmark** | wiki_small from S3; in its current form a *load exercise* of the search path (postings iteration, score compute, WAND/MaxScore pruning), not a per-commit-tracked metric. Low value as a default PR gate; results not stored/compared across commits |
| ~~vpack_fuzzer~~ | *(being dropped)* | custom loop; not worth migrating |
| **Auto PR build/test** | **missing** | manual checkbox only |
| **Path/changed-file filtering** | **missing** (except PG tests) | everything-or-nothing otherwise |
| **Perf regression tracking** | **missing** | no baseline, no history, no flamegraph, no gate |
| **Coverage in PR** | **missing** | -- |
| **PG wire-protocol fuzzing** | **missing** | -- |
| **Storage-layout/corruption fuzzing** | **missing** | -- |
| **Randomized fault-injection fuzzing** | **missing** | only hand-written recovery cases |
| **Flaky-test detection/quarantine** | **missing** | -- |
| **Test sharding across machines** | **missing** | sqllogic on 1 runner |
| **Crash -> issue automation** | **missing** | -- |

Strengths to preserve: clean reusable-workflow structure, real recovery/fault
injection (rare and valuable), multi-driver coverage, sanitizer suppressions
already curated, llvm-cov profiles already produced (reporting is the only
missing half).

---

## 3. What ClickHouse and DuckDB do that we don't

### ClickHouse (ambitious end)

- **Two-server perf comparison** -- baseline vs PR side by side, N runs,
  scipy stats, quantile ± 1.5x regression threshold; **all metrics stored in a
  ClickHouse table** (`query_metrics_v2`), queryable; **flamegraph
  auto-attached on regression** (GDB sampling). This is exactly the
  "see each commit's perf + compare flamegraphs" target.
- **Targeted testing** -- `find_tests.py` selects tests by changed files,
  previously-failed-for-this-PR (history with time decay), and
  code-diff->coverage relevance. Digest-based build cache skips rebuilds.
- **Fuzzer fleet** -- AST fuzzer (mutate real queries), BuzzHouse
  (SQLancer-style generate+compare), and a suite of libFuzzers including a
  `tcp_protocol_fuzzer` and per-format/compression/deserialization fuzzers.
  Persistent S3 corpus with coverage-guided minimization. Crash logs parsed -> issues.
- **Hash-based sharding** (`--run-by-hash-num/total`), flaky-check mode,
  per-test coverage + **diff-coverage report** + "newly covered code" per PR.

### DuckDB (pragmatic end -- closer to our scale)

- **`scripts/regression/test_runner.py`** -- base vs branch, **5 runs, geomean,
  10% + 50 ms thresholds**, GitHub step-summary table. Benchmarks declared as
  CSV lists (`.github/regression/*.csv`). Simple, robust, no big infra.
  **This is the model to start perf with.**
- **Storage backward-compat** -- old `.db` files committed
  (`test/storage/bc/db_0xx.db`), opened by new code every CI run;
  `test_storage_compatibility.py` cross-version.
- **Config-matrix testing** -- *one* runner + JSON configs (`force_storage`,
  `block_verification`, `wal_verification`, `block_size_16kB`, ...) instead of N
  jobs. Cheap way to multiply storage coverage.
- **Fuzzer-bug -> regression test** -- every fuzzer find becomes a committed
  `.test` under `test/fuzzer/{afl,duckfuzz,pedro,sqlsmith}/`.
- **`run_tests.py`** -- batching, slow-test isolation, retries, RSS-leak
  monitoring. **codecov on `/src` only**, informational (non-blocking).

---

## 4. Improvement plan (phased; ordering open for review)

Notation: **payoff** / **effort** are rough. "Fast lane" = the default
automatic PR pipeline targeted at ~8-12 min warm-ccache.

### P0 -- Make the *triggered* PR run fast and selective  *(prerequisite for everything)*

The manual trigger stays (required gate; avoids per-commit fleet waste). The
work is to tier and path-filter *what that trigger runs*, so a simple PR's run
is cheap and a deep change still gets the full matrix. The measured baseline
(§2.1) says where the time goes: sqllogic 473s, half of it sqlite-derived.

- **P0.0 Measure precisely** -- instrument the sqllogic runner to print
  per-subtree (sqlite vs ours) and per-engine (simple vs extended) wall time,
  and capture runner **queue/wait** time under concurrent PRs. Cheap, and turns
  the §2.1 estimates into exact budgets before we cut. payoff high / effort low
- **P0.1 Changed-files classifier** (`dorny/paths-filter` or a small script),
  computed at trigger time. **Coarse on purpose** -- four categories only:
  `duckdb`, `extension`, `iresearch`, `core` (default), with superset rules
  (duckdb ⊃ iresearch ⊃ core). No docs-only/cmake-only/connector-only cases.
  These gate the *extra* heavy suites (iresearch-tests, searchbench, extension
  tests); the integration tests rebuild `serened` regardless. Full design +
  the **validated CMake dependency graph** (corrects the remembered one) live
  in [`docs/issues/test-classification.md`](issues/test-classification.md).
  *The #1 lever for fast PRs and for the "extensions only if submodule changed"
  rule.* -- payoff high / effort low
- **P0.2 Move sqlite-derived sqllogic out of the PR tier** -> nightly only.
  396/756 files (§2.1), pure DuckDB SQL semantics. This is the biggest single
  time cut and the lowest risk (DuckDB already tests this; nightly still guards
  regressions in our PG-compat layer). payoff high / effort low
- **P0.3 Tier the triggered run by classifier output** (default checkbox = the
  fast tier; existing extra checkboxes -- ASAN/TSAN/MSAN/all-arch -- stay for
  opt-in heavy runs):
  - Fast tier: **all gtest unit suites** + **our** sqllogic (storage/connector/
    pg subtrees, sqlite excluded per P0.2) + **recovery smoke** + **driver
    smoke** (one psycopg path).
  - **Run our own (now-smaller) sqllogic + recovery under a sanitizer every
    PR** -- your example. ASAN by default (cheaper) and/or TSAN for the
    connector/server concurrency paths; this is *more* valuable per second than
    re-running DuckDB semantics uninstrumented. Budget it against P0.0's exact
    numbers -- sanitizers slow execution ~2-10x, so this only fits *because*
    P0.2 removed the sqlite bulk; if it's too slow, run sanitized-ours on the
    paths the classifier flags and keep a no-sanitizer run otherwise.
  - Conditional add-ons: extension tests **only if** `extension_submodule`
    changed; duckdb `unittest` subset only if `duckdb_submodule` changed.
  - Skip the build for `docs_only`; build-only for `cmake_only` where apt.
  - payoff high / effort medium
- **P0.4 Define the smoke set** -- a tag/marker convention (e.g. a `# smoke`
  directive or a manifest file) selecting the recovery/driver cases for the
  fast tier (our sqllogic mostly runs in full once sqlite is gone). Curation
  task, partly ongoing. payoff high / effort medium
- **P0.4b Push the rest of the long tail to nightly**: full sqllogic incl.
  `.test_slow` and the sqlite subtree, full recovery, full sqlsmith,
  searchbench, coverage (sanitizers already nightly). payoff high / effort low
- **P0.5b Drop searchbench from the default PR set** -- it's default-on today
  but in its current form is a search-path load exercise (postings/scoring/
  WAND/MaxScore), not a per-PR signal. Make it opt-in/nightly, and fold its
  workload into the real tracked `search-query` perf group under P1.2.
  payoff medium / effort low
- **P0.5** Keep the trigger as a required check; add fail-fast within the
  matrix; concurrency cancel already exists. payoff medium / effort low

### P1 -- Visibility: coverage + perf in the PR  *("see each commit's effect")*

- **P1.1 Coverage diff comment** -- build on the llvm-cov profiles already
  produced. Async, **non-blocking** PR job on its own machine: build
  instrumented, run unit + smoke, `llvm-cov export` + `diff-cover` against the
  merge-base, post "lines added by this PR not covered." Do not gate merges
  (DuckDB keeps it informational -- right call). payoff high / effort medium
- **P1.2 Perf regression -- DuckDB-style first, manual-mark only** *(decided)*:
  perf runs **only when a human marks the PR** (e.g. a `run-perf` checkbox/label
  in the bot comment). The human decides it's worth it -- connector/storage
  rewrite -> run; most PRs -> skip. No automatic per-path or per-commit triggering.
  When marked: on the dedicated cloud perf instance, build merge-base + PR in
  `bench` preset, run **microbench (`tests/bench/micro` -- already exists, just
  wire it up) + searchbench + a small set of representative end-to-end queries
  that hit iresearch**, 5 runs, geomean, 10% + absolute thresholds, post a
  step-summary/PR comment. payoff high / effort medium (script is largely
  portable from DuckDB)
  - *searchbench becomes the `search-query` group here:* the existing
    postings-iteration / scoring / WAND/MaxScore-pruning workload is exactly
    what we want measured properly (latency + pruning effectiveness per commit),
    rather than run as an untracked default PR gate.
- **P1.3 Perf history -- dogfood SereneDB** *(decided)*: store every run's
  metrics keyed by commit **in SereneDB itself**. Schema like
  `(commit, parent, ts, arch, bench_group, bench, query, metric, value,
  threshold)`. Build a Grafana / simple web dashboard. Eating our own dog food
  doubles as a stability test of the DB; accept early instability and keep a
  CSV fallback so a DB outage never blocks CI. payoff high / effort medium-high
  - *Note:* because perf is manual-mark only (P1.2), history is **sparse** --
    only marked PRs and their merge-bases land in the store, so the dashboard is
    a per-change comparison tool, not a dense per-commit timeline. If a
    continuous timeline is wanted later, add an opt-in periodic baseline run on
    `main` (open question in §7).
- **P1.4 Flamegraph on regression** -- when P1.2 flags a query, `perf record`
  the repro and attach a flamegraph SVG to the comment. We already keep a
  `perf` preset and a sample `perf.svg`. payoff medium / effort medium
- **P1.5 Provision + pin the cloud perf instance** -- fixed instance type
  (prefer bare-metal/dedicated over burstable), dedicated disk, governor
  `performance`, fixed frequency / controlled turbo, isolated CPUs, single job
  at a time. Stop-when-idle to save credits. Without this stability the numbers
  are noise. payoff high (prerequisite for P1.2-P1.4) / effort low-medium

### P2 -- Fuzz the novel surface  *(PG protocol + storage)*

- **P2.1 PG wire-protocol libFuzzer** -- harness feeding mutated bytes into the
  message decoder; seed corpus from captured psql/driver sessions; run under
  ASAN/UBSAN; nightly + persistent corpus. *Highest-value new fuzzer -- this
  surface is entirely ours, untested by DuckDB.* payoff high / effort medium
- **P2.2 Storage corruption libFuzzer** -- mutate serialized iresearch segments
  / WAL, feed to the reader under ASAN; assert graceful error, no crash/UB.
  payoff high / effort medium
- **P2.3 Randomized fault-injection fuzzing** -- extend `SET sdb_faults` to fuzz
  the *crash schedule* (lightweight ALICE/Jepsen-style), not just hand-picked
  points. High value for durability. payoff high / effort medium
- **P2.4 Enrich sqlsmith seed** with FTS/vector/index schema so generated SQL
  stresses the connector, not just DuckDB SQL. payoff medium / effort low
- **P2.5 Cross-version storage compat** (DuckDB's committed-old-datadir
  pattern) -- **defer until the on-disk format is declared stable**; we are
  early-stage and a clean break is still acceptable. payoff high (later) /
  effort medium
- **P2.6 Fuzzer-bug -> committed regression test** -- every find becomes a
  `.test` (DuckDB pattern), feeding the smoke/nightly suites. payoff high /
  effort low

### P3 -- Scale & robustness

- **P3.1 Shard nightly full sqllogic + recovery** by hash across the 5 amd64 +
  arm64 (CH's `--run-by-hash`). Keeps nightly bounded as the suite grows.
  payoff medium / effort medium
- **P3.2 Flaky detection** -- retry wrapper (DuckDB `retry.py`) + record
  "passed-on-retry" -> weekly report + quarantine list. No CIDB needed at our
  scale. payoff medium / effort low
- **P3.3 ARM64 nightly** -- second-arch correctness + parallel suites; gate on
  PR only when arch-sensitive code (SIMD/endianness/atomics) changes.
  payoff medium / effort low
- **P3.4 Fill connector/protocol unit-test gaps** -- many
  `tests/server/connector` tests are commented out; this is our highest-risk
  integration layer. payoff high / effort medium (ongoing)
- **P3.5 Config-matrix storage testing** (DuckDB-style JSON configs: small
  block size, WAL verification, forced checkpoints) reusing one runner.
  payoff medium / effort low-medium

### P4 -- Advanced automation  *(staged: issue -> repro -> bisect -> fix)*  *(decided staging)*

Build strictly in this order; each stage depends on the previous and on P2.6.

- **P4.1 Auto-issue from nightly failure/crash** -- parse sanitizer/crash logs,
  **dedupe by stack-hash** (essential to avoid spam), open a labeled GitHub
  issue with logs. *Start here.* payoff high / effort medium
- **P4.2 Automatic repro** -- attach a minimized repro to the issue (saved as a
  candidate `.test`, or the fuzzer input + command line). payoff high /
  effort medium
- **P4.3 Auto-bisect** -- bisect to the culprit commit and report it on the
  issue (correctness and perf regressions). payoff high / effort medium
- **P4.4 Auto-fix attempts** -- for clear-cut classes, a Claude-driven fix PR
  with guardrails (must pass the repro + full fast lane, human review
  required). Most ambitious; gated on 4.1-4.3 working well. payoff high /
  effort high
- **P4.5 Jepsen-style consistency / upgrade tests** -- once releases and storage
  format stabilize. payoff high (later) / effort high

---

## 5. Machine allocation summary

- **Dedicated cloud instance (AWS/GCS, from credits)** -- exclusive **perf
  runner** (fixed type, pinned, isolated, dedicated disk, stop-when-idle).
  Used by P1.2-P1.4, **only on manual mark**. Frees all 5 local machines for
  correctness work.
- **5x amd64 local** -- manual-triggered PR runs (fast tier + opt-in heavy) +
  nightly sanitizer/sqllogic shards + coverage job.
- **1x arm64** -- nightly parallel suites (sharded sqllogic, gtest-parallel) +
  second-arch correctness; PR only on arch-sensitive changes.

---

## 6. Suggested first slice (for when priorities are set)

P0.1 (classifier) + P0.2 (auto fast lane) unblock everything and deliver the
"fast PR" goal immediately. Then P1.2 (wire the *already-existing* microbench
into base-vs-PR -- likely the cheapest high-impact win) and P2.1 (protocol
fuzzer -- highest-value novel-surface fuzzer). P4 starts at P4.1 (issue only)
and grows toward auto-fix.

---

## 7. Open questions for review

- Smoke-set definition (P0.3): manifest file vs in-test marker directive?
- **Benchmark groups (P1.2/P1.3) -- needs more thinking.** Do we want named
  perf groups the human can pick when marking a run (so a storage rewrite runs
  only the storage group, etc.), or always run the full set on any mark?
  Tentative groupings to react to: `storage-write` (ingest/checkpoint/WAL),
  `search-query` (FTS / filters / scoring), `connector-throughput`
  (DuckDB<->iresearch vector/column paths), `e2e` (TPC-H-ish end-to-end),
  `micro` (the existing `tests/bench/micro`). Open: are these the right axes,
  and is per-group marking worth the complexity vs. just run-all-on-mark?
- Continuous main timeline (P1.3): perf is manual-only, so history is sparse.
  Do we ever want an opt-in periodic baseline run on `main` to anchor a dense
  timeline, or is per-change comparison enough?
- Perf E2E query set (P1.2): which representative iresearch/connector/pg
  queries are the canonical regression watchlist (and how do they map to the
  groups above)?
- Coverage gate (P1.1): informational-only forever, or eventually a
  diff-coverage floor on novel-surface dirs?
- libFuzzer build integration (P2): standalone targets vs reusing the existing
  Docker image + a new CMake option (e.g. `-DSDB_FUZZERS=ON`)?
