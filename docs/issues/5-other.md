# Issue 5 -- Other (scale, automation, test-runner strategy, pg-protocol)

Catch-all for everything not in issues 1-4. Roughly priority-ordered groups;
split out as any one becomes active.

---

## 5A. SQL test-runner strategy (decided direction)

Verified fact: **DuckDB's `unittest` is strictly in-process** (links libduckdb,
no wire/tcp/libpq mode). `sqllogictest-rs` is **over-the-wire, multi-engine**
(mysql / pg-simple / pg-extended). They cover different axes -- don't replace one
with the other. **Fork neither** (both are externally maintained -- the win).

- **Run `unittest` broadly** for what only it can do: DuckDB's own `test/` tree +
  **all** vendored extension suites (today it's wired to postgres_scanner only).
  In-process, doesn't need serened. Gate to **nightly** + the `duckdb`/
  `extension` change categories. Regression guard on the *patched* DuckDB.
- **Keep `sqllogictest-rs`** for over-the-wire + its superpower: run `any/*`
  against **both serened (simple+extended) and real PostgreSQL**, treat
  divergence as a bug (cheap differential).
- **Do NOT** port unittest's directives into sqllogictest-rs (= becoming a
  framework maintainer); those directives test SQL *semantics*, which is
  DuckDB's job, not ours.

## 5B. Test the PG protocol better (separate from runner choice)

Neither runner is a protocol tester -- both assert *rows*, not RowDescription
type OIDs, SQLSTATE, format codes, portals, COPY framing, pipelining, cancel.
Purpose-built coverage, in their natural homes:
- **Protocol-level differential** vs real PG (extend comparison beyond rows to
  type OIDs / SQLSTATE / error fields).
- **Extended-protocol conformance suite** via the existing **11-language driver
  harness**: Parse/Bind/Describe/Execute(max_rows)/portals, prepared-stmt reuse
  + cache invalidation, COPY in/out, pipelining, cancellation, mid-txn error
  recovery, multi-statement simple query.
- **Protocol fuzzing** -- see issue 4.1 (incl. the stateful sequence fuzzer).

## 5C. Scale & robustness (plan P3)
- **Shard nightly full sqllogic + recovery** across the 5 amd64 + arm64.
- *Deferred (not our scale yet):* **build-once, distribute the binary** -- build
  on one machine, publish as an artifact, fan test execution across machines
  that pull it instead of rebuilding. The right move when test count outgrows a
  single machine's parallelism (longest-first scheduling, issue 1F, is enough
  for now). Keep the test runners able to consume a prebuilt binary so this
  isn't blocked later.
- **Flaky detection** -- retry wrapper + record passed-on-retry -> weekly report +
  quarantine list (no CIDB needed at our scale).
- **ARM64 nightly** -- second-arch correctness + parallel suites; PR only on
  arch-sensitive changes (SIMD/endianness/atomics).
- **Fill connector/protocol unit-test gaps** -- many `tests/server/connector`
  tests are commented out; highest-risk integration layer.
- **Config-matrix storage testing** (DuckDB-style JSON configs: small block
  size, WAL verification, forced checkpoints) reusing one runner.

## 5D. Advanced automation (plan P4 -- staged: issue -> repro -> bisect -> fix)
Build strictly in order; each depends on the previous and on issue-4's crash
substrate.
1. **Auto-issue** from nightly failure/crash -- parse logs, **dedupe by
   stack-hash**, open labeled issue. *Start here.*
2. **Auto-repro** -- attach a minimized repro (`.test` / fuzzer input + cmdline).
3. **Auto-bisect** -- find the culprit commit, report on the issue (correctness
   + perf).
4. **Auto-fix** -- Claude-driven fix PR for clear-cut classes, guardrails (must
   pass repro + fast tier, human review required). Most ambitious; gated on 1-3.

## 5E. Later / once stable
- Storage cross-version compat (DuckDB committed-old-datadir pattern) -- **defer
  until on-disk format is declared stable** (early-stage, clean break still ok).
- Jepsen-style consistency / upgrade tests.

---

## Open questions
- 5A: is the near-term itch (a) regression-guard duckdb [wire unittest broadly,
  small], (b) better pg-protocol [5B + 4.1, runner-agnostic], or (c) both?
- 5C: do we want sqllogic sharding before or after the issue-1 sqlite cut (the
  cut may make PR sharding unnecessary; nightly still wants it)?
