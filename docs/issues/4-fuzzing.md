# Issue 4 -- Better fuzzing

**Scope:** fuzz SereneDB's novel surface -- **PG wire protocol** and **iresearch
storage** -- plus harden the existing SQL fuzzing. (Storage = iresearch only;
RocksDB/vpack are out, being dropped.)

---

## Current state
- `sqlsmith` (SQL-level) runs nightly (10k queries, seeded schema). SQL only.
- `vpack_fuzzer` exists -- **dropped** (vpack going away).
- No protocol fuzzing, no storage-format fuzzing, no randomized fault injection,
  no crash->issue automation.

## What CH/DuckDB do (model)
- ClickHouse: a **fleet** of libFuzzers (parsers, formats, compression,
  `tcp_protocol_fuzzer`), persistent S3 corpus with coverage-guided
  minimization, crash logs parsed -> issues.
- DuckDB: AFL / DuckFuzz / Pedro / SQLSmith; **every find becomes a committed
  `.test`** regression case.

## Targets, in priority order

### 4.1 PG wire-protocol libFuzzer  <- highest value (surface is 100% ours)
- libFuzzer target feeding **mutated bytes into the message decoder**.
- Seed corpus from captured psql / driver sessions (simple + extended).
- Run under ASAN/UBSAN; persistent corpus.
- Also a **stateful** variant: drive valid-but-adversarial *message sequences*
  (Parse/Bind/Describe/Execute/Sync, partial portals, mid-txn errors) -- this is
  where protocol bugs actually live (see issue 5, pg-protocol testing).

### 4.2 iresearch storage corruption libFuzzer
- Mutate serialized **iresearch segments / columnstore / WAL**, feed to the
  reader under ASAN; assert graceful error, no crash/UB.

### 4.3 Randomized fault-injection fuzzing
- Extend `SET sdb_faults` to fuzz the **crash schedule** (lightweight
  ALICE/Jepsen-style), not just hand-picked points. High value for durability;
  reuses the existing recovery harness.

### 4.4 Enrich sqlsmith seed
- Add FTS / vector / index schema to the seed so generated SQL stresses the
  **connector**, not just DuckDB SQL.

## Build integration (open question)
- Standalone libFuzzer targets vs reuse the build image + a new CMake option
  (e.g. `-DSDB_FUZZERS=ON`). Prefer the latter for consistency.

## Crash -> regression test (shared with issue 5 automation)
- Every find: dedupe by **stack-hash**, save a **minimized repro** as a committed
  `.test` (DuckDB pattern), feed it into smoke/nightly. This is the substrate
  for issue-5's auto-issue -> auto-repro -> auto-bisect -> auto-fix ladder.

## When to run
- Nightly continuous (own time budget per fuzzer); not on PR critical path.
- Optionally gate a storage/protocol fuzz smoke on the `iresearch` / pg-server
  change category.

## Done when
- Protocol + storage fuzzers run nightly with persistent corpora, and a crash
  yields a deduped issue + a committed minimized repro.
