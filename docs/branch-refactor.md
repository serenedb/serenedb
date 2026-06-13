# Branch refactor — whole-diff simplification (umbrella)

**Goal:** across the *entire* `mbkkt/new-server` diff, simplify and raise code quality **with no perf regression**. The
new code grew iteratively (canonical symptoms: pg-wire's 2× `SetupConnection`, two coexisting error models, leaky
`WireSinkContext`, an over-split `pg_frame_codec`, a dead `CopyMessagesQueue` layer). The mandate is the whole branch,
not just pg-wire, and **best code over low-churn**.

## Principles (apply to every subsystem)
- **Read → understand the algorithm + the load-bearing invariants → rewrite clean** — not a line-by-line port.
- **No perf regression.** Preserve the measured hot paths and seams verbatim; any hot-path/structural change takes a
  `build_perf` before/after. The user runs the full perf + benchmark suite at the end.
- **Simplify:** delete dead code, duplication, over-engineering, and leaky boundaries; one concept = one implementation
  (e.g. backpressure must live in exactly one place).
- **#796-aligned** naming/placement (greppable globally-unique types, no `duckdb` in first-party identifiers, file =
  class minus dir context) applied with judgment; namespaces are flattened in the coordinated tree-wide pass, not
  piecemeal.
- Each subsystem stays green and suite-gated at every step; nothing big-bang.

## Streams
1. **Error handling (cross-cutting)** — `docs/error-handling.md`. One error model (Result for storage/background;
   exceptions with a single per-request catch boundary for the request path) applied to *every* subsystem. Lands first
   where it shapes a subsystem's request path. Phase-1's pg-wire error work is step E1 there.
2. **pg-wire structural rewrite** — `docs/pgwire-rewrite-plan.md`. Subsystem 1, in progress (phase 1 done; the shared
   `Transport` base done + green). This is the *template* for the rest; its error-path work defers to stream 1.
3. **Whole-branch simplification (the rest)** — the same understand→plan→execute treatment per subsystem, informed by the
   pre-merge review (`TODO.md`) and a per-subsystem study like the pg-wire one.

## Subsystems to treat (each: understand-pass → plan → small verified steps, gated on its suite)
- **net/** — pg-wire (in progress); then http/es (handler duplication, `dsl.cpp` double-parse, agg-per-query, the
  shared `Transport` migration for http).
- **connector/** — functions / optimizer / storage extension / copy / client state (COPY's per-field `duckdb::Value`
  vs flat-vector writes; the es-function error model).
- **catalog/ + rocksdb_engine_catalog/** — the drop/background task model, `Result` usage, the dying rocksdb
  (`catalog/store/rocksdb`).
- **storage/ + search/** — the per-table/index runtime; "shard" → "storage" naming per #796.
- **pg/ non-network** — system catalogs, options parsing, command_tag.

## Sequencing
Error-model decisions (stream 1) land first since they shape every request path. pg-wire (stream 2) continues as the
template. Then per-subsystem simplification (stream 3): each gets a focused understand-pass (a workflow like the pg-wire
one), a plan with the same Design/Steps/Invariants/Gate/Gotchas specificity, then small verified commits. The pre-merge
review's `simplify`/`perf` findings feed directly into the relevant subsystem.
