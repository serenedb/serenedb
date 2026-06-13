# TODO — `mbkkt/new-server` pre-merge review

Review of the whole branch (serenedb `main..HEAD`, 80 new/changed source files) plus
the duckdb fork commits this branch adds (`539c9b30f6b..0e60fa7b129`, parser arena +
moodycamel scheduler + optimizer rules). Findings from a 12-way parallel review; the
"fix before merging" bugs were adversarially re-verified against the code (6/7 confirmed
real, 1 refuted and demoted).

Each section has three buckets:
- **Fix before merging** — sure it's a real defect / safe in-scope cleanup; should block merge.
- **Land separately** — real and sound, but larger/riskier/out-of-scope → validate with me + open an issue.
- **Unsure** — plausible but unconfirmed (may be intentional, needs runtime/measurement).

---

## Bugs

### Fix before merging this branch

- ✅ **FIXED (this pass)** — **`server/network/http/response_writer.h`** bodiless-status framing (204/304/1xx wrote an unframed body → keep-alive desync). `EncodeHead` now returns `bodiless`; `WriteHead`/`Fixed` zero the body for those statuses. Repro: `/_test/status?code=204` pipelined with a ping was `…204\r\n\r\n{"code":204}HTTP/1.1 200…`, now clean. HEAD + transport gtests pass.
- ✅ **FIXED (this pass)** — **`server/network/pg/pg_wire_session.h`** double `SetupConnection` (first on the io thread). Dropped the `Run()` call; `SessionMain` does it once on the worker (db-not-found still errors correctly). Repro: thread-id probe showed 2 calls on 2 tids → now 1, on the worker. 1455 driver tests pass.
- ✅ **FIXED (this pass)** — **`server/connector/duckdb_pg_binary_copy.cpp:351`** `SetCardinality`→`SetChildCardinality`. Repro: binary COPY + `WHERE s LIKE 'x%'` → `Mismatch in input vector sizes … left has 0 rows but right has 3`; now `COPY 2` (filter applied correctly).

### Land separately (validate + open issue)

- **`server/network/http/es/handlers.cpp:158-160`** — ES `_bulk` hardcodes `"errors":false`; `es_bulk` THROW_SQL_ERRORs on the first bad document, so one bad line **fails the whole request** instead of ES-style partial success (good items indexed, bad items reported per-item). Compatibility deviation clients/esrally may rely on. Decide if partial-success bulk is in scope.
- **`server/network/acceptor.h:88`** — accept loop busy-spins (100% io CPU) on recurring EMFILE/ENFILE. *Re-scoped to non-blocking (your call): the loop already yields to the io_context on each `co_await AcceptInto`, so live connections are NOT starved — the only harm is CPU burn under real fd exhaustion (an overload state).* No clean cheap fix: a fixed timer backoff is an arbitrary delay; libuv's reserved-fd drain is "not reliable in a multi-threaded environment" (its own comment) and we run several io threads; userver just `Yield()`s and keeps burning CPU too. The proper fix is event-driven pause-on-EMFILE + resume-on-connection-close (folly `pauseAccepting`), needing Session→Acceptor wiring. Track an issue.
- ✅ **FIXED (this pass)** — **`third_party/duckdb` `string_util` `TryParseFormattedBytes`/`ParseFormattedBytes`** (the unbounded `arg[idx]` OOB). Reverted `std::string_view` → `const string &` (the pre-WIP/upstream signature): `std::string::operator[](size())` is `'\0'`, so the scan loops are safe again — no bounds checks, no caller changes (all already pass `std::string`), no string_view→string churn. serened builds green. *(duckdb-fork edit → needs a fork commit + gitlink bump.)*

### Unsure — needs deeper validation

- **`server/network/http/response_writer.h:149-167`** — Chunk size is fixed **8 hex digits**; a single `Write()` payload > 4 GiB silently truncates the chunk-size header → body corruption, no assert. Latent (handlers chunk in bounded pieces). Add an assert or split oversized writes.
- **`server/network/http/es/dsl.cpp:366-369`** — `bool.must_not` over a text-field match emits `NOT(@@)`; if the inverted-index planner claims `@@` all-or-nothing (like the guarded `should` case), the negation may hit the runtime stub and throw. Verify `{"bool":{"must_not":[{"match":…}]}}` actually plans/executes.
- **`server/network/http/es/handlers.cpp:858-880`** — Match-query **scroll** ANDs `@@` with an `_id` keyset and orders by `_id`; same planner-claim concern. Confirm paged match scroll is correct, else restrict scroll to filter-only.
- **`server/network/http/es/handlers.cpp:771-778`** — `max_score` for `from>0` reports the **current page's** top score, not the global max (ES reports global on every page). Minor fidelity; decide or document.
- **`server/network/network_server_feature.cpp:275-284`** — Shutdown calls `io_context::stop()` while the accept coroutine is suspended → posted acceptor-stop lambda + in-flight `async_accept` never run; the coroutine frame (holding `self`) leaks. Benign at process exit; matters for graceful drain / start-stop in tests. Drain the acceptor's `operation_aborted` before stopping the contexts.
- **`server/scheduler/background_scheduler.cpp:65`** — `Delay()` reads `NetworkServerFeature::_pool` **unsynchronized** and the io pool is torn down before the background pool (LIFO); in-flight detached drop tasks can race `_pool.reset()` (data race) and arm a timer on an already-stopped `io_context` whose callback never fires → promise destroyed unset (possible `Wait()` hang). Either reorder shutdown (background stops before network) or publish the pool atomically + guarantee the timer promise is always set.
- **`server/connector/functions/es.cpp:216,822,904`** — es_* binds throw `duckdb::BinderException` (not `THROW_SQL_ERROR`) for NULL args. *(Verify pass refuted the merge-blocker rationale — DuckDB bind errors may already surface with a proper sqlstate. Confirm the user-visible error/sqlstate before deciding; align with `BindWriteTarget` if it's wrong.)*
- **`third_party/duckdb/src/parser/peg/matcher.cpp:488-504`** — Single-quoted string-literal table/file names are **no longer case-folded** (the `string result_text` branch skips `Lower`), a behavior change bundled silently into arena/perf commits. Could be a latent-bug fix or an accidental regression. Confirm intent; pin with a sqllogic test either way.

---

## Perf

### Fix before merging this branch

- **`server/network/http/es/common.cpp:98-110`** — `SqlLiteral` escapes the **entire bulk ndjson body char-by-char** with `push_back` (called from `BulkHandler`, the highest-throughput ES endpoint), and `reserve(size+2)` leaves no room for doubled quotes so apostrophe-bearing payloads realloc mid-loop.
  **Fix:** scan with `memchr`/`find` for the next quote, `append` the run in one shot, then the doubled quote.

### Land separately (validate + open issue)

- **`server/network/http/es/handlers.cpp:819-831`** — Each aggregation runs its **own** SQL query over the same relation+filter (K aggs = K scans, K re-evaluations of `@@`), vs ES's single multi-aggregate pass. Combine flat aggs sharing relation+filter into one grouped statement, or at least run concurrently.
- **`server/connector/duckdb_pg_binary_copy.cpp:318-349`** — `COPY FROM binary` builds a `duckdb::Value` per field then `SetValue`, vs the es write path's direct `FlatVector` writes. Add a per-type deserialize-into-flat-vector path.
- **`third_party/concurrentqueue/concurrentqueue.h` (commit `1375d3112d8`)** — moodycamel bump defaults `RECYCLE_ALLOCATED_BLOCKS=false` → scheduler queue blocks `free()`'d to heap instead of recycled; the per-message slice pattern can churn malloc/free on enqueue. Upstream default, not a regression. Micro-bench the scheduler path; if it shows, set the trait true via custom Traits.

### Unsure — needs deeper validation

- **`server/network/http/es/dsl.cpp:671-690`** — Search query JSON is **parsed twice** (raw_json capture into `query_raw`, then a second simdjson parse to translate). `query_raw` is only needed for scroll; non-scroll `_search` pays an extra parse + padded_string copy. Translate from the live value, materialize `query_raw` lazily only when scrolling. Measure first.
- **`server/catalog/drop_task.cpp:87`** — `ERROR_LOCKED` exponential backoff is **defeated when the io pool is down** (Delay returns immediately) — exactly during WAL recovery (drops scheduled before `network.start()`) and shutdown → tight retry loop hammering a background thread. Make the backoff robust when Delay can't arm a timer (self-reenqueue after sleep). Confirm `ERROR_LOCKED` actually persists in recovery first.
- **`third_party/concurrentqueue/lightweightsemaphore.h` (commit `1d12d66993d`)** — scheduler semaphore spin count cut **10000 → 100** globally (affects all queries' idle/wake latency, not just pg-wire). Intentional per the wire-perf work; confirm 100 is the A/B'd value and drop a one-line comment at `task_scheduler.cpp:48` recording why (the value now lives implicitly in a third-party header).

---

## Simplify

### Fix before merging this branch

- **`libs/app/app_server.cpp:48`** — Dead `kRules` entry `{"server/rest_server/endpoint_feature.cpp", "server"}` for a file this branch deleted (the sibling scheduler/general_server/ssl rules were already removed; this one was missed). Remove the line.
- **`server/catalog/server_state.{h,cpp}`** — Dead weight (confirmed: no `ServerState`/`GetMode`/`ReadOnly`/… symbol used anywhere; read-only & maintenance enforcement is genuinely gone from the new path, not just unwired). Delete both files, drop from `catalog/CMakeLists.txt`, and remove the 6 stale `#include "catalog/server_state.h"` (object.cpp, role.cpp, local_catalog.cpp, pg/system_catalog.cpp, rocksdb_engine_catalog/options.cpp). **[doing now]**

### Land separately (validate + open issue)

- **`server/network/pg/auth.cpp:31-33`** — Header comment says "SASLprep is not applied" but the SCRAM/cleartext paths **do** call `SaslPrep()` (an ASCII passthrough seam). Reword to match `auth.h:72` so readers know the seam exists.
- **`third_party/duckdb/third_party/fast_float/` (bundled copy)** — Orphaned float-only fast_float (dead, not in any include path) whose `from_chars` 4th arg is `bool strict`. The branch's new `parse_result.hpp` relies on the serenedb fast_float's base-argument integer overload; if the dead copy ever re-entered the include path, `from_chars(...,8)` would silently parse base-10 floats and corrupt every octal/hex escape. Delete the dead copy.

> Note: read-only / maintenance-mode request rejection is genuinely **absent** from the new pg-wire/HTTP path (it lived in the old general_server). Deleting `ServerState` removes the dead carcass; re-implementing the *feature* on the new path is a separate decision/issue, not part of this cleanup.

---

## Larger work items (beyond the line-level review)

These are bodies of work, not single-line fixes — each gets its own plan + verification.

### COPY: make it mature + verified across every independent case
COPY currently passes some tests but is not mature. It must work and be **verified independently** across the full matrix:
- **direction:** `COPY FROM` × `COPY TO`
- **format:** `binary` (PG binary) × `csv`/`text` × `parquet` (csv/text/parquet likely already fast/correct via DuckDB — confirm, don't assume)
- **transport:** pg-wire `CopyData`/`CopyDone` stdin × DuckDB stdin (`serened shell`, `/dev/stdin`) × real files

**Critical perf invariant:** the **binary** COPY path must be **at least as fast as — ideally faster than — the pg-wire text path**, because DuckDB's `postgres_scanner` issues `COPY ... (FORMAT binary)` to execute its scans, so binary-COPY throughput is on the federation hot path. Deliverables: a verification matrix (sqllogic/driver), a binary-vs-text COPY throughput bench, and hardening of the parse/deserialize path (see the just-fixed `SetChildCardinality` and the per-field `duckdb::Value` perf item — COPY-binary should write flat vectors like the es write path).

### Auth: complete the methods for both pg-wire and ES
Enumerate and implement the full intended auth set; today it's a flag-configured single user.
- **pg-wire:** have SCRAM-SHA-256 + cleartext + TLS client-cert. Missing / to confirm: real user store + RBAC seam (not one flag user), legacy `md5`, `trust`, SCRAM channel binding (`SCRAM-SHA-256-PLUS`), per-user/per-host method selection (hba-like).
- **ES / HTTP:** have Basic + ApiKey + Bearer (flag-configured). Missing: real key/token stores, per-realm config, and parity with the pg-wire user store.
First step: write down the exact target set (what "all needed" means) and reconcile both protocols against one credential store.

### Rewrite `server/network/pg/pg_wire_session.h` — clean structure, no perf regression
2309-line header grown iteratively → spaghetti: random `try`/`catch`/`throw`, `ThrowDuck`, comment noise, no clear structure (the now-fixed 2× `SetupConnection` is the canonical symptom). Rewrite critically.
- **Constraints:** same-or-better perf; do **not** fragment the single session coroutine into many coros (that was the wrong call); remove `ThrowDuck` and similar; keep the perf-critical seams intact (io↔duck handoff, lazy-kickoff scheduler signal, zero-copy frames, send backpressure/high-water).
- **Method:** read → extract the protocol state machine + the invariants/caveats that MUST be preserved → write clean code from that understanding (not a line-by-line port). Small verified steps, each re-run against the 1455 driver tests.
- The same iterative-cruft problem exists in other new code; tackle pg_wire first, then apply the pattern incrementally elsewhere. **Plan: `docs/pgwire-rewrite-plan.md`** (8-way module study → phased, perf-gated steps; awaiting sign-off on phasing + the shared-transport decision).
