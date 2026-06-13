# Error handling — structural cleanup + coherent model (separate work stream)

The error handling is **genuinely badly written throughout** — not "almost-right pieces missing a model." Two problems
compound: (1) the *code* abuses exceptions structurally (oversized `try`/`catch` scopes that are really on-failure
cleanup, catch-and-ignore, terminal handling buried inside loops, hand-recomputing what an API already returns), and
(2) there is no *model* — `Result`, `THROW_SQL_ERROR`/`SqlException`, and DuckDB `ErrorData` are mixed ad-hoc (391
`THROW_SQL_ERROR`, 147 `throw`, **73 `catch`** / 59 `try`, 50 `ErrorData`). This document fixes both — as its own
stream, because it is cross-cutting and must be decided once and applied uniformly. **Perf-neutral by construction:**
the ok path takes no throw; the single per-request catch sits off the hot path.

## Bad patterns to eliminate (the actual mess)
Seen all over the request path; the `SendWriter` write-failure block is the canonical example.
- **`try`/`catch` used as on-failure cleanup.** A `try` wraps one operation; its `catch` runs a big terminal block
  (poison + wake-all + close + return) and *ignores the exception value*. That is not exception handling. Express it as
  control flow (op fails → `break` → run the cleanup once) or, when cleanup must run on every exit, a scope guard
  (`irs::Finally`; `absl::Cleanup` only when it must be cancelable). Never a bare `catch (const std::exception&)` that
  drops the value.
- **Terminal handling inside the loop.** The failure cleanup + `co_return` sits in the `for(;;)` body; it belongs once,
  *after* the loop — the loop only `continue`s on success.
- **Recomputing what the API returns.** e.g. summing `view` buffer sizes for the byte count `_socket.Write` already
  returns (`Async<std::size_t>`). Use the return value.
- **Scattered intermediate `try`/`catch`** in the request path instead of one boundary, plus the
  throw-then-rethrow-to-preserve-type dance at the DuckDB edge.
These are structural quality bugs; the rules below fix them — naming a model is necessary but not sufficient.

## The two domains (the rule)
Pick the mechanism by **who is waiting**, never mix within a function:

1. **Storage / catalog / background tasks** — no client awaiting a synchronous reply (WAL recovery, drop/compaction/
   refresh tasks, rocksdb, settings). Use **`Result`** (`libs/basics/result.h`): move-only, `[[nodiscard]]`,
   ok-or-`{ErrorCode,message}`. Errors are returned and checked; background loops log + retry/backoff. This is correct
   today — keep it. Do **not** throw across a background-task boundary.
2. **Request handling** — a client awaits a response (pg-wire command loop, http/es handlers, the SQL functions and
   query execution they drive). DuckDB itself throws here. Use **exceptions**: our errors via `THROW_SQL_ERROR`
   (`SqlException` carrying `SqlErrorData{errcode,errmsg,detail,hint}`); DuckDB raises `duckdb::Exception`. The request
   has exactly **one** catch boundary that turns any escape into a wire error.

## Rules that make the request path clean
1. **One raise per domain** — `THROW_SQL_ERROR` in the request domain, `Result` in the storage/background domain. A
   function does one or the other, never both. (e.g. `es.cpp` binds that throw raw `duckdb::BinderException` must use
   `THROW_SQL_ERROR`, matching `BindWriteTarget` — see TODO.md.)
2. **One catch boundary per request unit** — the pg-wire command loop (per message) and the http handler dispatch (per
   request). It is the *only* `try`/`catch` in the request path. Delete every intermediate one and let exceptions
   propagate to it:
   - `SetupConnection`'s GUC `try/catch` → a non-throwing `SetSettingChecked` status (or let it propagate to the
     command-loop boundary).
   - COPY's `exception_ptr`/rethrow stash → `CopyInScope` RAII join lets the `SqlException` propagate (plan phase 7).
   - auth's `fail()` lambdas → `PgAuthenticator` returns `{Ok, Failed(SqlErrorData), Closed}` (plan phase 5).
3. **One DuckDB boundary** — DuckDB captures errors into a result's `ErrorData` (it does *not* propagate them). Surface
   them with the single `ThrowIfError(result)` primitive (rethrows the preserved `exception_ptr`, so a serenedb
   `SqlException` survives typed; a bare `duckdb::Exception` is caught at the boundary). The `DriveToResult`/`DriveWire`
   primitives (plan phase 6) call `ThrowIfError` internally, so call sites never repeat `HasError()` checks (kills the
   remaining `GetErrorObject().Throw()` sites).
4. **One funnel at the boundary** — `wire_frames::ToSqlError(const std::exception&)` (done, phase 1): `SqlException`→its
   data, `duckdb::Exception`→`DuckErrorToSqlData`, else `XX000`. The es/http handlers must route through the *same*
   funnel; the ES sqlstate→ES-error JSON map wraps `ToSqlError`'s output rather than re-deriving it.
5. **Assertions are for programmer errors only** — `SDB_ENSURE`/`SDB_ASSERT` (673 uses) guard invariants that user input
   can never trigger; user-facing failures are always `THROW_SQL_ERROR`. Keep them; never conflate the two.
6. **Background catches are a single pattern** — the legitimate "never let a background task throw past its boundary"
   catches (rocksdb_background_thread ×7, sync_thread ×4, inverted_index_shard ×9, search/task, settings_manager) all
   become one shape: catch at the task boundary → log on the right topic → record a `Result`/continue. Standardize the
   pattern; don't hand-roll each.

## Why this is perf-neutral
The ok path never enters a `try` and never throws (zero cost on modern ABIs). The one catch per request is taken only on
error. `Result` is a value return (no exception machinery). `ThrowIfError` is a single predicate + (on error only) a
rethrow. So consolidating the boundary removes code, not speed.

## Work plan (apply across the whole branch, not just pg-wire)
This is its own stream; the structural-refactor phases reference it rather than owning it.
- **E1 (done, phase 1):** `ThrowDuck`→`ThrowIfError` template; the 3-arm command-loop catch → one `ToSqlError` arm.
- **E2 pg-wire request path:** delete the intermediate request-path try/catch (SetupConnection GUC, COPY exception_ptr,
  auth fail-lambdas) — folded into plan phases 5/6/7; gate on drivers (1455).
- **E3 http/es:** one catch at handler dispatch; reconcile the ES-error JSON map to wrap `ToSqlError`; convert
  connector `es.cpp` raw `BinderException` → `THROW_SQL_ERROR`. Gate on es/http (49).
- **E4 functions/connector:** audit `server/connector/functions/**` for raw duckdb throws on user-facing paths →
  `THROW_SQL_ERROR`; ensure none catch-and-swallow.
- **E5 storage/background:** keep `Result`; standardize the background-task catch pattern (one helper) across
  rocksdb_*/search/*; no behaviour change.
Each step is build + suite-gated and behaviour-preserving; the win is a single, legible error path everywhere.
