# pg-wire session rewrite — plan

Goal: turn `server/network/pg/pg_wire_session.h` (2309-line god-header, grown iteratively) into clean, clearly-layered
code **with no perf regression**. Derived from an 8-way per-concern study of the whole pg-wire module (session + every
dep + the design doc), not a line-by-line port.

> **Scope note.** This is **subsystem 1** of the whole-branch simplification effort (`docs/branch-refactor.md`), used as
> its template. **Error handling is a separate cross-cutting stream** (`docs/error-handling.md`): this plan defers all
> error-path work there — phase 1's change is step E1, and the error aspects of phases 5/6/7 follow stream 1's rules
> (one per-request catch boundary, no intermediate try/catch, `ThrowIfError` at the one DuckDB boundary, `ToSqlError`
> funnel). The phases below own the *structural* rewrite only.

## Hard constraints (non-negotiable)
- **One duck session coroutine** (`SessionMain`/SessionTask hosted by `TaskRunner`) + the two io byte-pump coroutines
  (`Run`/RecvLoop and `SendWriter`) + the **transient** per-COPY feeder. Do **not** fragment into more coroutines.
- **No perf regression.** The four perf seams below are preserved byte-for-byte in behaviour; hot-path phases get a
  `build_perf` micro-bench before/after.
- Remove `ThrowDuck` and ad-hoc `try`/`catch`/`throw`; comments become WHY-only.

## Perf seams + correctness invariants to preserve (the gate for every step)
These came out of the study with file:line; any step that would change one of these is wrong.

**The 4 perf seams**
1. **Lazy scheduler kickoff** — the io side only `RequestRun()`s; `TaskRunner` parks itself (`kParked/kQueued/kRunning/
   kRunningWoke`) and the per-flush task wake is gated by *armed interest* (`_send_waiter`, Dekker/seq_cst fences in
   `ArmSendWaiter`↔`SendWriter` and `RequestRun`↔`Execute`). A dropped wake wedges the session — fences are sacred.
2. **Zero-copy borrowed frames** — `Frame.payload` is a view into `_recv`, consumed *after* the handler runs
   (`recv_consume`/`_dispatch_consume`); only chunk-spanning frames take the one-copy `_scratch` linearization.
3. **Zero-copy send splice + write-behind** — sinks encode into per-lstate buffers; sealed chains are `SpliceCommitted`
   into `_send` (O(1), never re-copied); `_send` auto-flushes at `kSendFlushSize` via `OnSendViewReady` so encoding
   overlaps the socket write; single-slot `_write_view/_write_armed` handoff.
4. **Send high-water backpressure** — `committed − written > kSendHighWater`, judged against the *role-appropriate*
   committed counter (plain producer vs direct-sink `direct_committed`); parallel mode adds queued/pinned caps.

**Key correctness contracts**
- Single `_recv` consumer at any instant; role hands off startup-read → task-park → copy-gate (`_copy_route`). Never
  concurrent.
- RowDescription committed to `_send` **before** `PendingQuery` arms a direct-mode sink; CommandComplete after the drain.
- Auth + `AuthenticationOk`/startup burst ordering: auth io-side **before** handoff; burst duck-side **after**
  `SetupConnection`.
- CVE-2021-23214: no buffered plaintext in `_recv` before the TLS handshake on SSLRequest upgrade.
- SCRAM/cleartext compares stay constant-time (`ConstantTimeEqual`, vendored OpenSSL).
- `TaskRunner` publishes parked-state from `Execute()` *after* the coroutine suspends, never from the awaiter.
- Teardown runs on the duck worker where objects were created; close is `post`ed to the io thread after `DrainSendOnTask`.
- `max_len` enforced before alloc; over-cap → `TooLarge`, not a silent drop.
- COPY: strict lock-step single-in-flight borrowed view (no queue); feeder always joined before the stack bridge dies.

## What stays as-is (proven seams — do not touch)
`TaskRunner`, `Gate`, `IoExecutor`, `CancelRegistry`/`CancelToken`, `auth.{h,cpp}` (crypto, shared with HTTP Basic),
`message::Buffer` (libs/basics, shared with HTTP), `wire_frames` as pure zero-copy encoders. The connector-side COPY
adapters (DuckDB FileSystem + binary `CopyFunction`) stay registered on the DuckDB instance; `CopyInBridge` stays the seam.

## Target shape (reconciled)
A slim `PgWireSession<Kind>` lifecycle shell + cohesive collaborators that are **plain objects the one coroutine calls**
(not new coroutines):
- **RecvFramer** — owns `_recv`, `_scratch`, `max_len`, consume bookkeeping; folds in `pg_frame_codec` (delete that file);
  one `await Frame(wait_strategy)` replacing `NextFrame/AwaitFrame/FeedFrame`; RAII consume handle replacing the ~13
  manual `Consume` sites + `_dispatch_consume`.
- **SendChannel** — owns `_send` + `SendWriter` + the single-slot write-behind + the `_send_waiter` Dekker handshake +
  `_io_broken`; exposes `Kick`/`Drain(predicate,waker)`/`Broken`; **one** high-water predicate (kills the 3–4
  duplications). One `PoisonAndWake()` replaces the two duplicated poison sequences.
- **PgAuthenticator** (non-template, `.cpp`) — SCRAM/cleartext driver over `(reader, writer)` callbacks; SCRAM/SASL
  message *parsing* moves into `auth.cpp` as pure `string_view`→struct functions (unit-testable, not per-Kind).
- **Query execution** — two shared drive primitives `DriveToResult` / `DriveWire` replace ~5 duplicated drive blocks
  (simple == extended at the execution layer); `WireSinkContext` split into an immutable per-query binding vs a
  collector-owned `WireQueue` (one backpressure authority; kill the `Reset()`/`use_count==1` reuse hack);
  `ResolveStatement`/`ResolvePortal` fold the anon special-case.
- **CopyInScope** (RAII) — owns one COPY-FROM-STDIN (bridge + feeder + route flip + join-on-scope-exit); deletes the
  manual `exception_ptr`/rethrow and the park-until-`_feeder_done` loop. Delete the dead old-server `CopyMessagesQueue`
  layer entirely. One `WriteCopyDataFrame` helper for text-out/binary header/row/trailer.
- **pg-error bridge** — one home for `DuckExceptionToErrcode`/`DuckErrorToSqlData` + new `ToSqlError(const
  std::exception&)` and `ThrowIfError(result)`. `ThrowDuck` deleted; the 3-arm command-loop catch collapses to one.

## Decision (chosen): Option B — shared transport up front
`http/session.h` (587 lines) **already duplicates** pg-wire's RecvLoop/SendWriter/`_write_gate`/`_send_written`/
`AwaitSendBelowHighWater` machinery. We extract a shared `network/transport.h` (templated on SocketKind) **early**, so
pg and http converge on **one** implementation of every send/recv seam instead of two drifting copies.

`network/transport.h` owns: `_socket`, the `_recv` byte-channel + io producer pump, `_send` + the `SendWriter`
coroutine + single-slot write-behind + the `_send_waiter` Dekker handshake + `_io_broken` + `_producer_gate`, and the
**one** high-water predicate. Each protocol supplies only its codec/framing + its session body on top. Migrate pg-wire
onto it first, then HttpSession (deleting http's duplicate machinery) — that second migration is the payoff of B.
Protocol-specific *framing* (pg `RecvFramer`, http `H1Codec`) stays per-protocol on top of the shared recv channel.

## Phased plan (Option B order) — gate per phase: build + 1455 pg driver tests + 49 es/http + gtests; cross-protocol/hot phases also get a `build_perf` bench
0. **Harness/baseline** — pin the pg driver suite + es/http suite + gtests as the per-step gate; capture `build_perf`
   baselines for **both** pg-wire and http (simple-query, extended, large-result, COPY; http throughput/latency) to
   compare against — the shared transport touches both protocols.
1. **Error model + scars** (pure, pg-local, no behaviour change — done first to shrink the header before transport
   surgery) — delete `ThrowDuck`; add `ToSqlError`/`ThrowIfError` + pg-error bridge; collapse the 3-arm catch;
   `SetupConnection` non-throwing; drop `_io`/ctor-body `_ioexec`/`DuckDBStatement::extracted`+`current_stmt_idx`;
   collapse the 3 `TaskRunner` awaiters; delete the dead `CopyMessagesQueue` layer; comments → WHY-only.
2. **Shared transport core** `network/transport.h` (templated on SocketKind) — factor the send half (`SendWriter`,
   write-behind single slot, `_send_written`/`_send_waiter` Dekker, **one** high-water predicate, `OnSendViewReady`,
   one poison path) + the raw `_recv` byte-channel + io producer pump out of the session. **Migrate pg-wire onto it.**
   **(hot path → pg perf bench)**
3. **Migrate `HttpSession` onto the shared transport** — delete http's duplicated send/recv/backpressure machinery; the
   two protocols now share one transport. **(cross-protocol → http perf bench; the Option-B payoff)**
4. **RecvFramer** (pg framing on top of the transport recv channel) — fold `pg_frame_codec` in; one parameterized
   frame-await replacing `NextFrame/AwaitFrame/FeedFrame`; RAII consume handle. (verify split-frame + COPY)
5. **Auth** — SCRAM/SASL parsing → `auth.cpp`; non-template `PgAuthenticator(reader,writer)`; `Handshake()` prologue
   enum. (verify auth driver tests)
6. **Query exec** — `DriveToResult`/`DriveWire` (collapse ~5 duplicated drive blocks); split `WireSinkContext` into
   per-query binding + collector `WireQueue`; one backpressure authority; `Resolve{Statement,Portal}`. **(hot path → perf bench)**
7. **COPY** — `CopyInScope` RAII; `WriteCopyDataFrame`; `CopyInBridge::Fail`; single post-switch consume. (verify COPY +
   the binary-copy `WHERE` repro)

Each phase is independently revertible and leaves the tree green; the rewrite is incremental, never a big-bang replace.
Phase 2/3 (the shared transport + http migration) is the largest and riskiest — it is gated on both protocol suites
plus before/after perf for each.

## Progress
- **Phase 1 DONE** (committed): error model — `ThrowDuck` → `ThrowIfError` template + one `wire_frames::ToSqlError`
  funnel, 3-arm command-loop catch collapsed to one (`f386b709`); dead `DuckDBStatement::extracted`/`current_stmt_idx`
  removed (`907d2057`). 1455 driver tests pass. Remaining Phase-1 scars (`_io`/ctor `_ioexec`, the 3 TaskRunner
  awaiters, dead `CopyMessagesQueue` layer) deferred into their structural phase (2 / 7) to avoid double-touching.

## Phase 2 design basis (confirmed by reading both sessions)
`HttpSession<Kind>` (`http/session.h`) and `PgWireSession<Kind>` carry the **identical** send/recv transport surface —
extract verbatim into `network/transport.h` (templated on SocketKind), each session embeds one:
- **members:** `Socket<Kind> _socket`, `message::Buffer _recv`, `message::Buffer _send` (constructed with
  `kSendFlushSize` + the `OnSendViewReady` flush callback), `message::SequenceView _write_view`,
  `std::atomic<bool> _write_armed`, `Gate _write_gate`, `std::atomic<size_t> _send_written`,
  `std::atomic<size_t> _send_waiter{kSendWaiterIdle}`, `std::atomic<bool> _io_broken`, `_producer_gate`, `_ioexec`.
- **methods (verbatim, fences intact):** `SendWriter()` coroutine, `OnSendViewReady`, `KickSend`, `HasUnsentBytes`,
  `SendBroken`, `ArmSendWaiter`/`DisarmSendWaiter`, `AwaitSendBelowHighWater`, `DrainSendOnTask`, plus the recv
  byte-channel + watermark wait. The `_send_waiter` Dekker handshake (`ArmSendWaiter`↔`SendWriter`) and the seq_cst
  fences move **unchanged** into the transport — this is the one place they must live afterwards.
- protocol-specific framing stays on top: pg `RecvFramer` / `NextFrame`, http `H1Codec`. The session keeps `_task`
  (pg) / SessionMain body and supplies the recv-consumer wake (`_task->RequestRun()` vs http's wake) — that wiring is
  the only per-protocol divergence on the recv side.
Naming/placement (per #796, applied with judgment): a greppable type (e.g. `Connection`/`Transport`) in
`server/network/` root, no `duckdb` in its identifiers; namespace stays `sdb::network` for now (the flat-`sdb` sed is
a coordinated tree-wide pass, not piecemeal).

### Phase 2 status + remaining migration (mechanical)
DONE: `Transport<Kind, Session>` CRTP base added to `connection.h` (`7606b3fe`) — owns `_socket`, `_ioexec`, `_recv`,
`_send`, `_write_view`/`_write_armed`/`_write_gate`, `_send_written`, `_send_waiter`/`kSendWaiterIdle`, `_io_broken`,
`_writer_stop`, `_producer_gate`, `_task`, `_task_spawned`; methods `KickSend`/`HasUnsentBytes`/`SendBroken`/
`OverSendHighWater`/`OnSendViewReady`/`ArmSendWaiter`/`DisarmSendWaiter`/`AwaitSendBelowHighWater`/`DrainSendOnTask`/
`Flush`/`AwaitMoreBytes`/`SendWriter` (fences verbatim). One CRTP hook `OnSendPoison()`.

REMAINING — migrate each session (do HTTP first, smaller; build + es/http(49)+gtests; then pg, build + drivers(1455);
each its own commit). For `HttpSession` (`http/session.h`) and then `PgWireSession` (`pg_wire_session.h`):
1. Base-list: add `public Transport<Kind, HttpSession>,` (resp. `PgWireSession`).
2. Ctors: replace the `_socket`/`_io`/`_ioexec` initializers with a base delegation — Tcp/non-Ssl → `Connection{exec}`;
   Ssl/MaybeTls → `Connection{exec, *ctx.ssl}`. Keep the protocol-specific initializers (`_deadline`/`_router`/`_auth`
   for http; `_credentials`/`_allow_cleartext`/`_cancel`/`_max_message` for pg).
3. Delete `Close()`/`Lowest()` (base provides) and the redundant `_io` member; rewrite the one `asio_ns::post(_io, …)`
   teardown to `asio_ns::post(_ioexec->Context(), …)`.
4. Delete the now-inherited members: `_socket _ioexec _recv _send _write_view _write_armed _write_gate _send_written
   kSendWaiterIdle _send_waiter _io_broken _writer_stop _task _task_spawned` (pg also `_producer_gate`). Keep everything
   protocol-specific (http: `_deadline _router _auth _codec _dechunk _idle _conn _connection_ctx _user`; pg: `_scratch
   _dispatch_consume _params … _copy_gate _copy_route _feeder_done _statements _portals …`).
5. Delete the inherited methods + their out-of-line defs: `OnSendViewReady KickSend HasUnsentBytes SendBroken`
   (`OverSendHighWater` pg) `ArmSendWaiter DisarmSendWaiter AwaitSendBelowHighWater DrainSendOnTask` (`Flush` pg)
   `AwaitMoreBytes` (http) `SendWriter`. Keep `Run`, `SessionMain`, and all framing (`ReadHead`/`ReadBody` http;
   `TryAssembleFrame`/`NextFrame`/`AwaitFrame`/`FeedFrame`/`PeekTotalLength`/`CopyRecvInto` pg).
6. Add `public: void OnSendPoison() {}` — http empty; pg `{ _copy_gate.Kick(); }`.
7. `Start()` keeps `SendWriter().Detach()` (now the inherited one) + `Run().Detach()`.
8. **CRITICAL (dependent-base lookup):** `Transport<Kind, …>` is templated on `Kind`, so it is a *dependent* base —
   unqualified `_send`, `_recv`, `_socket`, `_task`, `KickSend()`, `SendWriter()`, `AwaitSendBelowHighWater()`, etc. in
   the session body are NOT found by two-phase lookup. Rather than sprinkle `this->` over hundreds of sites, add one
   block of `using Transport<Kind, HttpSession>::member;` declarations near the top of the derived class for every
   inherited name the session references (`_socket _ioexec _recv _send _write_view _write_armed _write_gate
   _send_written _send_waiter _io_broken _writer_stop _producer_gate _task _task_spawned KickSend HasUnsentBytes
   SendBroken OverSendHighWater OnSendViewReady ArmSendWaiter DisarmSendWaiter AwaitSendBelowHighWater DrainSendOnTask
   Flush AwaitMoreBytes SendWriter`). The compiler flags any missing `using` as an undeclared-identifier error, so
   iterate build → add the missing `using` → rebuild until green. (`OnSendPoison` is called by the base via
   `static_cast<Session*>` so it needs no `using`, but it must be public.) Keep the session bodies otherwise unchanged.
Then phase 3 = delete http's now-dead duplicate (covered as http migrates here); phases 4-7 per the list above.

### Phase 2 progress note (base done, sessions pending)
- The shared base is `Transport<Kind, Session>` in `connection.h`, committed + green but unused (`7606b3fe`, renamed
  from `Connection` in `6d152b39` to avoid the `duckdb::Connection` shadowing — an unqualified `Connection` in a
  session resolved to the non-template duckdb type). `Transport` is globally unique/greppable.
- Migration use: in each session add `using Transport<Kind, SessionName<Kind>>::member;` (explicit `<Kind>`, the form
  that parses; `using Base = …` aliasing tripped two-phase lookup). Build → add each undeclared name's `using` → repeat.
- A first HttpSession migration attempt was reverted to keep the tree green; redo it cleanly with the unique name +
  explicit-`<Kind>` usings, build against es/http(49)+gtests, commit; then pg against drivers(1455).

---

# Phase specifications (3–7)

Phases 0–2 are covered above (0 harness, 1 error-model+scars DONE, 2 transport base DONE + migration checklist). The
sections below give phases 3–7 the same design/steps/gates/gotchas detail. Every phase keeps the **one session
coroutine** and the **four perf seams**, builds green, and is gated on the functional suites (pg drivers 1455, es/http
49, the `serenedb-tests_basics` gtests); the two hot-path phases (6, and the migrations in 2/3) also take a `build_perf`
before/after.

## Phase 3 — migrate `HttpSession` onto `Transport`; delete its duplicate transport

**Design.** With pg-wire on `Transport<Kind, PgWireSession<Kind>>` (phase 2), do the same for HTTP so both protocols
share one implementation of every send/recv seam. http's framing (`H1Codec`, `ReadHead`/`ReadBody`, `_dechunk`) and its
deadline/idle machinery (`_deadline`, `_idle`, `kHttp*ReadTimeout`) stay http-specific on top of the inherited transport.

**Steps.**
1. Base-list: `class HttpSession final : public Transport<Kind, HttpSession<Kind>>, public std::enable_shared_from_this<…>, public http::ResponseSink, public RequestContext`.
2. Ctors delegate to the base: non-Ssl → `Transport<Kind, HttpSession<Kind>>{exec}`; Ssl → `…{exec, *ctx.ssl}`. Drop the
   `_socket`/`_io`/`_ioexec` initializers; keep `_deadline`/`_router`/`_auth`.
3. Add the dependent-base `using Transport<Kind, HttpSession<Kind>>::member;` block (see phase-2 checklist) for every
   inherited name http references (`_socket _ioexec _recv _send _task _task_spawned KickSend HasUnsentBytes SendBroken
   AwaitSendBelowHighWater DrainSendOnTask AwaitMoreBytes`; add more on undeclared-identifier errors).
4. Delete the now-inherited members + methods (`OnSendViewReady KickSend HasUnsentBytes SendBroken ArmSendWaiter
   DisarmSendWaiter AwaitSendBelowHighWater DrainSendOnTask AwaitMoreBytes SendWriter` + the `_socket/_recv/_send/
   _write_*/ _send_*/ _io_broken/ _writer_stop/ _task/ _task_spawned` members) and the redundant `_io`; rewrite the one
   teardown `asio_ns::post(_io, …)` to `asio_ns::post(_ioexec->Context(), …)`.
5. `void OnSendPoison() {}` (http owes no extra wake — no COPY feeder / producer gate). `Start()` keeps
   `this->SendWriter().Detach()` + `Run().Detach()`.
6. The `ResponseSink` overrides stay: `Drain() { return AwaitSendBelowHighWater(); }`, `Broken() { return SendBroken(); }`
   (now the inherited ones).

**Gate.** build → `serened serenedb-tests_basics` → `NetworkHttp.*`/`NetworkRouter.*` gtests → es/http driver suite (49)
→ `build_perf` http throughput/latency vs the phase-0 baseline.

**Gotchas.** Dependent-base lookup (usings, not `this->` soup, not a `Base` alias). `Transport` name avoids the
`duckdb::Connection` shadowing. http never calls `Flush()`/`_producer_gate` — they're inherited-but-unused (harmless;
one extra atomic kick per 64 KiB flush, below the bench's noise). The `_send` flush-callback now lives in the base, so
http's member block must NOT redeclare `_send`.

## Phase 4 — `RecvFramer`: fold `pg_frame_codec` in, one parameterized frame-await, RAII consume

**Design.** The receive side is currently smeared between the session (`TryAssembleFrame`, the three
`NextFrame`/`AwaitFrame`/`FeedFrame` wrappers, `PeekTotalLength`, `CopyRecvInto`, `_scratch`, the `recv_consume`/
`_dispatch_consume` bookkeeping) and the over-extracted `pg_frame_codec.{h,cpp}` (a 2-method class + 3 result structs
for ~20 lines of pure parsing, called from two places, that *leaks* the split-frame reassembly back into the session).
Collapse it into one `RecvFramer` owning the recv view over `Transport::_recv`, with `pg_frame_codec`'s `Parse`/
`StartupCode` folded in as private free functions (delete `pg_frame_codec.{h,cpp}`, keep the `FrameStatus` enum, drop the
`WireFrame`/`FrameResult` wrappers — one `Frame` type). `wire_frames.{h,cpp}` is the opposite concern (response
*writing*) and stays separate.

**Steps.**
1. New `RecvFramer` (own header or a clearly-bounded section) holding `_scratch`, `max_len`, the consume bookkeeping, and
   a reference to the transport's `_recv`. Move `TryAssembleFrame`/`PeekTotalLength`/`CopyRecvInto` in; inline
   `pg_frame_codec::Parse`/`StartupCode`.
2. One co_await-able `NextFrame(bool typed, uint32_t max_len, Waiter wait)` parameterized by the wait strategy —
   replacing the three near-duplicate wrappers (`Waiter` = socket-read for startup, `_task->Park` for the command phase,
   `_copy_gate.Wait` for the feeder). The wait *is* the io↔duck seam; keep all three call patterns.
3. Replace `recv_consume`/`_dispatch_consume` + the ~13 manual `if (frame.recv_consume) _recv.Consume(...)` sites with an
   RAII frame handle: contiguous frames hold the pending consume and `Consume` on scope-exit (or an explicit `Commit()`
   the caller invokes after it's done with the borrowed payload); split frames already consumed → handle is a no-op.
   COPY-FROM-STDIN moves the handle to the feeder instead of the early-consume-then-zero hack.
4. Make non-`Ok` outcomes a status-only `Frame` variant so `.payload` is unreadable on error.

**Invariants.** Single recv consumer at any instant (the role handoff is the `Waiter` choice). Zero-copy contiguous
payload view into `_recv`, consumed only after the handler runs. One-copy `_scratch` linearization for chunk-spanning
frames only. `max_len` enforced before alloc; over-cap → `TooLarge`, never a silent drop. Big-endian length + typed/
untyped offset (startup frames have no type byte). CVE-2021-23214 empty-buffer check before the TLS handshake.

**Gate.** build → drivers (1455) — exercise split frames (large statements/params) + COPY. **Optional, bench-gated
follow-up:** the "always-contiguous via Reserve-the-rest" idea that would delete `PeekTotalLength`/`CopyRecvInto`/
`_scratch` — only if a COPY/large-result `build_perf` shows it's a wash or better.

**Gotchas.** The borrow-then-consume contract is load-bearing; the RAII handle must not consume before the handler
finishes with the view. Keep `wire_frames` separate; move `DuckErrorToSqlData`/`ToSqlError` with the error concern, not
here.

## Phase 5 — `PgAuthenticator`: SCRAM parsing → `auth.cpp`, non-template driver, named `Handshake()`

**Design.** `auth.{h,cpp}` (pure crypto, shared with HTTP Basic) stays. The wrong boundary is *inside* the session: the
SCRAM/SASL message parsing (gs2 header, client-first-bare, client-final proof/cbind/nonce) lives in the `SocketKind`
template header though it has zero dependency on the socket. Move it to `auth.cpp` as pure functions; lift the
`Authenticate*` trio into a non-template `PgAuthenticator` so the ~220-line auth body leaves every `Kind` instantiation.

**Steps.**
1. In `auth.cpp`: `struct ScramClientFirst {…}`, `struct ScramClientFinal {…}`, and
   `optional<ScramClientFirst> ParseScramClientFirst(string_view)` / `…Final(string_view)` (discriminated error reason →
   right SQLSTATE). Unit-testable, no io.
2. New non-template `PgAuthenticator` (`.cpp`) driving SCRAM/cleartext over two callbacks — a frame reader
   `Future<optional<string_view>> ReadPasswordMessage()` and `WriteAuth(int code, string_view)`+flush — returning
   `{Ok, Failed(SqlErrorData), Closed}`. The session passes lambdas binding `NextFrame`/`_send`. No `fail()` lambda
   scattering, no exceptions.
3. Session `Run()` prologue → a named `Handshake()` returning `{Proceed, Closed}` (the SSLRequest/GSS/Cancel/version loop
   as states, not inline `for(;;)`+break); delete the dead `Ssl`-Kind arm and the swallow-all `try/catch` in favour of an
   `absl::Cleanup` teardown.
4. `SetupConnection` GUC loop → a non-throwing `SetSetting` status path (per the THROW_SQL_ERROR rule). Replace the manual
   `kAuthOk` byte array with `WriteAuthRequest(_send, 0, {})`; add `WriteAuthSASL(mechanisms)` to `wire_frames`.

**Invariants.** `AuthenticationOk` + the startup burst go out **after** `Authenticate()` succeeds **and after**
`SetupConnection`, on the duck worker. CVE plaintext check on the SSLRequest→TLS upgrade. Constant-time secret compares
(`ConstantTimeEqual`, vendored OpenSSL). `NextFrame` is the only frame source during startup/auth (no SessionTask yet).
BackendKeyData pid=high32/secret=low32.

**Gate.** build → drivers (1455) — exercise the SCRAM, cleartext, and trust auth paths (and any md5/channel-binding added
under the separate auth work-item). Add a small gtest for the new pure SCRAM parsers.

**Gotchas.** Lifting auth out of the `Kind` template shrinks per-Kind code but the authenticator still needs
`_socket.IsTls()` (one bool) — pass it, don't re-template. Keep auth io-side; do not pull `SetupConnection`/burst back
from the duck side for symmetry.

## Phase 6 — query execution: two drive primitives, split `WireSinkContext`, one backpressure authority

**Design.** The session/`wire_collector` split is right in principle (the collector is a `PhysicalResultCollector`, a
DuckDB operator) but leaky: `WireSinkContext` is half session-owned (send/written/task/formats/proto) and half
collector-owned (mode/rows/batches/blocked + the high-water math), and the backpressure decision is split across **three**
places that must stay in lockstep (`DrainWire` session, `OverHighWater` collector, `UnblockSinks` ctx) — the single
biggest hazard here. Also the execution core is duplicated across ~5 drive blocks (simple + extended).

**Steps.**
1. Two shared drive primitives (free functions / a thin `Executor`, **no new coroutines**): `DriveToResult(prepared,
   params) -> unique_ptr<QueryResult>` (ensure-txn + `PendingQueryEnsured` + `DriveQuery` + `Execute` + `ThrowIfError`)
   and `DriveWire(prepared, params, formats) -> uint64 rows` (`WriteRowDescription` + `MakeWireContext` +
   `PendingQueryEnsured(wire)` + `DriveQuery(wire)` + `FinishWireDrain` + cleanup `Execute`). Replace the ~5 duplicated
   drive blocks in `RunSimpleQuery`/`HandleExecute`/`RunCopyFromStdin`/`SerializePage`. `DriveQuery` itself is unchanged
   (the handoff seam — preserve `kInlineSliceBudget=8`, the `ExecuteTask([this]{_task->RequestRun();})` lambda, the
   arm/disarm-on-wire branch).
2. Split `WireSinkContext` along its real seam: an immutable per-query `SessionWireBinding {send, send_written, task,
   formats, proto}` the session hands in, and a collector-owned `WireQueue {mode, batches, blocked, queued/pinned_bytes,
   min_batch, Push/Pop/Block/Unblock}`. Kill the `Reset()`/`use_count()==1` reuse hack.
3. Make `WireQueue` the **one** backpressure authority: fold `DrainWire`'s per-mode switch + the session's
   `OverSendHighWater` duplication into `ctx.Drain(send, written, finished)` / `ctx.OnWriterProgress(written)`; co-locate
   the `kSend*`/`kWire*` constants. `FinishWireDrain` becomes a thin loop over `ctx.Drain(finished=true)` + Park.
4. `ResolveStatement(name)`/`ResolvePortal(name)` fold the `_anon_*` special-case (or key unnamed entries under `""`);
   `DescribeStatement`'s dummy-replan → a named `ResolveResultTypes(prepared)`.

**Invariants.** RowDescription committed to `_send` **before** `PendingQuery` in direct mode (then CommandComplete after
the drain). Lazy kickoff (`DriveQuery`'s gated `RequestRun`, `kInlineSliceBudget=8`). Zero-copy splice
(`SpliceCommitted`, `WriteDataChunkRange` builds the DataRow prefix in place). The **three** distinct backpressure
accounting modes each survive. `PendingQuery` `allow_stream_result=false` only when a wire ctx is armed. RocksDB txn/
snapshot ensured per the prepared statement's read/modified set. `Execute()`'s row count is *this* Execute's, not the
portal total.

**Gate.** build → drivers (1455) — large results, extended protocol, cursors/paging → `build_perf` (simple-query,
extended, large-result) vs baseline (hot path).

**Gotchas.** The three-place backpressure lockstep is the top correctness hazard — collapsing it onto `WireQueue` is the
point; verify the ordered-mode lookahead window + pinned cap behaviour is unchanged. Keep `wire_frames` free encoders;
don't class-ify them.

## Phase 7 — `CopyInScope` RAII; delete the dead old-server `CopyMessagesQueue` layer

**Design.** COPY-FROM-STDIN control belongs in the session (it owns `_recv`/`_send`/`_copy_gate`/`_copy_route`); the
DuckDB FileSystem + binary `CopyFunction` adapters stay in `connector/` (registered on the DuckDB instance), with
`CopyInBridge` the seam — keep that two-layer shape. But the session leaks COPY state across 5 members + 3 methods with
manual lifetime/join/rethrow wiring, and a whole `CopyMessagesQueue` layer from the old server is dead.

**Steps.**
1. `CopyInScope` (RAII, header-only, pg module): ctor sets up the stack bridge, flips `_copy_route=true`, resets the
   client-state `copy_stdin_*` fields, writes `CopyInResponse`, kicks send, spawns the feeder; dtor / an explicit
   `co_await Join()` does `bridge.Abort()` + `_copy_gate.Kick()` + park-until-`_feeder_done` + `_copy_route=false` +
   `SetCopyInBridge(nullptr)`. `RunCopyFromStdin` becomes: construct scope, Prepare/Drive/Execute letting `SqlException`
   propagate, `co_await scope.Join()`, `WriteCommandTag` — deleting the `try/catch`/`exception_ptr`/rethrow.
2. Delete the dead old-server queue layer: `copy_messages_queue.h`, `ConnectionContext::_copy_queue`/`GetCopyQueue`, the
   `CopyInFileHandle` queue ctor arg/`_iterator`/bridge-vs-iterator ternary, and the dead `if (auto* queue =
   GetCopyQueue())` branch in `duckdb_client_state.cpp` (`_copy_queue` is always null in the new server — `SetupConnection`
   passes `nullptr`). `CopyInFileHandle` collapses to bridge-only (CSV replay-buffer stays).
3. `CopyInBridge::Fail(SqlErrorData)` overload so the feeder stops hand-building `make_exception_ptr`+`source_location` at
   three sites; hoist the per-arm `_recv.Consume` to one post-switch consume (mirror the command loop).
4. One `WriteCopyDataFrame(message::Buffer&, payload-callback)` (reserve 5-byte prefix, write payload, backpatch type+len,
   `Commit(false)`) shared by text-out and binary header/row/trailer — replacing the duplicated dance in
   `CopyOutFileHandle::DoWrite` and `pg_binary_copy::FinishCopyData`.

**Invariants.** Strict lock-step single-in-flight borrowed view between feeder and worker (one `OneShotEvent` pair, no
queue, no per-packet copy). `_copy_route` recv-consumer handoff (exactly one consumer of `_recv`). Detection-before-
Prepare + feeder-live-before-sniff ordering. Feeder always joined before the stack bridge is destroyed, even on error.
CSV-sniff re-open replay vs binary single-open no-replay (`copy_stdin_no_replay`). Binary TO STDOUT single-threaded
(`SetChildCardinality`, already fixed). Zero-copy CopyData framing (prefix-reserve + backpatch).

**Gate.** build → drivers (1455) incl. `test_copy`/`test_shell_copy` + the binary-COPY-`WHERE` repro
(`COPY t FROM STDIN (FORMAT binary) WHERE s LIKE 'x%'`).

**Gotchas.** Do NOT merge the connector adapters into the session (they're the DuckDB-facing half, registered on the
instance). The feeder is a transient per-COPY coroutine that joins back — not a new long-lived coroutine; do not fragment
`RunCopyInFeeder`.

---

## Cross-phase cleanups (apply as each phase touches the area)
- Comments → WHY-only; strip the "mirrors / matches X / without RTTI" narration the iterative growth left behind.
- Keep one error-mapping home (the phase-1 `ToSqlError`/`ThrowIfError` + `DuckErrorToSqlData`); no new `throw`/`catch`
  outside the single command-loop boundary and the feeder join.
- After all phases: re-evaluate whether `pg_wire_session.h` should split into `.h` (decl + hot inline helpers) + a
  `.cpp`/per-concern headers so a touch doesn't recompile every `SocketKind` instantiation (compile-time win, no
  behaviour change). Naming/placement per #796 (greppable unique types, no `duckdb` in identifiers) applied with judgment.
