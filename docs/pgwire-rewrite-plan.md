# pg-wire session rewrite — plan

Goal: turn `server/network/pg/pg_wire_session.h` (2309-line god-header, grown iteratively) into clean, clearly-layered
code **with no perf regression**. Derived from an 8-way per-concern study of the whole pg-wire module (session + every
dep + the design doc), not a line-by-line port.

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
DONE: `Connection<Kind, Session>` CRTP base added to `connection.h` (`7606b3fe`) — owns `_socket`, `_ioexec`, `_recv`,
`_send`, `_write_view`/`_write_armed`/`_write_gate`, `_send_written`, `_send_waiter`/`kSendWaiterIdle`, `_io_broken`,
`_writer_stop`, `_producer_gate`, `_task`, `_task_spawned`; methods `KickSend`/`HasUnsentBytes`/`SendBroken`/
`OverSendHighWater`/`OnSendViewReady`/`ArmSendWaiter`/`DisarmSendWaiter`/`AwaitSendBelowHighWater`/`DrainSendOnTask`/
`Flush`/`AwaitMoreBytes`/`SendWriter` (fences verbatim). One CRTP hook `OnSendPoison()`.

REMAINING — migrate each session (do HTTP first, smaller; build + es/http(49)+gtests; then pg, build + drivers(1455);
each its own commit). For `HttpSession` (`http/session.h`) and then `PgWireSession` (`pg_wire_session.h`):
1. Base-list: add `public Connection<Kind, HttpSession>,` (resp. `PgWireSession`).
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
Then phase 3 = delete http's now-dead duplicate (already covered above since http migrates here); phases 4-7 per the
list above.
