# pg-wire execution model -- analysis & design

Status: proposal. Companion to `networking-architecture.md` (Part B experiments).
Subsumes the planning of "parallelize result serialization" and "audit
scheduler/reschedule overhead", and adapts issue #646 to the new server.

Every claim below was verified against the code; file:line references are to
the current tree (branch `mbkkt/new-server`).

## 1. What exists

### 1.1 Old server (`server/pg/pg_comm_task.*` + `general_server/`) -- the reference for io overlap

For one connection, three things genuinely overlap in time:

- **Read-ahead.** `AsyncReadSome` unconditionally re-arms after every
  `ReadCallback` (`generic_comm_task.cpp:117-135`); the io thread frames and
  queues the next pipelined messages (and COPY data into `_copy_queue`,
  `pg_comm_task.cpp:1731-1734`) while a query runs.
- **Write-behind.** `message::Buffer` is an SPSC chunk chain: the compute
  thread keeps encoding rows past the atomic `_send_end` watermark while an
  `async_write` of earlier chunks is in flight. `FlushStart`'s exchange either
  starts a write or extends the in-flight goal; `FlushDone` (io thread) frees
  sent chunks and continues or CAS-releases ownership
  (`message_buffer.cpp:36-47,147-165`). Built precisely for this; the new
  server doesn't use this mode at all.
- **DuckDB workers** run pipeline tasks concurrently with both.

What must NOT be copied: per-message folly posts (`ScheduleProcess*`,
`pg_feature.h:45-67`), triple copy + memmove of every inbound message into
`queue<std::string>`, 4+ `_queue_mutex` acquisitions per packet, the
NO_TASKS busy-spin More-loop (`pg_comm_task.cpp:1252-1255,1097-1101`), and
`FetchRaw` cond-var-blocking a pool worker.

### 1.2 New server (`server/network/pg/pg_wire_session.h`) -- minimal per-message cost, zero overlap

One coroutine per connection ping-pongs io <-> duck:

- **2 thread handoffs per protocol message**, unconditionally:
  `co_await yaclib::On(*_duck)` before EVERY dispatch (`:1776`, futex wake) and
  `co_await yaclib::On(*_ioexec)` after (`:1814`, eventfd wake) -- even for
  Close/Describe/Bind-decode/empty-Execute, which do trivial or no DuckDB work.
  A pipelined P/B/D/E/S cycle = 1 recv + **8 handoffs** + 1 writev. Postgres
  does 0. This is the remaining structural cost behind the measured select-1
  CPU gap.
- **No read-ahead**: `NextFrame` is pull-only (`:374-414`); while the coroutine
  is duck-side there is no outstanding read -- pipelined bytes stall in the
  kernel, and a client disconnect mid-query is invisible (no cancel).
- **No write-behind**: `Flush` is the only socket-write site and the coroutine
  awaits write completion (`:343-349`). `_send` is constructed without
  `send_callback`/`flush_size` (`:315`; defaults `message_buffer.h:36-38`), so
  `SerializeResult` encodes the **entire result into RAM** before one gather
  write. For `SELECT * FROM range(10M)`: TTFB = full execution + full
  serialization; memory = the whole encoded result. Strictly worse than the
  old server here.
- **Single-threaded serialization** on one duck worker, interleaved with
  `FetchRaw` on the same thread (`:605-639`); the per-column serializer vector
  is rebuilt on every call (`:617-622`).
- The pump completes a non-inline query by **re-enqueuing the continuation
  through the scheduler** (`query_pump.h:147-149`) although the completing
  thread is itself a duck worker -- one more enqueue + futex per query.

### 1.3 Issue #646, validated

All four per-chunk costs of the streaming path are real, and the new server
pays all of them (it streams: `PendingQueryEnsured` passes
`allow_stream_result=true`, `:553`):

| Cost (per chunk) | Verdict |
|---|---|
| `DataChunk::Copy` + `Initialize` in `SimpleBufferedData::Append` | verified (`simple_buffered_data.cpp:114-122`); worse: `gstate.glock` is held across the whole copy (`physical_buffered_collector.cpp:31-39`), so even the "parallel" collector serializes all sink threads |
| two mutexes | understated: >=4 end-to-end (producer glock x2 + consumer ClientContext lock + Scan glock) |
| `RecursiveToUnifiedFormat` allocs per column | overstated for flat/constant/dictionary columns (one vector alloc per chunk + refcount traffic; real allocs only for nested types) -- but the re-decode work itself is real and duplicated on the consumer |
| all per-row encoding on one thread | verified for both servers |

Corrections for the new server: `client_config.get_result_collector` is
**bypassed when streaming** (`client_context.cpp:633-637`) -- a custom
collector must run with `allow_stream_result=false` (then it fully replaces
the streaming machinery; no fork patch needed). Ordering: a plain
`SELECT * FROM t` is the **batch-index-parallel** case
(`physical_result_collector.cpp:25-52`), so a parallel encode-in-Sink
collector must splice per-worker byte chains in `batch_index` order; only
declared non-order-preserving plans may splice in completion order.
`message::Buffer`'s chunk chain makes `SpliceCommitted` O(1) and the SPSC
invariants hold (single shared atomic `_send_end`).

### 1.4 DuckDB thread constraints (what is actually legal)

- `PendingQueryResult::ExecuteTask` is an embedder API legal **from any
  thread**; the CLI drives whole queries on the caller's thread. Serialization
  is only the ClientContext mutex. No TLS in the execution path; cross-slice
  state lives on the Executor. **Mid-query migration between threads is legal**
  and already happens (pump slices land on whichever worker dequeues them).
- The fork's executor wake (`on_reschedule`) is one-shot, stored on
  NO_TASKS/BLOCKED, fired on completion / chunk-ready / unblock / error, from
  arbitrary threads -- thread-agnostic; an io-side continuation can be woken
  just as well as a duck-side one.
- A hop to a sleeping duck worker costs sem_post + futex sleep/wake (~2
  syscalls; spin window patched 10000->100); the duck->io resume is an
  eventfd/epoll wake. These are the per-message taxes.
- Hard caveats for running query work inline on an io thread:
  1. **COPY FROM STDIN must never run inline** -- the bridge `Read()` blocks
     the executing thread waiting for CopyData that only that same io thread
     can feed (self-deadlock). Generalizes to any operator whose synchronous
     reads are serviced by the session's own io thread.
  2. **A slice is unbounded**: PROCESS_PARTIAL = up to 50 chunks
     (`pipeline.hpp:28`), and operators may block synchronously.
  3. **Live results must not be destroyed io-side**: tearing down an active
     executor runs `CancelTasks` -> executes in-flight tasks on the calling
     thread (`executor.cpp:440-470`). (Destroying *finished/materialized*
     results is any-thread-legal; the destroy-on-worker teardown is hygiene +
     notice ordering, not a DuckDB rule.)
  4. Allocator hygiene: jemalloc tcache / block caches are flushed only in
     duck workers' idle loop; io threads doing query work need an idle flush
     hook.

## 2. Design

Three orthogonal changes. Together they give: ~0 handoffs for OLTT-class
queries, old-server-grade io overlap for long queries, and parallel
serialization overlapped with socket writes for row-heavy queries.

### A. v2 (AGREED, supersedes the v1 inline-first/escalation design): three
### pinned coroutines, two SPSC buffers, one wake primitive

v1 kept one session coroutine migrating between pools and accumulated
placement rules (`OnIo()` checks, escalate-before-serialize predicates,
live-portal destroy escalation). v2 removes the question: **nothing migrates;
data moves instead.**

```
              +---------------------------- PgSession ------------------------------+
              |                                                                     |
              |  io thread (session's io worker)        DuckDB scheduler            |
              | +----------------------------+   +-----------------------------+    |
 client bytes | | RecvLoop (coroutine)       |   | SessionTask                 |    |
 ------------>| |  startup/TLS/auth phase,   |   |  (endless coroutine that    |    |
              | |  then byte pump:           |   |   IS a duckdb::Task)        |    |
              | |  read ALWAYS armed         |   |                             |    |
              | |  chunks -> mailbox         |   |  loop:                      |    |
              | |  -> RequestRun(session) ---+-->|   splice mailbox -> recv    |    |
              | |  EOF/error -> interrupt    |   |   frame <- recv             |    |
              | |  query, poison             |   |   parse/bind/execute        |    |
              | +----------------------------+   |   serialize -> send.Commit  |    |
              |                                  |                             |    |
              |  mailbox: SPSC chunk chain       |  parks (suspend; worker     |    |
              |  (io appends filled chunks,      |  freed) on:                 |    |
              |   duck takes all; recv buffer    |   WAIT_MESSAGE (recv empty) |    |
              |   itself stays single-owner)     |   WAIT_QUERY (NO_TASKS/     |    |
              |                                  |               BLOCKED)      |    |
              |  send: SPSC chain (duck->io,     |   WAIT_SEND (> high-water)  |    |
              |   B1's _send_end machinery,      |                             |    |
              |   unchanged)                     |  ALL wakes = RequestRun ->  |    |
              | +----------------------------+   |  scheduler enqueue -> fresh |    |
 client bytes | | SendWriter (coroutine, B1) |<--+  Execute() stack frame      |    |
 <------------| |  async_write(send chains)  |   |  (never a recursive/inline  |    |
              | |  drain -> RequestRun ------+-->|   resume -- stack-safe by   |    |
              | +----------------------------+   |   construction)             |    |
              |                                  +-----------------------------+    |
              +---------------------------------------------------------------------+
```

Decisions and reasoning:

- **Three coroutines, not two**: one io coroutine cannot await a read and a
  write concurrently (they are two independent *pending* ops -- full duplex),
  and a hand-rolled single-reactor loop would re-introduce read/write
  scheduling policy and break `ssl::stream` state handling. Under asio the
  two-coroutine io side has the identical syscall profile (one epoll pass
  delivers both events; speculative non-blocking IO happens at initiation).
  A future io_uring transport collapses the io side into one submission loop
  behind the same buffer interface -- the boundary is transport-agnostic.
- **SessionTask absorbs QueryPump.** The session IS the duckdb Task; the
  `kParked/kQueued/kRunning/kRunningWoke` coalescing state machine generalizes
  from "drive one query" to "drive the whole session". Three park reasons, one
  reason-agnostic wake (`RequestRun` -> enqueue via the io worker's producer
  token); on resume the coroutine re-checks its condition (spurious-tolerant).
  Query driving inlines ExecuteTask slices: RESULT_NOT_READY -> yield
  (re-enqueue, don't hog the worker); NO_TASKS/BLOCKED -> park, woken by the
  executor's on_reschedule patch.
- **No recursion by construction**: every wake goes through the scheduler
  queue; every resume is a fresh `Execute()` frame from ExecuteForever. The
  v1 inline `_job->Call()` chain is gone.
- **recv is the same Buffer-as-channel as send, mirrored.** The buffer already
  was an SPSC channel in the send direction (producer owns tail+commit,
  consumer owns head+free, one atomic watermark between); recv mirrors it:
  RecvLoop produces via Reserve/CommitWrite (CommitWrite publishes the atomic
  `_read_end` watermark), and the consumer-side operations (Readable/Front/
  ReadableView/Consume) bound themselves by a consumer-held snapshot of that
  watermark -- never by the producer's tail/end. Chunks before the snapshot
  chunk are final; the snapshot chunk is bounded by the offset; the consumer
  frees only strictly-before-watermark chunks. Single-owner users (HTTP
  session, the startup phase) are unaffected. Zero copies, no extra structure.
- **Startup is io-only; SessionTask starts lazily.** RecvLoop handles
  SSLRequest/TLS upgrade, startup params, cancel-request connections, and
  auth entirely on the io thread (no DuckDB). On success it spawns the
  SessionTask, whose prologue does SetupConnection + cancel registration +
  the startup burst, then enters the message loop. After spawning, RecvLoop
  is a byte-dumb pump.
- **COPY FROM STDIN keeps the feeder, channel-fed.** The io-pinned feeder
  coroutine survives (the CopyInBridge API and framing logic stay unchanged)
  but it no longer reads the socket: it takes over the recv-consumer role,
  assembling CopyData frames from the channel and waiting on a gate that
  RecvLoop kicks while a copy is routed (`_copy_route`). The SessionTask is
  parked in DriveQuery for the whole COPY and joins the feeder by parking on
  a done-flag, so the single-consumer invariant holds. (Folding the feeder
  into the bridge's worker-side Read remains a possible later simplification.)
- **Disconnect-cancel native**: the always-armed read means RecvLoop sees EOF
  mid-query; it interrupts the ClientContext (existing CancelToken), poisons
  the buffers, and RequestRuns -- the SessionTask unwinds and tears down on a
  duck worker, where result destruction belongs.
- **Per-message wake budget**: sparse select-1 pays 2 wakes (recv futex +
  writer eventfd). Under pipelining/load the wakes amortize toward zero: a
  P/B/D/E/S burst is ONE wake (SessionTask drains all buffered frames before
  parking), the writer batches at the flush threshold, and a saturated
  SessionTask never parks. We trade some sparse-c1 latency vs v1's zero-hop
  inline path for a model with no placement rules at all; the v1 caveats
  (inline slice blocking on disk IO, COPY screening, remote-source screen)
  disappear because DuckDB work simply never runs on io threads.

### B. Write-behind + backpressure (DONE, kept wholesale by v2)

`_send` runs in the buffer's callback/threshold mode (`kSendFlushSize` 64KB,
`kSendHighWater` 4MB): frame helpers' existing `Commit(false)` calls auto-arm
a view and kick the io-pinned SendWriter; encoding the next rows overlaps the
socket write of earlier ones (`FlushStart`/`FlushDone` SPSC machinery -- the
old server's proven path). Producer-side `TotalCommitted()` vs the writer's
written-bytes counter gives drain (`Flush()`) and the high-water backpressure
park. Write errors poison the session and close the socket. Measured: TTFB
263ms -> 0.9ms on a 369MB result; stalled-client RSS capped ~5MB (was: the
whole encoded result); 10M-row byte-exact. In v2 the backpressure wait becomes
a WAIT_SEND park of the SessionTask, woken by SendWriter's RequestRun.

### C. Parallel serialization: encode-in-Sink collector (#646, as-designed)

The structural answer for rows/wide (today 1.8x / 0.20x vs pg): move per-row
wire encoding off the single SessionTask thread onto the DuckDB executor
workers that already run the query, and reuse DuckDB's own streaming
backpressure + batch-ordering rather than reinventing them.

**Reuse, not reinvent (constraint 3).** DuckDB already parallelizes the
collector Sink and already solves ordering at plan-construction
(`physical_result_collector.cpp:25-52`): unordered plans →
`PhysicalBufferedCollector(parallel=true)`; order-preserving + batch-index →
`PhysicalBufferedBatchCollector` (+ `BatchedBufferedData`, which reorders by
`batch_index`); order-preserving without batch index → single-threaded. We
add ONE pg-wire-specific variant of each that changes only *what a chunk
becomes*: instead of `Append(copy of DataChunk)` then consumer-side
`FetchRaw → RecursiveToUnifiedFormat → encode`, the Sink **encodes the rows to
wire bytes in place** into a per-`LocalSinkState` `message::Buffer`, and the
stored unit is a `message::Buffer::Chain` (an O(1)-spliceable byte run), not a
copied DataChunk. Everything else -- pipeline scheduling, parallel sink, the
`batch_index` reorder, the BLOCKED/UnblockSinks backpressure, the
on_reschedule wake -- is inherited unchanged.

Concretely:
- `PgWireBufferedData : BufferedData` mirrors `BatchedBufferedData` but its
  queues hold `Chain`s keyed by batch index (ordered) or a FIFO (unordered).
  Backpressure is the existing `total_buffer_size` / BlockSink / UnblockSinks
  on aggregate **encoded** bytes.
- `PhysicalPgWireCollector` mirrors `PhysicalBuffered[Batch]Collector`:
  `LocalSinkState` owns a `message::Buffer` + the per-query serializer vector
  (built ONCE at GetLocalSinkState -- also kills the rebuild-per-call wart) +
  a `RecursiveUnifiedVectorFormat` scratch. `Sink()` calls the existing
  `WriteDataChunkRange` serializers (verified transplant-clean) into the local
  buffer; at the batch boundary / Combine it `ReleaseChain()`s into the
  buffered-data queue. `GetResult` returns a `StreamQueryResult` over the
  buffered-data, exactly like the batch collector.
- **Consumer = the SessionTask, draining bytes not rows.** Instead of
  `SerializeResult`'s FetchRaw loop, the session downcasts the result's
  buffered-data and calls a `FetchChain()` that returns the next in-order
  ready `Chain` (or blocks until on_reschedule when none is ready and the
  query is live), `SpliceCommitted`s it onto `_send`, and lets the SendWriter
  drain it -- the splice is O(1), so the SessionTask stays a coordinator, not
  the data path. Draining below `total_buffer_size` calls `UnblockSinks`.
- **Threading session state into the DuckDB-built operator.** Set
  `client_config.get_result_collector` to a per-query lambda right before
  PendingQuery, capturing this portal's output formats + a SerializationContext
  template (from `_connection_ctx`); clear after. One query at a time on a
  single-owner config -> no races. Requires a **one-line fork patch** so the
  hook is honored for streaming too (today gated `!stream_result`,
  `client_context.cpp:635`); the returned collector is itself streaming
  (`IsStreaming()=true`), so `output_type` logic is unchanged.

**Direct-to-`_send` for single-threaded sinks (mbkkt).** The serializers write
through `SerializationContext.buffer`, so the encode target is a per-query
construction choice: when the sink is single-threaded (the order-preserving
no-batch-index bucket -- `ParallelSink()==false`), point it at **`_send`
itself** -- no lstate buffer, no chain, no splice, no extra wake, and
`Commit(false)`'s threshold auto-flush streams bytes to the client DURING
execution. Safety is the v2 producer-role argument, not thread identity: a
single-threaded sink may run on any worker (or the session's inline slice),
but it is the only `_send` producer for the duration (the session writes
RowDescription before the drive and CommandComplete after, nothing during),
consecutive Sinks are sequenced by the single-threaded-sink guarantee, and
the completion wake's seq_cst handshake hands the role back. Backpressure:
the sink returns BLOCKED (InterruptState) on `_send` unsent bytes over
high-water; the session -- awake on writer wakes during the drive -- fires
the unblocks. Per-lstate buffers + chains are used ONLY for genuinely
parallel sinks.

**Constraint 1 (don't regress short queries).** The hook is installed only for
`QUERY_RESULT` statements. A tiny select in the single-threaded bucket takes
the direct-`_send` path: strictly less work than today (no FetchRaw round, no
chunk copy). In the batch-index bucket it pays one small lstate buffer + one
O(1) splice; if the bench shows that, start lstate buffers tiny (~1KB growth).
select1/wide must show no regression; gate on it.

**Constraint 2 (sorted AND unordered).** Inherited for free from the
collector-selection switch: order-preserving plans get the batch-index variant
(FetchChain pops in `batch_index` order, exactly as `BatchedBufferedData`
gates on `min_batch_index`); unordered plans get the FIFO variant (pop in
completion order). We do not decide ordering ourselves -- DuckDB's
`PreserveInsertionOrder` / `UseBatchIndex` predicates do, same as the stock
collectors.

Eliminated per chunk (all verified in §1.3): `DataChunk::Copy` + `Initialize`,
the >=4 mutex round-trips, the consumer-side `RecursiveToUnifiedFormat`
re-decode, and the single-threaded encode -- encoding now runs parallel across
workers and overlapped with the socket write.

**Staging:**
- C0 (`ReleaseChain`/`SpliceCommitted`) -- DONE.
- C1+C2 -- DONE (`server/network/pg/wire_collector.{h,cpp}`): all three modes
  (Direct/Ordered/Unordered); no fork patch (armed queries run
  `allow_stream_result=false` where the hook already fires; the
  materialized-empty result gives eager cleanup). Extended protocol defers
  planning from Bind to Execute so `max_rows` picks wire vs streaming-paging
  (plan errors surface at Execute; DescribePortal answers from the prepared
  statement). Validated: ordered 10M-row md5-exact, unordered multiset-exact,
  paging fallback intact, TTFB 1.8ms, 369MB drain 9% faster, backpressure
  4MB cap, short-query c1 no-regress.
- Backpressure hardening (post-C2, found by parquet-source stress + perf):
  - Sinks must never fire InterruptState callbacks: a callback for a task
    that does not end up descheduled spins `Executor::RescheduleTask` (it
    busy-loops on `to_be_rescheduled_tasks` under `executor_lock`) forever.
    `BlockSink` registers + wakes the session; the session is the only
    unblocker. The collector overrides `RequiredPartitionInfo()` (BatchIndex
    for Ordered) or the pipeline never populates `partition_info`.
  - Ordered admission is a batch WINDOW, not a byte budget
    (`kWireOrderedLookahead` beyond the emit cursor = `min_batch_index`).
    Pinned bytes (batches beyond the cursor) only unpin when the cursor
    reaches them, so any byte cap fills once and then admits a single
    follower per cursor advance -- serializing the encode tail of every
    batch. Bytes remain as safety nets: `pinned_bytes` backstop and an
    emittable cap (`queued - pinned`) that backpressures the cursor itself
    against a slow client (the cursor is otherwise exempt: it must always
    advance). The min batch streams live -- a single sink thread produces it
    in order, so its accumulated chain is emittable while in progress.
  - One `Chain` per batch (`Chain::Append` is O(1)); the splice loop pops a
    whole batch per pass, send-cap granularity becomes per batch, which is
    free because the bytes already exist either way.
  - Measured (32-core, loopback raw-drain, 428MB / 20M doubles, parquet
    source): ordered 839 -> 3200 MB/s (6.7 cores), unordered 700 -> 4760+
    MB/s (11-15 cores), direct parity ~900 MB/s; duckdb-quack bulk benchmark
    (psycopg2, SF1 lineitem 6M rows) 7.2s -> 1.29s (5.6x). RSS flat under a
    stalled client; instant full-rate drain after.
- C3 -- DONE: `SerializeResult` (dual-purpose row-streaming + tag coroutine)
  is gone. Tags go through the plain `WriteCommandTag`; the only remaining
  FetchRaw consumer is `SerializePage` (cursor paging), which also serves the
  paged-portal-then-unlimited drain (`max_rows==0` re-Execute) -- the old
  separate full-drain branch ignored the carried chunk and could drop rows
  mid-page. `DriveQueryWire` merged into `DriveQuery(pending, wire=nullptr)`.
- Lazy kickoff -- DONE (fork patch: executor.{hpp,cpp} + event.cpp; the
  scheduler is untouched). Stock duckdb fans every query out through the
  global task queue twice (pipeline-initialize task, then the pipeline task)
  and the parked driver pays a completion wake -- ~3 scheduler round-trips
  (enqueue + futex + cache-cold worker) for a 1-row query. Now
  `Event::SetTasks` with exactly ONE task, called on the thread currently
  driving that executor (thread_local set in Initialize/ExecuteTask/
  WorkOnTasks), hands the task to an executor-local `inline_task` slot; the
  driver consumes the slot before polling the queue and re-checks it before
  any NO_TASKS park. A trivial query never leaves its driver: zero scheduler
  round-trips. Multi-task events schedule normally, so parallel pipelines
  still wake sleeping workers. Two designs that DON'T work, for the record:
  enqueueing unsignaled into the shared queue breaks its global
  task-per-signal conservation (an awake worker steals the unsignaled task
  and burns a signal owed to another producer -- wedges the whole server);
  and guarding the slot with executor_lock self-deadlocks (Initialize holds
  it across ScheduleEvents), hence the dedicated nested lock. Measured local
  bare c16: select1_prepared 230k -> 487k, rows +22%, analytical +37%.
- Perf polish (pending): per-query `WireSinkContext` allocation
  (shared_ptr + map + mutex + std::function) shows as c8 select1 prepared
  -9% vs pre-C in bare A/B; reuse one context per session and reset between
  queries.

### Small verified warts to fix opportunistically

- `DrainNotices` takes an absl::Mutex per message even when empty
  (`connection_context.h:69-72`) -- add an atomic empty-check or version gate
  like `ReportChangedParameters`.
- One `ProducerToken` per io worker is shared by all its sessions and every
  hop serializes on its mutex (`io_executor.h:56-61`,
  `task_scheduler.cpp:72`) -- with A this stops being per-message, but watch
  it under multi-session load.

## 3. Phasing (each gated on the wire bench)

1. **B write-behind** -- DONE (see 2.B).
2. **v2 session model** -- RecvLoop/SendWriter/SessionTask + mailbox; absorbs
   QueryPump, deletes DuckExecutor and all migration/placement logic; COPY
   feeder merges into the bridge. Read-ahead + disconnect-cancel are
   intrinsic. Benchmark select1 all modes + analytical vs old server and PG.
3. **C parallel collector** -- the row-heavy throughput win, now native to the
   model: the SessionTask is the coordinator, not the data path -- all duck
   workers encode rows in Sink() into per-worker chains, spliced in
   batch_index order onto `send`, drained by SendWriter; row bytes never pass
   through the session coroutine. Also removes the remaining
   FetchRaw-blocks-a-worker waits (today serialization still drives streaming
   results on the SessionTask's worker, including cond-var waits -- accepted
   until C). Phase order inside C as in issue #646 (SpliceCommitted ->
   single-threaded collector -> parallel + batch-order splice -> retire the
   streaming path for pg-wire).
