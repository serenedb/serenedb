# Sequences: design for approval (Suggestion 2)

Status: v4. Points 1 and 3 are accepted. Point 2 is accepted with your batch correction, written up in "Batch = N per-row calls". Waiting for your go on v4. The code in the working tree is partial, unbuilt and uncommitted. Nothing more will be written until you approve.

Changes from v1, per your corrections:
- There is no background sync and no prefetch. The committing thread fsyncs, using DuckDB's group commit.
- Block size is B = 32 + CACHE, as in PostgreSQL. It was max(32, CACHE).
- A new record is written only when the logged values run out, as in PostgreSQL. It was written when half remained.

## Background

- **Pre-split (601aeb9bb, `server/catalog/sequence.cpp`):** the position lived only in the catalog log. `nextval` returned a value only after a record "never below value+32" was fsynced, so it waited once per 33 values, even in transactions that later rolled back. A crash skipped up to 32 values and never reissued anything. Today's branch ports the same scheme.
- **PostgreSQL (checked on 18.3):** one WAL for everything.
  - `nextval` logs 32 values ahead, plus CACHE. `last_value | log_cnt` goes `1|32`, `2|31` … `33|0`, then `34|32`.
  - The record is written without a flush; the commit's flush makes it durable. Rows and sequence records share the WAL, so an INSERT pays one flush.
  - A crash skips up to 32 values, plus each session's cached values. A value from a transaction that never committed can come back.
  - CACHE is per session.
- **Why we must choose:** we have two logs, the catalog log and each database's WAL. Suggestion 2 keeps sequence state only in the catalog log, so a Raft catalog log (ClickHouse Keeper style) owns it alone, and data logs never depend on it.

## Design

### State

- A sequence's position lives only in the catalog log, as reservation records meaning "restart no lower than N". Replay keeps the highest record, so records are idempotent and their order doesn't matter.
- Data WALs never carry sequence values: DuckDB's per-transaction sequence records are skipped for catalog-log sequences. Checkpoint, shutdown and DROP DATABASE have nothing sequence-specific to do.
- In memory, per sequence:
  - `next`: the next value to hand out.
  - `reserved`: the highest reservation appended to the log.
  - `durable`: the highest reservation known to be on disk.
- Block size B = 32 + CACHE, as in PostgreSQL. Search-table PK sequences use CACHE 65536.

### `nextval` and bulk `NextValues`

They take values from memory and record them in the transaction. No I/O, no catalog-log lock, never wait.

### Commit: the one rule

A transaction's rows are written only after every value it drew is below `durable`. This is checked at the start of the commit, in the committing thread, before its rows go to the database WAL:

1. If `durable` covers the values, nothing happens.
2. If `reserved` covers them (another committer has appended the record and is syncing it), join that sync.
3. Otherwise, append a reservation `next + B` and sync it.

The sync is DuckDB's group commit (`WriteAheadLog::GroupSync`, flat combining):
- The record is appended under the catalog-log lock, which takes microseconds, and the fsync runs with the lock released.
- Concurrent committers park on the fsync in flight or share the next one.
- To fsync without the lock, the committer pins the catalog log object with a `shared_ptr`, because compaction can replace it. This is how the data WAL's commit path already does it.

Example with CACHE 1 (the same cadence as PostgreSQL's `log_cnt`):

| commit using value | catalog log | data WAL |
|---|---|---|
| 1 | append "restart ≥ 34", fsync | row, fsync |
| 2 … 33 | - (covered) | row, fsync |
| 34 | append "restart ≥ 67", fsync | row, fsync |
| crash | restart at 67 (at most 32 values skipped) | |

### Commit that goes through the catalog log (several databases, or DDL)

- The reservation is appended before the commit decision, so the decision's own sync covers it. No extra fsync.
- If that commit fails, the record is truncated with it. `reserved` in memory moves only after the decision is durable.

### Other operations

- **Rollback:** writes nothing.
- **`setval`:** writes an exact record that outranks every reservation; it is durable when it returns.
- **Catalog-log rewrite (compaction):** writes each sequence's `reserved`.

### After a crash

- A sequence restarts at its highest durable reservation.
- A committed value is never reissued. At most B values are skipped. PostgreSQL skips at most 32, plus each session's cached values.
- A value drawn by a transaction that never committed can come back, as in PostgreSQL.

### Local cost (CACHE 1)

| workload | main | today's branch | this design |
|---|---|---|---|
| single-row SERIAL insert | 1 fsync per commit | 1 per commit, plus a catalog fsync inside `nextval` every 33 | 1 per commit. Every 33rd commit first does a catalog fsync, shared by concurrent committers |
| 100k-row BIGSERIAL insert | 1 | 49 catalog + 1 | 1 catalog + 1 data |
| autocommit `SELECT nextval` | 1 per statement | 1 per 33, inside `nextval` | 1 catalog fsync per 33 statements, at commit |
| rollback after `nextval` | 0 | pays the `nextval` sync | 0 |
| DDL or multi-database commit | - | - | no extra fsync |

### Raft later (ClickHouse Keeper style)

- The sequence code uses two log operations: append, and wait until durable. Under Raft these become propose and wait until committed. Group commit becomes batching proposals.
- A larger CACHE amortizes the consensus round per block.
- With several writer nodes, the record becomes "reserve B", applied by the log's state machine, which gives each node a disjoint range. Each node runs the same next/reserved/durable logic. This is future work, not built now.

## Points (your answers, v3)

### 1. Uncommitted values can come back after a crash: accepted

- **Downside, inside the database:** none. No durable row, index entry or committed result holds such a value.
- **Downside, outside the database:** a value can leave the database before its transaction commits, for example an order number shown to a user, sent to another service or used as an idempotency key. If the server crashes before COMMIT, the same number can later go to a different row. PostgreSQL has exactly this window.
- **What closing the window costs:** pre-split and today's branch close it by making `nextval` itself wait for a catalog fsync whenever it crosses a block, even in transactions that roll back.

### 2. CACHE per session, as in PostgreSQL

PostgreSQL (`src/backend/commands/sequence.c`):
- Each backend keeps a `SeqTableData` per sequence with fields `last` (for `currval`) and `cached`.
- `nextval` returns cached values without touching the shared sequence. When the cache is empty, it fetches CACHE values and logs CACHE + 32 ahead.
- The session's own `setval`, ALTER SEQUENCE, or a replaced sequence drops its cache. Other sessions keep theirs until used up.
- There is no batch: the executor calls `nextval_internal` once per row (`execExprInterp.c`, `ExecEvalNextValueExpr`).

Here:
- **State:** each session keeps, per sequence it used, the range of values it fetched but hasn't handed out yet, and `last`. It sits in the session's `ClientData` with its own small lock, because DuckDB runs one query on many threads.
- **`nextval`:** served from that range with no shared access. When the range is empty, it fetches CACHE values from the shared counter.
- **Commit rule:** unchanged. The transaction remembers the highest shared position its values came from.
- **Reservation:** shared `next` + 32 + CACHE, which is PostgreSQL's fetch + 32.
- **Invalidation:** the session's own `setval` or ALTER SEQUENCE drops its range. Other sessions keep theirs.
- **`currval`:** becomes per session. Today it reads the shared `data.last_value`, so it returns the latest value handed to any session.
- **Losses:** a crash or disconnect loses a session's unused cached values.
- **Size:** moderate. Session state, `nextval.cpp`, `setval`/`currval`, and tests. CACHE 1 (the default) behaves exactly like today's shared counter.

**Batch = N per-row calls, computed in one step** (your correction):
- A batch of N returns exactly the values N per-row `nextval` calls from this session would return, and leaves the session's range in the same state. The only thing skipped is another session taking a block between our refills, which is one of the outcomes PostgreSQL allows.
- Steps:
  1. Take up to N values from the session's range.
  2. If more are needed, take from the shared counter, in one step, as many whole CACHE blocks as the per-row calls would refill.
  3. Hand out what is needed and keep the rest of the last block in the session's range.
- Example with CACHE 10, the session holding 8–10, and a 25-row batch:
  - Per-row: 8, 9, 10, then refills 11–20, 21–30 and 31–40; rows get 8…32 and 33–40 stay cached.
  - Batched: 8–10 from the range, then 11–40 in one shared step; rows get the same 8…32 and 33–40 stay cached.
- **Result shape:** at most two contiguous runs (the range's leftovers, then the new blocks). The vectorized `nextval` (`nextval.cpp:88`) fills its vector from those runs. With CACHE 1 (the default) it is always one run, the same as today.
- **Search-table generated PKs** (internal sequence, CACHE 65536): the search-table sinks need one contiguous PK run per chunk (`pk_base + i` in `duckdb_physical_search_insert.cpp` and `duckdb_physical_search_update.cpp`). Proposal: this internal sequence keeps taking one contiguous run per chunk from the shared counter, without a session range. Its CACHE then only sets the reservation size, so the log gets one record per 65568 PKs. The alternative is teaching both sinks to split a chunk at the run boundary.
- The reservation comes from the shared counter, so batches are covered automatically.

### 3. Move the catalog log's two-phase commit fsync out of the lock: accepted, in this change

- **Why it's needed:** today every catalog-log commit (DDL, multi-database, sequence reservation) holds the lock through its fsync. Commits therefore run one fsync at a time and group commit can't combine anything. No second committer can even append while the first syncs.
- **Plan:**
  - Under the lock: prepare participants, append the decision (`COMMIT_PREPARED`), flush it to the OS without fsync, pin the catalog log (`shared_ptr`), and take an apply-order ticket.
  - Release the lock, then `GroupSync` to the decision's offset. It is shared with every other catalog commit and sequence reservation in flight.
  - Apply the participants (`CommitTransaction`) in decision order.
  - Errors before the unlock truncate the records, as now. After the unlock the only failure is the fsync, which poisons the log (fatal), as for the data WAL.
- **Also under the lock today:** each data participant's prepare-time fsync (its database WAL). The same pattern can move it out, so DDL and multi-database commits overlap their data fsyncs too.

## As built (v4, approved)

- **Reservation is shared counter + 32.** A session's fetch already moves the shared counter by CACHE, so this is exactly PostgreSQL's "CACHE + 32, counted from the start of the fetch". Adding CACHE again gave one value more than PostgreSQL, 41 instead of 40 after START 7 and a crash.
- **Search-table generated PKs (internal):**
  - They take blocks of max(chunk, 2^20) from the shared counter.
  - A chunk that doesn't fit in the rest of a block skips to a fresh one, so the sinks still get one contiguous run.
  - The same +32 rule then means one catalog record per ~1M PKs, and a bulk commit pays at most one catalog fsync.
  - There is no session cache on this path.
- **Two-phase commit (catalog-log decision):**
  - Under the lock: prepare, append the decision, register it, raise `reserved`, and count the commit as in flight.
  - Then unlock and group-commit the fsync.
  - Then apply the participants, and uncount.
  - Catalog-log compaction skips while any commit is in flight. Otherwise it could rewrite the log from memory before this commit's DDL is applied and drop it.
  - Participants in the same database are already applied in decision order, because that database's WAL lock is held from prepare to apply.
- **Deadlock found and fixed while testing:** the 2PC reservation wrote its record while holding the sequence lock, and `WriteSequenceValue` reads `entry->GetData()`, which takes the same lock.
- **Lock order:** the catalog-log lock may be held while taking a sequence's lock, never the reverse; a sequence's lock may be held while taking the transaction's lock (`PushSequenceUsage`). The commit path takes no transaction lock around sequences.
  - This was found by CI's TSAN, in the docs bootstrap: 86 lock-order-inversion reports. It is fixed in fork 6dac779a5e2.
  - The cluster's catalog-log `shared_ptr` is replaced under both the catalog-log lock and `_log_mutex`, and read under either (root f69f1fffd).
- **Tests:**
  - `any/pg/simple/sequence.test`: per-session CACHE across two connections, a 25-row batch equal to per-row calls, and per-session `currval`. It passes on serenedb (both wire engines) and on PostgreSQL 18.3.
  - `recovery/sequence_horizon.test`: the crash gap, `setval`, CACHE after a crash, and per-session blocks.
  - `recovery/sequence_commit_recovery.test`: checkpoint, two-database commit, search-table PK, DDL and uncommitted crash windows, and drop with a deferred close.
  - The four older recovery tests are back to 32-ahead expectations.

## Working tree now (uncommitted)

- **Fork:** sequence reservations and the commit rule, reservations in the 2PC path, sequence values skipped in the data WAL, and `Catalog::SyncCatalogLog`. It still contains v1's prefetch and `RequestCatalogLogSync`, which v2 removes. Suggestion 1's checkpoint copy and shutdown-order change are already removed. Not built.
- **Root:** v1's background-sync hook in `ClusterCatalog`, which v2 removes. The detached-database guard stays. Tests and docs still describe Suggestion 1.
- **Committed and pushed:** only the DROP DATABASE boot fix (8818602a6). Its CI run 36585187963: dev, asan and validate-pg passed.
