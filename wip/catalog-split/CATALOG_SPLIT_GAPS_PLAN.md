# Catalog split: remaining gaps

Branches: `mbkkt/catalog-split-fixes` in serenedb (PR #1259) and in the duckdb fork (PR serenedb/duckdb#121). References: pre-split `601aeb9bb` (fork `fbfea7c150ea`), split `4d9824d9c`.

Rules:
- Every P item is in this PR; only the items under "OK / not planned" are left out.
- A new feature is built only when it's very simple; otherwise it becomes an issue.
- In the fork, each commit is self-contained and formatted (`scripts/format_duckdb.sh`), and the `regen:` commit comes last. Root-repo commits get squashed.
- No new code comments.
- Docs are updated in the same change for anything user-visible.
- Each item ends with focused sqllogic/recovery tests; the CI suites aren't run locally.

## Execution order

1. Small, independent items: P1, P2, P3, P4, P11, P12, P13.
2. P7: the catalog WAL, the foundation for P5, P6, P8 and P10, including multi-database data transactions.
3. Items that need P7: P5 (sequences), P6 (no orphans), P8 (parallel recovery), P10 (fault points).
4. P9: parallel DML feed.

## Items

| # | Gap (verified) | Fix | Done when | Size |
|---|---|---|---|---|
| P1 | A database whose file is missing at boot silently comes back empty (tested: data gone, no error). | In `server/catalog/boot.cpp`, check the file before attaching. Refuse to boot by default, and restore `--missing_database=refuse\|skip\|drop` (pre-split `server/catalog/ddl/catalog.cpp:82,461`): `skip` leaves the database unattached, `drop` removes it from the catalog. | A recovery test deletes a database file: boot fails with the hint, and `skip`/`drop` do what they say. | small |
| P2 | A user type's oid is looked up by name through `search_path` (`server/pg/pg_types.cpp` `UserTypeEntry`). A type off the path reports `text`/`record`, and a shadowed name reports the other type's oid. | At CREATE TYPE, stamp the entry oid into the type's extension info (pre-split `StampUserType`, `server/catalog/entry/duckdb_schema_entry.cpp:1459`); columns carry the stamped type. `Type2Oid` reads the stamp, and the name lookup goes away. | Row description oids are right for an enum and a composite in a schema off `search_path`, and for a shadowed name. | small |
| P3 | PK/UNIQUE/CHECK oids are synthesized (`KeyIndexOid`/`ConstraintOid` in `server/pg/pg_types.h`: bits 62/61 + constraint position + table oid), so they change when an earlier constraint is dropped. No oid may be synthesized. | Restore `Constraint::oid` (fork `fbfea7c150ea` `constraint.hpp:71`) the way `ColumnDefinition::catalog_oid` was restored: allocated from the oid counter in PG mode at CREATE TABLE and ADD CONSTRAINT, serialized (`nodes.json` + regen last), claimed at load. `pg_class`, `pg_index`, `pg_constraint` and `pg_depend` use it; `KeyIndexOid`/`ConstraintOid` are deleted. | Each constraint and key index shows its own allocated oid, unchanged by dropping an earlier constraint and by a restart. | small–medium |
| P4 | A rename under dependents is refused: table, view, function, sequence or schema rename, and `RENAME COLUMN`/`ALTER COLUMN TYPE` even for a column no view reads. The error doesn't say what depends on the object. | In this PR:<br>(a) Allow `RENAME COLUMN`, `ALTER COLUMN TYPE` and `DROP COLUMN` when no dependent reads the column, using the sub-dependency check DROP COLUMN already applies to views (a `SELECT *` view reads every column). Functions record the columns they read the same way, if their body is bound at CREATE.<br>(b) Every remaining refusal names the dependents, like the DROP error (`view "v" depends on table "t"`), with a hint to drop and recreate them.<br>(c) Verify the suspected hole: renaming a function that an index expression uses is let through, because index dependents are, but the expression names the function. Refuse it if so.<br>Making dependents follow a rename (references by oid) is follow-up #1267. | (a) works for a column no dependent reads and is refused for one it reads; (b) the error lists every dependent; (c) is settled with a test. | small |
| P5 | Sequences are stock duckdb: an fsync per `nextval` statement, a value can be handed out again after a crash (tested: 2 and 3 handed out, crash, `nextval` = 2), and `NextValues` loops per value under a lock. | Port the pre-split `SequenceCounter` onto the catalog WAL. See P5 design. | `recovery/sequence_horizon.test` is back to the pre-split expectations (no value reissued after a crash), `INSERT … SELECT` of N rows into a serial column takes one reservation, and nothing costs more than pre-split. | medium |
| P6 | Orphans must not exist, and there is no boot sweep. This is also what makes re-deriving the oid counter at boot safe. Windows: an inverted-index directory or `<oid>.db` created before its CREATE commits; a DROP committed but its artifact not yet removed (index dirs by a background task, `<oid>.db` in the entry destructor). | Drive file operations from the catalog WAL, not from a directory scan. Before an artifact is created, an intent record goes into the creating transaction's catalog run. Replay removes the artifacts of intents with no commit, and repeats the removal for committed drops whose artifact still exists. A catalog rewrite keeps a drop record until its removal is done. | A recovery test crashes in each window (fault points from P10): no unreferenced file or directory remains, and a reused oid never opens old data. | medium |
| P7 | The catalog isn't separate. A database's DDL is logged in that database's duckdb WAL, interleaved with its rows; only roles and databases go to `engine_catalog/catalog.db`. So recovery can't know the catalog before replaying data (every index loads unbound, buffers the replay and binds serially afterwards), and `CREATE ROLE` next to database DDL fails with 0A000 (#1208.1). | One instance catalog WAL for all DDL and sequences; database WALs carry data plus the physical side of layout-changing DDL; atomic by two-phase commit. See P7 design. | `recovery/ddl_rollback_durability.test` is un-muted and passes. Mixed cluster/database DDL commits atomically. Recovery replays the catalog first and binds every inverted index before any data replays. A crash in any 2PC window leaves both halves or neither. Data-only transactions write exactly what they write today. | large |
| P8 | Inverted-index recovery is serial (#1208.2). `recovery_replay_depth` is registered but nothing reads it. | With P7's catalog known ahead, restore main's model: inverted indexes are bound before their table's data WAL replays, with per-index parallel replay (`external_range_replay`, a `ReplayQueue` window = `recovery_replay_depth`) and the parallel finish stage (`FinishReplay` + `Refresh` per index on the `TaskExecutor`). | Recovery of N indexes runs in parallel, the setting bounds the window, and the replay does no `UnboundIndex` buffering for inverted indexes. | large |
| P9 | The inverted-index DML commit feed is serial (#1208.3). | Restore `FeedPool` + `LiveFeed::FeedChunkParallel`, `external_local_append` + `AppendLocalRange`, and the two-phase prepare/finish at one tick (`main:` references in #1208.3). | The feed runs in parallel, and `inverted_index_hnsw_ef_search.test` is back to main's multi-segment rows. | large |
| P10 | Crash fault points removed by the split: `crash_before/after_search_commit`, `crash_sst_sink_after_ingest`, `crash_on_drop`, `unable_to_create`, `pause_sst_sink_mid_copy`, `crash_before_catalog_commit`, `crash_after_catalog_before_data`, `catalog_append_fails`, `compact_inside_ddl`/`compact_inside_drop`. | Map each to its crash window in the P7 design and re-add it where the window still exists, together with the recovery tests that used it (`inverted_index_rollback`, `inverted_index_json_crash`, `txn`, `ctas`, `cross_schema_recovery`, `database_drop_recovery`, `unable_to_create`, `test_progress.py`). Add the 2PC windows: after the prepared data batch, after the catalog commit record, and before the in-memory commit. Remove stale references (`tests/drivers/stress/faults.py`). | Every removed fault point is either back and covered by a test, or recorded as having no window left. | small–medium |
| P11 | #1208.4: the CREATE INDEX projection slot walk differs from main. | Add a test with mixed `(col, lower(body), …)` keys; fix the walk if the test fails. | The test passes. | small |
| P12 | iresearch `SetTickSource` is dead code. | Remove it. | Gone. | trivial |
| P13 | Schema USAGE isn't checked (a PostgreSQL gap, the same before the split): a role with `SELECT` on `s.t` but no `USAGE` on schema `s` can read `s.t`. PostgreSQL says `permission denied for schema s`. | In `server/auth/enforce.cpp`, require `USAGE` on the parent schema of every entry the statement resolved (`_props.resolved_entries`), skipping owners, superusers and `pg_catalog`/`information_schema`. `public` grants `USAGE` to PUBLIC by default, as in PostgreSQL. | The case above fails with 42501 and passes after `GRANT USAGE ON SCHEMA s`; rbac tests pass, with `GRANT USAGE` added where PostgreSQL needs it. | small |

## P7 design

Decisions taken:
- Definitions live only in the catalog, and the cluster attachment is storage-less.
- Commit is two-phase, with zero overhead for transactions without DDL.
- Everything uses duckdb's own formats in our files, and no code comes from the reverted fork PR #73 (its own record/serialization formats were the mistake).

Concretely:
- **Catalog WAL:** a plain duckdb `WriteAheadLog` with duckdb's native catalog records (CREATE_TABLE, ALTER_INFO, DROP_*, CREATE_INDEX definition, …). One new record, `USE_CATALOG(database oid)`, routes the records that follow to that database's catalog.
- **Compaction:** rewrite the log as the native create records of the live entries.
- **Database files:** hold only duckdb table data (`TableDataWriter`/`ReadTableData` format) and ART storage, keyed by table oid. A database is attached catalog-only first (file opened, nothing loaded); its rows are loaded after the catalog WAL has built its entries.

**Logs and checkpoints**
- **Catalog WAL.** One duckdb `WriteAheadLog` at `engine_catalog/catalog.wal`, owned by a storage-less cluster attachment (pre-split `__sdb_global`); `catalog.db` goes away. It records, in duckdb's own record types, every catalog change of every database (each record names its database by oid), plus roles, databases, permissions and comments. It also carries sequence values (P5), intent records (P6), and the 2PC commit decisions.
- **Catalog checkpoint.** A rewrite of the log from the live catalogs, parents before children, keeping pending removals (P6). It is abandoned when a commit lands meanwhile (`GetTotalWritten()` guard) and triggered by size (pre-split: 1 MiB and 2x live bytes). No oid horizon is written.
- **Database WAL.** Data records plus the physical side of every layout change, in duckdb's own record types: create storage (layout by `catalog_oid`), add, drop or retype a column, truncate, drop storage. At replay these apply to storage only, because the entry is already final from the catalog. The storage layout evolves through the history, and indexes follow it by `catalog_oid`, using the layout sync this PR added.
- **Database checkpoint.** Rows, ART storage and the physical column layout per table oid, with no definitions. The load takes definitions from the catalog through a fork seam like pre-split's `host_table_provider`.

**Where each statement writes**
- **Catalog only:** CREATE/DROP of views, macros, types, schemas, roles, databases, tokenizers and servers; GRANT/REVOKE; COMMENT; every RENAME; CREATE/DROP INDEX; SET/DROP NOT NULL; ADD/DROP CONSTRAINT. An ART is rebuilt after data replay, and an inverted index keeps its own directory and WAL cursor.
- **Catalog and database WAL (2PC):** CREATE TABLE and CTAS, DROP TABLE, TRUNCATE, ADD/DROP COLUMN, ALTER COLUMN TYPE and struct field changes, and any transaction that mixes DDL with DML in the same database.
- **Database WAL only:** INSERT, UPDATE, DELETE and COPY.

**Commit**
- **Data only:** the database WAL batch with its flush marker, exactly as today, group commit included. DML plus `nextval` is data-only too, because sequence records go to the catalog WAL outside the transaction (P5).
- **DDL only:** the catalog run in one catalog-WAL append and fsync (pre-split `EndCommittingCatalogRun`). Cluster and database DDL together is one run, which makes the mixed case atomic.
- **DDL and data, two-phase:**
  1. The database-WAL batch ends with `PREPARED(txid)` instead of the flush marker, and is synced.
  2. The catalog run plus `COMMIT(txid)` is appended and synced. This is the decision.
  3. The in-memory commit.

  Recovery reads the catalog WAL first, so every decision is known before any database WAL replays: a prepared batch is applied only if its txid committed. A database checkpoint can't run between steps 1 and 3; the commit holds the checkpoint lock in shared mode through step 3.
- **Multi-database data, in scope:** a transaction that writes data in several databases, with or without DDL. It uses the same protocol: a prepared batch in each written database and one catalog `COMMIT(txid)`, with no DDL needed. The `ModifyDatabase` single-writer rule is relaxed only for this path, and a single-database transaction is unchanged.
- **Alternative considered:** DDL prepared in the catalog first, with the data commit as the decision. The fork patch is smaller (only a txid marker in the data batch). But after a crash between the data commit and the catalog commit record, the DDL is in doubt until the database WAL is scanned, which defeats knowing the catalog ahead; it also can't commit several databases atomically. Rejected.

**Boot**
1. Replay the catalog WAL. All catalogs are complete, catalog-only, with no database file open; commit decisions are collected and intents resolved (P6).
2. Open the database files in parallel and attach rows to entries by table oid.
3. Bind every inverted index from its directory and WAL cursor.
4. Replay each database WAL. Prepared batches are filtered by the decisions, physical records apply to storage, and data streams into the bound inverted indexes (P8). ARTs load or rebuild as today.
5. Replay the search-table WALs.

**Fork patch surface** (moderate; each item its own commit):
- WAL: a `PREPARED(txid)` batch terminator, plus a replay decision callback in `WriteAheadLogReplayer::ReplayLog` (`src/storage/wal_replay.cpp`), in place of the `WAL_FLUSH` commit.
- Commit path: the two-phase variant in `DuckTransaction::Commit`/`DuckTransactionManager`, with the checkpoint lock held through the decision; `CatalogLog()`/`FlushCatalogLog()`; the `ModifyDatabase` carve-out; a two-pass `MetaTransaction::Commit`.
- `WALWriteState::WriteCatalogEntry` routing: catalog records to the catalog log; layout records to both logs.
- Replay applying layout records to storage only (the storage swap on a final entry), plus the checkpoint without definitions and the load seam that takes definitions from the catalog.

**Server:** the cluster attachment and the catalog WAL (open, replay, rewrite), catalog-only attach of databases at boot, and the decision table. `sdb_cluster`'s `catalog.db` code is removed.

**Implementation status (in progress, not committed):**
- Fork: record types `USE_CATALOG`, `WAL_PREPARED(txid)`, `COMMIT_PREPARED(txid)`; `USE_TABLE` carries the table oid. `WALWriteState` routes catalog records to the catalog log, and the physical side of table records (create, drop, and add/drop/rename/retype column and struct field) also to the database WAL. `MetaTransaction::Commit` runs the two-phase path when a transaction has catalog changes or writes several databases: each participant writes its batch ending in `WAL_PREPARED` and syncs, then the catalog run plus `COMMIT_PREPARED` and `WAL_FLUSH` is synced, then the in-memory commits. The single-writer rule is relaxed for catalog-log catalogs only.
- Fork: rows-only database checkpoints (column layout + rows + index storage keyed by index oid), deferred storage load (`AttachOptions::defer_storage_load`, `StorageManager::FinishLoad`), and `TableStorageLoad`, which replays the physical records under final entries using duckdb's own alter code, then installs the storage and rebuilds ARTs that were not in the checkpoint. Replay skips prepared batches whose txid has no commit decision.
- Fork: `WriteCatalogEntries` writes the live entries of a catalog as native create records (compaction).
- Server: storage-less `sdb_cluster`, catalog log at `engine_catalog/catalog.wal`, boot replays the catalog log, attaches databases catalog-only, then loads rows; the size-triggered rewrite runs after commits (1 MiB floor, 2x live bytes). `--missing_database` works on the catalog log.
- Smoke: clean restart and kill -9 recovery of tables, views, indexes, roles, CTAS, ADD/DROP/RENAME COLUMN, DROP, CREATE DATABASE, a role + table transaction and a two-database transaction. `ddl_rollback_durability.test` is un-muted.

## P8 status

**Implementation status (in progress, not committed):**
- One connection for the whole boot: `InitCatalog`'s. `FinishLoad` hands its context to `TableStorageLoad`, which creates none, and `InitInvertedIndexes` has no connection of its own.
- `TableStorageLoad` binds every external index (index types with `defer_implicit_bind`) against the final table entry: right after attaching it to the shadow storage at checkpoint load and at a positional CREATE_INDEX, and at install for definitions that neither loaded. So the WAL replay streams into bound inverted indexes. The layout sync carries them through the historical layouts. The fork's buffered-replay hooks for external indexes (`OnReplayRange`, `FinishReplay`, `ReplayRange::commit_offset`) are removed.
- An inverted index bound at load gets its replay session at bind time, with its tokenizers bound on the load connection there, so workers never bind. Per index, the replay thread evaluates the index expressions and queues a copy of the values and row ids. One task per index on its own `TaskExecutor` feeds them into the replay transaction in WAL order. `recovery_replay_depth` bounds the queue, and the replay thread help-executes when it is full, otherwise waits with `absl::Mutex::Await`. Entries already durable in the index are skipped by the entry offset the replay publishes.
- `InitInvertedIndexes` runs at the end of `InitCatalog`, while the boot connection is alive: one task per index on a `TaskExecutor`, `FinishReplay()` (drain, then commit at one tick) followed by that index's `Refresh()`, with no barrier between indexes. `search.start()` only starts the background loops (`StartInvertedIndexTasks`).
- Not restored: splitting a single index's `ROW_GROUP_DATA` range across workers (`external_range_replay`). Parallelism is across indexes and against the WAL read.
- Test: `recovery/inverted_index_parallel_replay.test`.

## P9 status

**Implementation status (in progress, not committed):**
- One connection: the committing session's. Pre-split gave every feed worker its own `duckdb::Connection` (`FeedPool::Bundle::expr_conn`) to bind tokenizers; that is not restored.
- A SQL or lambda tokenizer binds its expression in a transaction, and `TransactionContext::Commit` detaches the transaction before the indexes are fed. So `TransactionPreCommit`, while the transaction is still active, walks the tables this transaction appended to (`LocalStorage::GetTables`) and prepares, per inverted index, `clamp(rows / 64, 1, threads)` slots: an iresearch transaction plus an insert writer whose tokenizers are bound on the session's context. The slots live in the session's search transaction for the whole commit.
- At commit, the committing thread evaluates the index expressions once per chunk. Slice k of the chunk feeds slot k on a worker; the committing thread helps, and all slices join before the append returns. Deletes go to slot 0. `CommitSearch` commits every slot at the commit's tick.
- `IndexTokenizers::Acquire(field, context)` takes the binding context from the caller, so a long-lived index no longer binds with the context it was created on.
- Tests: `inverted_index_hnsw_ef_search`, `ts_dict_cartesian_multi`, `ts_dict_aggs` and `ts_dict_cartesian` are back to main's multi-segment results; `inverted_index_scan_metrics` scans 64 row groups instead of 4.

## P6 status

**Implementation status (in progress, not committed):**
- An `ARTIFACT(type, catalog oid, oid, paths)` record is appended before a database file, an inverted index directory or a search table directory is created, and at DROP DATABASE. Drops decided at commit (index, search table) are noted in memory and carried by the rewrite. Boot resolves artifacts: tombstones (`*.dropped`) are removed, then the paths of every object that is not live. Replayed artifacts claim their oids, so a crashed create's oid is never handed out again.
- The rewrite keeps a drop record until its removal is done (no path or tombstone left) and drops creation records of live objects.
- Tests: `inverted_index_create_crash` (no index files left after `crash_before_catalog_commit`, the crashed oid is not reused), `drop` (no index files left after `crash_on_drop`), `database_file_lifecycle` (one file per database after `crash_after_catalog_before_data` and after `crash_on_drop` on DROP DATABASE).

## P10 status

**Implementation status (in progress, not committed):** the catalog-log commit calls two hooks on the log's owner catalog: after every participant's batch is durable (`OnCatalogLogPrepared`) and after the decision is durable (`OnCatalogLogDecided`). The cluster catalog implements the fault points there.

| Fault point | Now |
|---|---|
| `crash_before_search_commit`, `crash_after_search_commit` | Renamed by the split to `crash_before_commit`/`crash_after_commit` at the same sites; their tests use the new names. |
| `pause_sst_sink_mid_copy` | Renamed to `pause_copy_from_mid_stream` at the same site; `test_progress.py` uses it. |
| `crash_sst_sink_after_ingest` | No window left: CTAS is duckdb's own operator now. Its window (rows staged, nothing decided) is `crash_before_commit` in `ctas.test`. |
| `crash_before_catalog_commit` | After the prepared data batches are durable, before the decision. The 2PC window "after the prepared data batch". Tests: `ctas`, `index_backfill`, `inverted_index_create_crash`, `catalog_crash_windows`, `view_index_create_crash_iceberg`. |
| `crash_after_catalog_before_data` | After the decision is durable, before any in-memory commit. The 2PC windows "after the catalog commit record" and "before the in-memory commit" are this one point: nothing runs between them. Tests: `catalog_crash_windows`, `database_file_lifecycle`, `dependency_edges_after_rename`. |
| `crash_on_drop` | The same point: the drop is decided and no artifact is removed yet. Tests: the twelve drop recovery tests and `database_file_lifecycle`. |
| `catalog_append_fails` | At the prepared point: the decision append fails and the transaction rolls back with a SQL error. Test: `catalog_crash_windows`. |
| `compact_inside_ddl`, `compact_inside_drop` | Force the catalog rewrite right after the commit, the earliest point a rewrite can run: a rewrite and a commit serialize on the catalog log lock. Tests: `catalog_checkpoint`, `catalog_crash_windows`, `dependency_edges_after_rename`, `alter_index_rename_recovery`. |
| `unable_to_create` | CREATE TABLE, SCHEMA and DATABASE fail after validation (CREATE DATABASE after its artifact record). Test: `unable_to_create`. |

Stale stress references are fixed: `tests/drivers/stress/faults.py` and `chaos.py` name `crash_before_commit`/`crash_after_commit`, and drop `crash_sst_sink_after_ingest` and `pause_ctas_mid_ingest`.

Found while restoring the tests: the rewrite dropped the 2PC decisions that database WALs still referenced, so a table created before a rewrite lost its rows at the next boot. `COMMIT_PREPARED` now carries its participants (database oid, WAL generation), and the rewrite keeps a decision until every participant has checkpointed past it or no longer exists.

Found, outside P10: CTAS no longer reports progress (`pg_stat_progress_create_table_as`), so `test_progress.py::test_create_table_as_progress` has nothing to pause at.

## P5 design (sequences)

- Port `SequenceCounter` (`server/catalog/sequence.{h,cpp}` @601aeb9bb):
  - an atomic counter shared by every version of the entry;
  - values durable before they are handed out, with the horizon logged 32×increment ahead (PostgreSQL's `SEQ_LOG_VALS`);
  - the horizon record appended outside the counter lock, so concurrent bumps share one fsync;
  - `CACHE` via a reserved range;
  - `setval` exact (drains in-flight appends, no log-ahead);
  - `Reserve(count)` in O(1) for batch `nextval`.
- The records are max-merge sequence-value records in the catalog WAL (P7), not per-transaction usage in the database WAL. `nextval` no longer makes the transaction a writer of the database WAL. The catalog rewrite writes the current horizons; a dropped sequence writes a dropped record.
- **Implementation status (in progress, not committed):** in the fork's `SequenceCatalogEntry`: native `SEQUENCE_VALUE` records in the catalog log, 32 values ahead, appended with the sequence lock released; setval exact (its record outranks every earlier horizon); `NextValues` is O(1) without CYCLE; the rewrite writes the durable horizon. A sequence created in the still-open transaction is logged by its create record at commit. CACHE (2026-09-29): pre-split's cache was internal (only generated-PK sequences used it, with 65536; user `CACHE` was rejected by the transformer on both pre-split and main). Now `CreateSequenceInfo::cache` (serialized, default 1) widens the log-ahead to max(32, n) values, as pre-split's shared cache refill did; a search table's generated-PK sequence gets pre-split's 65536 again; `CREATE SEQUENCE ... CACHE n` is accepted (new, a PEG rule; pg_dump emits it); `pg_sequence.seqcache`/`pg_sequences.cache_size` report it. ALTER SEQUENCE ... CACHE stays unsupported like every other ALTER SEQUENCE option but OWNED BY/RENAME (stock duckdb). `recovery/sequence_horizon.test` is back to the pre-split expectations.

## Perf (2026-09-29)

Binaries: branch perf build at 77c338d63, main = merge-base 8d74d6e29, pre-split = 601aeb9bb (all `perf` preset, scratchpad worktrees). The box has one consumer NVMe shared with everyone's builds: fsync-bound rows swung 4x between adjacent runs while loaded, so the numbers below are from a quiet window (load ~0.2) and repeat within a few percent.

| workload | pre-split | main | branch |
|---|---|---|---|
| seq: 20000 nextval in one query | 377k/s | 1.05M/s | 363k/s |
| seq_stmt: nextval per statement | 8810/s | 1160/s | 7250/s |
| ddl: CREATE+DROP TABLE pairs | 636/s | 1170/s | 645/s |
| nextval insert, 8 clients | 4846 | 5950 | 4842 |
| txn_small (10 rows, inverted), 8 clients | 2636 | 4668-5602 | 2985 |
| rollback (10 rows), 8 clients | 5838 | 18257 | 5687 |
| txn_large (100k rows, inverted) | ~165 ms | ~80 ms | ~165 ms |
| recovery, index replay (1 index, 300k delta) | failed (below) | 0.360 s | 0.327 s |
| recovery, table replay | - | 0.295 s | 0.296 s |

Against pre-split the branch is at parity or better everywhere. Against main (the split), four rows are slower, each a consequence of a design choice in this plan:
- Sequences (seq, rollback, nextval insert): values are durable before they are handed out, so a sequence syncs the catalog log once per 33 values and every caller waits for it. Main never syncs a value (it can hand one out twice after a crash). PostgreSQL logs 32 ahead too but flushes at commit, so it pays nothing on rollback. Open decision: keep "durable before hand-out" (the P5 ruling) or flush at commit.
- DDL: CREATE/DROP TABLE, column changes and index storage go to both logs, so they are 2PC participants with two sequential fdatasyncs (database WAL, then catalog WAL; strace-verified); main does one. Open decision.
- txn_large: the restored P9 feed slices every 2048-row chunk into clamp(rows/64, 1, threads) tasks; for short rows the per-chunk fork/join and thread wakeups (profile: kernel scheduling, native_write_msr) cost more than the work. Pre-split had the same 165 ms. A fix is slicing by work (text bytes) instead of 64-row slices, which changes the multi-segment layout the P9 acceptance pinned (hnsw_ef_search, ts_dict_*, scan_metrics). Open decision.
- seq_stmt against pre-split (0.82x) is not the sequence path: syncs are identical (10 per 330 statements, strace) and `SELECT 1` costs pre-split 43-51 us vs main/branch 53-67 us per statement, a split-era overhead present on main.
- The pre-split recovery comparison failed on the pre-split side: after its crash it never reached the pre-crash hit count within the bench's wait.

### Follow-ups done (2026-09-29, after the table above)

- Multi-database transactions with inverted indexes in both databases: `CommitSearch` committed every database's indexes from the first database's hook with that database's WAL cursor, so the other database's indexes skipped their tail or re-streamed rows after a crash (reproduced: 40102 docs for 20101 rows). Fixed per database (f7227351f); `recovery/cross_database_commit_recovery.test` covers the commit, both 2PC crash windows for data and for DDL in both databases.
- Feed (user direction: no split vectors, large batches, profile first): with one slot the feed was already faster than main on short rows (62 vs 75 ms per 100k); the 64-row slicing only cost wakeups and one segment per thread, which doubled the checkpoint reps (heavy rows 402-525 ms vs main 208-261). Now one slot per 16384 rows, whole vectors round-robin to per-slot queues (the replay queue, shared), joined by a new fork hook `BoundIndex::FinishAppend` inside the local-storage commit so feed errors still fail the COMMIT (e2e41d9fd, fork 9bf180921dc). Checkpoint reps dropped ~1.5-2x vs the 64-row feed and pre-split. Test: `sdb/pg/index/inverted_index_parallel_feed.test`; the P9-pinned multi-segment tests are back to main's expectations; scan_metrics sets threads before its inserts.
- The 100k-row BIGSERIAL insert gap is the sequence path, not the feed: 49 synchronous catalog-log fdatasyncs (one per 2048-row vector) on the branch and on pre-split, 1 on main (strace). So P5's "one reservation per INSERT…SELECT" is not met. Open decision with the user (Suggestion 1: horizon rides the committing transaction's data WAL record, once per 33 values; Suggestion 2: catalog log synced at commit).

### Sequences: PostgreSQL-like (user-approved design v4, `SEQUENCES_DESIGN.md`), committed and pushed

- Suggestion 1 was dropped in favour of the user's choice: sequence state lives only in the catalog log, ready for a Raft catalog log.
- Fork commit 4344bb7d868 ("perf: group-commit the catalog log's commit decision"): the two-phase commit's catalog decision is fsynced outside the log lock.
- Fork commit 7b3be97679f ("feat: sequences durable like PostgreSQL, with per-session caches").
- Root commit ed8c7645e, pointer bump 4216e2ce7. CI run 36604395437.
- Behaviour:
  - `nextval` never does I/O.
  - A commit whose values pass the durable reservation writes counter + 32 and waits with group commit.
  - CACHE is per session. A batch equals N per-row calls. `currval` is per session.
  - Search-table generated keys use 2^20-value blocks.
  - A crash skips at most 32 values plus cached ones. A value from an uncommitted transaction can come back.
- Found and fixed while building it:
  - A 2PC reservation self-deadlocked: `WriteSequenceValue` reads `GetData()` under the sequence lock.
  - Compaction could run between the unlocked decision fsync and the apply; it is now held off while commits are in flight.
  - A dropped database's deferred close could write the catalog log; a detached catalog now has none.
- Tests:
  - `any/pg/simple/sequence.test` passes on serenedb and on PostgreSQL 18.3.
  - `recovery/sequence_commit_recovery.test` and `recovery/sequence_horizon.test`.
  - Full runs: sdb 2516/0, recovery 290/291 (one docker/MinIO infrastructure flake, passes alone), gtests 1179/0.

### Found: DROP DATABASE, then a crash, and boot refused (a branch regression vs main)

P1's missing-file policy ran at every replay attach, including a `USE_CATALOG` for a database whose DROP comes later in the log (its files already removed): FATAL. The policy now runs after replay, only for databases still in the catalog. Test: `recovery/drop_database_restart.test`. The P1 driver test still passes.

### CI runs 36604395437 and 36607082287 (TSAN)

- 36604395437 (4216e2ce7): tsan failed at build time. The docs bootstrap reported 86 lock-order inversions: the commit path held the transaction's sequence lock around sequence calls. Fixed in fork 6dac779a5e2. Root f69f1fffd also guards the catalog-log `shared_ptr` against compaction replacing it.
- 36607082287 (41eddcf5f): asan, perf and validate-pg passed. tsan failed `sdb/pg/index/inverted_index_background_settings.test`: a rolled-back ALTER INDEX had compacted the index.
  - Cause: ALTER INDEX/TABLE SET wrote the options straight into the shared storage, and the background loops re-read them every second.
  - Fix 1f9566e11 (was a28d96e6c before the include fix): the options are deferred to the session transaction's commit, and a rollback just drops them. The test now waits inside the open transaction, for an index and for a search table. It fails on the 41eddcf5f binary at line 104 and passes on the fix, on both wire protocols.
  - dev: three duckdb unittest cases expected `currval` to survive a restart or a database copy. Per-session `currval` (the approved PG semantics) makes that an error. Fork 393ced38c7 aligns them with their `_not_supported` siblings.
- Work happens in the worktree `scratchpad/wt_final`, because the main directory is on the user's other branch.

### CI run 36620140957 (f4fb46f03) and the flaky tests

- dev passed. perf failed only on `sdb/pg/simple/rename_table_iceberg.test_slow`. The run was cancelled for 36625507503 (0e56a21a6).
- A scan of every failed dev/asan/tsan/perf job since 2026-09-23 (90 runs, all branches) found these recurring flakes:
  - `recovery/view_index_biglake_goshan_dr_hybrid_loop_iceberg.test_slow`: 9 of 59 dev runs, including on main. Cause: every IVF segment flush built a 3072×3072 random rotation (O(d³), 2–3 s in a dev build), although only SuperKMeans (splits of 512+ clusters) reads it. The one-row delta reindex after the restart therefore took 3–4 s, past the 5 s retry budget under CI load. Fix 2d38c8f09 builds the rotation lazily on the first SuperKMeans split: same matrix, same centroids. The delta now takes ~0.1 s and the test 5 s instead of 15–25 s. iresearch-tests 4427/0, index ivf/vector/iceberg sqllogic 110/0, recovery view-index/ivf/iceberg 29/0.
  - `sdb/pg/simple/rename_table_iceberg.test_slow`: 3 other branches plus this one. Cause: duckdb-iceberg's `DoTableRename` threw "already created by a different transaction" when the schema cache already held the new name, after the REST catalog had committed the rename. Any listing from another session caches the new name. Fixed in serenedb/duckdb-iceberg#23 (b6c0ea56, base v2026.09.02). The deterministic reproducer (bd9c929c3: a stale cache entry through a second attachment) fails on the old extension with the CI error and passes on the fix.
  - `sdb/pg/index/view_index_reindex_statements_iceberg.test_slow` ("REINDEX already in progress"): every failure predates #1224 (2026-09-28), which is in this branch. No action needed.
  - `thread_pool_test.test_max_threads_mt`: the known flake (user ruling).
  - `recovery/wal_index_recovery_concurrent_refresh.test`: still open. It failed once in ~90 scanned runs (77c338d63), never on other branches, and not in 177 local runs. Reading ruled out the serialized commit path (pin, tick, cursor and commit all under the WAL lock), VACUUM's refresh (same path) and the checkpoint refresh (only from `SerializeToDisk`).

### Paused 2026-09-29 20:35 (user request)

- Root head 0e56a21a6 is pushed. CI run 36625507503 was left running.
- The worktree `scratchpad/wt_final` is detached at 0e56a21a6, so the main directory can check the branch out again.
- `wt_final/build_perf` is configured and about 85% built (resume with `ninja -C build_perf serened`). Then copy the binary to `bins/serened-final` and run `scratchpad/run_perf_final.sh` in a quiet window.
- `wal_index_recovery_concurrent_refresh.test` (the earlier CI flake): 177 local runs of the final dev binary, 160 of them at load 130-140, all passed. Not reproduced.

### CI run 36574687195 (7b5e9266d)

dev 288/289: `recovery/view_index_biglake_goshan_dr_hybrid_loop_iceberg.test_slow` saw 1154 instead of 1155 when its 5 s retry budget ran out, so the view reindex had not picked up the new iceberg row. The test took 29.6 s there vs 15.1 s in the previous run, and passes locally in 6 s. The reindex path (its own iresearch commit) is untouched by the last commits. Watching the next run.

## Open: CI flake `recovery/wal_index_recovery_concurrent_refresh.test`

Run 36527977949 (dev): after the crash the table had 6000 rows and `cc_idx` 5750, i.e. one committed 250-row INSERT is missing from the index. The server log shows index recovery in 77 us: nothing was replayed, so the last stamped cursor covered every commit while the flushed segments lacked one. Not reproduced in 60 sequential + 72 parallel local runs, nor in a crash-free 80k-row run of the same workload (index == table). Ruled out by reading: ticks are handed out under the WAL lock (WAL order); `PrepareFlush` waits on the flush context's `pending` for every pinned slot, and slots are pinned before `Next()`; replay skips only entries below the stamped end offset; the replay queue hand-off is atomic. Still open.

## Additional items (done in this PR)

- **Refresh and compaction settings.** The whole matrix was checked: new and existing inverted indexes and search tables × enable, disable, change, RESET, session defaults, rolled-back ALTER, restart. Fixed:
  - the refresh and reindex loops ran one more tick after being disabled;
  - the compaction coordinator compacted once more after being disabled;
  - a changed interval was seen only after the stretched delay; the loops now re-read it every second of the wait;
  - a rolled-back ALTER INDEX left its options on the storage;
  - the settings raced with ALTER; they are now atomics.

  Test: `sdb/pg/index/inverted_index_background_settings.test`.
- **MERGE `RETURNING merge_action` with generated columns.** The bug hits STORED generated columns only; VIRTUAL, released duckdb 1.5.3 and nightly duckdb v2.0.0-alpha (`09eb7f7004`) are correct. STORED is fork-only, so there was nothing upstream to cherry-pick. Fixed in the fork: `BindReturning` bound STORED columns as derived while the returned chunk carries them. Test: `sdb/pg/dml/merge.test`.

## OK / not planned (user rulings)

- duckdb error texts.
- Plain (ART) partial indexes: a colleague is fixing them; issue #1265.
- `force_checkpoint()` on the current database.
- duckdb's concurrency model stays: DDL, DML and queries all run under MVCC with optimistic concurrency. Nothing waits on a lock; a conflict fails the loser at the conflicting write or at COMMIT (40001). Example, tested in both orders: DROP TABLE against a concurrent CREATE INDEX, where whichever commits first wins (commit-time checks `DependencyManager::VerifyExistence` and `VerifyCommitDrop`).
- DROP COLUMN in front of an ART/PK/UNIQUE index column.
- The orphan boot sweep: a kludge; P6 makes orphans impossible instead.
- The oid horizon (a persisted counter high-water mark): never restored. The counter is re-derived from live objects at boot, and P6 makes that safe. Search-WAL records can't leak into a table that reuses an id, because a new shard seeds its committed tick at the WAL's current tick.
- Oids are 64-bit; there is no 2^32 limit. They are never synthesized (no bit-packed or position-derived oids), only allocated from the counter.
- WAL layout for this PR: per database, a duckdb WAL and a search-table WAL; per instance, one catalog WAL (P7). Merging the search-table WAL into the duckdb WAL is a separate task.
- `CREATE SERVER` rejecting unknown options: an improvement.
- Making dependents follow a rename (references by oid): follow-up #1267. The small part of P4 is in this PR.
