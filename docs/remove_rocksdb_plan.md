# Removing RocksDB: design and plan

Branch: `mbkkt/remove-rocksdb`. Goal: delete RocksDB entirely. Regular tables
become DuckDB native tables (ART for secondary/unique indexes), inverted
indexes key documents by duckdb rowid instead of an encoded PK blob, the
catalog moves into DuckDB storage. No backwards compatibility. This is the
interim engine so people can use serenedb now; the long-term engine (own WAL,
ducklake-like inline data, iresearch-files-as-parquet, doc_id-native lookups)
replaces it later.

Fixed constraints (user decisions):

- **One DuckDB database file for everything** (catalog + data of all pg
  databases). DuckDB allows a transaction to write only one attached database
  (`meta_transaction.cpp:259` "single transaction can only write to a single
  attached database"), so transactional DDL and multi-table writes in one
  transaction force a single file. Layout: `<datadir>/engine_duckdb/store.db`
  (+ `store.db.wal`).
- **The lookup function is source-generic.** `ATTACH 'x.db' (TYPE duckdb)`,
  `CREATE VIEW v AS SELECT * FROM x.t`, `CREATE INDEX ... ON v` must work the
  same way parquet/iceberg view indexes work today. Native tables and attached
  duckdb tables are the 1-number (rowid) case; glob/iceberg stay the 2-number
  (file_idx, row) case.

## 1. Target architecture

Today the DuckDB instance is in-memory (`libs/basics/duckdb_engine.cpp:78`,
`DuckDB(nullptr, ...)`); each pg database is `ATTACH '<ObjectId>' (TYPE
serenedb)` → `SereneDBCatalog : duckdb::Catalog` (a facade — no duckdb
storage), and `SereneDBTransactionManager` is a no-op shell; all real state is
RocksDB (3 CFs: row data / catalog definitions / sequence counters) plus
iresearch directories under `engine_search/`.

Target:

```
duckdb instance (in-memory, unchanged)
├── __sdb_store        ← hidden ATTACH of engine_duckdb/store.db (plain DuckCatalog,
│   │                     native single-file storage, own WAL + checkpoints)
│   ├── sdb_catalog    ← catalog blob table (one row per catalog object)
│   ├── sdb_seq        ← sequence counters (one row per sequence)
│   └── t_<ObjectId>   ← one native table per serenedb table (+ its ART indexes)
├── postgres, <user dbs>  ← per-pg-database serenedb facades, unchanged role:
│                            name resolution, DML planning hooks, pg semantics
└── <user ATTACHes>    ← external duckdb files, parquet/iceberg views, etc.
```

The facades stay because they carry all pg-compat behavior (search paths,
DML planning, error shimming, entry cache). They delegate *storage* to native
`DuckTableEntry`/`DataTable` objects living in `__sdb_store`, named by
ObjectId (globally unique, so one flat schema suffices). Because facade
catalogs never register with `MetaTransaction::ModifyDatabase` (the no-op
transaction manager never did), every write — any pg database, any table,
plus catalog rows — lands in the single writable `__sdb_store`, satisfying
duckdb's one-writable-db rule while permitting cross-pg-database transactions
and (later) transactional DDL.

Rejected layouts:

- *Per-pg-database files / per-tablespace files*: breaks single-transaction
  writes across databases and catalog+data atomicity (`meta_transaction.cpp:259`).
- *`SereneDBCatalog : DuckCatalog` hybrid* (duckdb auto-instantiates
  `SingleFileStorageManager` for `IsDuckCatalog()` storage-extension catalogs,
  `attached_database.cpp:157-159`): would put our entries under duckdb's
  CatalogSet MVCC while LocalCatalog keeps its own snapshot-swap MVCC — two
  MVCC models describing the same objects, plus per-pg-database files anyway.
  The facade+hidden-store split keeps exactly one source of truth for names
  (LocalCatalog) and one for storage (native catalog of `__sdb_store`, which
  we never rename/alter — only create/drop by ObjectId).

## 2. Catalog persistence

Decision: **option 1** — serialized catalog blobs in a native table.

Framing for option 2 ("duckdb-native catalog"): we already use duckdb's
native catalog for everything duckdb understands — the hidden `t_<ObjectId>`
tables, their columns, and their ART indexes are real `DuckCatalog` entries,
checkpointed and WAL-replayed by duckdb itself (phase B depends on this). The
question is only whether the *semantic* layer (pg names/databases/schemas,
roles, tokenizers, inverted-index defs, shards, tombstones, view/function/
type defs) also becomes duckdb entries instead of blob rows. Three blockers:

1. **No extensible persisted entry kinds.** DuckCatalog checkpoints a fixed
   `CatalogType` set; roles, tokenizers, inverted-index entries (per-kind
   field ids, analyzers, expression entries), shard stats, and tombstones
   have no slot. They would be blobs regardless — so option 2 ends as a
   permanent split-brain: half the objects native, half blobs, every load
   and every drop-resume walking two representations.
2. **Two MVCC models over the same objects.** duckdb CatalogSet entries are
   versioned per transaction; LocalCatalog is snapshot-clone-and-swap, and
   *every* consumer (binder hooks, pg_catalog views, search, drop tasks, the
   dependency graph) reads LocalCatalog snapshots. Either LocalCatalog stays
   and becomes a derived cache of duckdb's catalog — then duckdb's
   transactional DDL visibility buys nothing, because readers consult the
   cache anyway — or all consumers rebase onto duckdb catalog reads, which
   is a rewrite of the catalog subsystem, not an interim step.
3. **Identity model.** Our identity is ObjectId (iresearch directories,
   field ids, pg oids all key off it); duckdb entries are name-keyed with
   catalog-local oids, and rename/alter create new entry versions through
   duckdb's ALTER paths. Blob rows keep identity-by-id with the name as
   payload — rename stays a one-row update (today: Put on the same key).

Concrete walk-through of the "native" end state, using one sdb table T with
an inverted index: duckdb can natively hold T's columns/types/PK (the
`CREATE TABLE` info). It cannot hold: T's ObjectId (entries are name-keyed;
our oid/iresearch-dir/field-id identity), `sdb_indexonly` column modes, the
generated-pk sequence link, PG-text check constraints, the table shard, the
inverted index (tokenizers, per-entry per-kind field ids, expression
entries, scorer options), roles/ACLs on T, or T's drop tombstone. So T's
definition splits: half in the native entry, half in blob rows that must
reference the native entry by name-or-oid mapping — a mapping that itself
needs storing. Boot then reads duckdb's catalog through CatalogSet C++
iteration *and* the blob rows, joins them, and validates consistency; every
ALTER keeps both halves in sync through two different mutation APIs. The
blob design keeps T's definition in ONE self-describing row; the native
`t_<id>` entry is a dumb storage container found by id. What native dedup
would save is storing the column list twice — that's all.

A second reading of "native": PG-style *relational* catalog tables with real
columns (`sdb_tables(name, schema_id, ...)`, `sdb_columns(...)`) instead of
blobs. Rejected for the same economy: the BinarySerializer/pfr round-trip
for every object kind already exists and is tested
(`catalog_persistence_test.cpp`); a relational schema means hand-written
column mappings per kind plus schema migrations per added field, and the
polymorphic payloads (inverted-index entries, expression trees, embedded
`CreateInfo`) end up in blob columns anyway. PG needs a relational catalog
because clients query `pg_catalog` against storage; our `pg_catalog` views
are generated from LocalCatalog memory, and the storage-side catalog is
read once at boot.

What option 2 would buy: transactional DDL visibility (deferred anyway, and
the hard part — LocalCatalog MVCC — is needed in both options), catalog
introspection via `duckdb_tables()` (a debug nicety; a blob-decoding table
function gives the same), and duckdb dependency tracking (we have our own
graph with cross-object deps duckdb can't express). None of it pays for the
blockers. The door stays open: kinds whose payload is already a serialized
duckdb `CreateInfo` (views/functions/types) could migrate to real entries
later without touching the rest.

One positive argument *for* blobs-in-the-same-file: CREATE TABLE becomes one
duckdb transaction containing {catalog blob insert + native CREATE TABLE of
`t_<id>`} — single commit, single WAL record stream, crash-atomic. That is
strictly better than today, where index def + shard def are two non-atomic
batches.

### How duckdb persists its own catalog, and the custom-WAL-records option

A third option (user question): don't store our catalog in a table at all —
reproduce duckdb's *mechanism* for our own layout. DuckDB's own catalog
persistence is: logical WAL records per DDL (`WALType::CREATE_TABLE`,
`DROP_TABLE`, `ALTER_INFO`, …, `wal_type.hpp:15-45`), sequence advances as
tiny delta records (`SEQUENCE_VALUE`, replayed via max-merge `ReplayValue`),
and at checkpoint a full serialization of the live catalog into the data
file's metadata blocks (`checkpoint_manager.cpp` walks all schemas/entries),
after which the WAL truncates. Load = read snapshot + replay records.

The decisive observation: **a duckdb table already implements exactly this
pattern.** Writing a blob row inside a transaction emits a logical
WAL record at commit (`INSERT_TUPLE`/`DELETE_TUPLE` against `sdb_catalog`);
at checkpoint the WAL truncates and the table's current contents are written
into the data file — a snapshot of current catalog state; boot = checkpoint
+ WAL replay. Option 1 is not an alternative to "WAL deltas + checkpoint
snapshot" — it is that design, rented through the table abstraction, paying
a small constant overhead (tuple encoding bytes per record, dead-row churn
compacted at checkpoint) for zero patches.

Hand-rolling our own records (true option 3) would require, with vendored
duckdb: a new WAL record kind + replay routing to a registered handler
(`WALType` is a closed enum — verified, no extension type); emission wired
into the transaction commit path (`wal_write_state.cpp`) so a custom record
is crash-atomic with the native `t_<id>` entry created in the same
transaction; and a home for the checkpoint snapshot — either a table (back
to option 1) or custom metadata blocks patched into
`SingleFileCheckpointWriter/Reader`. All three patches sit in the
most correctness-critical recovery code and would need re-porting on every
duckdb rebase (we track latest). What it buys at DDL rate: smaller WAL
records and no boot-time table scan — both negligible.

The one genuinely attractive piece is sequences: a `(seq_id, +N)` delta
record (= rocksdb's merge-op reborn, = duckdb's own `SEQUENCE_VALUE`) avoids
the read-modify-write row churn entirely. But the dominant refill cost is
the WAL fsync, identical in both shapes, and durable-step reservation (§8)
already amortizes it. Decision: **table for phase A; custom WAL records kept
as a targeted escape hatch** — if profiling ever shows `sdb_seq` churn
mattering, a `SEQUENCE_VALUE`-style record for our counters is a small,
local mix-in, and the engine API hides the swap from all callers. Same
applies later to the own-WAL project: catalog deltas could migrate there
without touching `catalog/` code.

Today's engine API is small and clean (`rocksdb_engine_catalog.cpp:1072-1125`):
`CreateDefinition` / `Write(CatalogWriteContext)` (atomic multi-object batch) /
`Drop*` / `VisitDefinitions(parent_id, type)` over key
`[parent_id BE][ObjectType 1B][id BE]` → BinarySerializer blob. The values are
already duckdb-BinarySerializer-encoded end to end (`object.h:198-219`,
boost::pfr `WriteTuple` payloads, embedded `CreateInfo::Serialize` for
views/functions/types) — the serialization layer ports unchanged.

Replacement: a plain table, written **read-free** (delete+insert by key),
**no PK/index** (ART is fully in-memory in vendored duckdb, the
buffer-managed/compact ART PRs are unmerged upstream, and an index would
cost maintenance + checkpoint serialization for nothing):

```sql
CREATE TABLE sdb_catalog (parent_id UBIGINT, type UTINYINT, id UBIGINT, def BLOB);
```

- **Writes**: Put = `DELETE WHERE (parent_id,type,id)` + `INSERT` in the
  same transaction (today's Put-overwrite is already full-value; no
  read-modify-write of `def` exists anywhere). `DropEntry`/`DropRange` =
  `DELETE WHERE`. The DELETE's filtered scan costs tens of µs on a
  single-row-group table while the commit's WAL fsync costs ~1ms — the scan
  is noise under the fsync at any realistic catalog size (a 10k-object
  catalog still fits one row group).
- **Garbage is duckdb's problem, not ours**: dead versions from
  delete+insert are ordinary MVCC garbage, vacuumed automatically at
  checkpoint. No compaction subsystem, no garbage accounting, no
  crash-mid-compact recovery cases — this is the decisive simplicity win
  over the append-only-log variant (below).
- **Boot**: one full scan `ORDER BY parent_id, type, id` into the ordered
  map — the old BE-key ordering, so `VisitDefinitions(parent, type)` keeps
  the load contract (tombstone-type-first, parents before shards —
  `object.h:43-77`) intact. A duplicate-key `SDB_ENSURE` during the scan is
  the free corruption tripwire.
- **Concurrency**: DDL is already fully serialized by LocalCatalog's single
  `absl::Mutex` (`local_catalog.h:167-170`, clone→mutate→persist→swap);
  each DDL runs its delete+insert+commit on the internal connection while
  holding the mutex, and cross-object ordering follows from the snapshot
  swap happening after persist. DDL latency floor becomes one WAL fsync
  (~ms, same as PG) — irrelevant at DDL frequency. Sequence-refill writes
  to `sdb_seq` run outside the DDL mutex on their own connection;
  per-`Sequence` in-process locks serialize same-counter writers, so no
  optimistic conflicts arise.
- **Rowid hygiene**: no rowid caching ever — with zero indexes the
  checkpoint vacuum may renumber rowids (§5), and a stale cached rowid
  would delete the wrong row. All access by key filter.

*Recorded alternative — append-only log* (rejected for now as the more
complex shape, revisit only if DDL-time scans ever measurably matter, i.e.
pathological catalog sizes): writes become pure appends (no scan at all),
boot does a last-write-wins reduce, and a compaction pass
`{DELETE WHERE seq <= S; re-append live snapshot}` runs counter-triggered
at boot-end / background task / clean shutdown under the DDL mutex. Erase
records need no extra `op` column: reuse the `type` column — a reserved
erase type value erases by `(parent_id, id)` (ids are globally unique, the
target's own type is unneeded), and a second reserved range-erase value
(or `id = 0`) erases a whole parent subtree, replacing `DropRange`. Plus a
`seq UBIGINT` ordering column (local monotonic, `max(seq)+1` at boot; gaps
from aborted txns harmless). Sequences would become `(seq, id, counter)`
appends with last-per-id-wins — the merge-op pattern as inserts.
- `Write(CatalogWriteContext)` → one duckdb transaction appending all
  records of the DDL (or the user's transaction, see §7). This is *more*
  atomic than today (index def + shard def are currently two non-atomic
  writes, `local_catalog.cpp:1743-1755`).
- Tombstone/resume machinery (`create_with_tombstone`, DropTask rescheduling)
  survives as-is (the Tombstone *object*, `ObjectType=1`, is ordinary put/
  erase records like any other object — unrelated to op=erase log records).
  Still needed because iresearch directory removal is a non-transactional
  filesystem operation.

Bootstrap order (replaces `engine.start → recovery.start → catalog.start`):

1. duckdb instance up (unchanged, in `main()`).
2. `DatabasePathFeature` resolves datadir → internal `ATTACH
   'engine_duckdb/store.db' AS __sdb_store` — duckdb replays its WAL here.
   The replay-recorder (§6) captures appended/deleted rowids per table.
3. Read `sdb_catalog` → build LocalCatalog snapshot (load order via
   ORDER BY as today via enum order). The server tick is not persisted at
   all — it is recovered as the max over loaded ids (every deserialized
   Object bumps it, `object.cpp:38`). No format-version check (§2.1).
4. ATTACH the per-pg-database facades (`catalog.cpp:704-717`, unchanged).
5. Search engine start → index catch-up from the replay-recorder (§6).

No circularity: step 2 needs no sdb catalog (plain DuckCatalog attach); facade
attaches in step 4 open no files.

`EnsureSystemDatabase` (first boot: `postgres` + `public`) becomes "create
store.db + seed tables + rows" — same logic, new substrate.

### 2.1 phase A detailed design (catalog first, rocksdb keeps row data)

Transitional state after phase A (implemented): rocksdb survives for row
data, secondary indexes, and the WAL for search recovery — opened with the
**Default CF only**. Deleted from the engine: the Definitions and Sequences
CFs, `RocksDBSettingsManager` wholesale (persisted
tick/released_tick/last_sync were ArangoDB heritage with no live consumer —
the recovery-helper framework it fed has zero registered helpers;
released_tick stays as a purely in-memory WAL-prune floor),
`StartupVersionCheck` (deleted outright, not ported),
`EnsureSystemDatabase`, the shard-stats sync, all orphaned catalog methods,
and the sequences merge operator. Catalog and sequence counters live in
`engine_duckdb/store.db`. One invariant restored explicitly: the iresearch
tick protocol requires seqno ≥ 1, and catalog writes no longer seed a fresh
rocksdb — the engine now mints the first tick itself (one empty-key delete
at first boot). Recovery suite: 106/106 after these changes.

**Tables** (all in `__sdb_store.main`, created on first boot):

```sql
CREATE TABLE sdb_catalog (parent_id UBIGINT, type UTINYINT, id UBIGINT, def BLOB);
CREATE TABLE sdb_seq     (id UBIGINT, counter UBIGINT);
```

Two tables only (no `sdb_meta` — user decision). No PKs/indexes anywhere;
all writes are delete+insert by key filter, runtime reads are filtered
scans (§2: the scan is noise under the per-commit WAL fsync; MVCC garbage
is vacuumed by duckdb checkpoints; never cache rowids — no-index tables may
be renumbered at checkpoint vacuum). **Boot load is in-pipeline** (user
decision; implemented): registered table in-out functions consume the scan
inside the query — `SELECT * FROM sdb_init_catalog((SELECT parent_id,
type, id, def FROM sdb_catalog))` and `sdb_init_sequences((SELECT id,
counter FROM sdb_seq))` — chunks arrive verbatim from the scan (no result
materialization, no Flatten, `UnifiedVectorFormat` consumers, single
thread via default `GlobalTableFunctionState::MaxThreads()`), building the
(parent,type)-keyed boot map + sequence-counter map that serve
`VisitBoot` range iteration (hierarchical call order in
`catalog.cpp:596-635` unchanged) and `Sequence` counter loads until
`ReleaseBootState()`. No ORDER BY: the map is order-insensitive and a sort
would materialize in the sink anyway. The two ex-`sdb_meta` records are not
ported at all:
- **format version → dropped** (user decision; the old `StartupVersionCheck`
  is an ArangoDB-heritage datadir-version marker, and the no-backwards-compat
  stance makes it moot). The duckdb file carries duckdb's own storage
  version (incompatible files are refused with a clear duckdb error), and
  blob-decode failures hit serializer `SDB_ENSURE`s. If compatibility ever
  starts mattering, a version row is a one-row addition.
- **server tick → not persisted** (ArangoDB artifact; recovery already
  effectively works this way today). The tick is fully recoverable at boot:
  every deserialized Object bumps it (`object.cpp:38`),
  `BumpTickServerForEntryIds` covers expression field ids, and any id that
  can still be referenced by a durable artifact is reachable from the scan —
  live objects trivially; mid-drop objects via their Tombstone rows (which
  exist precisely until the DropTask finishes); fully dropped ids are
  unreferenced and safe to reuse. This deletes the 2.5s settings-sync duty
  outright. The rocksdb-domain values (`released_tick`, `last_sync`) stay
  on the rocksdb side for the transition — a settings key in the still-alive
  Default CF, written by the existing background thread — and die with
  rocksdb in phase B.

**New class `CatalogStore` in `server/catalog/store/store.{h,cpp}`** —
placement and naming pre-conform to the issue #796 restructure (glossary:
*store* = catalog persistence; backends live in `src/catalog/store/`,
"rocksdb dies in place; engine-file next"; the word `duckdb` is banned from
first-party identifiers/filenames/dirs, so no `DuckDB*` names anywhere in
this work). It mirrors the rocksdb engine's persistence API so `catalog/`
callers keep their call shapes. Open naming question for the datadir:
`engine_duckdb/` (earlier working name) violates the #796 banned-word rule
— `engine_store/` proposed instead; one-line change either way.

| today (`RocksDBEngineCatalog`) | phase A (`StoreEngine`) |
|---|---|
| `CreateDefinition(parent, obj)` / Change = Put | delete+insert by key, internal txn |
| `Write(fill)` + `CatalogWriteContext` {PutDefinition, PutSequence, DropDefinition, DropSequence, WriteTombstone} | same context API, all ops in **one** duckdb txn |
| `VisitDefinitions(parent, type, cb)` | boot-time scan → ordered map; Visit = in-memory range iteration |
| `DropEntry(id)` / `DropRange(parent)` | `DELETE WHERE` |
| `EnsureSystemDatabase()` | first-boot seed (postgres db + public schema rows) |
| `StartupVersionCheck` | **deleted** — duckdb's own storage version guards the file; no-compat stance |
| settings manager sync (2.5s bg) | **deleted** — tick recovered from boot scan; released_tick/last_sync transitionally in rocksdb Default CF |
| `SyncTableShard` stats Puts | **dropped in phase A** (user decision): the TableShard def row is still written at create/drop (catalog structure), but the periodic stats re-serialization goes away immediately — stats live in memory only and reset on restart; any test that depended on persisted counts is adjusted when the suites run. In phase B stats become native (exact committed row counts + column stats in the table checkpoint), and `TableShard::_num_rows` + `UpdateNumRows` commit deltas are deleted too; `TableShard` survives only as the table-lock holder |
| Sequences CF Merge/Get/Put | `sdb_seq` counter row delete+insert (refill-rate, own connection) |

**Connections**: catalog writes go through one dedicated internal
`duckdb::Connection` guarded by a mutex (DDL is already serialized under
LocalCatalog's `_mutex`, so this adds no real contention); sequence refills
use a second dedicated connection (they happen during DML, must not wait on
DDL) — per-`Sequence` in-process locking already serializes refills of the
same counter, so no write-write conflicts arise. Prepared statements first
(catalog ops are DDL-rate; parser cost irrelevant); direct `DataTable` API is
a later optimization if ever needed.

**Bootstrap order (phase A)**: DatabasePathFeature → internal ATTACH of
store.db (duckdb WAL replay happens here; on first boot, create + seed) →
rocksdb engine start (Default CF only; recovery manager reads
last_sync/released_tick from its Default-CF settings key) → catalog load
from `sdb_catalog` (ORDER BY replaces enum-key ordering; duplicate-key
`SDB_ENSURE` tripwire; tick = max over loaded ids) →
facade attaches → search start (unchanged, still rocksdb-WAL replay).
Shutdown: search/engine stop as today → DETACH store.db
(checkpoint_on_shutdown truncates its WAL).

**Cross-engine atomicity seam** (catalog in duckdb, data in rocksdb until
phase B) — safe because nothing relies on catalog+data sharing a WriteBatch
even today: data ops run in their own rocksdb txn; catalog/DDL atomicity is
within the catalog write (now one duckdb txn); cross-engine consistency is
already handled by the tombstone/resume protocol (CTAS and CREATE INDEX use
`create_with_tombstone` → build → remove-tombstone; DROP uses tombstone →
async DropTask). Crash between catalog commit and rocksdb data work resumes
exactly as today. Catalog durability actually *improves*: duckdb fsyncs its
WAL at commit vs today's unsynced Put + 100ms `SyncWAL` window (only the
ServerTick write is fsync'd today).

**Acceptance**: the ~46 catalog/DDL/sequence recovery tests + full sqllogic;
no EXPLAIN churn in phase A (scan operators untouched).

## 3. Table data path

One native table per serenedb table, created in `__sdb_store` at CREATE TABLE
time with the real column types, real `PRIMARY KEY` clause, and ART indexes
for unique/secondary indexes.

- **Scans**: `SereneDBTableEntry::GetScanFunction` returns duckdb's native
  `TableScanFunction` bound to the hidden table's `DataTable`. This replaces
  the whole RBO pushdown layer (`optimizer/rocksdb_plan.cpp`,
  `rocksdb_filter.cpp`, ~1600 lines, plus 6 scan table functions) with
  duckdb's native filter/projection pushdown, zonemap pruning, ART index
  scans, parallel scanning, and real statistics. Virtual columns (rowid) come
  for free.
- **DML**: keep our planner hooks (`PlanInsert/PlanDelete/PlanUpdate/...`) and
  physical operators as thin wrappers that (a) perform the native operation
  against the hidden `DataTable` (LocalStorage append / delete by rowid /
  update), (b) feed the inverted-index sinks (§5), (c) shim constraint errors
  to PG text (`_pkey`, `Key (cols)=(vals) already exists.` — duckdb's
  constraint message differs; the shim logic in `duckdb_constraint_verify.*`
  survives in reduced form). Native appends handle CHECK/NOT NULL/PK
  enforcement; the app-side conflict prober (`HandleWriteConflicts`,
  snapshot `txn->Get` probing) is deleted.
- **ON CONFLICT**: THROW = native unique enforcement; DO NOTHING / REPLACE map
  onto duckdb's native ON CONFLICT machinery (DO NOTHING / DO UPDATE SET all
  columns). Session default `sdb_write_conflict_policy` is applied at plan
  time. ON CONFLICT DO UPDATE (unsupported today) likely falls out for free —
  verify, don't promise.
- **PK-less tables**: native rowid replaces the sequence-backed hidden
  generated PK (`kGeneratedPKId` + `ReserveWriteUnsafe`) for duckdb-native
  tables. The generated-PK machinery itself is **kept in the codebase** — the
  eventually-consistent search-table engine needs it for its first steps
  (decision Q2, §11); native tables just stop using it. Semantic change:
  rowid values become positional and can be reused after deletes (§4 pins
  them only for indexed tables).
- **TRUNCATE**: native delete-all (or drop+recreate of the hidden table) in
  the transaction, plus the existing iresearch `TruncateBegin/Commit` anchored
  to the commit ordinal (§6). Today's non-transactional `DeleteRange` hack
  dies.
- **Bulk path**: `SereneDBPhysicalSSTInsert` (per-column SstFileWriter +
  `IngestExternalFile`, WAL-bypassing) is deleted; COPY/CTAS/INSERT…SELECT use
  the native batch-append path (duckdb already optimizes large appends and can
  skip WAL via `auto_checkpoint_skip_wal_threshold` + direct checkpoint, which
  the checkpoint-flush coupling in §6 makes index-safe automatically).
- **VACUUM**: `compact_rocksdb` verb dies; iresearch verbs
  (refresh/compact/sync_stats) survive unchanged; optionally add a
  `CHECKPOINT` mapping (`SereneDBTransactionManager::Checkpoint` is a no-op
  today and should stop being one).
- **kIndexOnly columns** keep zero base-table storage; they exist only in the
  iresearch columnstore. Their durability rides §6 (no more PutLogData WAL
  markers; the recorder + re-evaluation covers them, same as today's replay
  re-evaluates expressions).

## 4. Secondary indexes → ART

ART (vendored) capabilities vs today's RocksDB SK (full matrix in exploration
notes): same or better on unique enforcement (incl. compound + multi-NULL),
point lookups, single-column ranges, IN probes, type coverage
(decimal/uuid/...), and it adds expression indexes. Three regressions, all
accepted for the interim engine:

1. **No compound-ART index scans** ("FIXME: No support for index scans on
   compound ARTs", `table_scan.cpp:650-663`) — multi-column indexes still
   enforce uniqueness but multi-column range/prefix queries fall back to
   (zonemap-pruned, parallel) filtered scans.
2. **No IS NULL acceleration** (ART doesn't index NULLs).
3. **Index-scan cost gate**: `MAX(index_scan_max_count=2048, 0.1% of table)` —
   big range predicates use filtered full scans. Both knobs are settings;
   benchmark and tune rather than patch first.

`SELECT * FROM <secondary_index_name>` (index-as-table for SK,
`duckdb_entry_cache.cpp:366`) has no ART equivalent → **dropped** for
secondary indexes (decision Q3, §11); tests switch to
`FROM <table_name>`. An ordered ART scan could resurrect it later — separate
concern, not this branch. Inverted-index-as-table is iresearch-side and
survives.

Deleted wholesale: `duckdb_secondary_sink_writer.h`, the SK third of
`duckdb_index_utils.cpp`, 3 SK scan functions, SK key codecs, the
`EnsureIndexesTransactions` rocksdb branch. Also fixes a live bug class:
DROP INDEX today leaks SK keys (no Default-CF range delete exists for it).

## 5. Inverted index: PK becomes rowid; generic lookup

The plumbing is already PK-spec-shaped; this is mostly a new enum value plus
deletion of the rocksdb cases.

- **New `PkSpec::DuckDBRowId`** (1×int64). Existing
  `FileRowNumber/FileOffset` (1×int64) and
  `FileIndexPlusRowNumber/FileIndexPlusOffset` (2×int64) stay untouched.
  `RocksDBExplicitPK`/`RocksDBGeneratedRowId` are deleted.
- **Write side** (`search_sink_writer.cpp` `MaybeEmitPk`): the per-doc PK is
  still stored twice — as a verbatim term under `kPKFieldId` (DELETE-from-index
  via `SearchRemoveFilter`, needs ≤1 alive doc per PK) and as a columnstore
  column (doc_id → PK materialization). Both switch from var-length blob to
  fixed 8 bytes (big-endian for the term so term ordering stays sane; the
  column can hold raw int64 — `skip_validity` already set). The sink interface
  `row_keys: span<const string_view>` becomes a numeric span.
- **Read side**: `SegmentPkColumn` fetch + `AppendPrimaryKey` decode already
  produce `PrimaryKeyI64`; `MakeIndexSource` grows a
  `DuckDBRowIdIndexSource` that batches sorted rowids into a new
  **`duckdb_rowid_lookup` table function** using `DataTable::Fetch`
  (`data_table.cpp:491`) through the existing patched
  `TableFunctionInput::pk_lookups`/`pk_output_positions` channel — the exact
  pattern `MakeParquetLookupTableFunction` et al. use (`view_fast_path.cpp`).
  `RocksDBIndexSource`, `ViewRocksDBIndexSource`, the 16-byte prefix-gap
  trick, and `MultiGetContext` are deleted.
- **Generic over sources**: the same `DuckDBRowId` spec + lookup TF binds to
  (a) our hidden native tables, (b) tables in user-attached duckdb files
  (CREATE VIEW over `ATTACH ... TYPE duckdb` + CREATE INDEX — `ResolveIndexRelation`
  already accepts views; the view fast-path re-bind machinery already exists
  for file readers). `DataTable::Fetch` silently skips non-resolving/invisible
  rowids — identical to today's "deleted pk → row skipped" semantics.
- **Visibility semantics improve via duckdb MVCC**: the lookup TF executes
  inside the query's own duck transaction, so it sees the statement's MVCC
  snapshot — strictly better than today, where materialization uses a *fresh*
  `db->GetSnapshot()` (`duckdb_scan_base.cpp:90`) that can be newer than the
  statement's own snapshot. Index doc_ids are still stale by refresh-interval
  relative to that snapshot: rows deleted before the snapshot are skipped
  (as today), and a rowid reused before the snapshot materializes the new row
  for a doc matched on old values — same anomaly class as today's user-reused
  PKs; accepted (decision Q2, §11). What matters is that every row visible in
  the snapshot is always materializable, which `DataTable::Fetch` with the
  query transaction guarantees. No long-pinned duck transaction is needed
  (pinning one per InvertedIndexSnapshot would block checkpoints and hold
  undo buffers for the whole refresh interval — not worth it).
- **DELETE/UPDATE**: our DELETE operator reads the rowid virtual column from
  the scan and feeds `SearchRemoveFilter` with 8-byte terms; UPDATE =
  remove(old rowid) + insert(new rowid) — native UPDATE of any ART/inverted
  indexed column is delete+insert with a fresh rowid anyway
  (`table_catalog_entry.cpp:307-325`).

### Rowid stability — the load-bearing detail

Verified in vendored source (`row_group_collection.cpp`, vacuum-inside-
checkpoint): `can_change_row_ids = !has_indexes || can_rebuild_indexes`.
Surviving rows are renumbered at checkpoint **only when the table has no
entries in `TableIndexList`** (the rebuild path additionally requires the
experimental `vacuum_rebuild_indexes` setting + all-ART, i.e. off for us).
Therefore every table with an inverted index **must keep at least one entry in
`TableIndexList`**. Tables with a PK or any secondary index have one (the
ART); for inverted-index-only tables we register a stub custom index type
(`IndexTypeSet::RegisterIndexType`) whose presence pins rowids; its
`AppendToIndexes`/`RemoveFromIndexes` callbacks double as the WAL-replay
recorder (§6). Residual reuse: trailing fully-deleted row groups are still
dropped (`total_rows` shrinks) even with indexes, so tail rowids can be reused
— covered by the same eventual-consistency argument above, and converged by
recovery's delete-then-reinsert idempotency (§6).

## 6. Crash consistency and index recovery

Today's contract: iresearch lags rocksdb; flush subscriptions pin the rocksdb
WAL; startup replays `GetUpdatesSince(per-shard payload tick)`. Tick ≡ rocksdb
seqno everywhere. All of it loses its substrate.

Options considered:

- (R2) make iresearch commits synchronous with every transaction commit —
  correct, kills ingest performance and the async design; rejected.
- (R3) build the own WAL now — that's the 3-month project; out of scope.
- **(R1, chosen) checkpoint-coupled flush + WAL-replay re-feed**:

Invariant: *anything not yet durable in iresearch segments is still in
store.db's WAL.* Enforced by hooking checkpoint (StorageExtension
`OnCheckpointStart`, or the stub index's checkpoint/serialize callback):
before duckdb truncates its WAL, force `CommitWait()` on all dirty shards.
Async refresh commits keep happening between checkpoints exactly as today
(visibility + durability); the hook only makes the WAL-truncation boundary
safe. Bulk appends that skip the WAL trigger a checkpoint, hence the hook,
hence are index-safe too.

Recovery: ATTACH replays store.db's WAL; the stub-index callbacks (or, if
custom indexes turn out not to be driven during replay, a small patch in
`wal_replay.cpp` — we patch duckdb freely) record per table: appended rowid
ranges + deleted rowids. After the catalog and shards load, a catch-up pass
re-feeds each affected shard from those rowids by scanning current table
state (re-evaluating indexed expressions through a private connection —
exactly what `wal_recovery.cpp` does today), using today's replay idempotency
pattern: `DeleteRowImpl(pk)` for every touched pk, then re-insert
(last-write-wins per rowid). No tick comparison needed: replayed-WAL contents
are by construction a superset of what segments may be missing. Checkpoint is
blocked until catch-up completes, so a crash during catch-up just replays
again.

Consequences:

- `FlushFeature`, flush subscriptions, `released_tick`, WAL-prune machinery:
  deleted (their only purpose was pinning the rocksdb WAL for replay).
- Segment payload `[tick:8][iceberg_snapshot_id:8]` stays structurally;
  the tick field becomes a per-shard monotonic commit ordinal (kept for
  ordering/debug; no longer a recovery cursor). Iceberg pinning unchanged.
- `query::Transaction::Commit` ordering survives with the rocksdb leg
  replaced: duckdb commit (via the existing `SereneDBClientState` hooks,
  internal-15) then iresearch `Commit(ordinal)`. The empty-Delete
  seqno-padding hacks die.
- The 48 `wal_index_recovery_*` tests keep their scenario matrix; failpoints
  move: `crash_before/after_rocksdb_commit` → `crash_before/after_duckdb_commit`,
  plus a block-checkpoint failpoint; `SearchCommitTask::commitUnsafe` freeze
  works unchanged.
- Power-loss durability window: today = `SyncWAL` every 100ms; duckdb fsyncs
  the WAL at every commit flush (`write_ahead_log.cpp:567-579`) — stronger,
  slower for tiny autocommit writes; benchmark (§10) and consider a relaxed
  group-commit knob later if it hurts.

### Checkpoint policy (who calls it, when)

One database file = one checkpoint domain; catalog tables, data tables, and
ART indexes all checkpoint together — no separate policy for the catalog.
A checkpoint here is heavier than vanilla duckdb: it first forces dirty
iresearch shards to flush (the §6 invariant) and it runs vacuum/row-group
merging. So it must not fire implicitly inline on an arbitrary user commit.

Callers, in priority order:

1. **Background checkpoint task (primary)** — the slot vacated by
   `RocksDBBackgroundThread`. Periodically checks store.db WAL size and time
   since last checkpoint; issues CHECKPOINT on an internal connection when
   `wal_size > --server_checkpoint_wal_size` (default in the 16–64 MiB range)
   or age exceeds a max (bounds restart replay time). Because background
   refresh tasks already flush iresearch every `refresh_interval` (default
   1s), the forced flush at checkpoint is normally near-free; only
   `refresh_interval=0` shards pay a real flush here.
2. **duckdb's own commit-time auto-checkpoint as safety net** — keep it but
   set `checkpoint_threshold` a healthy multiple above the background task's
   trigger so it fires only if the background task falls behind; under
   concurrent load it degrades to a CONCURRENT_CHECKPOINT (no delete
   vacuuming, rowids untouched), which is fine.
3. **Bulk loads** — appends above `auto_checkpoint_skip_wal_threshold`
   bypass the WAL and checkpoint directly at commit; the inline cost rides a
   connection that already ran a long bulk operation. Nothing to do.
4. **Startup** — after the §6 catch-up completes (and shards flushed), issue
   a checkpoint to truncate the replayed WAL; otherwise every restart
   re-replays a monotonically growing WAL. Checkpoint stays blocked *until*
   catch-up completes.
5. **Shutdown** — `checkpoint_on_shutdown` (default true) on detach: clean
   shutdown ⇒ empty WAL ⇒ fast next boot.
6. **Manual** — `CHECKPOINT` SQL: `SereneDBTransactionManager::Checkpoint`
   (a no-op today) forwards to checkpointing `__sdb_store` regardless of
   which facade catalog the connection targets. Optionally also a
   `VACUUM (CHECKPOINT)` verb alongside the iresearch verbs.

Note on vacuum quality: FULL checkpoints (which reclaim deleted rows) require
no concurrent write transactions; under sustained load most background
checkpoints will be concurrent ones. That only delays dead-row reclaim, never
correctness; if it ever matters, a quiet-period FULL checkpoint can be forced
from the background task.

Needs prototype verification (top risk): whether custom-index callbacks fire
during WAL replay and with what context; whether `OnCheckpointStart` can
safely block/wait; provisional→final rowid mapping for execution-time
tokenization (insert sinks tokenize at execution time, but final rowids are
assigned at commit when LocalStorage merges — the per-batch mapping is
`final = table_total_rows_at_commit + local_offset`, so the PK column/term can
be emitted at commit with a constant delta; verify ON CONFLICT row-skips keep
both sides aligned).

## 7. Transactions

The rocksdb transaction/snapshot dies; the duck transaction on `__sdb_store`
*is* the transaction. `SereneDBTransactionManager` stays a shell for facades;
`pg_comm_task` stops calling `EnsureRocksDBTransaction/Snapshot`.

Semantic changes (open question §11 Q1):

- **Concurrency control**: today = pessimistic per-row locks
  (`GetKeyLock`) + app-side conflict probing; duckdb = optimistic, concurrent
  writers to the same row get a transaction-conflict abort instead of
  blocking. PG users expect blocking; interim engine accepts abort-and-retry
  semantics with PG-shimmed error codes (40001 serialization_failure).
- **Isolation**: duckdb gives snapshot isolation per transaction.
  REPEATABLE_READ maps cleanly. READ_COMMITTED's per-statement snapshot
  refresh inside an explicit transaction has no duckdb equivalent
  (one duck transaction = one snapshot); inside explicit transactions
  READ_COMMITTED silently behaves as snapshot isolation (stricter, allowed by
  SQL standard). Autocommit statements are unaffected (txn per statement).
- **Read-your-own-writes**: native LocalStorage gives uncommitted-read within
  the transaction for free (today's `sdb_read_your_own_writes` toggle +
  iterate-through-txn machinery simplifies; search scans inside RYOW still
  throw NOT_IMPLEMENTED as today).
- **DDL**: stays statement-committed (as today), but each DDL becomes
  internally atomic (one duckdb txn for all catalog rows + native
  create/drop). True transactional DDL (rollback of CREATE TABLE inside
  BEGIN) requires MVCC on LocalCatalog's in-memory snapshot — the single-file
  layout enables it; explicitly deferred, not part of this branch.
- **DDL inside explicit transactions — REVISED after issue #790**: psycopg
  (default mode) wraps every statement, DDL included, in implicit
  transactions, so a hard error by default would break stock Python
  drivers. Shipped behavior: default keeps statement-commit DDL but emits a
  NOTICE ("DDL is not transactional: the statement commits immediately and
  is not undone by ROLLBACK"); `sdb_strict_ddl = true` upgrades it to a
  hard error (SQLSTATE 25001). Guard lives in `PendingQueryEnsured`
  (covers simple + extended protocol); DDL = CREATE/DROP/ALTER/ATTACH/
  DETACH statement types. The original analysis below stands — until
  LocalCatalog gets MVCC, no interim
  can make DDL-in-txn honest, so the safest behavior for real users is to
  refuse it loudly: a DDL statement inside an explicit multi-statement
  transaction throws a clear serenedb error ("DDL is not yet supported
  inside multi-statement transactions"). Autocommit DDL (the overwhelming
  case) is unaffected. An escape-hatch GUC
  (`sdb_allow_nontransactional_ddl`) can restore today's silent
  statement-commit semantics for migration tools that insist on wrapping
  DDL in BEGIN/COMMIT — opt-in, documented as non-transactional.
  Why not today's silent behavior: a failed migration leaves half-applied
  DDL after ROLLBACK — the worst failure mode, invisible until much later.
  Why not "catalog in a separate duckdb database so mixing throws":
  the throw only materializes if catalog writes ride the *user's*
  transaction; a pure-DDL transaction then doesn't throw — its catalog rows
  roll back in duckdb while LocalCatalog already swapped at statement end,
  which is precisely the divergence to avoid; the error is duckdb's
  internal "single transaction can only write to a single attached
  database" (meaningless to a pg user) and fires on whichever statement
  touches the *second* database (order-dependent: DDL-first makes the
  innocent INSERT fail); and it permanently forfeits the single-file prize
  — {catalog rows + native `t_<id>` entry} in one atomic transaction, and
  the path to true transactional DDL later. Statement-scope abort safety is
  already guaranteed regardless: `LocalCatalog::Apply` mutates a clone and
  swaps only after successful persistence, so any throw during the write
  (including a hypothetical ModifyDatabase conflict) discards the clone —
  in-memory state cannot diverge at statement scope. The only unsolvable
  case without MVCC is the *deferred* abort (ROLLBACK after a succeeded
  DDL statement), and that is exactly what the error forbids. When
  LocalCatalog MVCC lands, the error is lifted and catalog writes move into
  the user's transaction — PG semantics, no architectural change.

## 8. Sequences, settings, tick

- **Sequences**: counter row per sequence in `sdb_seq`; `RefillCache`/
  `AdvanceCounter` (cache=65536 for serial columns) becomes a short internal
  transaction rewriting the counter row (delete+insert). Crash burns the
  reserved range — same semantics as today's merge-op. Refills are already
  serialized per sequence by the in-process `Sequence` object, so the
  counter row never sees concurrent writers (no optimistic-conflict
  retries).
  Why not duckdb native sequences (verified in vendored source):
  duckdb has **no `setval`** at all (PG requires it; only WAL-replay-internal
  `ReplayValue` exists, `sequence_catalog_entry.cpp:84`); `NextValue` is
  one-value-per-call under the entry lock — no batch `Reserve(count)`;
  counter durability is journaled at the *calling transaction's* commit
  (`wal_write_state.cpp:300`), entangling nextval durability with user-txn
  outcome, where PG semantics want it independent. All PG semantics
  (cycle/min/max/cache, OWNED BY, pg_sequences) already live in our
  `Sequence` object — its only rocksdb dependency is the persisted uint64 +
  merge-op increment, so swapping just the counter's home is the minimal,
  semantics-preserving change. Mirroring every sdb sequence into duckdb
  catalog entries would add DDL double-bookkeeping for negative gain.
  Performance: the hot path (in-memory `fetch_add` within the cached range)
  is untouched in every design. The refill changes from an unsynced rocksdb
  Merge (~µs, 100ms durability window) to a fsync'd duckdb micro-txn (~ms).
  Amortized over cache=65536 (serial PKs) that is invisible; for cache=1
  user sequences it would be a per-call fsync — fixed by **durable-step
  reservation** (PG's own pattern: PG WAL-logs every 32 values): persist
  `counter += max(cache, kDurableStep)` per refill while handing out values
  one at a time; crash burns at most the step. With that, ours ≈ duckdb
  native amortized, with PG semantics intact. And for PK-less *native*
  tables the comparison disappears entirely — they use rowid, no sequence
  at all (the machinery stays for the search-table engine).
- **Server tick** (ObjectId allocator): not persisted (§2.1) — recovered at
  boot as the max over all ids reachable from the catalog scan (`Object`
  ctor bump, `object.cpp:38`; Tombstone rows cover mid-drop objects until
  the DropTask finishes, so reusable ids are exactly the unreferenced ones).
- **Format version**: dropped (§2.1) — `StartupVersionCheck` is not ported;
  duckdb's own storage version guards the file.
- **TableShard `num_rows` stats**: native tables know their counts; the
  periodic `SyncTableShard` re-serialization dies, `pg_class.reltuples`-style
  reads pull from native storage info.

## 9. Runtime surface

Deleted with no replacement: `RocksDBOptionFeature`, `RocksDBEngineCatalog`,
`RocksDBRecoveryManager`, `RocksDBSyncThread`, `RocksDBBackgroundThread`,
`RocksDBSettingsManager`, both listeners, WAL prune/purge machinery,
`FlushFeature`, dead replication/dump managers, `engine_rocksdb` dir,
rocksdb third_party dependency (build-time win), `BENCH_ROCKSDB` ifdefs,
rocksdb version in the server version string (`libs/rest/version.cpp`).

Replaced:

- Startup: `recovery.start()` slot becomes the §6 catch-up; shutdown
  `engine.unprepare()` slot becomes detach/checkpoint of `__sdb_store`
  (`checkpoint_on_shutdown` default true covers it).
- Size functions (`pg_relation_size` family) → duckdb storage info / block
  counts per hidden table.
- `pg_am` rows: `rocksdb` → `duckdb`; `secondary` stays (ART-backed); two test
  fixtures pin the names.
- Memory budgeting: rocksdb block cache disappears; set duckdb
  `memory_limit` (nothing is configured today in `duckdb_engine.cpp`) from a
  server flag — without this, native tables shift the whole page-cache budget
  unbounded.
- Health check disk-free thresholds: relocate if kept.
- Isolation-after-read guard (`HasRocksDBRead`, `any/pg/txn/variables.test`):
  re-decide against duck transaction state.

## 10. Benchmarks (find where new < old)

Build main (rocksdb) vs branch on the same machine, psql-driven + micro.
Baseline binary: `build_perf/bin/serened` built from main
(`aa1595ddb`, static + jemalloc, RelWithDebInfo), copied to
`~/baselines/serened-main-aa1595ddb` so it survives the branch diverging.

| case | suspicion |
|---|---|
| single-row INSERT autocommit (1 client) | duckdb fsyncs WAL per commit vs rocksdb 100ms group sync — likely worse |
| concurrent small writers, same table | optimistic aborts vs row-lock blocking — worse under contention |
| point UPDATE/DELETE heavy churn | delete-bitmap + checkpoint rewrite amplification vs LSM — unclear |
| PK point lookup / small IN | ART + Fetch vs MultiGet — likely better |
| PK/SK range scan large | index-scan gate falls back to full scan — check gate tuning |
| multi-column SK range scan | no compound ART scans — worse, mitigated by zonemaps |
| IS NULL filtered scan | no NULL in ART — worse |
| full scan / OLAP aggregates | columnar vs per-column-KV zip — much better |
| COPY bulk load | native batch append vs SST sort+ingest — likely better |
| search query, non-covering materialization | rowid Fetch vs rocksdb MultiGet — likely better |
| nextval-heavy insert | counter-row txn vs merge-op — likely fine (cache=65536) |
| sustained ingest + checkpoint pauses | checkpoint-coupled iresearch flush adds latency — measure |
| recovery time (large WAL replay + catch-up) | new mechanism — measure |

Each "worse" row gets a follow-up issue with the tuning lever (gate settings,
group commit, checkpoint threshold) rather than blocking the merge.

## 11. Decision record (user, 2026-06-12)

1. **Concurrency semantics**: accepted — duckdb optimistic conflicts
   (abort+retry, PG error 40001) replace pessimistic row-lock blocking;
   READ_COMMITTED-inside-explicit-txn becomes snapshot isolation. Less
   expected for pg users, better than nothing.
2. **Materialization visibility**: stale doc_id materializing the *new* row
   is acceptable; what must hold is that visible rows are always
   materializable. Use duckdb MVCC — lookups run in the query's own duck
   transaction (§5), which is strictly more consistent than today's
   fresh-snapshot behavior.
3. **PK-less tables**: native rowid for duckdb-native tables, but the
   generated-PK machinery is NOT removed — the search-table engine will need
   it for its first steps.
4. **`SELECT * FROM <secondary_index_name>`**: dropped; tests replace it with
   `FROM <table_name>`. Ordered ART scan = possible later, separate concern.
5. **PK term-in-dictionary mess** (dual PK storage): known, stays for now,
   improved later (post-rocksdb-removal index work).
6. **Phasing**: catalog-first order approved (§12); all phases on this one
   branch, not separate PRs. Baseline binary for benchmarks:
   `build_perf/bin/serened` built from main (static, self-contained), copied
   to `~/baselines/serened-main-aa1595ddb` — see §10.
7. **Catalog storage shape**: plain table + delete+insert writes, no
   PK/index (ART in-memory footprint vetoed), scan-per-DDL accepted (noise
   under the per-commit fsync); append-only-log variant and custom duckdb
   WAL records recorded as escape hatches only (§2).
8. **DDL inside explicit transactions throws** a clear serenedb error
   (safest interim until LocalCatalog MVCC); escape-hatch GUC restores
   today's statement-commit semantics for migration tools. Separate-catalog-
   database idea rejected (§7).
9. **No `sdb_meta`**: format version check dropped entirely (ArangoDB
   artifact; duckdb's own storage version guards the file), server tick not
   persisted (recovered from the boot scan; ArangoDB artifact),
   released_tick/last_sync stay rocksdb-side until phase B (§2.1).
10. **Shard stats not persisted** from phase A on; native table stats take
   over in phase B (§2.1).

## 12. Implementation phases

All phases land on this branch (`mbkkt/remove-rocksdb`); each phase brings
the full sqllogic + recovery suites back to green before the next starts:

- **phase A — store.db substrate + catalog**: hidden ATTACH, `sdb_catalog`/
  `sdb_seq`, port engine catalog API, sequences, bootstrap order,
  shutdown checkpoint; rocksdb keeps row data only (Definitions/Sequences CFs
  emptied). Acceptance: the ~46 catalog/DDL recovery tests.
- **phase B — the flip**, split into sub-phases (user decision, design
  iteration in progress before implementation):
  - **B1 — lookup functions first**: `PkSpec::DuckDBRowId` + the
    rowid-lookup table function, validated in isolation via
    `ATTACH 'x.db'` + `CREATE VIEW` over the attached table +
    `CREATE INDEX` on the view, with dedicated tests — no storage flip yet.
  - **B2 — native tables**: our tables become real `DuckTableEntry`s in
    `__sdb_store`; DML delegation; the DML/DDL surface questions below
    must be answered first.
  - **B3 — wire indexes**: ART secondary indexes + inverted-index
    integration (rowid PK, checkpoint-coupled recovery from §6).
  - **B4 — suites green** (EXPLAIN re-records, recovery-test failpoint
    migration).
  **Design iteration results (research complete, see decisions below):**
  - **B1 primary shape: `read_duckdb`, not ATTACH.** The vendored tree ships
    `read_duckdb` (`src/function/table/read_duckdb.cpp`) — a
    MultiFileReader-based TF like `read_parquet`: glob support, `rowid` +
    `row_number` virtual columns (:506, :520), `table_name` option. So the
    parquet-symmetric design works verbatim: register `read_duckdb` in the
    view-fast-path registry; `CREATE VIEW v AS SELECT * FROM
    read_duckdb('x.db', table_name='t')` + CREATE INDEX → `PkSpec::DuckDBRowId`
    (single file) / file_idx+rowid (glob). The path lives in the view body
    exactly like parquet paths — no ATTACH, no re-attach-after-restart
    problem, no need to store paths in the index. Views over
    catalog-ATTACHed tables (`attached.t`) become a secondary branch (the
    fast-path classifier currently falls through at `view_fast_path.cpp:287`
    and silently builds a useless synthetic-counter index — that gets a
    proper classification either way). External-file contract: no snapshot
    pin exists for duckdb files (weaker than iceberg) — documented as
    "valid until the external file compacts".
  - **B3 verdict: inverted index as a native BoundIndex is feasible — GO.**
    Nothing in the contract blocks it: a non-unique custom index receives
    all inserts exactly once at commit (serialized, final rowids), deletes
    at commit with values re-fetched, UPDATE arrives as delete+insert
    automatically, ON CONFLICT never consults it, and **WAL replay feeds
    custom indexes via UnboundIndex buffering + ApplyBufferedReplays at
    first bind** — the §6 recovery design falls out of the architecture
    instead of being built next to it. The existing sink core
    (`SearchSinkInsertBaseImpl`) becomes the BoundIndex body; all five of
    its inputs are bind-time-capturable, except indexed-expression
    execution which needs a private `Connection` (the exact
    `wal_recovery.cpp` precedent). Design items: (i) commit tick moves from
    rocksdb seqno to duckdb commit id via the existing SereneDBClientState
    pre/post-commit hooks; (ii) `SerializeToDisk` must fabricate a valid
    `IndexStorageInfo` (free-form options map carries our payload);
    (iii) `create_instance` registration required before first
    post-recovery DML. Open risks: commit-time tokenization is serialized
    per index (today it runs in parallel pipeline threads at execution) —
    ingest-throughput benchmark needed early; partial-failure compensation
    calls `Delete` on just-appended docs (remove-by-rowid handles it).
  - **`sdb_indexonly` is REMOVED outright (user decision)** — the
    eventual-consistent iresearch-only tables are the real answer to that
    use case; interim column duplication is acceptable. (It was also the
    one structural casualty of the BoundIndex route: `RemoveFromIndexes`
    force-fetches indexed columns from the table, impossible for
    index-only data.) Scope: `ColumnStoreMode::kIndexOnly` +
    `ApplyColumnModes` option, `indexonly_marker` PutLogData plumbing, the
    scan-rejection and SK-restriction paths, WAL-recovery marker replay,
    `indexonly_*` tests. Independent of B1 — lands as its own early
    commit.
  - **Column naming, final**: real names stay (revisited `c_<id>` at user
    request and rejected): CHECK/DEFAULT/generated expressions are stored
    as parsed trees referencing columns BY NAME and would need two-way
    rewriting under id-names, and native error messages would leak
    `c_<id>` requiring a per-table dictionary in the PG error shim — both
    permanent translation layers, versus a five-line metadata-only native
    RENAME mirror in the same transaction.
  - **Column naming (user decision)**: native columns keep their REAL names
    — duckdb RENAME COLUMN is pure catalog metadata (storage is positional,
    `DuckTableEntry::RenameColumn` shares storage), so facade renames
    mirror a metadata-only native rename in the same store.db transaction.
    Table naming, final (user proposal, adopted — supersedes the earlier
    `t_<ObjectId>` idea): the native table name is the full pg path as ONE
    quoted identifier — `"<database>.<schema>.<table>"` — in store.db's
    default schema. Uniqueness = the full path is unique by definition;
    no mangling convention (dots are just characters; the name is
    write-only — identity stays ObjectId, nothing ever parses it back);
    EXPLAIN output and native error messages read correctly with near-zero
    shim work; standalone store.db is browsable. Cost: RENAME TABLE
    mirrors a metadata-only native rename in the same transaction (the
    only rename supported today; future schema/database renames would
    cascade, still metadata-only). Why NOT per-pg-database files instead:
    each attached db is its own durability domain (own WAL/txn-manager,
    sequential per-db commits, no coordinated commit) — lifting the
    single-writable-db rule safely requires cross-database 2PC (prepare
    records + coordinator log + in-doubt recovery), a distributed-commit
    subsystem out of scope for the interim engine; and catalog+data in
    different files would make every DDL a cross-db transaction.
  - **Catalog metadata stays authoritative, not deduplicated** (user
    question, resolved): names/types/defaults could be derived from the
    native entry at boot, but the minimum residue (Column::Id identity,
    id↔position mapping, sdb attributes) must stay catalog-side anyway,
    and the upcoming iresearch-native tables have no underlying duckdb
    table — deriving would fork `catalog::Table` into two shapes. So
    TableData remains the single uniform source of truth across engines;
    the native schema is a derived, enforced duplicate, validated against
    TableData at boot with a cheap mismatch assert (catches ALTER sync
    bugs). Revisit dedup only if the redundancy hurts.
  - **B1 test matrix** must cover: single-file `read_duckdb('x.db',
    table_name=...)`, **glob** `read_duckdb('dir/*.db', ...)` (two-number
    file_idx+rowid spec, same machinery as parquet globs), and the
    attached-catalog-view branch — each with covered queries,
    materializing queries, and the degradation paths (missing file,
    externally compacted file, detached database).
  - **UNIQUE/FK flip (user decision)**: native enforcement is embraced
    (today both are parsed and silently dropped); needs dedicated tests.
  - **Triggers (user decision)**: deferred — separate DDL surface; the
    delegation design keeps statement-level AFTER triggers an almost-free
    later addition (BEFORE/INSTEAD OF parse but never fire — never expose).
  - **RETURNING is silently broken today** (INSERT returns garbage,
    UPDATE/DELETE internal error, zero coverage) — native operators fix it;
    add tests in B2.
  - `sdb_write_conflict_policy=replace` re-lands as a plan-time ON
    CONFLICT-on-PK rewrite (duckdb has no blind-replace); PG error-text
    shims move to a wrapper over native constraint errors; SST bulk path is
    replaced by native batch insert (the PK-sort prepass dies).

  Original direction notes (kept for context):
  - **No stubs anywhere in the DML surface.** Native tables get the full
    duckdb behavior by genuine delegation — ON CONFLICT DO UPDATE,
    RETURNING, MERGE INTO come from duckdb's own operators ("same as a
    duckdb table, plus whatever the inverted index needs on top").
  - **Preferred B3 architecture: inverted index as a first-class duckdb
    index type** — a real BoundIndex in the table's TableIndexList, fed by
    duckdb's own PhysicalInsert/Delete/Update through
    AppendToIndexes/RemoveFromIndexes, instead of serenedb's custom DML
    operators feeding search sinks. Search QUERIES keep going through
    iresearch_scan + the optimizer. Feasibility research running (what the
    index hooks receive, append timing vs commit, WAL-replay routing,
    expression/tokenizer context capture); fall back to custom operators
    only if something genuinely prevents it. Side benefit if it works:
    rowid pinning, delete notification, and WAL-replay re-feeding all come
    from the same registration — unifying with the §6 recovery design.
  - **ALTER TABLE: both worlds must work** — everything serenedb supports
    today keeps working, and duckdb-native forms (ADD/DROP COLUMN at least
    for columns without indexes) become available rather than rejected.
  - **B1 external-source semantics = the iceberg/parquet-glob contract**:
    an index over a view of an attached duckdb table behaves like the
    existing file-view indexes (index pins what it indexed; source
    mutation has the same caveats). The duckdb-table lookup is implemented
    as one more source kind alongside parquet/iceberg BEFORE any storage
    flip.
- **phase B details (original notes)**: native tables + DML delegation + ART secondary indexes
  + `PkSpec::DuckDBRowId` + `duckdb_rowid_lookup` TF + checkpoint-coupled
  recovery (§6) + failpoint migration. The big one; rocksdb row-data paths and
  SK machinery deleted within it. Includes EXPLAIN re-records (53 files) and
  the wal_index_recovery rework (48 files).
- **phase C — extinction**: remove rocksdb_engine_catalog/, third_party rocksdb,
  runtime features, version string, pg_am, gtest deletions
  (column_serializer, key_builder), comment sweeps.
- **phase D — benchmarks**: §10 matrix, tuning, follow-up issues.

Prototype-first items inside phase B (de-risk before bulk work): custom-index
callbacks during WAL replay; OnCheckpointStart blocking; provisional→final
rowid delta at commit; native TableScanFunction bound through a facade entry.

## 13. Deletion inventory (very rough)

`server/rocksdb_engine_catalog/` (whole dir), `rocksdb_plan.cpp` +
`rocksdb_filter.cpp` (~1600 lines), 6 rocksdb scan TFs, reader/writer/key
codecs (`duckdb_rocksdb_{reader,writer}`, `key_utils`, `duckdb_key_builder`,
`primary_key`), SK sink writer + scans, `index_source_rocksdb`,
`index_source_view_rocksdb`, SST insert path, `indexonly_marker` PutLogData
plumbing, `wal_recovery.cpp` (rewritten around the recorder),
`flush_feature`, 2 gtest files (~1.5k lines), rocksdb from third_party and
all link lines. 129 files reference rocksdb today; the large majority are
deletions, not rewrites.
