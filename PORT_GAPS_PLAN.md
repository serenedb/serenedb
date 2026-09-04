# Closing the catalog-port gaps

What the V2 port lost, and how each piece comes back. Companion to `PLAN.md`,
which stays the governing brief -- this document only says *how* and *in what
order*, never what the design should be.

## Evidence base, and what it does not cover

- `sdb/pg/index`: **105 pass / 111 fail / 2 crash** of 218, measured with a
  crash-recovering harness (a runner that does not restart the server counts
  every test after a crash as a pass; the older "106" figure was measured that
  way and is not comparable).
- An old-vs-new audit of eight subsystems produced 76 candidate gaps. 48 went
  through adversarial verification: **42 confirmed, 6 refuted**.
- **28 lower-severity candidates were never verified** -- verification was
  capped at the six most severe per subsystem. They are claims, not findings,
  and nothing below rests on them.
- The items in "Verified live" were reproduced against a running server, not
  inferred from code.

## Status

**155 pass / 61 fail / 2 crash** of 218, from a 105/111/2 baseline: **+50, zero
regressions** (no test that passed before now fails). Landed: D1, I1, I4.

The remaining 61 are dominated by one item, which reorders the rest of this
plan:

| n | item |
|---|---|
| **26** | **D2** -- `CREATE INDEX` on a view |
| ~13 | EXPLAIN plan-text mismatches (cosmetic; M1/C-cluster) |
| 3 | duckdb assertion in a statement |
| 3 | `sdb_metrics` (F) |
| 2 | index "not bound on the table it indexes" |
| **2 (crash)** | **S3** -- generated-PK sequence |
| rest | singletons |

S3 is now worth 2 tests, not the headline. D2 is worth 26.

## Ordering principle

Dependency first, then tests unblocked. S1 is the foundation: four separate
symptoms are one missing base class, and fixing it is a precondition for S2
and S3. The inverted-index items are independent of the search-table items and
can go in parallel.

Phase discipline from `PLAN.md` is preserved: nothing here implements phase-2
durability, phase-3 PostgreSQL semantics, or phase-4 RBAC. Items that belong to
those phases are listed at the end, unfixed, so they are not mistaken for
oversights.

---

## S1. `SearchTableEntry` must derive from `DuckTableEntry`

**The single highest-leverage fix.** Four unrelated-looking failures are one
cause.

Verified live -- on one search table:

| statement | today |
|---|---|
| `DROP TABLE t` | `Calling GetStorage on a TableCatalogEntry that is not a DuckTableEntry` -- the table cannot be dropped |
| `CHECKPOINT` | same error; the database can never be checkpointed while a search table exists |
| `ALTER TABLE t ADD COLUMN c INT` | `Unsupported alter type for catalog entry!` |
| `DELETE FROM t` | `reinterpret_cast<const T*> == dynamic_cast<const T*>` assert |

Root cause: `SearchTableEntry final : public duckdb::TableCatalogEntry`
(`server/catalog1/entry/search_table.h:81`). The old entry was
`SereneDBTableEntry : public duckdb::DuckTableEntry`
(`4859c777d:server/catalog/entry/duckdb_table_entry.h:74`), holding a
`shared_ptr<DataTable>` that was **null for a search table**, and separately
overriding `IsDuckTable()` to return false.

That split is the whole design, and it is duckdb's own extension point:

- *Physically* a `DuckTableEntry`, so `GetStorage()`, `AlterEntry()`, drop and
  checkpoint work through duckdb's machinery unchanged.
- *Logically* `IsDuckTable() == false`, so every planner path that asks "may I
  reach past the entry into the rows" is told no and routes to iresearch.

The three fatal call sites -- `DuckSchemaEntry::DropEntry`
(`duck_schema_entry.cpp:394`) and `checkpoint_manager.cpp:309,365,615` -- call
`GetStorage()` unconditionally for any `CatalogType::TABLE_ENTRY` and do **not**
consult `IsDuckTable()`. Deriving from `DuckTableEntry` satisfies them without
touching the fork.

**Fix**: change the base to `duckdb::DuckTableEntry`; override
`IsDuckTable()` to return `false`; carry the empty/duck storage the base
expects. Port back `GetVirtualColumns()` and `GetRowIdColumns()` (old header
`:115-117`) -- their absence is the separately-reported "search table publishes
no row identity", and it is the same fix.

**Rejected alternative**: patching the three duckdb call sites to consult
`IsDuckTable()`. It is more code in the fork, needs owner approval per
`PLAN.md`, and re-implements what deriving already gives. `PLAN.md`: added
kinds get their properties "by construction -- by going through the same
machinery, never through code that imitates it."

**Verify**: `CREATE`/`INSERT`/`SELECT`/`DELETE`/`ALTER`/`DROP`/`CHECKPOINT`
round-trip on one search table; `search_table_*.test`.

## S2. Route the rest of the DML

`SereneDBCatalog` overrides `PlanInsert` and nothing else
(`server/catalog1/catalog.h:63-71`), so `DELETE`/`UPDATE`/`MERGE`/CTAS reach
duckdb code that casts to `DuckTableEntry`.

duckdb already declares all of them virtual (`catalog.hpp:346-356`):
`PlanCreateTableAs`, `PlanInsert`, `PlanDelete` (2 overloads), `PlanUpdate`
(2), `PlanMergeInto`. **No fork patch is needed** -- add the overrides beside
the existing `PlanInsert`, dispatching to the search-table operators the way
insert already does.

**The operators already exist and are dead code.** `SereneDBSearchDelete`,
`SereneDBSearchUpdate` and `SereneDBSearchTruncate` are fully implemented in
`connector/duckdb_physical_search_{delete,update,truncate}.{h,cpp}`, and
nothing in the tree constructs any of them. Only the hooks are missing.

### Attempted, reverted -- two blockers this ordering missed

Porting the old `PlanDelete`/`PlanUpdate` and the entry's row-identity
overrides compiles cleanly and is still the right destination, but it does not
work yet, and the entry overrides must **not** land first:

1. **The scan cannot project a PK virtual column.**
   `duckdb_search_full_scan.cpp:401-408` throws
   "projecting virtual column N through an inverted-index scan is not
   supported". Its own comment explains why it was safe:
   *"needs TableCatalogEntry to publish them; the pinned duckdb's
   GetVirtualColumns() offers rowid alone, so nothing produces such an id."*
   Publishing them from `SearchTableEntry` makes that stub reachable, so
   `DELETE` swaps one failure for another. Note the id spaces differ:
   `VIRTUAL_COLUMN_START + i` encodes the table's *logical* column index, while
   the scan's normal path indexes `bind_data.column_ids` -- the translation is
   not simply subtracting the base.

2. **`SereneDBSearchUpdate` wants every column, not the SET columns.**
   `duckdb_physical_search_update.cpp:101` asserts
   `_update_columns.size() == p`, "search UPDATE must project every
   non-generated-PK column", but `LogicalUpdate::expressions` carries only the
   SET list. iresearch has no in-place update (it is delete + reinsert), so the
   old tree must have widened UPDATE at bind time; that widening has to be
   found and ported, or the operator's contract changed.

**Corrected order**: scan-side virtual-column projection first, then the
entry's `GetVirtualColumns()`/`GetRowIdColumns()`, then `PlanDelete`, and
`PlanUpdate` last behind the bind-time widening. Landing the entry overrides
before the scan support makes `DELETE` fail differently and `UPDATE` abort the
server on the assert above.

**Verify**: `search_table_alp_filter`, `search_table_isnull_validity`,
`search_table_rle_filter` (all three fail today on the `DELETE` assert).

## S3. Create the generated-PK sequence

Verified live -- the plainest documented search table kills the server:

```sql
CREATE TABLE t(a INT, b TEXT) WITH (storage = 'search');
INSERT INTO t VALUES (1,'x');
```
`search_table_dispatch.cpp:170`, `SDB_ASSERT(target.generated_pk_seq)`.

`FindGeneratedPkSequence` scans `DependencyManager` for an ownership edge, but
`AddOwnership`'s only caller in the tree is duckdb's own
`ALTER SEQUENCE ... OWNED BY`, which serenedb never issues, and
`grep -r CreateSequence server/` is empty -- nothing creates a sequence at all.
The old producer was `4859c777d:server/catalog/ddl/tables.cpp:182-196`.

**Fix**: at create time, for a search table whose declared key set is empty:
create a `<table>_pk_seq` in the same schema, then
`AddOwnership(transaction, table, sequence)` so the edge the reader already
scans for exists. Keeping the edge in duckdb's `DependencyManager` rather than
a field on the entry is the `PLAN.md`-aligned choice: drop and cascade then
come for free, and it is not a side registry.

Two defects to fix in the same change:

- The ownership edge is read in **opposite directions** at
  `search_table_dispatch.cpp:126` and `pg_class.cpp:214`. One is wrong; they
  must agree.
- Turn the three `SDB_ASSERT(generated_pk_seq)` sites into a raised SQL error.
  A catalog inconsistency must not abort the server. (Same class of bug as the
  `SDB_ENSURE` at `duckdb_table_function.cpp:356` already fixed this session --
  `SDB_ENSURE` fires `SDB_ASSERT(false, ...)` *before* throwing, so with
  assertions on it aborts.)

**Seam problem -- needs a fork patch, so it is blocked on approval.**
`Catalog::CreateTable` is not virtual. `MakeTableEntry` *is* a fork hook
(`duck_catalog.hpp:49`) but takes only `(DuckSchemaEntry&,
BoundCreateTableInfo&)` -- no transaction, so it cannot create a sequence or
register ownership. The transaction does exist one frame up, in
`DuckSchemaEntry::CreateTable(CatalogTransaction, BoundCreateTableInfo&)`
(`duck_schema_entry.cpp:156-181`), which already ends with the placed entry in
hand. The minimal patch is a post-create hook there:

```cpp
auto entry = AddEntryInternal(transaction, std::move(table),
                              info.Base().on_conflict, info.dependencies);
if (!entry) {
  return nullptr;
}
catalog.Cast<DuckCatalog>().OnTableCreated(transaction, *this, *entry);
return entry;
```

plus an empty `virtual void OnTableCreated(...)` on `DuckCatalog`. Roughly six
lines in the fork.

**Verify**: `inverted_index_multiterm_score`, `search_table_returning` (the two
crashes), plus every search-table test that inserts without a declared key.

---

## I1. Write `sdb_index_id` / `sdb_table_id`

`kIndexIdOption` has two readers (`inverted_store_index.cpp:1237,1814`) and
**no writer**; `IdOption` returns 0 when absent, so every inverted index in the
process has `_index_id == 0`. `Transaction::_search_feeds` is keyed by that id,
so the first index to append in a commit installs its feed session at key 0 and
every other index on the table is handed the same session -- their documents
are written by index #1's writer into index #1's directory. Index #1 ends up
with (indexes x rows) documents; indexes #2..N get none. Every over-count in
the failing suite matches that product exactly.

The old writer was `4859c777d:server/catalog/log/store.cpp:141-144`, which
could write the id because the old catalog assigned ids up front
(`info->oid = index.GetId().id()`).

**Why it cannot go where the other options go**: `_info->options` is written at
`duckdb_physical_create_index.cpp:461-465` (`EncodeInvertedIndexOptions`,
`WritePkPolicy`), but the entry does not exist until `~:495` and its oid is
first available at `:500`. The fossil comment at
`search_table_dispatch.cpp:210-212` describes exactly this constraint.

**Fix**: write the id into the created entry's options after `CreateEntry`
returns, and into `IndexStorageInfo::options` in `SerializeToDisk` /
`SerializeToWAL`. Both are required, because the two load paths read different
maps: `index_binder.cpp:47` takes `input.options` from `create_info.options`
(the normal bind, used by `FindInvertedStore` -> `BindIndexes`), while
`wal_replay.cpp:865` takes it from `index_storage_info.options`.

The same edit fixes the checkpoint-teardown defect for free: `SerializeToDisk`
returning a bare `IndexStorageInfo{name}` is what drops the options *and* what
makes `IsValid()` false. While there, initialise `IndexStorageInfo::root` --
`idx_t root;` (`index_storage_info.hpp:57`) has no initialiser, neither
constructor assigns it, and `Serialize` writes it, so every checkpoint of a
bound inverted index currently writes uninitialised stack bytes in **release**
builds too.

**Verify**: ~18 tests, mostly `inverted_index_matrix_*`. The signature to watch
is a count that is an exact multiple of the index count.

## I2. Bind the partial-index predicate

Found independently by three auditors, confirmed by all three verifiers, and
**no test currently reaches it**.

`HasPredicate()` is true because the config copied `info.where_clause`
(`catalog1/entry/inverted_index.cpp:484-486`), but `unbound_expressions`
contains only the keys -- duckdb's `IndexBinder` turns `WHERE` into a
`LogicalFilter` and never an index expression. So `Predicate()`
(`inverted_store_index.cpp:383-385`) hands `SelectRows` the **last key's value
vector**, which it reads as `bool`. The old code appended the bound predicate
as one extra trailing expression, which is what made
`_results.data[ColumnCount()-1]` correct.

Consequence: the backfill is right (the predicate really is a `LogicalFilter`
in the build plan), so `CREATE INDEX ... WHERE` looks correct -- then every
later INSERT, `VACUUM (REFRESH_TABLE)` and WAL replay selects rows by
misreading a value vector. A partial index silently diverges from its
definition. `REINDEX` repairs it until the next write. Not a memory-safety bug:
the unified format always yields at least one byte per element.

**Fix**: bind `_config->predicate` (a `ParsedExpression` that survives on the
config) into its own bound expression and evaluate it separately, rather than
restoring the trailing-slot trick. The slot-alignment convention is already
fragile -- `catalog1/entry/inverted_index.h:266-269` documents that slot *i*
must line up across three independent vectors -- and adding a slot that feeds no
field makes it worse.

**Verify**: `inverted_index_partial.test:62-79` (`dead_row` must be 0). That
file currently fails earlier on an EXPLAIN diff, so fix the rendering first or
the assertion stays unreached.

## I3. Depend on the dictionary, and resolve it by oid

Two halves of one problem.

`grep AddDependency server` yields **one** hit -- the index->view edge at
`duckdb_physical_create_index.cpp:487`. The old DDL looped
`Index::GetTokenizers()` and registered an edge per dictionary
(`4859c777d:server/catalog/ddl/indexes.cpp:120-131`). Without it,
`DROP TEXT SEARCH DICTIONARY` will not be blocked by a dependent index --
which `basic.test:30` asserts, and which becomes visible the moment D1 lands.

Separately, the persisted `dictionary_oid` is written and decoded but **never
used to resolve**; resolution goes by name on every read. `PLAN.md` names this
trap directly: "Anything keyed by *name* breaks under RENAME; PostgreSQL
semantics are by oid. Dependencies and storage references included."

**Fix**: register the dependency at create; resolve by the persisted oid and
keep the name only for diagnostics.

**Note for the commit**: duckdb words the dependent as
`text search dictionary "x"` (`dependency_manager.cpp:448`) where
`basic.test:33` expects `tokenizer "test_english"`. Per `PLAN.md` that is
error wording, so the **test** changes, not the code.

## I4. `IsKeywordField` lost two things

`InvertedIndexFieldOptions::IsKeywordField` is now
`entry != nullptr && !entry->HasTextDictionary()` -- true only for a field that
names *no* dictionary. But `FillEntryFromTokenizer`
(`index_opclass.cpp:505`) sets `text_dictionary = dict.oid` for **any** named
dictionary including `template='keyword'`, so the predicate answers false for
precisely the fields that are keyword fields. It also lost the old
`IsTermDict()` guard, so include-only and IVF fields now answer true -- wrong
in the other direction.

Four call sites (`ts_dict_plan.cpp:897,1023,1828,1839`). The old version
resolved the dictionary and tested for `irs::StringTokenizer::Options`; the new
signature dropped the `ClientContext`, which is why the check was replaced by an
approximation.

**Fix**: precompute an `is_keyword` flag onto `InvertedIndexField` at
`FillEntryFromTokenizer`, where the analyzer is already in hand, and restore the
`IsTermDict()` guard. Cheaper than re-threading a `TokenizerMap` through four
call sites, and it persists with the rest of the definition.

**Verify**: `ts_dict_predicates`, `ts_dict_minmax_count`, `ts_dict_mixing`, and
probably `ts_dict_facets` / `ts_dict_array_agg`.

---

## D1. `DROP TEXT SEARCH DICTIONARY`

30 tests, one bug, reproducible in two statements with no dependent index:

```sql
CREATE TEXT SEARCH DICTIONARY d (template='text', locale='en_US.UTF-8');
DROP TEXT SEARCH DICTIONARY d;
-- ERROR: Attempting to do catalog changes on a transaction that is read-only
```

The PEG transformer rewrites the statement into a `PragmaStatement`, and
`Binder::Bind(PragmaStatement&)` never calls `RegisterDBModify`, so the
transaction keeps the `is_read_only = true` it is born with and
`PushCatalogEntry` throws. The rewrite already has the workaround --
`catalog::DeclareModified` (`catalog1/catalog.cpp:63-70`), whose comment says
these paths "have no binder" -- and `CreateTokenizer` calls it (`:141`), which is
why CREATE works. The drop path bypasses `SereneDBCatalog` entirely and calls
`duckdb::Catalog::DropEntry` directly (`duckdb_tokenizer_function.cpp:133`).

**Fix**: add a `SereneDBCatalog::DropTokenizer` that calls `DeclareModified`
then delegates, and call it from the pragma. Per repo convention every
`SereneDBCatalog` method is defined in `catalog1/catalog.cpp`. Give
`DeclareModified` a modification-type parameter so a drop registers
`DROP_CATALOG_ENTRY` rather than the hardcoded `CREATE_CATALOG_ENTRY`.

In 24 of the 30 the drop is the file's last teardown statement, so those tests
are otherwise green -- expect a large, cheap win, then I3's dependency error to
surface in `basic.test`.

## D2. `CREATE INDEX` on a view

25 tests, all of which create a view. duckdb's `Catalog::BindCreateIndex` is a
virtual whose base throws "can only create an index on a base table" for
anything that is not `TABLE_ENTRY`, documented as "Catalogs that support
indexing views override this method" (`catalog.cpp:337`). `catalog1` has no
override -- `git log -S` confirms it never had one -- so dispatch lands on
`DuckCatalog`'s.

**Fix**: add `SereneDBCatalog::BindCreateIndex`, handling `VIEW_ENTRY` and
delegating to duckdb's for a base table.

**Now the largest remaining item (26 of 61 failures), and bigger than this
entry first claimed.** The good news is that everything below the binder is
already ported: `SereneDBCreateIndexPlan`
(`duckdb_physical_create_index.cpp:1013`) has a full `VIEW_ENTRY` branch, the
fork already widened `LogicalCreateIndex::table` to `CatalogEntry&` ("Either a
table or a view; catalog decides how to handle each"), and
`view_fast_path.{h,cpp}`, `index_source_view_{table,file}` and
`index_source_external_lookup` all exist. Only the bind hook is missing.

The bad news is that duckdb's `IndexBinder::BindCreateIndex` cannot be
delegated to for a view: it takes a `TableCatalogEntry&`, casts the child plan
to `LogicalGet`, and casts its bind data to `TableScanBindData` -- none of
which hold for a view's plan. The old override was ~570 lines
(`4859c777d:server/catalog/ddl/duckdb_catalog.cpp:1504-2075`) and carried view
fast paths, captured manifests, delta/REINDEX passes, kept-position remapping
and predicate normalisation.

**Open question before implementing**: how much of that to port. A minimal
version -- bind the key expressions against the view's plan, attach the
`WHERE` as a `LogicalFilter`, build `LogicalCreateIndex` over the view entry --
may cover most of the 26 without the fast-path/manifest/delta machinery, since
the physical side already reads what it needs from options. Worth trying
minimal first and measuring.

## M1. Smaller DDL losses

Confirmed, each independent, none blocking:

- Unknown `CREATE INDEX ... WITH` options are silently accepted;
  `kCreateInvertedOptions` is dead code, alongside the already-known dead
  `kAlterableInvertedOptions`.
- `USING btree` / `USING secondary` now error, and the access-method name is
  compared case-sensitively in one place and case-insensitively in another, so
  `USING INVERTED` builds a `DuckIndexEntry` that is then cast to
  `InvertedIndexEntry`.
- A schema-qualified opclass naming the index's own schema no longer resolves.
- `WHERE` on a non-inverted (ART) index is no longer rejected, producing a
  silently partial ART.
- `ALTER INDEX ... RENAME TO` unsupported; `ALTER INDEX ... SET/RESET` still
  unimplemented.

## G1/G2. Outside the index suite

- **`SERIAL` silently writes NULLs.** `CREATE TABLE s(id SERIAL)` succeeds,
  `INSERT` succeeds, `id` is NULL. PostgreSQL gives 1, 2 and `NOT NULL`. The
  types are still declared (`pg_logical_types.h:89-91`) but nothing expands them
  into a sequence plus a `nextval` default; the old expansion was
  `4859c777d:server/catalog/ddl/tables.cpp:145-175`. Silent wrong data is the
  worst failure mode here -- this should not wait for the index work.
- **`setval()` is unregistered** (both arities). Sequences themselves work on
  duckdb's implementation; `nextval` is fine.

---

## Deferred, by `PLAN.md` -- real, not oversights

**Phase 2 (durability; gate is the recovery suite).** Nothing reopens an
inverted index's iresearch storage after a restart; artifacts and persisted
expression keys are keyed by duckdb oids that are not durable; a committed DROP
never releases artifacts (`MarkDropped`'s only caller is rollback); there is no
boot orphan sweep. `PLAN.md`: "in phase 1, stock duckdb durability stands in."
Practical consequence now: use a fresh datadir for manual testing.

**Phase 3 (PostgreSQL semantics).** `pragma_storage_info` on an index;
`SELECT ... FROM <ART index name>`; and:

**`DROP ... CASCADE` granularity -- flagged for a decision.** Verified live:

```sql
CREATE SEQUENCE dseq;
CREATE TABLE dep(a INT DEFAULT nextval('dseq'), b TEXT);
INSERT INTO dep(b) VALUES ('r1');
DROP SEQUENCE dseq CASCADE;   -- dep is gone, rows and all
```

PostgreSQL drops the default expression and keeps the table. Strictly,
`PLAN.md` lists duckdb's `DependencyManager` with RESTRICT/CASCADE under "use it
100%" and defers PG semantics to phase 3, so this is a recorded deferral rather
than a defect. It is listed here anyway because the deferral costs a user a
table and its rows with no warning, which is a different risk class from the
error-wording differences the rule is aimed at. **Owner's call.**

Related and confirmed: an index dependent never blocks a DROP, because duckdb
marks index edges non-blocking -- `DROP FUNCTION`/`SEQUENCE`/`TYPE` without
CASCADE silently deletes the index.

## Dead machinery removed (behaviour-neutral)

Four mechanisms that survived the port with no live producer. All removed;
the suite was byte-identical before and after (155/61/2 either way).

- `wrote_roles` (`connection_context.h`) -- never assigned `true` anywhere, and
  its callee `BumpRoleGeneration()` is `{}` in `role_closure_stub.cpp`, which is
  what the build actually compiles. `role_closure.cpp`, `rbac.cpp` and
  `optimizer/rbac.cpp` are all out of the build.
- `transaction_abort_cleanup` (`duckdb_client_state.h`) plus the whole
  `Backfill` struct -- `PushTransactionOverride` is never called in `server/`
  and `Backfill::store_db`/`::txn` are only ever read, so the registered lambda
  and the twin pop/rollback in `Finalize` were both permanently unreachable.
- `_feeds_inverted` and `CreateIndexGlobalState::inverted_index` -- the same
  `EqualsIgnoreCase(index_type, "inverted")` evaluated twice inside an operator
  that only exists for inverted indexes: `SereneDBCreateIndexPlan` is the sole
  constructor and is registered as `create_plan` on the inverted index type
  alone, which duckdb dispatches per resolved type. Two
  `SDB_ASSERT(state->inverted_index)` were asserting a tautology.

**One gap this uncovered.** `transaction_abort_cleanup`'s comment named CTAS as
its user, but no CTAS producer exists either. If `CREATE TABLE ... AS` stages
anything outside duckdb's undo that an abort must compensate for, that
compensation is currently missing, and the dead slot was hiding it. duckdb's
`ClientContextState::TransactionPreRollback` is still the seam.

## Refuted -- do not re-chase

Six claims failed verification. Two matter because they contradict things
asserted earlier in this work:

- **`EqualOptions` / segment reuse.** `INVERTED_DEFINITION.md` claims the
  pointer-identity default is a hard constraint the design must preserve. The
  behaviour is achieved today by another mechanism with no user-visible
  consequence. **Treat that section of that document as suspect.**
- **`LookupField` no longer registering the generated-PK field id** -- the old
  branch was already unreachable.

Also refuted: `GetFileManifest()` "hard-stubbed"; the ALTER-INDEX live push (not
independent -- it is the tail of the missing ALTER); the index-scan
`TableIndexList` concurrency claim (its old-code premise was wrong);
`pragma_storage_info` (phase-3 deferral).

## Measuring

Use a runner that restarts the server on death and gates each test on a live
connection; without that, a crash silently converts the remainder of the suite
into apparent passes. Re-run the whole 218 after each item -- several of these
interact (D1 unmasks I3; I1 changes counts across `matrix_*`; S1 is a
precondition for S2/S3).

Current baseline to beat: **105 / 218**.

**Testing trap**: a search table's reads are eventually consistent -- rows
become visible only after `refresh_interval_ms` (measured at roughly 12s with
the defaults). A `SELECT` immediately after an `INSERT` legitimately returns
the older row count. Do not read that as data loss or a projection bug; wait,
or set the interval down for the test.
