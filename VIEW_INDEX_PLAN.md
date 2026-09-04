# CREATE INDEX over a view

## Goal

Make `CREATE INDEX ... USING inverted (...)` work when the relation is a view.
Today it fails at bind with duckdb's `can only create an index on a base table`.

Gates **21** tests in `sdb/pg/index`:

- 17 `inverted_index_view_*`, `reindex_view_*`, `secondary_index_view`,
  `vector_search_view_json`
- `ts_dict_view`, `ts_dict_facets`, `ts_dict_residual_filter`,
  `ts_dict_residual_filter_complex`

## What already exists

Nothing below needs to be written. The fork was built for this end to end and
only the bind step was ever missing.

### duckdb already routes views to us

- `bind_create.cpp:904` -- when the bound relation is not a `LOGICAL_GET` with a
  table, it resolves the `ViewCatalogEntry` and calls
  `view.catalog.BindCreateIndex(*this, stmt, view, std::move(plan))`, handing us
  the already-bound view-body plan.
- `catalog.cpp:337` -- `Catalog::BindCreateIndex` is virtual, commented
  *"Catalogs that support indexing views override this method."*
  The base and `DuckCatalog::BindCreateIndex` (`bind_create.cpp:799`) both throw
  for a non-`TABLE_ENTRY`; that throw is what we currently inherit.
- `logical_create_index.hpp:28` -- `LogicalCreateIndex::table` is a
  `CatalogEntry&`, commented *"Either a table or a view; catalog decides how to
  handle each."*

### serenedb's physical side already handles views

- `duckdb_physical_create_index.cpp:986` -- full `VIEW_ENTRY` branch: columns
  come off `view.GetColumnInfo()`, filtered by `_sdb_view_kept_positions`.
- `view_fast_path.h` -- `ResolveViewFastPath(context, view_info, key_columns)`
  returns `ViewFastPath{pk_spec, key_columns, projection_columns, is_glob, ...}`,
  plus `BackfillPkVirtualColumns`, `MakeFastPathLookupFunction`,
  `BindFastPathSource`, `KeyColumnsFromOptions`.
- `index_source_factory.cpp:46` -- `MakeIndexSource` already selects between
  `ViewTableIndexSource`, `ExternalLookupIndexSource`, `ViewFileGlobIndexSource`
  and `ViewFileSingleFileIndexSource`.
- `duckdb_reindex_function.cpp:897` (`ResolveSource`) -- REINDEX already drives
  the whole resolve/bind sequence for views. It is the working reference.

### The gap

`SereneDBCatalog::BindCreateIndex` does not exist.

Its two outputs are read by the physical operator and written by nobody:

| option | read at | feeds |
| --- | --- | --- |
| `_sdb_view_kept_positions` | `duckdb_physical_create_index.cpp:997` | which view columns the scan emits, in order |
| `_sdb_view_fast_path_pk` | `duckdb_physical_create_index.cpp:240` | `pk_shape` (Single / Struct, `file_row`) |

## Design: how much should the view path differ?

Six sites branch on `IsDuckDBTable()`. They do not all mean the same thing.

| site | gates | verdict |
| --- | --- | --- |
| `:371` `col_index_to_id` | scan slot -> ColumnId | incidental |
| `:561` `state->columns` | the same mapping again | incidental |
| `:439` `ResolveInvertedIndexOptions(..., table_backed, pk_shape)` | (a) `reindex_interval` is view-only; (b) `pk_shape` | (a) essential, (b) incidental |
| `:513` `PublishNewInvertedIndex(table_obj->GetStorage(), ...)` | registers with the DataTable index list so DML live-feeds it | essential |
| `:547` `SetDeleteLogRowidEnd(GetNextRowId())` | backfill / live-feed horizon | essential |
| `:645` `uncommitted_min_slot` | per-sink uncommitted-min rowid tracking | essential |

`:371` and `:561` are the same mapping written twice, one indirection apart:

```cpp
if (is_table) { ... columns[_info->column_ids[chunk_idx]] ... }   // indirect
else          { ... columns[i] ...                            }   // identity
```

The view path only needs the second form because it invented
`_sdb_view_kept_positions` instead of filling `info.column_ids`.

The three essential branches all reduce to one fact: **a table-backed index is
live-fed from a DataTable that has rowids and concurrent DML; a view-backed index
is snapshot-built and refreshed by REINDEX.** That is a lifecycle difference, not
an options difference, and it does not collapse into `ResolveViewFastPath`.

### Decisions

1. **Drop `_sdb_view_kept_positions`.** The view bind fills `_info->column_ids`
   with view positions. `:371` and `:561` lose their branches; both paths read
   one field. Also stops persisting transport state -- `CreateIndexInfo::options`
   is serialized (property 208), so this option would otherwise live forever on
   the entry and show up in `pg_indexes.indexdef` beside `sdb_table_id`.
2. **Drop `_sdb_view_fast_path_pk`.** `pk_shape` rides the channel that already
   persists PK decisions, `WritePkPolicy` / `ReadPkPolicy`, instead of a second
   stringly-typed one. Open: confirm `PkPolicy` can express Struct + `file_row`;
   if not, extend it there rather than add an option.
3. **Rename `IsDuckDBTable()`.** It reads like type dispatch but means "is this
   index live-fed". `HasLiveFeed()` (or similar) makes the three remaining
   branches self-evidently the real distinction rather than residue.

After this, views differ from tables in exactly two honest places: how the source
is resolved, and whether there is a live feed.

## Steps

1. Add `SereneDBCatalog::BindCreateIndex` override.
   `entry.type == TABLE_ENTRY` delegates to `DuckCatalog::BindCreateIndex`, which
   now handles `WHERE` via the `IndexBinder` patch already in the fork.
2. View branch:
   - `ResolveViewFastPath(context, view_info, KeyColumnsFromOptions(info.options))`;
     on `nullopt` raise the existing "not a recognised fast-path source" error
     (`index_source_factory.cpp:50`).
   - Fill `_info->column_ids` with the view positions the scan emits (decision 1).
   - Record `pk_shape` through `WritePkPolicy` (decision 2).
   - Build the scan: a `LogicalGet` over `MakeFastPathLookupFunction(fp)` bound
     with `BindFastPathSource`, projecting `fp.projection_columns` plus
     `BackfillPkVirtualColumns(fp)`.
   - Bind the index expressions and assemble
     `LogicalCreateIndex{..., table = view}`.
3. Collapse the `:371` and `:561` branches; rename the predicate (decision 3).
4. Physical operator otherwise unchanged.

## Risks

- **`IndexBinder::BindCreateIndex` takes `TableCatalogEntry&`**
  (`index_binder.hpp:29`), so the view path cannot reuse it. Its body has to be
  replicated: `InitCreateIndexInfo`-equivalent, expression binding, operator
  construction. `InitCreateIndexInfo` itself is also unusable verbatim -- it does
  `get.GetTable()->catalog.GetName()`, and a table-function get has no table.
  This is the fiddliest part.
- **Reuse vs rebuild the scan.** duckdb hands us a bound view-body plan, but the
  physical side's expectations imply a fresh get over the fast-path source.
  Rebuilding is probably right; reusing may be simpler for the `DuckDBRowId` and
  catalog-ref cases. Settle while writing, not now.
- **Expect waves.** The 21 tests span several `PkSpec`s (glob, iceberg, external
  key, rowid) and `ResolveViewFastPath` returns `nullopt` for bodies it does not
  recognise, so some will still fail on unsupported shapes after the bind lands.

## Verification

- `scratchpad/run_all.sh` over `sdb/pg/index/*.test`; baseline to beat is
  **161 PASS / 57 FAIL / 0 CRASH**.
- The harness boots its own server on a fresh datadir per run. Do not rebuild
  while a run is in flight, and do not let it adopt a server already listening --
  both produced invalid runs earlier.

## Adjacent, not in scope

- `sdb_index_id` is read but never written, so only the first index on a table
  gets rows (7 `matrix_*` tests).
- The generated-PK sequence is not owned by its table, so `DROP TABLE` leaks it.
- `InvertedStoreIndex::SerializeToDisk` returns `IndexStorageInfo{name}`, which
  can never satisfy `IsValid()` (`root_block_ptr.IsValid() || !allocator_infos.empty()`),
  so every checkpoint over an inverted index invalidates the database. Currently
  masked by the test runner reconnecting.
