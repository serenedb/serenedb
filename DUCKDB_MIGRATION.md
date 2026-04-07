# DuckDB Migration Status

## Architecture

```
PG wire (pg_comm_task.cpp) -> DuckDB (plan + execute)
                               ↓
                    StorageExtension + Catalog
                    (PlanInsert/Delete/Update)
                               ↓
                     RocksDB + IResearch
```

- DuckDB is the query engine (optimizer + executor), not a standalone DB
- PG wire protocol handling stays in `pg_comm_task.cpp`
- SereneDB catalog/RocksDB/IResearch stays as storage layer
- DuckDB fork at `serenedb/duckdb`, submodule at `third_party/duckdb`

## What Works

### Core DML (all via DuckDB -> RocksDB)
- **SELECT** -- full scan via `duckdb_table_function.cpp`, projection pushdown, self-joins, LATERAL, recursive CTEs
- **INSERT** -- generated PK (RevisionId) + explicit PK, conflict detection (THROW policy)
- **DELETE** -- PK columns injected via virtual columns (`GetRowIdColumns`/`GetVirtualColumns`)
- **UPDATE** -- column-wise writes, PK columns available via same virtual column mechanism

### DDL
- **CREATE TABLE** -- through DuckDB catalog -> `SereneDBSchemaEntry::CreateTable` -> SereneDB catalog
- **DROP TABLE** -- through DuckDB catalog -> `SereneDBSchemaEntry::DropEntry`

### Infrastructure
- **Transactions** -- `ConnectionContext` accessible via `SereneDBClientState` registered on DuckDB's `ClientContext`. Physical operators use `GetSereneDBContext(context.client)` to access RocksDB transaction. BEGIN/COMMIT/ROLLBACK work.
- **Per-session DuckDB connection** -- stored on `PgSQLCommTaskBase::_duckdb_conn`, created during PG auth
- **Config (SET/SHOW)** -- PG variables registered via `AddExtensionOption`. SHOW patched in DuckDB parser (`transform_show.cpp`) to use `current_setting()`.
- **Extended protocol** -- Parse/Bind/Execute with proper binary serialization via `duckdb_serialize.cpp`
- **EXPLAIN** -- DuckDB native, projection pushdown visible
- **Scan rescan** -- fixed via LookupEntry cache in schema entry
- **Command tags** -- proper INSERT/UPDATE/DELETE/CREATE/DROP tags
- **Multi-statement** -- via `ExtractStatements`
- **core_functions extension** -- enabled for `current_setting()`, `version()`, etc.

### Serialization
- `duckdb_serialize.cpp` -- full PG wire serializer adapted from Velox version, supports text+binary for all standard types (int, float, varchar, bool, timestamp, date, interval, uuid, decimal, bytea, arrays)
- Used by extended protocol for proper format negotiation

## Key Files

### DuckDB Engine
| File | Purpose |
|------|---------|
| `server/query/duckdb_engine.h/cpp` | Singleton DuckDB instance, connection factory, config registration |
| `server/pg/duckdb_query_handler.h/cpp` | Simple protocol: execute SQL, serialize results to PG wire |
| `server/pg/duckdb_serialize.h/cpp` | DuckDB Vector -> PG wire serialization (text+binary) |

### Storage Extension (Catalog)
| File | Purpose |
|------|---------|
| `server/connector/duckdb_storage_extension.h/cpp` | Registers SereneDB storage in DuckDB |
| `server/connector/duckdb_catalog.h/cpp` | Custom Catalog: PlanInsert/Delete/Update/CreateTableAs |
| `server/connector/duckdb_schema_entry.h/cpp` | Schema: LookupEntry, CreateTable, DropEntry, Scan |
| `server/connector/duckdb_table_entry.h/cpp` | Table: GetScanFunction, GetStorageInfo, type conversion |
| `server/connector/duckdb_transaction.h/cpp` | TransactionManager (stub, real txn via ConnectionContext) |
| `server/connector/duckdb_client_state.h/cpp` | ClientContextState: bridges DuckDB -> ConnectionContext |

### RocksDB Integration
| File | Purpose |
|------|---------|
| `server/connector/duckdb_table_function.h/cpp` | RocksDB scan as DuckDB TableFunction, projection pushdown |
| `server/connector/duckdb_rocksdb_reader.h/cpp` | Per-column type-dispatched reading from RocksDB -> DuckDB Vector |
| `server/connector/duckdb_rocksdb_writer.h/cpp` | DuckDB Vector -> RocksDB value serialization |
| `server/connector/duckdb_physical_insert.h/cpp` | PhysicalInsert: DataChunk -> RocksDB writes |
| `server/connector/duckdb_physical_delete.h/cpp` | PhysicalDelete: PK columns -> RocksDB deletes |
| `server/connector/duckdb_physical_update.h/cpp` | PhysicalUpdate: column-wise RocksDB updates |

### DuckDB Patches (serenedb/duckdb fork)
| File | Change |
|------|--------|
| `src/parser/transform/statement/transform_show.cpp` | SHOW varname -> SELECT current_setting('varname') |

## Key Patterns

### Accessing ConnectionContext from DuckDB operators
```cpp
#include "connector/duckdb_client_state.h"
auto& conn_ctx = GetSereneDBContext(context.client);
conn_ctx.AddRocksDBWrite();
auto* txn = &conn_ctx.EnsureRocksDBTransaction();
```

### PK columns in DELETE/UPDATE scans
PK columns are injected via virtual columns with IDs `VIRTUAL_COLUMN_START + col_index`:
- `GetRowIdColumns()` returns `[VIRTUAL_COLUMN_START + pk_col_0, ..., COLUMN_IDENTIFIER_ROW_ID]`
- `GetVirtualColumns()` declares them with proper types
- `BindRowIdColumns` adds them to scan automatically
- Physical operators find PK values at `child_output_size - 1 - num_pk + i`

### RocksDB key format
- Key: `[ObjectId(8)][ColumnId(8)][PK bytes]`
- PK encoding: big-endian + sign-bit-flip for integers, null-escape + terminator for strings
- NULL value: empty RocksDB value (0 bytes)
- Empty string: single `\0` byte

### Type mapping
- Velox types still in catalog (`Column::type` is `velox::TypePtr`) -- temporary
- `VeloxTypeToDuckDB` / `DuckDBTypeToVelox` bridge functions in `duckdb_table_entry.cpp`
- Will be removed when catalog switches to DuckDB types

## Known Issues / TODO

### Bugs
- `COUNT(*)` returns column name `count_star()` instead of `count`
- Bulk `INSERT ... SELECT` count wrong in extended protocol (returns 1 instead of N)
- `ON CONFLICT DO NOTHING` blocked by DuckDB's `IsDuckTable()` check
- SET per-connection scope not working (extension options use global scope)
- Dummy rowid column still generated (may be removable now that PK virtual columns work)

### Not Implemented Yet

#### DML (for colleague)
- **SST file writer** for bulk insert (COPY FROM, CTAS, index backfill)
- **Index writers** -- secondary + inverted index updates during INSERT/UPDATE/DELETE (existing `SinkIndexWriter` interface)
- **ON CONFLICT DO UPDATE** (DO NOTHING needs DuckDB IsDuckTable fix)
- **RETURNING** clause
- **INSERT ... SELECT** with proper row counting

#### DDL
- **CREATE INDEX** (USING INVERTED, USING secondary)
- **CTAS** (CREATE TABLE AS SELECT)
- **ALTER TABLE**
- **CREATE VIEW**

#### Search (BM25/TFIDF/PHRASE)
- Register search functions as DuckDB scalar stubs
- Search table function with `pushdown_complex_filter`
- BM25()/TFIDF() as score columns
- TopScoreCollector with segment index (12-byte struct)
- LIMIT pushdown into search

#### COPY
- COPY FROM CSV/Parquet -> RocksDB (with SST writers)
- COPY TO

#### PG Compatibility
- pg_catalog tables as DuckDB TableFunctions
- information_schema
- System functions (many already in DuckDB core_functions)
- Custom logical types for OID/Reg* types

#### Extended Protocol
- Parameter binding ($1, $2) -- currently skipped

#### Infrastructure
- Replace Velox types in catalog with DuckDB LogicalType
- Remove Velox/Axiom submodules and all references
- Remove folly dependency
- Rewrite C++ connector tests from Velox to DuckDB

## Build

```bash
cd build
ninja serened
```

DuckDB build options are configured in `third_party/CMakeLists.txt`.

## Development Setup

Two developers working on same machine -- use separate ports and data dirs:

| Developer | Focus | Port |
|-----------|-------|------|
| mbkkt | PG compat, DDL, types, search | 6161 |
| colleague | DML, SST writer, indexes, COPY | 6162 |

Colleague uses port 6162:
```bash
./build/bin/serened ./build_data --server.endpoint pgsql+tcp://0.0.0.0:6162
psql -h localhost -p 6162 -d postgres -U postgres
```

## Testing

**Important: always remove data dir before running tests**

```bash
# Start server (always clean data dir first)
kill -9 $(lsof -t -i:6161)
rm -rf build_data
./build/bin/serened ./build_data --server.endpoint pgsql+tcp://0.0.0.0:6161

# psql
psql -h localhost -p 6161 -d postgres -U postgres

# sqllogictest
./third_party/sqllogictest-rs/target/release/sqllogictest \
  --engine postgres -p 6161 -u postgres -d postgres \
  tests/sqllogic/any/pg/simple/basic_dml.test

# Extended protocol
./third_party/sqllogictest-rs/target/release/sqllogictest \
  --engine postgres-extended -p 6161 -u postgres -d postgres \
  tests/sqllogic/any/pg/simple/basic_dml.test
```
