# WAL-Based Index Recovery Plan

## Goal
Replace the current index recovery with a WALDataSource that reads RocksDB WAL entries and feeds them into the existing IndexBackfill DataSink through a standard Velox pipeline.

## Architecture

### Current CREATE INDEX backfill pipeline
```
SqlAnalyzer: TableScanNode(table) → TableWriteNode(table)
CreateIndex: table.BackfillIndexId() = index_id
Velox:       createDataSource() → RocksDBSnapshotFullScanDataSource (reads ALL rows)
             createDataSink()   → RocksDBIndexBackfillDataSink (writes to index)
```

### New recovery pipeline (same sink, different source)
```
Recovery:    TableScanNode(table) → TableWriteNode(table)
             table.BackfillIndexId() = index_id
             table.WALRecoveryRange() = {index_tick, rocksdb_tick}
Velox:       createDataSource() → WALDataSource (reads only WAL delta)
             createDataSink()   → RocksDBIndexBackfillDataSink (writes to index)
```

If index_tick == rocksdb_tick → nothing to do, skip recovery.

## Implementation Steps

### Step 1: WALDataSource
- **New files:** `server/connector/wal_data_source.hpp`, `server/connector/wal_data_source.cpp`
- Implements `velox::connector::DataSource`
- Constructor takes: `db`, `table ObjectId`, `column_family`, `column_ids`, `row_type`, `start_sequence`, `end_sequence`, `memory_pool`
- `next(size, future)`:
  - Iterates WAL via `db->GetUpdatesSince(start_sequence)` up to `end_sequence`
  - Filters by table ObjectId + default CF
  - Groups per-column PutCF/DeleteCF entries by PK
  - Reassembles into RowVectorPtr batches (same column encoding as RocksDBFullScanDataSource)
  - Returns batches of up to `size` rows
- Handles INSERT/DELETE/UPDATE correctly:
  - INSERT: N PutCFs → row emitted
  - DELETE: N DeleteCFs → row NOT emitted
  - INSERT then DELETE: → not emitted
  - UPDATE (Delete+Put same PK): → emitted with new values
  - PK-change UPDATE: old PK deleted, new PK emitted

### Step 2: Route in createDataSource()
- **Modify:** `server/connector/serenedb_connector.hpp`
- Add `WALRecoveryRange` (start_seq, end_seq) to `RocksDBTable`
- In `createDataSource()`: if `WALRecoveryRange` is set, return `WALDataSource` instead of full scan

### Step 3: Integrate in InitPostRecovery
- **Modify:** `server/search/inverted_index_shard.cpp`
- In `InitPostRecovery` callback, when `_recovery_tick < recovery_tick`:
  1. Get table metadata from catalog snapshot
  2. Build logical plan: `TableScanNode → TableWriteNode`
  3. Set `table.BackfillIndexId() = index_id`
  4. Set `table.WALRecoveryRange() = {_recovery_tick, recovery_tick}`
  5. Compile + run the Velox pipeline
  6. Then proceed with `FinishCreation()` + `StartTasks()`

### Step 4: Update build
- **Modify:** `server/connector/CMakeLists.txt` — add `wal_data_source.cpp`

### Step 5: Testing
- Extend `tests/sqllogic/recovery/index_backfill.test`
- Test: insert rows, crash before iResearch commit, recover, verify index has all rows
- Test: insert + delete in WAL range, verify deleted rows not in index
- Test: equal ticks → no recovery needed

## Key Design Decisions
- WALDataSource processes WAL batch-by-batch (bounded memory)
- Row accumulator groups entries by PK within a processing window
- Flush after each WriteBatch if accumulated enough rows (e.g. 1024)
- WAL pruned past recovery tick → fall back to full reindex or error
