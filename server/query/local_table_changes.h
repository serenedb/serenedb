////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
///
/// Licensed under the Apache License, Version 2.0 (the "License");
/// you may not use this file except in compliance with the License.
/// You may obtain a copy of the License at
///
///     http://www.apache.org/licenses/LICENSE-2.0
///
/// Unless required by applicable law or agreed to in writing, software
/// distributed under the License is distributed on an "AS IS" BASIS,
/// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
/// See the License for the specific language governing permissions and
/// limitations under the License.
///
/// Copyright holder is SereneDB GmbH, Berlin, Germany
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <cstdint>
#include <duckdb/common/types/column/column_data_collection.hpp>
#include <memory>
#include <vector>

#include "basics/containers/flat_hash_map.h"
#include "catalog/identifiers/object_id.h"

namespace sdb::query {

// Per-search-table, per-transaction buffer of in-flight changes.
//
// Lives on query::Transaction (one entry per touched search table). Three
// roles, all consumed at txn commit / read overlay time -- the buffer is
// passive during the transaction itself:
//
//   1. **Write source.** At commit, `inserts` is drained into the iresearch
//      writer via SearchTableSinkWriter -- one columnstore Append per
//      column plus per-row work for tokenized columns.
//   2. **WAL marker payload.** The same `inserts` ColumnDataCollection is
//      serialised into the column-batch WAL marker, riding the txn's
//      RocksDB WriteBatch.
//   3. **Read-your-own-writes overlay.** During the same transaction, the
//      search-table scan operator scans `inserts` before falling through
//      to the iresearch reader, so the txn sees its own pending writes.
//
// Why ColumnDataCollection: DuckDB's BufferManager spills it to disk under
// memory pressure transparently, so a wide INSERT doesn't blow heap. We
// follow DuckLake's `LocalTableChanges` pattern; see design doc D15.
//
// PR 3.1 scope: struct + per-transaction map only. Population by the
// SereneDBSearchInsert operator and consumption by the scan / commit lands
// in 3.4 / M4.
// Per-Sink-call sequence range for tables without an explicit PRIMARY KEY.
// SereneDBSearchInsert reserves contiguous PKs at Sink time
// (`generated_pk_seq->ReserveWriteUnsafe(num_rows)`); a contiguous range
// is small (one pair) so we carry the list of ranges instead of per-row
// PK values. At Finalize the per-row PK is recovered via a cursor that
// walks ranges in append order -- ranges and buffer rows are in 1:1
// arrival correspondence even when CDC consolidates Sink chunks.
struct ReservedPkRange {
  uint64_t base;
  duckdb::idx_t count;
};

struct LocalTableChangesEntry {
  // Lazily constructed on first append (the operator needs LogicalTypes
  // and an Allocator that aren't available at Transaction-construction
  // time). nullptr until the first INSERT chunk arrives.
  std::unique_ptr<duckdb::ColumnDataCollection> inserts;

  // Generated-PK reservations -- one entry per Sink call (in arrival order).
  // Sum of counts equals inserts->Count(). Empty for tables with an
  // explicit PRIMARY KEY -- the PK is in `inserts` directly and
  // MakeColumnKey reads it from chunk columns.
  std::vector<ReservedPkRange> reserved_pk_ranges;

  // Affected-row identifiers for in-flight UPDATE/DELETE (M6). Empty in
  // 3.1 -- carried here now to match DuckLake's struct shape and avoid
  // a layout migration later.
  std::vector<int64_t> row_ids;
};

// Map keyed by SearchTableShard's table_id (one search table -> one
// entry). Cleared on Transaction::Destroy.
using LocalTableChanges =
  containers::FlatHashMap<ObjectId, LocalTableChangesEntry>;

}  // namespace sdb::query
