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
// Lives on query::Transaction (one entry per touched search table). The
// parallel SereneDBSearchInsert operator registers one ColumnDataCollection
// per sink thread here (see GetLocalSinkState) and each thread appends only
// to its own collection during Sink -- so the per-thread append is
// lock-free. The set of collections has three roles, consumed *after* the
// parallel Sink phase (single-threaded, at the Finalize/commit barrier or
// read time):
//
//   1. **Write source.** Each thread drains its own chunk directly into its
//      own iresearch IndexWriter::Transaction during Sink; the collections
//      are then retained only for roles 2 and 3.
//   2. **WAL marker payload.** At Finalize each collection is serialised
//      into a column-batch WAL marker, riding the txn's RocksDB WriteBatch.
//   3. **Read-your-own-writes overlay (future).** A search-table scan in the
//      same transaction can scan the collections before falling through to
//      the iresearch reader, so the txn sees its own pending writes.
//
// Why ColumnDataCollection: DuckDB's BufferManager spills it to disk under
// memory pressure transparently, so a wide INSERT doesn't blow heap. We
// follow DuckLake's `LocalTableChanges` pattern; see design doc D15.
struct LocalTableChangesEntry {
  // One collection per parallel sink thread, registered in
  // SereneDBSearchInsert::GetLocalSinkState. unique_ptr so a thread's
  // collection pointer (held in its LocalSinkState) stays stable across
  // vector growth. Empty until the first INSERT thread registers one. The
  // generated PK is assigned inline in Sink (each thread reserves its own
  // contiguous range from the sequence), so no per-row PK bookkeeping is
  // carried here.
  std::vector<std::unique_ptr<duckdb::ColumnDataCollection>> insert_collections;

  // Affected-row identifiers for in-flight UPDATE/DELETE (M6). Empty until
  // then -- carried here now to match DuckLake's struct shape and avoid a
  // layout migration later.
  std::vector<int64_t> row_ids;
};

// Map keyed by SearchTableShard's table_id (one search table -> one
// entry). Cleared on Transaction::Destroy.
using LocalTableChanges =
  containers::FlatHashMap<ObjectId, LocalTableChangesEntry>;

}  // namespace sdb::query
