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
#include "search/search_db_wal.h"

namespace sdb::search {

// Per-search-table, per-transaction buffer of in-flight changes.
//
// Lives on search::SearchTableTransaction (one entry per touched search
// table). The
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
  // One buffered INSERT per parallel sink thread, registered in
  // SereneDBSearchInsert::GetLocalSinkState. Both members are unique_ptr so the
  // raw pointers a thread caches in its LocalSinkState (collection /
  // pk_segments) stay valid while other threads push more buffers and grow this
  // vector -- the heap pointees don't move, only the 16-byte element does
  // (nobody caches a pointer to that). Empty until the first INSERT thread
  // registers one.
  struct InsertBuffer {
    // This thread's rows. The generated PK is assigned inline in Sink (each
    // thread reserves its own contiguous range from the sequence), so no
    // per-row PK bookkeeping is carried here.
    std::unique_ptr<duckdb::ColumnDataCollection> collection;

    // One InlinePk{base, count} per Sink chunk appended to `collection`, in
    // append order -- the generated-PK base and that chunk's row count.
    // Recorded per Sink chunk (NOT per collection Chunk: ColumnDataCollection
    // coalesces partial appends) so replay re-slices by count and reproduces
    // each chunk's synthetic PKs (WAL_DESIGN.md §5.6). base is 0 for
    // explicit-PK.
    std::unique_ptr<std::vector<SearchDbWal::InlinePk>> pk_segments;
  };

  std::vector<InsertBuffer> inserts;
};

// Map keyed by SearchTableShard's table_id (one search table -> one
// entry). Cleared when the SearchTableTransaction is reset
// (Transaction::Destroy).
using LocalTableChanges =
  containers::FlatHashMap<ObjectId, LocalTableChangesEntry>;

}  // namespace sdb::search
