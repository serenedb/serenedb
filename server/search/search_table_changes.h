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
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/storage/buffer_manager.hpp>
#include <memory>
#include <span>
#include <string>
#include <vector>

#include "basics/containers/flat_hash_map.h"
#include "catalog/identifiers/object_id.h"
#include "search/search_db_wal.h"

namespace sdb::search {

// Per-search-table, per-transaction ordered op manifest. Statement order is
// preserved so recovery reproduces the live single-trx `_queries` ordering
// between inserts and deletes (delete-then-reinsert, interleaved INSERT/DELETE,
// ...). Consecutive inserts -- inline OR bulk -- accumulate into one insert
// run; only a DELETE seals the run, so the next insert starts a fresh one.
struct LocalTableChangesEntry {
  // One op in the manifest. A DELETE op has `delete_pks`; otherwise it is an
  // insert run carrying inline rows (`collection` + per-chunk generated-PK
  // bases) and/or bulk chunk-file refs (`seg_ids`) -- inline and bulk inserts
  // share one run since neither seals it.
  struct Op {
    std::unique_ptr<duckdb::ColumnDataCollection> collection;
    std::unique_ptr<std::vector<SearchDbWal::InlinePk>> pk_segments;
    std::vector<uint64_t> seg_ids;
    std::vector<std::string> delete_pks;

    bool IsDelete() const noexcept { return !delete_pks.empty(); }
  };

  std::vector<Op> ops;

  // Coalesce a single-threaded INSERT chunk into the current insert run (a new
  // run is started only if the last op is a DELETE). `pk_base` is recorded per
  // chunk only for generated-PK shards.
  void AppendInsertChunk(duckdb::BufferManager& bm,
                         const duckdb::vector<duckdb::LogicalType>& types,
                         duckdb::DataChunk& chunk, bool uses_generated_pk,
                         uint64_t pk_base) {
    auto& op = CurrentInsertRun();
    if (op.collection == nullptr) {
      op.collection = std::make_unique<duckdb::ColumnDataCollection>(bm, types);
      if (uses_generated_pk) {
        op.pk_segments = std::make_unique<std::vector<SearchDbWal::InlinePk>>();
      }
    }
    op.collection->Append(chunk);
    if (op.pk_segments != nullptr) {
      op.pk_segments->push_back({pk_base, chunk.size()});
    }
  }

  // Add bulk chunk-file refs to the current insert run (does not seal it).
  // Batched: one statement's segments resolve the current run once, instead of
  // re-checking the seal condition per seg_id on this hot path.
  void AppendReference(std::span<const uint64_t> seg_ids) {
    if (seg_ids.empty()) {
      return;
    }
    auto& run = CurrentInsertRun();
    run.seg_ids.insert(run.seg_ids.end(), seg_ids.begin(), seg_ids.end());
  }

  // Append a DELETE op, sealing the current insert run. (A delete op is
  // identified by a non-empty delete_pks, so an empty batch is a no-op.)
  void AppendDeletes(std::span<const std::string> pks) {
    if (pks.empty()) {
      return;
    }
    ops.emplace_back().delete_pks.assign(pks.begin(), pks.end());
  }

 private:
  // The insert run to append to: reuse the last op unless it's a DELETE.
  Op& CurrentInsertRun() {
    if (ops.empty() || ops.back().IsDelete()) {
      ops.emplace_back();
    }
    return ops.back();
  }
};

using LocalTableChanges =
  containers::FlatHashMap<ObjectId, LocalTableChangesEntry>;

}  // namespace sdb::search
