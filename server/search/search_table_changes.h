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

#include "basics/assert.h"
#include "basics/containers/flat_hash_map.h"
#include "catalog/identifiers/object_id.h"
#include "search/search_db_wal.h"

namespace sdb::search {

// Per-search-table, per-transaction ordered op manifest.
struct LocalTableChangesEntry {
  struct Op {
    std::unique_ptr<duckdb::ColumnDataCollection> collection;
    std::unique_ptr<std::vector<SearchDbWal::InlinePk>> pk_segments;
    std::vector<SearchDbWal::PendingChunk> chunks;
    std::vector<std::string> delete_pks;
    bool truncate = false;

    bool IsDelete() const noexcept { return !delete_pks.empty(); }
    bool IsTruncate() const noexcept { return truncate; }
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

  // Move a bulk statement's chunk files into the current insert run (does not
  // seal it). Batched: one statement's chunks resolve the current run once,
  // instead of re-checking the seal condition per chunk on this hot path.
  void AppendReference(std::vector<SearchDbWal::PendingChunk>&& chunks) {
    if (chunks.empty()) {
      return;
    }
    auto& run = CurrentInsertRun();
    if (run.chunks.empty()) {
      run.chunks = std::move(chunks);  // fresh run: take the whole vector
      return;
    }
    // A prior bulk statement already coalesced into this run; append.
    for (auto& c : chunks) {
      run.chunks.push_back(std::move(c));
    }
  }

  // Append a DELETE op, sealing the current insert run. (A delete op is
  // identified by a non-empty delete_pks, so an empty batch is a no-op.)
  void AppendDeletes(std::span<const std::string> pks) {
    if (pks.empty()) {
      return;
    }
    ops.emplace_back().delete_pks.assign(pks.begin(), pks.end());
  }

  // Append a TRUNCATE op (wipe the shard). Autocommit-only, so it is the sole
  // op in the transaction; it still seals any current run defensively.
  void AppendTruncate() { ops.emplace_back().truncate = true; }

  // A TRUNCATE is always the sole op (autocommit-only), so the first op decides
  // it; assert that invariant.
  bool HasTruncate() const noexcept {
    if (ops.empty() || !ops.front().IsTruncate()) {
      return false;
    }
    SDB_ASSERT(ops.size() == 1,
               "TRUNCATE must be the only op in its transaction");
    return true;
  }

 private:
  // The insert run to append to: reuse the last op unless it's a DELETE or
  // TRUNCATE (both seal the run).
  Op& CurrentInsertRun() {
    if (ops.empty() || ops.back().IsDelete() || ops.back().IsTruncate()) {
      ops.emplace_back();
    }
    return ops.back();
  }
};

using LocalTableChanges =
  containers::FlatHashMap<ObjectId, LocalTableChangesEntry>;

}  // namespace sdb::search
