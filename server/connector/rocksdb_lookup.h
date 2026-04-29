////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2026 SereneDB GmbH, Berlin, Germany
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

#include <absl/container/flat_hash_map.h>
#include <rocksdb/db.h>
#include <rocksdb/utilities/transaction.h>

#include <duckdb/common/types.hpp>
#include <duckdb/common/types/data_chunk.hpp>
#include <functional>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "catalog/table_options.h"
#include "catalog/types.h"
#include "connector/multiget_context.hpp"

namespace sdb::connector {

// Resolves PK bytes into row values from RocksDB. Picks one of three
// strategies per batch:
//
// - batch > kSeekThreshold (100): per-column `rocksdb::Iterator::Seek`
//   over the sorted pks, with iterators CACHED across batches so a
//   multi-batch query amortises the iterator setup + warms the block
//   cache sequentially.
// - batch > MultiGetContext::kMultiGetThreshold: `rocksdb::MultiGet`
//   over a sorted key batch -- fans out block reads in parallel and
//   shares filter-block lookups.
// - small batches: a plain `db->Get(...)` loop.
//
// All strategies pre-sort `pk_bytes` once per batch and reuse a single
// contiguous key buffer (`_multi_get_buffer`) so keys are laid out for
// streaming reads. The caller's original pk order is restored via a
// permutation vector (`_read_idxs`).
class RocksDBLookup {
 public:
  RocksDBLookup(ObjectId table_id, const rocksdb::Snapshot* snapshot,
                std::span<const duckdb::idx_t> projected_columns,
                std::span<const duckdb::LogicalType> projected_types,
                std::span<const catalog::Column::Id> bind_column_ids,
                rocksdb::Transaction* txn);

  // `pk_bytes.size()` rows, one-to-one by position into `output`.
  void Lookup(std::span<const std::string_view> pk_bytes,
              duckdb::DataChunk& output);

 private:
  using DecodeFn =
    std::function<void(size_t original_idx, std::string_view value)>;

  // Dispatch the per-column read to the best strategy for `pk_bytes`'s
  // size. `column_key_prefix` = `[ObjectId][ColumnId]`, exactly
  // `kColumnKeySize` bytes.
  void DispatchColumnRead(std::string_view column_key_prefix,
                          catalog::Column::Id column_id,
                          std::span<const std::string_view> pk_bytes,
                          const DecodeFn& decode);

  // Simple loop: one `db->Get(...)` per pk. Smallest batches.
  void IterateColumnKeys(std::string_view column_key_prefix,
                         std::span<const std::string_view> pk_bytes,
                         const DecodeFn& decode);

  // MultiGet of up to `MultiGetContext::kBatchSize` sorted keys at a
  // time. `_multi_get_buffer` must have been prepared.
  void MultiGetIterateColumnKeys(std::string_view column_key_prefix,
                                 std::span<const std::string_view> pk_bytes,
                                 const DecodeFn& decode);

  // Seek on a per-column `rocksdb::Iterator`, kept alive across
  // Lookup() calls so sequential batches skip setup. Large batches.
  void SeekIterateColumnKeys(std::string_view column_key_prefix,
                             catalog::Column::Id column_id,
                             std::span<const std::string_view> pk_bytes,
                             const DecodeFn& decode);

  // Sort `pk_bytes` indices into `_read_idxs` and stage sorted pk bytes
  // into `_multi_get_buffer`, leaving `kColumnKeySize` gaps at the head
  // of each entry for the column-key prefix. Idempotent within a batch.
  void PrepareSortedBatch(std::span<const std::string_view> pk_bytes);

  ObjectId _table_id;
  rocksdb::ReadOptions _read_options;
  rocksdb::DB* _db = nullptr;
  rocksdb::ColumnFamilyHandle* _cf = nullptr;
  rocksdb::Transaction* _txn = nullptr;
  MultiGetContext _multiget_ctx;

  // Projection layout (fixed at construction).
  std::vector<duckdb::idx_t> _projected_columns;
  std::vector<duckdb::LogicalType> _projected_types;
  std::vector<catalog::Column::Id> _bind_column_ids;

  // Simple-get scratch buffer (single-value reads).
  std::string _value_buffer;

  // Per-batch sort + arena.
  bool _new_batch = true;
  std::vector<size_t> _read_idxs;
  std::string _multi_get_buffer;
  std::vector<rocksdb::Slice> _key_slices;

  // Iterator cache across batches (one per column_id).
  containers::FlatHashMap<catalog::Column::Id,
                          std::unique_ptr<rocksdb::Iterator>>
    _iterators;
};

}  // namespace sdb::connector
