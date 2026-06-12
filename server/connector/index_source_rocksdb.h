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
#include <duckdb/common/vector.hpp>
#include <functional>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "catalog/identifiers/object_id.h"
#include "catalog/table_options.h"
#include "connector/index_source.h"
#include "connector/multiget_context.hpp"

namespace sdb::connector {

// Resolves PK bytes into row values from RocksDB. Per call we pick a strategy
// by batch size: Seek over sorted PKs (large), MultiGet (medium), or plain
// per-row Get (small). The sorted strategies stamp [ObjectId][ColumnId] into
// each PK's pre-reserved kPrefixGap region (PrimaryKeysBytes::Append leaves
// 16 bytes before the pk bytes) so no staging copy of pk bytes is needed.
class RocksDBIndexSource : public IndexSource {
 public:
  RocksDBIndexSource(ObjectId table_id, const rocksdb::Snapshot* snapshot,
                     std::span<const duckdb::idx_t> projected_columns,
                     std::span<const duckdb::LogicalType> projected_types,
                     std::span<const catalog::Column::Id> bind_column_ids,
                     rocksdb::Transaction* txn);

  PrimaryKeyBatch CreatePkBatch() const final {
    return PrimaryKeyBatch{std::in_place_type<PrimaryKeysBytes>};
  }
  duckdb::idx_t Materialize(duckdb::ClientContext& context,
                            PrimaryKeyBatch& batch, duckdb::idx_t start,
                            duckdb::idx_t count,
                            duckdb::DataChunk& output) final;

  // View-caller path: writes each projected column into the matching slot of
  // `per_col_targets` rather than into an output DataChunk.
  void LookupInto(std::span<const std::string_view> pk_bytes,
                  std::span<duckdb::Vector* const> per_col_targets);

 private:
  using DecodeFn =
    std::function<void(size_t original_idx, std::string_view value)>;

  // `column_key_prefix` is exactly kPrefixGap bytes ([ObjectId][ColumnId]).
  void DispatchColumnRead(std::string_view column_key_prefix,
                          catalog::Column::Id column_id,
                          std::span<const std::string_view> pk_bytes,
                          const DecodeFn& decode);

  void IterateColumnKeys(std::string_view column_key_prefix,
                         std::span<const std::string_view> pk_bytes,
                         const DecodeFn& decode);

  void MultiGetIterateColumnKeys(std::string_view column_key_prefix,
                                 std::span<const std::string_view> pk_bytes,
                                 const DecodeFn& decode);

  void SeekIterateColumnKeys(std::string_view column_key_prefix,
                             catalog::Column::Id column_id,
                             std::span<const std::string_view> pk_bytes,
                             const DecodeFn& decode);

  // Idempotent within a batch (`_new_batch` guards).
  void EnsureSortedBatch(std::span<const std::string_view> pk_bytes);

  ObjectId _table_id;
  rocksdb::ReadOptions _read_options;
  rocksdb::DB* _db = nullptr;
  rocksdb::ColumnFamilyHandle* _cf = nullptr;
  rocksdb::Transaction* _txn = nullptr;
  MultiGetContext _multiget_ctx;

  std::vector<duckdb::idx_t> _projected_columns;
  std::vector<duckdb::LogicalType> _projected_types;
  std::vector<catalog::Column::Id> _bind_column_ids;

  std::string _value_buffer;

  bool _new_batch = true;
  std::vector<size_t> _read_idxs;
  std::vector<rocksdb::Slice> _key_slices;

  // Cached iterators across batches, keyed by column_id.
  containers::FlatHashMap<catalog::Column::Id,
                          std::unique_ptr<rocksdb::Iterator>>
    _iterators;
};

}  // namespace sdb::connector
