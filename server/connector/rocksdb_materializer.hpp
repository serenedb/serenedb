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

#include <velox/common/memory/MemoryPool.h>
#include <velox/connectors/Connector.h>
#include <velox/vector/ComplexVector.h>

#include <span>
#include <string>

#include "basics/fwd.h"
#include "catalog/identifiers/object_id.h"
#include "catalog/table_options.h"
#include "connector/multiget_context.hpp"
#include "rocksdb/utilities/transaction.h"

namespace sdb::connector {

class RocksDBMaterializer {
 public:
  RocksDBMaterializer(velox::memory::MemoryPool& memory_pool,
                      const rocksdb::Snapshot* snapshot, rocksdb::DB* db,
                      rocksdb::Transaction* transaction,
                      rocksdb::ColumnFamilyHandle& cf,
                      velox::RowTypePtr row_type,
                      std::vector<catalog::Column::Id> column_oids,
                      catalog::Column::Id effective_column_id,
                      ObjectId object_key);

  velox::RowVectorPtr ReadRows(std::span<const std::string> row_keys,
                               velox::VectorPtr scores,
                               std::vector<velox::VectorPtr> offsets_per_field);

 protected:
  const std::string& ReadValue(std::string_view full_key);

  velox::VectorPtr ReadColumnKeys(std::span<const std::string> row_keys,
                                  catalog::Column::Id column_id,
                                  const velox::TypePtr& type,
                                  std::string_view column_key);

  void PrepareSortedBatch(std::span<const std::string> row_keys);

  template<typename Decoder>
  void DispatchColumnRead(std::string_view column_key,
                          catalog::Column::Id column_id,
                          std::span<const std::string> row_keys,
                          const Decoder& func);

  template<typename Decoder>
  void IterateColumnKeys(std::string_view column_key,
                         std::span<const std::string> row_keys,
                         const Decoder& func);

  template<typename Decoder, typename MultiGetSource>
  void MultiGetIterateColumnKeys(std::string_view column_key,
                                 std::span<const std::string> row_keys,
                                 MultiGetSource& src, const Decoder& func);

  template<typename Decoder, typename MultiGetSource>
  void SeekIterateColumnKeys(std::string_view column_key,
                             catalog::Column::Id column_id,
                             std::span<const std::string> row_keys,
                             MultiGetSource& src, const Decoder& func);

  velox::VectorPtr ReadGeneratedColumnKeys(
    std::span<const std::string> row_keys);

  velox::VectorPtr ReadUnknownColumnKeys(std::span<const std::string> row_keys);

  velox::memory::MemoryPool& _memory_pool;
  rocksdb::DB* _db;
  rocksdb::Transaction* _transaction;
  rocksdb::ColumnFamilyHandle& _cf;
  velox::RowTypePtr _row_type;
  std::vector<catalog::Column::Id> _column_ids;
  // Column ID to use for iteration when the requested column is stored in the
  // key (e.g., kGeneratedPKId). This points to a column whose values are
  // stored in RocksDB as *values*, not inside *keys*. It's convenient to
  // store it here for scans where we need only columns that are stored as
  // parts of the key. Tables with only such columns are tables without
  // columns at all *for now*, this case is handled in SqlAnalyzer code, such
  // scans are replaced with empty Values node.
  catalog::Column::Id _effective_column_id;
  ObjectId _object_key;
  size_t _produced = 0;
  std::string _value_buffer;
  rocksdb::ReadOptions _read_options;
  MultiGetContext _multiget_ctx;
  std::unique_ptr<velox::HashStringAllocator> _multiget_buffer_allocator;
  velox::HashStringAllocator::Header* _multi_get_buffer = nullptr;
  containers::FlatHashMap<catalog::Column::Id,
                          std::unique_ptr<rocksdb::Iterator>>
    _iterators;
  bool _new_batch = true;
  std::vector<size_t> _read_idxs;
  std::vector<rocksdb::Slice> _key_slices;
};

}  // namespace sdb::connector
