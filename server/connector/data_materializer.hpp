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

#include "basics/fwd.h"
#include "catalog/identifiers/object_id.h"
#include "catalog/table_options.h"
#include "rocksdb/utilities/transaction.h"

namespace sdb::connector {

class Materializer {
 public:
  Materializer(velox::memory::MemoryPool& memory_pool,
               const rocksdb::Snapshot* snapshot, rocksdb::DB* db,
               rocksdb::Transaction* transaction,
               rocksdb::ColumnFamilyHandle& cf, velox::RowTypePtr row_type,
               std::vector<catalog::Column::Id> column_oids,
               catalog::Column::Id effective_column_id, ObjectId object_key)
    : _memory_pool{memory_pool},
      _snapshot{snapshot},
      _db{db},
      _transaction{transaction},
      _cf{cf},
      _row_type{std::move(row_type)},
      _column_ids(std::move(column_oids)),
      _effective_column_id(std::move(effective_column_id)),
      _object_key{object_key} {
    SDB_ASSERT((_db != nullptr) != (_transaction != nullptr),
               "Only one data source should be specified");
  }

 protected:
  velox::RowVectorPtr ReadRows(std::span<std::string> row_keys);

  std::unique_ptr<rocksdb::Iterator> CreateIterator();

  velox::VectorPtr ReadColumnKeys(rocksdb::Iterator& it,
                                  std::span<std::string> row_keys,
                                  catalog::Column::Id column_id,
                                  velox::TypeKind kind,
                                  std::string_view column_key);

  template<typename Decoder>
  void IterateColumnKeys(rocksdb::Iterator& it, std::string_view column_key,
                         std::span<std::string> row_keys, const Decoder& func);

  template<typename Decoder>
  void ReadColumnCell(rocksdb::Iterator& it, std::string_view full_key,
                      bool use_seek, const Decoder& func);

  template<bool StoreLast>
  velox::VectorPtr ReadGeneratedColumn(
    rocksdb::Iterator& it, uint64_t max_size, std::string_view column_key,
    [[maybe_unused]] std::optional<std::string>& last_key);

  velox::VectorPtr ReadGeneratedColumnKeys(std::span<std::string> row_keys);

  velox::VectorPtr ReadUnknownColumnKeys(std::span<std::string> row_keys);

  template<velox::TypeKind Kind>
  velox::VectorPtr ReadScalarColumnKeys(rocksdb::Iterator& it,
                                        std::span<std::string> row_keys,
                                        std::string_view column_key);

  template<typename T>
  static void ReadScalarType(std::string_view value, velox::vector_size_t idx,
                             velox::FlatVector<T>& vector);

  velox::memory::MemoryPool& _memory_pool;
  const rocksdb::Snapshot* _snapshot;
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
  bool _is_range = true;
  size_t _produced = 0;
};

}  // namespace sdb::connector
