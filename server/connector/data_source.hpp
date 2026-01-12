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

class SereneDBConnectorSplit;

class RocksDBDataSource final : public velox::connector::DataSource {
 public:
  RocksDBDataSource(velox::memory::MemoryPool& memory_pool,
                    // use just snapshot for now. But maybe we will need to have
                    // this class template (or use some wrapper) to work with
                    // WriteBatchWithindex or plain DB with snapshot
                    const rocksdb::Snapshot* snapshot, rocksdb::DB& db,
                    rocksdb::ColumnFamilyHandle& cf, velox::RowTypePtr row_type,
                    std::vector<catalog::Column::Id> column_ids,
                    ObjectId object_key);

  void addSplit(std::shared_ptr<velox::connector::ConnectorSplit> split) final;
  std::optional<velox::RowVectorPtr> next(uint64_t size,
                                          velox::ContinueFuture& future) final;
  void addDynamicFilter(
    velox::column_index_t output_channel,
    const std::shared_ptr<velox::common::Filter>& filter) final;
  uint64_t getCompletedBytes() final;
  uint64_t getCompletedRows() final;
  std::unordered_map<std::string, velox::RuntimeMetric> getRuntimeStats() final;
  void cancel() final;

 private:
  velox::VectorPtr ReadColumn(rocksdb::Iterator& it, uint64_t max_size,
                              std::string_view column_key,
                              const velox::TypePtr& type,
                              catalog::Column::Id column_id,
                              size_t table_prefix_size, std::string* last_key);

  template<velox::TypeKind Kind>
  velox::VectorPtr ReadScalarColumn(rocksdb::Iterator& it, uint64_t max_size,
                                    std::string_view column_key,
                                    std::string* last_key);
  velox::VectorPtr ReadUnknownColumn(rocksdb::Iterator& it, uint64_t max_size,
                                     std::string_view column_key,
                                     std::string* last_key);

  velox::VectorPtr ReadColumnFromKey(rocksdb::Iterator& it, uint64_t max_size,
                                     std::string_view column_key,
                                     size_t table_prefix_size,
                                     std::string* last_key);

  template<typename Callback>
  uint64_t IterateColumn(rocksdb::Iterator& it, uint64_t max_size,
                         std::string_view column_key, const Callback& func,
                         std::string* last_key);

  std::unique_ptr<rocksdb::Iterator> CreateColumnIterator(
    const std::string_view column_key,
    const rocksdb::ReadOptions& read_options);

  velox::memory::MemoryPool& _memory_pool;
  const rocksdb::Snapshot* _snapshot;
  rocksdb::DB& _db;
  rocksdb::ColumnFamilyHandle& _cf;
  velox::RowTypePtr _row_type;
  std::vector<catalog::Column::Id> _column_ids;
  std::shared_ptr<velox::connector::ConnectorSplit> _current_split;
  ObjectId _object_key;
  std::string _last_read_key;
  uint64_t _produced{0};
};

}  // namespace sdb::connector
