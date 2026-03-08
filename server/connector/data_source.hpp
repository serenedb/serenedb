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
#include <velox/type/Type.h>
#include <velox/vector/FlatVector.h>

#include "catalog/identifiers/object_id.h"
#include "catalog/table_options.h"
#include "rocksdb/db.h"
#include "rocksdb/utilities/transaction.h"
#include "rocksdb_engine_catalog/rocksdb_option_feature.h"

namespace sdb::connector {

class SereneDBConnectorSplit;

class MultiGetContext {
 public:
  // TODO benchmark and choose best threshold
  static constexpr size_t kThreshold = 1;
  static constexpr size_t kBatchSize = 32;

  template<
    std::invocable<rocksdb::ColumnFamilyHandle&, const rocksdb::Slice&,
                   rocksdb::PinnableSlice*, rocksdb::Status*>
      SingleFn,
    std::invocable<rocksdb::ColumnFamilyHandle&, size_t, const rocksdb::Slice*,
                   rocksdb::PinnableSlice*, rocksdb::Status*>
      BatchFn>
  void MultiGet(rocksdb::ColumnFamilyHandle& cf,
                const std::vector<rocksdb::Slice>& keys,
                std::vector<std::string>& values,
                std::vector<rocksdb::Status>& statuses, SingleFn&& single_fn,
                BatchFn&& batch_fn) {
    const size_t n = keys.size();
    values.resize(n);
    statuses.resize(n);

    if (n <= kThreshold) {
      for (size_t i = 0; i < n; ++i) {
        _pinnable[0].Reset();
        single_fn(cf, keys[i], _pinnable.data(), statuses.data() + i);
        ExtractValues(1, i, values);
      }
      return;
    }

    for (size_t start = 0; start < n; start += kBatchSize) {
      const size_t batch_size = std::min(kBatchSize, n - start);
      for (size_t i = 0; i < batch_size; ++i) {
        _pinnable[i].Reset();
      }
      batch_fn(cf, batch_size, keys.data() + start, _pinnable.data(),
               statuses.data() + start);
      ExtractValues(batch_size, start, values);
    }
  }

 private:
  void ExtractValues(size_t count, size_t dest_start,
                     std::vector<std::string>& values);

  std::array<rocksdb::PinnableSlice, kBatchSize> _pinnable;
};

class RocksDBFullScanDataSource : public velox::connector::DataSource {
 public:
  virtual void addSplit(
    std::shared_ptr<velox::connector::ConnectorSplit> split) override = 0;
  std::optional<velox::RowVectorPtr> next(uint64_t size,
                                          velox::ContinueFuture& future) final;
  void addDynamicFilter(
    velox::column_index_t output_channel,
    const std::shared_ptr<velox::common::Filter>& filter) final;
  uint64_t getCompletedBytes() final;
  uint64_t getCompletedRows() final;
  std::unordered_map<std::string, velox::RuntimeMetric> getRuntimeStats() final;
  void cancel() final;

 protected:
  RocksDBFullScanDataSource(velox::memory::MemoryPool& memory_pool,
                            rocksdb::ColumnFamilyHandle& cf,
                            velox::RowTypePtr row_type,
                            std::vector<catalog::Column::Id> column_ids,
                            catalog::Column::Id effective_column_id,
                            ObjectId object_key,
                            const rocksdb::Snapshot* snapshot);

  template<std::invocable<const rocksdb::ReadOptions&> CreateFn>
  void InitIterators(CreateFn&& create);

  velox::memory::MemoryPool& _memory_pool;
  rocksdb::ColumnFamilyHandle& _cf;
  const rocksdb::Snapshot* _snapshot;
  ObjectId _object_key;
  std::vector<catalog::Column::Id> _column_ids;

 private:
  static constexpr size_t kTablePrefixSize = sizeof(ObjectId);

  velox::VectorPtr ReadColumn(velox::column_index_t col_idx, uint64_t max_size);

  template<velox::TypeKind Kind>
  velox::VectorPtr ReadScalarColumn(rocksdb::Iterator& it, uint64_t max_size);

  velox::VectorPtr ReadUnknownColumn(rocksdb::Iterator& it, uint64_t max_size);

  velox::VectorPtr ReadColumnFromKey(rocksdb::Iterator& it, uint64_t max_size);

  template<
    std::invocable<uint64_t, std::string_view, std::string_view> Callback>
  uint64_t IterateColumn(rocksdb::Iterator& it, uint64_t max_size,
                         const Callback& func);

 private:
  velox::RowTypePtr _row_type;
  std::vector<std::string> _column_keys;
  std::string _upper_bound_keys_data;
  std::vector<rocksdb::Slice> _upper_bound_slices;
  std::vector<std::unique_ptr<rocksdb::Iterator>> _iterators;
  // Column ID to use for iteration when the requested column is stored in the
  // key (e.g., kGeneratedPKId). This points to a column whose values are stored
  // in RocksDB as *values*, not inside *keys*. It's convenient to store it here
  // for scans where we need only columns that are stored as parts of the key.
  // Tables with only such columns are tables without columns at all *for now*,
  // this case is handled in SqlAnalyzer code, such scans are replaced with
  // empty Values node.
  catalog::Column::Id _effective_column_id;
  std::shared_ptr<velox::connector::ConnectorSplit> _current_split;
  uint64_t _produced = 0;
};

// Read-Your-Own-Write
class RocksDBRYOWFullScanDataSource : public RocksDBFullScanDataSource {
 public:
  RocksDBRYOWFullScanDataSource(velox::memory::MemoryPool& memory_pool,
                                rocksdb::Transaction& transaction,
                                rocksdb::ColumnFamilyHandle& cf,
                                velox::RowTypePtr row_type,
                                std::vector<catalog::Column::Id> column_ids,
                                catalog::Column::Id effective_column_id,
                                ObjectId object_key);

  void addSplit(std::shared_ptr<velox::connector::ConnectorSplit> split) final;

 private:
  rocksdb::Transaction& _transaction;
};

class RocksDBSnapshotFullScanDataSource : public RocksDBFullScanDataSource {
 public:
  RocksDBSnapshotFullScanDataSource(
    velox::memory::MemoryPool& memory_pool, rocksdb::DB& db,
    rocksdb::ColumnFamilyHandle& cf, velox::RowTypePtr row_type,
    std::vector<catalog::Column::Id> column_ids,
    catalog::Column::Id effective_column_id, ObjectId object_key,
    const rocksdb::Snapshot* snapshot = nullptr);
  void addSplit(std::shared_ptr<velox::connector::ConnectorSplit> split) final;

 private:
  rocksdb::DB& _db;
};

template<typename Derived>
class RocksDBPointLookupDataSource : public velox::connector::DataSource {
 public:
  void addDynamicFilter(velox::column_index_t,
                        const std::shared_ptr<velox::common::Filter>&) final {
    VELOX_UNSUPPORTED();
  }
  uint64_t getCompletedBytes() final { return 0; }
  uint64_t getCompletedRows() final { return _produced; }
  std::unordered_map<std::string, velox::RuntimeMetric> getRuntimeStats()
    final {
    return {};
  }
  void cancel() final {}

  void addSplit(std::shared_ptr<velox::connector::ConnectorSplit> split) final {
    SDB_ENSURE(split, ERROR_INTERNAL, "RocksDBDataSource: split is null");
    if (_current_split) {
      SDB_THROW(ERROR_INTERNAL,
                "RocksDBDataSource: a split is already being processed");
    }
    _current_split = std::move(split);
    _offset = 0;
  }

  std::optional<velox::RowVectorPtr> next(uint64_t size,
                                          velox::ContinueFuture& future) final;

 protected:
  RocksDBPointLookupDataSource(velox::memory::MemoryPool& memory_pool,
                               rocksdb::ColumnFamilyHandle& cf,
                               velox::RowTypePtr row_type,
                               std::vector<catalog::Column::Id> column_ids,
                               ObjectId object_key, velox::RowVectorPtr values)
    : _memory_pool{memory_pool},
      _cf{cf},
      _row_type{std::move(row_type)},
      _column_ids{std::move(column_ids)},
      _object_key{object_key},
      _values{std::move(values)} {}

 private:
  velox::memory::MemoryPool& _memory_pool;
  rocksdb::ColumnFamilyHandle& _cf;
  velox::RowTypePtr _row_type;
  std::vector<catalog::Column::Id> _column_ids;
  ObjectId _object_key;
  std::shared_ptr<velox::connector::ConnectorSplit> _current_split;
  uint64_t _produced = 0;
  size_t _offset = 0;
  velox::RowVectorPtr _values;
  std::vector<std::string> _keys;
  std::vector<rocksdb::Slice> _key_slices;
  std::vector<std::string> _raw_values;
  std::vector<rocksdb::Status> _statuses;
  MultiGetContext _ctx;
};

class RocksDBRYOWPointLookupDataSource final
  : public RocksDBPointLookupDataSource<RocksDBRYOWPointLookupDataSource> {
 public:
  RocksDBRYOWPointLookupDataSource(velox::memory::MemoryPool& memory_pool,
                                   rocksdb::Transaction& transaction,
                                   rocksdb::ColumnFamilyHandle& cf,
                                   velox::RowTypePtr row_type,
                                   std::vector<catalog::Column::Id> column_ids,
                                   ObjectId object_key,
                                   velox::RowVectorPtr values)
    : RocksDBPointLookupDataSource{memory_pool,         cf,
                                   std::move(row_type), std::move(column_ids),
                                   object_key,          std::move(values)},
      _transaction{transaction} {
    _ro.async_io = IsIOUringEnabled();
  }

  void DoGet(rocksdb::ColumnFamilyHandle& cf, const rocksdb::Slice& key,
             rocksdb::PinnableSlice* value, rocksdb::Status* status) {
    *status = _transaction.Get(_ro, &cf, key, value);
  }

  void DoMultiGetBatch(rocksdb::ColumnFamilyHandle& cf, size_t keys_number,
                       const rocksdb::Slice* keys,
                       rocksdb::PinnableSlice* values,
                       rocksdb::Status* statuses) {
    _transaction.MultiGet(_ro, &cf, keys_number, keys, values, statuses);
  }

 private:
  rocksdb::Transaction& _transaction;
  rocksdb::ReadOptions _ro;
};

class RocksDBSnapshotPointLookupDataSource final
  : public RocksDBPointLookupDataSource<RocksDBSnapshotPointLookupDataSource> {
 public:
  RocksDBSnapshotPointLookupDataSource(
    velox::memory::MemoryPool& memory_pool, rocksdb::DB& db,
    rocksdb::ColumnFamilyHandle& cf, velox::RowTypePtr row_type,
    std::vector<catalog::Column::Id> column_ids, ObjectId object_key,
    const rocksdb::Snapshot* snapshot, velox::RowVectorPtr values)
    : RocksDBPointLookupDataSource{memory_pool,         cf,
                                   std::move(row_type), std::move(column_ids),
                                   object_key,          std::move(values)},
      _db{db} {
    _ro.async_io = IsIOUringEnabled();
    _ro.snapshot = snapshot;
  }

  void DoGet(rocksdb::ColumnFamilyHandle& cf, const rocksdb::Slice& key,
             rocksdb::PinnableSlice* value, rocksdb::Status* status) {
    *status = _db.Get(_ro, &cf, key, value);
  }

  void DoMultiGetBatch(rocksdb::ColumnFamilyHandle& cf, size_t keys_number,
                       const rocksdb::Slice* keys,
                       rocksdb::PinnableSlice* values,
                       rocksdb::Status* statuses) {
    _db.MultiGet(_ro, &cf, keys_number, keys, values, statuses);
  }

 private:
  rocksdb::DB& _db;
  rocksdb::ReadOptions _ro;
};

}  // namespace sdb::connector
