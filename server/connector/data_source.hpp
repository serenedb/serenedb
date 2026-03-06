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
#include <velox/vector/FlatVector.h>

#include "basics/fwd.h"
#include "catalog/identifiers/object_id.h"
#include "catalog/table_options.h"
#include "connector/key_utils.hpp"
#include "rocksdb/db.h"
#include "rocksdb/utilities/transaction.h"
#include "rocksdb_engine_catalog/rocksdb_option_feature.h"
#include "rocksdb_engine_catalog/rocksdb_utils.h"

namespace sdb::connector {

class SereneDBConnectorSplit;

class RocksDBFullScanDataSource : public velox::connector::DataSource {
 public:
  virtual void addSplit(
    std::shared_ptr<velox::connector::ConnectorSplit> split) override = 0;
  std::optional<velox::RowVectorPtr> next(
    uint64_t size, velox::ContinueFuture& future) override;
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

 protected:
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

  void addSplit(
    std::shared_ptr<velox::connector::ConnectorSplit> split) override;

 protected:
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
  void addSplit(
    std::shared_ptr<velox::connector::ConnectorSplit> split) override;

  //  private:
  rocksdb::DB& _db;  // NOLINT
};

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
                                          velox::ContinueFuture& future) final {
    SDB_ASSERT(size);

    if (!_current_split) {
      return nullptr;
    }

    if (!_values) [[unlikely]] {
      _current_split.reset();
      return nullptr;
    }
    SDB_ASSERT(_values->size() > 0,
               "Case of empty filters should be processed in connector");

    SDB_PRINT("row_type=", _row_type->toString());
    const auto num_columns = _row_type->size();

    // For now, we do not reject filters, so this data source is reading at
    // least 1 column, even for queries like
    // SELECT count(*) FROM t WHERE a = 1;
    // In this example column a will be read. When we'll properly reject some
    // filters, this assert may be failed, so we have to store and use effective
    // column id.
    SDB_ASSERT(num_columns > 0);

    const auto total_points = static_cast<size_t>(_values->size());
    const auto batch_size =
      std::min(static_cast<size_t>(size), total_points - _offset);

    if (batch_size == 0) {
      _current_split.reset();
      return nullptr;
    }

    const size_t total_keys = batch_size * num_columns;

    // Build keys for [_offset, _offset + batch_size), laid out as
    // [point_idx * num_columns + col_idx]
    std::vector<std::string> keys(total_keys);
    for (size_t point_idx = 0; point_idx < batch_size; ++point_idx) {
      for (size_t col_idx = 0; col_idx < num_columns; ++col_idx) {
        std::string key = key_utils::PrepareTableKey(_object_key);
        key_utils::AppendColumnKey(key, _column_ids[col_idx]);
        primary_key::Create(*_values, _offset + point_idx, key);
        keys[point_idx * num_columns + col_idx] = std::move(key);
      }
    }

    std::vector<rocksdb::Slice> key_slices(total_keys);
    for (size_t i = 0; i < total_keys; ++i) {
      key_slices[i] = keys[i];
    }

    std::vector<rocksdb::ColumnFamilyHandle*> cfs(total_keys, &_cf);
    std::vector<std::string> raw_values(total_keys);
    const auto statuses = DoMultiGet(cfs, key_slices, raw_values);

    std::vector<velox::VectorPtr> columns;
    columns.reserve(num_columns);
    for (size_t column_idx = 0; column_idx < num_columns; ++column_idx) {
      const auto& type = _row_type->childAt(column_idx);
      columns.emplace_back(VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
        ReadColumnValues, type->kind(), column_idx, batch_size, statuses,
        raw_values));
    }

    _produced += batch_size;
    _offset += batch_size;
    if (_offset >= total_points) {
      _current_split.reset();
      _offset = 0;
    }
    return std::make_shared<velox::RowVector>(&_memory_pool, _row_type, nullptr,
                                              batch_size, std::move(columns));
  }

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

  // Perform the batched read. Keys are laid out as [point * num_columns + col].
  virtual std::vector<rocksdb::Status> DoMultiGet(
    const std::vector<rocksdb::ColumnFamilyHandle*>& cfs,
    const std::vector<rocksdb::Slice>& key_slices,
    std::vector<std::string>& raw_values) = 0;

 private:
  // Builds a FlatVector<T> of size num_points for column col_idx.
  template<velox::TypeKind Kind>
  velox::VectorPtr ReadColumnValues(
    size_t col_idx, size_t num_points,
    const std::vector<rocksdb::Status>& statuses,
    const std::vector<std::string>& values) {
    using T = typename velox::TypeTraits<Kind>::NativeType;
    static constexpr std::string_view kTrueValue{"\1", 1};
    const size_t num_columns = _row_type->size();

    auto result = velox::BaseVector::create<velox::FlatVector<T>>(
      velox::Type::create<Kind>(), num_points, &_memory_pool);

    for (size_t point_idx = 0; point_idx < num_points; ++point_idx) {
      const size_t idx = point_idx * num_columns + col_idx;
      const auto& status = statuses[idx];
      const auto& value = values[idx];

      if (!status.ok() && !status.IsNotFound()) {
        auto res = rocksutils::ConvertStatus(status);
        SDB_THROW(res.errorNumber(),
                  "Failed to read value by PK with point data source: ",
                  res.errorMessage());
      }
      if (status.IsNotFound()) {
        continue;
      } else {
        if constexpr (std::is_same_v<T, velox::StringView>) {
          const size_t offset = value[0] == 0 ? 1 : 0;
          result->set(point_idx, velox::StringView(value.data() + offset,
                                                   value.size() - offset));
        } else if constexpr (std::is_same_v<T, bool>) {
          SDB_ASSERT(
            value.size() == kTrueValue.size(),
            "RocksDBDataSource: unexpected value size for bool column");
          result->set(point_idx, value == kTrueValue);
        } else {
          SDB_ASSERT(
            value.size() == sizeof(T),
            "RocksDBDataSource: unexpected value size for scalar column");
          T tmp;
          memcpy(&tmp, value.data(), sizeof(T));
          result->set(point_idx, tmp);
        }
      }
    }
    return result;
  }

  velox::memory::MemoryPool& _memory_pool;
  rocksdb::ColumnFamilyHandle& _cf;
  velox::RowTypePtr _row_type;
  std::vector<catalog::Column::Id> _column_ids;
  ObjectId _object_key;
  std::shared_ptr<velox::connector::ConnectorSplit> _current_split;
  uint64_t _produced = 0;
  size_t _offset = 0;
  velox::RowVectorPtr _values;
};

class RocksDBRYOWPointLookupDataSource final
  : public RocksDBPointLookupDataSource {
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
      _transaction{transaction} {}

 protected:
  std::vector<rocksdb::Status> DoMultiGet(
    const std::vector<rocksdb::ColumnFamilyHandle*>& cfs,
    const std::vector<rocksdb::Slice>& key_slices,
    std::vector<std::string>& raw_values) override {
    rocksdb::ReadOptions ro;
    ro.async_io = IsIOUringEnabled();
    return _transaction.MultiGet(ro, cfs, key_slices, &raw_values);
  }

 private:
  rocksdb::Transaction& _transaction;
};

class RocksDBSnapshotPointLookupDataSource final
  : public RocksDBPointLookupDataSource {
 public:
  RocksDBSnapshotPointLookupDataSource(
    velox::memory::MemoryPool& memory_pool, rocksdb::DB& db,
    rocksdb::ColumnFamilyHandle& cf, velox::RowTypePtr row_type,
    std::vector<catalog::Column::Id> column_ids, ObjectId object_key,
    const rocksdb::Snapshot* snapshot, velox::RowVectorPtr values)
    : RocksDBPointLookupDataSource{memory_pool,         cf,
                                   std::move(row_type), std::move(column_ids),
                                   object_key,          std::move(values)},
      _db{db},
      _snapshot{snapshot} {}

 protected:
  std::vector<rocksdb::Status> DoMultiGet(
    const std::vector<rocksdb::ColumnFamilyHandle*>& cfs,
    const std::vector<rocksdb::Slice>& key_slices,
    std::vector<std::string>& raw_values) override {
    rocksdb::ReadOptions ro;
    ro.async_io = IsIOUringEnabled();
    ro.snapshot = _snapshot;
    return _db.MultiGet(ro, cfs, key_slices, &raw_values);
  }

 private:
  rocksdb::DB& _db;
  const rocksdb::Snapshot* _snapshot;
};
}  // namespace sdb::connector
