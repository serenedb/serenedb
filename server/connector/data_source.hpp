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
#include "rocksdb_engine_catalog/rocksdb_utils.h"

namespace sdb::connector {

class SereneDBConnectorSplit;

class RocksDBDataSource : public velox::connector::DataSource {
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
  RocksDBDataSource(velox::memory::MemoryPool& memory_pool,
                    rocksdb::ColumnFamilyHandle& cf, velox::RowTypePtr row_type,
                    std::vector<catalog::Column::Id> column_ids,
                    catalog::Column::Id effective_column_id,
                    ObjectId object_key, const rocksdb::Snapshot* snapshot);

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

// RocksDB Read Your Own Writes DataSource
class RocksDBRYOWFullScanDataSource : public RocksDBDataSource {
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

class RocksDBSnapshotFullScanDataSource : public RocksDBDataSource {
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

// TODO do not derive full scan, for now just to not reimplement most of the
// things
// TODO save constant expr here for eq comparison, use it in next for fast
// calling multiget
class RocksDBRYOWMultiGetDataSource final
  : public RocksDBRYOWFullScanDataSource {
 public:
  RocksDBRYOWMultiGetDataSource(velox::memory::MemoryPool& memory_pool,
                                rocksdb::Transaction& transaction,
                                rocksdb::ColumnFamilyHandle& cf,
                                velox::RowTypePtr row_type,
                                std::vector<catalog::Column::Id> column_ids,
                                catalog::Column::Id effective_column_id,
                                ObjectId object_key)
    : RocksDBRYOWFullScanDataSource{
        memory_pool, transaction,         cf,        row_type,
        column_ids,  effective_column_id, object_key} {}
  RocksDBRYOWMultiGetDataSource(const RocksDBRYOWMultiGetDataSource&) = default;
  RocksDBRYOWMultiGetDataSource(RocksDBRYOWMultiGetDataSource&&) = default;

  void addSplit(std::shared_ptr<velox::connector::ConnectorSplit> split) final {
    // VELOX_UNSUPPORTED();
    // Just do nothing? Or store for check that next is not called before next
  }
  std::optional<velox::RowVectorPtr> next(uint64_t size,
                                          velox::ContinueFuture& future) final {
    VELOX_UNSUPPORTED();
    return {};
  }
};

class RocksDBSnapshotMultiGetDataSource final
  : public RocksDBSnapshotFullScanDataSource {
 public:
  RocksDBSnapshotMultiGetDataSource(
    velox::memory::MemoryPool& memory_pool, rocksdb::DB& db,
    rocksdb::ColumnFamilyHandle& cf, velox::RowTypePtr row_type,
    std::vector<catalog::Column::Id> column_ids,
    catalog::Column::Id effective_column_id, ObjectId object_key,
    const rocksdb::Snapshot* snapshot, velox::VectorPtr value)
    : RocksDBSnapshotFullScanDataSource{memory_pool, db,
                                        cf,          row_type,
                                        column_ids,  effective_column_id,
                                        object_key,  snapshot},
      _value{std::move(value)} {
    SDB_PRINT("[mkornaukhov] creating RocksDBSnapshotMultiGetDataSource");
  }

  void addSplit(std::shared_ptr<velox::connector::ConnectorSplit> split) final {
    // VELOX_UNSUPPORTED();
    // Just do nothing? Or store for check that next is not called before next
    SDB_ENSURE(split, ERROR_INTERNAL, "RocksDBDataSource: split is null");
    if (_current_split) {
      SDB_THROW(ERROR_INTERNAL,
                "RocksDBDataSource: a split is already being processed");
    }
    _current_split = std::move(split);
  }

  std::optional<velox::RowVectorPtr> next(uint64_t size,
                                          velox::ContinueFuture& future) final {
    SDB_ASSERT(size);

    if (!_current_split) {
      return nullptr;
    }

    const auto num_columns = _row_type->size();
    std::vector<velox::VectorPtr> columns;
    columns.reserve(num_columns);

    // SDB_ASSERT(_column_ids.size() == 1);
    // TODO assert that pk consists of single column, otherwise does not work
    // for now

    std::string key = key_utils::PrepareTableKey(_object_key);
    std::string pk;
    primary_key::AppendKeyValue(pk, *_value, 0);
    std::string value;

    rocksdb::ReadOptions ro;
    ro.async_io = false;  // how is better?
    ro.snapshot = _snapshot;

    // =========
    // count(*) does not work yet
    SDB_ASSERT(num_columns > 0);
    columns.reserve(num_columns);
    for (size_t col_idx = 0; col_idx < num_columns; ++col_idx) {
      key.resize(sizeof(_object_key));
      key_utils::AppendColumnKey(key, _column_ids[col_idx]);
      key += pk;

      value.clear();

      // TODO do multiget

      rocksdb::Status r;
      if (r = _db.Get(ro, key, &value); !r.ok() && !r.IsNotFound()) {
        auto res = rocksutils::ConvertStatus(r);
        SDB_THROW(res.errorNumber(),
                  "Failed to read value by PK with point data source: ",
                  res.errorMessage());
      }

      const auto& type = _row_type->childAt(col_idx);

      columns.emplace_back(
        VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(ReadValue, type->kind(), r, value));
    }
    SDB_ASSERT(absl::c_all_of(columns,
                              [&](const velox::VectorPtr& vec) {
                                return vec->size() == columns.front()->size();
                              }),
               "RocksDBDataSource(multi get): inconsistent number of rows "
               "among columns");
    _produced += columns.front()->size();
    _current_split.reset();
    return std::make_shared<velox::RowVector>(&_memory_pool, _row_type, nullptr,
                                              columns.front()->size(),
                                              std::move(columns));
  }

 private:
  template<velox::TypeKind Kind>
  velox::VectorPtr ReadValue(rocksdb::Status status, const std::string& value) {
    using T = typename velox::TypeTraits<Kind>::NativeType;
    static constexpr std::string_view kTrueValue{"\1", 1};

    if (status.IsNotFound()) {
      return velox::BaseVector::create<velox::FlatVector<T>>(
        velox::Type::create<Kind>(), 0, &_memory_pool);
    }
    SDB_ASSERT(status.ok());
    auto result = velox::BaseVector::create<velox::FlatVector<T>>(
      velox::Type::create<Kind>(), 1, &_memory_pool);
    if (!value.empty()) {
      if constexpr (std::is_same_v<T, velox::StringView>) {
        const size_t offset = value[0] == 0 ? 1 : 0;
        velox::StringView val(value.data() + offset, value.size() - offset);
        result->set(0, val);
      } else if constexpr (std::is_same_v<T, bool>) {
        SDB_ASSERT(value.size() == kTrueValue.size(),
                   "RocksDBDataSource: unexpected value size for bool column");
        result->set(0, value == kTrueValue);
      } else {
        SDB_ASSERT(
          value.size() == sizeof(T),
          "RocksDBDataSource: unexpected value size for scalar column");
        T tmp;
        memcpy(&tmp, value.data(), sizeof(T));
        result->set(0, tmp);
      }
    } else {
      result->setNull(0, true);
    }
    return result;
  }

  velox::VectorPtr _value;
};
}  // namespace sdb::connector
