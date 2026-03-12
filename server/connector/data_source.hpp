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
#include <velox/core/ExpressionEvaluator.h>
#include <velox/core/Expressions.h>
#include <velox/type/Type.h>
#include <velox/vector/DecodedVector.h>
#include <velox/vector/FlatVector.h>

#include <algorithm>
#include <numeric>

#include "catalog/identifiers/object_id.h"
#include "catalog/table_options.h"
#include "rocksdb/db.h"
#include "rocksdb/utilities/transaction.h"
#include "rocksdb_engine_catalog/rocksdb_option_feature.h"

namespace sdb::connector {

class SereneDBConnectorSplit;

template<typename Source>
class MultiGetContext {
 public:
  // TODO benchmark and choose best threshold
  static constexpr size_t kThreshold = 1;
  static constexpr size_t kBatchSize = 32;

  MultiGetContext(Source& source, rocksdb::ReadOptions ro)
    : _source{source}, _ro{std::move(ro)} {}

  void MultiGet(rocksdb::ColumnFamilyHandle& cf,
                const std::vector<rocksdb::Slice>& keys,
                std::vector<std::string>& values,
                std::vector<rocksdb::Status>& statuses) {
    const size_t n = keys.size();
    values.resize(n);
    statuses.resize(n);

    if (n <= kThreshold) {
      for (size_t i = 0; i < n; ++i) {
        _pinnable[0].Reset();
        statuses[i] = _source.Get(_ro, &cf, keys[i], _pinnable.data());
        ExtractValues(1, i, values);
      }
      return;
    }

    for (size_t start = 0; start < n; start += kBatchSize) {
      const size_t batch_size = std::min(kBatchSize, n - start);
      for (size_t i = 0; i < batch_size; ++i) {
        _pinnable[i].Reset();
      }
      _source.MultiGet(_ro, &cf, batch_size, keys.data() + start,
                       _pinnable.data(), statuses.data() + start,
                       /*sorted_input=*/true);
      ExtractValues(batch_size, start, values);
    }
  }

 private:
  void ExtractValues(size_t count, size_t dest_start,
                     std::vector<std::string>& values) {
    for (size_t i = 0; i < count; ++i) {
      if (_pinnable[i].IsPinned()) {
        values[dest_start + i] = _pinnable[i].ToStringView();
      } else {
        values[dest_start + i] = std::move(*_pinnable[i].GetSelf());
      }
    }
  }

  Source& _source;
  rocksdb::ReadOptions _ro;
  std::array<rocksdb::PinnableSlice, kBatchSize> _pinnable;
};

class RocksDBBaseDataSource : public velox::connector::DataSource {
 protected:
  velox::memory::MemoryPool& _memory_pool;
  rocksdb::ColumnFamilyHandle& _cf;
  velox::RowTypePtr _read_type;

  ObjectId _object_key;
  std::vector<catalog::Column::Id> _column_ids;
  size_t _output_column_count;
  std::shared_ptr<velox::connector::ConnectorSplit> _current_split;
  uint64_t _produced = 0;
  std::unique_ptr<velox::exec::ExprSet> _remaining_expr_set;
  velox::core::ExpressionEvaluator* _evaluator = nullptr;

  RocksDBBaseDataSource(velox::memory::MemoryPool& memory_pool,
                        rocksdb::ColumnFamilyHandle& cf,
                        velox::RowTypePtr read_type, const ObjectId& object_key,
                        std::vector<catalog::Column::Id> column_ids,
                        size_t output_column_count,
                        velox::core::TypedExprPtr remaining_filter,
                        velox::core::ExpressionEvaluator* evaluator)
    : velox::connector::DataSource{},
      _memory_pool{memory_pool},
      _cf{cf},
      _read_type{std::move(read_type)},
      _object_key{object_key},
      _column_ids{std::move(column_ids)},
      _output_column_count{output_column_count},
      _evaluator{evaluator} {
    SDB_ASSERT(_read_type, "RocksDBDataSource: row type is null");
    SDB_ASSERT(_object_key.isSet(), "RocksDBDataSource: object key is empty");
    SDB_ASSERT(!_column_ids.empty(),
               "RocksDBDataSource: at least one column must be requested");
    SDB_ASSERT(
      _read_type->size() == 0 || _read_type->size() == _column_ids.size(),
      "RocksDBDataSource: number of columns does not match row type");
    if (remaining_filter && evaluator) {
      _evaluator = evaluator;
      _remaining_expr_set = evaluator->compile(remaining_filter);
      SDB_ASSERT(_remaining_expr_set->size() == 1);
    }
  }

  velox::RowVectorPtr ApplyRemainingFilter(velox::RowVectorPtr batch);
};

class RocksDBFullScanDataSource : public RocksDBBaseDataSource {
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
                            velox::RowTypePtr read_type,
                            std::vector<catalog::Column::Id> column_ids,
                            catalog::Column::Id effective_column_id,
                            ObjectId object_key, size_t output_column_count,
                            const rocksdb::Snapshot* snapshot,
                            velox::core::TypedExprPtr remaining_filter,
                            velox::core::ExpressionEvaluator* evaluator);

  template<std::invocable<const rocksdb::ReadOptions&> CreateFn>
  void InitIterators(CreateFn&& create);

  const rocksdb::Snapshot* _snapshot;

 private:
  velox::VectorPtr ReadColumn(velox::column_index_t col_idx, uint64_t max_size);

  template<velox::TypeKind Kind>
  velox::VectorPtr ReadScalarColumn(rocksdb::Iterator& it, uint64_t max_size);

  velox::VectorPtr ReadUnknownColumn(rocksdb::Iterator& it, uint64_t max_size);

  velox::VectorPtr ReadColumnFromKey(rocksdb::Iterator& it, uint64_t max_size);

  template<
    std::invocable<uint64_t, std::string_view, std::string_view> Callback>
  uint64_t IterateColumn(rocksdb::Iterator& it, uint64_t max_size,
                         const Callback& func);

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
};

// Read Your Own Writes
class RocksDBRYOWFullScanDataSource : public RocksDBFullScanDataSource {
 public:
  RocksDBRYOWFullScanDataSource(velox::memory::MemoryPool& memory_pool,
                                rocksdb::Transaction& transaction,
                                rocksdb::ColumnFamilyHandle& cf,
                                velox::RowTypePtr read_type,
                                std::vector<catalog::Column::Id> column_ids,
                                catalog::Column::Id effective_column_id,
                                ObjectId object_key, size_t output_column_count,
                                velox::core::TypedExprPtr remaining_filter,
                                velox::core::ExpressionEvaluator* evaluator);

  void addSplit(std::shared_ptr<velox::connector::ConnectorSplit> split) final;

 private:
  rocksdb::Transaction& _transaction;
};

class RocksDBSnapshotFullScanDataSource : public RocksDBFullScanDataSource {
 public:
  RocksDBSnapshotFullScanDataSource(
    velox::memory::MemoryPool& memory_pool, rocksdb::DB& db,
    rocksdb::ColumnFamilyHandle& cf, velox::RowTypePtr read_type,
    std::vector<catalog::Column::Id> column_ids,
    catalog::Column::Id effective_column_id, ObjectId object_key,
    size_t output_column_count, const rocksdb::Snapshot* snapshot = nullptr,
    velox::core::TypedExprPtr remaining_filter = nullptr,
    velox::core::ExpressionEvaluator* evaluator = nullptr);
  void addSplit(std::shared_ptr<velox::connector::ConnectorSplit> split) final;

 private:
  rocksdb::DB& _db;
};

template<typename Source>
class RocksDBPointLookupDataSource : public RocksDBBaseDataSource {
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
  RocksDBPointLookupDataSource(
    velox::memory::MemoryPool& memory_pool, rocksdb::ColumnFamilyHandle& cf,
    velox::RowTypePtr read_type, std::vector<catalog::Column::Id> column_ids,
    ObjectId object_key, velox::RowVectorPtr values, size_t output_column_count,
    velox::core::TypedExprPtr remaining_filter,
    const rocksdb::Snapshot* snapshot,
    velox::core::ExpressionEvaluator* evaluator, Source& source)
    : RocksDBBaseDataSource{memory_pool,
                            cf,
                            std::move(read_type),
                            object_key,
                            std::move(column_ids),
                            output_column_count,
                            std::move(remaining_filter),
                            evaluator},
      _values{std::move(values)},
      _ctx{source, [snapshot] {
             rocksdb::ReadOptions ro;
             ro.async_io = IsIOUringEnabled();
             ro.snapshot = snapshot;
             return ro;
           }()} {
    const size_t num_cols = _column_ids.size();
    _sorted_col_indices.resize(num_cols);
    std::iota(_sorted_col_indices.begin(), _sorted_col_indices.end(), 0);
    std::sort(
      _sorted_col_indices.begin(), _sorted_col_indices.end(),
      [&](size_t a, size_t b) { return _column_ids[a] < _column_ids[b]; });
    _col_rank.resize(num_cols);
    for (size_t rank = 0; rank < num_cols; ++rank) {
      _col_rank[_sorted_col_indices[rank]] = rank;
    }
  }

 private:
  // Build _keys[rank * batch_size + point_idx] for key_cols column slots.
  void BuildKeys(size_t batch_size, size_t key_cols);

  // Copy _keys[0..total_keys) into _key_slices, then call MultiGetContext.
  void PerformMultiGet(size_t total_keys);

  // Count entries in _statuses[0..batch_size) that are not NotFound and throw
  // if any error happens.
  size_t CheckAndCountFound(size_t batch_size) const;

  // Advance _offset by batch_size; reset _current_split when all points done.
  void FinalizeOffset(size_t batch_size, size_t total_points);

  size_t _offset = 0;
  velox::RowVectorPtr _values;
  std::vector<size_t> _sorted_col_indices;
  std::vector<size_t> _col_rank;
  // TODO(mkornaukhov) use std::array, need pass callback into multiget context
  // for custom processing of window-by-window logics.
  std::vector<std::string> _keys;
  std::vector<rocksdb::Slice> _key_slices;
  std::vector<std::string> _raw_values;
  std::vector<rocksdb::Status> _statuses;
  MultiGetContext<Source> _ctx;
};

class RocksDBRYOWPointLookupDataSource final
  : public RocksDBPointLookupDataSource<rocksdb::Transaction> {
 public:
  RocksDBRYOWPointLookupDataSource(
    velox::memory::MemoryPool& memory_pool, rocksdb::Transaction& transaction,
    rocksdb::ColumnFamilyHandle& cf, velox::RowTypePtr read_type,
    std::vector<catalog::Column::Id> column_ids, ObjectId object_key,
    velox::RowVectorPtr values, size_t output_column_count,
    velox::core::TypedExprPtr remaining_filter,
    velox::core::ExpressionEvaluator* evaluator)
    : RocksDBPointLookupDataSource{memory_pool,
                                   cf,
                                   std::move(read_type),
                                   std::move(column_ids),
                                   object_key,
                                   std::move(values),
                                   output_column_count,
                                   std::move(remaining_filter),
                                   transaction.GetSnapshot(),
                                   evaluator,
                                   transaction} {}
};

class RocksDBSnapshotPointLookupDataSource final
  : public RocksDBPointLookupDataSource<rocksdb::DB> {
 public:
  RocksDBSnapshotPointLookupDataSource(
    velox::memory::MemoryPool& memory_pool, rocksdb::DB& db,
    rocksdb::ColumnFamilyHandle& cf, velox::RowTypePtr read_type,
    std::vector<catalog::Column::Id> column_ids, ObjectId object_key,
    const rocksdb::Snapshot* snapshot, velox::RowVectorPtr values,
    size_t output_column_count, velox::core::TypedExprPtr remaining_filter,
    velox::core::ExpressionEvaluator* evaluator)
    : RocksDBPointLookupDataSource{memory_pool,
                                   cf,
                                   std::move(read_type),
                                   std::move(column_ids),
                                   object_key,
                                   std::move(values),
                                   output_column_count,
                                   std::move(remaining_filter),
                                   snapshot,
                                   evaluator,
                                   db} {}
};

}  // namespace sdb::connector
