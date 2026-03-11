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
                            ObjectId object_key, size_t output_column_count,
                            const rocksdb::Snapshot* snapshot,
                            velox::core::TypedExprPtr remaining_filter,
                            velox::core::ExpressionEvaluator* evaluator);

  template<std::invocable<const rocksdb::ReadOptions&> CreateFn>
  void InitIterators(CreateFn&& create);

  velox::memory::MemoryPool& _memory_pool;
  rocksdb::ColumnFamilyHandle& _cf;
  const rocksdb::Snapshot* _snapshot;
  ObjectId _object_key;
  std::vector<catalog::Column::Id> _column_ids;

 private:
  // Applies _remainingFilterExprSet to batch, returning a (possibly smaller)
  // RowVector with only the rows where the filter evaluates to true.
  // Returns batch unchanged if no filter is set.
  velox::RowVectorPtr ApplyRemainingFilter(velox::RowVectorPtr batch);

  velox::VectorPtr ReadColumn(velox::column_index_t col_idx, uint64_t max_size);

  template<velox::TypeKind Kind>
  velox::VectorPtr ReadScalarColumn(rocksdb::Iterator& it, uint64_t max_size);

  velox::VectorPtr ReadUnknownColumn(rocksdb::Iterator& it, uint64_t max_size);

  velox::VectorPtr ReadColumnFromKey(rocksdb::Iterator& it, uint64_t max_size);

  template<
    std::invocable<uint64_t, std::string_view, std::string_view> Callback>
  uint64_t IterateColumn(rocksdb::Iterator& it, uint64_t max_size,
                         const Callback& func);

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
  // Number of columns in the projected output. May be less than
  // _row_type->size() when filter-only columns are appended for
  // ApplyRemainingFilter evaluation.
  size_t _output_column_count;
  std::shared_ptr<velox::connector::ConnectorSplit> _current_split;
  uint64_t _produced = 0;
  velox::core::ExpressionEvaluator* _evaluator = nullptr;
  std::unique_ptr<velox::exec::ExprSet> _remainingFilterExprSet;
};

// Read Your Own Writes
class RocksDBRYOWFullScanDataSource : public RocksDBFullScanDataSource {
 public:
  RocksDBRYOWFullScanDataSource(velox::memory::MemoryPool& memory_pool,
                                rocksdb::Transaction& transaction,
                                rocksdb::ColumnFamilyHandle& cf,
                                velox::RowTypePtr row_type,
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
    rocksdb::ColumnFamilyHandle& cf, velox::RowTypePtr row_type,
    std::vector<catalog::Column::Id> column_ids,
    catalog::Column::Id effective_column_id, ObjectId object_key,
    size_t output_column_count, const rocksdb::Snapshot* snapshot,
    velox::core::TypedExprPtr remaining_filter = nullptr,
    velox::core::ExpressionEvaluator* evaluator = nullptr);
  void addSplit(std::shared_ptr<velox::connector::ConnectorSplit> split) final;

 private:
  rocksdb::DB& _db;
};

template<typename Source>
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
                               ObjectId object_key, velox::RowVectorPtr values,
                               Source& source, rocksdb::ReadOptions ro)
    : _memory_pool{memory_pool},
      _cf{cf},
      _row_type{std::move(row_type)},
      _column_ids{std::move(column_ids)},
      _object_key{object_key},
      _values{std::move(values)},
      _ctx{source, std::move(ro)} {
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

  // Count entries in _statuses[0..batch_size) that are not NotFound.
  size_t CountFound(size_t batch_size) const;

  // Advance _offset by batch_size; reset _current_split when all points done.
  void FinalizeOffset(size_t batch_size, size_t total_points);

  velox::memory::MemoryPool& _memory_pool;
  rocksdb::ColumnFamilyHandle& _cf;
  velox::RowTypePtr _row_type;
  std::vector<catalog::Column::Id> _column_ids;
  ObjectId _object_key;
  std::shared_ptr<velox::connector::ConnectorSplit> _current_split;
  uint64_t _produced = 0;
  size_t _offset = 0;
  velox::RowVectorPtr _values;
  std::vector<size_t> _sorted_col_indices;
  std::vector<size_t> _col_rank;
  std::vector<std::string> _keys;
  std::vector<rocksdb::Slice> _key_slices;
  std::vector<std::string> _raw_values;
  std::vector<rocksdb::Status> _statuses;
  MultiGetContext<Source> _ctx;
};

class RocksDBRYOWPointLookupDataSource final
  : public RocksDBPointLookupDataSource<rocksdb::Transaction> {
 public:
  RocksDBRYOWPointLookupDataSource(velox::memory::MemoryPool& memory_pool,
                                   rocksdb::Transaction& transaction,
                                   rocksdb::ColumnFamilyHandle& cf,
                                   velox::RowTypePtr row_type,
                                   std::vector<catalog::Column::Id> column_ids,
                                   ObjectId object_key,
                                   velox::RowVectorPtr values)
    : RocksDBPointLookupDataSource{memory_pool,
                                   cf,
                                   std::move(row_type),
                                   std::move(column_ids),
                                   object_key,
                                   std::move(values),
                                   transaction,
                                   [] {
                                     rocksdb::ReadOptions ro;
                                     ro.async_io = IsIOUringEnabled();
                                     return ro;
                                   }()} {}
};

class RocksDBSnapshotPointLookupDataSource final
  : public RocksDBPointLookupDataSource<rocksdb::DB> {
 public:
  RocksDBSnapshotPointLookupDataSource(
    velox::memory::MemoryPool& memory_pool, rocksdb::DB& db,
    rocksdb::ColumnFamilyHandle& cf, velox::RowTypePtr row_type,
    std::vector<catalog::Column::Id> column_ids, ObjectId object_key,
    const rocksdb::Snapshot* snapshot, velox::RowVectorPtr values)
    : RocksDBPointLookupDataSource{memory_pool,
                                   cf,
                                   std::move(row_type),
                                   std::move(column_ids),
                                   object_key,
                                   std::move(values),
                                   db,
                                   [snapshot] {
                                     rocksdb::ReadOptions ro;
                                     ro.async_io = IsIOUringEnabled();
                                     ro.snapshot = snapshot;
                                     return ro;
                                   }()} {}
};

}  // namespace sdb::connector
