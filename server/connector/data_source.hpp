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

#include "basics/containers/bitset.hpp"
#include "catalog/identifiers/object_id.h"
#include "catalog/table_options.h"
#include "connector/common.h"
#include "connector/key_builder.hpp"
#include "connector/multiget_context.hpp"
#include "connector/rocksdb_column_decoder.hpp"
#include "connector/rocksdb_filter.hpp"
#include "connector/rocksdb_materializer.hpp"
#include "connector/secondary_sink_writer.hpp"
#include "rocksdb/db.h"
#include "rocksdb/utilities/transaction.h"
#include "rocksdb_engine_catalog/rocksdb_option_feature.h"

namespace sdb::connector {

class SereneDBConnectorSplit;

class RocksDBBaseDataSource : public velox::connector::DataSource {
 public:
  void addDynamicFilter(
    velox::column_index_t output_channel,
    const std::shared_ptr<velox::common::Filter>& filter) final;
  uint64_t getCompletedBytes() final;
  uint64_t getCompletedRows() final;
  std::unordered_map<std::string, velox::RuntimeMetric> getRuntimeStats() final;
  void cancel() final;

 protected:
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
};

// Base for all data sources that read data using one RocksDB iterator per
// column. Provides iterator lifecycle management and column reading machinery.
// Subclasses implement addSplit() to create and position the iterators.
template<typename Source>
class RocksDBPerColumnIteratorDataSource : public RocksDBBaseDataSource {
 public:
  RocksDBPerColumnIteratorDataSource(
    velox::memory::MemoryPool& memory_pool, Source& source,
    rocksdb::ColumnFamilyHandle& cf, velox::RowTypePtr read_type,
    std::vector<catalog::Column::Id> column_ids,
    catalog::Column::Id effective_column_id, ObjectId object_key,
    size_t output_column_count, const rocksdb::Snapshot* snapshot,
    velox::core::TypedExprPtr remaining_filter = nullptr,
    velox::core::ExpressionEvaluator* evaluator = nullptr);

  std::optional<velox::RowVectorPtr> next(uint64_t size,
                                          velox::ContinueFuture& future) final;

 private:
  velox::VectorPtr ReadColumn(velox::column_index_t col_idx, uint64_t max_size);

  velox::VectorPtr ReadColumnFromKey(rocksdb::Iterator& it, uint64_t max_size);

  template<
    std::invocable<uint64_t, std::string_view, std::string_view> Callback>
  uint64_t IterateColumn(rocksdb::Iterator& it, uint64_t max_size,
                         const Callback& func);

 protected:
  Source& _source;
  const rocksdb::Snapshot* _snapshot;
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

template<typename Source>
class RocksDBFullScanDataSource
  : public RocksDBPerColumnIteratorDataSource<Source> {
  using Base = RocksDBPerColumnIteratorDataSource<Source>;

 public:
  using Base::Base;

  void addSplit(std::shared_ptr<velox::connector::ConnectorSplit> split) final;

 private:
  template<typename CreateFn>
  void InitIterators(CreateFn&& create);
};

class PointLookupPKColumnBuilder {
 public:
  static constexpr bool kIsSecondaryIndex = false;

  void Init(const velox::TypePtr& type, size_t capacity,
            velox::memory::MemoryPool& pool);
  void Fill(size_t batch_idx, size_t found_idx,
            const rocksdb::PinnableSlice& val);
  velox::VectorPtr Finish(size_t found_count);
  const irs::bitset& PresentRows() const { return _present_rows; }

 private:
  std::unique_ptr<RocksDBColumnDecoder> _decoder;
  irs::bitset _present_rows;
};

template<typename Materializer>
class PointLookupSKColumnBuilder {
 public:
  static constexpr bool kIsSecondaryIndex = true;

  PointLookupSKColumnBuilder(Materializer materializer,
                             velox::memory::MemoryPool& pool)
    : _materializer{std::move(materializer)}, _row_keys{pool} {}

  void Init(const velox::TypePtr& type, size_t capacity,
            velox::memory::MemoryPool& pool) {
    _row_keys.reserve(capacity);
  }

  void Fill(size_t batch_idx, size_t found_idx,
            const rocksdb::PinnableSlice& val) {
    // we store pk in value only for unique non-null SKs, otherwise
    // pointlookup is not supposed to be used.
    SDB_ASSERT(val.size() > 1);
    SDB_ASSERT(val[0] == secondary_key::kPKInValue);
    _row_keys.emplace_back(val.data() + 1, val.size() - 1);
  }

  velox::RowVectorPtr Finish(size_t found_count) {
    return _materializer.ReadRows(_row_keys, nullptr, {});
  }

  const irs::bitset& PresentRows() const { return _dummy; }

 private:
  irs::bitset _dummy;
  Materializer _materializer;
  primary_key::Keys _row_keys;
};

template<bool ReadYourOwnWrites>
struct PKLookupPolicy {
  using Source =
    std::conditional_t<ReadYourOwnWrites, rocksdb::Transaction, rocksdb::DB>;

  using KeyBuilder = PrimaryKeyBuilder;

  using ResultCollector = PointLookupPKColumnBuilder;
};

template<bool ReadYourOwnWrites, typename Materializer>
struct SKLookupPolicy {
  using Source =
    std::conditional_t<ReadYourOwnWrites, rocksdb::Transaction, rocksdb::DB>;

  using KeyBuilder = SecondaryKeyBuilder;

  using ResultCollector = PointLookupSKColumnBuilder<Materializer>;
};

template<typename Policy>
class RocksDBPointLookupDataSource : public RocksDBBaseDataSource {
  using Source = typename Policy::Source;

  using KeyBuilder = typename Policy::KeyBuilder;

  using ResultCollector = typename Policy::ResultCollector;

  static constexpr bool kIsSecondaryIndex = ResultCollector::kIsSecondaryIndex;

 public:
  RocksDBPointLookupDataSource(
    velox::memory::MemoryPool& memory_pool, rocksdb::ColumnFamilyHandle& cf,
    velox::RowTypePtr read_type, std::vector<catalog::Column::Id> column_ids,
    ObjectId object_key, std::vector<ResolvedPoint> values,
    size_t output_column_count, velox::core::TypedExprPtr remaining_filter,
    const rocksdb::Snapshot* snapshot,
    velox::core::ExpressionEvaluator* evaluator, Source& source,
    KeyBuilder key_builder, ResultCollector collector)
    : RocksDBBaseDataSource{memory_pool,
                            cf,
                            std::move(read_type),
                            object_key,
                            std::move(column_ids),
                            output_column_count,
                            std::move(remaining_filter),
                            evaluator},
      _values{std::move(values)},
      _key_builder{std::move(key_builder)},
      _collector{std::move(collector)},
      _source{source},
      _ctx{cf, [snapshot] {
             rocksdb::ReadOptions ro;
             ro.async_io = IsIOUringEnabled();
             ro.snapshot = snapshot;
             return ro;
           }()} {
    _sorted_col_indices.resize(_column_ids.size());
    std::iota(_sorted_col_indices.begin(), _sorted_col_indices.end(), 0);
    std::ranges::sort(_sorted_col_indices, [&](size_t a, size_t b) {
      return _column_ids[a] < _column_ids[b];
    });
  }

  void addSplit(std::shared_ptr<velox::connector::ConnectorSplit> split) final;
  std::optional<velox::RowVectorPtr> next(uint64_t size,
                                          velox::ContinueFuture& future) final;

 private:
  std::vector<ResolvedPoint> _values;
  KeyBuilder _key_builder;
  ResultCollector _collector;
  std::vector<size_t> _sorted_col_indices;
  size_t _values_offset = 0;
  Source& _source;
  MultiGetContext _ctx;
};

// Scans a set of pre-sorted KeyConstraint ranges using N independent RocksDB
// iterators (one per range per column). Each iterator seeks to its own lower
// bound and stops when the key no longer matches its prefix or exceeds its
// explicit upper bound. Bounds are checked in Valid() rather than via
// iterate_upper_bound.
template<typename Source>
class RocksDBPrefixRangeDataSource
  : public RocksDBPerColumnIteratorDataSource<Source> {
 public:
  RocksDBPrefixRangeDataSource(
    velox::memory::MemoryPool& memory_pool, Source& source,
    rocksdb::ColumnFamilyHandle& cf, velox::RowTypePtr read_type,
    std::vector<catalog::Column::Id> column_ids,
    catalog::Column::Id effective_column_id, ObjectId object_key,
    size_t output_column_count, const rocksdb::Snapshot* snapshot,
    std::vector<ResolvedRange> ranges, velox::RowTypePtr pk_type,
    velox::core::TypedExprPtr remaining_filter = nullptr,
    velox::core::ExpressionEvaluator* evaluator = nullptr);

  // NOLINTNEXTLINE(readability-identifier-naming)
  void addSplit(std::shared_ptr<velox::connector::ConnectorSplit> split) final;

 private:
  template<typename CreateFn>
  void InitIterators(CreateFn&& create);

  velox::RowTypePtr _pk_type;
  std::vector<std::string> _split_prefix_keys;
  std::vector<std::string> _split_upper_bound_keys;
};

// Read Your Own Writes
using RocksDBRYOWFullScanDataSource =
  RocksDBFullScanDataSource<rocksdb::Transaction>;
using RocksDBRYOWPointLookupDataSource =
  RocksDBPointLookupDataSource<PKLookupPolicy<true>>;
using RocksDBRYOWPrefixRangeLookupDataSource =
  RocksDBPrefixRangeDataSource<rocksdb::Transaction>;

using RocksDBSnapshotFullScanDataSource =
  RocksDBFullScanDataSource<rocksdb::DB>;
using RocksDBSnapshotPointLookupDataSource =
  RocksDBPointLookupDataSource<PKLookupPolicy<false>>;
using RocksDBSnapshotPrefixRangeLookupDataSource =
  RocksDBPrefixRangeDataSource<rocksdb::DB>;

}  // namespace sdb::connector
