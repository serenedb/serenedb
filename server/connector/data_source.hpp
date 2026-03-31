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

#include "basics/containers/bitset.hpp"
#include "catalog/identifiers/object_id.h"
#include "catalog/table_options.h"
#include "connector/multiget_context.hpp"
#include "connector/rocksdb_filter.hpp"
#include "rocksdb/db.h"
#include "rocksdb/utilities/transaction.h"

namespace sdb::connector {

class SereneDBConnectorSplit;

class RocksDBBaseDataSource : public velox::connector::DataSource {
 public:
  void addDynamicFilter(
    velox::column_index_t output_channel,
    const std::shared_ptr<velox::common::Filter>& filter) override;
  uint64_t getCompletedBytes() override;
  uint64_t getCompletedRows() override;
  std::unordered_map<std::string, velox::RuntimeMetric> getRuntimeStats()
    override;
  void cancel() override;

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

template<typename Source>
class RocksDBFullScanDataSource : public RocksDBBaseDataSource {
 public:
  RocksDBFullScanDataSource(
    velox::memory::MemoryPool& memory_pool, Source& source,
    rocksdb::ColumnFamilyHandle& cf, velox::RowTypePtr read_type,
    std::vector<catalog::Column::Id> column_ids,
    catalog::Column::Id effective_column_id, ObjectId object_key,
    size_t output_column_count, const rocksdb::Snapshot* snapshot,
    velox::core::TypedExprPtr remaining_filter = nullptr,
    velox::core::ExpressionEvaluator* evaluator = nullptr);

  void addSplit(std::shared_ptr<velox::connector::ConnectorSplit> split) final;
  std::optional<velox::RowVectorPtr> next(uint64_t size,
                                          velox::ContinueFuture& future) final;

 private:
  template<std::invocable<const rocksdb::ReadOptions&> CreateFn>
  void InitIterators(CreateFn&& create);

  velox::VectorPtr ReadColumn(velox::column_index_t col_idx, uint64_t max_size);

  template<velox::TypeKind Kind>
  velox::VectorPtr ReadScalarColumn(rocksdb::Iterator& it, uint64_t max_size);

  velox::VectorPtr ReadUnknownColumn(rocksdb::Iterator& it, uint64_t max_size);

  velox::VectorPtr ReadArrayColumn(rocksdb::Iterator& it, uint64_t max_size,
                                   velox::TypePtr array_type);

  template<velox::TypeKind ElemKind>
  velox::VectorPtr ReadScalarArrayColumn(rocksdb::Iterator& it,
                                         uint64_t max_size,
                                         velox::TypePtr array_type);

  velox::VectorPtr ReadColumnFromKey(rocksdb::Iterator& it, uint64_t max_size);

  template<
    std::invocable<uint64_t, std::string_view, std::string_view> Callback>
  uint64_t IterateColumn(rocksdb::Iterator& it, uint64_t max_size,
                         const Callback& func);

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
class RocksDBPointLookupDataSource : public RocksDBBaseDataSource {
 public:
  RocksDBPointLookupDataSource(
    velox::memory::MemoryPool& memory_pool, rocksdb::ColumnFamilyHandle& cf,
    velox::RowTypePtr read_type, std::vector<catalog::Column::Id> column_ids,
    ObjectId object_key, const std::vector<SpecificPoint>& values,
    velox::RowTypePtr pk_type, size_t output_column_count,
    velox::core::TypedExprPtr remaining_filter,
    const rocksdb::Snapshot* snapshot,
    velox::core::ExpressionEvaluator* evaluator, Source& source);

  void addSplit(std::shared_ptr<velox::connector::ConnectorSplit> split) final;
  std::optional<velox::RowVectorPtr> next(uint64_t size,
                                          velox::ContinueFuture& future) final;

 private:

  void PrepareBatchKeys(catalog::Column::Id col_id, size_t start, size_t count);

  template<velox::TypeKind Kind>
  velox::VectorPtr ReadColumnMakeMask(catalog::Column::Id col_id, size_t batch_size);

  template<velox::TypeKind Kind>
  velox::VectorPtr ReadColumnWithMask(catalog::Column::Id col_id,
                                   size_t found_count);

  template<typename VectorType>
  void ReadDispatch(std::string_view value, velox::vector_size_t idx, VectorType& result);

  const std::vector<SpecificPoint>& _values;
  velox::RowTypePtr _pk_type;
  Source& _source;
  std::vector<size_t> _sorted_col_indices;
  // presence mask for the current batch window
  irs::bitset _present_rows_batch;
  // index into _values for the start of the current batch
  size_t _values_offset = 0;
  rocksdb::ReadOptions _read_options;
  MultiGetContext _multiget_ctx;
  // full keys built in Step 1; reused across columns in Step 2
  std::vector<std::string> _batch_keys;
  std::vector<rocksdb::Slice> _key_slices;
  // reused across ReadColumnWithMask calls; all col prefixes have same size
  std::string _col_prefix;
};

// Read Your Own Writes
using RocksDBRYOWFullScanDataSource =
  RocksDBFullScanDataSource<rocksdb::Transaction>;
using RocksDBRYOWPointLookupDataSource =
  RocksDBPointLookupDataSource<rocksdb::Transaction>;

using RocksDBSnapshotFullScanDataSource =
  RocksDBFullScanDataSource<rocksdb::DB>;
using RocksDBSnapshotPointLookupDataSource =
  RocksDBPointLookupDataSource<rocksdb::DB>;

}  // namespace sdb::connector
