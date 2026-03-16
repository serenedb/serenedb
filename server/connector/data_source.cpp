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

#include "data_source.hpp"

#include <absl/algorithm/container.h>
#include <absl/base/internal/endian.h>
#include <velox/vector/DecodedVector.h>
#include <velox/vector/FlatVector.h>

#include "basics/assert.h"
#include "catalog/identifiers/object_id.h"
#include "catalog/table_options.h"
#include "common.h"
#include "connector/primary_key.hpp"
#include "key_utils.hpp"
#include "rocksdb_engine_catalog/rocksdb_common.h"

namespace sdb::connector {
namespace {

constexpr uint64_t kInitialVectorSize = 1;  // arbitrary value
constexpr size_t kTablePrefixSize = sizeof(ObjectId);
constexpr size_t kKeyPrefixSize =
  kTablePrefixSize + sizeof(catalog::Column::Id);

template<typename T>
void SetResultValue(std::string_view value, size_t idx,
                    velox::FlatVector<T>& result) {
  if constexpr (std::is_same_v<T, velox::StringView>) {
    const size_t offset = value[0] == 0 ? 1 : 0;
    result.set(idx,
               velox::StringView(value.data() + offset, value.size() - offset));
  } else if constexpr (std::is_same_v<T, bool>) {
    SDB_ASSERT(value.size() == kTrueValue.size(),
               "RocksDBDataSource: unexpected value size for bool column");
    result.set(idx, value == kTrueValue);
  } else {
    SDB_ASSERT(value.size() == sizeof(T),
               "RocksDBDataSource: unexpected value size for scalar column");
    T tmp;
    memcpy(&tmp, value.data(), sizeof(T));
    result.set(idx, tmp);
  }
}

template<velox::TypeKind Kind>
velox::VectorPtr ReadPointColumnValues(
  size_t sort_rank, size_t read_points_count, size_t write_points_count,
  const std::vector<rocksdb::Status>& statuses,
  const std::vector<std::string>& values,
  velox::memory::MemoryPool& memory_pool) {
  using T = typename velox::TypeTraits<Kind>::NativeType;

  auto result = velox::BaseVector::create<velox::FlatVector<T>>(
    velox::Type::create<Kind>(), write_points_count, &memory_pool);

  size_t written = 0;
  for (size_t read_idx = 0; read_idx < read_points_count; ++read_idx) {
    const size_t idx = sort_rank * read_points_count + read_idx;
    const auto& status = statuses[idx];
    const auto& value = values[idx];

    if (status.IsNotFound()) {
      // Filtered point is not found, it's OK, just skip
      continue;
    }

    SDB_ASSERT(!value.empty());
    SDB_ENSURE(status.ok(), rocksutils::ConvertStatus(status));
    SetResultValue(value, written++, *result);
  }
  SDB_ASSERT(written == write_points_count);
  return result;
}

}  // namespace

RocksDBFullScanDataSource::RocksDBFullScanDataSource(
  velox::memory::MemoryPool& memory_pool, rocksdb::ColumnFamilyHandle& cf,
  velox::RowTypePtr read_type, std::vector<catalog::Column::Id> column_oids,
  catalog::Column::Id effective_column_id, ObjectId object_key,
  size_t output_column_count, const rocksdb::Snapshot* snapshot,
  velox::core::TypedExprPtr remaining_filter,
  velox::core::ExpressionEvaluator* evaluator)
  : RocksDBBaseDataSource{memory_pool,
                          cf,
                          std::move(read_type),
                          object_key,
                          std::move(column_oids),
                          output_column_count,
                          std::move(remaining_filter),
                          evaluator},
    _snapshot{snapshot},
    _effective_column_id(std::move(effective_column_id)) {
  SDB_ASSERT(_read_type, "RocksDBDataSource: row type is null");
  SDB_ASSERT(_object_key.isSet(), "RocksDBDataSource: object key is empty");
  SDB_ASSERT(!_column_ids.empty(),
             "RocksDBDataSource: at least one column must be requested");
  SDB_ASSERT(
    _read_type->size() == 0 || _read_type->size() == _column_ids.size(),
    "RocksDBDataSource: number of columns does not match row type");

  std::string key = key_utils::PrepareTableKey(_object_key);

  const auto num_columns = _column_ids.size();

  _column_keys.reserve(num_columns);
  _upper_bound_keys_data.reserve(kKeyPrefixSize * num_columns);
  _upper_bound_slices.reserve(num_columns);

  for (const auto column_id : _column_ids) {
    auto read_column_id = column_id;
    if (column_id == catalog::Column::kGeneratedPKId) {
      SDB_ASSERT(_effective_column_id != catalog::Column::kGeneratedPKId,
                 "RocksDBDataSource: generated PK column is not effective");
      read_column_id = _effective_column_id;
    }

    SDB_ASSERT(read_column_id !=
               std::numeric_limits<catalog::Column::Id>::max());

    basics::StrResize(key, kTablePrefixSize);

    _upper_bound_keys_data.append(key);
    key_utils::AppendColumnKey(_upper_bound_keys_data, read_column_id + 1);

    key_utils::AppendColumnKey(key, read_column_id);
    _column_keys.push_back(key);
  }

  for (size_t i = 0; i < num_columns; ++i) {
    _upper_bound_slices.emplace_back(
      _upper_bound_keys_data.data() + i * kKeyPrefixSize, kKeyPrefixSize);
  }
}

RocksDBRYOWFullScanDataSource::RocksDBRYOWFullScanDataSource(
  velox::memory::MemoryPool& memory_pool, rocksdb::Transaction& transaction,
  rocksdb::ColumnFamilyHandle& cf, velox::RowTypePtr read_type,
  std::vector<catalog::Column::Id> column_ids,
  catalog::Column::Id effective_column_id, ObjectId object_key,
  size_t output_column_count, velox::core::TypedExprPtr remaining_filter,
  velox::core::ExpressionEvaluator* evaluator)
  : RocksDBFullScanDataSource{memory_pool,
                              cf,
                              std::move(read_type),
                              std::move(column_ids),
                              effective_column_id,
                              object_key,
                              output_column_count,
                              transaction.GetSnapshot(),
                              std::move(remaining_filter),
                              evaluator},
    _transaction{transaction} {}

RocksDBSnapshotFullScanDataSource::RocksDBSnapshotFullScanDataSource(
  velox::memory::MemoryPool& memory_pool, rocksdb::DB& db,
  rocksdb::ColumnFamilyHandle& cf, velox::RowTypePtr read_type,
  std::vector<catalog::Column::Id> column_ids,
  catalog::Column::Id effective_column_id, ObjectId object_key,
  size_t output_column_count, const rocksdb::Snapshot* snapshot,
  velox::core::TypedExprPtr remaining_filter,
  velox::core::ExpressionEvaluator* evaluator)
  : RocksDBFullScanDataSource{memory_pool,
                              cf,
                              std::move(read_type),
                              std::move(column_ids),
                              effective_column_id,
                              object_key,
                              output_column_count,
                              snapshot,
                              std::move(remaining_filter),
                              evaluator},
    _db{db} {}

void RocksDBRYOWFullScanDataSource::addSplit(
  std::shared_ptr<velox::connector::ConnectorSplit> split) {
  RocksDBFullScanDataSource::addSplit(std::move(split));
  InitIterators([&](const rocksdb::ReadOptions& options) {
    return std::unique_ptr<rocksdb::Iterator>(
      _transaction.GetIterator(options, &_cf));
  });
}

void RocksDBSnapshotFullScanDataSource::addSplit(
  std::shared_ptr<velox::connector::ConnectorSplit> split) {
  RocksDBFullScanDataSource::addSplit(std::move(split));
  InitIterators([&](const rocksdb::ReadOptions& options) {
    return std::unique_ptr<rocksdb::Iterator>(_db.NewIterator(options, &_cf));
  });
}

void RocksDBFullScanDataSource::addSplit(
  std::shared_ptr<velox::connector::ConnectorSplit> split) {
  SDB_ENSURE(split, ERROR_INTERNAL, "RocksDBDataSource: split is null");
  if (_current_split) {
    SDB_THROW(ERROR_INTERNAL,
              "RocksDBDataSource: a split is already being processed");
  }
  _current_split = std::move(split);
}

template<std::invocable<const rocksdb::ReadOptions&> CreateFn>
void RocksDBFullScanDataSource::InitIterators(CreateFn&& create_iter) {
  // Creating iterator API expects options by const reference, but all
  // implementations copies this argument, so it should be safe.
  rocksdb::ReadOptions options;
  options.snapshot = _snapshot;
  options.async_io = false;
  options.adaptive_readahead = true;
  options.auto_prefix_mode = true;

  _iterators.clear();
  _iterators.reserve(_column_keys.size());
  for (size_t i = 0; i < _column_keys.size(); ++i) {
    options.iterate_upper_bound = &_upper_bound_slices[i];
    auto it = create_iter(options);
    it->Seek(_column_keys[i]);
    _iterators.push_back(std::move(it));
  }
}

std::optional<velox::RowVectorPtr> RocksDBFullScanDataSource::next(
  uint64_t size, velox::ContinueFuture& future) {
  SDB_ASSERT(size);
  if (!_current_split) {
    return nullptr;
  }

  const auto num_columns = _read_type->size();
  std::vector<velox::VectorPtr> columns;
  columns.reserve(num_columns);

  if (num_columns > 0) {
    columns.reserve(num_columns);
    for (size_t column_idx = 0; column_idx < num_columns; ++column_idx) {
      columns.emplace_back(ReadColumn(column_idx, size));

      // All rows are read
      if (columns[column_idx]->size() == 0) {
        SDB_ASSERT(column_idx == 0,
                   "RocksDBDataSource: inconsistent number of columns");
        _current_split.reset();
        return nullptr;
      }
    }
    SDB_ASSERT(absl::c_all_of(columns,
                              [&](const velox::VectorPtr& vec) {
                                return vec->size() == columns.front()->size();
                              }),
               "RocksDBDataSource: inconsistent number of rows among columns");
    auto batch = ApplyRemainingFilter(std::make_shared<velox::RowVector>(
      &_memory_pool, _read_type, nullptr, columns.front()->size(),
      std::move(columns)));

    _produced += batch->size();
    return batch;
  }
  SDB_ASSERT(_column_ids.size() == 1);
  SDB_ASSERT(!_remaining_expr_set,
             "RocksDBFullScanDataSource: count(*) with filter should set up "
             "filter columns in _read_type");
  const auto read = IterateColumn(
    *_iterators[0], size, [](uint64_t, std::string_view, std::string_view) {});

  // All rows are read
  if (read == 0) {
    _current_split.reset();
    return nullptr;
  }
  _produced += read;
  return velox::BaseVector::create<velox::RowVector>(_read_type, read,
                                                     &_memory_pool);
}

void RocksDBFullScanDataSource::addDynamicFilter(
  velox::column_index_t output_channel,
  const std::shared_ptr<velox::common::Filter>& filter) {
  VELOX_UNSUPPORTED();
}

uint64_t RocksDBFullScanDataSource::getCompletedBytes() {
  // TODO: implement completed bytes tracking
  return 0;
}

uint64_t RocksDBFullScanDataSource::getCompletedRows() { return _produced; }

std::unordered_map<std::string, velox::RuntimeMetric>
RocksDBFullScanDataSource::getRuntimeStats() {
  // TODO: implement runtime stats reporting
  return {};
}

void RocksDBFullScanDataSource::cancel() {
  // TODO: implement cancellation logic
}

velox::VectorPtr RocksDBFullScanDataSource::ReadColumn(
  velox::column_index_t col_idx, uint64_t max_size) {
  auto& it = *_iterators[col_idx];
  auto column_id = _column_ids[col_idx];

  if (column_id == catalog::Column::kGeneratedPKId) {
    return ReadColumnFromKey(it, max_size);
  }

  const auto& type = _read_type->childAt(col_idx);
  if (type->kind() == velox::TypeKind::UNKNOWN) {
    return ReadUnknownColumn(it, max_size);
  }

  return VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(ReadScalarColumn, type->kind(), it,
                                            max_size);
}

template<velox::TypeKind Kind>
velox::VectorPtr RocksDBFullScanDataSource::ReadScalarColumn(
  rocksdb::Iterator& it, uint64_t max_size) {
  using T = typename velox::TypeTraits<Kind>::NativeType;
  auto result = velox::BaseVector::create<velox::FlatVector<T>>(
    velox::Type::create<Kind>(), kInitialVectorSize, &_memory_pool);
  result->resize(max_size, false);

  const auto vector_size = IterateColumn(
    it, max_size,
    [&](uint64_t value_idx, std::string_view, std::string_view value) {
      if (!value.empty()) {
        SetResultValue(value, value_idx, *result);
      } else {
        result->setNull(value_idx, true);
      }
    });

  if (vector_size != result->size()) {
    SDB_ASSERT(vector_size < result->size(),
               "RocksDBDataSource: inconsistent vector size");
    result->resize(vector_size, false);
  }
  return result;
}

velox::VectorPtr RocksDBFullScanDataSource::ReadUnknownColumn(
  rocksdb::Iterator& it, uint64_t max_size) {
  uint64_t vector_size = IterateColumn(
    it, max_size, [](uint64_t, std::string_view, std::string_view) {});
  return velox::BaseVector::createNullConstant(velox::UNKNOWN(), vector_size,
                                               &_memory_pool);
}

velox::VectorPtr RocksDBFullScanDataSource::ReadColumnFromKey(
  rocksdb::Iterator& it, uint64_t max_size) {
  // For now only generated PK column is supported
  auto result = velox::BaseVector::create<velox::FlatVector<int64_t>>(
    velox::BIGINT(), kInitialVectorSize, &_memory_pool);
  result->resize(max_size, true);

  const auto vector_size = IterateColumn(
    it, max_size,
    [&](uint64_t value_idx, std::string_view key, std::string_view value) {
      auto val = primary_key::ReadSigned<int64_t>(
        std::string_view{key.begin() + kKeyPrefixSize, key.end()});
      result->set(value_idx, val);
    });
  if (vector_size != result->size()) {
    SDB_ASSERT(
      vector_size < result->size(),
      "RocksDBDataSource: inconsistent vector size of fake primary column");
    result->resize(vector_size, true);
  }
  return result;
}

template<std::invocable<uint64_t, std::string_view, std::string_view> Callback>
uint64_t RocksDBFullScanDataSource::IterateColumn(rocksdb::Iterator& it,
                                                  uint64_t max_size,
                                                  const Callback& func) {
  uint64_t vector_size = 0;

  while (it.Valid() && max_size > vector_size) {
    func(vector_size, it.key().ToStringView(), it.value().ToStringView());
    ++vector_size;
    it.Next();
  }

  rocksutils::CheckIteratorStatus(it);

  return vector_size;
}

template<typename Source>
void RocksDBPointLookupDataSource<Source>::BuildKeys(size_t batch_size,
                                                     size_t key_columns) {
  const size_t total_keys = batch_size * key_columns;
  const size_t old_size = _keys.size();
  _keys.resize(total_keys);
  if (old_size >= total_keys) {
    for (size_t rank = 0; rank < key_columns; ++rank) {
      for (size_t point_idx = 0; point_idx < batch_size; ++point_idx) {
        auto& key = _keys[rank * batch_size + point_idx];
        SDB_ASSERT(key.size() >= kKeyPrefixSize);
        key.resize(kKeyPrefixSize);
        primary_key::Create(*_values, _offset + point_idx, key);
      }
    }
  } else {
    for (size_t rank = 0; rank < key_columns; ++rank) {
      const size_t col_idx = _sorted_col_indices[rank];
      for (size_t point_idx = 0; point_idx < batch_size; ++point_idx) {
        auto& key = _keys[rank * batch_size + point_idx];
        key.clear();
        key_utils::AppendTableKey(key, _object_key);
        key_utils::AppendColumnKey(key, _column_ids[col_idx]);
        primary_key::Create(*_values, _offset + point_idx, key);
      }
    }
  }
}

template<typename Source>
void RocksDBPointLookupDataSource<Source>::PerformMultiGet(size_t total_keys) {
  _key_slices.resize(total_keys);
  for (size_t i = 0; i < total_keys; ++i) {
    _key_slices[i] = _keys[i];
  }
  _ctx.MultiGet(_cf, _key_slices, _raw_values, _statuses);
}

velox::RowVectorPtr RocksDBBaseDataSource::ApplyRemainingFilter(
  velox::RowVectorPtr batch) {
  if (!_remaining_expr_set) {
    SDB_ASSERT(batch->childrenSize() == _output_column_count);
    return batch;
  }

  const velox::vector_size_t num_rows = batch->size();
  velox::SelectivityVector rows(num_rows);
  velox::VectorPtr result;
  _evaluator->evaluate(_remaining_expr_set.get(), rows, *batch, result);

  velox::DecodedVector decoded(*result, rows);
  velox::vector_size_t passing = 0;
  auto indices = velox::allocateIndices(num_rows, &_memory_pool);
  auto* raw_indices = indices->template asMutable<velox::vector_size_t>();
  for (velox::vector_size_t i = 0; i < num_rows; ++i) {
    if (!decoded.isNullAt(i) && decoded.valueAt<bool>(i)) {
      raw_indices[passing++] = i;
    }
  }

  auto out_type = velox::ROW(
    std::vector<std::string>(_read_type->names().begin(),
                             _read_type->names().begin() +
                               static_cast<ptrdiff_t>(_output_column_count)),
    std::vector<velox::TypePtr>(
      _read_type->children().begin(),
      _read_type->children().begin() +
        static_cast<ptrdiff_t>(_output_column_count)));

  if (passing == 0) {
    return velox::BaseVector::create<velox::RowVector>(out_type, 0,
                                                       &_memory_pool);
  }

  if (passing != batch->size()) {
    std::vector<velox::VectorPtr> filtered;
    filtered.reserve(_output_column_count);
    for (size_t i = 0; i < _output_column_count; ++i) {
      filtered.push_back(velox::BaseVector::wrapInDictionary(
        nullptr, indices, passing, batch->childAt(static_cast<int32_t>(i))));
    }

    batch =
      std::make_shared<velox::RowVector>(&_memory_pool, std::move(out_type),
                                         nullptr, passing, std::move(filtered));
  }

  if (batch->childrenSize() > _output_column_count) {
    auto output_type =
      velox::ROW(std::vector<std::string>(
                   _read_type->names().begin(),
                   _read_type->names().begin() + _output_column_count),
                 std::vector<velox::TypePtr>(
                   _read_type->children().begin(),
                   _read_type->children().begin() + _output_column_count));
    std::vector<velox::VectorPtr> output_children(
      batch->children().begin(),
      batch->children().begin() + _output_column_count);
    batch = std::make_shared<velox::RowVector>(
      &_memory_pool, std::move(output_type), batch->nulls(), batch->size(),
      std::move(output_children));
  }
  return batch;
}

template<typename Source>
size_t RocksDBPointLookupDataSource<Source>::CheckAndCountFound(
  size_t keys_count) const {
  size_t found = 0;
  for (size_t i = 0; i < keys_count; ++i) {
    if (_statuses[i].ok()) {
      ++found;
    } else {
      SDB_ENSURE(_statuses[i].IsNotFound(),
                 rocksutils::ConvertStatus(_statuses[i]));
    }
  }
  return found;
}

template<typename Source>
void RocksDBPointLookupDataSource<Source>::FinalizeOffset(size_t batch_size) {
  _offset += batch_size;
  if (_offset >= _values->size()) {
    _current_split.reset();
    _offset = 0;
  }
}

template<typename Source>
std::optional<velox::RowVectorPtr> RocksDBPointLookupDataSource<Source>::next(
  uint64_t size, velox::ContinueFuture&) {
  SDB_ASSERT(size);
  if (!_current_split) {
    return nullptr;
  }

  SDB_ASSERT(_values);
  SDB_ASSERT(_values->size() > 0,
             "Case of empty filters should be processed in connector");

  const auto num_columns = _read_type->size();
  const auto total_points = static_cast<size_t>(_values->size());
  const auto batch_size =
    std::min(static_cast<size_t>(size), total_points - _offset);
  SDB_ASSERT(batch_size > 0);

  if (num_columns == 0) {
    // count(*) path: no projected columns — issue one read per point using
    // the effective column, count how many points exist, return a 0-column
    // RowVector with that many rows.
    SDB_ASSERT(_column_ids.size() == 1);
    SDB_ASSERT(!_remaining_expr_set);
    BuildKeys(batch_size, 1);

    for (size_t i = 1; i < batch_size; ++i) {
      SDB_ASSERT(_keys[i] > _keys[i - 1], "Failed on ", i);
    }

    PerformMultiGet(batch_size);
    const size_t found_count = CheckAndCountFound(batch_size);
    _produced += found_count;
    FinalizeOffset(batch_size);

    return velox::BaseVector::create<velox::RowVector>(_read_type, found_count,
                                                       &_memory_pool);
  }

  // TODO(mkornaukhov) try to load only the first column and get mask of which
  // rows are present. This may allow us to reduce number of multiget calls
  BuildKeys(batch_size, num_columns);
  PerformMultiGet(batch_size * num_columns);
  size_t found_count = CheckAndCountFound(batch_size * num_columns);
  SDB_ASSERT(found_count % num_columns == 0);
  found_count /= num_columns;

  std::vector<velox::VectorPtr> columns;
  columns.reserve(num_columns);
  for (size_t column_idx = 0; column_idx < num_columns; ++column_idx) {
    const auto& type = _read_type->childAt(column_idx);
    columns.emplace_back(VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
      ReadPointColumnValues, type->kind(), _col_rank[column_idx], batch_size,
      found_count, _statuses, _raw_values, _memory_pool));
  }

  SDB_ASSERT(
    absl::c_all_of(columns,
                   [&](const auto& col) { return col->size() == found_count; }),
    "RocksDBPointLookupDataSource: inconsistent columns");

  FinalizeOffset(batch_size);

  auto batch = ApplyRemainingFilter(std::make_shared<velox::RowVector>(
    &_memory_pool, _read_type, nullptr, found_count, std::move(columns)));

  _produced += batch->size();
  return batch;
}

template class RocksDBPointLookupDataSource<rocksdb::Transaction>;
template class RocksDBPointLookupDataSource<rocksdb::DB>;

}  // namespace sdb::connector
