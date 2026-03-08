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

template<velox::TypeKind Kind>
velox::VectorPtr ReadPointColumnValues(
  size_t col_idx, size_t num_points, size_t num_columns,
  const std::vector<rocksdb::Status>& statuses,
  const std::vector<std::string>& values,
  velox::memory::MemoryPool& memory_pool) {
  using T = typename velox::TypeTraits<Kind>::NativeType;

  auto result = velox::BaseVector::create<velox::FlatVector<T>>(
    velox::Type::create<Kind>(), num_points, &memory_pool);

  for (size_t point_idx = 0; point_idx < num_points; ++point_idx) {
    const size_t idx = point_idx * num_columns + col_idx;
    const auto& status = statuses[idx];
    const auto& value = values[idx];

    if (status.IsNotFound()) {
      // Filtered point is not found, it's OK, just skip
      continue;
    }

    if (!status.ok()) {
      auto res = rocksutils::ConvertStatus(status);
      SDB_THROW(std::move(res));
    }

    if constexpr (std::is_same_v<T, velox::StringView>) {
      const size_t offset = value[0] == 0 ? 1 : 0;
      result->set(point_idx, velox::StringView(value.data() + offset,
                                               value.size() - offset));
    } else if constexpr (std::is_same_v<T, bool>) {
      SDB_ASSERT(value.size() == kTrueValue.size(),
                 "RocksDBDataSource: unexpected value size for bool column");
      result->set(point_idx, value == kTrueValue);
    } else {
      SDB_ASSERT(value.size() == sizeof(T),
                 "RocksDBDataSource: unexpected value size for scalar column");
      T tmp;
      memcpy(&tmp, value.data(), sizeof(T));
      result->set(point_idx, tmp);
    }
  }
  return result;
}

}  // namespace

void MultiGetContext::ExtractValues(size_t count, size_t dest_start,
                                    std::vector<std::string>& values) {
  for (size_t i = 0; i < count; ++i) {
    if (_pinnable[i].IsPinned()) {
      values[dest_start + i] = _pinnable[i].ToStringView();
    } else {
      values[dest_start + i] = std::move(*_pinnable[i].GetSelf());
    }
  }
}

RocksDBFullScanDataSource::RocksDBFullScanDataSource(
  velox::memory::MemoryPool& memory_pool, rocksdb::ColumnFamilyHandle& cf,
  velox::RowTypePtr row_type, std::vector<catalog::Column::Id> column_oids,
  catalog::Column::Id effective_column_id, ObjectId object_key,
  const rocksdb::Snapshot* snapshot)
  : velox::connector::DataSource{},
    _memory_pool{memory_pool},
    _cf{cf},
    _snapshot{snapshot},
    _object_key{object_key},
    _column_ids(std::move(column_oids)),
    _row_type{std::move(row_type)},
    _effective_column_id(std::move(effective_column_id)) {
  SDB_ASSERT(_row_type, "RocksDBDataSource: row type is null");
  SDB_ASSERT(_object_key.isSet(), "RocksDBDataSource: object key is empty");
  SDB_ASSERT(!_column_ids.empty(),
             "RocksDBDataSource: at least one column must be requested");
  SDB_ASSERT(_row_type->size() == 0 || _row_type->size() == _column_ids.size(),
             "RocksDBDataSource: number of columns does not match row type");

  std::string key = key_utils::PrepareTableKey(_object_key);

  const auto num_columns = _column_ids.size();
  static constexpr size_t kKeySize =
    kTablePrefixSize + sizeof(catalog::Column::Id);

  _column_keys.reserve(num_columns);
  _upper_bound_keys_data.reserve(kKeySize * num_columns);
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
      _upper_bound_keys_data.data() + i * kKeySize, kKeySize);
  }
}

RocksDBRYOWFullScanDataSource::RocksDBRYOWFullScanDataSource(
  velox::memory::MemoryPool& memory_pool, rocksdb::Transaction& transaction,
  rocksdb::ColumnFamilyHandle& cf, velox::RowTypePtr row_type,
  std::vector<catalog::Column::Id> column_ids,
  catalog::Column::Id effective_column_id, ObjectId object_key)
  : RocksDBFullScanDataSource{memory_pool,
                              cf,
                              std::move(row_type),
                              std::move(column_ids),
                              effective_column_id,
                              object_key,
                              transaction.GetSnapshot()},
    _transaction{transaction} {}

RocksDBSnapshotFullScanDataSource::RocksDBSnapshotFullScanDataSource(
  velox::memory::MemoryPool& memory_pool, rocksdb::DB& db,
  rocksdb::ColumnFamilyHandle& cf, velox::RowTypePtr row_type,
  std::vector<catalog::Column::Id> column_ids,
  catalog::Column::Id effective_column_id, ObjectId object_key,
  const rocksdb::Snapshot* snapshot)
  : RocksDBFullScanDataSource{memory_pool,
                              cf,
                              std::move(row_type),
                              std::move(column_ids),
                              effective_column_id,
                              object_key,
                              snapshot},
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

  const auto num_columns = _row_type->size();
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
    _produced += columns.front()->size();
    return std::make_shared<velox::RowVector>(&_memory_pool, _row_type, nullptr,
                                              columns.front()->size(),
                                              std::move(columns));
  }
  SDB_ASSERT(_column_ids.size() == 1);
  const auto read = IterateColumn(
    *_iterators[0], size, [](uint64_t, std::string_view, std::string_view) {});

  // All rows are read
  if (read == 0) {
    _current_split.reset();
    return nullptr;
  }
  _produced += read;
  return velox::BaseVector::create<velox::RowVector>(_row_type, read,
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

  const auto& type = _row_type->childAt(col_idx);
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
        if constexpr (std::is_same_v<T, velox::StringView>) {
          const size_t offset = value[0] == 0 ? 1 : 0;
          velox::StringView val(value.data() + offset, value.size() - offset);
          result->set(value_idx, val);
        } else if constexpr (std::is_same_v<T, bool>) {
          SDB_ASSERT(
            value.size() == kTrueValue.size(),
            "RocksDBDataSource: unexpected value size for bool column");
          result->set(value_idx, value == kTrueValue);
        } else {
          SDB_ASSERT(
            value.size() == sizeof(T),
            "RocksDBDataSource: unexpected value size for scalar column");
          T tmp;
          memcpy(&tmp, value.data(), sizeof(T));
          result->set(value_idx, tmp);
        }
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
      auto val = primary_key::ReadSigned<int64_t>(std::string_view{
        key.begin() + kTablePrefixSize + sizeof(catalog::Column::Id),
        key.end()});
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

template<typename Derived>
std::optional<velox::RowVectorPtr> RocksDBPointLookupDataSource<Derived>::next(
  uint64_t size, velox::ContinueFuture&) {
  SDB_ASSERT(size);
  if (!_current_split)
    return nullptr;

  if (!_values) [[unlikely]] {
    _current_split.reset();
    return nullptr;
  }
  SDB_ASSERT(_values->size() > 0,
             "Case of empty filters should be processed in connector");

  const auto num_columns = _row_type->size();
  SDB_ASSERT(num_columns > 0);

  const auto total_points = static_cast<size_t>(_values->size());
  const auto batch_size =
    std::min(static_cast<size_t>(size), total_points - _offset);

  if (batch_size == 0) {
    _current_split.reset();
    return nullptr;
  }

  const size_t total_keys = batch_size * num_columns;

  _keys.resize(total_keys);
  for (size_t point_idx = 0; point_idx < batch_size; ++point_idx) {
    for (size_t col_idx = 0; col_idx < num_columns; ++col_idx) {
      auto& key = _keys[point_idx * num_columns + col_idx];
      key.clear();
      key_utils::AppendTableKey(key, _object_key);
      key_utils::AppendColumnKey(key, _column_ids[col_idx]);
      primary_key::Create(*_values, _offset + point_idx, key);
    }
  }

  _key_slices.resize(total_keys);
  for (size_t i = 0; i < total_keys; ++i) {
    _key_slices[i] = _keys[i];
  }

  _ctx.MultiGet(
    _cf, _key_slices, _raw_values, _statuses,
    [this](rocksdb::ColumnFamilyHandle& cf, const rocksdb::Slice& key,
           rocksdb::PinnableSlice* value, rocksdb::Status* status) {
      static_cast<Derived*>(this)->DoGet(cf, key, value, status);
    },
    [this](rocksdb::ColumnFamilyHandle& cf, size_t keys_number,
           const rocksdb::Slice* keys, rocksdb::PinnableSlice* values,
           rocksdb::Status* statuses) {
      static_cast<Derived*>(this)->DoMultiGetBatch(cf, keys_number, keys,
                                                   values, statuses);
    });

  std::vector<velox::VectorPtr> columns;
  columns.reserve(num_columns);
  for (size_t column_idx = 0; column_idx < num_columns; ++column_idx) {
    const auto& type = _row_type->childAt(column_idx);
    columns.emplace_back(VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
      ReadPointColumnValues, type->kind(), column_idx, batch_size, num_columns,
      _statuses, _raw_values, _memory_pool));
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

template class RocksDBPointLookupDataSource<RocksDBRYOWPointLookupDataSource>;
template class RocksDBPointLookupDataSource<
  RocksDBSnapshotPointLookupDataSource>;

}  // namespace sdb::connector
