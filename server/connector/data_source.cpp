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
#include "rocksdb_engine_catalog/rocksdb_option_feature.h"

namespace sdb::connector {
namespace {

constexpr uint64_t kInitialVectorSize = 1;  // arbitrary value

}  // namespace

RocksDBDataSource::RocksDBDataSource(
  velox::memory::MemoryPool& memory_pool, rocksdb::ColumnFamilyHandle& cf,
  velox::RowTypePtr row_type, std::vector<catalog::Column::Id> column_oids,
  catalog::Column::Id effective_column_id, ObjectId object_key,
  const rocksdb::Snapshot* snapshot)
  : velox::connector::DataSource{},
    _memory_pool{memory_pool},
    _cf{cf},
    _row_type{std::move(row_type)},
    _column_ids(std::move(column_oids)),
    _effective_column_id(std::move(effective_column_id)),
    _object_key{object_key} {
  SDB_ASSERT(_row_type, "RocksDBDataSource: row type is null");
  SDB_ASSERT(_object_key.isSet(), "RocksDBDataSource: object key is empty");
  SDB_ASSERT(!_column_ids.empty(),
             "RocksDBDataSource: at least one column must be requested");
  SDB_ASSERT(_row_type->size() == 0 || _row_type->size() == _column_ids.size(),
             "RocksDBDataSource: number of columns does not match row type");

  _read_options.snapshot = snapshot;
  _read_options.async_io = false;

  std::string key = key_utils::PrepareTableKey(_object_key);

  _column_keys.reserve(_column_ids.size());

  for (const auto column_id : _column_ids) {
    basics::StrResize(key, kTablePrefixSize);

    auto read_column_id = column_id;
    if (column_id == catalog::Column::kGeneratedPKId) {
      SDB_ASSERT(_effective_column_id != catalog::Column::kGeneratedPKId,
                 "RocksDBDataSource: generated PK column is not effective");
      read_column_id = _effective_column_id;
    }

    key_utils::AppendColumnKey(key, read_column_id);
    _column_keys.push_back(key);
  }

  // Build sorted indices by column_id for sequential RocksDB access
  _sorted_indices.resize(_row_type->size());
  absl::c_iota(_sorted_indices, 0);
  absl::c_sort(_sorted_indices, [&](auto lhs, auto rhs) {
    return _column_ids[lhs] < _column_ids[rhs];
  });
}

RocksDBRYOWDataSource::RocksDBRYOWDataSource(
  velox::memory::MemoryPool& memory_pool, rocksdb::Transaction& transaction,
  rocksdb::ColumnFamilyHandle& cf, velox::RowTypePtr row_type,
  std::vector<catalog::Column::Id> column_ids,
  catalog::Column::Id effective_column_id, ObjectId object_key)
  : RocksDBDataSource(memory_pool, cf, std::move(row_type),
                      std::move(column_ids), effective_column_id, object_key,
                      transaction.GetSnapshot()),
    _transaction{transaction} {
  InitIterators([&] {
    return std::unique_ptr<rocksdb::Iterator>(
      _transaction.GetIterator(_read_options, &_cf));
  });
}

RocksDBSnapshotDataSource::RocksDBSnapshotDataSource(
  velox::memory::MemoryPool& memory_pool, rocksdb::DB& db,
  rocksdb::ColumnFamilyHandle& cf, velox::RowTypePtr row_type,
  std::vector<catalog::Column::Id> column_ids,
  catalog::Column::Id effective_column_id, ObjectId object_key,
  const rocksdb::Snapshot* snapshot)
  : RocksDBDataSource(memory_pool, cf, std::move(row_type),
                      std::move(column_ids), effective_column_id, object_key,
                      snapshot),
    _db{db} {
  InitIterators([&] {
    return std::unique_ptr<rocksdb::Iterator>(
      _db.NewIterator(_read_options, &_cf));
  });
}

void RocksDBRYOWDataSource::addSplit(
  std::shared_ptr<velox::connector::ConnectorSplit> split) {
  RocksDBDataSource::addSplit(std::move(split));
  InitIterators([&] {
    return std::unique_ptr<rocksdb::Iterator>(
      _transaction.GetIterator(_read_options, &_cf));
  });
}

void RocksDBSnapshotDataSource::addSplit(
  std::shared_ptr<velox::connector::ConnectorSplit> split) {
  RocksDBDataSource::addSplit(std::move(split));
  InitIterators([&] {
    return std::unique_ptr<rocksdb::Iterator>(
      _db.NewIterator(_read_options, &_cf));
  });
}

void RocksDBDataSource::addSplit(
  std::shared_ptr<velox::connector::ConnectorSplit> split) {
  SDB_ENSURE(split, ERROR_INTERNAL, "RocksDBDataSource: split is null");
  if (_current_split) {
    SDB_THROW(ERROR_INTERNAL,
              "RocksDBDataSource: a split is already being processed");
  }
  _current_split = std::move(split);
}

template<typename CreateFn>
void RocksDBDataSource::InitIterators(CreateFn&& create) {
  _iterators.clear();
  _iterators.reserve(_column_keys.size());
  for (const auto& column_key : _column_keys) {
    auto it = create();
    it->Seek(column_key);
    if (!it->Valid() || !it->key().starts_with(column_key)) {
      it.reset();
    }
    _iterators.push_back(std::move(it));
  }
}

std::optional<velox::RowVectorPtr> RocksDBDataSource::next(
  uint64_t size, velox::ContinueFuture& future) {
  SDB_ASSERT(size);
  if (!_current_split) {
    return nullptr;
  }

  std::vector<velox::VectorPtr> columns;

  const auto num_columns = _row_type->size();

  if (num_columns > 0) {
    columns.resize(num_columns);
    for (const auto sorted_idx : _sorted_indices) {
      if (!_iterators[sorted_idx]) {
        // No rows found. This should happen only for the first column.
        // Otherwise we have a misaligned data.
        SDB_ASSERT(sorted_idx == _sorted_indices[0],
                   "RocksDBDataSource: inconsistent number of columns");
        _current_split.reset();
        return nullptr;
      }

      columns[sorted_idx] = ReadColumn(sorted_idx, size);

      // All rows are read
      if (sorted_idx == _sorted_indices[0] &&
          columns[sorted_idx]->size() == 0) {
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
  if (!_iterators[0]) {
    _current_split.reset();
    return nullptr;
  }
  const auto read =
    IterateColumn(*_iterators[0], size, _column_keys[0],
                  [](uint64_t, std::string_view, std::string_view) {});

  // All rows are read
  if (read == 0) {
    _current_split.reset();
    return nullptr;
  }
  _produced += read;
  return velox::BaseVector::create<velox::RowVector>(_row_type, read,
                                                     &_memory_pool);
}

void RocksDBDataSource::addDynamicFilter(
  velox::column_index_t output_channel,
  const std::shared_ptr<velox::common::Filter>& filter) {
  VELOX_UNSUPPORTED();
}

uint64_t RocksDBDataSource::getCompletedBytes() {
  // TODO: implement completed bytes tracking
  return 0;
}

uint64_t RocksDBDataSource::getCompletedRows() { return _produced; }

std::unordered_map<std::string, velox::RuntimeMetric>
RocksDBDataSource::getRuntimeStats() {
  // TODO: implement runtime stats reporting
  return {};
}

void RocksDBDataSource::cancel() {
  // TODO: implement cancellation logic
}

velox::VectorPtr RocksDBDataSource::ReadColumn(velox::column_index_t col_idx,
                                               uint64_t max_size) {
  auto& it = *_iterators[col_idx];
  const auto& column_key = _column_keys[col_idx];
  auto column_id = _column_ids[col_idx];

  if (column_id == catalog::Column::kGeneratedPKId) {
    return ReadColumnFromKey(it, max_size, column_key);
  }

  const auto& type = _row_type->childAt(col_idx);
  if (type->kind() == velox::TypeKind::UNKNOWN) {
    return ReadUnknownColumn(it, max_size, column_key);
  }

  return VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(ReadScalarColumn, type->kind(), it,
                                            max_size, column_key);
}

template<velox::TypeKind Kind>
velox::VectorPtr RocksDBDataSource::ReadScalarColumn(
  rocksdb::Iterator& it, uint64_t max_size, std::string_view column_key) {
  using T = typename velox::TypeTraits<Kind>::NativeType;
  auto result = velox::BaseVector::create<velox::FlatVector<T>>(
    velox::Type::create<Kind>(), kInitialVectorSize, &_memory_pool);

  const auto vector_size = IterateColumn(
    it, max_size, column_key,
    [&](uint64_t value_idx, [[maybe_unused]] std::string_view key,
        std::string_view value) {
      if (value_idx == result->size()) {
        result->resize(result->size() * 2, false);
      }
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

velox::VectorPtr RocksDBDataSource::ReadUnknownColumn(
  rocksdb::Iterator& it, uint64_t max_size, std::string_view column_key) {
  uint64_t vector_size =
    IterateColumn(it, max_size, column_key,
                  [](uint64_t, std::string_view, std::string_view) {});
  return velox::BaseVector::createNullConstant(velox::UNKNOWN(), vector_size,
                                               &_memory_pool);
}

velox::VectorPtr RocksDBDataSource::ReadColumnFromKey(
  rocksdb::Iterator& it, uint64_t max_size, std::string_view column_key) {
  auto result = velox::BaseVector::create<velox::FlatVector<int64_t>>(
    velox::BIGINT(), kInitialVectorSize, &_memory_pool);

  const auto vector_size = IterateColumn(
    it, max_size, column_key,
    [&](uint64_t value_idx, std::string_view key, std::string_view value) {
      if (value_idx == result->size()) {
        result->resize(result->size() * 2, false);
      }

      auto val = primary_key::ReadSigned<int64_t>(std::string_view{
        key.begin() + kTablePrefixSize + sizeof(catalog::Column::Id),
        key.end()});
      result->set(value_idx, val);
    });
  if (vector_size != result->size()) {
    SDB_ASSERT(
      vector_size < result->size(),
      "RocksDBDataSource: inconsistent vector size of fake primary column");
    result->resize(vector_size, false);
  }
  return result;
}

template<typename Callback>
uint64_t RocksDBDataSource::IterateColumn(rocksdb::Iterator& it,
                                          uint64_t max_size,
                                          std::string_view column_key,
                                          const Callback& func) {
  uint64_t vector_size = 0;

  while (it.Valid() && max_size > vector_size &&
         it.key().starts_with(column_key)) {
    func(vector_size, it.key().ToStringView(), it.value().ToStringView());
    ++vector_size;
    it.Next();
  }

  rocksutils::CheckIteratorStatus(it);

  return vector_size;
}

}  // namespace sdb::connector
