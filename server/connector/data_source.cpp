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

namespace sdb::connector {
namespace {

constexpr uint64_t kInitialVectorSize = 1;  // arbitrary value

}  // namespace

RocksDBDataSource::RocksDBDataSource(
  velox::memory::MemoryPool& memory_pool, rocksdb::Transaction* transaction,
  rocksdb::ColumnFamilyHandle& cf, velox::RowTypePtr row_type,
  std::vector<catalog::Column::Id> column_oids,
  catalog::Column::Id effective_column_id, ObjectId object_key)
  : velox::connector::DataSource{},
    _memory_pool{memory_pool},
    _transaction{transaction},
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

  _read_options.snapshot = _transaction->GetSnapshot();
  _read_options.async_io = false;

  std::string key = key_utils::PrepareTableKey(_object_key);
  _table_prefix_size = key.size();

  _column_keys.reserve(_column_ids.size());
  _last_read_keys.resize(_column_ids.size());

  for (const auto column_id : _column_ids) {
    basics::StrResize(key, _table_prefix_size);

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
  std::iota(_sorted_indices.begin(), _sorted_indices.end(), 0);
  std::sort(_sorted_indices.begin(), _sorted_indices.end(),
            [this](auto a, auto b) { return _column_ids[a] < _column_ids[b]; });

  // Create single iterator for read-your-own-writes
  _iterator.reset(_transaction->GetIterator(_read_options, &_cf));
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


std::optional<velox::RowVectorPtr> RocksDBDataSource::next(
  uint64_t size, velox::ContinueFuture& future) {
  SDB_ASSERT(size);
  if (!_current_split) {
    return nullptr;
  }

  std::vector<velox::VectorPtr> columns;

  const auto num_columns = _row_type->size();
  const auto table_prefix_size =
    _column_keys.empty()
      ? 0
      : _column_keys.front().size() - sizeof(catalog::Column::Id);

  if (num_columns) {
    columns.resize(num_columns);
    for (const auto sorted_idx : _sorted_indices) {
      const auto& column_key = _column_keys[sorted_idx];

      // Seek to the appropriate position
      if (_last_read_keys[sorted_idx].empty()) {
        _iterator->Seek(column_key);
      } else {
        _iterator->Seek(_last_read_keys[sorted_idx]);
        _iterator->Next();
      }

      if (!_iterator->Valid() || !_iterator->key().starts_with(column_key)) {
        // no rows found. This should happen only for the first column.
        // Otherwise we have a misaligned data.
        SDB_ASSERT(columns.empty() || absl::c_all_of(columns, [](const auto& c) { return !c; }),
                   "RocksDBDataSource: inconsistent number of columns");
        _current_split.reset();
        return nullptr;
      }

      columns[sorted_idx] = ReadColumn(
        size, _row_type->childAt(sorted_idx), _column_ids[sorted_idx],
        sorted_idx, table_prefix_size);

      // Check if data is exhausted (first column determines this)
      if (sorted_idx == _sorted_indices[0] && columns[sorted_idx]->size() == 0) {
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

  const auto& column_key = _column_keys[0];
  if (_last_read_keys[0].empty()) {
    _iterator->Seek(column_key);
  } else {
    _iterator->Seek(_last_read_keys[0]);
    _iterator->Next();
  }

  if (!_iterator->Valid() || !_iterator->key().starts_with(column_key)) {
    _current_split.reset();
    return nullptr;
  }

  const auto read =
    IterateColumn(size, column_key, 0,
                  [](uint64_t, std::string_view, std::string_view) {});
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

velox::VectorPtr RocksDBDataSource::ReadColumn(uint64_t max_size,
                                               const velox::TypePtr& type,
                                               catalog::Column::Id column_id,
                                               velox::column_index_t col_idx,
                                               size_t table_prefix_size) {
  const auto& column_key = _column_keys[col_idx];
  if (column_id == catalog::Column::kGeneratedPKId) {
    return ReadColumnFromKey(max_size, column_key, col_idx, table_prefix_size);
  }
  if (type->kind() == velox::TypeKind::UNKNOWN) {
    return ReadUnknownColumn(max_size, column_key, col_idx);
  }

  return VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(ReadScalarColumn, type->kind(),
                                            max_size, column_key, col_idx);
}

template<velox::TypeKind Kind>
velox::VectorPtr RocksDBDataSource::ReadScalarColumn(
  uint64_t max_size, std::string_view column_key, velox::column_index_t col_idx) {
  using T = typename velox::TypeTraits<Kind>::NativeType;
  auto result = velox::BaseVector::create<velox::FlatVector<T>>(
    velox::Type::create<Kind>(), kInitialVectorSize, &_memory_pool);

  const auto vector_size = IterateColumn(
    max_size, column_key, col_idx,
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
  uint64_t max_size, std::string_view column_key, velox::column_index_t col_idx) {
  uint64_t vector_size =
    IterateColumn(max_size, column_key, col_idx,
                  [](uint64_t, std::string_view, std::string_view) {});
  return velox::BaseVector::createNullConstant(velox::UNKNOWN(), vector_size,
                                               &_memory_pool);
}

velox::VectorPtr RocksDBDataSource::ReadColumnFromKey(
  uint64_t max_size, std::string_view column_key,
  velox::column_index_t col_idx, size_t table_prefix_size) {
  auto result = velox::BaseVector::create<velox::FlatVector<int64_t>>(
    velox::BIGINT(), kInitialVectorSize, &_memory_pool);

  const auto vector_size = IterateColumn(
    max_size, column_key, col_idx,
    [&](uint64_t value_idx, std::string_view key, std::string_view value) {
      if (value_idx == result->size()) {
        result->resize(result->size() * 2, false);
      }

      auto val = primary_key::ReadSigned<int64_t>(std::string_view{
        key.begin() + table_prefix_size + sizeof(catalog::Column::Id),
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
uint64_t RocksDBDataSource::IterateColumn(uint64_t max_size,
                                          std::string_view column_key,
                                          velox::column_index_t col_idx,
                                          const Callback& func) {
  uint64_t vector_size = 0;

  while (_iterator->Valid() && max_size > vector_size &&
         _iterator->key().starts_with(column_key)) {
    func(vector_size, _iterator->key().ToStringView(), _iterator->value().ToStringView());
    _last_read_keys[col_idx] = _iterator->key().ToString();
    ++vector_size;
    _iterator->Next();
  }
  return vector_size;
}

}  // namespace sdb::connector
