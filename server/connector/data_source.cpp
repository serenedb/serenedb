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
  velox::memory::MemoryPool& memory_pool, rocksdb::Snapshot* snapshot,
  rocksdb::DB& db, rocksdb::ColumnFamilyHandle& cf, velox::RowTypePtr row_type,
  std::vector<catalog::Column::Id> column_oids, ObjectId object_key)
  : velox::connector::DataSource{},
    _memory_pool{memory_pool},
    _snapshot{snapshot},
    _db{db},
    _cf{cf},
    _row_type{std::move(row_type)},
    _column_ids(std::move(column_oids)),
    _object_key{object_key} {
  SDB_ASSERT(_row_type, "RocksDBDataSource: row type is null");
  SDB_ASSERT(_object_key.isSet(), "RocksDBDataSource: object key is empty");
  SDB_ASSERT(!_column_ids.empty(),
             "RocksDBDataSource: at least one column must be requested");
  SDB_ASSERT(_row_type->size() == 0 || _row_type->size() == _column_ids.size(),
             "RocksDBDataSource: number of columns does not match row type");
}

void RocksDBDataSource::addSplit(
  std::shared_ptr<velox::connector::ConnectorSplit> split) {
  if (_current_split) {
    VELOX_FAIL("A split is already being processed");
  }
  _current_split = split;
  VELOX_CHECK_NOT_NULL(_current_split, "Wrong type of split");
  _last_read_key.clear();
}

std::unique_ptr<rocksdb::Iterator> RocksDBDataSource::CreateColumnIterator(
  const std::string_view column_key, const rocksdb::ReadOptions& read_options) {
  auto it =
    std::unique_ptr<rocksdb::Iterator>(_db.NewIterator(read_options, &_cf));
  it->Seek(column_key + _last_read_key);
  if (!_last_read_key.empty()) {
    SDB_ASSERT(
      it->Valid(),
      "RocksDBDataSource: inconsistent snapshot. Last read key not found");
    SDB_ASSERT(
      it->key() == column_key + _last_read_key,
      "RocksDBDataSource: inconsistent snapshot. Last read key mismatch");
    it->Next();
  }

  if (!it->Valid() || !it->key().starts_with(column_key)) {
    it.reset();
  }
  return it;
}

std::optional<velox::RowVectorPtr> RocksDBDataSource::next(
  uint64_t size, velox::ContinueFuture& future) {
  // TODO(Dronplane) open questions:
  // 1. if size is small just do get here if size is large use future?
  // 2. snapshot management?
  // 3. writebatch with index for read own writes?
  SDB_ASSERT(size);
  if (!_current_split) {
    SDB_ASSERT(_last_read_key.empty(),
               "RocksDBDataSource: inconsistent state, addSplit call missing");
    return nullptr;
  };

  rocksdb::ReadOptions read_options;
  read_options.async_io = size > 1;
  read_options.snapshot = _snapshot;
  std::vector<velox::VectorPtr> columns;

  const auto num_columns = _row_type->size();
  std::string last_column_key;
  std::string key = key_utils::PrepareTableKey(_object_key);
  const auto table_prefix_size = key.size();
  if (num_columns) {
    for (velox::column_index_t col_idx = 0; col_idx < num_columns; ++col_idx) {
      basics::StrResize(key, table_prefix_size);
      const auto column_id = _column_ids[col_idx];

      size_t read_col_idx = col_idx;
      if (column_id == catalog::Column::kGeneratedPKId) {
        SDB_ASSERT(col_idx + 1 < num_columns,
                   "Tables without columns must be processed in analyzer step");
        ++read_col_idx;
      }

      key_utils::AppendColumnKey(key, _column_ids[read_col_idx]);
      auto it = CreateColumnIterator(key, read_options);
      if (!it) {
        // no rows found. This should happen only for the first column.
        // Otherwise we have a misaligned data.
        SDB_ASSERT(columns.empty(),
                   "RocksDBDataSource: inconsistent number of columns");
        _current_split.reset();
        return nullptr;
      }
      columns.push_back(ReadColumn(*it, size, key, _row_type->childAt(col_idx),
                                   column_id, table_prefix_size,
                                   col_idx == 0 ? &last_column_key : nullptr));
    }
    _last_read_key = last_column_key;
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
  const std::string column_key =
    key_utils::PrepareColumnKey(_object_key, _column_ids.front());
  auto it = CreateColumnIterator(column_key, read_options);
  if (!it) {
    _current_split.reset();
    return nullptr;
  }
  const auto read = IterateColumn(
    *it, size, column_key, [](uint64_t, std::string_view, std::string_view) {},
    &_last_read_key);
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

velox::VectorPtr RocksDBDataSource::ReadColumn(
  rocksdb::Iterator& it, uint64_t max_size, std::string_view column_key,
  const velox::TypePtr& type, catalog::Column::Id column_id,
  size_t table_prefix_size, std::string* last_key) {
  if (column_id == catalog::Column::kGeneratedPKId) {
    return ReadColumnFromKey(it, max_size, column_key, table_prefix_size,
                             last_key);
  }
  if (type->kind() == velox::TypeKind::UNKNOWN) {
    return ReadUnknownColumn(it, max_size, column_key, last_key);
  }

  return VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(ReadScalarColumn, type->kind(), it,
                                            max_size, column_key, last_key);
}

template<velox::TypeKind Kind>
velox::VectorPtr RocksDBDataSource::ReadScalarColumn(
  rocksdb::Iterator& it, uint64_t max_size, std::string_view column_key,
  std::string* last_key) {
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
    },
    last_key);

  if (vector_size != result->size()) {
    SDB_ASSERT(vector_size < result->size(),
               "RocksDBDataSource: inconsistent vector size");
    result->resize(vector_size, false);
  }
  return result;
}

velox::VectorPtr RocksDBDataSource::ReadUnknownColumn(
  rocksdb::Iterator& it, uint64_t max_size, std::string_view column_key,
  std::string* last_key) {
  uint64_t vector_size = IterateColumn(
    it, max_size, column_key,
    [](uint64_t, std::string_view, std::string_view) {}, last_key);
  return velox::BaseVector::createNullConstant(velox::UNKNOWN(), vector_size,
                                               &_memory_pool);
}

velox::VectorPtr RocksDBDataSource::ReadColumnFromKey(
  rocksdb::Iterator& it, uint64_t max_size, std::string_view column_key,
  size_t table_prefix_size, std::string* last_key) {
  auto result = velox::BaseVector::create<velox::FlatVector<int64_t>>(
    velox::BIGINT(), kInitialVectorSize, &_memory_pool);

  const auto vector_size = IterateColumn(
    it, max_size, column_key,
    [&](uint64_t value_idx, std::string_view key, std::string_view value) {
      if (value_idx == result->size()) {
        result->resize(result->size() * 2, false);
      }

      auto val = primary_key::ReadSigned<int64_t>(std::string_view{
        key.begin() + table_prefix_size + sizeof(catalog::Column::Id),
        key.end()});
      result->set(value_idx, val);
    },
    last_key);
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
                                          const Callback& func,
                                          std::string* last_key) {
  uint64_t vector_size = 0;

  while (it.Valid() && max_size > vector_size &&
         it.key().starts_with(column_key)) {
    if (last_key) {
      *last_key = it.key().ToStringView().substr(column_key.size());
    }
    func(vector_size, it.key().ToStringView(), it.value().ToStringView());
    ++vector_size;
    it.Next();
  }
  return vector_size;
}

}  // namespace sdb::connector
