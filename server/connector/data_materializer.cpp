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

#include "data_materializer.hpp"

#include <velox/vector/FlatVector.h>

#include "common.h"
#include "key_utils.hpp"
#include "primary_key.hpp"

namespace sdb::connector {

velox::RowVectorPtr Materializer::ReadRows(std::span<std::string> row_keys) {
  std::vector<velox::VectorPtr> columns;
  const auto num_columns = _row_type->size();
  if (!num_columns) {
    return velox::BaseVector::create<velox::RowVector>(
      _row_type, row_keys.size(), &_memory_pool);
  }
  std::string key = key_utils::PrepareTableKey(_object_key);
  const auto table_prefix_size = key.size();

  for (velox::column_index_t col_idx = 0; col_idx < num_columns; ++col_idx) {
    basics::StrResize(key, table_prefix_size);
    const auto column_id = _column_ids[col_idx];

    auto read_column_id = _column_ids[col_idx];
    if (column_id == catalog::Column::kGeneratedPKId) {
      // TODO(Dronplane): optimize this case - if there is at least one
      // non-generated column we can read generated column in one pass with
      // actually stored column. More to say  - we must do this to properly
      // handle materialization failures. Same for UNKNOWN column.
      SDB_ASSERT(_effective_column_id != catalog::Column::kGeneratedPKId,
                 "DataSource: generated PK column is not an effective one");
      read_column_id = _effective_column_id;
    }

    key_utils::AppendColumnKey(key, read_column_id);
    auto it = CreateIterator();
    if (!it) {
      // no rows found. This should happen only for the first column.
      // Otherwise we have a misaligned data.
      SDB_ASSERT(columns.empty(), "DataSource: inconsistent number of columns");
      return nullptr;
    }
    columns.push_back(ReadColumnKeys(*it, row_keys, column_id,
                                     _row_type->childAt(col_idx)->kind(), key));
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

std::unique_ptr<rocksdb::Iterator> Materializer::CreateIterator() {
  rocksdb::ReadOptions read_options;
  read_options.async_io = false;
  read_options.snapshot = _snapshot;
  return std::unique_ptr<rocksdb::Iterator>(
    _db ? _db->NewIterator(read_options, &_cf)
        : _transaction->GetIterator(read_options, &_cf));
}

template<typename Decoder>
void Materializer::IterateColumnKeys(rocksdb::Iterator& it,
                                     std::string_view column_key,
                                     std::span<std::string> row_keys,
                                     const Decoder& func) {
  std::string buffer(column_key);
  auto cur = row_keys.begin();
  while (cur != row_keys.end()) {
    buffer.resize(column_key.size());
    buffer.append(*cur);
    // TODO(Dronplane) measure performance sorted vs unsorted keys.
    ReadColumnCell(it, buffer, cur == row_keys.begin(), func);
    ++cur;
  }
}

template<typename Decoder>
void Materializer::ReadColumnCell(rocksdb::Iterator& it,
                                  std::string_view full_key, bool use_seek,
                                  const Decoder& func) {
  auto key_slice = rocksdb::Slice{full_key};
  if (_is_range && !use_seek) {
    it.Next();
    if (it.key() != full_key) {
      _is_range = false;
      it.Seek(key_slice);
    }
  } else {
    it.Seek(key_slice);
  }
  SDB_ENSURE(it.Valid() && it.key() == full_key, ERROR_INTERNAL,
             "Invalid primary key read.");
  func(it.key().ToStringView(), it.value().ToStringView());
}

velox::VectorPtr Materializer::ReadColumnKeys(rocksdb::Iterator& it,
                                              std::span<std::string> row_keys,
                                              catalog::Column::Id column_id,
                                              velox::TypeKind kind,
                                              std::string_view column_key) {
  if (column_id == catalog::Column::kGeneratedPKId) {
    return ReadGeneratedColumnKeys(row_keys);
  }
  if (kind == velox::TypeKind::UNKNOWN) {
    return ReadUnknownColumnKeys(row_keys);
  }
  return VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(ReadScalarColumnKeys, kind, it,
                                            row_keys, column_key);
}

velox::VectorPtr Materializer::ReadGeneratedColumnKeys(
  std::span<std::string> row_keys) {
  using T = typename velox::TypeTraits<velox::TypeKind::BIGINT>::NativeType;
  auto result = velox::BaseVector::create<velox::FlatVector<T>>(
    velox::BIGINT(), row_keys.size(), &_memory_pool);
  velox::vector_size_t vector_idx = 0;
  for (const auto& key : row_keys) {
    SDB_ASSERT(key.size() == sizeof(int64_t));
    auto val = primary_key::ReadSigned<int64_t>(key);
    result->set(vector_idx++, val);
  }
  return result;
}

velox::VectorPtr Materializer::ReadUnknownColumnKeys(
  std::span<std::string> row_keys) {
  return velox::BaseVector::createNullConstant(velox::UNKNOWN(),
                                               row_keys.size(), &_memory_pool);
}

template<velox::TypeKind Kind>
velox::VectorPtr Materializer::ReadScalarColumnKeys(
  rocksdb::Iterator& it, std::span<std::string> row_keys,
  std::string_view column_key) {
  using T = typename velox::TypeTraits<Kind>::NativeType;
  auto result = velox::BaseVector::create<velox::FlatVector<T>>(
    velox::Type::create<Kind>(), row_keys.size(), &_memory_pool);
  velox::vector_size_t vector_idx = 0;
  IterateColumnKeys(
    it, column_key, row_keys,
    [&]([[maybe_unused]] std::string_view key, std::string_view value) {
      ReadScalarType(value, vector_idx++, *result);
    });
  return result;
}

template<typename T>
void Materializer::ReadScalarType(std::string_view value,
                                  velox::vector_size_t idx,
                                  velox::FlatVector<T>& vector) {
  if (!value.empty()) {
    if constexpr (std::is_same_v<T, velox::StringView>) {
      const size_t offset = value[0] == 0 ? 1 : 0;
      velox::StringView val(value.data() + offset, value.size() - offset);
      vector.set(idx, val);
    } else if constexpr (std::is_same_v<T, bool>) {
      SDB_ASSERT(value.size() == kTrueValue.size(),
                 "DataSource: unexpected value size for bool column");
      vector.set(idx, value == kTrueValue);
    } else {
      SDB_ASSERT(value.size() == sizeof(T),
                 "DataSource: unexpected value size for scalar column");
      T tmp;
      memcpy(&tmp, value.data(), sizeof(T));
      vector.set(idx, tmp);
    }
  } else {
    vector.setNull(idx, true);
  }
}

}  // namespace sdb::connector
