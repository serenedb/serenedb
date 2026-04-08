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

#include "rocksdb_materializer.hpp"

#include <velox/vector/FlatVector.h>

#include "key_utils.hpp"
#include "primary_key.hpp"
#include "rocksdb_column_decoder.hpp"
#include "rocksdb_engine_catalog/rocksdb_option_feature.h"
#include "rocksdb_engine_catalog/rocksdb_utils.h"

namespace sdb::connector {
namespace {

// TODO(mbkkt) benchmark and choose best threshold
constexpr size_t kSeekThreshold = 100;
constexpr size_t kColumnKeySize =
  sizeof(ObjectId) + sizeof(catalog::Column::Id);

}  // namespace

RocksDBMaterializer::RocksDBMaterializer(
  velox::memory::MemoryPool& memory_pool, const rocksdb::Snapshot* snapshot,
  rocksdb::DB* db, rocksdb::Transaction* transaction,
  rocksdb::ColumnFamilyHandle& cf, velox::RowTypePtr row_type,
  std::vector<catalog::Column::Id> column_oids,
  catalog::Column::Id effective_column_id, ObjectId object_key)
  : _memory_pool{memory_pool},
    _db{db},
    _transaction{transaction},
    _cf{cf},
    _row_type{std::move(row_type)},
    _column_ids(std::move(column_oids)),
    _effective_column_id(std::move(effective_column_id)),
    _object_key{object_key},
    _read_options{[&] {
      rocksdb::ReadOptions opts;
      opts.async_io = IsIOUringEnabled();
      opts.snapshot = snapshot;
      return opts;
    }()},
    _multiget_ctx{_cf, _read_options} {
  _multiget_buffer_allocator =
    std::make_unique<facebook::velox::HashStringAllocator>(&_memory_pool);
  SDB_ASSERT((_db != nullptr) != (_transaction != nullptr),
             "Only one data source should be specified");
}

const std::string& RocksDBMaterializer::ReadValue(std::string_view full_key) {
  rocksdb::Status status;
  if (_db) {
    status = _db->Get(_read_options, &_cf, full_key, &_value_buffer);
  } else {
    status = _transaction->Get(_read_options, &_cf, full_key, &_value_buffer);
  }
  if (!status.ok()) {
    auto res = sdb::rocksutils::ConvertStatus(status);
    SDB_THROW(res.errorNumber(),
              "Failed to read value by PK: ", res.errorMessage());
  }
  return _value_buffer;
}

velox::RowVectorPtr RocksDBMaterializer::ReadRows(
  std::span<const std::string> row_keys, velox::VectorPtr scores,
  std::vector<velox::VectorPtr> offsets_per_field) {
  std::vector<velox::VectorPtr> columns;
  _new_batch = true;
  const auto num_columns = _row_type->size();
  if (!num_columns) {
    return velox::BaseVector::create<velox::RowVector>(
      _row_type, row_keys.size(), &_memory_pool);
  }
  std::string key = key_utils::PrepareTableKey(_object_key);
  const auto table_prefix_size = key.size();

  size_t offsets_field_idx = 0;
  for (velox::column_index_t col_idx = 0; col_idx < num_columns; ++col_idx) {
    basics::StrResize(key, table_prefix_size);
    const auto column_id = _column_ids[col_idx];

    if (column_id == catalog::Column::kInvertedIndexScoreId) {
      SDB_ASSERT(scores);
      SDB_ASSERT(scores->size() == row_keys.size());
      columns.push_back(std::move(scores));
      continue;
    }

    if (column_id == catalog::Column::kInvertedIndexOffsetsId) {
      SDB_ASSERT(offsets_field_idx < offsets_per_field.size());
      auto& offsets = offsets_per_field[offsets_field_idx++];
      SDB_ASSERT(offsets);
      SDB_ASSERT(offsets->size() == row_keys.size());
      columns.push_back(std::move(offsets));
      continue;
    }
    auto read_column_id = _column_ids[col_idx];
    if (column_id == catalog::Column::kGeneratedPKId) {
      // TODO(Dronplane): optimize this case - if there is at least one
      // non-generated column we can read generated column in one pass with
      // actually stored column. Same for UNKNOWN column.
      SDB_ASSERT(_effective_column_id != catalog::Column::kGeneratedPKId,
                 "DataSource: generated PK column is not an effective one");
      read_column_id = _effective_column_id;
    }

    key_utils::AppendColumnKey(key, read_column_id);
    columns.push_back(
      ReadColumnKeys(row_keys, column_id, _row_type->childAt(col_idx), key));
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

template<typename Decoder>
void RocksDBMaterializer::IterateColumnKeys(
  std::string_view column_key, std::span<const std::string> row_keys,
  const Decoder& func) {
  std::string buffer(column_key);
  for (size_t idx = 0; idx < row_keys.size(); ++idx) {
    buffer.resize(column_key.size());
    buffer.append(row_keys[idx]);
    func(idx, buffer, ReadValue(buffer));
  }
}

void RocksDBMaterializer::PrepareSortedBatch(
  std::span<const std::string> row_keys) {
  if (_new_batch) {
    _read_idxs.resize(row_keys.size());
    absl::c_iota(_read_idxs, 0);
    absl::c_sort(_read_idxs, [&](size_t lhs, size_t rhs) {
      return row_keys[lhs] < row_keys[rhs];
    });
    size_t required_size = kColumnKeySize * row_keys.size();
    required_size = absl::c_accumulate(
      row_keys, required_size,
      [](auto init, const auto& rk) { return init + rk.size(); });
    SDB_ASSERT(required_size < std::numeric_limits<uint32_t>::max());
    if (!_multi_get_buffer || _multi_get_buffer->size() < required_size) {
      if (_multi_get_buffer) {
        _multiget_buffer_allocator->free(_multi_get_buffer);
      }
      _multi_get_buffer = _multiget_buffer_allocator->allocate(
        static_cast<uint32_t>(required_size));
    }
    size_t offset = kColumnKeySize;
    for (auto idx : _read_idxs) {
      const auto& key = row_keys[idx];
      memcpy(_multi_get_buffer->begin() + offset, key.data(), key.size());
      offset += key.size() + kColumnKeySize;
    }
    _new_batch = false;
  }
#ifdef SDB_DEV
  {
    SDB_ASSERT(_multi_get_buffer);
    size_t required_size = kColumnKeySize * row_keys.size();
    required_size = absl::c_accumulate(
      row_keys, required_size,
      [](auto init, const auto& rk) { return init + rk.size(); });
    SDB_ASSERT(_multi_get_buffer->size() >= required_size);
  }
#endif
}

template<typename Decoder, typename MultiGetSource>
void RocksDBMaterializer::MultiGetIterateColumnKeys(
  std::string_view column_key, std::span<const std::string> row_keys,
  MultiGetSource& src, const Decoder& func) {
  PrepareSortedBatch(row_keys);
  _key_slices.resize(row_keys.size());
  SDB_ASSERT(column_key.size() == kColumnKeySize);
  size_t offset = 0;
  for (size_t i = 0; i < _read_idxs.size(); ++i) {
    memcpy(_multi_get_buffer->begin() + offset, column_key.data(),
           kColumnKeySize);
    const auto full_key_size = kColumnKeySize + row_keys[_read_idxs[i]].size();
    _key_slices[i] = {_multi_get_buffer->begin() + offset, full_key_size};
    offset += full_key_size;
  }
  size_t sorted_pos = 0;
  _multiget_ctx.MultiGet(
    src, _key_slices,
    [&](rocksdb::Slice key, const rocksdb::PinnableSlice& value,
        rocksdb::Status status) {
      if (!status.ok()) {
        auto res = sdb::rocksutils::ConvertStatus(status);
        SDB_THROW(res.errorNumber(),
                  "Failed to read value by PK: ", res.errorMessage());
      }
      func(_read_idxs[sorted_pos++], key.ToStringView(), value.ToStringView());
    });
}

template<typename Decoder, typename MultiGetSource>
void RocksDBMaterializer::SeekIterateColumnKeys(
  std::string_view column_key, catalog::Column::Id column_id,
  std::span<const std::string> row_keys, MultiGetSource& src,
  const Decoder& func) {
  PrepareSortedBatch(row_keys);
  rocksdb::Iterator* column_iterator = nullptr;
  auto it = _iterators.find(column_id);

  auto make_iterator = [&] {
    auto it_options = _read_options;
    // We store iterators between batches and async_io can hang
    // if we start it on one thread but next batch would be handled by another
    // thread.
    it_options.async_io = false;
    if constexpr (std::is_same_v<MultiGetSource, rocksdb::Transaction>) {
      return std::unique_ptr<rocksdb::Iterator>(
        src.GetIterator(it_options, &_cf));
    } else {
      return std::unique_ptr<rocksdb::Iterator>(
        src.NewIterator(it_options, &_cf));
    }
  };

  if (it == _iterators.end()) {
    column_iterator =
      _iterators.emplace(column_id, make_iterator()).first->second.get();
  } else {
    column_iterator = it->second.get();
  }
  SDB_ASSERT(column_iterator);

  size_t offset = 0;
  for (auto idx : _read_idxs) {
    const auto& key = row_keys[idx];
    memcpy(_multi_get_buffer->begin() + offset, column_key.data(),
           kColumnKeySize);
    SDB_ASSERT(memcmp(_multi_get_buffer->begin() + offset + kColumnKeySize,
                      key.data(), key.size()) == 0);
    column_iterator->Seek(rocksdb::Slice(_multi_get_buffer->begin() + offset,
                                         kColumnKeySize + key.size()));
    if (!column_iterator->Valid()) {
      SDB_THROW(ERROR_INTERNAL, "Invalid iterator read state");
    }
    if (column_iterator->key() !=
        rocksdb::Slice(_multi_get_buffer->begin() + offset,
                       kColumnKeySize + key.size())) {
      SDB_THROW(ERROR_INTERNAL, "Missing key");
    }
    rocksutils::CheckIteratorStatus(*column_iterator);
    func(idx, column_iterator->key().ToStringView(),
         column_iterator->value().ToStringView());
    offset += kColumnKeySize + key.size();
  }
}

velox::VectorPtr RocksDBMaterializer::ReadColumnKeys(
  std::span<const std::string> row_keys, catalog::Column::Id column_id,
  const velox::TypePtr& type, std::string_view column_key) {
  // special case - exists only here
  if (column_id == catalog::Column::kGeneratedPKId) {
    return ReadGeneratedColumnKeys(row_keys);
  }
  // we know that all rows are present - special case of reading UNKNOWN
  if (type->kind() == velox::TypeKind::UNKNOWN) {
    return ReadUnknownColumnKeys(row_keys);
  }
  const auto n = static_cast<velox::vector_size_t>(row_keys.size());
  auto decoder = MakeRocksDBColumnDecoder(type, n, _memory_pool);
  DispatchColumnRead(
    column_key, column_id, row_keys,
    [&](size_t original_idx, [[maybe_unused]] std::string_view,
        std::string_view value) {
      decoder->Add(static_cast<velox::vector_size_t>(original_idx), value);
    });
  return decoder->Finish(n);
}

velox::VectorPtr RocksDBMaterializer::ReadGeneratedColumnKeys(
  std::span<const std::string> row_keys) {
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

velox::VectorPtr RocksDBMaterializer::ReadUnknownColumnKeys(
  std::span<const std::string> row_keys) {
  return velox::BaseVector::createNullConstant(velox::UNKNOWN(),
                                               row_keys.size(), &_memory_pool);
}

template<typename Decoder>
void RocksDBMaterializer::DispatchColumnRead(
  std::string_view column_key, catalog::Column::Id column_id,
  std::span<const std::string> row_keys, const Decoder& func) {
  if (row_keys.size() > kSeekThreshold) {
    if (_db) {
      SeekIterateColumnKeys(column_key, column_id, row_keys, *_db, func);
    } else {
      SeekIterateColumnKeys(column_key, column_id, row_keys, *_transaction,
                            func);
    }
  } else if (row_keys.size() > MultiGetContext::kMultiGetThreshold) {
    if (_db) {
      MultiGetIterateColumnKeys(column_key, row_keys, *_db, func);
    } else {
      MultiGetIterateColumnKeys(column_key, row_keys, *_transaction, func);
    }
  } else {
    IterateColumnKeys(column_key, row_keys, func);
  }
}

}  // namespace sdb::connector
