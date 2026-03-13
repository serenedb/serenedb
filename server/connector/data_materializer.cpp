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
#include "rocksdb_engine_catalog/rocksdb_common.h"
#include "rocksdb_engine_catalog/rocksdb_utils.h"

namespace sdb::connector {

namespace {

enum class MultiGetState : uint8_t {
  HasMore = 1U << 0U,
  Done = 1U << 1U,  // returned from func when no more keys
};

// TODO(mbkkt) benchmark and choose best threshold
constexpr size_t MultiGetThreshold = 1;

template<typename DataSource>
class MultiGetContext {
 public:


  static constexpr size_t kBatchSize = 32;  // copied from rocksdb



  MultiGetContext(rocksdb::ColumnFamilyHandle& cf, DataSource& data_source, const rocksdb::ReadOptions& options)
  : _cf{cf}, _data_source{data_source}, _read_options{options} { }

  void push_key(std::string_view key) {
    SDB_ASSERT(_current_size < _keys.size());
    _keys[_current_size++] = key;
  }

  // keys should be sorted by same order as rocksdb comparator produce
  template<typename KeyGenerator, typename ValueProcessor>
  void multiGet(KeyGenerator&& key_generator, ValueProcessor&& value_processor) {

  _current_size = 0;

  bool running = true;
  while (running) {
    const auto state = key_generator();
    if (state == MultiGetState::HasMore && _current_size < kBatchSize) {
      continue;
    }
    running = state != MultiGetState::Done;
    if (_current_size == 0) {
      continue;
    }
    if (_current_size <= MultiGetThreshold) {
      _statuses[0] =
         _data_source.Get(_read_options, &_cf, _keys[0], &_values[0]);
    } else {
      _data_source.MultiGet(_read_options, &_cf, _current_size, &_keys[0], &_values[0],
                        &_statuses[0], true);
    }
    for (size_t c = 0; c < _current_size; ++c) {
      if (!_statuses[c].ok()) {
        continue;
      }
      value_processor(_keys[c], _values[c]);
    }
    _current_size = 0;
  }
}

 private:
  // TODO(mbkkt) Maybe move on stack multiGet?
  std::array<rocksdb::Slice, kBatchSize> _keys;
  std::array<rocksdb::Status, kBatchSize> _statuses;
  std::array<rocksdb::PinnableSlice, kBatchSize> _values;
  size_t _current_size;
  rocksdb::ColumnFamilyHandle& _cf;
  DataSource& _data_source;
  const rocksdb::ReadOptions& _read_options;
};

}




Materializer::Materializer(velox::memory::MemoryPool& memory_pool,
                           const rocksdb::Snapshot* snapshot, rocksdb::DB* db,
                           rocksdb::Transaction* transaction,
                           rocksdb::ColumnFamilyHandle& cf,
                           velox::RowTypePtr row_type,
                           std::vector<catalog::Column::Id> column_oids,
                           catalog::Column::Id effective_column_id,
                           ObjectId object_key)
  : _memory_pool{memory_pool},
    _db{db},
    _transaction{transaction},
    _cf{cf},
    _row_type{std::move(row_type)},
    _column_ids(std::move(column_oids)),
    _effective_column_id(std::move(effective_column_id)),
    _object_key{object_key},
    _multiget_buffer_allocator{&_memory_pool} {
  SDB_ASSERT((_db != nullptr) != (_transaction != nullptr),
             "Only one data source should be specified");
  _read_options.async_io = true;
  _read_options.snapshot = snapshot;
  _read_options.verify_checksums = false;
  _upper_bound_keys_data.reserve((sizeof(catalog::Column::Id) + sizeof(ObjectId)) * _column_ids.size());
  if (_db) {
    _value_reader = [&](std::string_view full_key) -> std::string& {
      auto status = _db->Get(_read_options, &_cf, full_key, &_value_buffer);
      if (!status.ok()) {
        auto res = sdb::rocksutils::ConvertStatus(status);
        SDB_THROW(
          res.errorNumber(),
          "Failed to read value by PK from database: ", res.errorMessage());
      }
      return _value_buffer;
    };
  } else {
    _value_reader = [&](std::string_view full_key) -> std::string& {
      auto status =
        _transaction->Get(_read_options, &_cf, full_key, &_value_buffer);
      if (!status.ok()) {
        auto res = sdb::rocksutils::ConvertStatus(status);
        SDB_THROW(
          res.errorNumber(),
          "Failed to read value by PK from transaction: ", res.errorMessage());
      }
      return _value_buffer;
    };
  }
}

velox::RowVectorPtr Materializer::ReadRows(std::span<std::string> row_keys) {
  std::vector<velox::VectorPtr> columns;
  _new_batch = true;
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
      // actually stored column.
      SDB_ASSERT(_effective_column_id != catalog::Column::kGeneratedPKId,
                 "DataSource: generated PK column is not an effective one");
      read_column_id = _effective_column_id;
    }

    key_utils::AppendColumnKey(key, read_column_id);
    columns.push_back(ReadColumnKeys(row_keys, column_id,
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

template<typename Decoder>
void Materializer::IterateColumnKeys(std::string_view column_key,
                                     std::span<std::string> row_keys,
                                     const Decoder& func) {
  std::string buffer(column_key);
  auto cur = row_keys.begin();
  while (cur != row_keys.end()) {
    buffer.resize(column_key.size());
    buffer.append(*cur);
    SDB_ASSERT(_value_reader);
    func(buffer, _value_reader(buffer));
    ++cur;
  }
}

template<typename Decoder, typename MultiGetSource>
void Materializer::MultiGetIterateColumnKeys(std::string_view column_key,
                                             std::span<std::string> row_keys,
                                             MultiGetSource& src,
                                             const Decoder& func) {
  constexpr size_t ColumnKeySize =
      sizeof(ObjectId) + sizeof(catalog::Column::Id);
  
  MultiGetContext<MultiGetSource> ctx(_cf, src, _read_options);
  if (_new_batch) {
    _read_idxs.resize(row_keys.size());
    absl::c_iota(_read_idxs, 0);
    absl::c_sort(_read_idxs, [&](size_t lhs, size_t rhs) { return row_keys[lhs] < row_keys[rhs];});        
    _new_batch = false;
    size_t required_size = ColumnKeySize * row_keys.size();
    required_size = absl::c_accumulate(
      row_keys, required_size,
      [](auto init, const auto& rk) { return init + rk.size(); });
    SDB_ASSERT(required_size < std::numeric_limits<uint32_t>::max());
    if (!_multi_get_buffer || _multi_get_buffer->size() < required_size) {
      if (_multi_get_buffer) {
        _multiget_buffer_allocator.free(_multi_get_buffer);
      }
      _multi_get_buffer = _multiget_buffer_allocator.allocate(
        static_cast<uint32_t>(required_size));
    }

    size_t offset = ColumnKeySize;
    for (auto idx : _read_idxs) {
      const auto& key = row_keys[idx];
      memcpy(_multi_get_buffer->begin() + offset, key.data(), key.size());
      offset += key.size() + ColumnKeySize;
    }
  }
#ifdef SDB_DEV
  {
    SDB_ASSERT(_multi_get_buffer);
    size_t required_size = column_key.size() * row_keys.size();
    required_size = absl::c_accumulate(
      row_keys, required_size,
      [](auto init, const auto& rk) { return init + rk.size(); });
    SDB_ASSERT(_multi_get_buffer->size() >= required_size);
  }
#endif
  auto value_processor = [&](rocksdb::Slice key,
                             const rocksdb::PinnableSlice& value) {
    func(key.ToStringView(), value.ToStringView());
  };
  size_t full_key_offset = 0;
  size_t row_key_idx = 0;
  auto key_generator = [&] {
    SDB_ASSERT(column_key.size() == ColumnKeySize);
    auto& key = row_keys[_read_idxs[row_key_idx++]];
    memcpy(_multi_get_buffer->begin() + full_key_offset, column_key.data(),
           ColumnKeySize);
    SDB_ASSERT(
      memcmp(_multi_get_buffer->begin() + full_key_offset + ColumnKeySize,
             key.data(), key.size()) == 0);
    const auto full_key_size = ColumnKeySize + key.size();
    ctx.push_key(std::string_view(_multi_get_buffer->begin() + full_key_offset,
                                  full_key_size));
    full_key_offset += full_key_size;
    return row_key_idx == row_keys.size() ? MultiGetState::Done
                                          : MultiGetState::HasMore;
  };
  ctx.multiGet(std::move(key_generator), std::move(value_processor));
}

template<typename Decoder, typename MultiGetSource>
void Materializer::SeekIterateColumnKeys(std::string_view column_key,
                                        catalog::Column::Id column_id,
                                         std::span<std::string> row_keys,
                                         MultiGetSource& src,
                                         const Decoder& func) {
  constexpr size_t ColumnKeySize =
    sizeof(ObjectId) + sizeof(catalog::Column::Id);

  if (_new_batch) {
    _read_idxs.resize(row_keys.size());
    absl::c_iota(_read_idxs, 0);
    absl::c_sort(_read_idxs, [&](size_t lhs, size_t rhs) {
      return row_keys[lhs] < row_keys[rhs];
    });
    size_t required_size = ColumnKeySize * row_keys.size();
    required_size = absl::c_accumulate(
      row_keys, required_size,
      [](auto init, const auto& rk) { return init + rk.size(); });
    SDB_ASSERT(required_size < std::numeric_limits<uint32_t>::max());
    if (!_multi_get_buffer || _multi_get_buffer->size() < required_size) {
      if (_multi_get_buffer) {
        _multiget_buffer_allocator.free(_multi_get_buffer);
      }
      _multi_get_buffer = _multiget_buffer_allocator.allocate(
        static_cast<uint32_t>(required_size));
    }

    size_t offset = 0;
    for (auto idx : _read_idxs) {
      const auto& key = row_keys[idx];
      offset += ColumnKeySize;
      memcpy(_multi_get_buffer->begin() + offset , key.data(), key.size());
      offset += key.size();
    }
  }
  rocksdb::Iterator* column_iterator = nullptr;
  auto it = _iterators.find(column_id);
  if (it == _iterators.end()) {
    auto it_options = _read_options;
    it_options.adaptive_readahead = true;
    it_options.auto_prefix_mode = true;
    _upper_bound_keys_data.append(
      key_utils::PrepareColumnKey(_object_key, column_id + 1));

    _upper_bound_slices.emplace_back(_upper_bound_keys_data.data() +
                                       _upper_bound_keys_data.size() -
                                       ColumnKeySize,
                                     ColumnKeySize);
    //it_options.iterate_upper_bound = &_upper_bound_slices.back();
    column_iterator = _iterators.emplace(column_id, std::unique_ptr<rocksdb::Iterator>(
                                    _db->NewIterator(_read_options, &_cf))).first->second.get();
  } else {
    column_iterator = it->second.get();
  }
  SDB_ASSERT(column_iterator);

  size_t offset = 0;
  for (auto idx : _read_idxs) {
    const auto& key = row_keys[idx];
    memcpy(_multi_get_buffer->begin() + offset, column_key.data(),
           ColumnKeySize);
    SDB_ASSERT(memcmp(_multi_get_buffer->begin() + offset + ColumnKeySize,
                      key.data(), key.size()) == 0);
    column_iterator->Seek(rocksdb::Slice(_multi_get_buffer->begin() + offset, ColumnKeySize + key.size()));
    if (!column_iterator->Valid()) {
      SDB_THROW(ERROR_INTERNAL, "Invalid iterator read sate");
    }
    if (column_iterator->key() != rocksdb::Slice(_multi_get_buffer->begin() + offset, ColumnKeySize + key.size())) {
       SDB_THROW(ERROR_INTERNAL, "Missing key");
    }
    rocksutils::CheckIteratorStatus(*column_iterator); 
    func(column_iterator->key().ToStringView(), column_iterator->value().ToStringView());      
    offset += ColumnKeySize + key.size();
  }
}

velox::VectorPtr Materializer::ReadColumnKeys(std::span<std::string> row_keys,
                                              catalog::Column::Id column_id,
                                              velox::TypeKind kind,
                                              std::string_view column_key) {
  if (column_id == catalog::Column::kGeneratedPKId) {
    return ReadGeneratedColumnKeys(row_keys);
  }
  if (kind == velox::TypeKind::UNKNOWN) {
    return ReadUnknownColumnKeys(row_keys);
  }
  return VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(ReadScalarColumnKeys, kind,
                                            row_keys, column_key, column_id);
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
  std::span<std::string> row_keys, std::string_view column_key, catalog::Column::Id column_id) {
  using T = typename velox::TypeTraits<Kind>::NativeType;
  auto result = velox::BaseVector::create<velox::FlatVector<T>>(
    velox::Type::create<Kind>(), row_keys.size(), &_memory_pool);
  velox::vector_size_t vector_idx = 0;
  auto decoder_func= [&]([[maybe_unused]] std::string_view key, std::string_view value) {
        ReadScalarType(value, vector_idx++, *result);
  };
  if (row_keys.size() > 40) {
    if (_db) {
      SeekIterateColumnKeys(column_key,  column_id, row_keys, *_db, decoder_func);
    } else {
      SeekIterateColumnKeys(column_key,  column_id, row_keys, *_transaction, decoder_func);
    }
  } else if (row_keys.size() > MultiGetThreshold) {
    if (_db) {
      MultiGetIterateColumnKeys(column_key, row_keys, *_db, decoder_func);
    } else {
      MultiGetIterateColumnKeys(column_key, row_keys, *_transaction, decoder_func);
    }
  } else {
    IterateColumnKeys(
      column_key, row_keys,decoder_func);
  }
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
