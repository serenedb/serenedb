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
#include <velox/vector/ComplexVector.h>
#include <velox/vector/DecodedVector.h>
#include <velox/vector/FlatVector.h>

#include "basics/assert.h"
#include "catalog/identifiers/object_id.h"
#include "catalog/table_options.h"
#include "common.h"
#include "connector/primary_key.hpp"
#include "iresearch/utils/bytes_utils.hpp"
#include "key_utils.hpp"
#include "parquet_materializer.hpp"
#include "rocksdb_engine_catalog/rocksdb_common.h"
#include "text_materializer.hpp"

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

// Allocate an output FlatVector for a column with `count` found rows.
template<velox::TypeKind Kind>
velox::VectorPtr CreatePointsColumnVector(size_t count,
                                          velox::memory::MemoryPool& pool) {
  using T = typename velox::TypeTraits<Kind>::NativeType;
  return velox::BaseVector::create<velox::FlatVector<T>>(
    velox::Type::create<Kind>(), count, &pool);
}

// Fill values[0..n) into result starting at offset, for already-present rows.
template<velox::TypeKind Kind>
void FillPointsColumnValues(velox::BaseVector& result, size_t offset,
                            std::span<const rocksdb::PinnableSlice> values) {
  using T = typename velox::TypeTraits<Kind>::NativeType;
  auto& flat = static_cast<velox::FlatVector<T>&>(result);
  for (size_t i = 0; i < values.size(); ++i) {
    if (values[i].empty()) {
      flat.setNull(offset + i, true);
      continue;
    }
    SetResultValue(values[i].ToStringView(), offset + i, flat);
  }
}

}  // namespace

void PrimaryKeyColumnBuilder::Init(const velox::TypePtr& type, size_t capacity,
                                   velox::memory::MemoryPool& pool) {
  _type_kind = type->kind();
  _vec = VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(CreatePointsColumnVector,
                                            _type_kind, capacity, pool);
  _present_rows.reset(capacity);
}

void PrimaryKeyColumnBuilder::Fill(
  size_t batch_idx, size_t found_idx,
  std::span<const rocksdb::PinnableSlice> values) {
  _present_rows.set(batch_idx);
  VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(FillPointsColumnValues, _type_kind, *_vec,
                                     found_idx, values);
}

velox::VectorPtr PrimaryKeyColumnBuilder::Finish(size_t found_count) {
  SDB_ASSERT(_present_rows.count() == found_count);
  _vec->resize(found_count);
  return std::move(_vec);
}

template<typename Source>
RocksDBFullScanDataSource<Source>::RocksDBFullScanDataSource(
  velox::memory::MemoryPool& memory_pool, Source& source,
  rocksdb::ColumnFamilyHandle& cf, velox::RowTypePtr read_type,
  std::vector<catalog::Column::Id> column_oids,
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
    _source{source},
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

template<typename Source>
void RocksDBFullScanDataSource<Source>::addSplit(
  std::shared_ptr<velox::connector::ConnectorSplit> split) {
  SDB_ENSURE(split, ERROR_INTERNAL, "RocksDBDataSource: split is null");
  if (_current_split) {
    SDB_THROW(ERROR_INTERNAL,
              "RocksDBDataSource: a split is already being processed");
  }
  _current_split = std::move(split);
  if constexpr (std::is_same_v<Source, rocksdb::Transaction>) {
    InitIterators([&](const rocksdb::ReadOptions& options) {
      return std::unique_ptr<rocksdb::Iterator>(
        _source.GetIterator(options, &_cf));
    });
  } else {
    InitIterators([&](const rocksdb::ReadOptions& options) {
      return std::unique_ptr<rocksdb::Iterator>(
        _source.NewIterator(options, &_cf));
    });
  }
}

template<typename Source>
template<std::invocable<const rocksdb::ReadOptions&> CreateFn>
void RocksDBFullScanDataSource<Source>::InitIterators(CreateFn&& create_iter) {
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

template<typename Source>
std::optional<velox::RowVectorPtr> RocksDBFullScanDataSource<Source>::next(
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

void RocksDBBaseDataSource::addDynamicFilter(
  velox::column_index_t, const std::shared_ptr<velox::common::Filter>&) {
  VELOX_UNSUPPORTED();
}

uint64_t RocksDBBaseDataSource::getCompletedBytes() {
  // TODO: implement completed bytes tracking
  return 0;
}

uint64_t RocksDBBaseDataSource::getCompletedRows() { return _produced; }

std::unordered_map<std::string, velox::RuntimeMetric>
RocksDBBaseDataSource::getRuntimeStats() {
  // TODO: implement runtime stats reporting
  return {};
}

void RocksDBBaseDataSource::cancel() {
  // TODO: implement cancellation logic
}

template<typename Source>
velox::VectorPtr RocksDBFullScanDataSource<Source>::ReadColumn(
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

  if (type->kind() == velox::TypeKind::ARRAY) {
    return ReadArrayColumn(it, max_size, type);
  }

  return VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(ReadScalarColumn, type->kind(), it,
                                            max_size);
}

template<typename Source>
template<velox::TypeKind Kind>
velox::VectorPtr RocksDBFullScanDataSource<Source>::ReadScalarColumn(
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

template<typename Source>
velox::VectorPtr RocksDBFullScanDataSource<Source>::ReadUnknownColumn(
  rocksdb::Iterator& it, uint64_t max_size) {
  uint64_t vector_size = IterateColumn(
    it, max_size, [](uint64_t, std::string_view, std::string_view) {});
  return velox::BaseVector::createNullConstant(velox::UNKNOWN(), vector_size,
                                               &_memory_pool);
}

template<typename Source>
velox::VectorPtr RocksDBFullScanDataSource<Source>::ReadArrayColumn(
  rocksdb::Iterator& it, uint64_t max_size, velox::TypePtr array_type) {
  const auto elem_kind = array_type->asArray().elementType()->kind();
  return VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
    ReadScalarArrayColumn, elem_kind, it, max_size, std::move(array_type));
}

template<typename Source>
template<velox::TypeKind ElemKind>
velox::VectorPtr RocksDBFullScanDataSource<Source>::ReadScalarArrayColumn(
  rocksdb::Iterator& it, uint64_t max_size, velox::TypePtr array_type) {
  using ElemT = typename velox::TypeTraits<ElemKind>::NativeType;
  static_assert(!std::is_same_v<ElemT, void>,
                "Complex element types are not supported in array columns");

  const auto& elem_type = array_type->asArray().elementType();

  auto offsets_buf = velox::AlignedBuffer::allocate<velox::vector_size_t>(
    max_size, &_memory_pool);
  auto sizes_buf = velox::AlignedBuffer::allocate<velox::vector_size_t>(
    max_size, &_memory_pool);
  auto* raw_offsets = offsets_buf->template asMutable<velox::vector_size_t>();
  auto* raw_sizes = sizes_buf->template asMutable<velox::vector_size_t>();

  auto elements = velox::BaseVector::create<velox::FlatVector<ElemT>>(
    elem_type, 0, &_memory_pool);

  velox::BufferPtr null_buf;
  velox::vector_size_t elem_offset = 0;

  const auto vector_size = IterateColumn(
    it, max_size,
    [&](uint64_t value_idx, std::string_view, std::string_view value) {
      if (value.empty()) {
        // NULL array
        if (!null_buf) {
          null_buf = velox::allocateNulls(max_size, &_memory_pool);
        }
        velox::bits::setNull(null_buf->asMutable<uint64_t>(), value_idx);
        return;
      }

      raw_offsets[value_idx] = elem_offset;
      const auto* ptr = reinterpret_cast<const uint8_t*>(value.data());
      const uint32_t elem_count = irs::vread<uint32_t>(ptr);
      raw_sizes[value_idx] = static_cast<velox::vector_size_t>(elem_count);

      if (elem_count == 0) {
        return;
      }
      elements->resize(elem_offset + elem_count, true);

      const auto flags = static_cast<ValueFlags>(*ptr++);
      const bool is_constant =
        (flags & ValueFlags::Constant) != ValueFlags::None;
      const bool have_nulls =
        (flags & ValueFlags::HaveNulls) != ValueFlags::None;
      const bool have_length =
        (flags & ValueFlags::HaveLength) != ValueFlags::None;

      uint32_t length_array_size = 0;
      if (have_length) {
        length_array_size = irs::vread<uint32_t>(ptr);
      }

      if (is_constant) {
        SDB_ASSERT(!have_nulls);
        // Remaining bytes are the single constant value (or none for null).
        const auto remaining_size = static_cast<size_t>(
          reinterpret_cast<const uint8_t*>(value.data()) + value.size() - ptr);
        if (remaining_size == 0) {
          for (uint32_t i = 0; i < elem_count; i++) {
            elements->setNull(elem_offset + i, true);
          }
        } else {
          const std::string_view constant_val{
            reinterpret_cast<const char*>(ptr), remaining_size};
          for (uint32_t i = 0; i < elem_count; i++) {
            SetResultValue(constant_val, elem_offset + i, *elements);
          }
        }
      } else {
        // Read optional nulls bitmap.
        const uint8_t* elem_nulls = nullptr;
        if (have_nulls) {
          elem_nulls = ptr;
          ptr += velox::bits::nbytes(elem_count);
        }

        if constexpr (ElemKind == velox::TypeKind::BOOLEAN) {
          // Boolean data is a packed bitset.
          const auto bool_bytes = velox::bits::nbytes(elem_count);
          irs::ResolveBool(elem_nulls, [&]<bool HasNulls> {
            for (uint32_t i = 0; i < elem_count; i++) {
              if constexpr (HasNulls) {
                if (!velox::bits::isBitSet(elem_nulls, i)) {
                  elements->setNull(elem_offset + i, true);
                  continue;
                }
              }
              elements->set(elem_offset + i, velox::bits::isBitSet(ptr, i));
            }
          });
          ptr += bool_bytes;
        } else if constexpr (!velox::TypeTraits<ElemKind>::isFixedWidth) {
          // Variable-length elements (e.g., VARCHAR).
          // Layout: [length_array: length_array_size bytes][string data]
          const uint8_t* lptr = ptr;
          ptr += length_array_size;
          const uint8_t* data_ptr = ptr;

          irs::ResolveBool(elem_nulls, [&]<bool HasNulls> {
            for (uint32_t i = 0; i < elem_count; i++) {
              const uint32_t len = irs::vread<uint32_t>(lptr);
              if constexpr (HasNulls) {
                if (!velox::bits::isBitSet(elem_nulls, i)) {
                  elements->setNull(elem_offset + i, true);
                  data_ptr += len;
                  continue;
                }
              }
              elements->set(elem_offset + i,
                            velox::StringView(
                              reinterpret_cast<const char*>(data_ptr), len));
              data_ptr += len;
            }
          });
        } else {
          // Fixed-width scalar: packed array of count * sizeof(ElemT) bytes.
          memcpy(elements->mutableRawValues() + elem_offset, ptr,
                 elem_count * sizeof(ElemT));
          ptr += elem_count * sizeof(ElemT);
          if (have_nulls) {
            for (uint32_t i = 0; i < elem_count; i++) {
              if (!velox::bits::isBitSet(elem_nulls, i)) {
                elements->setNull(elem_offset + i, true);
              }
            }
          }
        }
      }

      elem_offset += elem_count;
    });

  SDB_ASSERT(vector_size <= max_size);

  rocksutils::CheckIteratorStatus(it);

  return std::make_shared<velox::ArrayVector>(
    &_memory_pool, std::move(array_type), std::move(null_buf), vector_size,
    std::move(offsets_buf), std::move(sizes_buf), std::move(elements));
}

template<typename Source>
velox::VectorPtr RocksDBFullScanDataSource<Source>::ReadColumnFromKey(
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

template<typename Source>
template<std::invocable<uint64_t, std::string_view, std::string_view> Callback>
uint64_t RocksDBFullScanDataSource<Source>::IterateColumn(
  rocksdb::Iterator& it, uint64_t max_size, const Callback& func) {
  uint64_t vector_size = 0;

  while (it.Valid() && max_size > vector_size) {
    func(vector_size, it.key().ToStringView(), it.value().ToStringView());
    ++vector_size;
    it.Next();
  }

  rocksutils::CheckIteratorStatus(it);

  return vector_size;
}

template<typename Policy>
void RocksDBPointLookupDataSource<Policy>::addSplit(
  std::shared_ptr<velox::connector::ConnectorSplit> split) {
  SDB_ENSURE(split, ERROR_INTERNAL, "RocksDBDataSource: split is null");
  if (_current_split) {
    SDB_THROW(ERROR_INTERNAL,
              "RocksDBDataSource: a split is already being processed");
  }
  _current_split = std::move(split);
  _values_offset = 0;
}

template<typename Policy>
void RocksDBPointLookupDataSource<Policy>::BuildBatchKeys(
  catalog::Column::Id col_id, size_t start, size_t count) {
  SDB_ASSERT(count <= kMultiGetChunkSize);
  for (size_t i = 0; i < count; ++i) {
    auto& key = _ctx.Key(i);
    key.clear();
    _key_builder.BuildFullKey(key, col_id, _values[start + i]);
  }
}

template<typename Policy>
size_t RocksDBPointLookupDataSource<Policy>::BuildBatchKeysUsingMask(
  catalog::Column::Id col_id, size_t batch_size) {
  const auto& present = _collector.PresentRows();
  size_t key_idx = 0;
  while (key_idx < batch_size && _in_batch_offset < present.size()) {
    if (present.test(_in_batch_offset)) {
      auto& key = _ctx.Key(key_idx++);
      key.clear();
      _key_builder.BuildFullKey(key, col_id,
                                _values[_values_offset + _in_batch_offset]);
    }
    ++_in_batch_offset;
  }
  return key_idx;
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

template<typename Policy>
std::optional<velox::RowVectorPtr> RocksDBPointLookupDataSource<Policy>::next(
  uint64_t size, velox::ContinueFuture& future) {
  SDB_ASSERT(size);
  if (!_current_split) {
    return nullptr;
  }

  SDB_ASSERT(!_values.empty(),
             "Case of empty filters should be processed in connector");

  const auto num_columns = _read_type->size();
  const size_t batch_size =
    std::min(static_cast<size_t>(size), _values.size() - _values_offset);
  SDB_ASSERT(batch_size > 0);

  // Step 1: fetch column 0 in sorted order to fill _present_rows_batch and,
  // when num_columns >= 1, also populate the column 0 vector directly.
  const size_t least_column_index = _sorted_col_indices[0];
  const auto least_column_id = _column_ids[least_column_index];

  if (num_columns == 0) {
    // count(*) path: only need presence count, no column data or mask needed.
    SDB_ASSERT(_column_ids.size() == 1);
    SDB_ASSERT(!_remaining_expr_set);
    size_t found_count = 0;
    for (size_t start = 0; start < batch_size; start += kMultiGetChunkSize) {
      const size_t chunk = std::min(kMultiGetChunkSize, batch_size - start);
      BuildBatchKeys(least_column_id, _values_offset + start, chunk);
      _ctx.Fetch(_cf, chunk);
      for (size_t i = 0; i < chunk; ++i) {
        if (_ctx.Status(i).ok()) {
          ++found_count;
        } else {
          SDB_ENSURE(_ctx.Status(i).IsNotFound(),
                     rocksutils::ConvertStatus(_ctx.Status(i)));
        }
      }
    }
    _values_offset += batch_size;
    if (_values_offset >= _values.size()) {
      _current_split.reset();
    }
    _produced += found_count;
    return velox::BaseVector::create<velox::RowVector>(_read_type, found_count,
                                                       &_memory_pool);
  }

  _collector.Init(_read_type->childAt(least_column_index), batch_size,
                  _memory_pool);

  size_t found_count = 0;
  for (size_t start = 0; start < batch_size; start += kMultiGetChunkSize) {
    const size_t chunk = std::min(kMultiGetChunkSize, batch_size - start);
    BuildBatchKeys(least_column_id, _values_offset + start, chunk);
    _ctx.Fetch(_cf, chunk);
    const auto all_values = _ctx.Values(chunk);
    for (size_t i = 0; i < chunk; ++i) {
      if (_ctx.Status(i).ok()) {
        _collector.Fill(start + i, found_count, all_values.subspan(i, 1));
        ++found_count;
      } else {
        SDB_ENSURE(_ctx.Status(i).IsNotFound(),
                   rocksutils::ConvertStatus(_ctx.Status(i)));
      }
    }
  }

  irs::Finally _ = [&] noexcept {
    _values_offset += batch_size;
    if (_values_offset >= _values.size()) {
      _current_split.reset();
    }
  };

  if (found_count == 0) {
    return nullptr;
  }

  // Materializer path: PKs collected, materialize and return.
  if constexpr (kIsSecondaryIndex) {
    _produced += found_count;
    return _collector.Finish(found_count);
  }

  // Column path: col0 done via collector, fetch remaining columns.
  auto least_column_vec = _collector.Finish(found_count);

  // Step 2: fill remaining columns using BuildBatchKeysUsingMask.
  std::vector<velox::VectorPtr> columns(num_columns);
  columns[least_column_index] = std::move(least_column_vec);
  for (size_t col_idx :
       std::span{_sorted_col_indices.begin() + 1, _sorted_col_indices.end()}) {
    const auto col_id = _column_ids[col_idx];
    const auto& type = _read_type->childAt(col_idx);

    auto col_vec = VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
      CreatePointsColumnVector, type->kind(), found_count, _memory_pool);

    size_t collected = 0;
    _in_batch_offset = 0;
    while (collected < found_count) {
      const size_t chunk_size =
        BuildBatchKeysUsingMask(col_id, kMultiGetChunkSize);
      _ctx.Fetch(_cf, chunk_size);
      for (size_t i = 0; i < chunk_size; ++i) {
        SDB_ENSURE(_ctx.Status(i).ok(),
                   rocksutils::ConvertStatus(_ctx.Status(i)));
      }
      const auto chunk_values = _ctx.Values(chunk_size);
      VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(FillPointsColumnValues, type->kind(),
                                         *col_vec, collected, chunk_values);
      collected += chunk_size;
    }

    columns[col_idx] = std::move(col_vec);
  }

  SDB_ASSERT(
    absl::c_all_of(columns,
                   [&](const auto& col) { return col->size() == found_count; }),
    "RocksDBPointLookupDataSource: inconsistent columns");

  auto batch = ApplyRemainingFilter(std::make_shared<velox::RowVector>(
    &_memory_pool, _read_type, nullptr, found_count, std::move(columns)));

  _produced += batch->size();
  return batch;
}

template class RocksDBFullScanDataSource<rocksdb::Transaction>;
template class RocksDBFullScanDataSource<rocksdb::DB>;
template class RocksDBPointLookupDataSource<PrimaryLookupPolicy<true>>;
template class RocksDBPointLookupDataSource<PrimaryLookupPolicy<false>>;
template class RocksDBPointLookupDataSource<
  SecondaryLookupPolicy<true, RocksDBMaterializer>>;
template class RocksDBPointLookupDataSource<
  SecondaryLookupPolicy<false, RocksDBMaterializer>>;
template class RocksDBPointLookupDataSource<
  SecondaryLookupPolicy<false, ParquetMaterializer>>;
template class RocksDBPointLookupDataSource<
  SecondaryLookupPolicy<false, TextMaterializer>>;

}  // namespace sdb::connector
