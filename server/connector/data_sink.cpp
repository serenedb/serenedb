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

#include "data_sink.hpp"

#include <velox/common/base/SimdUtil.h>
#include <velox/type/Type.h>
#include <velox/vector/BiasVector.h>
#include <velox/vector/BuilderTypeUtils.h>
#include <velox/vector/FlatMapVector.h>
#include <velox/vector/FlatVector.h>

#include "basics/assert.h"
#include "basics/containers/flat_hash_set.h"
#include "basics/endian.h"
#include "basics/exceptions.h"
#include "basics/misc.hpp"
#include "catalog/identifiers/object_id.h"
#include "catalog/table_options.h"
#include "common.h"
#include "connector/primary_key.hpp"
#include "iresearch/utils/bytes_utils.hpp"
#include "key_utils.hpp"
#include "rocksdb_engine_catalog/rocksdb_utils.h"

#if __has_feature(memory_sanitizer)
#define DATA_SINK_USE_MEMORY_SANITIZER
#endif

namespace {

// TODO(Dronplane) unify with key?
constexpr std::string_view kStringPrefix{"\0", 1};
constexpr std::string_view kZeroLengthVector{"\0", 1};
constexpr std::string_view kOneValueHeader{"\0\1", 2};

// We encode NULL as empty slice
void WriteNull(rocksdb::Transaction& trx, rocksdb::ColumnFamilyHandle& cf,
               std::string_view key) {
  auto status = trx.Put(&cf, rocksdb::Slice(key), {});
  if (!status.ok()) {
    SDB_THROW(sdb::rocksutils::ConvertStatus(status));
  }
}

}  // namespace

namespace sdb::connector {

RocksDBDataSink::RocksDBDataSink(
  rocksdb::Transaction& transaction, rocksdb::ColumnFamilyHandle& cf,
  velox::memory::MemoryPool& memory_pool, ObjectId object_key,
  std::span<const velox::column_index_t> key_childs,
  std::vector<catalog::Column::Id> column_oids, bool skip_primary_key_columns)
  : _transaction{transaction},
    _cf{cf},
    _object_key{object_key},
    _column_ids{std::move(column_oids)},
    _memory_pool{memory_pool},
    _row_slices{memory_pool},
    _keys_buffers{memory_pool},
    _bytes_allocator{&memory_pool},
    _skip_primary_key_columns{skip_primary_key_columns} {
  _key_childs.assign_range(key_childs);
  SDB_ASSERT(_object_key.isSet(), "RocksDBDataSink: object key is empty");
  SDB_ASSERT(!_column_ids.empty(), "RocksDBDataSink: no columns in a table");
}

// TODO(Dronplane)
// Looks like it is possible to inspect input vector and create vector of
// writers and avoid switch/case for kinds and encodings on each row.
void RocksDBDataSink::appendData(velox::RowVectorPtr input) {
  static_assert(basics::IsLittleEndian());
  SDB_ASSERT(input->encoding() == velox::VectorEncoding::Simple::ROW);
  // UPDATE with PK columns changing would have PK columns at the
  // beginning and same columns again as write data. So here we validate
  // column oids size against input type not row type size.
  SDB_ASSERT(input->type()->size() == _column_ids.size(),
             "RocksDBDataSink: column oids size ", _column_ids.size(),
             " doesn't match input type size ", input->type()->size());
  SDB_ASSERT(input->type()->kind() == velox::TypeKind::ROW);

  // TODO(Dronplane) implement updating PK fields
  const std::string table_key = key_utils::PrepareTableKey(_object_key);
  const auto num_rows = input->size();
  _keys_buffers.clear();
  _keys_buffers.reserve(num_rows);

  for (size_t row_idx = 0; row_idx < num_rows; ++row_idx) {
    auto& key_buffer = _keys_buffers.emplace_back();
    key_utils::MakeColumnKey(
      input, _key_childs, row_idx, table_key,
      [&](std::string_view row_key) {
        auto status = _transaction.GetKeyLock(&_cf, row_key, false, true);
        if (!status.ok()) {
          SDB_THROW(rocksutils::ConvertStatus(status));
        }
      },
      key_buffer);
  }

  velox::IndexRange all_rows(0, num_rows);
  const auto num_columns = input->childrenSize();
  [[maybe_unused]] const auto& input_type = input->type()->asRow();
  for (velox::column_index_t i = 0; i < num_columns; ++i) {
    if (_skip_primary_key_columns && i < _key_childs.size()) {
      continue;
    }
    _column_id = _column_ids[i];
    if (_column_id != catalog::Column::kGeneratedPKId) {
      WriteColumn(input->childAt(i), folly::Range{&all_rows, 1}, {});
    }
  }
}

// Stores column backed by Flat vector.
// Actual ranges might be from decoding of dictionary encoded vector.
// In that case original_idx stores indexes in the initial column - used for
// rocksdb key setting. Vector ranges are iterated and each element is stored in
// rocksdb as a single value.
template<velox::TypeKind Kind>
void RocksDBDataSink::WriteFlatColumn(
  const velox::BaseVector& input,
  const folly::Range<const velox::IndexRange*>& ranges,
  std::span<const velox::vector_size_t> original_idx) {
  // Flat vectors of complex types ARRAY, MAP and ROW / STRUCT are
  // represented using ArrayVector, MapVector and RowVector respectively.
  static_assert(velox::TypeTraits<Kind>::isPrimitiveType &&
                  Kind != velox::TypeKind::UNKNOWN,
                "Only primitive and known type kinds should be here.");
  using T = typename velox::TypeTraits<Kind>::NativeType;
  auto* flat_vector = input.as<velox::FlatVector<T>>();
  SDB_ASSERT(flat_vector);
  irs::ResolveBool(flat_vector->mayHaveNulls(), [&]<bool MayHaveNulls>() {
    size_t row_id = 0;
    for (const auto& range : ranges) {
      const auto range_end = range.begin + range.size;
      for (velox::vector_size_t idx = range.begin; idx < range_end; ++idx) {
        const auto& key = SetupRowKey(row_id++, original_idx);
        if constexpr (MayHaveNulls) {
          if (flat_vector->isNullAt(idx)) {
            WriteNull(_transaction, _cf, key);
            continue;
          }
        }
        ResetForNewRow();
        WriteFlatValue(*flat_vector, idx);
        WriteRowSlices(key);
      }
    }
  });
}

template<velox::TypeKind Kind>
void RocksDBDataSink::WriteBiasedColumn(
  const velox::BaseVector& input,
  const folly::Range<const velox::IndexRange*>& ranges,
  std::span<const velox::vector_size_t> original_idx) {
  using T = typename velox::TypeTraits<Kind>::NativeType;
  if constexpr (velox::admitsBias<T>()) {
    auto* bias_vector = input.as<velox::BiasVector<T>>();
    SDB_ASSERT(bias_vector);
    irs::ResolveBool(bias_vector->mayHaveNulls(), [&]<bool MayHaveNulls>() {
      size_t row_id = 0;
      for (const auto& range : ranges) {
        const auto range_end = range.begin + range.size;
        for (velox::vector_size_t idx = range.begin; idx < range_end; ++idx) {
          const auto& key = SetupRowKey(row_id++, original_idx);
          if constexpr (MayHaveNulls) {
            if (bias_vector->isNullAt(idx)) {
              WriteNull(_transaction, _cf, key);
              continue;
            }
          }
          ResetForNewRow();
          auto tmp = bias_vector->valueAtFast(idx);
          WritePrimitive(tmp);
          WriteRowSlices(key);
        }
      }
    });
  } else {
    SDB_THROW(ERROR_NOT_IMPLEMENTED,
              "RocksDB Sink does not support bias encoding of value kind ",
              velox::TypeKindName::toName(Kind));
  }
}

// Writes dictionary encoded column.
// We write only nulls decided by dictionary itself.
// For actual writing we just decode indexes and
// call write on the wrapped column
void RocksDBDataSink::WriteDictionaryColumn(
  const velox::VectorPtr& input,
  const folly::Range<const velox::IndexRange*>& ranges,
  std::span<const velox::vector_size_t> original_idx) {
  ManagedVector<velox::IndexRange> sub_ranges{_memory_pool};
  const auto may_nulls = input->mayHaveNulls();
  const velox::VectorPtr& wrapped =
    velox::BaseVector::wrappedVectorShared(input);
  ManagedVector<velox::vector_size_t> idx_vector{_memory_pool};
  if (original_idx.empty()) {
    SDB_ASSERT(ranges.size() == 1);
    SDB_ASSERT(ranges.begin()->begin == 0);
    idx_vector.resize(ranges.begin()->size);
    absl::c_iota(idx_vector, 0);
    original_idx = std::span<const velox::vector_size_t>(idx_vector);
  }
  size_t current = 0;
  size_t row_id = 0;
  for (const auto& range : ranges) {
    const auto end = range.begin + range.size;
    for (int32_t offset = range.begin; offset < end; ++offset) {
      if (may_nulls && input->isNullAt(offset)) {
        // Write not null rows in order to have continious original idx span
        if (!sub_ranges.empty()) {
          WriteColumn(wrapped, sub_ranges,
                      original_idx.subspan(current, sub_ranges.size()));
          sub_ranges.clear();
        }
        WriteNull(_transaction, _cf, SetupRowKey(row_id++, original_idx));
        current = row_id;
        continue;
      }
      ++row_id;
      sub_ranges.push_back(velox::IndexRange{input->wrappedIndex(offset), 1});
    }
  }
  if (!sub_ranges.empty()) {
    WriteColumn(wrapped, sub_ranges,
                original_idx.subspan(current, sub_ranges.size()));
  }
}

template<velox::VectorEncoding::Simple Encoding>
void RocksDBDataSink::WriteComplexColumn(
  const velox::BaseVector& input,
  const folly::Range<const velox::IndexRange*>& ranges,
  std::span<const velox::vector_size_t> original_idx) {
  size_t row_id = 0;
  for (size_t i = 0; i < ranges.size(); ++i) {
    int32_t begin = ranges[i].begin;
    int32_t end = begin + ranges[i].size;
    for (int32_t offset = begin; offset < end; ++offset) {
      const auto& key = SetupRowKey(row_id++, original_idx);
      if (input.isNullAt(offset)) {
        WriteNull(_transaction, _cf, key);
      } else {
        ResetForNewRow();
        static_assert(
          Encoding == velox::VectorEncoding::Simple::ARRAY ||
            Encoding == velox::VectorEncoding::Simple::ROW ||
            Encoding == velox::VectorEncoding::Simple::MAP ||
            Encoding == velox::VectorEncoding::Simple::FLAT_MAP,
          "Complex type encoding should be only ARRAY, MAP, FLAT_MAP or ROW");
        if constexpr (Encoding == velox::VectorEncoding::Simple::ARRAY) {
          WriteArrayValue(input, offset);
        } else if constexpr (Encoding == velox::VectorEncoding::Simple::ROW) {
          WriteRowValue(input, offset);
        } else if constexpr (Encoding == velox::VectorEncoding::Simple::MAP) {
          WriteMapValue(input, offset);
        } else {
          WriteFlatMapValue(input, offset);
        }
        WriteRowSlices(key);
      }
    }
  }
}

template<velox::TypeKind Kind>
void RocksDBDataSink::WriteConstantColumn(
  const velox::BaseVector& input,
  const folly::Range<const velox::IndexRange*>& ranges,
  std::span<const velox::vector_size_t> original_idx) {
  size_t row_id = 0;
  ResetForNewRow();
  if (input.isNullAt(0)) {
    _row_slices.emplace_back();
  } else {
    WriteConstantValue<Kind>(input);
  }
  for (const auto& range : ranges) {
    const auto end = range.begin + range.size;
    for (int32_t offset = range.begin; offset < end; ++offset) {
      const auto& key = SetupRowKey(row_id++, original_idx);
      WriteRowSlices(key);
    }
  }
}

template<>
void RocksDBDataSink::WriteConstantColumn<velox::TypeKind::OPAQUE>(
  const velox::BaseVector&, const folly::Range<const velox::IndexRange*>&,
  std::span<const velox::vector_size_t>) {
  SDB_THROW(ERROR_NOT_IMPLEMENTED,
            "RocksDB Sink does not support OPAQUE columns");
}

// Main writing method. Used to dispatch actual writes depending on column kind
// and encoding. See corresponding methods for description of storage formats.
void RocksDBDataSink::WriteColumn(
  const velox::VectorPtr& input,
  const folly::Range<const velox::IndexRange*>& ranges,
  std::span<const velox::vector_size_t> original_idx) {
  switch (input->encoding()) {
    case velox::VectorEncoding::Simple::FLAT:
      if (input->typeKind() == velox::TypeKind::UNKNOWN) {
        WriteConstantColumn<velox::TypeKind::UNKNOWN>(*input, ranges,
                                                      original_idx);
        break;
      }
      VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(WriteFlatColumn, input->typeKind(),
                                         *input, ranges, original_idx);
      break;
    case velox::VectorEncoding::Simple::CONSTANT:
      VELOX_DYNAMIC_TYPE_DISPATCH_ALL(WriteConstantColumn, input->typeKind(),
                                      *input, ranges, original_idx);
      break;
    case velox::VectorEncoding::Simple::DICTIONARY:
    case velox::VectorEncoding::Simple::SEQUENCE:
      // for dictionary we must force loading of wrapped lazy vector.
      WriteDictionaryColumn(velox::BaseVector::loadedVectorShared(input),
                            ranges, original_idx);
      break;
    case velox::VectorEncoding::Simple::BIASED:
      VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(WriteBiasedColumn, input->typeKind(),
                                         *input, ranges, original_idx);
      break;
    case velox::VectorEncoding::Simple::LAZY:
      WriteColumn(velox::BaseVector::loadedVectorShared(input), ranges,
                  original_idx);
      break;
    case velox::VectorEncoding::Simple::ARRAY:
      WriteComplexColumn<velox::VectorEncoding::Simple::ARRAY>(*input, ranges,
                                                               original_idx);
      break;
    case velox::VectorEncoding::Simple::ROW:
      WriteComplexColumn<velox::VectorEncoding::Simple::ROW>(*input, ranges,
                                                             original_idx);
      break;
    case velox::VectorEncoding::Simple::MAP:
      WriteComplexColumn<velox::VectorEncoding::Simple::MAP>(*input, ranges,
                                                             original_idx);
      break;
    case velox::VectorEncoding::Simple::FLAT_MAP:
      WriteComplexColumn<velox::VectorEncoding::Simple::FLAT_MAP>(
        *input, ranges, original_idx);
      break;
    default:
      SDB_THROW(ERROR_NOT_IMPLEMENTED,
                "RocksDB Sink does not support column encoding ",
                mapSimpleToName(input->encoding()));
  }
}

// Writes a vector as a single value. Actual format depends on kind and
// encoding. Method is like WriteColumn main dispatching method but for writing
// vectors as cell value.
void RocksDBDataSink::WriteVector(
  const velox::VectorPtr& input,
  const folly::Range<const velox::IndexRange*>& ranges,
  rocksdb::Slice wrapper_nulls, bool force_nulls) {
  SDB_ASSERT(!force_nulls || !wrapper_nulls.empty(),
             "Forced nulls should be present");
  SDB_ASSERT(!absl::c_any_of(ranges, [](const auto& r) { return r.size == 0; }),
             "Empty ranges are not supported - to make empty vector just pass "
             "empty ranges view");
  if (ranges.empty()) {
    // shortcut for empty vector (just a header with zero length)
    _row_slices.push_back(kZeroLengthVector);
    return;
  }

  if (input->encoding() == velox::VectorEncoding::Simple::DICTIONARY) {
    // Dictionary has an optimization to avoid loading wrapped lazy vector until
    // explicitly asked. Here we need to call "mayHaveNulls" so trigger loading.
    input->loadedVector();
  }

  irs::ResolveBool(
    !wrapper_nulls.empty() || input->mayHaveNulls(), [&]<bool MayHaveNulls>() {
      switch (input->encoding()) {
        case velox::VectorEncoding::Simple::FLAT: {
          if (input->typeKind() != velox::TypeKind::UNKNOWN) {
            VELOX_DYNAMIC_SCALAR_TEMPLATE_TYPE_DISPATCH(
              WriteFlatVector, MayHaveNulls, input->typeKind(), *input, ranges,
              wrapper_nulls, force_nulls);
          } else {
            WriteFlatVector<MayHaveNulls, velox::TypeKind::UNKNOWN>(
              *input, ranges, wrapper_nulls, force_nulls);
          }
          break;
        }
        case velox::VectorEncoding::Simple::DICTIONARY:
        case velox::VectorEncoding::Simple::SEQUENCE:
          // we should not encounter multiple layers of dictionary encoding
          // as we decode all layers in once call - that is how "wrappedIndex"
          // etc methods work in dictionary. And code below relies on that.
          SDB_ASSERT(!force_nulls, "Dictionary vectors should not be nested!");
          WriteDictionaryVector<MayHaveNulls>(input, ranges, wrapper_nulls);
          break;
        case velox::VectorEncoding::Simple::ROW:
          WriteRowVector<MayHaveNulls>(*input, ranges, wrapper_nulls,
                                       force_nulls);
          break;
        case velox::VectorEncoding::Simple::ARRAY:
          WriteArrayVector<MayHaveNulls>(*input, ranges, wrapper_nulls,
                                         force_nulls);
          break;
        case velox::VectorEncoding::Simple::MAP:
          WriteMapVector<MayHaveNulls>(*input, ranges, wrapper_nulls,
                                       force_nulls);

          break;
        case velox::VectorEncoding::Simple::FLAT_MAP:
          WriteFlatMapVector<MayHaveNulls>(*input, ranges, wrapper_nulls,
                                           force_nulls);
          break;
        case velox::VectorEncoding::Simple::CONSTANT:
          if (force_nulls) {
            VELOX_DYNAMIC_TEMPLATE_TYPE_DISPATCH_ALL(WriteConstantVector, true,
                                                     input->typeKind(), *input,
                                                     ranges, wrapper_nulls);
          } else {
            VELOX_DYNAMIC_TEMPLATE_TYPE_DISPATCH_ALL(WriteConstantVector, false,
                                                     input->typeKind(), *input,
                                                     ranges, wrapper_nulls);
          }
          break;
        case velox::VectorEncoding::Simple::LAZY:
          WriteVector(velox::BaseVector::loadedVectorShared(input), ranges,
                      wrapper_nulls, force_nulls);
          break;
        case velox::VectorEncoding::Simple::BIASED:
          VELOX_DYNAMIC_SCALAR_TEMPLATE_TYPE_DISPATCH(
            WriteBiasedVector, MayHaveNulls, input->typeKind(), *input, ranges,
            wrapper_nulls, force_nulls);
          break;
        default:
          SDB_THROW(ERROR_NOT_IMPLEMENTED,
                    "RocksDB Sink does not support value vector encoding ",
                    mapSimpleToName(input->encoding()));
      }
    });
}

// clang-format off
// Format is:
// [header] [nulls bitmap if any] [value]
// header is:
// [total number of rows]
// [flags]
// Nulls bitmask might be present if this constant vector was actually wrapped by dictionary
// with some nulls.
// clang-format on
template<bool ForceNulls, velox::TypeKind Kind>
void RocksDBDataSink::WriteConstantVector(
  const velox::BaseVector& input,
  const folly::Range<const velox::IndexRange*>& ranges,
  rocksdb::Slice wrapper_nulls) {
  const uint32_t total_rows_number = absl::c_accumulate(
    ranges, uint32_t{0}, [](auto v, const auto& r) { return v + r.size; });
  SDB_ASSERT(total_rows_number > 0);
  using T = typename velox::KindToFlatVector<Kind>::WrapperType;

  const auto header_size =
    irs::bytes_io<uint32_t>::vsize(total_rows_number) + sizeof(ValueFlags);
  auto* header = _bytes_allocator.allocate(header_size)->begin();
  _row_slices.emplace_back(header, header_size);
  irs::WriteVarint(total_rows_number, header);
  auto flags = ValueFlags::Constant;
  // Makes no sense to store nulls bitmap for UNKNOWN as they are all nulls
  // anyway
  if constexpr (ForceNulls && Kind != velox::TypeKind::UNKNOWN) {
    flags |= ValueFlags::HaveNulls;
    SDB_ASSERT(wrapper_nulls.size() == velox::bits::nbytes(total_rows_number));
    _row_slices.push_back(wrapper_nulls);
  }
  *(header++) = std::bit_cast<char>(flags);
  if constexpr (Kind == velox::TypeKind::UNKNOWN) {
    // shortcut for UNKNOWN vector.
    // Also we use this for optimizing FLAT of UNKNOWNS as they are always
    // constant NULL, so this check before cast to ConstantVector
    SDB_ASSERT(input.isNullAt(0));
    _row_slices.emplace_back();
    return;
  }
  auto* const_vector = input.as<velox::ConstantVector<T>>();
  SDB_ASSERT(const_vector);
  if (const_vector->isNullAt(0)) {
    _row_slices.emplace_back();
    return;
  }
  WriteConstantValue<Kind>(*const_vector);
}

// clang-format off
// Format is:
// [header] [nulls bitmap if any] [offsets array] [sizes array] [keys vector] [values vector]
// header is:
// [total number of rows]
// [flags]
// [size of keys vector in bytes]
// clang-format on
template<bool HaveNulls>
void RocksDBDataSink::WriteMapVector(
  const velox::BaseVector& input,
  const folly::Range<const velox::IndexRange*>& ranges,
  rocksdb::Slice wrapper_nulls, bool force_nulls) {
  SDB_ASSERT(input.encoding() == velox::VectorEncoding::Simple::MAP);
  SDB_ASSERT(input.typeKind() == velox::TypeKind::MAP);
#ifndef DATA_SINK_USE_MEMORY_SANITIZER
  const bool whole_vector =
    ranges.size() == 1 && ranges.front().size == input.size();
#else
  // Memory sanitizer may complain about reading uninitialized memory as for
  // "Nulls" in velox vectors might contain uninitialized data in Offsets/Sizes.
  // And RocksDB computes CRC over entire value triggering sanitizer, So we
  // disable whole vector optimization in that case.
  const bool whole_vector =
    !HaveNulls && ranges.size() == 1 && ranges.front().size == input.size();
#endif

  const uint32_t total_rows_number = [&] {
    if (whole_vector) {
      SDB_ASSERT(input.size() > 0);
      return static_cast<uint32_t>(input.size());
    }
    const auto res = absl::c_accumulate(
      ranges, uint32_t{0}, [](auto v, const auto& r) { return v + r.size; });
    SDB_ASSERT(res > 0);
    return res;
  }();
  // header placeholder
  const auto header_slice_idx = _row_slices.size();
  _row_slices.emplace_back();

  const auto* map_vec = input.as<velox::MapVector>();
  SDB_ASSERT(map_vec);
  if constexpr (HaveNulls) {
    GatherNulls(input, ranges, total_rows_number, whole_vector, wrapper_nulls,
                force_nulls);
  }

  auto raw_sizes = map_vec->rawSizes();
  auto raw_offsets = map_vec->rawOffsets();
  const uint32_t indexes_size =
    sizeof(velox::vector_size_t) * total_rows_number;
  ManagedVector<velox::IndexRange> elements_ranges{_memory_pool};
  if (whole_vector) {
    // TODO(Dronplane) check if all elements are needed and maybe it is still
    // worth recalculating offsets and store just parts of the elements.
    _row_slices.emplace_back(reinterpret_cast<const char*>(raw_offsets),
                             indexes_size);
    _row_slices.emplace_back(reinterpret_cast<const char*>(raw_sizes),
                             indexes_size);
    elements_ranges.emplace_back(0, map_vec->mapKeys()->size());
  } else {
    auto* offsets_slice_start =
      _bytes_allocator.allocate(indexes_size)->begin();
    auto* sizes_slice_start = _bytes_allocator.allocate(indexes_size)->begin();
    auto* offsets_slice =
      reinterpret_cast<velox::vector_size_t*>(offsets_slice_start);
    auto* sizes_slice =
      reinterpret_cast<velox::vector_size_t*>(sizes_slice_start);
    uint32_t current_offset = 0;
#ifdef SDB_DEV
    [[maybe_unused]] size_t row_idx = 0;
#endif
    for (const auto& range : ranges) {
      if constexpr (HaveNulls) {
        if (range.begin < 0) {
          // Null mark from wrapping vector so we omit writing actual value.
          // it should be already accounted in nulls bitmap
#ifdef SDB_DEV
          SDB_ASSERT(!wrapper_nulls.empty());
          SDB_ASSERT(!velox::bits::isBitSet(
            const_cast<char*>(_row_slices[header_slice_idx + 1].data()),
            row_idx++));
#endif
#ifdef DATA_SINK_USE_MEMORY_SANITIZER
          *offsets_slice = 0;
          *sizes_slice = 0;
#endif

          offsets_slice++;
          sizes_slice++;
          continue;
        }
      }
      for (int32_t i = range.begin; i < range.begin + range.size; ++i) {
        if (raw_sizes[i] && (!HaveNulls || !input.isNullAt(i))) {
          SDB_ASSERT(raw_offsets[i] + raw_sizes[i] <=
                     map_vec->mapKeys()->size());
          SDB_ASSERT(raw_offsets[i] + raw_sizes[i] <=
                     map_vec->mapValues()->size());
          elements_ranges.push_back(
            velox::IndexRange{raw_offsets[i], raw_sizes[i]});
        }
        *offsets_slice++ = current_offset;
        *sizes_slice++ = raw_sizes[i];
        current_offset += raw_sizes[i];
#ifdef SDB_DEV
        if constexpr (HaveNulls) {
          ++row_idx;
        }
#endif
      }
    }
    _row_slices.emplace_back(reinterpret_cast<const char*>(offsets_slice_start),
                             indexes_size);
    _row_slices.emplace_back(reinterpret_cast<const char*>(sizes_slice_start),
                             indexes_size);
  }

  const auto slices_before = _row_slices.size();
  WriteVector(map_vec->mapKeys(), elements_ranges, {}, false);
  const uint32_t keys_size = std::accumulate(
    _row_slices.begin() + slices_before, _row_slices.end(), uint32_t{},
    [&](uint32_t size, const auto& slice) { return size + slice.size(); });
  WriteVector(map_vec->mapValues(), elements_ranges, {}, false);
  const auto header_size = irs::bytes_io<uint32_t>::vsize(total_rows_number) +
                           sizeof(ValueFlags) +
                           irs::bytes_io<uint32_t>::vsize(keys_size);
  auto* header = _bytes_allocator.allocate(header_size)->begin();
  _row_slices[header_slice_idx] = rocksdb::Slice(header, header_size);
  irs::WriteVarint(total_rows_number, header);
  const auto flags = HaveNulls ? ValueFlags::HaveNulls : ValueFlags::None;
  *(header++) = std::bit_cast<char>(flags);
  irs::WriteVarint(keys_size, header);
}

// clang-format off
// Format is:
// [header] [nulls bitmap if any] [length array] [keys vector] [values vectors] [in_maps bitmaps]
// header is:
// [total number of rows]
// [flags]
// [size of lengths array in bytes]
// Number of values always match number of keys. But in_maps bitmaps might be empty
// - we write zero length for such cases so number of elements in length array is
// always equal to  1 + number of keys * 2. As one is for keys vector itself and
// each key has value vector and in_maps bitmap (possibly with zero length).
// clang-format on
template<bool HaveNulls>
void RocksDBDataSink::WriteFlatMapVector(
  const velox::BaseVector& input,
  const folly::Range<const velox::IndexRange*>& ranges,
  rocksdb::Slice wrapper_nulls, bool force_nulls) {
  SDB_ASSERT(input.encoding() == velox::VectorEncoding::Simple::FLAT_MAP);
  SDB_ASSERT(input.typeKind() == velox::TypeKind::MAP);
  const auto whole_vector =
    ranges.size() == 1 && ranges.front().size == input.size();
  const uint32_t total_rows_number = [&] {
    if (whole_vector) {
      SDB_ASSERT(input.size() > 0);
      return static_cast<uint32_t>(input.size());
    }
    const auto res = absl::c_accumulate(
      ranges, uint32_t{0}, [](auto v, const auto& r) { return v + r.size; });
    SDB_ASSERT(res > 0);
    return res;
  }();
  SDB_ASSERT(total_rows_number > 0);
  // header placeholder
  const auto header_slice_idx = _row_slices.size();
  _row_slices.emplace_back();

  const auto* map_vec = input.as<velox::FlatMapVector>();
  SDB_ASSERT(map_vec);
  if constexpr (HaveNulls) {
    GatherNulls(input, ranges, total_rows_number, whole_vector, wrapper_nulls,
                force_nulls);
  }
  ManagedVector<uint32_t> length{_memory_pool};
  uint32_t length_array_size = 0;
  containers::FlatHashSet<velox::vector_size_t> values_needed;
  auto slices_before = _row_slices.size();
  // TODO(Dronplane): make calculate_size common helper method
  auto calculate_size = [&] {
    const auto size = std::accumulate(
      _row_slices.begin() + slices_before, _row_slices.end(), uint32_t{},
      [&](uint32_t size, const auto& slice) { return size + slice.size(); });
    slices_before = _row_slices.size();
    return size;
  };
  // Store keys. Take only necessary keys if not whole vector
  if (whole_vector) {
    velox::IndexRange all_keys{0, map_vec->distinctKeys()->size()};
    WriteVector(map_vec->distinctKeys(), {&all_keys, 1}, {}, false);
  } else {
    // TODO(Dronplane): maybe use some kind of arenas or so for reusing vectors?
    // Or Scratch from velox::memory but with regular vectors to avoid padding
    // burden.
    ManagedVector<velox::IndexRange> keys_ranges{_memory_pool};
    for (velox::vector_size_t key_idx = 0; key_idx < map_vec->numDistinctKeys();
         ++key_idx) {
      bool key_needed = false;
      if (map_vec->inMaps().size() <= static_cast<size_t>(key_idx) ||
          map_vec->inMapsAt(key_idx) == nullptr) {
        // in maps bitmap not present for key or null - means key is present in
        // all rows
        key_needed = true;
      } else {
        const auto& in_map_bitmap = map_vec->inMapsAt(key_idx);
        for (const auto& range : ranges) {
          if constexpr (HaveNulls) {
            if (range.begin < 0) {
              continue;
            }
          }
          for (auto i = range.begin; i < range.begin + range.size; ++i) {
            if (velox::bits::isBitSet(in_map_bitmap->as<uint64_t>(), i)) {
              key_needed = true;
              break;
            }
          }
          if (key_needed) {
            break;
          }
        }
      }
      if (key_needed) {
        values_needed.insert(key_idx);
        if (keys_ranges.empty() ||
            keys_ranges.back().begin + keys_ranges.back().size != key_idx) {
          keys_ranges.emplace_back(key_idx, 1);
        } else {
          keys_ranges.back().size += 1;
        }
        if (values_needed.size() ==
            static_cast<size_t>(map_vec->numDistinctKeys())) {
          // all keys are needed - no need to continue checking
          values_needed.clear();
          keys_ranges.clear();
          keys_ranges.emplace_back(0, map_vec->numDistinctKeys());
          break;
        }
      }
    }
    SDB_ASSERT(!keys_ranges.empty());
    WriteVector(map_vec->distinctKeys(), keys_ranges, {}, false);
  }
  length.push_back(calculate_size());
  length_array_size += irs::bytes_io<uint32_t>::vsize(length.back());
  size_t needed_writes = 0;
  const auto inmap_count = map_vec->inMaps().size();
  if (values_needed.empty()) {
    needed_writes = map_vec->numDistinctKeys() + inmap_count;
  } else {
    needed_writes =
      values_needed.size() + std::min(values_needed.size(), inmap_count);
  }
  SDB_ASSERT(length.size() == 1);
  length.reserve(needed_writes + 1);
  for (velox::vector_size_t value_idx = 0;
       value_idx < map_vec->numDistinctKeys(); ++value_idx) {
    if (!values_needed.empty() && !values_needed.contains(value_idx)) {
      continue;
    }
    if constexpr (HaveNulls) {
      WriteVector(map_vec->mapValuesAt(value_idx), ranges,
                  _row_slices[header_slice_idx + 1], false);
    } else {
      WriteVector(map_vec->mapValuesAt(value_idx), ranges, {}, false);
    }
    length.push_back(calculate_size());
    length_array_size += irs::bytes_io<uint32_t>::vsize(length.back());
  }

  for (size_t value_idx = 0; value_idx < inmap_count; ++value_idx) {
    if (!values_needed.empty() && !values_needed.contains(value_idx)) {
      continue;
    }
    const auto& m = map_vec->inMapsAt(value_idx);
    if (m) {
      if (whole_vector) {
        SDB_ASSERT(m->size() > 0);
        _row_slices.emplace_back(m->as<char>(), m->size());
        length.push_back(m->size());
      } else {
        // lazy gather necessary bits
        auto inmap_buffer_size = velox::bits::nbytes(total_rows_number);
        auto* char_inmap =
          _bytes_allocator.allocate(inmap_buffer_size)->begin();
        memset(char_inmap, 0, velox::bits::nbytes(total_rows_number));
        _row_slices.emplace_back(char_inmap, inmap_buffer_size);
        length.push_back(inmap_buffer_size);
        const auto inmap_max_bit_count =
          static_cast<velox::vector_size_t>(m->size() * 8);
        size_t current_pos = 0;
        for (const auto& range : ranges) {
          if (range.begin < 0) {
            SDB_ASSERT(!velox::bits::isBitSet(char_inmap, current_pos++));
            continue;
          }
          auto src_idx = range.begin;
          auto src_end = range.begin + range.size;
          while (src_idx < inmap_max_bit_count && src_idx < src_end) {
            if (velox::bits::isBitSet(m->as<uint64_t>(), src_idx++)) {
              velox::bits::setBit(char_inmap, current_pos);
            }
            current_pos++;
          }
          while (src_idx < src_end) {
            velox::bits::setBit(char_inmap, current_pos++);
            src_idx++;
          }
        }
      }
    } else {
      length.push_back(0);
    }
    length_array_size += irs::bytes_io<uint32_t>::vsize(length.back());
  }

  const auto header_size =
    irs::bytes_io<uint32_t>::vsize(total_rows_number) + sizeof(ValueFlags) +
    irs::bytes_io<uint32_t>::vsize(length_array_size) + length_array_size;
  auto* header = _bytes_allocator.allocate(header_size)->begin();
  _row_slices[header_slice_idx] = rocksdb::Slice(header, header_size);
  irs::WriteVarint(total_rows_number, header);
  const auto flags = HaveNulls ? ValueFlags::HaveNulls | ValueFlags::FlatMap
                               : ValueFlags::FlatMap;
  *(header++) = std::bit_cast<char>(flags);
  irs::WriteVarint(length_array_size, header);
  for (auto l : length) {
    irs::WriteVarint(l, header);
  }
}

// clang-format off
// Storing Vector of vectors (maybe make array of childs as parameter????)
// Format is: [header] [nulls bitmap if any] [elements length data if any] [vector data]
//  header format is:
//   - vencoded uint32_t vector elements count
//   - 1 byte flags. Marks if there is nulls mask.
//   - [elements length data]
// clang-format on
template<bool HaveNulls>
void RocksDBDataSink::WriteRowVector(
  const velox::BaseVector& input,
  const folly::Range<const velox::IndexRange*>& ranges,
  rocksdb::Slice wrapper_nulls, bool force_nulls) {
  SDB_ASSERT(input.encoding() == velox::VectorEncoding::Simple::ROW);
  SDB_ASSERT(input.typeKind() == velox::TypeKind::ROW);
  const bool whole_vector =
    ranges.size() == 1 && ranges.front().size == input.size();
  const uint32_t total_rows_number = [&] {
    if (whole_vector) {
      SDB_ASSERT(input.size() > 0);
      return static_cast<uint32_t>(input.size());
    }
    const auto res = absl::c_accumulate(
      ranges, uint32_t{0}, [](auto v, const auto& r) { return v + r.size; });
    SDB_ASSERT(res > 0);
    return res;
  }();
  // header placeholder
  const auto header_slice_idx = _row_slices.size();
  _row_slices.emplace_back();
  ManagedVector<uint32_t> length{_memory_pool};
  uint32_t length_array_size = 0;
  const auto* row_vec = input.as<velox::RowVector>();
  SDB_ASSERT(row_vec);
  if constexpr (HaveNulls) {
    GatherNulls(input, ranges, total_rows_number, whole_vector, wrapper_nulls,
                force_nulls);
  }
  length.reserve(row_vec->childrenSize());
  // length placeholder
  _row_slices.emplace_back();
  auto slices_before = _row_slices.size();
  auto calculate_size = [&] {
    const auto size = std::accumulate(
      _row_slices.begin() + slices_before, _row_slices.end(), uint32_t{},
      [&](uint32_t size, const auto& slice) { return size + slice.size(); });
    slices_before = _row_slices.size();
    return size;
  };
  for (const auto& child : row_vec->children()) {
    if constexpr (HaveNulls) {
      WriteVector(child, ranges, _row_slices[header_slice_idx + 1], false);
    } else {
      WriteVector(child, ranges, {}, false);
    }

    SDB_ASSERT(slices_before < _row_slices.size());
    const auto child_size = calculate_size();
    length_array_size += irs::bytes_io<uint32_t>::vsize(child_size);
    length.push_back(child_size);
  }
  const auto header_size = irs::bytes_io<uint32_t>::vsize(total_rows_number) +
                           irs::bytes_io<uint32_t>::vsize(length_array_size) +
                           sizeof(ValueFlags);
  auto* header = _bytes_allocator.allocate(header_size)->begin();
  _row_slices[header_slice_idx] = rocksdb::Slice(header, header_size);
  irs::WriteVarint(total_rows_number, header);
  auto flags = HaveNulls ? ValueFlags::HaveNulls : ValueFlags::None;
  *(header++) = std::bit_cast<char>(flags);
  irs::WriteVarint(length_array_size, header);
  auto* length_slice = _bytes_allocator.allocate(length_array_size)->begin();
  _row_slices[header_slice_idx + (HaveNulls ? 2 : 1)] =
    rocksdb::Slice(length_slice, length_array_size);
  for (auto l : length) {
    irs::WriteVarint(l, length_slice);
  }
}

// clang-format off
// Storing Vector of arrays
// Format is: [header] [nulls bitmap if any] [offsets] [sizes] [elements vector]
//  header format is:
//   - vencoded uint32_t vector elements count
//   - 1 byte flags. Marks if there is nulls mask.
// clang-format on
template<bool HaveNulls>
void RocksDBDataSink::WriteArrayVector(
  const velox::BaseVector& input,
  const folly::Range<const velox::IndexRange*>& ranges,
  rocksdb::Slice wrapper_nulls, bool force_nulls) {
  SDB_ASSERT(input.encoding() == velox::VectorEncoding::Simple::ARRAY);
  SDB_ASSERT(input.typeKind() == velox::TypeKind::ARRAY);
#ifndef DATA_SINK_USE_MEMORY_SANITIZER
  const bool whole_vector =
    ranges.size() == 1 && ranges.front().size == input.size();
#else
  // Memory sanitizer may complain about reading uninitialized memory as for
  // "Nulls" in velox vectors might contain uninitialized data in Offsets/Sizes.
  // And RocksDB computes CRC over entire value triggering sanitizer, So we
  // disable whole vector optimization in that case.
  const bool whole_vector =
    !HaveNulls && ranges.size() == 1 && ranges.front().size == input.size();
#endif
  const uint32_t total_rows_number = [&] {
    if (whole_vector) {
      SDB_ASSERT(input.size() > 0);
      return static_cast<uint32_t>(input.size());
    }
    const auto res = absl::c_accumulate(
      ranges, uint32_t{0}, [](auto v, const auto& r) { return v + r.size; });
    SDB_ASSERT(res > 0);
    return res;
  }();
  // header placeholder
  const auto header_slice_idx = _row_slices.size();
  _row_slices.emplace_back();

  const auto* array_vec = input.as<velox::ArrayVector>();
  SDB_ASSERT(array_vec);
  if constexpr (HaveNulls) {
    GatherNulls(input, ranges, total_rows_number, whole_vector, wrapper_nulls,
                force_nulls);
  }
  auto* raw_sizes = array_vec->rawSizes();
  auto* raw_offsets = array_vec->rawOffsets();
  const uint32_t indexes_size =
    sizeof(velox::vector_size_t) * total_rows_number;
  ManagedVector<velox::IndexRange> elements_ranges{_memory_pool};
  if (whole_vector) {
    // TODO(Dronplane) check if all elements are needed and maybe it is still
    // worth recalculating offsets and store just parts of the elements.
    _row_slices.emplace_back(reinterpret_cast<const char*>(raw_offsets),
                             indexes_size);
    _row_slices.emplace_back(reinterpret_cast<const char*>(raw_sizes),
                             indexes_size);
    elements_ranges.emplace_back(0, array_vec->elements()->size());
  } else {
    auto* offsets_slice_start =
      _bytes_allocator.allocate(indexes_size)->begin();
    auto* sizes_slice_start = _bytes_allocator.allocate(indexes_size)->begin();
    auto* offsets_slice =
      reinterpret_cast<velox::vector_size_t*>(offsets_slice_start);
    auto* sizes_slice =
      reinterpret_cast<velox::vector_size_t*>(sizes_slice_start);
    uint32_t current_offset = 0;
#ifdef SDB_DEV
    [[maybe_unused]] size_t row_idx = 0;
#endif
    for (const auto& range : ranges) {
      if constexpr (HaveNulls) {
        if (range.begin < 0) {
          // Null mark from wrapping vector so we omit writing actual value.
          // it should be already accounted in nulls bitmap
#ifdef SDB_DEV
          SDB_ASSERT(!wrapper_nulls.empty());
          SDB_ASSERT(!velox::bits::isBitSet(
            const_cast<char*>(_row_slices[header_slice_idx + 1].data()),
            row_idx++));
#endif

#ifdef DATA_SINK_USE_MEMORY_SANITIZER
          *offsets_slice = 0;
          *sizes_slice = 0;
#endif
          offsets_slice++;
          sizes_slice++;
          continue;
        }
      }
      for (int32_t i = range.begin; i < range.begin + range.size; ++i) {
        if (raw_sizes[i] && (!HaveNulls || !input.isNullAt(i))) {
          SDB_ASSERT(raw_offsets[i] + raw_sizes[i] <=
                     array_vec->elements()->size());
          elements_ranges.push_back(
            velox::IndexRange{raw_offsets[i], raw_sizes[i]});
        }

        // size might be undefined for nulls.
        const auto cur_size = input.isNullAt(i) ? 0 : raw_sizes[i];
        *offsets_slice++ = current_offset;
        *sizes_slice++ = cur_size;
        current_offset += cur_size;
#ifdef SDB_DEV
        if constexpr (HaveNulls) {
          ++row_idx;
        }
#endif
      }
    }
    _row_slices.emplace_back(reinterpret_cast<const char*>(offsets_slice_start),
                             indexes_size);
    _row_slices.emplace_back(reinterpret_cast<const char*>(sizes_slice_start),
                             indexes_size);
  }

  WriteVector(array_vec->elements(), elements_ranges, {}, false);
  const auto header_size =
    irs::bytes_io<uint32_t>::vsize(total_rows_number) + sizeof(ValueFlags);
  auto* header = _bytes_allocator.allocate(header_size)->begin();
  _row_slices[header_slice_idx] = rocksdb::Slice(header, header_size);
  irs::WriteVarint(total_rows_number, header);
  const auto flags = HaveNulls ? ValueFlags::HaveNulls : ValueFlags::None;
  *(header++) = std::bit_cast<char>(flags);
}

// clang-format off
// Flat encoded vector.
// TODO(Dronplane) make header single slice!
// Format is: [header] [nulls bitmap if any] [elements length data if any] [vector data]
//  header format is:
//   - vencoded uint32_t vector elements count
//   - 1 byte flags. Marks if there is nulls mask. And if there is element length data (maybe we don't need length marker. It could be inferred from type when reading?)
//   - elements length data size if present (so reader could read length and  immediately read value then)
// nulls bitmap is stored as continious block. Size could be inferred from
// vector elements count (see header) elements length data have vencoded
// uint32_t value for each vector element. vector data contains continious
// elements without gaps in case of no length data (e.g. when elements have
// fixed size) in case of length data present it should be used for
// reading data. Method tries to store only pointers to vector data and avoid
// copying when possible. Currenly only exception is BOOLEAN kind as it is
// stored as bitset and we built a new bitset for reqired range.
// clang-format on
template<bool HaveNulls, velox::TypeKind Kind>
void RocksDBDataSink::WriteFlatVector(
  const velox::BaseVector& input,
  const folly::Range<const velox::IndexRange*>& ranges,
  rocksdb::Slice wrapper_nulls, bool force_nulls) {
  static_assert(Kind != velox::TypeKind::OPAQUE, "OPAQUE is not supported");
  // Flat vectors of complex types ARRAY, MAP and ROW / STRUCT are
  // represented using ArrayVector, MapVector and RowVector.
  // So here should be only primitive types
  static_assert(velox::TypeTraits<Kind>::isPrimitiveType);
  SDB_ASSERT(!ranges.empty(), "Empty ranges should be handled earlier");
  if constexpr (Kind == velox::TypeKind::UNKNOWN) {
    // UNKNOWN kind means all values are nulls.
    SDB_ASSERT(input.mayHaveNulls(),
               "Flat vector of UNKNOWN kind should have nulls");
    // to spare some space just write this vector as null constant
    WriteConstantVector<HaveNulls, velox::TypeKind::UNKNOWN>(input, ranges,
                                                             wrapper_nulls);
    return;
  };

  using T = typename velox::TypeTraits<Kind>::NativeType;
  auto* flat_vector = input.as<velox::FlatVector<T>>();
  SDB_ASSERT(flat_vector);
  static constexpr auto kNeedLength = !velox::TypeTraits<Kind>::isFixedWidth;
  static constexpr auto kFlatStorage =
    velox::TypeTraits<Kind>::isFixedWidth && Kind != velox::TypeKind::BOOLEAN;
  const bool whole_vector =
    ranges.size() == 1 && ranges.front().size == input.size();
  // header placeholder
  const auto header_slice_idx = _row_slices.size();
  _row_slices.emplace_back();

  const uint32_t total_rows_number = [&] {
    if (whole_vector) {
      SDB_ASSERT(input.size() > 0);
      return static_cast<uint32_t>(input.size());
    }
    const auto res = absl::c_accumulate(
      ranges, uint32_t{0}, [](auto v, const auto& r) { return v + r.size; });
    SDB_ASSERT(res > 0);
    return res;
  }();

  [[maybe_unused]] folly::Range<const velox::vector_size_t*> rows_range;
  [[maybe_unused]] IndiciesVector rows_holder(_memory_pool);

  [[maybe_unused]] ManagedVector<uint32_t> length{_memory_pool};
  [[maybe_unused]] uint32_t length_array_size = 0;
  const bool need_gather_nulls = HaveNulls && !whole_vector &&
                                 input.mayHaveNulls() &&
                                 (wrapper_nulls.empty() || !force_nulls);
  if ((Kind == velox::TypeKind::BOOLEAN && !whole_vector) ||
      need_gather_nulls) {
    rows_holder = GatherIndicies(ranges, total_rows_number);
    rows_range = folly::Range(rows_holder.data(), total_rows_number);
  }
  const auto slices_before_nulls = _row_slices.size();
  if constexpr (HaveNulls) {
    if (!wrapper_nulls.empty() && force_nulls) {
      // we have nulls from wrapping vector and we must store them
      _row_slices.push_back(wrapper_nulls);
    } else {
      // if we got here there are three possibilities:
      // 1. flat vector has nulls and we should store them. So we just do
      // that.
      // 2. we are inside complex structure that have nulls in wrapping vector
      // so there is at least one range denoting null. In that case we can
      // still put just default scalar value for wrapper nulls. So read vector
      // would be valid by itself and we could save space by not storing nulls
      // at all.
      SDB_ASSERT(input.mayHaveNulls() || !force_nulls);
      if (input.mayHaveNulls()) {
        if (whole_vector) {
          _row_slices.emplace_back(
            reinterpret_cast<const char*>(flat_vector->rawNulls()),
            velox::bits::nbytes(total_rows_number));
        } else {
          SDB_ASSERT(!rows_range.empty());
          const auto null_buffer_size = velox::bits::nbytes(total_rows_number);
          auto* char_nulls =
            _bytes_allocator.allocate(null_buffer_size)->begin();
          auto* nulls = reinterpret_cast<uint64_t*>(char_nulls);
          _row_slices.emplace_back(char_nulls, null_buffer_size);
          velox::simd::gatherBits(flat_vector->rawNulls(), rows_range, nulls);
        }
      }
    }
  }
  const auto have_nulls_slice = _row_slices.size() != slices_before_nulls;

  if constexpr (Kind == velox::TypeKind::BOOLEAN) {
#ifdef DATA_SINK_USE_MEMORY_SANITIZER
    // Memory sanitizer may complain about reading uninitialized memory as for
    // "Nulls" in velox flat vectos might contain uninitialized data in Values.
    // And RocksDB computes CRC over entire value triggering sanitizer, So we
    // avoid copying such values and put just zeroes in uninitialized parts.
    if constexpr (HaveNulls) {
      const auto bool_bitmap_size = velox::bits::nbytes(total_rows_number);
      auto* bitset_bytes = _bytes_allocator.allocate(bool_bitmap_size)->begin();
      memset(bitset_bytes, 0, bool_bitmap_size);
      velox::vector_size_t current_pos = 0;
      for (const auto& range : ranges) {
        if (range.begin < 0) {
          ++current_pos;
          continue;
        }
        for (velox::vector_size_t i = range.begin; i < range.begin + range.size;
             ++i) {
          if (!flat_vector->isNullAt(i) && flat_vector->valueAtFast(i)) {
            velox::bits::setBit(reinterpret_cast<uint64_t*>(bitset_bytes),
                                current_pos);
          }
          ++current_pos;
        }
      }
      _row_slices.emplace_back(reinterpret_cast<const char*>(bitset_bytes),
                               bool_bitmap_size);
    } else {
#else
    {
#endif
      if (whole_vector) {
        SDB_ASSERT(ranges.front().begin == 0);
        _row_slices.emplace_back(reinterpret_cast<const char*>(
                                   flat_vector->template rawValues<uint8_t>()),
                                 velox::bits::nbytes(total_rows_number));
      } else {
        const auto bool_bitmap_size = velox::bits::nbytes(total_rows_number);
        auto* bitset_bytes =
          _bytes_allocator.allocate(bool_bitmap_size)->begin();
        velox::simd::gatherBits(flat_vector->template rawValues<uint64_t>(),
                                rows_range,
                                reinterpret_cast<uint64_t*>(bitset_bytes));
        _row_slices.emplace_back(reinterpret_cast<const char*>(bitset_bytes),
                                 bool_bitmap_size);
      }
    }
  }

  if constexpr (kNeedLength) {
    length.reserve(total_rows_number);
    // placeholder slice for length array
    _row_slices.emplace_back();
  }

  if constexpr (kFlatStorage || kNeedLength) {
    for (const auto& range : ranges) {
      if constexpr (kNeedLength) {
        if constexpr (HaveNulls) {
          if (range.begin < 0) {
            // NULL from wrapping vector
            length.emplace_back(0);
            length_array_size += irs::bytes_io<uint32_t>::vsize(0);
            continue;
          }
        }
        for (velox::vector_size_t i = range.begin; i < range.begin + range.size;
             ++i) {
          // we need to store pointer to data. And data could be inlined
          // so we are not using valueAtFast as it will get us a temporary
          // StringView.
          const auto& str = flat_vector->rawValues()[i];
          length.push_back(str.size());
          length_array_size += irs::bytes_io<uint32_t>::vsize(str.size());
          _row_slices.emplace_back(str.data(), str.size());
        }
      } else {
        if constexpr (HaveNulls) {
          if (range.begin < 0) {
            // NULL from wrapping vector but for flat storage we still must
            // put something in value array
            static const T kNullPlaceholder{};
            _row_slices.emplace_back(
              reinterpret_cast<const char*>(&kNullPlaceholder), sizeof(T));
            continue;
          }
        }

#ifdef DATA_SINK_USE_MEMORY_SANITIZER
        // Memory sanitizer may complain about reading uninitialized memory as
        // for "Nulls" in velox flat vectors might contain uninitialized data in
        // Values. And RocksDB computes CRC over entire value triggering
        // sanitizer, So we avoid copying such values and put just zeroes in
        // uninitialized parts.
        if constexpr (HaveNulls) {
          auto* temp_bytes =
            _bytes_allocator.allocate(range.size * sizeof(T))->begin();
          memset(temp_bytes, 0, range.size * sizeof(T));
          auto* temp_values = reinterpret_cast<T*>(temp_bytes);
          _row_slices.emplace_back(temp_bytes, range.size * sizeof(T));
          for (velox::vector_size_t i = range.begin;
               i < range.begin + range.size; ++i) {
            if (!flat_vector->isNullAt(i)) {
              *temp_values++ = flat_vector->rawValues()[i];
            } else {
              // for flat storage we still must put something in value array
              *temp_values++ = T{};
            }
          }
          continue;
        }
#endif
        _row_slices.emplace_back(
          reinterpret_cast<const char*>(flat_vector->rawValues() + range.begin),
          range.size * sizeof(T));
      }
    }
  }

  auto header_size =
    irs::bytes_io<uint32_t>::vsize(total_rows_number) + sizeof(ValueFlags);

  if constexpr (kNeedLength) {
    SDB_ASSERT(!length.empty());
    header_size += irs::bytes_io<uint32_t>::vsize(length_array_size);
    auto* length_array = _bytes_allocator.allocate(length_array_size)->begin();
    _row_slices[header_slice_idx + (have_nulls_slice ? 2 : 1)] =
      rocksdb::Slice(length_array, length_array_size);
    for (auto l : length) {
      irs::WriteVarint(l, length_array);
      SDB_ASSERT(
        _row_slices[header_slice_idx + (have_nulls_slice ? 2 : 1)].data() +
          length_array_size >=
        length_array);
    }
  }
  auto* header = _bytes_allocator.allocate(header_size)->begin();
  _row_slices[header_slice_idx] = rocksdb::Slice(header, header_size);
  irs::WriteVarint(total_rows_number, header);
  auto flags = have_nulls_slice ? ValueFlags::HaveNulls : ValueFlags::None;
  if constexpr (kNeedLength) {
    flags |= ValueFlags::HaveLength;
  }
  *(header++) = std::bit_cast<char>(flags);
  if constexpr (kNeedLength) {
    irs::WriteVarint(length_array_size, header);
  }
}

template<bool HaveNulls, velox::TypeKind Kind>
void RocksDBDataSink::WriteBiasedVector(
  const velox::BaseVector& input,
  const folly::Range<const velox::IndexRange*>& ranges,
  rocksdb::Slice wrapper_nulls, bool force_nulls) {
  SDB_ASSERT(!ranges.empty(), "Empty ranges should be handled earlier");
  using T = typename velox::TypeTraits<Kind>::NativeType;

  if constexpr (velox::admitsBias<T>()) {
    using T = typename velox::TypeTraits<Kind>::NativeType;
    auto* biased_vector = input.as<velox::BiasVector<T>>();
    SDB_ASSERT(biased_vector);
    const auto whole_vector =
      ranges.size() == 1 && ranges.front().size == input.size();
    // header placeholder
    const auto header_slice_idx = _row_slices.size();
    _row_slices.emplace_back();
    const uint32_t total_rows_number = [&] {
      if (whole_vector) {
        SDB_ASSERT(input.size() > 0);
        return static_cast<uint32_t>(input.size());
      }
      const auto res = absl::c_accumulate(
        ranges, uint32_t{0}, [](auto v, const auto& r) { return v + r.size; });
      SDB_ASSERT(res > 0);
      return res;
    }();
    const auto slices_before_nulls = _row_slices.size();
    if constexpr (HaveNulls) {
      if (!wrapper_nulls.empty() && force_nulls) {
        // we have nulls from wrapping vector and we must store them
        _row_slices.push_back(wrapper_nulls);
      } else {
        SDB_ASSERT(input.mayHaveNulls() || !force_nulls);
        if (input.mayHaveNulls()) {
          const auto null_buffer_size = velox::bits::nbytes(total_rows_number);
          if (whole_vector) {
            _row_slices.emplace_back(
              reinterpret_cast<const char*>(input.rawNulls()),
              null_buffer_size);
          } else {
            const auto rows_holder = GatherIndicies(ranges, total_rows_number);
            auto* char_nulls =
              _bytes_allocator.allocate(null_buffer_size)->begin();
            auto* nulls = reinterpret_cast<uint64_t*>(char_nulls);
            _row_slices.emplace_back(char_nulls, null_buffer_size);
            velox::simd::gatherBits(
              biased_vector->rawNulls(),
              folly::Range(rows_holder.data(), total_rows_number), nulls);
          }
        }
      }
    }
    const auto have_nulls_slice = _row_slices.size() != slices_before_nulls;
    const auto flat_data_size =
      sizeof(T) * static_cast<size_t>(total_rows_number);
    auto* raw_data = _bytes_allocator.allocate(flat_data_size)->begin();
    _row_slices.emplace_back(raw_data, flat_data_size);
    auto* flat_values = reinterpret_cast<T*>(raw_data);

    for (const auto& range : ranges) {
      if constexpr (HaveNulls) {
        if (range.begin < 0) {
#ifdef DATA_SINK_USE_MEMORY_SANITIZER
          *flat_values = 0;
#endif
          ++flat_values;
          continue;
        }
      }
      for (velox::vector_size_t i = 0; i < range.size; ++i) {
        if constexpr (HaveNulls) {
          if (input.isNullAt(range.begin + i)) {
            // for biased vector we still must put something in value array
#ifdef DATA_SINK_USE_MEMORY_SANITIZER
            *flat_values = 0;
#endif
            ++flat_values;
            continue;
          }
        }
        *(flat_values++) = biased_vector->valueAt(range.begin + i);
      }
    }

    auto header_size =
      irs::bytes_io<uint32_t>::vsize(total_rows_number) + sizeof(ValueFlags);

    auto* header = _bytes_allocator.allocate(header_size)->begin();
    _row_slices[header_slice_idx] = rocksdb::Slice(header, header_size);
    irs::WriteVarint(total_rows_number, header);
    const auto flags =
      have_nulls_slice ? ValueFlags::HaveNulls : ValueFlags::None;
    *(header++) = std::bit_cast<char>(flags);
  } else {
    SDB_THROW(ERROR_NOT_IMPLEMENTED,
              "RocksDB Sink does not support BIASED encoding of vector kind ",
              velox::TypeKindName::toName(Kind));
  }
}

// Writes dictionary encoded vector. Vector is stored as unwrapped vector.
// Indexes are decoded. Nulls are combined.
template<bool HaveNulls>
void RocksDBDataSink::WriteDictionaryVector(
  const velox::VectorPtr& input,
  const folly::Range<const velox::IndexRange*>& ranges,
  rocksdb::Slice wrapper_nulls) {
  ManagedVector<velox::IndexRange> sub_ranges{_memory_pool};
  auto wrapped = velox::BaseVector::wrappedVectorShared(input);
  [[maybe_unused]] uint64_t* raw_nulls = nullptr;
  if constexpr (HaveNulls) {
    if (wrapper_nulls.empty()) {
      SDB_ASSERT(input->mayHaveNulls());
      const uint32_t total_rows_number = absl::c_accumulate(
        ranges, uint32_t{0}, [](auto v, const auto& r) { return v + r.size; });
      SDB_ASSERT(total_rows_number > 0);
      const auto null_buffer_size = velox::bits::nbytes(total_rows_number);
      auto* char_nulls = _bytes_allocator.allocate(null_buffer_size)->begin();
      memset(char_nulls, 0xFF, null_buffer_size);
      wrapper_nulls = rocksdb::Slice(char_nulls, null_buffer_size);
      raw_nulls = reinterpret_cast<uint64_t*>(char_nulls);
    } else if (input->mayHaveNulls()) {
      // we might add some nulls so we must copy wrapper nulls slice.
      auto* char_nulls =
        _bytes_allocator.allocate(wrapper_nulls.size())->begin();
      memcpy(char_nulls, wrapper_nulls.data(), wrapper_nulls.size());
      wrapper_nulls = rocksdb::Slice(char_nulls, wrapper_nulls.size());
      raw_nulls = reinterpret_cast<uint64_t*>(char_nulls);
    }
  }
  size_t row_id = 0;
  bool have_dictionary_nulls = false;
  for (const auto& range : ranges) {
    if constexpr (HaveNulls) {
      if (range.begin < 0) {
        SDB_ASSERT(!wrapper_nulls.empty());
        // null from wrapper - should be already marked in nulls bitset
        SDB_ASSERT(!velox::bits::isBitSet(raw_nulls, row_id));
        sub_ranges.emplace_back(range);
        ++row_id;
        continue;
      }
    }
    const auto end = range.begin + range.size;
    for (velox::vector_size_t i = range.begin; i < end; ++i, ++row_id) {
      if constexpr (HaveNulls) {
        if (input->isNullAt(i)) {
          SDB_ASSERT(raw_nulls);
          // all -1 start range should be treated as null by downstream
          sub_ranges.emplace_back(-1, 1);
          have_dictionary_nulls = true;
          velox::bits::clearBit(raw_nulls, row_id);
          continue;
        }
      }
      sub_ranges.emplace_back(input->wrappedIndex(i), 1);
    }
  }
  // we force writing nulls only if it was added by this dictionary layer
  // itself as we make a new vector so we must have valid nulls mask. If nulls
  // are only from wrapping vector - it is already accounted in wrapping
  // vector and downstream writer MAY write nulls bitset only if needed for
  // optimizations of storing e.g. avoid writing array if it is part of struct
  // that is null.
#ifdef SDB_DEV
  const auto sub_ranges_size =
    absl::c_accumulate(sub_ranges, velox::vector_size_t{0},
                       [](auto v, const auto& r) { return v + r.size; });
  const auto ranges_size =
    absl::c_accumulate(ranges, velox::vector_size_t{0},
                       [](auto v, const auto& r) { return v + r.size; });
  SDB_ASSERT(sub_ranges_size == ranges_size);
#endif
  WriteVector(wrapped, sub_ranges, wrapper_nulls, have_dictionary_nulls);
}

// Generic method of writing single value from vector.
// Technically we can replace this method with WriteVector and pass a range
// containing single element designated by idx but we do not want to write
// overhead related to storing vector header/nulls bitmap etc. if we are sure
// we need only one value.
void RocksDBDataSink::WriteValue(const velox::VectorPtr& input,
                                 velox::vector_size_t idx) {
  SDB_ASSERT(idx < input->size());
  if (input->isNullAt(idx)) {
    // TODO(Dronplane): we can avoid storing more than one empty slices in
    // _row_slices but we should adapt slices count asserts in code that
    // calculates sizes of written vectors/values
    _row_slices.emplace_back();
    return;
  }

  switch (input->encoding()) {
    case velox::VectorEncoding::Simple::FLAT:
      VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(WriteFlatValueWrapper,
                                         input->typeKind(), *input, idx);
      break;
    case velox::VectorEncoding::Simple::SEQUENCE:
    case velox::VectorEncoding::Simple::DICTIONARY: {
      auto wrapped = velox::BaseVector::wrappedVectorShared(input);
      WriteValue(wrapped, input->wrappedIndex(idx));
      break;
    }
    case velox::VectorEncoding::Simple::ARRAY:
      WriteArrayValue(*input, idx);
      break;
    case velox::VectorEncoding::Simple::ROW:
      WriteRowValue(*input, idx);
      break;
    case velox::VectorEncoding::Simple::MAP:
      WriteMapValue(*input, idx);
      break;
    case velox::VectorEncoding::Simple::FLAT_MAP:
      WriteFlatMapValue(*input, idx);
      break;
    case velox::VectorEncoding::Simple::LAZY:
      WriteValue(velox::BaseVector::loadedVectorShared(input), idx);
      break;
    case velox::VectorEncoding::Simple::CONSTANT:
      VELOX_DYNAMIC_TYPE_DISPATCH(WriteConstantValue, input->typeKind(),
                                  *input);
      break;
    case velox::VectorEncoding::Simple::BIASED:
      VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(WriteBiasedValue, input->typeKind(),
                                         *input, idx);
      break;
    default:
      SDB_THROW(ERROR_NOT_IMPLEMENTED,
                "RocksDB Sink does not support value vector encoding ",
                mapSimpleToName(input->encoding()));
  }
}

template<velox::TypeKind Kind>
void RocksDBDataSink::WriteBiasedValue(const velox::BaseVector& input,
                                       velox::vector_size_t idx) {
  SDB_ASSERT(idx < input.size());
  SDB_ASSERT(!input.isNullAt(idx));
  SDB_ASSERT(input.encoding() == velox::VectorEncoding::Simple::BIASED);
  using T = typename velox::TypeTraits<Kind>::NativeType;

  if constexpr (velox::admitsBias<T>()) {
    const auto* biased_vector = input.as<velox::BiasVector<T>>();
    SDB_ASSERT(biased_vector);
    // biased vector always returns temporary value so we must explicitly store
    auto* value =
      reinterpret_cast<T*>(_bytes_allocator.allocate(sizeof(T))->begin());
    *value = biased_vector->valueAt(idx);
    WritePrimitive(*value);
  } else {
    SDB_THROW(ERROR_NOT_IMPLEMENTED,
              "RocksDB Sink does not support BIASED encoding of value kind ",
              velox::TypeKindName::toName(Kind));
  }
}

template<velox::TypeKind Kind>
void RocksDBDataSink::WriteFlatValueWrapper(const velox::BaseVector& input,
                                            velox::vector_size_t idx) {
  using T = typename velox::TypeTraits<Kind>::NativeType;
  auto* flat_vector = input.asFlatVector<T>();
  SDB_ASSERT(flat_vector);
  WriteFlatValue(*flat_vector, idx);
}

template<typename T>
void RocksDBDataSink::WriteFlatValue(const velox::FlatVector<T>& input,
                                     velox::vector_size_t idx) {
  SDB_ASSERT(idx < input.size());
  SDB_ASSERT(!input.isNullAt(idx));
  if constexpr (std::is_same_v<T, bool>) {
    WritePrimitive(input.valueAtFast(idx));
  } else {
    auto raw = input.rawValues();
    WritePrimitive(raw[idx]);
  }
}

// Writes a single ROW value.
// Format: [length data size] [length data] [values data]
// length data is vencoded uint32_t for all values except last one.
// values data is raw data of struct fields in order determined by childs order
// in type. Particular values format is determined by value kind. Null value has
// 0 length.
void RocksDBDataSink::WriteRowValue(const velox::BaseVector& input,
                                    velox::vector_size_t idx) {
  SDB_ASSERT(idx < input.size());
  SDB_ASSERT(!input.isNullAt(idx));
  const auto* row_vec = input.as<velox::RowVector>();
  SDB_ASSERT(row_vec);
  const auto type = row_vec->rowType();
  SDB_ASSERT(type);
  const auto num_childs = row_vec->childrenSize();
  ManagedVector<uint32_t> length{_memory_pool};
  length.reserve(num_childs);
  uint32_t length_array_size = 0;
  const size_t header_idx = _row_slices.size();
  _row_slices.emplace_back();  // length array placeholder
  auto slices_before = _row_slices.size();
  auto calculate_size = [&] {
    const auto size = std::accumulate(
      _row_slices.begin() + slices_before, _row_slices.end(), uint32_t{},
      [&](uint32_t size, const auto& slice) { return size + slice.size(); });
    slices_before = _row_slices.size();
    return size;
  };
  for (size_t child_idx = 0; child_idx < num_childs; ++child_idx) {
    WriteValue(row_vec->childAt(child_idx), idx);
    SDB_ASSERT(slices_before < _row_slices.size());
    // do not write last length. It can be calculated from overal size and
    // all other values.
    if (child_idx != num_childs - 1) {
      uint32_t len = calculate_size();
      length_array_size += irs::bytes_io<uint32_t>::vsize(len);
      length.push_back(len);
    }
  }
  SDB_ASSERT(!_row_slices.empty());
  const auto header_size =
    length_array_size + irs::bytes_io<uint32_t>::vsize(length_array_size);
  auto* header = _bytes_allocator.allocate(header_size)->begin();
  _row_slices[header_idx] = rocksdb::Slice(header, header_size);
  irs::WriteVarint(length_array_size, header);
  for (const auto len : length) {
    irs::WriteVarint(len, header);
  }
}

// Each map value is written as header and two vectors - keys and values.
// Header contains variable encoded size of keys vector so reader could split
// the value and read each vector in parallel. And then combine original map.
// key/value vectors might have its own null mask.
// This is stored as part of the corresponding vector.
void RocksDBDataSink::WriteMapValue(const velox::BaseVector& input,
                                    velox::vector_size_t idx) {
  SDB_ASSERT(idx < input.size());
  SDB_ASSERT(!input.isNullAt(idx));
  SDB_ASSERT(input.encoding() == velox::VectorEncoding::Simple::MAP);
  const auto* map_vector = input.as<velox::MapVector>();
  SDB_ASSERT(map_vector);
  const auto* raw_sizes = map_vector->rawSizes();
  SDB_ASSERT(raw_sizes);
  const auto* raw_offsets = map_vector->rawOffsets();
  SDB_ASSERT(raw_offsets);
  if (!raw_sizes[idx]) {
    // map contains only empty entries
    SDB_ASSERT(irs::bytes_io<uint32_t>::vsize(1) == 1);
    _row_slices.push_back(kOneValueHeader);
    _row_slices.push_back(kZeroLengthVector);
    _row_slices.push_back(kZeroLengthVector);
    return;
  }
  velox::IndexRange range{.begin = raw_offsets[idx], .size = raw_sizes[idx]};
  folly::Range<const velox::IndexRange*> elements_range{&range, 1};
  const auto header_slice_idx = _row_slices.size();
  _row_slices.emplace_back();  // header placeholder
  const auto slices_before = _row_slices.size();
  WriteVector(map_vector->mapKeys(), elements_range, {}, false);
  SDB_ASSERT(slices_before < _row_slices.size());
  const auto keys_size = std::accumulate(
    _row_slices.begin() + slices_before, _row_slices.end(), uint32_t{},
    [&](uint32_t size, const auto& slice) { return size + slice.size(); });
  WriteVector(map_vector->mapValues(), elements_range, {}, false);
  const auto header_size =
    irs::bytes_io<uint32_t>::vsize(keys_size) + sizeof(ValueFlags);
  auto* header = _bytes_allocator.allocate(header_size)->begin();
  _row_slices[header_slice_idx] = rocksdb::Slice(header, header_size);
  *(header++) = std::bit_cast<char>(ValueFlags::None);
  irs::WriteVarint(keys_size, header);
}

// Flat Map is written as vector of keys (only present in current cell)
// and corresponding values for each key.
// Format is: [flags] [length array size] [length array] [keys vector] [values]
void RocksDBDataSink::WriteFlatMapValue(const velox::BaseVector& input,
                                        velox::vector_size_t idx) {
  SDB_ASSERT(idx < input.size());
  SDB_ASSERT(!input.isNullAt(idx));
  SDB_ASSERT(input.encoding() == velox::VectorEncoding::Simple::FLAT_MAP);
  const auto* map_vector = input.as<velox::FlatMapVector>();
  SDB_ASSERT(map_vector);
  auto keys = map_vector->distinctKeys();
  if (!keys || !keys->size()) {
    // map contains only empty entries
    SDB_ASSERT(irs::bytes_io<uint32_t>::vsize(1) == 1);
    _row_slices.push_back(kOneValueHeader);
    _row_slices.push_back(kZeroLengthVector);
    _row_slices.push_back(kZeroLengthVector);
    return;
  }

  const auto header_slice_idx = _row_slices.size();
  _row_slices.emplace_back();  // header placeholder

  ManagedVector<velox::IndexRange> keys_ranges{_memory_pool};
  ManagedVector<velox::VectorPtr> maps_ranges{_memory_pool};
  for (velox::vector_size_t i = 0; i < keys->size(); ++i) {
    if (map_vector->isInMap(i, idx)) {
      keys_ranges.emplace_back(i, 1);
      maps_ranges.emplace_back(map_vector->mapValuesAt(i));
    }
  }
  ManagedVector<uint32_t> length{_memory_pool};
  length.reserve(maps_ranges.size() + 1);  // + 1 for keys vector
  uint32_t length_array_size = 0;
  auto slices_before = _row_slices.size();
  auto calculate_size = [&] {
    const auto size = std::accumulate(
      _row_slices.begin() + slices_before, _row_slices.end(), uint32_t{},
      [&](uint32_t size, const auto& slice) { return size + slice.size(); });
    slices_before = _row_slices.size();
    return size;
  };
  WriteVector(keys, keys_ranges, {}, false);
  SDB_ASSERT(slices_before < _row_slices.size());
  length.push_back(calculate_size());
  length_array_size += irs::bytes_io<uint32_t>::vsize(length.back());
  for (const auto& vec : maps_ranges) {
    WriteValue(vec, idx);
    SDB_ASSERT(slices_before < _row_slices.size());
    length.push_back(calculate_size());
    length_array_size += irs::bytes_io<uint32_t>::vsize(length.back());
  }
  const auto header_size = sizeof(ValueFlags) +
                           irs::bytes_io<uint32_t>::vsize(length_array_size) +
                           length_array_size;
  auto* header = _bytes_allocator.allocate(header_size)->begin();
  _row_slices[header_slice_idx] = rocksdb::Slice(header, header_size);
  *(header++) = std::bit_cast<char>(ValueFlags::FlatMap);
  // TODO: write batch of varint's more optimally
  irs::WriteVarint(length_array_size, header);
  for (auto l : length) {
    irs::WriteVarint(l, header);
  }
}

// Array is just a vector. So write corresponding elements vector part as a
// value.
void RocksDBDataSink::WriteArrayValue(const velox::BaseVector& input,
                                      velox::vector_size_t idx) {
  SDB_ASSERT(idx < input.size());
  SDB_ASSERT(!input.isNullAt(idx));
  const auto* array_vector = input.as<velox::ArrayVector>();
  SDB_ASSERT(array_vector);
  const auto* raw_sizes = array_vector->rawSizes();
  const auto* raw_offsets = array_vector->rawOffsets();
  SDB_ASSERT(array_vector->elements());
  velox::IndexRange range{.begin = raw_offsets[idx], .size = raw_sizes[idx]};
  folly::Range<const velox::IndexRange*> elements_range{
    &range, static_cast<size_t>(raw_sizes[idx] ? 1 : 0)};
  WriteVector(array_vector->elements(), elements_range, {}, false);
}

void RocksDBDataSink::WriteRowSlices(std::string_view key) {
  rocksdb::Slice key_slice(key);
  rocksdb::Status status;
  SDB_ASSERT(!_row_slices.empty());
  if (_row_slices.size() == 1) {
    // Optimizing single slice case - rocksdb does not do additional copying
    // while gathering slice parts
    status = _transaction.Put(&_cf, key_slice, _row_slices.front());
  } else {
    // TODO(Dronplane): Currenly RocksDB does intermediate merging
    // all parts to a single string before actual inserting where it copies it
    // all again to the transaction buffer. Let's  propose a PR for them that
    // keeps SliceParts until they are copied to the transaction buffer.
    status = _transaction.Put(
      &_cf, rocksdb::SliceParts(&key_slice, 1),
      rocksdb::SliceParts(_row_slices.data(), _row_slices.size()));
  }
  if (!status.ok()) {
    SDB_THROW(rocksutils::ConvertStatus(status));
  }
}

template<velox::TypeKind Kind>
void RocksDBDataSink::WriteConstantValue(const velox::BaseVector& input) {
  using T = typename velox::KindToFlatVector<Kind>::WrapperType;
  auto const_vector = input.as<velox::ConstantVector<T>>();
  SDB_ASSERT(const_vector);
  SDB_ASSERT(!const_vector->isNullAt(0));
  if (const_vector->valueVector()) {
    WriteValue(const_vector->valueVector(), const_vector->wrappedIndex(0));
  } else {
    if constexpr (Kind == velox::TypeKind::OPAQUE) {
      SDB_THROW(ERROR_NOT_IMPLEMENTED,
                "RocksDB Sink does not support OPAQUE constant values");
    } else {
      // TODO(Dronplane): make assertion static. Currently it is not as
      // this code is compiled for ComplexTypes (ROW/MAP) - they always go
      // through valueVector() but we cant make it constexpr as valueVector
      // may be used for primitive types too.
      SDB_ASSERT(velox::TypeTraits<Kind>::isPrimitiveType);
      // no need to check for BOOLEAN as ConstVector can always return raw
      // pointer
      WritePrimitive(const_vector->rawValues()[0]);
    }
  }
}

// Writes trivial values and strings. Passed object is assumed as non
// temporary in context of serialization so it is safe to save the pointer to
// the value. PODs are copied verbatim as rocksdb value (in little endian byte
// order for integers). Booleans are stored as single byte 0x0 for false and
// 0x01 for true. Strings encoded as: Single byte 0x00 for empty string. If
// non-empty strings starts from 0x00 byte - additional 0x00 byte is added to
// the beginning. So reader should always skip first 0x00 byte if any. That will
// make empty string distinguishable from NULL.
template<typename T>
void RocksDBDataSink::WritePrimitive(const T& value) {
  static_assert(
    !std::is_same_v<T, void>,
    "Velox complex types that has void as NativeType should not get here");
  if constexpr (std::is_same_v<T, velox::StringView>) {
    if (value.empty()) {
      _row_slices.emplace_back(kStringPrefix);
      return;
    }
    static_assert(kStringPrefix.size() == 1,
                  "Prefix should be 1 char otherwise line below will not work");
    if (value.data()[0] == kStringPrefix.front()) {
      _row_slices.emplace_back(kStringPrefix);
    }
    // it is implied that buffer is persistent
    _row_slices.emplace_back(value.data(), value.size());
  } else if constexpr (std::is_same_v<T, bool>) {
    _row_slices.emplace_back(value ? kTrueValue : kFalseValue);
  } else {
    static_assert(std::is_trivially_copyable_v<T>,
                  "Only trivial values can be stored verbatim");
    if constexpr (basics::IsLittleEndian()) {
      _row_slices.emplace_back(reinterpret_cast<const char*>(&value),
                               sizeof(value));
    } else {
      static_assert(
        false,
        "Big endian machine should have byte swapping/storing code here");
    }
  }
}

const std::string& RocksDBDataSink::SetupRowKey(
  velox::vector_size_t idx,
  std::span<const velox::vector_size_t> original_idx) {
  SDB_ASSERT(original_idx.empty() ||
             original_idx.size() > static_cast<size_t>(idx));
  const auto row_id = original_idx.empty() ? idx : original_idx[idx];
  SDB_ASSERT(static_cast<size_t>(row_id) < _keys_buffers.size());

  auto& row_key = _keys_buffers[row_id];
  key_utils::SetupColumnForKey(row_key, _column_id);

  return row_key;
}

void RocksDBDataSink::ResetForNewRow() noexcept {
  _row_slices.clear();
  // memory reclaim is relatively expensive so do it only when we have
  // accumulated some noticable amount of memory.
  // TODO(Dronplane): make configurable option?
  constexpr uint64_t kMemoryReclaimThreshold = 10 * 1024 * 1024;  // 10 MB
  if (_bytes_allocator.currentBytes() > kMemoryReclaimThreshold) {
    _bytes_allocator.clear();
  }
}

void RocksDBDataSink::GatherNulls(
  const velox::BaseVector& input,
  const folly::Range<const velox::IndexRange*>& ranges,
  velox::vector_size_t total_rows_number, bool whole_vector,
  rocksdb::Slice wrapper_nulls, bool force_nulls) {
  if (!wrapper_nulls.empty() && (force_nulls || !input.mayHaveNulls())) {
    _row_slices.push_back(wrapper_nulls);
  } else {
    SDB_ASSERT(input.mayHaveNulls());
    uint64_t* nulls = nullptr;
    const auto null_buffer_size = velox::bits::nbytes(total_rows_number);
    if (whole_vector && wrapper_nulls.empty()) {
      SDB_ASSERT(ranges.front().begin == 0);
      _row_slices.emplace_back(reinterpret_cast<const char*>(input.rawNulls()),
                               null_buffer_size);
    } else {
      auto* char_nulls = _bytes_allocator.allocate(null_buffer_size)->begin();
      nulls = reinterpret_cast<uint64_t*>(char_nulls);
      _row_slices.emplace_back(char_nulls, null_buffer_size);
      folly::Range<const velox::vector_size_t*> rows_range;
      velox::raw_vector<velox::vector_size_t> rows_holder(&_memory_pool);
      rows_holder.resize(total_rows_number);
      velox::vector_size_t* raw_buffer = rows_holder.data();
#ifdef DATA_SINK_USE_MEMORY_SANITIZER
      // rawvector allocates tail long enough to load last simd batch but
      // does not initialize it. So we must do it here to avoid sanitizer
      // warnings while loading last batch.
      std::fill(raw_buffer + total_rows_number,
                raw_buffer + rows_holder.capacity(), 0);
#endif
      auto current = raw_buffer;
      for (const auto& range : ranges) {
        if (range.begin < 0) {
          *current++ = 0;
          continue;
        }
        const auto end = current + range.size;
        std::iota(current, end, range.begin);
        current = end;
      }
      rows_range = folly::Range(raw_buffer, total_rows_number);
      velox::simd::gatherBits(input.rawNulls(), rows_range, nulls);
      // now apply mask from wrapper
      if (!wrapper_nulls.empty()) {
        const auto* wrapper_words =
          reinterpret_cast<const uint64_t*>(wrapper_nulls.data());
        SDB_ASSERT(wrapper_nulls.size() == null_buffer_size);
        constexpr auto kLoadStep = xsimd::batch<uint64_t>::size;
        const auto steps = null_buffer_size / sizeof(uint64_t) / kLoadStep;
        const auto num_words = steps * kLoadStep;
        for (size_t i = 0; i < num_words; i += kLoadStep) {
          auto current_batch = xsimd::load_unaligned(nulls + i);
          current_batch &= xsimd::load_unaligned(wrapper_words + i);
          current_batch.store_unaligned(nulls + i);
        }
        for (size_t i = num_words * sizeof(uint64_t); i < null_buffer_size;
             ++i) {
          char_nulls[i] &= wrapper_nulls[i];
        }
      }
    }
  }
}

RocksDBDataSink::IndiciesVector RocksDBDataSink::GatherIndicies(
  const folly::Range<const velox::IndexRange*>& ranges,
  velox::vector_size_t total_rows_number) {
  IndiciesVector indicies(_memory_pool);
  // That would be used in gatherBits etc. So we must round up to batch size
  // otherwise memory sanitizer will be triggered for uninitialized memory read
  // while loading last batch of "batch::size" indicies.
  // TODO (Dronplane): check SimdUtils - maybe it can handle uneven loads
  // internally?
  indicies.resize(velox::bits::roundUp(
    total_rows_number, xsimd::batch<velox::vector_size_t>::size));
  velox::vector_size_t* raw_buffer = indicies.data();
  auto current = raw_buffer;
  for (const auto& range : ranges) {
    if (range.begin < 0) {
      *current++ = 0;
      continue;
    }
    const auto end = current + range.size;
    std::iota(current, end, range.begin);
    current = end;
  }
  SDB_ASSERT(current == raw_buffer + total_rows_number);
  SDB_ASSERT(current <= indicies.data() + indicies.size());
#ifdef DATA_SINK_USE_MEMORY_SANITIZER
  // initialize tail indicies to allow loading last batch with sanitizers
  // actually indicies after total_rows_number are not used but in fact loaded
  // into simd registers
  memset(current, 0,
         std::distance(current, indicies.data() + indicies.size()) *
           sizeof(velox::vector_size_t));
#endif
  return indicies;
}

bool RocksDBDataSink::finish() { return true; }

std::vector<std::string> RocksDBDataSink::close() { return {}; }

void RocksDBDataSink::abort() {
  // Transaction itself should be contolled outside and needed SavePoint should
  // be set.
  ResetForNewRow();
  // TODO(Dronplane) should we also shrink slice vector to save some memory?
}

velox::connector::DataSink::Stats RocksDBDataSink::stats() const {
  // TODO(Dronplane) implement
  return {};
}

RocksDBDeleteDataSink::RocksDBDeleteDataSink(
  rocksdb::Transaction& transaction, rocksdb::ColumnFamilyHandle& cf,
  velox::RowTypePtr row_type, ObjectId object_key,
  std::vector<catalog::Column::Id> column_oids)
  : _row_type{std::move(row_type)},
    _transaction{transaction},
    _cf{cf},
    _object_key{object_key},
    _column_ids{std::move(column_oids)} {
  SDB_ASSERT(_object_key.isSet(), "RocksDBDeleteDataSink: object key is empty");
  SDB_ASSERT(_column_ids.size() == _row_type->size(),
             "RocksDBDeleteDataSink: column oids size ", _column_ids.size(),
             " does not match row "
             "type size",
             _row_type->size());
  _key_childs.resize(_row_type->size());
  absl::c_iota(_key_childs, 0);
}

void RocksDBDeleteDataSink::appendData(velox::RowVectorPtr input) {
  // `input` is vector of rows that consists of **only primary** columns
  SDB_ASSERT(input->encoding() == velox::VectorEncoding::Simple::ROW);

  const std::string table_key = key_utils::PrepareTableKey(_object_key);
  const auto num_columns = _row_type->size();
  const auto num_rows = input->size();

  _key_childs.resize(input->childrenSize());
  std::string key_buffer;
  for (velox::vector_size_t row_idx = 0; row_idx < num_rows; ++row_idx) {
    key_utils::MakeColumnKey(
      input, _key_childs, row_idx, table_key,
      [&](std::string_view row_key) {
        auto status = _transaction.GetKeyLock(&_cf, row_key, false, true);
        if (!status.ok()) {
          SDB_THROW(rocksutils::ConvertStatus(status));
        }
      },
      key_buffer);

    for (velox::column_index_t col_idx = 0; col_idx < num_columns; ++col_idx) {
      key_utils::SetupColumnForKey(key_buffer, _column_ids[col_idx]);
      auto status = _transaction.Delete(&_cf, rocksdb::Slice(key_buffer));
      if (!status.ok()) {
        SDB_THROW(rocksutils::ConvertStatus(status));
      }
    }
  }
}

bool RocksDBDeleteDataSink::finish() { return true; }

std::vector<std::string> RocksDBDeleteDataSink::close() { return {}; }

void RocksDBDeleteDataSink::abort() {
  // TODO: implement
}

velox::connector::DataSink::Stats RocksDBDeleteDataSink::stats() const {
  // TODO: implement
  return {};
}

}  // namespace sdb::connector
