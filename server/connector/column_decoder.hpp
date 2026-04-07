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

#pragma once

#include <absl/functional/any_invocable.h>
#include <velox/common/memory/MemoryPool.h>
#include <velox/type/Type.h>
#include <velox/vector/BaseVector.h>
#include <velox/vector/ComplexVector.h>
#include <velox/vector/FlatVector.h>

#include <cstring>
#include <memory>
#include <string_view>

#include "basics/assert.h"
#include "basics/misc.hpp"
#include "connector/common.h"
#include "iresearch/utils/bytes_utils.hpp"

namespace sdb::connector {

namespace detail {

// Write one non-empty string_view into a typed FlatVector slot.
// Null-checking is the caller's responsibility.
template<typename T>
void SetScalarValue(std::string_view value, velox::vector_size_t idx,
                    velox::FlatVector<T>& vec) {
  if constexpr (std::is_same_v<T, velox::StringView>) {
    const size_t offset = value[0] == 0 ? 1 : 0;
    vec.set(idx, velox::StringView(value.data() + offset, value.size() - offset));
  } else if constexpr (std::is_same_v<T, bool>) {
    SDB_ASSERT(value.size() == kTrueValue.size(),
               "ColumnDecoder: unexpected value size for bool column");
    vec.set(idx, value == kTrueValue);
  } else {
    SDB_ASSERT(value.size() == sizeof(T),
               "ColumnDecoder: unexpected value size for scalar column");
    T tmp;
    memcpy(&tmp, value.data(), sizeof(T));
    vec.set(idx, tmp);
  }
}

}  // namespace detail

// ---------------------------------------------------------------------------
// IColumnDecoder — accumulate per-row slices, produce a VectorPtr.
// ---------------------------------------------------------------------------
class IColumnDecoder {
 public:
  using Writer = absl::AnyInvocable<void(velox::vector_size_t,
                                         std::string_view) const>;

  explicit IColumnDecoder(Writer writer) : _writer(std::move(writer)) {}
  virtual ~IColumnDecoder() = default;

  // Add one row. Empty value means NULL. Non-virtual: dispatches via _writer
  // set by the derived class constructor, avoiding a vtable call on the hot
  // per-cell path.
  void Add(velox::vector_size_t idx, std::string_view value) {
    _writer(idx, value);
  }

  // Finalise and return the vector. actual_rows <= max_rows given at
  // construction.
  virtual velox::VectorPtr Finish(velox::vector_size_t actual_rows) = 0;

 private:
  Writer _writer;
};

// ---------------------------------------------------------------------------
// ScalarColumnDecoder
// ---------------------------------------------------------------------------
template<velox::TypeKind Kind>
class ScalarColumnDecoder final : public IColumnDecoder {
  using T = typename velox::TypeTraits<Kind>::NativeType;

  std::shared_ptr<velox::FlatVector<T>> _vec;

 public:
  ScalarColumnDecoder(velox::vector_size_t max_rows,
                      velox::memory::MemoryPool& pool)
    : IColumnDecoder([this](velox::vector_size_t idx, std::string_view value) {
        if (value.empty()) {
          _vec->setNull(idx, true);
        } else {
          detail::SetScalarValue(value, idx, *_vec);
        }
      })
    , _vec(velox::BaseVector::create<velox::FlatVector<T>>(
        velox::Type::create<Kind>(), max_rows, &pool)) {}

  velox::VectorPtr Finish(velox::vector_size_t actual_rows) override {
    _vec->resize(actual_rows, false);
    return std::move(_vec);
  }
};

// ---------------------------------------------------------------------------
// ArrayColumnDecoder
// ---------------------------------------------------------------------------
template<velox::TypeKind ElemKind>
class ArrayColumnDecoder final : public IColumnDecoder {
  using ElemT = typename velox::TypeTraits<ElemKind>::NativeType;
  static_assert(!std::is_same_v<ElemT, void>,
                "Complex element types are not supported in array columns");

  velox::memory::MemoryPool& _pool;
  velox::TypePtr _array_type;

  velox::BufferPtr _offsets_buf;
  velox::BufferPtr _sizes_buf;
  velox::BufferPtr _null_buf;  // lazy — allocated on first NULL row
  velox::vector_size_t* _raw_offsets;
  velox::vector_size_t* _raw_sizes;

  std::shared_ptr<velox::FlatVector<ElemT>> _elements;
  velox::vector_size_t _elem_offset = 0;

 public:
  ArrayColumnDecoder(velox::TypePtr array_type, velox::vector_size_t max_rows,
                     velox::memory::MemoryPool& pool)
    : IColumnDecoder([this](velox::vector_size_t idx, std::string_view value) {
        if (value.empty()) {
          if (!_null_buf) {
            // Capacity matches max_rows passed at construction. The buffer is
            // zero-initialised by AlignedBuffer, so existing slots are non-null.
            _null_buf = velox::allocateNulls(
              _offsets_buf->capacity() / sizeof(velox::vector_size_t), &_pool);
          }
          velox::bits::setNull(_null_buf->asMutable<uint64_t>(), idx);
          return;
        }

        _raw_offsets[idx] = _elem_offset;
        const auto* ptr = reinterpret_cast<const uint8_t*>(value.data());
        const uint32_t elem_count = irs::vread<uint32_t>(ptr);
        _raw_sizes[idx] = static_cast<velox::vector_size_t>(elem_count);

        if (elem_count == 0) {
          return;
        }
        _elements->resize(_elem_offset + elem_count, true);

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
          const auto remaining_size = static_cast<size_t>(
            reinterpret_cast<const uint8_t*>(value.data()) + value.size() - ptr);
          if (remaining_size == 0) {
            for (uint32_t i = 0; i < elem_count; i++) {
              _elements->setNull(_elem_offset + i, true);
            }
          } else {
            const std::string_view constant_val{
              reinterpret_cast<const char*>(ptr), remaining_size};
            for (uint32_t i = 0; i < elem_count; i++) {
              detail::SetScalarValue(constant_val, _elem_offset + i, *_elements);
            }
          }
        } else {
          const uint8_t* elem_nulls = nullptr;
          if (have_nulls) {
            elem_nulls = ptr;
            ptr += velox::bits::nbytes(elem_count);
          }

          if constexpr (ElemKind == velox::TypeKind::BOOLEAN) {
            const auto bool_bytes = velox::bits::nbytes(elem_count);
            irs::ResolveBool(elem_nulls, [&]<bool HasNulls> {
              for (uint32_t i = 0; i < elem_count; i++) {
                if constexpr (HasNulls) {
                  if (!velox::bits::isBitSet(elem_nulls, i)) {
                    _elements->setNull(_elem_offset + i, true);
                    continue;
                  }
                }
                _elements->set(_elem_offset + i, velox::bits::isBitSet(ptr, i));
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
                    _elements->setNull(_elem_offset + i, true);
                    data_ptr += len;
                    continue;
                  }
                }
                _elements->set(_elem_offset + i,
                               velox::StringView(
                                 reinterpret_cast<const char*>(data_ptr), len));
                data_ptr += len;
              }
            });
          } else {
            // Fixed-width: packed array of elem_count * sizeof(ElemT) bytes.
            memcpy(_elements->mutableRawValues() + _elem_offset, ptr,
                   elem_count * sizeof(ElemT));
            ptr += elem_count * sizeof(ElemT);
            if (have_nulls) {
              for (uint32_t i = 0; i < elem_count; i++) {
                if (!velox::bits::isBitSet(elem_nulls, i)) {
                  _elements->setNull(_elem_offset + i, true);
                }
              }
            }
          }
        }

        _elem_offset += elem_count;
      })
    , _pool(pool)
    , _array_type(std::move(array_type))
    , _offsets_buf(velox::AlignedBuffer::allocate<velox::vector_size_t>(
        max_rows, &pool))
    , _sizes_buf(velox::AlignedBuffer::allocate<velox::vector_size_t>(
        max_rows, &pool))
    , _raw_offsets(_offsets_buf->asMutable<velox::vector_size_t>())
    , _raw_sizes(_sizes_buf->asMutable<velox::vector_size_t>())
    , _elements(velox::BaseVector::create<velox::FlatVector<ElemT>>(
        _array_type->asArray().elementType(), 0, &pool)) {}

  velox::VectorPtr Finish(velox::vector_size_t actual_rows) override {
    return std::make_shared<velox::ArrayVector>(
      &_pool, std::move(_array_type), std::move(_null_buf), actual_rows,
      std::move(_offsets_buf), std::move(_sizes_buf), std::move(_elements));
  }
};

// ---------------------------------------------------------------------------
// MakeColumnDecoder — factory dispatching on velox type
// ---------------------------------------------------------------------------
namespace detail {

template<velox::TypeKind Kind>
std::unique_ptr<IColumnDecoder> MakeScalarDecoder(
  velox::vector_size_t max_rows, velox::memory::MemoryPool& pool) {
  return std::make_unique<ScalarColumnDecoder<Kind>>(max_rows, pool);
}

template<velox::TypeKind ElemKind>
std::unique_ptr<IColumnDecoder> MakeArrayDecoder(
  velox::TypePtr array_type, velox::vector_size_t max_rows,
  velox::memory::MemoryPool& pool) {
  return std::make_unique<ArrayColumnDecoder<ElemKind>>(std::move(array_type),
                                                        max_rows, pool);
}

}  // namespace detail

inline std::unique_ptr<IColumnDecoder> MakeColumnDecoder(
  const velox::TypePtr& type, velox::vector_size_t max_rows,
  velox::memory::MemoryPool& pool) {
  if (type->isArray()) {
    const auto elem_kind = type->asArray().elementType()->kind();
    return VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(detail::MakeArrayDecoder,
                                              elem_kind, type, max_rows, pool);
  }
  return VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(detail::MakeScalarDecoder,
                                            type->kind(), max_rows, pool);
}

}  // namespace sdb::connector
