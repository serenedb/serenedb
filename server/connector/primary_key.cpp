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

#include "primary_key.hpp"

#include <basics/errors.h>
#include <basics/exceptions.h>
#include <velox/vector/SimpleVector.h>

#include "basics/endian.h"
#include "basics/string_utils.h"
#include "iresearch/utils/numeric_utils.hpp"

namespace sdb::connector::primary_key {
namespace {

constexpr std::string_view kStringEnd{"\0\0", 2};
constexpr std::string_view kNullEsc{"\0\1", 2};
constexpr std::string_view kTrueValue{"\1", 1};
constexpr std::string_view kFalseValue{"\0", 1};
// We stick to +0 for all float zeroes
constexpr std::string_view kFloatNull{"\x80\0\0\0\0\0\0\0", 8};
// We use quiet NaN with zero payload as normalization of all NaNs
constexpr std::string_view kFloatPosNaN{"\xFF\xC0\x00\x00", 4};
constexpr std::string_view kDblPosNaN{"\xFF\xF8\x00\x00\x00\x00\x00\x00", 8};

// clang-format off
// Binary sortable row key format:
//  + Integer columns are added in BigEndian with width of TypeKind.
//  + Floating columns are encoded as integer columns and stored as BigEndian.
//    Quiet/signaling NaNs are normalized to quiet form.
//    -0 is normalized to +0.
//    We consider NaN greater than all non-NaN values and treat all NaNs equal same as Postgres.
//    Negative NaN is normalized to Positive.
//  + Boolean columns are added as single byte 0x1 for true and 0x0 for false.
//  + String columns are postfixed with double zero bytes to serve as a separator.
//    Zero bytes in the string are encoded as {0x00, 0x01}.
//    Longer string is considered greater.
// TODO(Dronplane) support complex types
// TODO(Dronplane) make code vector specific, e.g. FlatVector will work faster
// clang-format on
template<velox::TypeKind Kind>
void AppendKeyValue(std::string& key, const velox::BaseVector& column,
                    velox::vector_size_t idx) {
  using T = velox::TypeTraits<Kind>::NativeType;
  if (column.isNullAt(idx)) {
    // Postgresql requires PK columns to be non NULL so do we
    SDB_THROW(ERROR_BAD_PARAMETER,
              "RocksDB Sink does not support NULL keys. Null found at row ",
              idx);
  }
  const auto base_size = key.size();
  if constexpr (std::is_same_v<T, bool>) {
    key.append(column.as<velox::SimpleVector<T>>()->valueAt(idx) ? kTrueValue
                                                                 : kFalseValue);
  } else if constexpr (std::is_integral_v<T>) {
    basics::StrAppend(key, sizeof(T));
    auto value = column.as<velox::SimpleVector<T>>()->valueAt(idx);
    absl::big_endian::Store(key.data() + base_size, value);
    key[base_size] = static_cast<uint8_t>(key[base_size]) ^ 0x80;
  } else if constexpr (std::is_floating_point_v<T>) {
    basics::StrAppend(key, sizeof(T));
    auto value = column.as<velox::SimpleVector<T>>()->valueAt(idx);
    const auto is_zero = value == 0;
    if (!is_zero && !std::isnan(value)) {
      if constexpr (std::is_same_v<T, float>) {
        absl::big_endian::Store32(key.data() + base_size,
                                  irs::numeric_utils::Ftoi32(value));
      } else {
        static_assert(std::is_same_v<T, double>,
                      "Only float and double are supported");
        absl::big_endian::Store64(key.data() + base_size,
                                  irs::numeric_utils::Dtoi64(value));
      }
      key[base_size] = static_cast<uint8_t>(key[base_size]) ^ 0x80;
      return;
    }
    // we want to normalize +0 and -0 and NaNs
    if (is_zero) {
      static_assert(kFloatNull.size() >= sizeof(T),
                    "Zero values constant is not long enought");
      memcpy(key.data() + base_size, kFloatNull.data(), sizeof(T));
      return;
    }
    SDB_ASSERT(std::isnan(value));
    if constexpr (std::is_same_v<T, float>) {
      static_assert(sizeof(T) == kFloatPosNaN.size(), "Float size mismatch");
      memcpy(key.data() + base_size, kFloatPosNaN.data(), sizeof(T));
    } else {
      static_assert(sizeof(T) == kDblPosNaN.size(), "Double size mismatch");
      memcpy(key.data() + base_size, kDblPosNaN.data(), sizeof(T));
    }
  } else if constexpr (std::is_same_v<T, velox::StringView>) {
    auto value = column.as<velox::SimpleVector<T>>()->valueAt(idx);
    // TODO(Dronplane) make helper method in basics for string capacity growth
    // and use it here and in other places
    auto curr = value.begin();
    const auto end = value.end();
    // TODO(Dronplane) use simd like:
    //  64 => check null (result false) + memcpy bytes
    //  64 => check null (result true) => write bytes
    // and benchmark if it is faster. It should be cache line wise!
    while (curr != end) {
      if (*curr) {
        key.push_back(*curr);
      } else {
        key.append(kNullEsc);
      }
      ++curr;
    }
    key += kStringEnd;
  } else if constexpr (std::is_same_v<T, velox::Timestamp>) {
    basics::StrAppend(key, sizeof(int64_t) + sizeof(uint64_t));
    const auto value = column.as<velox::SimpleVector<T>>()->valueAt(idx);
    absl::big_endian::Store(key.data() + base_size, value.getSeconds());
    absl::big_endian::Store(key.data() + base_size + sizeof(int64_t),
                            value.getNanos());
    key[base_size] = static_cast<uint8_t>(key[base_size]) ^ 0x80;
  } else {
    SDB_THROW(ERROR_NOT_IMPLEMENTED,
              "RocksDB Sink does not support key value kind ",
              velox::TypeKindName::toName(Kind));
  }
}
}  // namespace

void Create(const velox::RowVector& data,
            std::span<const velox::column_index_t> key_childs, Keys& buffer) {
  buffer.reserve(data.size());
  // TODO(Dronplane) benchmark this more.
  // Currenly on quick benchmarks implemented approach looks better.
  // But there are more options:
  //  - calculate required size. Move string and allocate again on each row.
  //    works best (around 10%) if there is only numerics and we have 100%
  //    reserve precision. but degrades significantly if we have strings and
  //    our reserve estimates are wrong.
  //
  //  - calculate required size and still do copy&&clear. Works almost the same
  //    as current approach.
  //  - more....
  std::string key;
  for (velox::vector_size_t i = 0; i < data.size(); ++i) {
    Create(data, key_childs, i, key);
    buffer.push_back(key);
    key.clear();
  }
}

void Create(const velox::RowVector& data,
            std::span<const velox::column_index_t> key_childs,
            velox::vector_size_t idx, std::string& key) {
  for (size_t child_idx : key_childs) {
    const auto column = data.childAt(child_idx);
    VELOX_DYNAMIC_TYPE_DISPATCH(AppendKeyValue, column->typeKind(), key,
                                *column, idx);
  }
}

void Create(const velox::RowVector& data, velox::vector_size_t idx,
            std::string& key) {
  for (const auto& column : data.children()) {
    VELOX_DYNAMIC_TYPE_DISPATCH(AppendKeyValue, column->typeKind(), key,
                                *column, idx);
  }
}

}  // namespace sdb::connector::primary_key
