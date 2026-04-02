////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2026 SereneDB GmbH, Berlin, Germany
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

#include "common.h"

#include <absl/strings/numbers.h>
#include <velox/type/Type.h>
#include <velox/vector/FlatVector.h>

namespace sdb::connector {
namespace {

template<typename T>
std::string VeloxValueToString(T val) {
  if constexpr (type::kIsOneOf<T, velox::StringView, velox::Timestamp>) {
    return static_cast<std::string>(val);
  } else if constexpr (std::is_same_v<T, velox::int128_t>) {
    std::string result;
    basics::StrResize(result, absl::numbers_internal::kFastToBuffer128Size);
    char* end = absl::numbers_internal::FastIntToBuffer(val, result.data());
    result.resize(end - result.data());
    return result;
  } else {
    return absl::StrCat(val);
  }
}

template<velox::TypeKind Kind>
std::string GetPKVeloxValue(velox::VectorPtr vec, velox::vector_size_t idx) {
  SDB_ASSERT(!vec->isNullAt(idx));
  using T = typename velox::TypeTraits<Kind>::NativeType;

  const auto* simple_vec = vec->as<velox::SimpleVector<T>>();
  if (!simple_vec) {
    // TODO(mkornaukhov) support not only simple vectors
    return "<unsupported>";
  }
  return VeloxValueToString(simple_vec->valueAt(idx));
}

std::string FormatKeyValue(const velox::RowVector& input,
                           velox::column_index_t key_idx,
                           velox::vector_size_t row_idx) {
  const auto& vector = input.childAt(key_idx);
  const auto& type = input.type()->asRow().childAt(key_idx);

  if (type->isPrimitiveType()) {
    return VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
      GetPKVeloxValue, vector->typeKind(), vector, row_idx);
  }

  // TODO(mkornaukhov) find out do we need complex types here
  // and implement in case of we do
  return "<unsupported>";
}

}  // namespace

std::string BuildUniqueViolationDetail(
  std::span<const velox::column_index_t> key_indices,
  std::span<const ColumnInfo> columns, const velox::RowVectorPtr& input,
  velox::vector_size_t row_idx) {
  static constexpr size_t kDetailReservedSize = 128;
  std::string detail;
  detail.reserve(kDetailReservedSize);

  // Build "Key (col1, col2, ...)="
  detail += "Key (";
  for (size_t i = 0; i < key_indices.size(); ++i) {
    if (i > 0) {
      detail += ", ";
    }
    detail += columns[key_indices[i]].name;
  }
  detail += ")=(";

  // Build "(val1, val2, ...) already exists."
  if (input) {
    for (size_t i = 0; i < key_indices.size(); ++i) {
      if (i > 0) {
        detail += ", ";
      }
      detail += FormatKeyValue(*input, key_indices[i], row_idx);
    }
  }
  detail += ") already exists.";

  return detail;
}

std::string BuildUniqueViolationDetail(
  std::span<const velox::column_index_t> key_indices,
  const velox::RowVector& input, velox::vector_size_t row_idx) {
  static constexpr size_t kDetailReservedSize = 128;
  std::string detail;
  detail.reserve(kDetailReservedSize);

  const auto& row_type = input.type()->asRow();
  detail += "Key (";
  for (size_t i = 0; i < key_indices.size(); ++i) {
    if (i > 0) {
      detail += ", ";
    }
    detail += row_type.nameOf(key_indices[i]);
  }
  detail += ")=(";

  for (size_t i = 0; i < key_indices.size(); ++i) {
    if (i > 0) {
      detail += ", ";
    }
    detail += FormatKeyValue(input, key_indices[i], row_idx);
  }
  detail += ") already exists.";

  return detail;
}

}  // namespace sdb::connector
