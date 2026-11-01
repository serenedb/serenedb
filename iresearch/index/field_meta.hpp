////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2016 by EMC Corporation, All Rights Reserved
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
/// Copyright holder is EMC Corporation
///
/// @author Andrey Abramov
/// @author Vasiliy Nabatchikov
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <compare>
#include <set>
#include <span>
#include <utility>

#include "iresearch/index/index_features.hpp"
#include "iresearch/store/data_input.hpp"
#include "iresearch/store/directory_attributes.hpp"
#include "iresearch/utils/attributes.hpp"
#include "iresearch/utils/string.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs {

enum class ValueKind : uint8_t {
  String = 0,
  False = 1,
  True = 2,
  Numeric = 3,
  SqlNull = 4,
  JsonNull = 5,
  NullElement = 6,
};

inline constexpr size_t kValueKinds = 7;
inline constexpr size_t kKeyedKinds = 2;

constexpr bool IsKeyed(ValueKind kind) noexcept {
  return kind == ValueKind::String || kind == ValueKind::Numeric;
}

constexpr size_t KeyedIndex(ValueKind kind) noexcept {
  return kind == ValueKind::Numeric ? 0 : 1;
}

constexpr ValueKind BoolKind(bool value) noexcept {
  return value ? ValueKind::True : ValueKind::False;
}

enum class DictLayout : uint8_t {
  Bool = 0,
  Numeric = 1,
  String = 2,
  BoolList = 3,
  NumericList = 4,
  StringList = 5,
  Json = 6,
};

inline constexpr size_t kDictLayouts = 7;
inline constexpr size_t kMaxLayoutKinds = 6;
inline constexpr size_t kNoPosition = kMaxLayoutKinds;

constexpr std::span<const ValueKind> LayoutKinds(DictLayout layout) noexcept {
  using enum ValueKind;
  switch (layout) {
    case DictLayout::Bool: {
      static constexpr ValueKind kKinds[]{SqlNull, False, True};
      return kKinds;
    }
    case DictLayout::Numeric: {
      static constexpr ValueKind kKinds[]{SqlNull, Numeric};
      return kKinds;
    }
    case DictLayout::String: {
      static constexpr ValueKind kKinds[]{SqlNull, String};
      return kKinds;
    }
    case DictLayout::BoolList: {
      static constexpr ValueKind kKinds[]{SqlNull, NullElement, False, True};
      return kKinds;
    }
    case DictLayout::NumericList: {
      static constexpr ValueKind kKinds[]{SqlNull, NullElement, Numeric};
      return kKinds;
    }
    case DictLayout::StringList: {
      static constexpr ValueKind kKinds[]{SqlNull, NullElement, String};
      return kKinds;
    }
    case DictLayout::Json: {
      static constexpr ValueKind kKinds[]{SqlNull, JsonNull, False,
                                          True,    Numeric,  String};
      return kKinds;
    }
  }
  return {};
}

constexpr size_t PositionOf(DictLayout layout, ValueKind kind) noexcept {
  const auto kinds = LayoutKinds(layout);
  for (size_t position = 0; position != kinds.size(); ++position) {
    if (kinds[position] == kind) {
      return position;
    }
  }
  return kNoPosition;
}

inline constexpr bytes_view kSlotTerm{};

struct FieldStats {
  // Total number of terms
  uint32_t len{};
};

struct FieldProperties {
  field_id norm{field_limits::invalid()};
  IndexFeatures index_features{IndexFeatures::None};
  uint32_t key_width = 0;
};

struct FieldMeta : FieldProperties {
 public:
  static const FieldMeta kEmpty;

  FieldMeta(field_id id, IndexFeatures index_features)
    : FieldProperties{.index_features = index_features}, id{id} {}

  FieldMeta(field_id id, ValueKind kind, IndexFeatures index_features)
    : FieldProperties{.index_features = index_features}, id{id}, kind{kind} {}

  FieldMeta() = default;
  FieldMeta(FieldMeta&& rhs) noexcept = default;
  FieldMeta& operator=(FieldMeta&& rhs) noexcept = default;
  FieldMeta(const FieldMeta&) = default;
  FieldMeta& operator=(const FieldMeta&) = default;

  bool operator==(const FieldMeta& rhs) const noexcept {
    return index_features == rhs.index_features && id == rhs.id &&
           kind == rhs.kind;
  }

  field_id id{field_limits::invalid()};
  ValueKind kind = ValueKind::String;
};

static_assert(std::is_move_constructible_v<FieldMeta>);

}  // namespace irs
