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
#include <string_view>
#include <utility>

#include "iresearch/index/index_features.hpp"
#include "iresearch/store/data_input.hpp"
#include "iresearch/store/directory_attributes.hpp"
#include "iresearch/utils/attributes.hpp"
#include "iresearch/utils/string.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs {

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
inline constexpr size_t kMaxLayoutMembers = 6;

struct DictRef {
  DictLayout layout = DictLayout::String;
  uint8_t member = 0;

  auto operator<=>(const DictRef&) const = default;
};

template<typename H>
H AbslHashValue(H h, DictRef ref) {
  return H::combine(std::move(h), ref.layout, ref.member);
}

struct BoolLayout {
  static constexpr DictRef kFalse{.layout = DictLayout::Bool, .member = 0};
  static constexpr DictRef kTrue{.layout = DictLayout::Bool, .member = 1};
  static constexpr DictRef kNulls{.layout = DictLayout::Bool, .member = 2};
};

struct NumericLayout {
  static constexpr DictRef kNumbers{.layout = DictLayout::Numeric, .member = 0};
  static constexpr DictRef kNulls{.layout = DictLayout::Numeric, .member = 1};
};

struct StringLayout {
  static constexpr DictRef kStrings{.layout = DictLayout::String, .member = 0};
  static constexpr DictRef kNulls{.layout = DictLayout::String, .member = 1};
};

struct BoolListLayout {
  static constexpr DictRef kFalse{.layout = DictLayout::BoolList, .member = 0};
  static constexpr DictRef kTrue{.layout = DictLayout::BoolList, .member = 1};
  static constexpr DictRef kNulls{.layout = DictLayout::BoolList, .member = 2};
  static constexpr DictRef kNullElements{.layout = DictLayout::BoolList,
                                         .member = 3};
};

struct NumericListLayout {
  static constexpr DictRef kNumbers{.layout = DictLayout::NumericList,
                                    .member = 0};
  static constexpr DictRef kNulls{.layout = DictLayout::NumericList,
                                  .member = 1};
  static constexpr DictRef kNullElements{.layout = DictLayout::NumericList,
                                         .member = 2};
};

struct StringListLayout {
  static constexpr DictRef kStrings{.layout = DictLayout::StringList,
                                    .member = 0};
  static constexpr DictRef kNulls{.layout = DictLayout::StringList,
                                  .member = 1};
  static constexpr DictRef kNullElements{.layout = DictLayout::StringList,
                                         .member = 2};
};

struct JsonLayout {
  static constexpr DictRef kStrings{.layout = DictLayout::Json, .member = 0};
  static constexpr DictRef kFalse{.layout = DictLayout::Json, .member = 1};
  static constexpr DictRef kTrue{.layout = DictLayout::Json, .member = 2};
  static constexpr DictRef kNumbers{.layout = DictLayout::Json, .member = 3};
  static constexpr DictRef kNulls{.layout = DictLayout::Json, .member = 4};
  static constexpr DictRef kJsonNulls{.layout = DictLayout::Json, .member = 5};
};

constexpr std::span<const std::string_view> LayoutMembers(
  DictLayout layout) noexcept {
  switch (layout) {
    case DictLayout::Bool: {
      static constexpr std::string_view kTags[]{"false", "true", "sql_null"};
      return kTags;
    }
    case DictLayout::Numeric: {
      static constexpr std::string_view kTags[]{"numeric", "sql_null"};
      return kTags;
    }
    case DictLayout::String: {
      static constexpr std::string_view kTags[]{"string", "sql_null"};
      return kTags;
    }
    case DictLayout::BoolList: {
      static constexpr std::string_view kTags[]{"false", "true", "sql_null",
                                                "null_element"};
      return kTags;
    }
    case DictLayout::NumericList: {
      static constexpr std::string_view kTags[]{"numeric", "sql_null",
                                                "null_element"};
      return kTags;
    }
    case DictLayout::StringList: {
      static constexpr std::string_view kTags[]{"string", "sql_null",
                                                "null_element"};
      return kTags;
    }
    case DictLayout::Json: {
      static constexpr std::string_view kTags[]{
        "string", "false", "true", "numeric", "sql_null", "json_null"};
      return kTags;
    }
  }
  return {};
}

constexpr bool Contains(DictRef ref) noexcept {
  return ref.member < LayoutMembers(ref.layout).size();
}

static_assert(Contains(BoolLayout::kNulls) && Contains(NumericLayout::kNulls) &&
              Contains(StringLayout::kNulls) &&
              Contains(BoolListLayout::kNullElements) &&
              Contains(NumericListLayout::kNullElements) &&
              Contains(StringListLayout::kNullElements) &&
              Contains(JsonLayout::kJsonNulls));

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

  FieldMeta() = default;
  FieldMeta(FieldMeta&& rhs) noexcept = default;
  FieldMeta& operator=(FieldMeta&& rhs) noexcept = default;
  FieldMeta(const FieldMeta&) = default;
  FieldMeta& operator=(const FieldMeta&) = default;

  bool operator==(const FieldMeta& rhs) const noexcept {
    return index_features == rhs.index_features && id == rhs.id;
  }

  field_id id{field_limits::invalid()};
};

static_assert(std::is_move_constructible_v<FieldMeta>);

}  // namespace irs
