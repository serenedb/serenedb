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
  JsonList = 7,
};

inline constexpr size_t kDictLayouts = 8;

enum class DictMember : uint8_t {
  Strings = 0,
  Numbers = 1,
  False = 2,
  True = 3,
  Nulls = 4,
  JsonNulls = 5,
  NullElements = 6,
};

inline constexpr size_t kDictMembers = 7;

constexpr DictMember BoolMember(bool value) noexcept {
  return value ? DictMember::True : DictMember::False;
}

constexpr bool IsJson(DictLayout layout) noexcept {
  return layout == DictLayout::Json || layout == DictLayout::JsonList;
}

constexpr bool IsList(DictLayout layout) noexcept {
  return layout == DictLayout::BoolList || layout == DictLayout::NumericList ||
         layout == DictLayout::StringList || layout == DictLayout::JsonList;
}

struct DictRef {
  DictLayout layout = DictLayout::String;
  DictMember member = DictMember::Strings;

  auto operator<=>(const DictRef&) const = default;
};

template<typename H>
H AbslHashValue(H h, DictRef ref) {
  return H::combine(std::move(h), ref.layout, ref.member);
}

template<DictLayout Layout>
struct LayoutRefs {
  static constexpr DictRef Of(DictMember member) noexcept {
    return {.layout = Layout, .member = member};
  }
};

struct BoolLayout : LayoutRefs<DictLayout::Bool> {
  static constexpr DictRef kFalse = Of(DictMember::False);
  static constexpr DictRef kTrue = Of(DictMember::True);
  static constexpr DictRef kNulls = Of(DictMember::Nulls);
};

struct NumericLayout : LayoutRefs<DictLayout::Numeric> {
  static constexpr DictRef kNumbers = Of(DictMember::Numbers);
  static constexpr DictRef kNulls = Of(DictMember::Nulls);
};

struct StringLayout : LayoutRefs<DictLayout::String> {
  static constexpr DictRef kStrings = Of(DictMember::Strings);
  static constexpr DictRef kNulls = Of(DictMember::Nulls);
};

struct BoolListLayout : LayoutRefs<DictLayout::BoolList> {
  static constexpr DictRef kFalse = Of(DictMember::False);
  static constexpr DictRef kTrue = Of(DictMember::True);
  static constexpr DictRef kNulls = Of(DictMember::Nulls);
  static constexpr DictRef kNullElements = Of(DictMember::NullElements);
};

struct NumericListLayout : LayoutRefs<DictLayout::NumericList> {
  static constexpr DictRef kNumbers = Of(DictMember::Numbers);
  static constexpr DictRef kNulls = Of(DictMember::Nulls);
  static constexpr DictRef kNullElements = Of(DictMember::NullElements);
};

struct StringListLayout : LayoutRefs<DictLayout::StringList> {
  static constexpr DictRef kStrings = Of(DictMember::Strings);
  static constexpr DictRef kNulls = Of(DictMember::Nulls);
  static constexpr DictRef kNullElements = Of(DictMember::NullElements);
};

struct JsonLayout : LayoutRefs<DictLayout::Json> {
  static constexpr DictRef kStrings = Of(DictMember::Strings);
  static constexpr DictRef kNumbers = Of(DictMember::Numbers);
  static constexpr DictRef kFalse = Of(DictMember::False);
  static constexpr DictRef kTrue = Of(DictMember::True);
  static constexpr DictRef kNulls = Of(DictMember::Nulls);
  static constexpr DictRef kJsonNulls = Of(DictMember::JsonNulls);
};

struct JsonListLayout : LayoutRefs<DictLayout::JsonList> {
  static constexpr DictRef kStrings = Of(DictMember::Strings);
  static constexpr DictRef kNumbers = Of(DictMember::Numbers);
  static constexpr DictRef kFalse = Of(DictMember::False);
  static constexpr DictRef kTrue = Of(DictMember::True);
  static constexpr DictRef kNulls = Of(DictMember::Nulls);
  static constexpr DictRef kJsonNulls = Of(DictMember::JsonNulls);
  static constexpr DictRef kNullElements = Of(DictMember::NullElements);
};

constexpr std::span<const DictMember> LayoutMembers(
  DictLayout layout) noexcept {
  using enum DictMember;
  switch (layout) {
    case DictLayout::Bool: {
      static constexpr DictMember kMembers[]{False, True, Nulls};
      return kMembers;
    }
    case DictLayout::Numeric: {
      static constexpr DictMember kMembers[]{Numbers, Nulls};
      return kMembers;
    }
    case DictLayout::String: {
      static constexpr DictMember kMembers[]{Strings, Nulls};
      return kMembers;
    }
    case DictLayout::BoolList: {
      static constexpr DictMember kMembers[]{False, True, Nulls, NullElements};
      return kMembers;
    }
    case DictLayout::NumericList: {
      static constexpr DictMember kMembers[]{Numbers, Nulls, NullElements};
      return kMembers;
    }
    case DictLayout::StringList: {
      static constexpr DictMember kMembers[]{Strings, Nulls, NullElements};
      return kMembers;
    }
    case DictLayout::Json: {
      static constexpr DictMember kMembers[]{Strings, Numbers, False,
                                             True,    Nulls,   JsonNulls};
      return kMembers;
    }
    case DictLayout::JsonList: {
      static constexpr DictMember kMembers[]{
        Strings, Numbers, False, True, Nulls, JsonNulls, NullElements};
      return kMembers;
    }
  }
  return {};
}

constexpr bool Contains(DictRef ref) noexcept {
  for (const auto member : LayoutMembers(ref.layout)) {
    if (member == ref.member) {
      return true;
    }
  }
  return false;
}

constexpr bool IsTermMember(DictMember member) noexcept {
  return member == DictMember::Strings || member == DictMember::Numbers;
}

static_assert(Contains(BoolLayout::kNulls) && Contains(NumericLayout::kNulls) &&
              Contains(StringLayout::kNulls) &&
              Contains(BoolListLayout::kNullElements) &&
              Contains(NumericListLayout::kNullElements) &&
              Contains(StringListLayout::kNullElements) &&
              Contains(JsonLayout::kJsonNulls) &&
              Contains(JsonListLayout::kNullElements) &&
              !Contains(NumericLayout::Of(DictMember::False)));

inline constexpr bytes_view kSlotTerm{};

struct FieldStats {
  // Total number of terms
  uint32_t len{};
};

struct FieldProperties {
  field_id norm{field_limits::invalid()};
  IndexFeatures index_features{IndexFeatures::None};
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
  DictRef dict;
};

static_assert(std::is_move_constructible_v<FieldMeta>);

}  // namespace irs
