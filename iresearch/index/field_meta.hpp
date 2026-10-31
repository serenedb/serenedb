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
#include <utility>

#include "iresearch/index/index_features.hpp"
#include "iresearch/store/data_input.hpp"
#include "iresearch/store/directory_attributes.hpp"
#include "iresearch/utils/attributes.hpp"
#include "iresearch/utils/string.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs {

enum class FieldSlot : uint8_t {
  SqlNull = 0,
  JsonNull = 1,
  NullElement = 2,
  False = 3,
  True = 4,
  Numeric = 5,
  String = 6,
};

inline constexpr size_t kFieldSlots = 7;
inline constexpr size_t kPostingSlots = 5;
inline constexpr size_t kKeyedSlots = kFieldSlots - kPostingSlots;

constexpr bool IsKeyed(FieldSlot slot) noexcept {
  return slot >= FieldSlot::Numeric;
}

constexpr size_t KeyedIndex(FieldSlot slot) noexcept {
  return static_cast<size_t>(slot) - kPostingSlots;
}

constexpr FieldSlot BoolSlot(bool value) noexcept {
  return value ? FieldSlot::True : FieldSlot::False;
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

  FieldMeta(field_id id, FieldSlot slot, IndexFeatures index_features)
    : FieldProperties{.index_features = index_features}, id{id}, slot{slot} {}

  FieldMeta() = default;
  FieldMeta(FieldMeta&& rhs) noexcept = default;
  FieldMeta& operator=(FieldMeta&& rhs) noexcept = default;
  FieldMeta(const FieldMeta&) = default;
  FieldMeta& operator=(const FieldMeta&) = default;

  bool operator==(const FieldMeta& rhs) const noexcept {
    return index_features == rhs.index_features && id == rhs.id &&
           slot == rhs.slot;
  }

  field_id id{field_limits::invalid()};
  FieldSlot slot = FieldSlot::String;
};

static_assert(std::is_move_constructible_v<FieldMeta>);

}  // namespace irs
