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

#include <set>

#include "iresearch/index/index_features.hpp"
#include "iresearch/store/data_input.hpp"
#include "iresearch/store/directory_attributes.hpp"
#include "iresearch/utils/attributes.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs {

struct FieldStats {
  // Total number of terms
  uint32_t len{};
  // Number of overlapped terms
  uint32_t num_overlap{};
  // Maximum number of terms in a field
  uint32_t max_term_freq{};
  // Number of unique terms
  uint32_t num_unique{};
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
};

static_assert(std::is_move_constructible_v<FieldMeta>);

}  // namespace irs
