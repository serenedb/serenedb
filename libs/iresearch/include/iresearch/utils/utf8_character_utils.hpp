////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2021 ArangoDB GmbH, Cologne, Germany
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
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
///
/// @author Andrey Abramov
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <absl/algorithm/container.h>

#include <algorithm>

#include "basics/shared.hpp"
#include "iresearch/utils/utf8_character_tables.hpp"

namespace irs::utf8_utils {

// Returns true if a specified character 'c' is a whitespace according to
// unicode standard, for details see https://unicode.org/reports
constexpr bool CharIsWhiteSpace(uint32_t c) noexcept {
  // For small size linear search faster than binary
  // https://dirtyhandscoding.github.io/posts/performance-comparison-linear-search-vs-binary-search.html
  // count generates avx/sse
  return absl::c_count(kWhiteSpaceTable, c) != 0;
}

// Note: frozen binary search make recursion and not inline for large sizes

// TODO(mbkkt)
// Our Category data format is force us to make complex search
// Instead of first symbol in range, better to have last symbol in range
// Another helpful shit, is make this table S+-tree, it will be 10 times faster
// https://en.algorithmica.org/hpc/data-structures/s-tree/

template<typename V, typename Container>
constexpr uint16_t CharCategoryImpl(V v, const Container& c) noexcept {
  const auto it = absl::c_lower_bound(c, Category<V>{v, 0});
  if (it != c.begin() && it->codepoint != v) {
    return std::prev(it)->category;
  }
  return it->category;
}

constexpr uint16_t CharGeneralCategory(uint32_t c) noexcept {
  if (c <= std::numeric_limits<uint16_t>::max()) [[likely]] {
    return CharCategoryImpl(static_cast<uint16_t>(c), kSmallCategoryTable);
  }
  return CharCategoryImpl(c, kLargeCategoryTable);
}

constexpr char CharPrimaryCategory(uint32_t c) noexcept {
  return static_cast<char>(CharGeneralCategory(c) >> 8U);
}

}  // namespace irs::utf8_utils
