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

#include "iresearch/utils/shared.hpp"
#include "iresearch/utils/utf8_case_tables.hpp"
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

template<const auto& Table>
class SimpleCaseStages {
 public:
  static constexpr uint32_t kMaxCp = Table.back().cp;

  constexpr SimpleCaseStages() noexcept {
    size_t next = 1;
    size_t block = 0;
    uint32_t prev = ~uint32_t{0};
    for (const CaseMap& m : Table) {
      const uint32_t b = m.cp >> kBlockBits;
      if (b != prev) {
        block = next++;
        _stage1[b] = static_cast<uint8_t>(block);
        prev = b;
      }
      _stage2[block * kBlock + (m.cp & (kBlock - 1))] =
        static_cast<uint16_t>(m.to - m.cp);
    }
  }

  constexpr uint32_t Map(uint32_t c) const noexcept {
    if (c > kMaxCp) {
      return c;
    }
    const uint16_t delta =
      _stage2[size_t{_stage1[c >> kBlockBits]} * kBlock + (c & (kBlock - 1))];
    return (c & ~uint32_t{0xFFFF}) | ((c + delta) & 0xFFFF);
  }

 private:
  static constexpr uint32_t kBlockBits = 6;
  static constexpr uint32_t kBlock = 1U << kBlockBits;

  static constexpr bool Sorted() noexcept {
    for (size_t i = 1; i < Table.size(); ++i) {
      if (Table[i - 1].cp >= Table[i].cp) {
        return false;
      }
    }
    return true;
  }

  static constexpr bool SamePlane() noexcept {
    for (const CaseMap& m : Table) {
      if ((m.to >> 16) != (m.cp >> 16)) {
        return false;
      }
    }
    return true;
  }

  static constexpr size_t CountBlocks() noexcept {
    size_t blocks = 1;
    uint32_t prev = ~uint32_t{0};
    for (const CaseMap& m : Table) {
      const uint32_t b = m.cp >> kBlockBits;
      blocks += (b != prev);
      prev = b;
    }
    return blocks;
  }

  static constexpr size_t kBlocks = CountBlocks();
  static_assert(Sorted());
  static_assert(SamePlane());
  static_assert(kBlocks <= 256);

  std::array<uint8_t, (kMaxCp >> kBlockBits) + 1> _stage1{};
  std::array<uint16_t, kBlocks * kBlock> _stage2{};
};

ABSL_CACHELINE_ALIGNED inline constexpr SimpleCaseStages<kSimpleLowerTable>
  kSimpleLowerStages{};
ABSL_CACHELINE_ALIGNED inline constexpr SimpleCaseStages<kSimpleUpperTable>
  kSimpleUpperStages{};

constexpr uint32_t CharToLowerSimple(uint32_t c) noexcept {
  return kSimpleLowerStages.Map(c);
}

constexpr uint32_t CharToUpperSimple(uint32_t c) noexcept {
  return kSimpleUpperStages.Map(c);
}

}  // namespace irs::utf8_utils
