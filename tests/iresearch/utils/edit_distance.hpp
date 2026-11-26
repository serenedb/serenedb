////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2019 ArangoDB GmbH, Cologne, Germany
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

#include <algorithm>
#include <cstdint>
#include <iresearch/utils/assert.hpp>
#include <iresearch/utils/containers/small_vector.hpp>
#include <iresearch/utils/levenshtein_utils.hpp>
#include <iresearch/utils/string.hpp>
#include <iresearch/utils/utf8_utils.hpp>
#include <iterator>
#include <numeric>
#include <vector>

namespace irs {

template<typename T, size_t SubstCost = 1>
inline size_t EditDistance(const T* lhs, size_t lhs_size, const T* rhs,
                           size_t rhs_size) {
  SDB_ASSERT(lhs || !lhs_size);
  SDB_ASSERT(rhs || !rhs_size);

  if (lhs_size > rhs_size) {
    std::swap(lhs, rhs);
    std::swap(lhs_size, rhs_size);
  }

  std::vector<size_t> cost(2 * (lhs_size + 1));

  auto current = cost.begin();
  auto next = cost.begin() + cost.size() / 2;
  std::iota(current, next, 0);

  for (size_t j = 1; j <= rhs_size; ++j) {
    next[0] = j;
    for (size_t i = 1; i <= lhs_size; ++i) {
      next[i] = std::min({
        next[i - 1] + 1,  // deletion
        current[i] + 1,   // insertion
        current[i - 1] +
          (lhs[i - 1] == rhs[j - 1] ? 0 : SubstCost)  // substitution
      });
    }
    std::swap(next, current);
  }

  return current[lhs_size];
}

template<typename Char>
inline size_t EditDistance(basic_string_view<Char> lhs,
                           basic_string_view<Char> rhs) {
  return EditDistance(lhs.data(), lhs.size(), rhs.data(), rhs.size());
}

namespace edit_distance_detail {

inline constexpr uint32_t kInvalidState = 0;

template<typename Iterator>
uint64_t Chi(Iterator begin, Iterator end, uint32_t c) noexcept {
  uint64_t chi = 0;
  for (size_t i = 0; begin < end; ++begin, ++i) {
    chi |= uint64_t(c == *begin) << i;
  }
  return chi;
}

}  // namespace edit_distance_detail

inline size_t EditDistance(const ParametricDescription& description,
                           const byte_type* lhs, size_t lhs_size,
                           const byte_type* rhs, size_t rhs_size) {
  SDB_ASSERT(description);

  irs::containers::SmallVector<uint32_t, 16> lhs_chars;
  utf8_utils::ToUTF32<false>({lhs, lhs_size}, std::back_inserter(lhs_chars));

  size_t state = 1;
  size_t offset = 0;

  for (auto* rhs_end = rhs + rhs_size; rhs < rhs_end;) {
    const auto c = utf8_utils::ToChar32(rhs);

    const auto begin = lhs_chars.begin() + ptrdiff_t(offset);
    const auto end =
      lhs_chars.begin() + std::min(offset + description.chi_size(),
                                   static_cast<uint64_t>(lhs_chars.size()));
    const auto chi = edit_distance_detail::Chi(begin, end, c);
    const auto& transition = description.transition(state, chi);

    if (edit_distance_detail::kInvalidState == transition.first) {
      return description.max_distance() + 1;
    }

    state = transition.first;
    offset += transition.second;
  }

  return description.distance(state, lhs_chars.size() - offset);
}

inline size_t EditDistance(const ParametricDescription& description,
                           bytes_view lhs, bytes_view rhs) {
  return EditDistance(description, lhs.data(), lhs.size(), rhs.data(),
                      rhs.size());
}

inline bool EditDistance(size_t& distance,
                         const ParametricDescription& description,
                         const byte_type* lhs, size_t lhs_size,
                         const byte_type* rhs, size_t rhs_size) {
  SDB_ASSERT(description);

  irs::containers::SmallVector<uint32_t, 16> lhs_chars;
  if (!utf8_utils::ToUTF32<true>({lhs, lhs_size},
                                 std::back_inserter(lhs_chars))) {
    return false;
  }

  size_t state = 1;
  size_t offset = 0;

  for (auto* rhs_end = rhs + rhs_size; rhs < rhs_end;) {
    const auto c = utf8_utils::ToChar32(rhs, rhs_end);

    if (c == utf8_utils::kInvalidChar32) {
      return false;
    }

    const auto begin = lhs_chars.begin() + static_cast<ptrdiff_t>(offset);
    const auto end =
      lhs_chars.begin() + std::min(offset + description.chi_size(),
                                   static_cast<uint64_t>(lhs_chars.size()));
    const auto chi = edit_distance_detail::Chi(begin, end, c);
    const auto& transition = description.transition(state, chi);

    if (edit_distance_detail::kInvalidState == transition.first) {
      distance = description.max_distance() + 1;
      return true;
    }

    state = transition.first;
    offset += transition.second;
  }

  distance = description.distance(state, lhs_chars.size() - offset);
  return true;
}

inline bool EditDistance(size_t& distance,
                         const ParametricDescription& description,
                         bytes_view lhs, bytes_view rhs) {
  return EditDistance(distance, description, lhs.data(), lhs.size(), rhs.data(),
                      rhs.size());
}

}  // namespace irs
