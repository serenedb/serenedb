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
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <bit>
#include <cmath>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <numeric>

#include "iresearch/utils/assert.hpp"

namespace irs::math {

template<std::floating_point T>
inline bool ApproxEquals(T lhs, T rhs,
                         T eps = std::numeric_limits<T>::epsilon()) noexcept {
  return std::fabs(rhs - lhs) < eps;
}

// Rounds the result of division (num/den) to the next greater integer value.
constexpr uint64_t DivCeil64(uint64_t num, uint64_t den) noexcept {
  SDB_ASSERT(den != 0);
  SDB_ASSERT((num + den) >= num);
  return (num + den - 1) / den;
}

template<typename Iterator>
constexpr size_t Popcount(Iterator begin, Iterator end) noexcept {
  return std::accumulate(begin, end, size_t{0}, [](size_t acc, auto word) {
    return acc + std::popcount(word);
  });
}

}  // namespace irs::math
