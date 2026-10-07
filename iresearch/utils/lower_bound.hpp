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
/// Copyright holder is SereneDB GmbH
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <bit>
#include <cstddef>
#include <functional>

#include "iresearch/utils/shared.hpp"

namespace irs {

template<size_t N, typename It, typename T, typename Cmp = std::less<>>
IRS_FORCE_INLINE It BranchlessLowerBound(It begin, const T& value,
                                         Cmp&& compare = {}) {
  static_assert(std::has_single_bit(N));
  for (size_t step = N / 2; step != 0; step /= 2) {
    if (compare(begin[step], value)) {
      begin += step;
    }
  }
  return begin + compare(*begin, value);
}

template<typename It, typename Pred>
IRS_FORCE_INLINE It BranchlessPartitionPoint(It begin, size_t len,
                                             Pred&& pred) {
  if (len == 0) {
    return begin;
  }
  const auto end = begin + len;
  size_t step = std::bit_floor(len);
  if (step != len && pred(begin[step])) {
    const auto rest = len - step - 1;
    if (rest == 0) {
      return end;
    }
    step = std::bit_ceil(rest);
    begin = end - step;
  }
  for (step /= 2; step != 0; step /= 2) {
    if (pred(begin[step])) {
      begin += step;
    }
  }
  return begin + pred(*begin);
}

}  // namespace irs
