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

#include <roaring/bitset_util.h>

#include <algorithm>
#include <cstdint>

#include "iresearch/search/detail/window.hpp"
#include "iresearch/types.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/lower_bound.hpp"
#include "iresearch/utils/shared.hpp"

namespace irs::docs_mask {

template<bool kAndNot>
IRS_FORCE_INLINE void Apply(uint64_t& word, uint64_t bits) noexcept {
  if constexpr (kAndNot) {
    word &= ~bits;
  } else {
    word |= bits;
  }
}

template<bool kAndNot>
IRS_FORCE_INLINE void ApplyRange(uint64_t* IRS_RESTRICT words, uint32_t first,
                                 uint32_t last) noexcept {
  if constexpr (kAndNot) {
    roaring::internal::bitset_reset_range(words, first, last);
  } else {
    roaring::internal::bitset_set_range(words, first, last);
  }
}

template<bool kAndNot>
inline void ApplyWords(const uint64_t* IRS_RESTRICT src, uint32_t src_words,
                       uint64_t src_base, uint64_t lo, uint64_t hi,
                       doc_id_t min, doc_id_t max,
                       uint64_t* IRS_RESTRICT dst) noexcept {
  const auto first = static_cast<uint32_t>((lo - min) / 64);
  const auto last = static_cast<uint32_t>((hi - min + 63) / 64);
  const uint32_t full = (max - min) / 64;
  const auto offset =
    static_cast<int64_t>(min) - static_cast<int64_t>(src_base);
  const auto stop = std::min(last, full);
  auto w = first;
  if (w < stop && offset + static_cast<int64_t>(w) * 64 < 0) {
    Apply<kAndNot>(
      dst[w],
      detail::WordAt(src, src_words, offset + static_cast<int64_t>(w) * 64));
    ++w;
  }
  if (w < stop) {
    SDB_ASSERT(offset + static_cast<int64_t>(w) * 64 >= 0);
    auto* IRS_RESTRICT out = dst + w;
    const auto start =
      static_cast<uint32_t>(offset + static_cast<int64_t>(w) * 64);
    const auto count = stop - w;
    for (uint32_t i = 0; i != count; ++i) {
      Apply<kAndNot>(out[i], detail::WordAt(src, src_words,
                                            int64_t{start} + int64_t{i} * 64));
    }
  }
  if (last > full) {
    const uint32_t rest = (max - min) % 64;
    Apply<kAndNot>(
      dst[full],
      detail::WordAt(src, src_words, offset + static_cast<int64_t>(full) * 64) &
        (~uint64_t{0} >> (64 - rest)));
  }
}

template<typename T, typename Less>
IRS_FORCE_INLINE uint32_t LowerBound(const T* values, uint32_t len,
                                     Less&& less) noexcept {
  return static_cast<uint32_t>(BranchlessPartitionPoint(values, len, less) -
                               values);
}

template<typename T, typename Less>
IRS_FORCE_INLINE uint32_t Gallop(const T* values, uint32_t from, uint32_t len,
                                 Less&& less) noexcept {
  constexpr uint32_t kLanes = 16;
  SDB_ASSERT(from != 0 && less(values[from - 1]));
  auto lo = from - 1;
  uint32_t step = 1;
  for (auto hi = lo + step; hi < len; hi = lo + step) {
    if (!less(values[hi])) {
      while (step > kLanes) {
        step /= 2;
        if (less(values[lo + step])) {
          lo += step;
        }
      }
      if (step == kLanes) {
        const T* window = values + lo + 1;
        uint32_t below = 0;
        for (uint32_t i = 0; i != kLanes; ++i) {
          below += less(window[i]);
        }
        return lo + 1 + below;
      }
      while (step > 1) {
        step /= 2;
        if (less(values[lo + step])) {
          lo += step;
        }
      }
      return lo + 1;
    }
    lo = hi;
    step *= 2;
  }
  const T* tail = values + lo + 1;
  return lo + 1 +
         static_cast<uint32_t>(PartitionPoint(tail, len - lo - 1, less) - tail);
}

}  // namespace irs::docs_mask
