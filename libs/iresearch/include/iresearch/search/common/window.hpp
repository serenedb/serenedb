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
/// Copyright holder is SereneDB GmbH, Berlin, Germany
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <absl/base/optimization.h>

#include <algorithm>
#include <bit>
#include <cstddef>
#include <cstdint>

#include "basics/assert.h"
#include "basics/bit_utils.hpp"
#include "basics/shared.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::search {

inline constexpr size_t kWindowWords = 64;
inline constexpr doc_id_t kWindowBits = BitsRequired<uint64_t>();
inline constexpr doc_id_t kWindowDocs = kWindowWords * kWindowBits;

static_assert(kWindowDocs < doc_limits::eof());

struct ABSL_CACHELINE_ALIGNED Scratch {
  uint64_t words[kWindowWords];

  IRS_FORCE_INLINE uint64_t* data() noexcept { return words; }
  IRS_FORCE_INLINE const uint64_t* data() const noexcept { return words; }
  IRS_FORCE_INLINE uint64_t* begin() noexcept { return words; }
  IRS_FORCE_INLINE uint64_t* end() noexcept { return words + kWindowWords; }
  IRS_FORCE_INLINE uint64_t& operator[](size_t i) noexcept { return words[i]; }
  IRS_FORCE_INLINE uint64_t operator[](size_t i) const noexcept {
    return words[i];
  }
};

IRS_FORCE_INLINE constexpr size_t WindowWords(doc_id_t min,
                                              doc_id_t max) noexcept {
  SDB_ASSERT(min < max);
  SDB_ASSERT(max - min <= kWindowDocs);
  return (max - min + kWindowBits - 1) / kWindowBits;
}

IRS_FORCE_INLINE constexpr bool NextWindow(doc_id_t min, doc_id_t next,
                                           doc_id_t& target) noexcept {
  const auto after = min + kWindowDocs;
  if (after < min) {
    return false;
  }
  target = std::max(next, after);
  return true;
}

inline IRS_FORCE_INLINE void Clear(uint64_t* IRS_RESTRICT dst,
                                   size_t words) noexcept {
  for (size_t w = 0; w != words; ++w) {
    dst[w] = 0;
  }
}

inline IRS_FORCE_INLINE uint32_t Cardinality(const uint64_t* IRS_RESTRICT src,
                                             size_t words) noexcept {
  uint32_t card = 0;
  for (size_t w = 0; w != words; ++w) {
    card += static_cast<uint32_t>(std::popcount(src[w]));
  }
  return card;
}

inline IRS_FORCE_INLINE uint32_t CountAndClear(uint64_t* IRS_RESTRICT dst,
                                               size_t words) noexcept {
  uint32_t card = 0;
  for (size_t w = 0; w != words; ++w) {
    card += static_cast<uint32_t>(std::popcount(dst[w]));
    dst[w] = 0;
  }
  return card;
}

inline IRS_FORCE_INLINE uint32_t FoldAnd(uint64_t* IRS_RESTRICT dst,
                                         const uint64_t* IRS_RESTRICT src,
                                         size_t words) noexcept {
  uint32_t card = 0;
  for (size_t w = 0; w != words; ++w) {
    const auto word = dst[w] & src[w];
    dst[w] = word;
    card += static_cast<uint32_t>(std::popcount(word));
  }
  return card;
}

inline IRS_FORCE_INLINE uint32_t FoldAndNot(uint64_t* IRS_RESTRICT dst,
                                            const uint64_t* IRS_RESTRICT src,
                                            size_t words) noexcept {
  uint32_t card = 0;
  for (size_t w = 0; w != words; ++w) {
    const auto word = dst[w] & ~src[w];
    dst[w] = word;
    card += static_cast<uint32_t>(std::popcount(word));
  }
  return card;
}

inline IRS_FORCE_INLINE uint64_t WordAt(const uint64_t* IRS_RESTRICT src,
                                        uint32_t count, int64_t at) noexcept {
  const int64_t idx = at >> 6;
  const auto shift = static_cast<uint32_t>(at & 63);
  const auto word = [&](int64_t i) noexcept -> uint64_t {
    return i >= 0 && i < static_cast<int64_t>(count)
             ? src[static_cast<size_t>(i)]
             : uint64_t{0};
  };
  const auto lo = word(idx);
  if (shift == 0) {
    return lo;
  }
  return (lo >> shift) | (word(idx + 1) << (kWindowBits - shift));
}

struct AndCursor {
  uint64_t* IRS_RESTRICT words;
  uint32_t at = 0;
  uint64_t keep = 0;

  static IRS_FORCE_INLINE uint64_t Between(uint64_t first,
                                           uint64_t last) noexcept {
    return ((uint64_t{2} << last) - 1) & (~uint64_t{0} << first);
  }

  IRS_FORCE_INLINE void Reach(uint64_t offset) noexcept {
    const auto word = static_cast<uint32_t>(offset / kWindowBits);
    if (word == at) {
      return;
    }
    words[at] &= keep;
    keep = 0;
    for (auto i = at + 1; i != word; ++i) {
      words[i] = 0;
    }
    at = word;
  }

  IRS_FORCE_INLINE void Doc(uint64_t offset) noexcept {
    Reach(offset);
    keep |= uint64_t{1} << (offset % kWindowBits);
  }

  IRS_FORCE_INLINE void Keep(uint64_t lo, uint64_t hi) noexcept {
    Reach(lo);
    const auto word = static_cast<uint32_t>(hi / kWindowBits);
    if (word == at) {
      keep |= Between(lo % kWindowBits, hi % kWindowBits);
      return;
    }
    words[at] &= keep | (~uint64_t{0} << (lo % kWindowBits));
    at = word;
    keep = (uint64_t{2} << (hi % kWindowBits)) - 1;
  }

  IRS_FORCE_INLINE void And(uint64_t lo, uint64_t hi, int64_t delta,
                            const uint64_t* IRS_RESTRICT src,
                            uint32_t count) noexcept {
    Reach(lo);
    words[at] &= keep | (~uint64_t{0} << (lo % kWindowBits));
    keep = 0;
    const auto first = static_cast<uint32_t>(lo / kWindowBits);
    const auto last = static_cast<uint32_t>(hi / kWindowBits);
    for (auto i = first; i <= last; ++i) {
      auto spare = uint64_t{0};
      if (i == first) {
        spare |= (uint64_t{1} << (lo % kWindowBits)) - 1;
      }
      if (i == last && hi % kWindowBits != kWindowBits - 1) {
        spare |= ~uint64_t{0} << (hi % kWindowBits + 1);
      }
      words[i] &=
        WordAt(src, count,
               static_cast<int64_t>(uint64_t{i} * kWindowBits) + delta) |
        spare;
    }
    at = last;
    keep = (uint64_t{2} << (hi % kWindowBits)) - 1;
  }

  IRS_FORCE_INLINE const doc_id_t* AndDocs(uint64_t lo, uint64_t hi,
                                           const doc_id_t* it,
                                           const doc_id_t* end,
                                           doc_id_t min) noexcept {
    Reach(lo);
    words[at] &= keep | (~uint64_t{0} << (lo % kWindowBits));
    keep = 0;
    const auto first = static_cast<uint32_t>(lo / kWindowBits);
    const auto last = static_cast<uint32_t>(hi / kWindowBits);
    const uint64_t stop = hi + 1;
    for (auto w = first; w <= last; ++w) {
      const uint64_t edge =
        std::min<uint64_t>(uint64_t{w + 1} * kWindowBits, stop);
      uint64_t own = 0;
      for (; it != end; ++it) {
        const uint64_t offset = *it - min;
        if (offset >= edge) {
          break;
        }
        own |= uint64_t{1} << (offset % kWindowBits);
      }
      uint64_t spare = 0;
      if (w == first) {
        spare |= (uint64_t{1} << (lo % kWindowBits)) - 1;
      }
      if (w == last && hi % kWindowBits != kWindowBits - 1) {
        spare |= ~uint64_t{0} << (hi % kWindowBits + 1);
      }
      words[w] &= own | spare;
    }
    at = last;
    keep = (uint64_t{2} << (hi % kWindowBits)) - 1;
    return it;
  }

  IRS_FORCE_INLINE void Settle(uint64_t limit) noexcept {
    const auto last = static_cast<uint32_t>((limit - 1) / kWindowBits);
    if (at > last) {
      return;
    }
    const auto tail = limit % kWindowBits;
    const uint64_t slack = tail == 0 ? uint64_t{0} : ~uint64_t{0} << tail;
    words[at] &= at == last ? (keep | slack) : keep;
    for (auto i = at + 1; i <= last; ++i) {
      words[i] = i == last ? (words[i] & slack) : uint64_t{0};
    }
    at = last + 1;
    keep = 0;
  }
};

inline IRS_FORCE_INLINE void ClearInclusive(uint64_t* IRS_RESTRICT words,
                                            uint64_t lo, uint64_t hi) noexcept {
  const auto first = static_cast<uint32_t>(lo / kWindowBits);
  const auto last = static_cast<uint32_t>(hi / kWindowBits);
  const uint64_t head = ~uint64_t{0} << (lo % kWindowBits);
  const uint64_t tail = ~uint64_t{0} >> (kWindowBits - 1 - hi % kWindowBits);
  if (first == last) {
    words[first] &= ~(head & tail);
    return;
  }
  words[first] &= ~head;
  for (auto i = first + 1; i != last; ++i) {
    words[i] = 0;
  }
  words[last] &= ~tail;
}

inline IRS_FORCE_INLINE void FoldCarry(uint64_t* IRS_RESTRICT planes,
                                       const uint64_t* IRS_RESTRICT src,
                                       size_t words, size_t top) noexcept {
  for (size_t w = 0; w != words; ++w) {
    const auto bits = src[w];
    if (bits == 0) {
      continue;
    }
    for (size_t j = top; j != 0; --j) {
      planes[j * kWindowWords + w] |= planes[(j - 1) * kWindowWords + w] & bits;
    }
    planes[w] |= bits;
  }
}

}  // namespace irs::search
