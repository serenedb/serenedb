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

#include "iresearch/types.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/bit_utils.hpp"
#include "iresearch/utils/shared.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::detail {

inline constexpr size_t kWindowWords = 64;
inline constexpr doc_id_t kWindowBits = BitsRequired<uint64_t>();
inline constexpr doc_id_t kWindowDocs = kWindowWords * kWindowBits;
inline constexpr uint32_t kSparseInverse = BitsRequired<doc_id_t>();
inline constexpr uint32_t kSparseWindow = kWindowDocs / kSparseInverse;
inline constexpr uint64_t kProbeSteps =
  std::countr_zero(doc_limits::kBlockSize) + 2;

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

template<typename Group>
consteval bool Retracts() {
  if constexpr (requires { Group::kRetracts; }) {
    return Group::kRetracts;
  } else {
    return false;
  }
}

template<typename Group>
consteval bool LazyReset() {
  if constexpr (requires { Group::kLazyReset; }) {
    return Group::kLazyReset;
  } else {
    return false;
  }
}

template<typename Group>
consteval bool Tallies() {
  if constexpr (requires { Group::kTally; }) {
    return Group::kTally;
  } else {
    return false;
  }
}

inline IRS_FORCE_INLINE void Clear(uint64_t* IRS_RESTRICT dst,
                                   size_t words) noexcept {
  for (size_t w = 0; w != words; ++w) {
    dst[w] = 0;
  }
}

inline void ClearScored(uint64_t* IRS_RESTRICT words,
                        score_t* IRS_RESTRICT window) noexcept {
  for (uint32_t w = 0; w != kWindowWords; ++w) {
    auto word = words[w];
    words[w] = 0;
    const auto base = w * kWindowBits;
    while (word != 0) {
      window[base + std::countr_zero(word)] = 0;
      word = PopBit(word);
    }
  }
}

inline doc_id_t FindSet(const uint64_t* IRS_RESTRICT mask,
                        doc_id_t offset) noexcept {
  auto word = offset / kWindowBits;
  auto bits = mask[word] & (~uint64_t{0} << (offset % kWindowBits));
  for (;;) {
    if (bits != 0) {
      return static_cast<doc_id_t>(word * kWindowBits + std::countr_zero(bits));
    }
    if (++word == kWindowWords) {
      return kWindowDocs;
    }
    bits = mask[word];
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

inline uint32_t TakeDocs(doc_id_t min, uint64_t* IRS_RESTRICT words,
                         size_t count, doc_id_t* IRS_RESTRICT out) noexcept {
  SDB_ASSERT(count <= kWindowWords);
  uint64_t live = 0;
  for (size_t w = 0; w != count; ++w) {
    live |= static_cast<uint64_t>(words[w] != 0) << w;
  }
  uint32_t n = 0;
  for (; live != 0; live = PopBit(live)) {
    const auto w = static_cast<uint32_t>(std::countr_zero(live));
    const auto base = min + w * kWindowBits;
    for (auto word = words[w]; word != 0; word = PopBit(word)) {
      out[n++] = base + static_cast<doc_id_t>(std::countr_zero(word));
    }
    words[w] = 0;
  }
  return n;
}

inline void PutDocs(doc_id_t min, const doc_id_t* IRS_RESTRICT docs, uint32_t n,
                    uint64_t* IRS_RESTRICT words) noexcept {
  for (uint32_t i = 0; i != n; ++i) {
    const auto offset = docs[i] - min;
    SetBit(words[offset / kWindowBits], offset % kWindowBits);
  }
}

template<bool Keep, typename Leaf>
uint32_t SiftByFill(Leaf& leaf, const doc_id_t* in, uint32_t n, doc_id_t* out) {
  uint64_t words[kWindowWords];
  uint32_t kept = 0;
  const doc_id_t stop = n != 0 ? in[n - 1] + 1 : 0;
  for (uint32_t i = 0; i != n;) {
    const auto min = in[i];
    const auto max = stop - min > kWindowDocs ? min + kWindowDocs : stop;
    const auto count = WindowWords(min, max);
    for (size_t w = 0; w != count; ++w) {
      words[w] = 0;
    }
    leaf.FillOr(min, max, words);
    do {
      const auto doc = in[i];
      const auto offset = doc - min;
      out[kept] = doc;
      kept += static_cast<uint32_t>(
        CheckBit(words[offset / kWindowBits], offset % kWindowBits) == Keep);
    } while (++i != n && in[i] < max);
  }
  return kept;
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

inline IRS_FORCE_INLINE uint64_t WordAt(const UnalignedWord* IRS_RESTRICT src,
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

}  // namespace irs::detail
