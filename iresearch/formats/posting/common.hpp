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

#include <absl/base/internal/endian.h>

#ifdef __AVX2__
#include <immintrin.h>
#endif

#include <bit>
#include <cstdint>
#include <functional>
#include <span>

#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/formats/posting/block_codec.hpp"
#include "iresearch/formats/posting_meta.hpp"
#include "iresearch/index/index_features.hpp"
#include "iresearch/types.hpp"
#include "iresearch/utils/bit_utils.hpp"
#include "iresearch/utils/file_utils_ext.hpp"
#include "iresearch/utils/lower_bound.hpp"
#include "iresearch/utils/shared.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs {

struct BlockLanding {
  // pointer to the beginning of document block
  uint64_t doc_ptr = 0;
  // last document in a previous block
  doc_id_t doc = doc_limits::invalid();
  uint32_t pos_offset = 0;
  uint64_t pos_ptr = 0;
  uint64_t pay_ptr = 0;
};

struct PosGroup {
  static constexpr uint32_t kBlocks = 16;
  static constexpr uint32_t kHeaderBytes = kBlocks * sizeof(uint16_t);
  static constexpr uint32_t kPositions = kBlocks * pos_limits::kBlockSize;

  IRS_FORCE_INLINE static uint32_t End(const byte_type* header,
                                       uint32_t block) noexcept {
    SDB_ASSERT(block < kBlocks);
    return absl::little_endian::Load16(header + block * sizeof(uint16_t));
  }

  IRS_FORCE_INLINE static uint32_t Start(const byte_type* header,
                                         uint32_t block) noexcept {
    return block == 0 ? 0 : End(header, block - 1);
  }

  IRS_FORCE_INLINE static uint64_t Next(uint64_t group,
                                        const byte_type* header) noexcept {
    return group + kHeaderBytes + End(header, kBlocks - 1);
  }
};

IRS_FORCE_INLINE constexpr bool FeaturesHaveFreq(
  IndexFeatures features) noexcept {
  return IndexFeatures::None != (features & IndexFeatures::Freq);
}

// TODO(mbkkt) Make it overloads
// Remove to many Readers implementations

template<typename Input>
void SkipScoreBounds(bool has_score_bounds, Input& in) {
  if (has_score_bounds) {
    in.Skip(in.ReadByte());
  }
}

inline constexpr uint64_t kMaxPrefetch = uint64_t{16} << 20;

template<typename Input>
void Hint(const Input& in, uint64_t offset, uint64_t size) noexcept {
  if (in.Warm(offset, size)) {
    file_utils::PollResidency();
    return;
  }
  if (size <= file_utils::kPrefetchChunk || !in.Resident(offset, size)) {
    in.Prefetch(offset, size);
  }
  in.MarkWarm(offset, size);
}

inline uint64_t DocExtent(const PostingMeta& meta) noexcept {
  if (meta.inline_size != 0 || meta.docs_count <= 1) {
    return 0;
  }
  return meta.docs_count > doc_limits::kBlockSize ? uint64_t{meta.doc_delta}
                                                  : file_utils::kPage;
}

inline uint64_t PosExtent(const PostingMeta& meta) noexcept {
  return pos_limits::kBlockSize < meta.freq
           ? meta.pos_extent
           : 2 * (PosGroup::kHeaderBytes + block_codec::kMaxBlockBytes);
}

inline uint64_t PayExtent(const PostingMeta& meta) noexcept {
  return pos_limits::kBlockSize < meta.freq
           ? meta.pay_extent
           : 2 * (PosGroup::kHeaderBytes + 2 * block_codec::kMaxBlockBytes);
}

template<typename Input>
void PrefetchDocs(const Input& in, const PostingMeta& meta) noexcept {
  if (const auto extent = DocExtent(meta); extent > file_utils::kPage) {
    Hint(in, meta.doc_start, std::min(extent, kMaxPrefetch));
  }
}

class GrowingHint {
 public:
  void Arm(uint64_t at, uint64_t stop) noexcept {
    _last = at;
    _end = at;
    _stop = stop;
    _run = 0;
    _skipped = 0;
    _window = kFirst;
  }

  template<typename Input>
  IRS_FORCE_INLINE void Advance(const Input& in, uint64_t at) noexcept {
    if (at > _last) {
      const auto gap = at - _last;
      (gap < file_utils::kPage ? _run : _skipped) += gap;
    }
    _last = at;
    if (at + _window / 2 >= _end && _end < _stop) [[unlikely]] {
      Grow(in, at);
    }
  }

 private:
  static constexpr uint64_t kFirst = file_utils::kPrefetchChunk;
  static constexpr uint64_t kLast = file_utils::kMaxReadahead;

  template<typename Input>
  void Grow(const Input& in, uint64_t at) noexcept {
    const auto from = std::max(at, _end);
    if (_run < file_utils::kPage || 2 * _run < _skipped || from >= _stop) {
      return;
    }
    const auto len = std::min(_window, _stop - from);
    Hint(in, from, len);
    _end = from + len;
    _window = std::min(2 * _window, kLast);
  }

  uint64_t _last = 0;
  uint64_t _end = 0;
  uint64_t _stop = 0;
  uint64_t _run = 0;
  uint64_t _skipped = 0;
  uint64_t _window = kFirst;
};

inline IRS_FORCE_INLINE void SetBitRange(uint64_t* IRS_RESTRICT words,
                                         uint64_t begin,
                                         uint64_t end) noexcept {
  SDB_ASSERT(begin < end);
  constexpr auto kBits = BitsRequired<uint64_t>();
  const auto first = begin / kBits;
  const auto last = (end - 1) / kBits;
  const uint64_t head = ~uint64_t{0} << (begin % kBits);
  const uint64_t tail = ~uint64_t{0} >> (kBits - 1 - (end - 1) % kBits);
  if (first == last) {
    words[first] |= head & tail;
    return;
  }
  words[first] |= head;
  for (auto i = first + 1; i != last; ++i) {
    words[i] = ~uint64_t{0};
  }
  words[last] |= tail;
}

inline IRS_FORCE_INLINE void ClearBitRange(uint64_t* IRS_RESTRICT words,
                                           uint64_t begin,
                                           uint64_t end) noexcept {
  SDB_ASSERT(begin < end);
  constexpr auto kBits = BitsRequired<uint64_t>();
  const auto first = begin / kBits;
  const auto last = (end - 1) / kBits;
  const uint64_t head = ~uint64_t{0} << (begin % kBits);
  const uint64_t tail = ~uint64_t{0} >> (kBits - 1 - (end - 1) % kBits);
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

template<typename Merge>
IRS_FORCE_INLINE void MergeBitsetAt(uint64_t* IRS_RESTRICT dst, uint64_t begin,
                                    const UnalignedWord* IRS_RESTRICT src,
                                    uint32_t words, uint64_t last,
                                    Merge&& merge) noexcept {
  SDB_ASSERT(words != 0);
  constexpr auto kBits = BitsRequired<uint64_t>();
  const auto tail = words - 1;
  dst += begin / kBits;
  const auto shift = begin % kBits;
  if (shift == 0) {
    for (uint32_t i = 0; i != tail; ++i) {
      merge(dst[i], src[i]);
    }
    merge(dst[tail], last);
    return;
  }
  uint64_t carry = 0;
  for (uint32_t i = 0; i != tail; ++i) {
    const auto word = src[i];
    merge(dst[i], (word << shift) | carry);
    carry = word >> (kBits - shift);
  }
  merge(dst[tail], (last << shift) | carry);
  carry = last >> (kBits - shift);
  if (carry != 0) {
    merge(dst[words], carry);
  }
}

inline IRS_FORCE_INLINE void OrBitsetAt(uint64_t* IRS_RESTRICT dst,
                                        uint64_t begin,
                                        const UnalignedWord* IRS_RESTRICT src,
                                        uint32_t words,
                                        uint64_t last) noexcept {
  MergeBitsetAt(dst, begin, src, words, last,
                [](uint64_t& word, uint64_t bits)
                  IRS_FORCE_INLINE { word |= bits; });
}

inline IRS_FORCE_INLINE void AndNotBitsetAt(
  uint64_t* IRS_RESTRICT dst, uint64_t begin,
  const UnalignedWord* IRS_RESTRICT src, uint32_t words,
  uint64_t last) noexcept {
  MergeBitsetAt(dst, begin, src, words, last,
                [](uint64_t& word, uint64_t bits)
                  IRS_FORCE_INLINE { word &= ~bits; });
}

template<size_t N, typename Visitor>
IRS_FORCE_INLINE void VisitDocs(uint32_t size, Visitor&& visit) {
  if constexpr (N == std::dynamic_extent) {
    for (uint32_t i = 0; i != size; ++i) {
      visit(i);
    }
  } else {
    static constexpr size_t kSlice = 16;
    static constexpr size_t kChains = N / kSlice;
    uint32_t i = 0;
    for (; i != kSlice; ++i) {
      for (uint32_t chain = 0; chain != kChains; ++chain) {
        visit(i + chain * kSlice);
      }
    }
    for (i *= kChains; i != N; ++i) {
      visit(i);
    }
  }
}

template<bool Freq, bool Pos, bool Offs>
struct IteratorTraitsImpl {
  static constexpr bool Frequency() noexcept { return Freq; }
  static constexpr bool Position() noexcept { return Freq && Pos; }
  static constexpr bool Offset() noexcept { return Position() && Offs; }
  static constexpr IndexFeatures Features() noexcept {
    auto r = IndexFeatures::None;
    if constexpr (Freq) {
      r |= IndexFeatures::Freq;
    }
    if constexpr (Pos) {
      r |= IndexFeatures::Pos;
    }
    if constexpr (Offs) {
      r |= IndexFeatures::Offs;
    }
    return r;
  }
};

}  // namespace irs
