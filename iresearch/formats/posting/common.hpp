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
#include <tuple>

#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/formats/posting_meta.hpp"
#include "iresearch/index/index_features.hpp"
#include "iresearch/types.hpp"
#include "iresearch/utils/bit_utils.hpp"
#include "iresearch/utils/file_utils_ext.hpp"
#include "iresearch/utils/shared.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs {

struct SkipState {
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

template<typename IteratorTraits>
IRS_FORCE_INLINE void CopyState(SkipState& to, const SkipState& from) noexcept {
  if constexpr (IteratorTraits::Offset()) {
    to = from;
  } else {
    to.doc_ptr = from.doc_ptr;
    to.doc = from.doc;
    if constexpr (IteratorTraits::Position()) {
      to.pos_offset = from.pos_offset;
      to.pos_ptr = from.pos_ptr;
    }
  }
}

// What a skip level holds beyond the document and where its block starts:
// a position pointer when the field has positions, a payload pointer beside
// it when the field has offsets, and a position offset closing the level.
// Which of them are there is the field's own answer and the same for every
// level and every term, so it is read once where the leaf is built.
struct SkipLayout {
  bool pos = false;
  bool offs = false;
};

IRS_FORCE_INLINE constexpr SkipLayout ToSkipLayout(
  IndexFeatures features) noexcept {
  return {.pos = IndexFeatures::None != (features & IndexFeatures::Pos),
          .offs = IndexFeatures::None != (features & IndexFeatures::Offs)};
}

IRS_FORCE_INLINE constexpr bool FeaturesHaveFreq(
  IndexFeatures features) noexcept {
  return IndexFeatures::None != (features & IndexFeatures::Freq);
}

// A level of a field whose positions the leaf never reads. What the field
// wrote for the streams it does not touch is stepped over, not parsed: the
// copy traits of every such leaf carry neither pointer out of a level, so
// accumulating them would be writing state nothing reads back.
template<typename Input>
IRS_FORCE_INLINE void ReadDocState(SkipState& state, Input& in,
                                   SkipLayout layout) {
  state.doc = in.ReadV32();
  state.doc_ptr += in.ReadV64();
  if (layout.pos) {
    in.SkipV64();
    if (layout.offs) {
      in.SkipV64();
    }
    in.Skip(sizeof(uint16_t));
  }
}

// A level of a field the leaf does read positions out of. Such a leaf is
// built only on a field that has them, so the position pointer is not a
// question -- `Offs` is whether this leaf decodes the payloads beside them,
// and `has_pay` whether the field wrote a pointer to step over either way.
template<bool Offs, typename Input>
IRS_FORCE_INLINE void ReadPosState(SkipState& state, Input& in, bool has_pay) {
  state.doc = in.ReadV32();
  state.doc_ptr += in.ReadV64();
  state.pos_ptr += in.ReadV64();
  if (has_pay) {
    if constexpr (Offs) {
      state.pay_ptr += in.ReadV64();
    } else {
      in.SkipV64();
    }
  }
  state.pos_offset = static_cast<uint16_t>(in.ReadI16());
}

template<typename IteratorTraits>
IRS_FORCE_INLINE void CopyState(SkipState& to,
                                const PostingMeta& from) noexcept {
  to.doc_ptr = from.doc_start;
  if constexpr (IteratorTraits::Position()) {
    to.pos_ptr = from.pos_start;
    if constexpr (IteratorTraits::Offset()) {
      to.pay_ptr = from.pay_start;
    }
    to.pos_offset = from.pos_offset;
  }
}

// TODO(mbkkt) Make it overloads
// Remove to many Readers implementations

template<typename Input>
void SkipScoreBounds(bool has_score_bounds, Input& in) {
  if (has_score_bounds) {
    in.Skip(in.ReadByte());
  }
}

inline constexpr uint64_t kDocsPerSkipByte = 4;
inline constexpr uint64_t kPosBytesPerFreq = 3;

template<typename Input>
void LimitDocReadahead(Input& in, const PostingMeta& meta) noexcept {
  const uint64_t blocks =
    meta.docs_count > doc_limits::kBlockSize
      ? uint64_t{meta.doc_delta} + meta.docs_count / kDocsPerSkipByte
      : 0;
  in.LimitReadahead(meta.doc_start + blocks + file_utils::kPage);
}

template<typename Input>
void LimitPosReadahead(Input& in, const PostingMeta& meta) noexcept {
  in.LimitReadahead(meta.pos_start + uint64_t{meta.freq} * kPosBytesPerFreq +
                    file_utils::kPage);
}

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
                                    const uint64_t* IRS_RESTRICT src,
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
                                        const uint64_t* IRS_RESTRICT src,
                                        uint32_t words,
                                        uint64_t last) noexcept {
  MergeBitsetAt(dst, begin, src, words, last,
                [](uint64_t& word, uint64_t bits)
                  IRS_FORCE_INLINE { word |= bits; });
}

inline IRS_FORCE_INLINE void OrBitsetAt(uint64_t* IRS_RESTRICT dst,
                                        uint64_t begin,
                                        const uint64_t* IRS_RESTRICT src,
                                        uint32_t words) noexcept {
  OrBitsetAt(dst, begin, src, words, src[words - 1]);
}

inline IRS_FORCE_INLINE void AndNotBitsetAt(uint64_t* IRS_RESTRICT dst,
                                            uint64_t begin,
                                            const uint64_t* IRS_RESTRICT src,
                                            uint32_t words,
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
    static constexpr size_t kChains = 8;
    static constexpr size_t kSlice = N / kChains;
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

template<size_t W>
IRS_FORCE_INLINE uint32_t CountLess(const uint32_t* begin,
                                    uint32_t value) noexcept {
  static_assert(W % 32 == 0);
  uint32_t count = 0;
  for (size_t i = 0; i != W; i += 32) {
#ifdef __AVX2__
    const __m256i bias = _mm256_set1_epi32(std::numeric_limits<int32_t>::min());
    const __m256i target = _mm256_xor_si256(
      _mm256_set1_epi32(static_cast<int32_t>(value)), bias);
    const auto less = [&](size_t j) IRS_FORCE_INLINE {
      return _mm256_cmpgt_epi32(
        target,
        _mm256_xor_si256(
          _mm256_loadu_si256(reinterpret_cast<const __m256i*>(begin + j)),
          bias));
    };
    const __m256i low = _mm256_packs_epi32(less(i), less(i + 8));
    const __m256i high = _mm256_packs_epi32(less(i + 16), less(i + 24));
    count += std::popcount(static_cast<uint32_t>(
      _mm256_movemask_epi8(_mm256_packs_epi16(low, high))));
#else
    using U32x8 = uint32_t __attribute__((vector_size(32)));
    using I32x8 = int32_t __attribute__((vector_size(32)));
    const U32x8 target = U32x8{} + value;
    I32x8 acc{};
    for (size_t j = i; j != i + 32; j += 8) {
      U32x8 v;
      std::memcpy(&v, begin + j, sizeof(v));
      acc += (I32x8)(v < target);
    }
    int32_t sum = 0;
    for (size_t lane = 0; lane != 8; ++lane) {
      sum += acc[lane];
    }
    count += static_cast<uint32_t>(-sum);
#endif
  }
  return count;
}

template<size_t N, typename It, typename T, typename Cmp = std::less<>>
IRS_FORCE_INLINE It BranchlessLowerBound(It begin, const T& value,
                                         Cmp&& compare = {}) {
  static_assert(std::has_single_bit(N));
  constexpr size_t kWindow = 64;
  if constexpr (N > kWindow && std::is_pointer_v<It> &&
                std::is_same_v<std::remove_const_t<std::remove_pointer_t<It>>,
                               uint32_t> &&
                std::is_same_v<T, uint32_t> &&
                std::is_same_v<std::remove_cvref_t<Cmp>, std::less<>>) {
    for (size_t step = N / 2; step >= kWindow; step /= 2) {
      if (begin[step - 1] < value) {
        begin += step;
      }
    }
    return begin + CountLess<kWindow>(begin, value);
  } else {
    for (size_t step = N / 2; step != 0; step /= 2) {
      if (compare(begin[step], value)) {
        begin += step;
      }
    }
    return begin + compare(*begin, value);
  }
}

template<typename FormatTraits, bool Freq, bool Pos, bool Offs>
struct IteratorTraitsImpl : FormatTraits {
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
