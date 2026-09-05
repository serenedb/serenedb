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

#include <bit>
#include <cstdint>
#include <functional>
#include <span>
#include <tuple>

#include "basics/bit_utils.hpp"
#include "basics/shared.hpp"
#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/formats/posting_meta.hpp"
#include "iresearch/index/index_features.hpp"
#include "iresearch/types.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs {

struct SkipState {
  // pointer to the beginning of document block
  uint64_t doc_ptr = 0;
  // last document in a previous block
  doc_id_t doc = doc_limits::invalid();
  // positions to skip before new document block
  uint32_t pos_offset = 0;
  // pointer to the positions of the first document in a document block
  uint64_t pos_ptr = 0;
  // pointer to the payloads of the first document in a document block
  uint64_t pay_ptr = 0;
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
    std::ignore = in.ReadByte();
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
  state.pos_offset = in.ReadByte();
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

inline IRS_FORCE_INLINE void OrBitsetAt(uint64_t* IRS_RESTRICT dst,
                                        uint64_t begin,
                                        const uint64_t* IRS_RESTRICT src,
                                        uint32_t words,
                                        uint64_t last) noexcept {
  SDB_ASSERT(words != 0);
  constexpr auto kBits = BitsRequired<uint64_t>();
  const auto tail = words - 1;
  dst += begin / kBits;
  const auto shift = begin % kBits;
  if (shift == 0) {
    for (uint32_t i = 0; i != tail; ++i) {
      dst[i] |= src[i];
    }
    dst[tail] |= last;
    return;
  }
  uint64_t carry = 0;
  for (uint32_t i = 0; i != tail; ++i) {
    const auto word = src[i];
    dst[i] |= (word << shift) | carry;
    carry = word >> (kBits - shift);
  }
  dst[tail] |= (last << shift) | carry;
  carry = last >> (kBits - shift);
  if (carry != 0) {
    dst[words] |= carry;
  }
}

inline IRS_FORCE_INLINE void OrBitsetAt(uint64_t* IRS_RESTRICT dst,
                                        uint64_t begin,
                                        const uint64_t* IRS_RESTRICT src,
                                        uint32_t words) noexcept {
  OrBitsetAt(dst, begin, src, words, src[words - 1]);
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
