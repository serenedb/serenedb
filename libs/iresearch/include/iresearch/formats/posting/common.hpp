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

#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/formats/formats_attributes.hpp"
#include "iresearch/search/cost.hpp"
#include "iresearch/types.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs {

// TODO(mbkkt) avoid logic duplication
// I think there's two options:
// 1. each file contains this file version
// 2. single per segment file controls over versions

// Format of terms, written in ".tm" file
enum class TermsFormat : int32_t {
  Min = 0,
  Max = Min,
};

// Format of postings, written in ".doc", ".pos", ".pay" files
enum class PostingsFormat : int32_t {
  Min = 0,

  WandSimd = Min,

  Max = WandSimd,
};

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

template<typename FieldTraits, typename Input>
IRS_FORCE_INLINE void ReadState(SkipState& state, Input& in) {
  state.doc = in.ReadV32();
  state.doc_ptr += in.ReadV64();
  if constexpr (FieldTraits::Position()) {
    state.pos_ptr += in.ReadV64();
    if constexpr (FieldTraits::Offset()) {
      state.pay_ptr += in.ReadV64();
    }
    state.pos_offset = in.ReadByte();
  }
}

template<typename IteratorTraits>
IRS_FORCE_INLINE void CopyState(SkipState& to,
                                const TermMetaImpl& from) noexcept {
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

template<typename WandExtent, typename Input>
void CommonSkipWandData(WandExtent extent, Input& in) {
  switch (auto count = extent.GetExtent(); count) {
    case 0:
      return;
    case 1:
      in.Skip(in.ReadByte());
      return;
    default: {
      uint64_t skip{};
      for (; count; --count) {
        skip += in.ReadByte();
      }
      in.Skip(skip);
      return;
    }
  }
}
template<typename It, typename T, typename Cmp = std::less<>>
It branchless_lower_bound(It begin, It end, const T& value,
                          Cmp&& compare = {}) {
  size_t len = end - begin;
  if (len == 0) {
    return end;
  }
  size_t step = std::bit_floor(len);
  if (step != len && compare(begin[step], value)) {
    len -= step + 1;
    if (len == 0) {
      return end;
    }
    step = std::bit_ceil(len);
    begin = end - step;
  }
  for (step /= 2; step != 0; step /= 2) {
    if (compare(begin[step], value)) {
      begin += step;
    }
  }
  return begin + compare(*begin, value);
}

template<typename IteratorTraits>
using AttributesImpl =
  std::conditional_t<IteratorTraits::Frequency(),
                     std::tuple<FreqBlockAttr, CostAttr>, std::tuple<CostAttr>>;

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

template<typename PostingImpl>
struct PostingAdapter {
  PostingAdapter(DocIterator::ptr it) noexcept : _it{std::move(it)} {
    SDB_ASSERT(_it);
    SDB_ASSERT(dynamic_cast<PostingImpl*>(_it.get()));
  }

  PostingAdapter(PostingAdapter&&) noexcept = default;
  PostingAdapter& operator=(PostingAdapter&&) noexcept = default;

  IRS_FORCE_INLINE operator DocIterator::ptr&&() && noexcept {
    return std::move(_it);
  }

  IRS_FORCE_INLINE Attribute* GetMutable(TypeInfo::type_id type) noexcept {
    return self().GetMutable(type);
  }

  IRS_FORCE_INLINE const doc_id_t& value() const noexcept {
    return self().value();
  }

  IRS_FORCE_INLINE doc_id_t advance() { return self().advance(); }

  IRS_FORCE_INLINE doc_id_t seek(doc_id_t target) {
    return self().seek(target);
  }

  IRS_FORCE_INLINE doc_id_t LazySeek(doc_id_t target) {
    return self().LazySeek(target);
  }

  IRS_FORCE_INLINE void FetchScoreArgs(uint16_t index) {
    return self().FetchScoreArgs(index);
  }

  IRS_FORCE_INLINE ScoreFunction
  PrepareScore(const PrepareScoreContext& ctx) const noexcept {
    return self().PrepareScore(ctx);
  }

  IRS_FORCE_INLINE std::pair<doc_id_t, bool> FillBlock(
    doc_id_t min, doc_id_t max, uint64_t* mask, FillBlockScoreContext score,
    FillBlockMatchContext match) {
    return self().FillBlock(min, max, mask, score, match);
  }

 protected:
  IRS_FORCE_INLINE PostingImpl& self() const noexcept {
    return static_cast<PostingImpl&>(*_it);
  }

  DocIterator::ptr _it;
};

}  // namespace irs
