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

#include <algorithm>
#include <bit>

#include "basics/bit_utils.hpp"
#include "iresearch/formats/posting_meta.hpp"
#include "iresearch/search/common/posting_leaf.hpp"
#include "iresearch/search/common/window.hpp"
#include "iresearch/store/data_input.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::search {

template<typename InputType>
class PostingFill : public PostingLeaf<InputType, kWindowShape> {
  using Base = PostingLeaf<InputType, kWindowShape>;

  using Base::_doc;
  using Base::_docs;
  using Base::_last;
  using Base::_left_in_leaf;
  using Base::_left_in_list;
  using Base::Behind;
  using Base::Enc;
  using Base::In;
  using Base::kBits;
  using Base::kBlock;
  using Base::SkipFreqs;
  using Base::StableBitset;

 public:
  PostingFill() = default;

  PostingFill(const PostingMeta& meta, const IndexInput& doc_in,
              bool has_score_bounds, bool has_freq) {
    Prepare(meta, doc_in, has_score_bounds, has_freq);
  }

  void Prepare(const PostingMeta& meta, const IndexInput& doc_in,
               bool has_score_bounds, bool has_freq) {
    SDB_ASSERT(meta.docs_count != 0);
    this->SetFreqLen(has_freq);

    if (meta.docs_count == 1) {
      _doc = this->SetSingle(meta);
      _leaf = {.bitset = nullptr,
               .words = 0,
               .max = _last,
               .kind = FormatTraits128::FillLeaf::Kind::Docs};
      _leaf_base = _last - 1;
      _leaf_len = 1;
      return;
    }

    this->OpenInput(meta, doc_in, has_score_bounds);
  }

  doc_id_t FillOr(doc_id_t min, doc_id_t max, uint64_t* IRS_RESTRICT mask) {
    return FillImpl<false>(min, max, mask, [](size_t) IRS_FORCE_INLINE {});
  }

  doc_id_t FillSum(doc_id_t min, doc_id_t max, uint64_t* IRS_RESTRICT mask,
                   score_t* IRS_RESTRICT scores, score_t constant) {
    return FillImpl<true>(min, max, mask, [=](size_t offset) IRS_FORCE_INLINE {
      Merge<ScoreMergeType::Sum>(scores[offset], constant);
    });
  }

  doc_id_t FillMax(doc_id_t min, doc_id_t max, uint64_t* IRS_RESTRICT mask,
                   score_t* IRS_RESTRICT scores, score_t constant) {
    return FillImpl<true>(min, max, mask, [=](size_t offset) IRS_FORCE_INLINE {
      Merge<ScoreMergeType::Max>(scores[offset], constant);
    });
  }

  template<bool Scored, typename Write>
  doc_id_t FillImpl(doc_id_t min, doc_id_t max, uint64_t* IRS_RESTRICT mask,
                    Write&& write) {
    SDB_ASSERT(min < max);

    if (_doc >= max) {
      return _doc;
    }

    const auto* const end = std::cend(_docs);
    if (_left_in_leaf != 0) {
      const auto* begin = Behind(end - _left_in_leaf, end, min);
      if (begin == end) {
        _left_in_leaf = 0;
      } else if (_last >= max) {
        const auto* const it = SetUntil(begin, end, min, max, mask, write);
        _left_in_leaf = static_cast<uint32_t>(end - it);
        return _doc = *it;
      } else {
        if (end - begin == kBlock) [[likely]] {
          SetBlock(begin, min, mask, write);
        } else {
          SetRange(begin, end, min, mask, write);
        }
        _left_in_leaf = 0;
      }
    }

    for (;;) {
      if (_left_in_list == 0) {
        return _doc = doc_limits::eof();
      }

      auto& in = In();
      const auto len = std::min(_left_in_list, kBlock);
      const auto base = _last;
      const auto leaf =
        FormatTraits128::ReadTailForFill(len, in, Enc(), _docs, base);
      _left_in_list -= len;
      _last = leaf.max;

      if (leaf.max < min) {
        SkipFreqs(len);
        continue;
      }

      if (leaf.Maskable()) {
        if constexpr (!Scored) {
          if (base >= min) [[likely]] {
            const auto live = FormatTraits128::MaskLeaf(
              leaf, base, len, min, max, mask, std::end(_docs));
            SkipFreqs(len);
            if (live == 0) {
              continue;
            }
            _left_in_leaf = live;
            return _doc = *(std::end(_docs) - live);
          }
        }
        Materialize(leaf, base, len);
      }

      SkipFreqs(len);

      const auto* const begin = Behind(end - len, end, min);
      if (leaf.max < max) {
        if (begin != end) {
          if (end - begin == kBlock) [[likely]] {
            SetBlock(begin, min, mask, write);
          } else {
            SetRange(begin, end, min, mask, write);
          }
        }
        continue;
      }

      const auto* const it = SetUntil(begin, end, min, max, mask, write);
      _left_in_leaf = static_cast<uint32_t>(end - it);
      return _doc = *it;
    }
  }

  doc_id_t FillAnd(doc_id_t min, doc_id_t max, uint64_t* IRS_RESTRICT mask) {
    SDB_ASSERT(min < max);
    const uint64_t limit = max - min;
    AndCursor cursor{.words = mask};

    if (_doc >= max) {
      cursor.Settle(limit);
      return _doc;
    }

    if (_leaf_len != 0 && _last >= min) {
      const auto at = AndLeaf(cursor, min, max);
      if (_last >= max) {
        cursor.Settle(limit);
        return _doc = at;
      }
    }

    for (;;) {
      if (_left_in_list == 0) {
        cursor.Settle(limit);
        return _doc = doc_limits::eof();
      }
      ReadLeaf();
      if (_last < min) {
        continue;
      }
      const auto at = AndLeaf(cursor, min, max);
      if (_last >= max) {
        cursor.Settle(limit);
        return _doc = at;
      }
    }
  }

  doc_id_t FillAndNot(doc_id_t min, doc_id_t max, uint64_t* IRS_RESTRICT mask) {
    SDB_ASSERT(min < max);

    if (_doc >= max) {
      return _doc;
    }

    if (_leaf_len != 0 && _last >= min) {
      const auto at = ClearLeaf(mask, min, max);
      if (_last >= max) {
        return _doc = at;
      }
    }

    for (;;) {
      if (_left_in_list == 0) {
        return _doc = doc_limits::eof();
      }
      ReadLeaf();
      if (_last < min) {
        continue;
      }
      const auto at = ClearLeaf(mask, min, max);
      if (_last >= max) {
        return _doc = at;
      }
    }
  }

 private:
  void ReadLeaf() {
    const auto len = std::min(_left_in_list, kBlock);
    _leaf_base = _last;
    _leaf =
      FormatTraits128::ReadTailForFill(len, In(), Enc(), _docs, _leaf_base);
    _leaf.bitset = StableBitset(_leaf);
    _leaf_len = len;
    _left_in_list -= len;
    _last = _leaf.max;
    SkipFreqs(len);
  }

  IRS_FORCE_INLINE bool Span(doc_id_t min, doc_id_t max, uint64_t& lo,
                             uint64_t& hi) const noexcept {
    const auto first = std::max<doc_id_t>(_leaf_base + 1, min);
    const auto stop = std::min<doc_id_t>(_last + 1, max);
    if (first >= stop) {
      return false;
    }
    lo = first - min;
    hi = stop - 1 - min;
    return true;
  }

  doc_id_t AndLeaf(AndCursor& cursor, doc_id_t min, doc_id_t max) noexcept {
    uint64_t lo = 0;
    uint64_t hi = 0;
    if (!Span(min, max, lo, hi)) {
      return InLeafFrom(max);
    }
    if (_leaf.IsRun()) {
      cursor.Keep(lo, hi);
      return std::max<doc_id_t>(max, _leaf_base + 1);
    }
    if (_leaf.IsBitset()) {
      cursor.And(lo, hi,
                 static_cast<int64_t>(min) - static_cast<int64_t>(_leaf_base),
                 _leaf.bitset, _leaf.words);
      return InLeafFrom(max);
    }
    const auto* const end = std::cend(_docs);
    const auto* it = Behind(end - _leaf_len, end, min);
    it = cursor.AndDocs(lo, hi, it, end, min);
    return it != end ? *it : _last;
  }

  doc_id_t ClearLeaf(uint64_t* IRS_RESTRICT mask, doc_id_t min,
                     doc_id_t max) noexcept {
    uint64_t lo = 0;
    uint64_t hi = 0;
    if (!Span(min, max, lo, hi)) {
      return InLeafFrom(max);
    }
    if (_leaf.IsRun()) {
      ClearInclusive(mask, lo, hi);
      return std::max<doc_id_t>(max, _leaf_base + 1);
    }
    if (_leaf.IsBitset()) {
      const auto delta =
        static_cast<int64_t>(min) - static_cast<int64_t>(_leaf_base);
      const auto first = static_cast<uint32_t>(lo / kBits);
      const auto last = static_cast<uint32_t>(hi / kBits);
      for (auto i = first; i <= last; ++i) {
        auto bits = WordAt(_leaf.bitset, _leaf.words,
                           static_cast<int64_t>(uint64_t{i} * kBits) + delta);
        if (i == first) {
          bits &= ~uint64_t{0} << (lo % kBits);
        }
        if (i == last && hi % kBits != kBits - 1) {
          bits &= ~(~uint64_t{0} << (hi % kBits + 1));
        }
        mask[i] &= ~bits;
      }
      return InLeafFrom(max);
    }
    const auto* const end = std::cend(_docs);
    const auto stop = std::min<doc_id_t>(_last + 1, max);
    const auto* it = end - _leaf_len;
    for (; it != end; ++it) {
      const auto doc = *it;
      if (doc < min) {
        continue;
      }
      if (doc >= stop) {
        break;
      }
      const size_t offset = doc - min;
      UnsetBit(mask[offset / kBits], offset % kBits);
    }
    return it != end ? *it : _last;
  }

  doc_id_t InLeafFrom(doc_id_t target) const noexcept {
    SDB_ASSERT(target <= _last);
    if (_leaf.IsRun()) {
      return std::max<doc_id_t>(target, _leaf_base + 1);
    }
    if (_leaf.IsBitset()) {
      const auto total = uint32_t{_leaf.words} * kBits;
      auto bit = target > _leaf_base ? target - _leaf_base : uint32_t{1};
      while (bit < total) {
        const auto word = bit / kBits;
        const auto bits = _leaf.bitset[word] & (~uint64_t{0} << (bit % kBits));
        if (bits != 0) {
          return static_cast<doc_id_t>(_leaf_base + word * kBits +
                                       std::countr_zero(bits));
        }
        bit = (word + 1) * kBits;
      }
      return _last;
    }
    const auto* const end = std::cend(_docs);
    for (const auto* it = end - _leaf_len; it != end; ++it) {
      if (*it >= target) {
        return *it;
      }
    }
    return _last;
  }

  void Materialize(const FormatTraits128::FillLeaf& leaf, doc_id_t base,
                   uint32_t len) noexcept {
    auto* const out = std::end(_docs) - len;
    if (leaf.IsRun()) {
      FormatTraits128::FillSameDelta(out, len, base, 1);
      return;
    }
    SDB_ASSERT(leaf.IsBitset());
    FormatTraits128::MaterializeBitsetFrom(base, leaf.bitset, 0, leaf.bitset[0],
                                           leaf.words, out);
  }

  template<typename Write>
  IRS_FORCE_INLINE static void SetRange(const doc_id_t* begin,
                                        const doc_id_t* end, doc_id_t min,
                                        uint64_t* IRS_RESTRICT mask,
                                        Write&& write) noexcept {
    for (; begin != end; ++begin) {
      const size_t offset = *begin - min;
      SetBit(mask[offset / kBits], offset % kBits);
      write(offset);
    }
  }

  template<typename Write>
  IRS_FORCE_INLINE static const doc_id_t* SetUntil(const doc_id_t* begin,
                                                   const doc_id_t* end,
                                                   doc_id_t min, doc_id_t max,
                                                   uint64_t* IRS_RESTRICT mask,
                                                   Write&& write) noexcept {
    for (; begin != end; ++begin) {
      const auto doc = *begin;
      if (doc >= max) {
        break;
      }
      const size_t offset = doc - min;
      SetBit(mask[offset / kBits], offset % kBits);
      write(offset);
    }
    return begin;
  }

  template<typename Write>
  IRS_FORCE_INLINE static void SetBlock(const doc_id_t* begin, doc_id_t min,
                                        uint64_t* IRS_RESTRICT mask,
                                        Write&& write) noexcept {
    VisitDocs<doc_limits::kBlockSize>(
      doc_limits::kBlockSize, [&](uint32_t i) IRS_FORCE_INLINE {
        const size_t offset = begin[i] - min;
        SetBit(mask[offset / kBits], offset % kBits);
        write(offset);
      });
  }

  FormatTraits128::FillLeaf _leaf{};
  doc_id_t _leaf_base = 0;
  uint32_t _leaf_len = 0;
};

}  // namespace irs::search
