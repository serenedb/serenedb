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

#ifdef __AVX2__
#include <immintrin.h>
#endif

#include <algorithm>
#include <bit>
#include <cstdint>
#include <span>

#include "iresearch/formats/posting/common.hpp"
#include "iresearch/formats/posting_meta.hpp"
#include "iresearch/search/detail/posting_leaf.hpp"
#include "iresearch/search/detail/window.hpp"
#include "iresearch/store/data_input.hpp"
#include "iresearch/utils/bit_utils.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::detail {

inline constexpr LeafShape kFillShape{.cursor = true, .holes = true};

template<typename InputType>
class PostingFill : public PostingLeaf<InputType, kFillShape> {
  using Base = PostingLeaf<InputType, kFillShape>;
  using Kind = block_io::FillLeaf::Kind;

  using Base::_cursor;
  using Base::_docs;
  using Base::_last;
  using Base::_left_in_list;
  using Base::kBits;
  using Base::kBlock;
  using Base::ReadLeafFill;
  using Base::SeekToLeaf;

 public:
  PostingFill() = default;

  PostingFill(const PostingMeta& meta, const IndexInput& doc_in,
              IndexFeatures layout, bool has_score_bounds, bool has_freq) {
    Prepare(meta, doc_in, layout, has_score_bounds, has_freq);
  }

  void Prepare(const PostingMeta& meta, const IndexInput& doc_in,
               IndexFeatures layout, bool has_score_bounds, bool has_freq) {
    SDB_ASSERT(meta.docs_count != 0);
    this->SetFreqLen(has_freq);

    if (meta.docs_count == 1) {
      _cursor.base = this->SetSingle(meta) - 1;
      _at = kBlock - 1;
      _len = 1;
      return;
    }

    this->OpenInput(meta, doc_in, has_score_bounds);
    this->ArmWalk(meta, layout, has_score_bounds);
  }

  doc_id_t FillOr(doc_id_t min, doc_id_t max, uint64_t* IRS_RESTRICT mask) {
    Walk(
      min, max,
      [&](const doc_id_t* docs, uint32_t n)
        IRS_FORCE_INLINE { Scatter<false>(docs, n, min, mask, [](size_t) {}); },
      [&](uint32_t lo, uint32_t hi)
        IRS_FORCE_INLINE { SetBitRange(mask, lo, hi); },
      [&] {
        if (Inside(min, max)) {
          OrBitsetAt(mask, _cursor.base + 1 - min, _bitset, _words,
                     _bitset[_words - 1]);
          return;
        }
        Bits(min, max, [&](uint32_t w, uint64_t bits) IRS_FORCE_INLINE {
          mask[w] |= bits;
        });
      });
    return Seek(max);
  }

  doc_id_t FillAndNot(doc_id_t min, doc_id_t max, uint64_t* IRS_RESTRICT mask) {
    Walk(
      min, max,
      [&](const doc_id_t* docs, uint32_t n)
        IRS_FORCE_INLINE { Scatter<true>(docs, n, min, mask, [](size_t) {}); },
      [&](uint32_t lo, uint32_t hi)
        IRS_FORCE_INLINE { ClearBitRange(mask, lo, hi); },
      [&] {
        if (Inside(min, max)) {
          AndNotBitsetAt(mask, _cursor.base + 1 - min, _bitset, _words,
                         _bitset[_words - 1]);
          return;
        }
        Bits(min, max, [&](uint32_t w, uint64_t bits) IRS_FORCE_INLINE {
          mask[w] &= ~bits;
        });
      });
    return Seek(max);
  }

  doc_id_t FillSum(doc_id_t min, doc_id_t max, uint64_t* IRS_RESTRICT mask,
                   score_t* IRS_RESTRICT scores, score_t constant) {
    return FillScored(min, max, mask, [=](size_t offset) IRS_FORCE_INLINE {
      Merge<ScoreMergeType::Sum>(scores[offset], constant);
    });
  }

  doc_id_t FillMax(doc_id_t min, doc_id_t max, uint64_t* IRS_RESTRICT mask,
                   score_t* IRS_RESTRICT scores, score_t constant) {
    return FillScored(min, max, mask, [=](size_t offset) IRS_FORCE_INLINE {
      Merge<ScoreMergeType::Max>(scores[offset], constant);
    });
  }

  doc_id_t FillAnd(doc_id_t min, doc_id_t max, uint64_t* IRS_RESTRICT mask) {
    SDB_ASSERT(min < max);
    AndCursor cursor{.words = mask};
    if (Reach(min)) {
      for (;;) {
        const auto base = _cursor.base;
        if (base + 1 >= max) {
          break;
        }
        const uint64_t lo = std::max<doc_id_t>(min, base + 1) - min;
        const uint64_t hi = std::min<doc_id_t>(_last, max - 1) - min;
        if (_kind == Kind::Docs) {
          const auto first = SeekIn(_at, min);
          const auto* const end = _docs.data() + kBlock;
          const auto* const it =
            cursor.AndDocs(lo, hi, _docs.data() + first, end, min);
          _at = static_cast<uint32_t>(it - _docs.data());
        } else if (_kind == Kind::Bitset) {
          cursor.And(lo, hi, static_cast<int64_t>(min) - base - 1, _bitset,
                     _words);
        } else {
          cursor.Keep(lo, hi);
        }
        if (_last + 1 >= max || _left_in_list == 0) {
          break;
        }
        Load(_last);
      }
    }
    cursor.Settle(max - min);
    return Seek(max);
  }

  uint32_t Keep(const doc_id_t* in, uint32_t n, doc_id_t* out) {
    return Sift<true>(in, n, out);
  }

  uint32_t Drop(const doc_id_t* in, uint32_t n, doc_id_t* out) {
    return Sift<false>(in, n, out);
  }

  IRS_FORCE_INLINE doc_id_t Seek(doc_id_t target) {
    if (!Reach(target)) [[unlikely]] {
      return doc_limits::eof();
    }
    if (_kind == Kind::Docs) [[likely]] {
      _at = SeekIn(_at, target);
      return _docs[_at];
    }
    if (_kind == Kind::Run) {
      return std::max<doc_id_t>(target, _cursor.base + 1);
    }
    return NextBit(target);
  }

  IRS_FORCE_INLINE uint64_t FillCost(doc_id_t min, doc_id_t max) {
    if (!Reach(min)) {
      return 0;
    }
    if (_kind == Kind::Docs) {
      return uint64_t{_len} * (max - min) / (_last - _cursor.base);
    }
    return (max - min + kBits - 1) / kBits;
  }

  IRS_FORCE_INLINE bool Sparse() const noexcept {
    return _kind == Kind::Docs &&
           (_len < kWindowDocs / kSparseInverse ||
            uint64_t{_len} * kSparseInverse < _last - _cursor.base);
  }

  IRS_FORCE_INLINE std::span<const doc_id_t> Rest(doc_id_t end) const noexcept {
    SDB_ASSERT(_kind == Kind::Docs);
    SDB_ASSERT(_at < kBlock);
    const auto stop = _last < end ? kBlock : SeekIn(_at, end);
    return {_docs.data() + _at, _docs.data() + stop};
  }

 private:
  IRS_FORCE_INLINE bool Reach(doc_id_t target) {
    if (target <= _last) [[likely]] {
      return true;
    }
    return SeekToLeaf(target,
                      [this](doc_id_t prev) IRS_FORCE_INLINE { Load(prev); });
  }

  uint32_t Landings(const doc_id_t* in, uint32_t n, uint32_t* blocks) {
    auto& walk = this->_walk;
    if (!walk.Armed() || walk.Pending() || n < 2 || in[n - 1] <= _last ||
        (in[n - 1] - in[0]) / (n - 1) <= _last - _cursor.base) {
      return 0;
    }
    const auto& index = walk.Index();
    const auto size = index.Size();
    auto block = walk.NextBlock(_left_in_list);
    doc_id_t upper = _last;
    uint32_t landings = 0;
    for (uint32_t i = 0; i != n && landings != kBlock; ++i) {
      if (in[i] <= upper) {
        continue;
      }
      const auto next = block;
      if (block == size || index.Last(block) < in[i]) {
        block = index.Find(block, in[i]);
      }
      if (block == size) {
        break;
      }
      blocks[landings++] = block;
      if constexpr (InputType::kVolatileAlways) {
        if (block != next) {
          const auto* const at = this->In().At(walk.BlockStart(block));
          __builtin_prefetch(at, 0, 2);
          __builtin_prefetch(at + ABSL_CACHELINE_SIZE, 0, 2);
        }
      }
      upper = index.Last(block);
      ++block;
    }
    return landings;
  }

  void LandOn(uint32_t block) {
    auto& walk = this->_walk;
    if (block == walk.NextBlock(_left_in_list)) {
      Load(_last);
      return;
    }
    _left_in_list = walk.MoveTo(block);
    const auto landing = walk.Landing();
    this->In().Seek(landing.doc_ptr);
    Load(landing.doc);
  }

  IRS_FORCE_INLINE void Load(doc_id_t prev) {
    const auto read = ReadLeafFill(prev);
    _kind = read.leaf.kind;
    _bitset = read.bitset;
    _words = read.leaf.words;
    _len = read.len;
    _at = kBlock - read.len;
  }

  IRS_FORCE_INLINE uint32_t SeekIn(uint32_t at,
                                   doc_id_t target) const noexcept {
    SDB_ASSERT(at < kBlock);
    SDB_ASSERT(_docs[kBlock - 1] >= target);
    if (_len == kBlock) [[likely]] {
      return static_cast<uint32_t>(
        BranchlessLowerBound<kBlock>(_docs.data(), target) - _docs.data());
    }
#ifdef __AVX2__
    const auto key = _mm256_set1_epi32(static_cast<int32_t>(target));
    for (;;) {
      const auto docs =
        _mm256_loadu_si256(reinterpret_cast<const __m256i*>(_docs.data() + at));
      const auto below = static_cast<uint32_t>(
        _mm256_movemask_ps(_mm256_castsi256_ps(_mm256_cmpgt_epi32(key, docs))));
      const auto step = static_cast<uint32_t>(std::countr_one(below));
      at += step;
      if (step != 8) {
        return at;
      }
    }
#else
    while (_docs[at] < target) {
      ++at;
    }
    return at;
#endif
  }

  IRS_FORCE_INLINE bool HasBit(doc_id_t doc) const noexcept {
    const uint32_t bit = doc - _cursor.base - 1;
    const uint64_t word = _bitset[std::min(bit / kBits, _words - 1)];
    return (bit < _words * kBits) & (((word >> (bit % kBits)) & 1) != 0);
  }

  doc_id_t NextBit(doc_id_t target) const noexcept {
    const auto base = _cursor.base;
    const uint32_t bit = target > base ? target - base - 1 : 0;
    auto w = bit / kBits;
    auto word = uint64_t{_bitset[w]} & (~uint64_t{0} << (bit % kBits));
    while (word == 0) {
      word = _bitset[++w];
    }
    return base + 1 + w * kBits + static_cast<uint32_t>(std::countr_zero(word));
  }

  template<typename OnDocs, typename OnRun, typename OnBits>
  IRS_FORCE_INLINE void Walk(doc_id_t min, doc_id_t max, OnDocs&& on_docs,
                             OnRun&& on_run, OnBits&& on_bits) {
    SDB_ASSERT(min < max);
    if (!Reach(min)) {
      return;
    }
    for (;;) {
      const auto base = _cursor.base;
      if (base + 1 >= max) {
        return;
      }
      if (_kind == Kind::Docs) {
        const auto first = _docs[_at] >= min ? _at : SeekIn(_at, min);
        const auto stop = _last < max ? kBlock : SeekIn(first, max);
        if (first != stop) {
          on_docs(_docs.data() + first, stop - first);
        }
        _at = stop;
      } else if (_kind == Kind::Bitset) {
        on_bits();
      } else {
        const doc_id_t lo = std::max<doc_id_t>(min, base + 1);
        const auto hi =
          static_cast<doc_id_t>(std::min<uint64_t>(uint64_t{_last} + 1, max));
        on_run(lo - min, hi - min);
      }
      if (_last + 1 >= max || _left_in_list == 0) {
        return;
      }
      Load(_last);
    }
  }

  template<typename Write>
  doc_id_t FillScored(doc_id_t min, doc_id_t max, uint64_t* IRS_RESTRICT mask,
                      Write&& write) {
    Walk(
      min, max,
      [&](const doc_id_t* docs, uint32_t n)
        IRS_FORCE_INLINE { Scatter<false>(docs, n, min, mask, write); },
      [&](uint32_t lo, uint32_t hi) IRS_FORCE_INLINE {
        SetBitRange(mask, lo, hi);
        for (auto offset = lo; offset != hi; ++offset) {
          write(offset);
        }
      },
      [&] {
        Bits(min, max, [&](uint32_t w, uint64_t bits) IRS_FORCE_INLINE {
          mask[w] |= bits;
          for (; bits != 0; bits = PopBit(bits)) {
            write(size_t{w} * kBits +
                  static_cast<size_t>(std::countr_zero(bits)));
          }
        });
      });
    return Seek(max);
  }

  template<bool Clear, typename Write>
  IRS_FORCE_INLINE static void Scatter(const doc_id_t* IRS_RESTRICT docs,
                                       uint32_t n, doc_id_t min,
                                       uint64_t* IRS_RESTRICT words,
                                       Write&& write) noexcept {
    const auto mark = [&](uint32_t i) IRS_FORCE_INLINE {
      const size_t offset = docs[i] - min;
      if constexpr (Clear) {
        UnsetBit(words[offset / kBits], offset % kBits);
      } else {
        SetBit(words[offset / kBits], offset % kBits);
      }
      write(offset);
    };
    const auto quarter = n / 4;
    for (uint32_t i = 0; i != quarter; ++i) {
      mark(i);
      mark(quarter + i);
      mark(2 * quarter + i);
      mark(3 * quarter + i);
    }
    for (auto i = 4 * quarter; i != n; ++i) {
      mark(i);
    }
  }

  IRS_FORCE_INLINE bool Inside(doc_id_t min, doc_id_t max) const noexcept {
    return _cursor.base >= min &&
           uint64_t{_cursor.base} + 1 + uint64_t{_words} * kBits <= max;
  }

  IRS_FORCE_INLINE uint64_t LeafWord(int64_t i) const noexcept {
    const auto clamped =
      static_cast<uint32_t>(std::clamp<int64_t>(i, 0, _words - 1));
    const uint64_t word = _bitset[clamped];
    return i == clamped ? word : 0;
  }

  template<typename Visit>
  IRS_FORCE_INLINE void Bits(doc_id_t min, doc_id_t max,
                             Visit&& visit) const noexcept {
    const auto base = _cursor.base;
    const doc_id_t lo = std::max<doc_id_t>(min, base + 1) - min;
    const auto hi =
      static_cast<uint32_t>(std::min<uint64_t>(uint64_t{_last} + 1, max) - min);
    SDB_ASSERT(lo < hi);
    const auto first = lo / kBits;
    const auto last = (hi - 1) / kBits;
    const auto at = static_cast<int64_t>(uint64_t{first} * kBits) +
                    static_cast<int64_t>(min) - base - 1;
    const auto shift = static_cast<uint32_t>(at & (kBits - 1));
    auto i = at >> 6;
    const auto head = ~uint64_t{0} << (lo % kBits);
    const auto tail = ~uint64_t{0} >> (kBits - 1 - (hi - 1) % kBits);
    const auto clip = [&](uint32_t w, uint64_t bits) IRS_FORCE_INLINE {
      bits &= w == first ? head : ~uint64_t{0};
      bits &= w == last ? tail : ~uint64_t{0};
      return bits;
    };
    if (shift == 0) {
      for (auto w = first; w <= last; ++w, ++i) {
        visit(w, clip(w, LeafWord(i)));
      }
      return;
    }
    auto low = LeafWord(i);
    for (auto w = first; w <= last; ++w) {
      const auto high = LeafWord(++i);
      visit(w, clip(w, (low >> shift) | (high << (kBits - shift))));
      low = high;
    }
  }

  template<bool Keep>
  IRS_FORCE_INLINE uint32_t Sift(const doc_id_t* in, uint32_t n,
                                 doc_id_t* out) {
    uint32_t blocks[kBlock];
    const auto landings = n != 0 ? Landings(in, n, blocks) : 0;
    uint32_t landed = 0;
    uint32_t kept = 0;
    uint32_t i = 0;
    while (i != n) {
      if (in[i] > _last && landed != landings) {
        LandOn(blocks[landed++]);
      } else if (!Reach(in[i])) [[unlikely]] {
        if constexpr (!Keep) {
          for (; i != n; ++i) {
            out[kept++] = in[i];
          }
        }
        return kept;
      }
      const auto last = _last;
      if (_kind == Kind::Docs) [[likely]] {
        auto at = _at;
        do {
          const auto doc = in[i];
          at = SeekIn(at, doc);
          out[kept] = doc;
          kept += static_cast<uint32_t>((_docs[at] == doc) == Keep);
        } while (++i != n && in[i] <= last);
        _at = at;
      } else if (_kind == Kind::Bitset) {
        do {
          const auto doc = in[i];
          out[kept] = doc;
          kept += static_cast<uint32_t>(HasBit(doc) == Keep);
        } while (++i != n && in[i] <= last);
      } else {
        const auto base = _cursor.base;
        do {
          const auto doc = in[i];
          out[kept] = doc;
          kept += static_cast<uint32_t>((doc > base) == Keep);
        } while (++i != n && in[i] <= last);
      }
    }
    return kept;
  }

  const UnalignedWord* _bitset = nullptr;
  uint32_t _words = 0;
  uint32_t _len = 0;
  uint32_t _at = kBlock;
  Kind _kind = Kind::Docs;
};

}  // namespace irs::detail
