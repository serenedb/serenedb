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
#include <cstring>
#include <span>

#include "basics/down_cast.h"
#include "basics/empty.hpp"
#include "basics/shared.hpp"
#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/error/error.hpp"
#include "iresearch/formats/posting/common.hpp"
#include "iresearch/formats/posting/format_block_128.hpp"
#include "iresearch/formats/posting_meta.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/column_collector.hpp"
#include "iresearch/search/common/enc_buf.hpp"
#include "iresearch/search/common/score_args.hpp"
#include "iresearch/search/common/score_provider.hpp"
#include "iresearch/search/common/skip_walk.hpp"
#include "iresearch/search/scorer.hpp"
#include "iresearch/store/data_input.hpp"
#include "iresearch/utils/attribute_provider.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::search {

struct LeafShape {
  bool scored = false;
  bool defer = false;
  bool freqs = false;
  bool gather = false;
  bool cursor = false;
  bool slack = false;
  bool enc = false;
  bool delta = false;
};

inline constexpr LeafShape kWindowShape{.slack = true, .delta = true};

inline constexpr LeafShape kWindowScoredShape{
  .scored = true,
  .freqs = true,
  .slack = true,
  .enc = true,
};

inline constexpr LeafShape kCursorShape{
  .cursor = true,
  .slack = true,
  .delta = true,
};

inline constexpr LeafShape kCursorScoredShape{
  .defer = true,
  .freqs = true,
  .gather = true,
  .cursor = true,
  .slack = true,
  .delta = true,
};

inline constexpr LeafShape kProbeShape{.cursor = true};

inline constexpr LeafShape kProbeScoredShape{
  .defer = true,
  .freqs = true,
  .gather = true,
  .cursor = true,
};

struct FreqLen {
  uint32_t value = 0;
};

struct LeafScore {
  ColumnArgsFetcher* fetcher = nullptr;
  ScoreFunction score;
};

template<typename InputType>
struct LeafCursor {
  SkipWalk<InputType> walk;
  doc_id_t base = 0;
  doc_id_t upper_bound = doc_limits::eof();
};

template<typename InputType, LeafShape Shape>
class PostingLeaf {
 public:
  PostingLeaf() = default;

  PostingLeaf(const PostingLeaf&) = delete;
  PostingLeaf& operator=(const PostingLeaf&) = delete;
  PostingLeaf(PostingLeaf&&) = delete;
  PostingLeaf& operator=(PostingLeaf&&) = delete;

  IRS_FORCE_INLINE doc_id_t Advance() {
    if (_left_in_leaf == 0) [[unlikely]] {
      if (_left_in_list == 0) [[unlikely]] {
        return _doc = doc_limits::eof();
      }
      ReadLeafDelta(_last);
    }
    _doc = *(std::end(_docs) - _left_in_leaf);
    --_left_in_leaf;
    return _doc;
  }

  IRS_FORCE_INLINE doc_id_t Probe(doc_id_t target) {
    static_assert(Shape.cursor && Shape.delta);
    if (target <= _doc) [[unlikely]] {
      return _doc;
    }

    if (_last < target && !ReadToDelta(target)) [[unlikely]] {
      _left_in_leaf = 0;
      return _doc = doc_limits::eof();
    }

    if (_left_in_list != 0) [[likely]] {
      const auto* it =
        BranchlessLowerBound<doc_limits::kBlockSize>(std::begin(_docs), target);
      _left_in_leaf = static_cast<uint32_t>(std::end(_docs) - it) - 1;
      return _doc = *it;
    }

    return Scan(target);
  }

  IRS_FORCE_INLINE doc_id_t Seek(doc_id_t target) {
    static_assert(Shape.cursor && Shape.delta);
    if (target <= _doc) [[unlikely]] {
      return _doc;
    }

    if (_last < target && !ReadToDelta(target)) [[unlikely]] {
      _left_in_leaf = 0;
      return _doc = doc_limits::eof();
    }

    return Scan(target);
  }

 protected:
  static constexpr bool kEnc = Shape.enc || !InputType::kVolatileAlways;
  static constexpr uint32_t kBlock = doc_limits::kBlockSize;
  static constexpr auto kBits = BitsRequired<uint64_t>();

  IRS_FORCE_INLINE InputType& In() const noexcept {
    return sdb::basics::downCast<InputType>(*_in);
  }

  IRS_FORCE_INLINE uint32_t* Enc() noexcept {
    if constexpr (kEnc) {
      return _enc.data;
    } else {
      return nullptr;
    }
  }

  IRS_FORCE_INLINE score_t* Scores() noexcept {
    static_assert(Shape.enc && sizeof(score_t) == sizeof(uint32_t));
    return reinterpret_cast<score_t*>(_enc.data);
  }

  IRS_FORCE_INLINE const score_t* Scores() const noexcept {
    static_assert(Shape.enc && sizeof(score_t) == sizeof(uint32_t));
    return reinterpret_cast<const score_t*>(_enc.data);
  }

  IRS_FORCE_INLINE static const doc_id_t* Behind(const doc_id_t* begin,
                                                 const doc_id_t* end,
                                                 doc_id_t min) noexcept {
    while (begin != end && *begin < min) {
      ++begin;
    }
    return begin;
  }

  IRS_FORCE_INLINE const uint64_t* StableBitset(
    const FormatTraits128::FillLeaf& leaf) noexcept {
    if constexpr (InputType::kVolatileAlways) {
      return leaf.bitset;
    } else {
      if (!leaf.IsBitset()) {
        return leaf.bitset;
      }
      SDB_ASSERT(leaf.words <=
                 _docs.size() * sizeof(doc_id_t) / sizeof(uint64_t));
      std::memcpy(_docs.begin(), leaf.bitset,
                  size_t{leaf.words} * sizeof(uint64_t));
      return reinterpret_cast<const uint64_t*>(_docs.begin());
    }
  }

  void OpenInput(const PostingMeta& meta, const IndexInput& doc_in,
                 bool bounds) {
    _in = doc_in.Reopen();
    if (!_in) [[unlikely]] {
      throw IoError{"failed to reopen document input"};
    }
    auto& in = In();
    in.Seek(meta.doc_start);
    if (meta.docs_count < kBlock) {
      SkipScoreBounds(bounds, in);
    }
    _left_in_list = meta.docs_count;
  }

  void ArmWalk(const PostingMeta& meta, IndexFeatures layout, bool bounds) {
    static_assert(Shape.cursor);
    if (meta.docs_count > kBlock) {
      const auto skip = ToSkipLayout(layout);
      _cursor.walk.Arm(meta,
                       {.bounds = bounds, .pos = skip.pos, .offs = skip.offs});
      _cursor.upper_bound = doc_limits::invalid();
    }
  }

  IRS_FORCE_INLINE void SetFreqLen(bool has_freq) noexcept {
    static_assert(!Shape.freqs);
    _freq_len.value = has_freq ? kBlock : 0;
  }

  IRS_FORCE_INLINE doc_id_t SetSingle(const PostingMeta& meta) noexcept {
    const auto doc = doc_limits::min() + meta.doc_delta;
    *(std::end(_docs) - 1) = doc;
    if constexpr (Shape.freqs) {
      *(std::end(_freqs.data) - 1) = meta.freq;
    }
    _last = doc;
    _left_in_leaf = 1;
    return doc;
  }

  void MakeScore(const SubReader& segment, const TermReader& field,
                 const ScoreArgs& args) {
    static_assert(Shape.scored && Shape.freqs);
    _score.fetcher = args.fetcher;
    _provider.freq.value = _freqs.data;
    SDB_ASSERT(args.scorer != nullptr);
    _score.score = args.scorer->PrepareScorer({
      .segment = segment,
      .field = field.meta(),
      .doc_attrs = _provider,
      .fetcher = args.fetcher,
      .stats = args.stats,
      .boost = args.boost,
    });
  }

  IRS_FORCE_INLINE void SetRecipe(const SubReader& segment,
                                  const TermReader& field,
                                  const ScoreArgs& args) noexcept {
    static_assert(Shape.defer);
    _recipe.segment = &segment;
    _recipe.field = &field;
    _recipe.args = args;
  }

  ScoreFunction MakeDeferredScore() {
    static_assert(Shape.defer && Shape.gather);
    _provider.freq.value = _gather.data;
    SDB_ASSERT(_recipe.segment != nullptr && _recipe.field != nullptr);
    SDB_ASSERT(_recipe.args.scorer != nullptr);
    return _recipe.args.scorer->PrepareScorer({
      .segment = *_recipe.segment,
      .field = _recipe.field->meta(),
      .doc_attrs = _provider,
      .fetcher = _recipe.args.fetcher,
      .stats = _recipe.args.stats,
      .boost = _recipe.args.boost,
    });
  }

  IRS_FORCE_INLINE void SkipFreqs(uint32_t len) {
    static_assert(!Shape.freqs);
    SDB_ASSERT(len != 0);
    if (len == _freq_len.value) {
      FormatTraits128::SkipBlock(In());
    }
  }

  IRS_FORCE_INLINE void TakeFreqs(uint32_t len) {
    if constexpr (Shape.freqs) {
      FormatTraits128::ReadTail(len, In(), Enc(), _freqs.data);
    } else {
      SkipFreqs(len);
    }
  }

  void ReadLeafDelta(doc_id_t prev) {
    static_assert(Shape.delta);
    if constexpr (Shape.cursor) {
      _cursor.base = prev;
    }
    auto& in = In();
    if (_left_in_list >= kBlock) [[likely]] {
      FormatTraits128::ReadBlockDelta(in, Enc(), _docs, prev);
      _left_in_leaf = kBlock;
      _left_in_list -= kBlock;
      TakeFreqs(kBlock);
    } else {
      const auto tail = _left_in_list;
      FormatTraits128::ReadTailDelta(tail, in, Enc(), _docs, prev);
      _left_in_leaf = tail;
      _left_in_list = 0;
      TakeFreqs(tail);
    }
    _last = *(std::end(_docs) - 1);
  }

  bool ReadLeafBelow(uint32_t len, doc_id_t min) {
    static_assert(Shape.scored && Shape.freqs && Shape.enc);
    auto& in = In();
    FormatTraits128::ReadTailDelta(len, in, _enc.data, _docs, _last);
    _last = *(std::cend(_docs) - 1);
    if (_last < min) {
      FormatTraits128::SkipTail(len, in);
      return false;
    }
    FormatTraits128::ReadTail(len, in, _enc.data, _freqs.data);
    ScoreLeaf(kBlock - len, len);
    return true;
  }

  void ScoreLeaf(uint32_t offset, uint32_t len) {
    static_assert(Shape.scored && Shape.freqs && Shape.enc);
    const auto* const docs = _docs + offset;
    if (len == kBlock) {
      if (_score.fetcher != nullptr) {
        _score.fetcher->FetchPostingBlock(
          std::span<const doc_id_t, kBlock>{docs, kBlock});
      }
      _score.score.ScorePostingBlock(Scores());
      return;
    }
    _provider.freq.value = _freqs.data + offset;
    if (_score.fetcher != nullptr) {
      _score.fetcher->Fetch(std::span<const doc_id_t>{docs, len});
    }
    _score.score.Score(Scores() + offset, static_cast<scores_size_t>(len));
    _provider.freq.value = _freqs.data;
  }

  template<ScoreMergeType MergeType, bool Counts>
  IRS_FORCE_INLINE static void AddDoc(doc_id_t doc, score_t score, doc_id_t min,
                                      uint32_t* IRS_RESTRICT counts,
                                      uint64_t* IRS_RESTRICT mask,
                                      score_t* IRS_RESTRICT window) noexcept {
    const size_t offset = doc - min;
    if constexpr (Counts) {
      ++counts[offset];
    }
    SetBit(mask[offset / kBits], offset % kBits);
    irs::Merge<MergeType>(window[offset], score);
  }

  template<ScoreMergeType MergeType, bool Counts>
  IRS_FORCE_INLINE void AddWhole(const doc_id_t* begin, const doc_id_t* end,
                                 doc_id_t min, uint32_t* IRS_RESTRICT counts,
                                 uint64_t* IRS_RESTRICT mask,
                                 score_t* IRS_RESTRICT window) const noexcept {
    const auto* const scores = Scores() + (begin - std::cbegin(_docs));
    const auto len = static_cast<uint32_t>(end - begin);
    if (len == kBlock) [[likely]] {
      VisitDocs<doc_limits::kBlockSize>(
        doc_limits::kBlockSize, [&](uint32_t i) IRS_FORCE_INLINE {
          AddDoc<MergeType, Counts>(begin[i], scores[i], min, counts, mask,
                                    window);
        });
    } else {
      for (uint32_t i = 0; i != len; ++i) {
        AddDoc<MergeType, Counts>(begin[i], scores[i], min, counts, mask,
                                  window);
      }
    }
  }

  template<ScoreMergeType MergeType, bool Counts>
  IRS_FORCE_INLINE const doc_id_t* AddUntil(
    const doc_id_t* begin, const doc_id_t* end, doc_id_t min, doc_id_t max,
    uint32_t* IRS_RESTRICT counts, uint64_t* IRS_RESTRICT mask,
    score_t* IRS_RESTRICT window) const noexcept {
    const auto* scores = Scores() + (begin - std::cbegin(_docs));
    for (; begin != end && *begin < max; ++begin, ++scores) {
      AddDoc<MergeType, Counts>(*begin, *scores, min, counts, mask, window);
    }
    SDB_ASSERT(begin != end);
    return begin;
  }

  doc_id_t Scan(doc_id_t target) {
    for (auto left = _left_in_leaf; left != 0; --left) {
      const auto doc = *(std::end(_docs) - left);
      if (target <= doc) {
        _left_in_leaf = left - 1;
        return _doc = doc;
      }
    }

    _left_in_leaf = 0;
    return _doc = doc_limits::eof();
  }

  template<typename Read>
  IRS_NO_INLINE bool SeekToLeaf(doc_id_t target, Read&& read) {
    static_assert(Shape.cursor);
    const auto span = _last - _cursor.base;
    const bool avoid_seek =
      target - _last <= span || target <= _cursor.upper_bound;

    if (avoid_seek) [[unlikely]] {
      if (_left_in_list == 0) [[unlikely]] {
        return false;
      }
      read(_last);
      if (target <= _last) [[likely]] {
        return true;
      }
      if (_left_in_list == 0) [[unlikely]] {
        return false;
      }
    }

    const auto left = _cursor.walk.Seek(target, *_in);
    _cursor.upper_bound = _cursor.walk.UpperBound();
    if (left == 0) [[unlikely]] {
      return false;
    }
    _left_in_list = left;
    In().Seek(_cursor.walk.Landing().doc_ptr);
    read(_cursor.walk.Landing().doc);
    return true;
  }

  IRS_FORCE_INLINE bool ReadToDelta(doc_id_t target) {
    return SeekToLeaf(
      target, [this](doc_id_t prev) IRS_FORCE_INLINE { ReadLeafDelta(prev); });
  }

  struct FillRead {
    FormatTraits128::FillLeaf leaf;
    const uint64_t* bitset;
    uint32_t len;
  };

  IRS_FORCE_INLINE FillRead ReadLeafFill(doc_id_t prev) {
    static_assert(Shape.cursor && !Shape.delta);
    auto& in = In();
    const auto len = std::min(_left_in_list, kBlock);
    const auto leaf =
      FormatTraits128::ReadTailForFill(len, in, Enc(), _docs, prev);
    _left_in_list -= len;
    const auto* const bitset = StableBitset(leaf);
    TakeFreqs(len);
    _cursor.base = prev;
    _last = leaf.max;
    return {.leaf = leaf, .bitset = bitset, .len = len};
  }

  [[no_unique_address]] utils::Need<kEnc, EncBuf> _enc;
  [[no_unique_address]] utils::Need<Shape.freqs, FreqBuf> _freqs;
  [[no_unique_address]] utils::Need<Shape.gather, GatherBuf> _gather;
  SlackBuf<doc_id_t, doc_limits::kBlockSize,
           Shape.slack ? doc_limits::kDocsSlack : 0>
    _docs;
  IndexInput::ptr _in;
  doc_id_t _doc = doc_limits::invalid();
  doc_id_t _last = doc_limits::invalid();
  uint32_t _left_in_leaf = 0;
  uint32_t _left_in_list = 0;
  [[no_unique_address]] utils::Need<!Shape.freqs, FreqLen> _freq_len;
  [[no_unique_address]] utils::Need<Shape.scored, LeafScore> _score;
  [[no_unique_address]] utils::Need<Shape.scored || Shape.defer, LeafProvider>
    _provider;
  [[no_unique_address]] utils::Need<Shape.defer, LeafRecipe> _recipe;
  [[no_unique_address]] utils::Need<Shape.cursor, LeafCursor<InputType>>
    _cursor;
};

}  // namespace irs::search
