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
#include <array>
#include <limits>
#include <memory>
#include <span>
#include <vector>

#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/formats/posting/block_index.hpp"
#include "iresearch/formats/posting/block_io.hpp"
#include "iresearch/formats/posting/common.hpp"
#include "iresearch/formats/posting/doc_input.hpp"
#include "iresearch/formats/posting_meta.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/detail/column_collector.hpp"
#include "iresearch/search/detail/enc_buf.hpp"
#include "iresearch/search/detail/landings.hpp"
#include "iresearch/search/detail/masked_leaf.hpp"
#include "iresearch/search/detail/posting_leaf.hpp"
#include "iresearch/search/detail/score_filter.hpp"
#include "iresearch/search/scorers/score_args.hpp"
#include "iresearch/search/scorers/score_provider.hpp"
#include "iresearch/search/scorers/scorer.hpp"
#include "iresearch/utils/attribute_provider.hpp"
#include "iresearch/utils/bit_utils.hpp"
#include "iresearch/utils/lower_bound.hpp"
#include "iresearch/utils/shared.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::top {

template<typename RunScore, typename BlockScore>
IRS_FORCE_INLINE uint32_t FirstCompetitive(const BlockIndex& index, uint32_t b,
                                           score_t threshold,
                                           RunScore&& run_score,
                                           BlockScore&& block_score) {
  const auto n = index.Size();
  while (b != n) {
    const auto r = b / BlockIndex::kRun;
    if (run_score(r) <= threshold) {
      b = std::min(n, (r + 1) * BlockIndex::kRun);
      continue;
    }
    if (block_score(b) > threshold) {
      break;
    }
    ++b;
  }
  return b;
}

inline constexpr detail::LeafShape kPrunedShape{
  .scored = true,
  .defer = true,
  .freqs = true,
  .enc = true,
  .delta = true,
};

template<typename InputType, bool Standalone>
class PrunedLeafBase : protected detail::PostingLeaf<InputType, kPrunedShape> {
  using Base = detail::PostingLeaf<InputType, kPrunedShape>;

 public:
  using Base::kDefaultInit;

 protected:
  using Base::_doc;
  using Base::_docs;
  using Base::_enc;
  using Base::_freqs;
  using Base::_hint;
  using Base::_in;
  using Base::_last;
  using Base::_left_in_leaf;
  using Base::_left_in_list;
  using Base::_provider;
  using Base::_recipe;
  using Base::_score;
  using Base::_walk;
  using Base::In;
  using Base::Scores;

  class NoBoundSource final : public ScoreBoundSource {
   public:
    Attribute* GetMutable(TypeInfo::type_id) noexcept final { return nullptr; }
    void Read(DataInput& in, size_t size) final { in.Skip(size); }
    void Set(uint32_t, uint32_t) noexcept final {}
  };

 public:
  bool PrepareCommon(const PostingMeta& meta, const IndexInput& doc_in,
                     IndexFeatures layout, const SubReader& segment,
                     const TermReader& field, const detail::ScoreArgs& args) {
    SDB_ASSERT(meta.docs_count != 0);
    SDB_ASSERT(args.scorer != nullptr);
    SDB_ASSERT(args.fetcher != nullptr);
    SDB_ASSERT(FeaturesHaveFreq(layout));
    this->SetRecipe(segment, field, args);
    _cost = meta.docs_count;

    auto source = args.scorer->PrepareScoreBoundSource();
    if (source) {
      _bound_func =
        irs::detail::PrepareScore(args, segment, field.meta(), *source);
      _bound_source = std::move(source);
    } else {
      _bound_func =
        ScoreFunction::Constant(std::numeric_limits<score_t>::max());
      _bound_source = std::make_unique<NoBoundSource>();
    }

    this->MakeScore(segment, field, args);

    _walk.Disarm();
    _cached.fill(kNoBlock);
    _cached_runs.fill(kNoBlock);
    _root_score = std::numeric_limits<score_t>::max();
    _threshold = std::numeric_limits<score_t>::lowest();

    if (meta.docs_count == 1) {
      *(std::end(_docs) - 1) = doc_limits::min() + meta.doc_delta;
      *(std::end(_freqs.data) - 1) = meta.freq;
      _left_in_list = 0;
      _len = 1;
      _scored = false;
      return true;
    }

    _in = OpenDocInput(meta, doc_in);
    auto& in = In();
    *(std::end(_docs) - 1) = doc_limits::invalid();
    _left_in_list = meta.docs_count;
    _hint.Arm(meta);

    if (meta.docs_count > doc_limits::kBlockSize) {
      _walk.Arm(meta, BlockIndexShapeOf(layout, true));
      _walk.Load(in);
      _root_score = BoundScore(_walk.Index().Root());
    } else {
      const auto size = in.ReadByte();
      _bound_source->Read(in, size);
      _root_score = _bound_func.Score();
    }
    return false;
  }

  doc_id_t Value() const noexcept { return _doc; }

  uint32_t Cost() const noexcept { return _cost; }

  score_t RootScore() const noexcept { return _root_score; }

  doc_id_t AdvanceBlock(doc_id_t target) {
    if (!_walk.Armed()) [[unlikely]] {
      return doc_limits::eof();
    }
    if (_walk.UpperBound() >= target) {
      return _walk.UpperBound();
    }
    const auto left = SeekCursor(target);
    if (_needs_reposition || target > _last) {
      _left_in_list = left;
      _left_in_leaf = 0;
      _last = doc_limits::invalid();
      _needs_reposition = true;
      _doc = _walk.LandingDoc();
    }
    return _walk.UpperBound();
  }

  score_t MaxScore(doc_id_t doc) {
    if (!_walk.Armed()) {
      return _root_score;
    }
    const auto& index = _walk.Index();
    const auto n = index.Size();
    auto b = _walk.Block();
    if (b == n) {
      return _root_score;
    }
    if (_doc > index.Last(b) && !doc_limits::eof(_doc)) {
      b = index.Find(b, _doc);
      if (b == n) {
        return _root_score;
      }
    }
    if (doc <= index.Last(b)) [[likely]] {
      return BlockScore(b);
    }
    const auto r = b / BlockIndex::kRun;
    if (r + kCachedRuns < index.Runs() &&
        doc > index.RunLast(r + kCachedRuns - 1)) {
      return _root_score;
    }
    const auto last = std::min(index.Find(b, doc), n - 1);
    auto score = BlockScore(b);
    if (last - b < kCachedBlocks) {
      for (auto k = b + 1; k <= last; ++k) {
        score = std::max(score, BlockScore(k));
      }
      return score;
    }
    for (auto k = b + 1, end = (r + 1) * BlockIndex::kRun; k < end; ++k) {
      score = std::max(score, BlockScore(k));
    }
    for (auto k = r + 1, end = last / BlockIndex::kRun; k <= end; ++k) {
      score = std::max(score, RunScore(k));
    }
    return score;
  }

  doc_id_t SkipNonCompetitive(doc_id_t target, doc_id_t limit,
                              score_t threshold) {
    if (!_walk.Armed()) {
      return target;
    }
    const auto& index = _walk.Index();
    const auto n = index.Size();
    const auto b = index.Find(_walk.Block(), target);
    const auto first = FirstCompetitive(
      index, b, threshold, [&](uint32_t r) { return RunScore(r); },
      [&](uint32_t k) { return BlockScore(k); });
    if (first == b) {
      return target;
    }
    const auto next =
      first == n ? limit : std::min(limit, index.Last(first - 1) + 1);
    AdvanceBlock(next);
    return next;
  }

 protected:
  static constexpr uint32_t kNoBlock = std::numeric_limits<uint32_t>::max();
  static constexpr uint32_t kCachedBlocks = BlockIndex::kRun;
  static constexpr uint32_t kCachedRuns = 8;

  score_t BoundScore(BoundPair bound) {
    _bound_source->Set(bound.freq, bound.norm);
    return _bound_func.Score();
  }

  score_t BlockScore(uint32_t k) {
    const auto slot = k % kCachedBlocks;
    if (_cached[slot] != k) {
      _cached[slot] = k;
      _cached_scores[slot] = BoundScore(_walk.Index().Bound(k));
    }
    return _cached_scores[slot];
  }

  score_t RunScore(uint32_t r) {
    const auto slot = r % kCachedRuns;
    if (_cached_runs[slot] != r) {
      _cached_runs[slot] = r;
      _run_scores[slot] = BoundScore(_walk.Index().RunBound(r));
    }
    return _run_scores[slot];
  }

  uint32_t SeekCursor(doc_id_t target) {
    if constexpr (Standalone) {
      const auto& index = _walk.Index();
      return _walk.MoveTo(FirstCompetitive(
        index, index.Find(_walk.Block(), target), _threshold,
        [&](uint32_t r) { return RunScore(r); },
        [&](uint32_t b) { return BlockScore(b); }));
    } else {
      return _walk.Seek(target, In());
    }
  }

  IRS_FORCE_INLINE void Reposition() {
    if (!_needs_reposition) {
      return;
    }
    _needs_reposition = false;
    const auto state = _walk.Landing();
    In().Seek(state.doc_ptr);
    _doc = state.doc;
  }

  void RepositionForWindow(doc_id_t min) {
    if (!_needs_reposition || _left_in_list == 0) [[likely]] {
      return;
    }
    _needs_reposition = false;
    const auto state = _walk.Landing();
    In().Seek(state.doc_ptr);
    ReadLeaf(state.doc);
    const auto* const first =
      FirstNotBelow(std::end(_docs) - _left_in_leaf, min);
    SDB_ASSERT(first != std::end(_docs));
    _doc = *first;
    _left_in_leaf = static_cast<uint32_t>(std::end(_docs) - first) - 1;
  }

  IRS_FORCE_INLINE const doc_id_t* FirstNotBelow(const doc_id_t* begin,
                                                 doc_id_t max) const noexcept {
    return LeafLowerBound<doc_limits::kBlockSize>(std::cbegin(_docs), _len,
                                                  begin, max);
  }

  void ReadLeaf(doc_id_t prev) {
    this->ReadLeafDelta(prev);
    _len = _left_in_leaf;
    _base = prev;
    _scored = false;
  }

  template<typename Visitor>
  IRS_FORCE_INLINE void Emit(doc_id_t* docs, uint32_t len, Visitor&& visit) {
    if (!_scored) {
      ScoreLeaf();
    }
    visit(docs, len, Scores() + (docs - std::begin(_docs)));
  }

  IRS_FORCE_INLINE uint32_t ScoreKept(const doc_id_t* docs, uint32_t len,
                                      uint32_t least, doc_id_t* kept_docs,
                                      score_t* kept_scores) {
    ABSL_CACHELINE_ALIGNED uint32_t kept_freqs[doc_limits::kBlockSize];
    const auto kept =
      irs::detail::FilterFreqs(docs, _freqs.data + (docs - std::begin(_docs)),
                               len, least, kept_docs, kept_freqs);
    if (kept != 0) {
      _provider.freq.value = kept_freqs;
      FetchAndScore(*_score.fetcher, _score.score, kept_docs, kept_scores,
                    kept);
      _provider.freq.value = _freqs.data;
    }
    return kept;
  }

  template<typename Visitor>
  void ForEachBlock(doc_id_t max, Visitor&& visit) {
    if (_doc >= max) [[unlikely]] {
      return;
    }
    RepositionForWindow(_doc);

    SDB_ASSERT(_left_in_leaf < doc_limits::kBlockSize);
    doc_id_t last = *(std::end(_docs) - 1);
    {
      const auto count = _left_in_leaf + 1;
      if (last >= max) {
        _left_in_leaf = count;
        goto tail;
      }
      if (count == doc_limits::kBlockSize) {
        goto full;
      }
      visit(std::end(_docs) - count, count);
    }

    for (;;) {
      if (_left_in_list == 0) [[unlikely]] {
        _left_in_leaf = 0;
        goto done;
      }
      ReadLeaf(last);
      last = *(std::end(_docs) - 1);
      if (last >= max || _left_in_leaf != doc_limits::kBlockSize) {
        goto tail;
      }
    full:
      visit(std::begin(_docs), doc_limits::kBlockSize);
    }

  tail: {
    auto* const begin = std::end(_docs) - _left_in_leaf;
    auto* const end = FirstNotBelow(begin, max);
    _left_in_leaf = static_cast<uint32_t>(std::end(_docs) - end);
    if (end != begin) {
      visit(begin, static_cast<uint32_t>(end - begin));
    }
  }

  done:
    if (_left_in_leaf != 0) {
      _doc = *(std::end(_docs) - _left_in_leaf);
      --_left_in_leaf;
    } else {
      _doc = doc_limits::eof();
    }
  }

  template<typename Visitor>
  void ForEachScoredBlock(doc_id_t max, Visitor&& visit) {
    ForEachBlock(max, [&](doc_id_t* docs, uint32_t len)
                        IRS_FORCE_INLINE { Emit(docs, len, visit); });
  }

  void ScoreLeaf() {
    _scored = true;
    Base::ScoreLeaf(doc_limits::kBlockSize - _len, _len);
  }

  ScoreFunction _bound_func;
  ScoreBoundSource::ptr _bound_source;
  std::array<uint32_t, kCachedBlocks> _cached;
  std::array<score_t, kCachedBlocks> _cached_scores;
  std::array<uint32_t, kCachedRuns> _cached_runs;
  std::array<score_t, kCachedRuns> _run_scores;
  score_t _root_score = std::numeric_limits<score_t>::max();
  score_t _threshold = std::numeric_limits<score_t>::lowest();
  uint32_t _len = 0;
  doc_id_t _base = 0;
  uint32_t _cost = 0;
  bool _needs_reposition = false;
  bool _scored = false;
};

template<typename InputType>
class PrunedProbeBase : public PrunedLeafBase<InputType, false> {
  using Base = PrunedLeafBase<InputType, false>;

 public:
  uint32_t TakeReads() noexcept { return std::exchange(_reads, 0); }

 protected:
  using Base::_base;
  using Base::_doc;
  using Base::_docs;
  using Base::_enc;
  using Base::_freqs;
  using Base::_hint;
  using Base::_last;
  using Base::_left_in_list;
  using Base::_needs_reposition;
  using Base::_walk;
  using Base::In;

  static constexpr uint32_t kBlock = doc_limits::kBlockSize;

  template<typename Visit>
  IRS_FORCE_INLINE void Members(const doc_id_t* docs, uint32_t n,
                                Visit&& visit) {
    ForEachLeafOf(docs, n, [&](uint32_t i) IRS_FORCE_INLINE {
      const auto doc = docs[i];
      _doc = _leaf.Find(std::begin(_docs), _base, doc);
      const bool hit = _doc == doc;
      DecodeLazyFreqs(hit);
      visit(i, doc, hit, _freqs.data[_leaf.Index()]);
      return i + 1;
    });
  }

  template<typename Visit>
  IRS_FORCE_INLINE void Matches(const doc_id_t* docs, uint32_t n,
                                Visit&& visit) {
    uint32_t index[kScoreBlock];
    ForEachLeafOf(docs, n, [&](uint32_t i) IRS_FORCE_INLINE {
      const auto end = CountLess<kScoreBlock>(docs, _last + 1);
      SDB_ASSERT(i < end && end <= n);
      ScoreMask hits = 0;
      _doc = _leaf.template Match<kScoreBlock>(std::begin(_docs), _base, docs,
                                               i, end, hits, index);
      if (hits != 0) {
        DecodeLazyFreqs();
        for (; hits != 0; hits &= hits - 1) {
          const auto j = static_cast<uint32_t>(std::countr_zero(hits));
          visit(j, _freqs.data[index[j]]);
        }
      }
      return end;
    });
  }

  template<typename Visit>
  IRS_FORCE_INLINE void ForEachLeafOf(const doc_id_t* docs, uint32_t n,
                                      Visit&& visit) {
    SDB_ASSERT(n <= kScoreBlock);
    uint32_t blocks[kScoreBlock];
    const auto landings = Landings(docs, n, blocks);
    uint32_t landed = 0;
    for (uint32_t i = 0; i != n;) {
      if (docs[i] > _last) {
        if (landed != landings) {
          LandOn(blocks[landed++]);
        } else if (!Reach(docs[i])) {
          _doc = doc_limits::eof();
          return;
        }
      }
      i = visit(i);
    }
  }

  IRS_FORCE_INLINE void DecodeLazyFreqs(bool needed = true) noexcept {
    if constexpr (InputType::kVolatileAlways) {
      if (_lazy != nullptr) [[unlikely]] {
        if (needed) {
          DecodeFreqs();
        }
      }
    }
  }

  uint32_t Landings(const doc_id_t* in, uint32_t n, uint32_t* blocks) {
    if (!_walk.Armed() || n < 2 || in[n - 1] <= _last ||
        (!_needs_reposition &&
         (in[n - 1] - in[0]) / (n - 1) <= _last - _base)) {
      return 0;
    }
    const auto& index = _walk.Index();
    const auto size = index.Size();
    const auto block = _walk.NextBlock(_left_in_list);
    return irs::detail::PlanLandings(
      index,
      {.block = block,
       .next = _needs_reposition ? size : block,
       .upper = _needs_reposition ? doc_limits::invalid() : _last,
       .cap = n},
      in, n, blocks, [&](uint32_t landed) IRS_FORCE_INLINE {
        if constexpr (InputType::kVolatileAlways) {
          const auto begin =
            reinterpret_cast<uintptr_t>(In().At(_walk.BlockStart(landed)));
          const auto end = landed + 1 != size
                             ? reinterpret_cast<uintptr_t>(
                                 In().At(_walk.BlockStart(landed + 1)))
                             : begin + 1;
          for (auto line = begin & ~uintptr_t{ABSL_CACHELINE_SIZE - 1};
               line < end; line += ABSL_CACHELINE_SIZE) {
            __builtin_prefetch(reinterpret_cast<const void*>(line), 0, 2);
          }
        }
      });
  }

  void LandOn(uint32_t block) {
    if (!_needs_reposition && block == _walk.NextBlock(_left_in_list)) {
      ReadFill(_last);
      return;
    }
    _left_in_list = _walk.MoveTo(block);
    _needs_reposition = false;
    const auto landing = _walk.Landing();
    In().Seek(landing.doc_ptr);
    ReadFill(landing.doc);
  }

  bool Reach(doc_id_t target) {
    if (!_walk.Armed()) {
      if (_left_in_list == 0) {
        return false;
      }
      ReadFill(_last);
      return target <= _last;
    }
    const auto& index = _walk.Index();
    auto block = _walk.NextBlock(_left_in_list);
    if (block < index.Size() && index.Last(block) < target) {
      block = index.Find(block, target);
    }
    if (block >= index.Size()) {
      return false;
    }
    LandOn(block);
    return true;
  }

  void ReadFill(doc_id_t prev) {
    ++_reads;
    auto& in = In();
    _hint.Advance(in, in.Position());
    const auto len = std::min(_left_in_list, kBlock);
    const auto leaf =
      block_io::ReadTailForFill(len, in, _enc.data, nullptr, _docs, prev);
    _left_in_list -= len;
    const auto* const bitset =
      block_io::StableBitset<InputType>(leaf, std::begin(_docs));
    if constexpr (InputType::kVolatileAlways) {
      _lazy = in.Current();
      block_io::SkipTail(len, in);
    } else {
      block_io::ReadTail<block_io::kFreqBias>(len, in, _enc.data, _freqs.data);
    }
    _base = prev;
    _last = leaf.max;
    _leaf.Reset(leaf, bitset, len);
  }

  IRS_NO_INLINE void DecodeFreqs() noexcept {
    block_io::DecodeTail<block_io::kFreqBias>(_lazy, _leaf.Len(), _freqs.data);
    _lazy = nullptr;
  }

  const byte_type* _lazy = nullptr;
  detail::MaskedLeaf _leaf;
  uint32_t _reads = 0;
};

}  // namespace irs::top
