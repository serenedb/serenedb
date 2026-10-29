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
#include "iresearch/search/detail/masked_leaf.hpp"
#include "iresearch/search/scorers/score_args.hpp"
#include "iresearch/search/scorers/score_provider.hpp"
#include "iresearch/search/scorers/scorer.hpp"
#include "iresearch/utils/attribute_provider.hpp"
#include "iresearch/utils/bit_utils.hpp"
#include "iresearch/utils/down_cast.hpp"
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

template<typename InputType, bool Standalone>
class PrunedLeafBase {
 protected:
  class NoBoundSource final : public ScoreBoundSource {
   public:
    Attribute* GetMutable(TypeInfo::type_id) noexcept final { return nullptr; }
    void Read(DataInput& in, size_t size) final { in.Skip(size); }
    void Set(uint32_t, uint32_t) noexcept final {}
  };

 public:
  static constexpr bool kDefaultInit = true;

  PrunedLeafBase() = default;

  PrunedLeafBase(const PrunedLeafBase&) = delete;
  PrunedLeafBase& operator=(const PrunedLeafBase&) = delete;
  PrunedLeafBase(PrunedLeafBase&&) = delete;
  PrunedLeafBase& operator=(PrunedLeafBase&&) = delete;

  bool PrepareCommon(const PostingMeta& meta, const IndexInput& doc_in,
                     IndexFeatures layout, const SubReader& segment,
                     const TermReader& field, const detail::ScoreArgs& args) {
    SDB_ASSERT(meta.docs_count != 0);
    SDB_ASSERT(args.scorer != nullptr);
    SDB_ASSERT(args.fetcher != nullptr);
    SDB_ASSERT(FeaturesHaveFreq(layout));
    _fetcher = args.fetcher;
    _recipe = {&segment, &field, args};
    _provider.freq.value = _freqs.data;
    _cost = meta.docs_count;

    auto source = args.scorer->PrepareScoreBoundSource();
    if (source) {
      _bound_func = args.scorer->PrepareScorer({
        .segment = segment,
        .field = field.meta(),
        .doc_attrs = *source,
        .fetcher = *args.fetcher,
        .stats = args.stats,
        .boost = args.boost,
      });
      _bound_source = std::move(source);
    } else {
      _bound_func =
        ScoreFunction::Constant(std::numeric_limits<score_t>::max());
      _bound_source = std::make_unique<NoBoundSource>();
    }

    _score = args.scorer->PrepareScorer({
      .segment = segment,
      .field = field.meta(),
      .doc_attrs = _provider,
      .fetcher = *args.fetcher,
      .stats = args.stats,
      .boost = args.boost,
    });

    _cursor.Disarm();
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
    if (const auto extent = DocExtent(meta); extent != 0) {
      _hint.Arm(meta.doc_start, meta.doc_start + extent);
    }

    if (meta.docs_count > doc_limits::kBlockSize) {
      _cursor.Arm(meta, BlockIndexShapeOf(layout, true));
      _cursor.Load(in);
      _root_score = BoundScore(_cursor.Index().Root());
    } else {
      const auto size = in.ReadByte();
      _bound_source->Read(in, size);
      _root_score = _bound_func.Score();
    }
    return false;
  }

  doc_id_t Value() const noexcept { return _doc; }

  uint32_t Cost() const noexcept { return _cost; }

  doc_id_t AdvanceBlock(doc_id_t target) {
    if (!_cursor.Armed()) [[unlikely]] {
      return doc_limits::eof();
    }
    if (_cursor.UpperBound() >= target) {
      return _cursor.UpperBound();
    }
    const auto left = SeekCursor(target);
    if (_needs_reposition || target > _max_in_leaf) {
      _left_in_list = left;
      _left_in_leaf = 0;
      _max_in_leaf = doc_limits::invalid();
      _needs_reposition = true;
      _doc = _cursor.LandingDoc();
    }
    return _cursor.UpperBound();
  }

  score_t MaxScore(doc_id_t doc) {
    if (!_cursor.Armed()) {
      return _root_score;
    }
    const auto& index = _cursor.Index();
    const auto n = index.Size();
    auto b = _cursor.Block();
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
    if (!_cursor.Armed()) {
      return target;
    }
    const auto& index = _cursor.Index();
    const auto n = index.Size();
    const auto b = index.Find(_cursor.Block(), target);
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

  IRS_FORCE_INLINE InputType& In() const noexcept {
    return irs::utils::downCast<InputType>(*_in);
  }

  score_t BoundScore(BoundPair bound) {
    _bound_source->Set(bound.freq, bound.norm);
    return _bound_func.Score();
  }

  score_t BlockScore(uint32_t k) {
    const auto slot = k % kCachedBlocks;
    if (_cached[slot] != k) {
      _cached[slot] = k;
      _cached_scores[slot] = BoundScore(_cursor.Index().Bound(k));
    }
    return _cached_scores[slot];
  }

  score_t RunScore(uint32_t r) {
    const auto slot = r % kCachedRuns;
    if (_cached_runs[slot] != r) {
      _cached_runs[slot] = r;
      _run_scores[slot] = BoundScore(_cursor.Index().RunBound(r));
    }
    return _run_scores[slot];
  }

  uint32_t SeekCursor(doc_id_t target) {
    if constexpr (Standalone) {
      const auto& index = _cursor.Index();
      return _cursor.MoveTo(FirstCompetitive(
        index, index.Find(_cursor.Block(), target), _threshold,
        [&](uint32_t r) { return RunScore(r); },
        [&](uint32_t b) { return BlockScore(b); }));
    } else {
      return _cursor.Seek(target, In());
    }
  }

  IRS_FORCE_INLINE void Reposition() {
    if (!_needs_reposition) {
      return;
    }
    _needs_reposition = false;
    const auto state = _cursor.Landing();
    In().Seek(state.doc_ptr);
    _doc = state.doc;
  }

  void RepositionForWindow(doc_id_t min) {
    if (!_needs_reposition || _left_in_list == 0) [[likely]] {
      return;
    }
    _needs_reposition = false;
    const auto state = _cursor.Landing();
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
    if (_len == doc_limits::kBlockSize) [[likely]] {
      return BranchlessLowerBound<doc_limits::kBlockSize>(std::cbegin(_docs),
                                                          max);
    }
    return std::find_if(begin, std::cend(_docs),
                        [max](doc_id_t doc) { return doc >= max; });
  }

  void ReadLeaf(doc_id_t prev) {
    auto& in = In();
    _hint.Advance(in, in.Position());
    const auto len = std::min(_left_in_list, doc_limits::kBlockSize);
    block_io::ReadTailDelta(len, in, _enc.data, _docs, prev);
    block_io::ReadTail<block_io::kFreqBias>(len, in, _enc.data, _freqs.data);
    _left_in_leaf = len;
    _len = len;
    _left_in_list -= len;
    _base = prev;
    _max_in_leaf = *(std::end(_docs) - 1);
    _scored = false;
  }

  template<typename Visitor>
  IRS_FORCE_INLINE void Emit(doc_id_t* docs, uint32_t len, Visitor&& visit) {
    if (!_scored) {
      ScoreLeaf();
    }
    visit(docs, len, Scores() + (docs - std::begin(_docs)));
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

  IRS_FORCE_INLINE score_t* Scores() noexcept {
    static_assert(sizeof(score_t) == sizeof(_enc.data[0]));
    return reinterpret_cast<score_t*>(std::begin(_enc.data));
  }

  void ScoreLeaf() {
    _scored = true;
    if (_len == doc_limits::kBlockSize) {
      _fetcher->FetchPostingBlock(
        std::span<const doc_id_t, doc_limits::kBlockSize>{
          std::begin(_docs), doc_limits::kBlockSize});
      _score.ScorePostingBlock(Scores());
      return;
    }
    const auto offset = doc_limits::kBlockSize - _len;
    _fetcher->Fetch(
      std::span<const doc_id_t>{std::begin(_docs) + offset, _len});
    _provider.freq.value = _freqs.data + offset;
    _score.Score(Scores() + offset, static_cast<scores_size_t>(_len));
    _provider.freq.value = _freqs.data;
  }

  detail::EncBuf _enc;
  detail::FreqBuf _freqs;
  DocsBuf _docs;
  IndexInput::ptr _in;
  ColumnArgsFetcher* _fetcher = nullptr;
  ScoreFunction _score;
  detail::LeafProvider _provider;
  detail::LeafRecipe _recipe;
  BlockCursor _cursor;
  GrowingHint _hint;
  ScoreFunction _bound_func;
  ScoreBoundSource::ptr _bound_source;
  std::array<uint32_t, kCachedBlocks> _cached;
  std::array<score_t, kCachedBlocks> _cached_scores;
  std::array<uint32_t, kCachedRuns> _cached_runs;
  std::array<score_t, kCachedRuns> _run_scores;
  score_t _root_score = std::numeric_limits<score_t>::max();
  score_t _threshold = std::numeric_limits<score_t>::lowest();
  doc_id_t _doc = 0;
  uint32_t _left_in_leaf = 0;
  uint32_t _len = 0;
  doc_id_t _base = 0;
  doc_id_t _max_in_leaf = doc_limits::invalid();
  uint32_t _left_in_list = 0;
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
  using Base::_cursor;
  using Base::_doc;
  using Base::_docs;
  using Base::_enc;
  using Base::_freqs;
  using Base::_hint;
  using Base::_left_in_list;
  using Base::_max_in_leaf;
  using Base::_needs_reposition;
  using Base::In;

  static constexpr uint32_t kBlock = doc_limits::kBlockSize;

  template<typename Visit>
  IRS_FORCE_INLINE void Members(const doc_id_t* docs, uint32_t n,
                                Visit&& visit) {
    SDB_ASSERT(n <= kScoreBlock);
    uint32_t blocks[kScoreBlock];
    const auto landings = Landings(docs, n, blocks);
    uint32_t landed = 0;
    for (uint32_t i = 0; i != n;) {
      if (docs[i] > _max_in_leaf) {
        if (landed != landings) {
          LandOn(blocks[landed++]);
        } else if (!Reach(docs[i])) {
          _doc = doc_limits::eof();
          return;
        }
      }
      const auto last = _max_in_leaf;
      do {
        const auto doc = docs[i];
        _doc = _leaf.Find(std::begin(_docs), _base, doc);
        const bool hit = _doc == doc;
        if constexpr (InputType::kVolatileAlways) {
          if (hit && _lazy != nullptr) [[unlikely]] {
            DecodeFreqs();
          }
        }
        visit(i, hit, _freqs.data[_leaf.Index()]);
      } while (++i != n && docs[i] <= last);
    }
  }

  uint32_t Landings(const doc_id_t* in, uint32_t n, uint32_t* blocks) {
    if (!_cursor.Armed() || n < 2 || in[n - 1] <= _max_in_leaf ||
        (!_needs_reposition &&
         (in[n - 1] - in[0]) / (n - 1) <= _max_in_leaf - _base)) {
      return 0;
    }
    const auto& index = _cursor.Index();
    const auto size = index.Size();
    auto block = _cursor.NextBlock(_left_in_list);
    auto next = _needs_reposition ? size : block;
    doc_id_t upper = _needs_reposition ? doc_limits::invalid() : _max_in_leaf;
    uint32_t landings = 0;
    for (uint32_t i = 0; i != n; ++i) {
      if (in[i] <= upper) {
        continue;
      }
      if (block == size || index.Last(block) < in[i]) {
        block = index.Find(block, in[i]);
      }
      if (block == size) {
        break;
      }
      blocks[landings++] = block;
      if constexpr (InputType::kVolatileAlways) {
        if (block != next) {
          const auto begin =
            reinterpret_cast<uintptr_t>(In().At(_cursor.BlockStart(block)));
          const auto end = block + 1 != size
                             ? reinterpret_cast<uintptr_t>(
                                 In().At(_cursor.BlockStart(block + 1)))
                             : begin + 1;
          for (auto line = begin & ~uintptr_t{ABSL_CACHELINE_SIZE - 1};
               line < end; line += ABSL_CACHELINE_SIZE) {
            __builtin_prefetch(reinterpret_cast<const void*>(line), 0, 2);
          }
        }
      }
      upper = index.Last(block);
      next = ++block;
    }
    return landings;
  }

  void LandOn(uint32_t block) {
    if (!_needs_reposition && block == _cursor.NextBlock(_left_in_list)) {
      ReadFill(_max_in_leaf);
      return;
    }
    _left_in_list = _cursor.MoveTo(block);
    _needs_reposition = false;
    const auto landing = _cursor.Landing();
    In().Seek(landing.doc_ptr);
    ReadFill(landing.doc);
  }

  bool Reach(doc_id_t target) {
    if (!_cursor.Armed()) {
      if (_left_in_list == 0) {
        return false;
      }
      ReadFill(_max_in_leaf);
      return target <= _max_in_leaf;
    }
    const auto& index = _cursor.Index();
    auto block = _cursor.NextBlock(_left_in_list);
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
    const auto* bitset = leaf.bitset;
    if constexpr (!InputType::kVolatileAlways) {
      if (leaf.IsBitset()) {
        std::memcpy(std::begin(_docs), leaf.bitset,
                    size_t{leaf.words} * sizeof(uint64_t));
        bitset = reinterpret_cast<const UnalignedWord*>(std::begin(_docs));
      }
    }
    if constexpr (InputType::kVolatileAlways) {
      _lazy = in.Current();
      block_io::SkipTail(len, in);
    } else {
      block_io::ReadTail<block_io::kFreqBias>(len, in, _enc.data, _freqs.data);
    }
    _base = prev;
    _max_in_leaf = leaf.max;
    _leaf.Reset(leaf, bitset, len);
  }

  IRS_NO_INLINE void DecodeFreqs() noexcept {
    using Codec = block_io::Codec;
    const auto len = _leaf.Len();
    if (len == kBlock) {
      Codec::DecodeValuesBlock<block_io::kFreqBias>(_lazy, _freqs.data);
    } else {
      Codec::DecodeValuesTail<block_io::kFreqBias>(
        _lazy, len, _freqs.data + (kBlock - len));
    }
    _lazy = nullptr;
  }

  ABSL_CACHELINE_ALIGNED uint32_t _gather[kScoreBlock]{};
  const byte_type* _lazy = nullptr;
  detail::MaskedLeaf _leaf;
  uint32_t _reads = 0;
};

}  // namespace irs::top
