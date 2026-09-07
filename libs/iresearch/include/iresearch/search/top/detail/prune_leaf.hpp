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
#include <limits>
#include <memory>
#include <span>
#include <vector>

#include "basics/bit_utils.hpp"
#include "basics/down_cast.h"
#include "basics/shared.hpp"
#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/error/error.hpp"
#include "iresearch/formats/posting/common.hpp"
#include "iresearch/formats/posting/format_block_128.hpp"
#include "iresearch/formats/posting/skip_list.hpp"
#include "iresearch/formats/posting_meta.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/column_collector.hpp"
#include "iresearch/search/common/enc_buf.hpp"
#include "iresearch/search/common/score_args.hpp"
#include "iresearch/search/common/score_provider.hpp"
#include "iresearch/search/scorer.hpp"
#include "iresearch/utils/attribute_provider.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::search {

template<typename InputType, bool Standalone>
class PruneLeafBase {
 protected:
  using BoundTraits = IteratorTraitsImpl<FormatTraits128, true, false, false>;

  class NoBoundSource final : public ScoreBoundSource {
   public:
    Attribute* GetMutable(TypeInfo::type_id) noexcept final { return nullptr; }
    void Read(DataInput& in, size_t size) final { in.Skip(size); }
  };

  class BoundReadSkip {
   public:
    explicit BoundReadSkip(bool)
      : _skip_levels(1), _skip_scores(1, std::numeric_limits<score_t>::max()) {
      Disable();
    }

    void SetBoundScorer(ScoreFunction func,
                        ScoreBoundSource::ptr source) noexcept {
      _bound_func = std::move(func);
      _bound_source = std::move(source);
    }

    void SetLayout(SkipLayout layout) noexcept { _layout = layout; }

    void SetSkipBoundsBelow(doc_id_t max) noexcept { _skip_bounds_below = max; }
    doc_id_t SkipBoundsBelow() const noexcept { return _skip_bounds_below; }

    score_t& Threshold() noexcept { return _threshold; }

    void Disable() noexcept {
      SDB_ASSERT(!_skip_levels.empty());
      _skip_levels.back().doc = doc_limits::eof();
    }

    void Enable(const PostingMeta& state) noexcept {
      SDB_ASSERT(state.docs_count > doc_limits::kBlockSize);
      CopyState<BoundTraits>(_skip_levels.front(), state);
      SDB_ASSERT(doc_limits::eof(_skip_levels.back().doc));
      _skip_levels.back().doc = doc_limits::invalid();
    }

    void Init(size_t num_levels, score_t max_score) {
      SDB_ASSERT(num_levels != 0);
      _skip_levels.resize(num_levels);
      _skip_scores.resize(num_levels, std::numeric_limits<score_t>::max());
      _global_max_score = max_score;
    }

    IRS_FORCE_INLINE bool IsLess(size_t level, doc_id_t target) const noexcept {
      if constexpr (Standalone) {
        return _skip_levels[level].doc < target ||
               _skip_scores[level] <= _threshold;
      } else {
        return _skip_levels[level].doc < target;
      }
    }

    IRS_FORCE_INLINE bool IsLessThanUpperBound(doc_id_t target) const noexcept {
      if constexpr (Standalone) {
        return _skip_levels.back().doc < target ||
               _skip_scores.back() <= _threshold;
      } else {
        return _skip_levels.back().doc < target;
      }
    }

    IRS_FORCE_INLINE void MoveDown(size_t level) noexcept {
      CopyState<BoundTraits>(_skip_levels[level], _prev_skip);
    }

    IRS_FORCE_INLINE void Read(size_t level, InputType& in) {
      auto& next = _skip_levels[level];
      CopyState<BoundTraits>(_prev_skip, next);
      ReadDocState(next, in, _layout);
      if (_skip_bounds_below != 0 && next.doc < _skip_bounds_below)
        [[unlikely]] {
        SkipBounds(in);
      } else {
        _skip_scores[level] = ReadBound(in);
      }
    }

    void Seal(size_t level) {
      auto& next = _skip_levels[level];
      CopyState<BoundTraits>(_prev_skip, next);
      next.doc = doc_limits::eof();
      _skip_scores[level] = std::numeric_limits<score_t>::max();
    }

    IRS_FORCE_INLINE size_t AdjustLevel(size_t level) const noexcept {
      if constexpr (Standalone) {
        while (level != 0 &&
               _skip_levels[level].doc >= _skip_levels[level - 1].doc) {
          --level;
        }
      }
      return level;
    }

    IRS_FORCE_INLINE doc_id_t UpperBound() const noexcept {
      return _skip_levels.back().doc;
    }

    IRS_FORCE_INLINE score_t ReadBound(IndexInput& in) {
      const auto size = in.ReadByte();
      _bound_source->Read(in, size);
      return _bound_func.Score();
    }

    IRS_FORCE_INLINE void SkipBounds(InputType& in) {
      SkipScoreBounds(true, in);
    }

    SkipState& State() noexcept { return _prev_skip; }

    IRS_FORCE_INLINE score_t MaxScore(doc_id_t doc) const noexcept {
      for (size_t i = _skip_levels.size(); i-- != 0;) {
        if (_skip_levels[i].doc >= doc) {
          return _skip_scores[i];
        }
      }
      return _global_max_score;
    }

   private:
    std::vector<SkipState> _skip_levels;
    std::vector<score_t> _skip_scores;
    score_t _global_max_score = std::numeric_limits<score_t>::max();
    SkipState _prev_skip;
    ScoreFunction _bound_func;
    ScoreBoundSource::ptr _bound_source;
    score_t _threshold = std::numeric_limits<score_t>::lowest();
    doc_id_t _skip_bounds_below = 0;
    SkipLayout _layout;
  };

 public:
  PruneLeafBase()
    : _skip{doc_limits::kBlockSize, doc_limits::kSkipSize, true} {}

  PruneLeafBase(const PruneLeafBase&) = delete;
  PruneLeafBase& operator=(const PruneLeafBase&) = delete;
  PruneLeafBase(PruneLeafBase&&) = delete;
  PruneLeafBase& operator=(PruneLeafBase&&) = delete;

  bool PrepareCommon(const PostingMeta& meta, const IndexInput& doc_in,
                     IndexFeatures layout, const SubReader& segment,
                     const TermReader& field, const ScoreArgs& args) {
    SDB_ASSERT(meta.docs_count != 0);
    SDB_ASSERT(args.scorer != nullptr);
    SDB_ASSERT(args.fetcher != nullptr);
    SDB_ASSERT(FeaturesHaveFreq(layout));
    _skip.Reader().SetLayout(ToSkipLayout(layout));
    _fetcher = args.fetcher;
    _recipe = {&segment, &field, args};
    _provider.freq.value = _freqs.data;

    auto source = args.scorer->PrepareScoreBoundSource();
    if (source) {
      auto bound = args.scorer->PrepareScorer({
        .segment = segment,
        .field = field.meta(),
        .doc_attrs = *source,
        .stats = args.stats,
        .boost = args.boost,
      });
      _skip.Reader().SetBoundScorer(std::move(bound), std::move(source));
    } else {
      _skip.Reader().SetBoundScorer(
        ScoreFunction::Constant(std::numeric_limits<score_t>::max()),
        std::make_unique<NoBoundSource>());
    }

    _score = args.scorer->PrepareScorer({
      .segment = segment,
      .field = field.meta(),
      .doc_attrs = _provider,
      .fetcher = args.fetcher,
      .stats = args.stats,
      .boost = args.boost,
    });

    if (meta.docs_count == 1) {
      *(std::end(_docs) - 1) = doc_limits::min() + meta.doc_delta;
      *(std::end(_freqs.data) - 1) = meta.freq;
      _left_in_list = 0;
      return true;
    }

    _in = doc_in.Reopen();
    if (!_in) [[unlikely]] {
      throw IoError{"failed to reopen document input"};
    }
    auto& in = In();
    in.Seek(meta.doc_start);
    _left_in_list = meta.docs_count;

    if (meta.docs_count > doc_limits::kBlockSize) {
      _skip.Reader().Enable(meta);
      PrepareSkip(meta.doc_start + meta.doc_delta, meta.docs_count);
      _upper_bound = doc_limits::invalid();
    } else if (meta.docs_count < doc_limits::kBlockSize) {
      _skip.Reader().SkipBounds(in);
    }
    return false;
  }

  doc_id_t Value() const noexcept { return _doc; }

  score_t MaxScore(doc_id_t doc) noexcept {
    return _skip.Reader().MaxScore(doc);
  }

  void SetSkipBoundsBelow(doc_id_t max) noexcept {
    _skip.Reader().SetSkipBoundsBelow(max);
  }

 protected:
  IRS_FORCE_INLINE InputType& In() const noexcept {
    return sdb::basics::downCast<InputType>(*_in);
  }

  void PrepareSkip(uint64_t skip_offs, uint32_t docs_count) {
    std::unique_ptr<InputType> skip_in{
      sdb::basics::downCast<InputType>(In().Dup().release())};
    if (!skip_in) [[unlikely]] {
      throw IoError{"failed to duplicate document input"};
    }
    skip_in->Seek(skip_offs);
    const auto global_max_score = _skip.Reader().ReadBound(*skip_in);
    _skip.Prepare(std::move(skip_in), docs_count);
    const auto num_levels = _skip.NumLevels();
    if (num_levels == 0 || num_levels > doc_limits::kMaxSkipLevels)
      [[unlikely]] {
      throw IndexError{"invalid number of skip levels"};
    }
    _skip.Reader().Init(num_levels, global_max_score);
  }

  IRS_FORCE_INLINE void Reposition() {
    if (!_needs_reposition) {
      return;
    }
    _needs_reposition = false;
    auto& state = _skip.Reader().State();
    if (state.doc_ptr != 0) [[likely]] {
      In().Seek(state.doc_ptr);
    }
    _doc = state.doc;
  }

  void RepositionForWindow(doc_id_t min) {
    if (!_needs_reposition || _left_in_list == 0) [[likely]] {
      return;
    }
    _needs_reposition = false;
    auto& state = _skip.Reader().State();
    if (state.doc_ptr != 0) [[likely]] {
      In().Seek(state.doc_ptr);
    }
    ReadLeaf(state.doc);
    const auto* const first =
      std::find_if(std::end(_docs) - _left_in_leaf, std::end(_docs),
                   [min](doc_id_t doc) { return doc >= min; });
    SDB_ASSERT(first != std::end(_docs));
    _doc = *first;
    _left_in_leaf = static_cast<uint32_t>(std::end(_docs) - first) - 1;
  }

  void ReadLeaf(doc_id_t prev) {
    auto& in = In();
    const auto len = std::min(_left_in_list, doc_limits::kBlockSize);
    FormatTraits128::ReadTailDelta(len, in, _enc.data, _docs, prev);
    FormatTraits128::ReadTail(len, in, _enc.data, _freqs.data);
    _left_in_leaf = len;
    _len = len;
    _left_in_list -= len;
    _base = prev;
    _max_in_leaf = *(std::end(_docs) - 1);
  }

  template<typename Visitor>
  IRS_FORCE_INLINE void Emit(doc_id_t* docs, uint32_t len, Visitor&& visit) {
    static_assert(sizeof(score_t) == sizeof(_enc.data[0]));
    const auto offset = static_cast<size_t>(docs - std::begin(_docs));
    score_t* p;
    if (len == doc_limits::kBlockSize) {
      _fetcher->FetchPostingBlock(
        std::span<const doc_id_t, doc_limits::kBlockSize>{
          docs, doc_limits::kBlockSize});
      p = reinterpret_cast<score_t*>(std::begin(_enc.data));
      _provider.freq.value = _freqs.data;
      _score.ScorePostingBlock(p);
    } else {
      _fetcher->Fetch(std::span<const doc_id_t>{docs, len});
      p = reinterpret_cast<score_t*>(std::end(_enc.data) - len);
      _provider.freq.value = _freqs.data + offset;
      _score.Score(p, static_cast<scores_size_t>(len));
      _provider.freq.value = _freqs.data;
    }
    visit(docs, len, p);
  }

  EncBuf _enc;
  FreqBuf _freqs;
  DocsBuf _docs;
  IndexInput::ptr _in;
  ColumnArgsFetcher* _fetcher = nullptr;
  ScoreFunction _score;
  LeafProvider _provider;
  LeafRecipe _recipe;
  SkipReader<BoundReadSkip, InputType> _skip;
  doc_id_t _doc = 0;
  uint32_t _left_in_leaf = 0;
  uint32_t _len = 0;
  doc_id_t _base = 0;
  doc_id_t _max_in_leaf = doc_limits::invalid();
  doc_id_t _upper_bound = doc_limits::eof();
  uint32_t _left_in_list = 0;
  bool _needs_reposition = false;
};

}  // namespace irs::search
