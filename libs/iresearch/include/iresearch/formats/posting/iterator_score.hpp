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

#include "iresearch/formats/posting/common.hpp"
#include "iresearch/formats/posting/skip_list.hpp"
#include "iresearch/index/index_reader.hpp"

namespace irs {

template<typename WandExtent>
IRS_FORCE_INLINE score_t CommonReadWandData(WandExtent wextent, uint8_t index,
                                            const ScoreFunction& func,
                                            WandSource& ctx, DataInput& in) {
  const auto extent = wextent.GetExtent();
  SDB_ASSERT(extent);
  SDB_ASSERT(index < extent);
  if (extent == 1) [[likely]] {
    const auto size = in.ReadByte();
    ctx.Read(in, size);
    return func.Score();
  }

  uint8_t i = 0;
  uint64_t scorer_offset = 0;
  for (; i < index; ++i) {
    scorer_offset += in.ReadByte();
  }
  const auto size = in.ReadByte();
  ++i;
  uint64_t block_offset = 0;
  for (; i < extent; ++i) {
    block_offset += in.ReadByte();
  }

  if (scorer_offset) {
    in.Skip(scorer_offset);
  }
  ctx.Read(in, size);
  const auto score = func.Score();
  if (block_offset) {
    in.Skip(block_offset);
  }
  return score;
}

template<typename FormatTraits>
using WandTraits = IteratorTraitsImpl<FormatTraits, true, false, false>;

template<typename FormatTraits, bool Root, bool Pos, bool Offs,
         typename WandExtent, typename InputType>
class SingleWandIterator : public DocIterator {
  using IteratorTraits = WandTraits<FormatTraits>;
  using FieldTraits = IteratorTraitsImpl<FormatTraits, true, Pos, Offs>;

  class DefaultWandSource final : public WandSource {
   public:
    Attribute* GetMutable(TypeInfo::type_id) noexcept final { return nullptr; }
    void Read(DataInput& in, size_t size) final {
      while (size--) {
        in.ReadByte();
      }
    }
  };

 public:
  static_assert(doc_limits::kBlockSize % kScoreBlock == 0,
                "kBlockSize must be a multiple of kScoreBlock");

  explicit SingleWandIterator(WandExtent extent)
    : _skip{doc_limits::kBlockSize, doc_limits::kSkipSize, extent} {}

  ~SingleWandIterator() {
    if (_doc_in) {
      std::allocator<uint32_t>{}.deallocate(_collected_freqs, kScoreBlock);
    }
  }

  ScoreFunction PrepareScore(const PrepareScoreContext& ctx) final {
    SDB_ASSERT(ctx.scorer);
    if (auto wand_source = ctx.scorer->prepare_wand_source()) {
      auto wand_func = ctx.scorer->PrepareScorer({
        .segment = *ctx.segment,
        .field = _field,
        .doc_attrs = *wand_source,
        .stats = _stats,
        .boost = _boost,
      });
      _skip.Reader().SetWandScore(std::move(wand_func), std::move(wand_source));
    }
    if (_deferred_skip_offs) {
      PrepareSkipReader(_deferred_skip_offs, _deferred_skip_docs_count);
      _deferred_skip_offs = 0;
    }
    return ctx.scorer->PrepareScorer({
      .segment = *ctx.segment,
      .field = _field,
      .doc_attrs = *this,
      .fetcher = ctx.fetcher,
      .stats = _stats,
      .boost = _boost,
    });
  }

  void Prepare(const PostingCookie& meta, const IndexInput* doc_in,
               uint8_t wand_index);

  void SetSkipWandBelow(doc_id_t max) noexcept {
    _skip.Reader().SetSkipWandBelow(max);
  }

  IRS_NO_INLINE Attribute* GetMutable(TypeInfo::type_id type) noexcept final {
    if (type == irs::Type<ScoreThresholdAttr>::id()) {
      return &_skip.Reader().Threshold();
    }
    return irs::GetMutable(_attrs, type);
  }

  IRS_FORCE_INLINE doc_id_t advance() final { return seek(value() + 1); }

  IRS_FORCE_INLINE doc_id_t seek(doc_id_t target) final;

  IRS_FORCE_INLINE doc_id_t LazySeek(doc_id_t target) final {
    SDB_ASSERT(target >= value());
    return seek(target);
  }

  uint32_t count() final {
    _doc = doc_limits::eof();
    const auto left_in_leaf = std::exchange(_left_in_leaf, 0);
    const auto left_in_list = std::exchange(_left_in_list, 0);
    return left_in_leaf + left_in_list;
  }

  void Collect(const ScoreFunction& scorer, ColumnArgsFetcher& fetcher,
               ScoreCollector& collector) final;

  void FetchScoreArgs(uint16_t index) final {
    SDB_ASSERT(_collected_freqs);
    SDB_ASSERT(_left_in_leaf < doc_limits::kBlockSize);
    _collected_freqs[index] = *(std::end(_freqs) - _left_in_leaf - 1);
  }

  void Init(const PostingCookie& cookie) noexcept {
    _field = cookie.field;
    _stats = cookie.stats;
    _boost = cookie.boost;
  }

 private:
  class WandReadSkip {
   public:
    explicit WandReadSkip(WandExtent extent)
      : _skip_levels(1),
        _skip_scores(1, std::numeric_limits<score_t>::max()),
        _wand_extent{extent} {
      Disable();
    }

    void SetWandIndex(uint8_t index) noexcept { _wand_index = index; }

    void SetWandScore(ScoreFunction func,
                      WandSource::ptr wand_source) noexcept {
      _wand_func = std::move(func);
      _wand_source = std::move(wand_source);
    }

    void SetSkipWandBelow(doc_id_t max) noexcept { _skip_wand_below = max; }

    ScoreThresholdAttr& Threshold() noexcept { return _threshold; }

    void EnsureSorted() const noexcept {
      SDB_ASSERT(absl::c_is_sorted(
        _skip_levels,
        [](const auto& lhs, const auto& rhs) { return lhs.doc > rhs.doc; }));
      if constexpr (Root) {
        SDB_ASSERT(absl::c_is_sorted(_skip_scores, std::greater<>{}));
      }
    }

    void Disable() noexcept {
      SDB_ASSERT(!_skip_levels.empty());
      SDB_ASSERT(!doc_limits::valid(_skip_levels.back().doc));
      _skip_levels.back().doc = doc_limits::eof();
    }

    void Enable(const TermMetaImpl& state) noexcept {
      SDB_ASSERT(state.docs_count > doc_limits::kBlockSize);
      auto& top = _skip_levels.front();
      CopyState<IteratorTraits>(top, state);
      Enable();
    }

    void Init(size_t num_levels, score_t max_score) {
      SDB_ASSERT(num_levels);
      _skip_levels.resize(num_levels);
      _skip_scores.resize(num_levels, std::numeric_limits<score_t>::max());
      _global_max_score = max_score;
    }

    IRS_FORCE_INLINE bool IsLess(size_t level, doc_id_t target) const noexcept {
      if constexpr (Root) {
        return _skip_levels[level].doc < target ||
               _skip_scores[level] <= _threshold.value;
      } else {
        return _skip_levels[level].doc < target;
      }
    }
    IRS_FORCE_INLINE bool IsLessThanUpperBound(doc_id_t target) const noexcept {
      if constexpr (Root) {
        return _skip_levels.back().doc < target ||
               _skip_scores.back() <= _threshold.value;
      } else {
        return _skip_levels.back().doc < target;
      }
    }

    IRS_FORCE_INLINE void MoveDown(size_t level) noexcept {
      auto& next = _skip_levels[level];
      CopyState<IteratorTraits>(next, _prev_skip);
    }

    IRS_FORCE_INLINE void Read(size_t level, InputType& in) {
      auto& next = _skip_levels[level];
      CopyState<IteratorTraits>(_prev_skip, next);
      ReadState<FieldTraits>(next, in);
      if (_skip_wand_below && next.doc < _skip_wand_below) [[unlikely]] {
        SkipWandData(in);
      } else {
        _skip_scores[level] = ReadWandScore(in);
      }
    }

    void Seal(size_t level) {
      auto& next = _skip_levels[level];

      // Store previous step on the same level
      CopyState<IteratorTraits>(_prev_skip, next);

      // Stream exhausted
      next.doc = doc_limits::eof();
      _skip_scores[level] = std::numeric_limits<score_t>::max();
    }

    IRS_FORCE_INLINE size_t AdjustLevel(size_t level) const noexcept {
      if constexpr (Root) {
        while (level &&
               _skip_levels[level].doc >= _skip_levels[level - 1].doc) {
          SDB_ASSERT(_skip_levels[level - 1].doc != doc_limits::eof());
          --level;
        }
      }
      return level;
    }

    IRS_FORCE_INLINE doc_id_t UpperBound() const noexcept {
      SDB_ASSERT(!_skip_levels.empty());
      return _skip_levels.back().doc;
    }

    IRS_FORCE_INLINE score_t ReadWandScore(IndexInput& in) {
      return CommonReadWandData(_wand_extent, _wand_index, _wand_func,
                                *_wand_source, in);
    }

    IRS_FORCE_INLINE void SkipWandData(InputType& in) {
      CommonSkipWandData(_wand_extent, in);
    }

    SkipState& State() noexcept { return _prev_skip; }
    SkipState& Next() noexcept { return _skip_levels.back(); }

    IRS_FORCE_INLINE score_t GetMaxScore(doc_id_t doc) noexcept {
      for (size_t i = _skip_levels.size(); i--;) {
        if (_skip_levels[i].doc >= doc) {
          return _skip_scores[i];
        }
      }
      return _global_max_score;
    }

    doc_id_t GetUpperBound(size_t i) noexcept {
      SDB_ASSERT(i < _skip_levels.size());
      return _skip_levels[i].doc;
    }

   private:
    void Enable() noexcept {
      SDB_ASSERT(!_skip_levels.empty());
      SDB_ASSERT(doc_limits::eof(_skip_levels.back().doc));
      _skip_levels.back().doc = doc_limits::invalid();
    }

    std::vector<SkipState> _skip_levels;
    std::vector<score_t> _skip_scores;
    score_t _global_max_score = std::numeric_limits<score_t>::max();
    SkipState _prev_skip;
    ScoreFunction _wand_func;
    WandSource::ptr _wand_source;
    ScoreThresholdAttr _threshold;
    uint8_t _wand_index = 0;
    doc_id_t _skip_wand_below = 0;
    [[no_unique_address]] WandExtent _wand_extent;
  };

 public:
  score_t GetMaxScore(doc_id_t doc) noexcept {
    return _skip.Reader().GetMaxScore(doc);
  }

  doc_id_t SeekToBlock(doc_id_t target) {
    target = ShallowSeekToBlock(target);
    if (!doc_limits::eof(target)) {
      _doc = _skip.Reader().State().doc;
    }
    return target;
  }

  doc_id_t ShallowSeekToBlock(doc_id_t target) {
    if (!_skip.NumLevels()) [[unlikely]] {
      return doc_limits::eof();
    }
    _skip.Reader().EnsureSorted();
    const auto upper_bound = _skip.Reader().UpperBound();
    if (upper_bound >= target) {
      return upper_bound;
    }
    _left_in_list = _skip.Seek(target);
    _left_in_leaf = 0;
    _needs_reposition = true;
    return _skip.Reader().UpperBound();
  }

  std::pair<doc_id_t, bool> FillBlock(const doc_id_t min, const doc_id_t max,
                                      uint64_t* IRS_RESTRICT const doc_mask,
                                      FillBlockScoreContext score,
                                      FillBlockMatchContext match) final;

  template<typename DocsContainer, typename ScoresContainer>
  void CollectRange(DocsContainer& docs, ScoresContainer& scores,
                    const ScoreFunction& scorer, ColumnArgsFetcher* fetcher,
                    doc_id_t min, doc_id_t max);

  // Score candidate docs one by one: seek to each, score, accumulate.
  // Score non-essential candidates. Handles seek + SetSkipWandBelow internally.
  // If required=true, compacts out non-matching candidates and resizes buffers.
  template<typename DocsBuffer, typename ScoresBuffer>
  void ScoreCandidates(DocsBuffer& cand_docs, ScoresBuffer& cand_scores,
                       const ScoreFunction& scorer, ColumnArgsFetcher* fetcher,
                       bool required, doc_id_t window_max);

 private:
  IRS_FORCE_INLINE InputType& GetDocIn() const noexcept {
    return sdb::basics::downCast<InputType>(*this->_doc_in);
  }

  template<size_t N>
  IRS_FORCE_INLINE const score_t* ScoreBlock(std::span<const doc_id_t, N> docs,
                                             const ScoreFunction& score,
                                             ColumnArgsFetcher* fetcher);

  IRS_FORCE_INLINE void ReadBlock(doc_id_t prev_doc);
  void PrepareSkipReader(uint64_t skip_offs, uint32_t docs_count);

  template<ScoreMergeType MergeType, bool FillMask, size_t N>
  bool ProcessBatch(std::span<const doc_id_t, N> docs, const doc_id_t min,
                    uint64_t* IRS_RESTRICT doc_mask,
                    FillBlockScoreContext score);

  using Attributes = AttributesImpl<IteratorTraits>;

  FieldProperties _field;
  const byte_type* _stats = nullptr;
  score_t _boost = kNoBoost;

  uint32_t _enc_buf[doc_limits::kBlockSize];
  uint32_t* _collected_freqs = nullptr;
  [[no_unique_address]] uint32_t _freqs[doc_limits::kBlockSize];
  doc_id_t _docs[doc_limits::kBlockSize];
#ifdef __AVX2__
  [[maybe_unused]] doc_id_t _placeholder_for_bitset_materialize[8];
#endif
  doc_id_t _max_in_leaf = doc_limits::invalid();
  uint32_t _left_in_leaf = 0;
  uint32_t _left_in_list = 0;
  bool _needs_reposition = false;
  IndexInput::ptr _doc_in;
  Attributes _attrs;
  SkipReader<WandReadSkip, InputType> _skip;
  uint64_t _deferred_skip_offs = 0;
  uint32_t _deferred_skip_docs_count = 0;
};

// TODO(gnusi): Deduplicate ScoreBlock and Collect at least
template<typename IteratorTraits, bool Root, bool Pos, bool Offs,
         typename WandExtent, typename InputType>
template<size_t N>
const score_t*
SingleWandIterator<IteratorTraits, Root, Pos, Offs, WandExtent,
                   InputType>::ScoreBlock(std::span<const doc_id_t, N> docs,
                                          const ScoreFunction& score,
                                          ColumnArgsFetcher* fetcher) {
  if constexpr (N == kPostingBlock) {
    SDB_ASSERT(std::data(_docs) == docs.data());
    if (fetcher) {
      fetcher->FetchPostingBlock(docs);
    }
    std::get<FreqBlockAttr>(_attrs).value = std::begin(_freqs);
    auto* p = reinterpret_cast<score_t*>(std::begin(_enc_buf));
    score.ScorePostingBlock(p);
    return p;
  } else {
    SDB_ASSERT(std::data(_docs) <= docs.data());
    SDB_ASSERT(docs.data() <= std::data(_docs) + std::size(_docs));
    if (fetcher) {
      fetcher->Fetch(docs);
    }
    const auto offset = docs.data() - std::data(_docs);
    std::get<FreqBlockAttr>(_attrs).value = std::begin(_freqs) + offset;
    // TODO(mbkkt) use offset here?
    auto* p = reinterpret_cast<score_t*>(std::end(_enc_buf) - docs.size());
    score.Score(p, docs.size());
    return p;
  }
}

template<typename IteratorTraits, bool Root, bool Pos, bool Offs,
         typename WandExtent, typename InputType>
void SingleWandIterator<IteratorTraits, Root, Pos, Offs, WandExtent,
                        InputType>::Collect(const ScoreFunction& scorer,
                                            ColumnArgsFetcher& fetcher,
                                            ScoreCollector& collector) {
  ResolveScoreCollector(collector, [&](auto& collector) IRS_FORCE_INLINE {
    auto process_block = [&]<size_t N>(size_t left_in_leaf) IRS_FORCE_INLINE {
      std::span<const doc_id_t, N> docs{std::end(_docs) - left_in_leaf,
                                        left_in_leaf};
      const auto* scores = ScoreBlock(docs, scorer, &fetcher);
      // TODO(mbkkt): bulk threshold check will make it faster
      for (size_t i = 0; i < docs.size(); ++i) {
        collector.Add(scores[i], docs[i]);
      }
    };

    if (_left_in_leaf != 0) [[unlikely]] {
      process_block.template operator()<std::dynamic_extent>(_left_in_leaf);
      _left_in_leaf = 0;
    } else {
      *(std::end(_docs) - 1) = _doc;
    }

    SDB_ASSERT(_left_in_leaf == 0);
    while (_left_in_list != 0) {
      auto last_doc = *(std::end(_docs) - 1);
      if (last_doc + 1 > _skip.Reader().UpperBound()) {
        _left_in_list = _skip.Seek(last_doc + 1);
        auto& state = _skip.Reader().State();
        if (state.doc_ptr) [[likely]] {
          GetDocIn().Seek(state.doc_ptr);
        }
        last_doc = state.doc;
      }
      ReadBlock(last_doc);
      if (_left_in_leaf == kPostingBlock) {
        process_block.template operator()<kPostingBlock>(kPostingBlock);
      } else {
        process_block.template operator()<std::dynamic_extent>(_left_in_leaf);
        _left_in_leaf = 0;
      }
    }
  });
  _doc = doc_limits::eof();
}

template<typename IteratorTraits, bool Root, bool Pos, bool Offs,
         typename WandExtent, typename InputType>
template<ScoreMergeType MergeType, bool FillMask, size_t N>
bool SingleWandIterator<
  IteratorTraits, Root, Pos, Offs, WandExtent,
  InputType>::ProcessBatch(std::span<const doc_id_t, N> docs,
                           const doc_id_t min, uint64_t* IRS_RESTRICT doc_mask,
                           FillBlockScoreContext score) {
  auto* IRS_RESTRICT const score_window = score.score_window;
  const score_t* IRS_RESTRICT score_ptr =
    ScoreBlock(docs, *score.score, score.fetcher);

  for (size_t i = 0; i < docs.size(); ++i) {
    const size_t offset = docs[i] - min;
    if constexpr (FillMask) {
      SetBit(doc_mask[offset / BitsRequired<uint64_t>()],
             offset % BitsRequired<uint64_t>());
    }
    if constexpr (MergeType != ScoreMergeType::Noop) {
      Merge<MergeType>(score_window[offset], score_ptr[i]);
    }
  }
  return false;
}

template<typename IteratorTraits, bool Root, bool Pos, bool Offs,
         typename WandExtent, typename InputType>
template<typename DocsContainer, typename ScoresContainer>
void SingleWandIterator<IteratorTraits, Root, Pos, Offs, WandExtent,
                        InputType>::CollectRange(DocsContainer& out_docs,
                                                 ScoresContainer& out_scores,
                                                 const ScoreFunction& scorer,
                                                 ColumnArgsFetcher* fetcher,
                                                 doc_id_t min, doc_id_t max) {
  // Iterator already past window — nothing to do.
  if (value() >= max) [[unlikely]] {
    return;
  }

  auto process_batch = [&]<size_t N>(std::span<const doc_id_t, N> docs) {
    const auto old_size = out_docs.size();
    const auto new_size = old_size + docs.size();
    SDB_ASSERT(new_size <= out_docs.capacity());

    out_docs.resize(new_size);
    std::memcpy(out_docs.data() + old_size, docs.data(),
                docs.size() * sizeof(doc_id_t));

    auto* scores = out_scores.data() + out_scores.size();

    if constexpr (N == kPostingBlock) {
      SDB_ASSERT(std::data(_docs) == docs.data());
      if (fetcher) {
        fetcher->FetchPostingBlock(docs);
      }
      if constexpr (IteratorTraits::Frequency()) {
        std::get<FreqBlockAttr>(_attrs).value = std::begin(_freqs);
      }
      out_scores.resize(out_scores.size() + docs.size());
      scorer.ScorePostingBlock(scores);
    } else {
      SDB_ASSERT(std::data(_docs) <= docs.data());
      SDB_ASSERT(docs.data() <= std::data(_docs) + std::size(_docs));
      if (fetcher) {
        fetcher->Fetch(docs);
      }
      if constexpr (IteratorTraits::Frequency()) {
        const auto offset = docs.data() - std::data(_docs);
        std::get<FreqBlockAttr>(_attrs).value = std::begin(_freqs) + offset;
      }
      out_scores.resize(out_scores.size() + docs.size());
      scorer.Score(scores, docs.size());
    }
  };

  // ShallowSeekToBlock may have repositioned the skip reader without
  // updating the doc stream.  Reposition and decode the first block.
  if (_needs_reposition && _left_in_list != 0) [[unlikely]] {
    _needs_reposition = false;
    auto& state = _skip.Reader().State();
    if (state.doc_ptr) [[likely]] {
      GetDocIn().Seek(state.doc_ptr);
    }
    ReadBlock(state.doc);
    // The decoded block may contain docs before value()/min.
    // Find the first doc >= min and use that as our leftover range.
    const auto* first_valid =
      std::find_if(std::end(_docs) - _left_in_leaf, std::end(_docs),
                   [&](doc_id_t doc) { return doc >= min; });
    _left_in_leaf = static_cast<uint32_t>(std::end(_docs) - first_valid);
    // Now _left_in_leaf points to docs >= min.  Fall through to the
    // normal leftover path which will handle the remaining logic.
  }

  SDB_ASSERT(_left_in_leaf <= kPostingBlock);

  // leftover from current decoded block
  {
    auto count = _left_in_leaf;

    // Include value() if it sits at _docs[end - count - 1]
    if (count < kPostingBlock && *(std::end(_docs) - count - 1) == value()) {
      ++count;
    }

    if (count > 0) {
      if (*(std::end(_docs) - 1) >= max) {
        _left_in_leaf = count;
        goto collect_range_tail;
      }
      process_batch(std::span<const doc_id_t>{std::end(_docs) - count, count});
    }
  }

  // full blocks only
  for (;;) {
    if (_left_in_list == 0) [[unlikely]] {
      _left_in_leaf = 0;
      goto collect_range_done;
    }
    ReadBlock(*(std::end(_docs) - 1));
    if (*(std::end(_docs) - 1) >= max || _left_in_leaf != kPostingBlock) {
      goto collect_range_tail;
    }
    process_batch(std::span<const doc_id_t, kPostingBlock>{std::begin(_docs),
                                                           kPostingBlock});
  }

collect_range_tail: {
  const auto* begin = std::end(_docs) - _left_in_leaf;
  const auto* tail_end = std::find_if(begin, std::cend(_docs),
                                      [&](doc_id_t doc) { return doc >= max; });
  if (tail_end != begin) {
    process_batch(std::span{begin, tail_end});
  }
  _left_in_leaf = static_cast<uint32_t>(std::end(_docs) - tail_end);
}

collect_range_done:
  if (_left_in_leaf > 0) {
    _doc = *(std::end(_docs) - _left_in_leaf);
    --_left_in_leaf;
  } else {
    _doc = doc_limits::eof();
  }

  if constexpr (IteratorTraits::Frequency()) {
    std::get<FreqBlockAttr>(_attrs).value = _collected_freqs;
  }
}

template<typename IteratorTraits, bool Root, bool Pos, bool Offs,
         typename WandExtent, typename InputType>
template<typename DocsBuffer, typename ScoresBuffer>
void SingleWandIterator<IteratorTraits, Root, Pos, Offs, WandExtent,
                        InputType>::ScoreCandidates(DocsBuffer& cand_docs,
                                                    ScoresBuffer& cand_scores,
                                                    const ScoreFunction& scorer,
                                                    ColumnArgsFetcher* fetcher,
                                                    bool required,
                                                    doc_id_t window_max) {
  SDB_ASSERT(!cand_docs.empty());

  size_t out = 0;  // compacted output index (used when required=true)
  SetSkipWandBelow(window_max);

  Finally unset = [&] noexcept {
    SetSkipWandBelow(0);
    if (required) {
      cand_docs.resize(out);
      cand_scores.resize(out);
    }
  };

  // TODO(gnusi): this is clearly redundant, but some heavy queries become
  // considerably slower, while smaller becoming faster. It needs to be
  // carefully integrated into the iteration logic below.
  if (value() < cand_docs[0]) {
    seek(cand_docs[0]);
  }

  const size_t cand_count = cand_docs.size();
  const doc_id_t max = cand_docs[cand_count - 1] + 1;

  if (value() >= max) [[unlikely]] {
    return;
  }

  doc_id_t docs[kScoreBlock];
  uint32_t freqs[kScoreBlock];
  size_t indices[kScoreBlock];
  size_t count = 0;
  if constexpr (IteratorTraits::Frequency()) {
    std::get<FreqBlockAttr>(_attrs).value = freqs;
  }

  auto score_block = [&]<size_t N>(std::span<const doc_id_t, N> docs) {
    SDB_ASSERT(!docs.empty());
    if (fetcher) {
      fetcher->Fetch(docs);
    }
    auto* p = reinterpret_cast<score_t*>(std::end(_enc_buf) - docs.size());
    if constexpr (N == kScoreBlock) {
      scorer.ScoreBlock(p);
    } else {
      scorer.Score(p, docs.size());
    }
    for (size_t j = 0; j < docs.size(); ++j) {
      cand_scores[indices[j]] += p[j];
    }
    count = 0;
  };

  size_t cand_idx = 0;

  // Find candidates in a decoded block using linear scan, batch matches.
  auto find_in_block = [&](const doc_id_t* begin, const doc_id_t* end) {
    while (cand_idx < cand_count && begin < end) {
      const doc_id_t cand = cand_docs[cand_idx];
      if (cand > *(end - 1)) {
        break;
      }
      auto* it = std::find(begin, end, cand);
      if (it != end) {
        if (required) {
          // Compact: move matched candidate to output position.
          cand_docs[out] = cand_docs[cand_idx];
          cand_scores[out] = cand_scores[cand_idx];
          indices[count] = out;
          ++out;
        } else {
          indices[count] = cand_idx;
        }
        const auto freq_idx = static_cast<size_t>(it - std::begin(_docs));
        docs[count] = cand;
        if constexpr (IteratorTraits::Frequency()) {
          freqs[count] = _freqs[freq_idx];
        }
        ++count;
        if (count == kScoreBlock) {
          score_block(
            std::span<const doc_id_t, kScoreBlock>{docs, kScoreBlock});
        }
        begin = it + 1;
      }
      ++cand_idx;
    }
  };

  // Reposition if needed (same logic as CollectRange).
  if (_needs_reposition && _left_in_list != 0) [[unlikely]] {
    _needs_reposition = false;
    auto& state = _skip.Reader().State();
    if (state.doc_ptr) [[likely]] {
      GetDocIn().Seek(state.doc_ptr);
    }
    ReadBlock(state.doc);
    const auto* first_valid =
      std::find_if(std::end(_docs) - _left_in_leaf, std::end(_docs),
                   [&](doc_id_t doc) { return doc >= cand_docs[0]; });
    _left_in_leaf = static_cast<uint32_t>(std::end(_docs) - first_valid);
  }

  SDB_ASSERT(_left_in_leaf <= kPostingBlock);

  // Leftover from current decoded block.
  {
    auto count = _left_in_leaf;
    if (count < kPostingBlock && *(std::end(_docs) - count - 1) == value()) {
      ++count;
    }
    if (count > 0) {
      const auto* begin = std::end(_docs) - count;
      if (*(std::end(_docs) - 1) >= max) {
        _left_in_leaf = count;
        goto score_cand_tail;
      }
      find_in_block(begin, std::end(_docs));
      if (cand_idx >= cand_count) {
        goto score_cand_done;
      }
    }
  }

  // Full blocks.
  for (;;) {
    if (_left_in_list == 0) [[unlikely]] {
      _left_in_leaf = 0;
      goto score_cand_done;
    }
    // Skip ahead if next candidate is beyond the current block's upper bound.
    {
      const doc_id_t next_cand = cand_docs[cand_idx];
      const doc_id_t last_doc = *(std::end(_docs) - 1);
      if (next_cand > last_doc + kPostingBlock &&
          next_cand > _skip.Reader().UpperBound()) {
        _left_in_list = _skip.Seek(next_cand);
        auto& state = _skip.Reader().State();
        if (state.doc_ptr) [[likely]] {
          GetDocIn().Seek(state.doc_ptr);
        }
        ReadBlock(state.doc);
      } else {
        ReadBlock(last_doc);
      }
    }
    if (*(std::end(_docs) - 1) >= max || _left_in_leaf != kPostingBlock) {
      goto score_cand_tail;
    }
    find_in_block(std::begin(_docs), std::begin(_docs) + kPostingBlock);
    if (cand_idx >= cand_count) {
      goto score_cand_done;
    }
  }

score_cand_tail: {
  const auto* begin = std::end(_docs) - _left_in_leaf;
  const auto* tail_end = std::find_if(begin, std::cend(_docs),
                                      [&](doc_id_t doc) { return doc >= max; });
  if (tail_end != begin) {
    find_in_block(begin, tail_end);
  }
  _left_in_leaf = static_cast<uint32_t>(std::end(_docs) - tail_end);
}

score_cand_done:
  if (count > 0) {
    score_block(std::span<const doc_id_t>{docs, count});
  }

  if (_left_in_leaf > 0) {
    _doc = *(std::end(_docs) - _left_in_leaf);
    --_left_in_leaf;
  } else if (_left_in_list > 0) {
    _doc = *(std::end(_docs) - 1);
  } else {
    _doc = doc_limits::eof();
  }

  if constexpr (IteratorTraits::Frequency()) {
    std::get<FreqBlockAttr>(_attrs).value = _collected_freqs;
  }
}

template<typename IteratorTraits, bool Root, bool Pos, bool Offs,
         typename WandExtent, typename InputType>
std::pair<doc_id_t, bool>
SingleWandIterator<IteratorTraits, Root, Pos, Offs, WandExtent,
                   InputType>::FillBlock(const doc_id_t min, const doc_id_t max,
                                         uint64_t* IRS_RESTRICT const doc_mask,
                                         FillBlockScoreContext score,
                                         FillBlockMatchContext) {
  SDB_ASSERT(!IteratorTraits::Position());
  SDB_ASSERT(min < max);
  SDB_ASSERT(value() >= min);
  SDB_ASSERT(score.score && !score.score->IsDefault());
  SDB_ASSERT(score.merge_type == ScoreMergeType::Sum);

  // Iterator already past window — nothing to do.
  if (value() >= max) [[unlikely]] {
    return std::pair{_doc, true};
  }

  return ResolveBool(doc_mask != nullptr, [&]<bool FillMask> {
    bool empty = true;

    // ShallowSeekToBlock may have repositioned the skip reader without
    // updating the doc stream.  Reposition and decode the first block.
    if (_needs_reposition && _left_in_list != 0) [[unlikely]] {
      _needs_reposition = false;
      auto& state = _skip.Reader().State();
      if (state.doc_ptr) [[likely]] {
        GetDocIn().Seek(state.doc_ptr);
      }
      ReadBlock(state.doc);
      // The decoded block may contain docs before value()/min.
      // Find the first doc >= min and use that as our leftover range.
      const auto* first_valid =
        std::find_if(std::end(_docs) - _left_in_leaf, std::end(_docs),
                     [&](doc_id_t doc) { return doc >= min; });
      _left_in_leaf = static_cast<uint32_t>(std::end(_docs) - first_valid);
      // Now _left_in_leaf points to docs >= min.  Fall through to the
      // normal leftover path which will handle the remaining logic.
    }

    SDB_ASSERT(_left_in_leaf <= kPostingBlock);

    // leftover from current decoded block
    {
      auto count = _left_in_leaf;

      // Include value() if it sits at _docs[end - count - 1]
      if (count < kPostingBlock && *(std::end(_docs) - count - 1) == value()) {
        ++count;
      }

      if (count > 0) {
        if (*(std::end(_docs) - 1) >= max) {
          _left_in_leaf = count;
          goto fill_block_tail;
        }
        empty &= ProcessBatch<ScoreMergeType::Sum, FillMask>(
          std::span<const doc_id_t>{std::end(_docs) - count, count}, min,
          doc_mask, score);
      }
    }

    // full blocks only
    for (;;) {
      if (_left_in_list == 0) [[unlikely]] {
        _left_in_leaf = 0;
        goto fill_block_done;
      }
      ReadBlock(*(std::end(_docs) - 1));
      if (*(std::end(_docs) - 1) >= max || _left_in_leaf != kPostingBlock) {
        goto fill_block_tail;
      }
      empty &= ProcessBatch<ScoreMergeType::Sum, FillMask>(
        std::span<const doc_id_t, kPostingBlock>{std::begin(_docs),
                                                 kPostingBlock},
        min, doc_mask, score);
    }

  fill_block_tail: {
    const auto* begin = std::end(_docs) - _left_in_leaf;
    const auto* tail_end = std::find_if(
      begin, std::cend(_docs), [&](doc_id_t doc) { return doc >= max; });
    if (tail_end != begin) {
      empty &= ProcessBatch<ScoreMergeType::Sum, FillMask>(
        std::span{begin, tail_end}, min, doc_mask, score);
    }
    _left_in_leaf = static_cast<uint32_t>(std::end(_docs) - tail_end);
  }

  fill_block_done:
    if (_left_in_leaf > 0) {
      _doc = *(std::end(_docs) - _left_in_leaf);
      --_left_in_leaf;
    } else {
      _doc = doc_limits::eof();
    }

    if constexpr (IteratorTraits::Frequency()) {
      std::get<FreqBlockAttr>(_attrs).value = _collected_freqs;
    }
    return std::pair{_doc, empty};
  });
}

template<typename IteratorTraits, bool Root, bool Pos, bool Offs,
         typename WandExtent, typename InputType>
doc_id_t SingleWandIterator<IteratorTraits, Root, Pos, Offs, WandExtent,
                            InputType>::seek(doc_id_t target) {
  if (target <= _doc) [[unlikely]] {
    return _doc;
  }

  if (_skip.Reader().IsLessThanUpperBound(target)) [[unlikely]] {
    SeekToBlock(target);
  }

  // Position from skip state if no decoded docs remain.
  if (_left_in_leaf == 0) [[unlikely]] {
    if (_left_in_list == 0) [[unlikely]] {
      return _doc = doc_limits::eof();
    }

    if (_needs_reposition) {
      _needs_reposition = false;
      auto& state = _skip.Reader().State();
      if (state.doc_ptr) [[likely]] {
        GetDocIn().Seek(state.doc_ptr);
      }
      _doc = state.doc;
    }
    ReadBlock(_doc);
  }

  for (;;) {
    while (_left_in_leaf != 0) {
      const auto doc = *(std::end(_docs) - _left_in_leaf);

      --_left_in_leaf;

      if (target <= doc) {
        return _doc = doc;
      }
    }

    // Block exhausted without finding target. Read next block from doc
    // stream. Handles the case where ShallowSeekToBlock advanced the skip
    // reader past the current decoded block.
    if (_left_in_list == 0) [[unlikely]] {
      return _doc = doc_limits::eof();
    }
    ReadBlock(*(std::end(_docs) - 1));
  }
}

template<typename FormatTraits, bool Root, bool Pos, bool Offs,
         typename WandExtent, typename InputType>
void SingleWandIterator<FormatTraits, Root, Pos, Offs, WandExtent,
                        InputType>::Prepare(const PostingCookie& meta,
                                            const IndexInput* doc_in,
                                            uint8_t wand_index) {
  Init(meta);

  _skip.Reader().SetWandIndex(wand_index);

  // Set default wand state with max score so no blocks are ever pruned
  _skip.Reader().SetWandScore(
    ScoreFunction::Constant(std::numeric_limits<score_t>::max()),
    std::make_unique<DefaultWandSource>());

  auto& term_state = sdb::basics::downCast<CookieImpl>(meta.cookie)->meta;
  std::get<CostAttr>(_attrs).reset(term_state.docs_count);

  if (term_state.docs_count > 1) {
    _left_in_list = term_state.docs_count;
    SDB_ASSERT(_left_in_leaf == 0);
    SDB_ASSERT(_max_in_leaf == doc_limits::invalid());

    if (!_doc_in) {
      _doc_in = doc_in->Reopen();

      if (!_doc_in) {
        SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
                  "Failed to reopen document input");
        throw IoError("failed to reopen document input");
      }
    }

    auto& freq_block = std::get<FreqBlockAttr>(_attrs);
    _collected_freqs = std::allocator<uint32_t>{}.allocate(kScoreBlock);
    freq_block.value = _collected_freqs;

    GetDocIn().Seek(term_state.doc_start);
    SDB_ASSERT(!GetDocIn().IsEOF());
  } else {
    SDB_ASSERT(term_state.docs_count == 1);
    auto* doc = std::end(_docs) - 1;
    *doc = doc_limits::min() + term_state.e_single_doc;

    auto* freq = std::end(_freqs) - 1;
    *freq = term_state.freq;
    _collected_freqs = freq;

    auto& freq_block = std::get<FreqBlockAttr>(_attrs);
    freq_block.value = freq;

    _left_in_list = 0;
    _left_in_leaf = 1;
    _max_in_leaf = *doc;
  }

  SDB_ASSERT(term_state.freq);

  if (term_state.docs_count > doc_limits::kBlockSize) {
    _skip.Reader().Enable(term_state);
    _deferred_skip_offs = term_state.doc_start + term_state.e_skip_start;
    _deferred_skip_docs_count = term_state.docs_count;
  } else if (1 < term_state.docs_count &&
             term_state.docs_count < doc_limits::kBlockSize) {
    _skip.Reader().SkipWandData(GetDocIn());
  }
}

template<typename FormatTraits, bool Root, bool Pos, bool Offs,
         typename WandExtent, typename InputType>
void SingleWandIterator<FormatTraits, Root, Pos, Offs, WandExtent,
                        InputType>::ReadBlock(doc_id_t prev_doc) {
  if (const auto tail = _left_in_list; tail >= doc_limits::kBlockSize)
    [[likely]] {
    IteratorTraits::ReadBlockDelta(GetDocIn(), _enc_buf, _docs, prev_doc);
    _max_in_leaf = *(std::end(_docs) - 1);
    _left_in_leaf = doc_limits::kBlockSize;
    _left_in_list -= doc_limits::kBlockSize;
    IteratorTraits::ReadBlock(GetDocIn(), _enc_buf, _freqs);
  } else {
    IteratorTraits::ReadTailDelta(tail, GetDocIn(), _enc_buf, _docs, prev_doc);
    _max_in_leaf = *(std::end(_docs) - 1);
    _left_in_leaf = tail;
    _left_in_list = 0;
    IteratorTraits::ReadTail(tail, GetDocIn(), _enc_buf, _freqs);
  }
}

template<typename FormatTraits, bool Root, bool Pos, bool Offs,
         typename WandExtent, typename InputType>
void SingleWandIterator<FormatTraits, Root, Pos, Offs, WandExtent,
                        InputType>::PrepareSkipReader(uint64_t skip_offs,
                                                      uint32_t docs_count) {
  SDB_ASSERT(docs_count > 0);

  std::unique_ptr<InputType> skip_in_ptr{
    sdb::basics::downCast<InputType>(GetDocIn().Dup().release())};
  if (!skip_in_ptr) {
    SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
              "Failed to duplicate document input");
    throw IoError("Failed to duplicate document input");
  }
  auto& skip_in = *skip_in_ptr;

  SDB_ASSERT(!_skip.NumLevels());
  skip_in.Seek(skip_offs);
  const auto global_max_score = _skip.Reader().ReadWandScore(skip_in);
  _skip.Prepare(std::move(skip_in_ptr), docs_count);

  if (const auto num_levels = _skip.NumLevels();
      0 < num_levels && num_levels <= doc_limits::kMaxSkipLevels) [[likely]] {
    SDB_ASSERT(!doc_limits::valid(_skip.Reader().UpperBound()));
    _skip.Reader().Init(num_levels, global_max_score);
  } else {
    SDB_ASSERT(false);
    throw IndexError{absl::StrCat("Invalid number of skip levels ", num_levels,
                                  ", must be in range of [1, ",
                                  doc_limits::kMaxSkipLevels, "].")};
  }
}

}  // namespace irs
