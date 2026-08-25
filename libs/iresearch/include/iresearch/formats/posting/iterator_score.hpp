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
#include "iresearch/formats/posting/skip_column.hpp"
#include "iresearch/index/index_reader.hpp"

namespace irs {

IRS_FORCE_INLINE score_t ReadScoreBound(const ScoreFunction& func,
                                        ScoreBoundSource& ctx, DataInput& in) {
  const auto size = in.ReadByte();
  ctx.Read(in, size);
  return func.Score();
}

template<typename FormatTraits>
using ScoreBoundTraits = IteratorTraitsImpl<FormatTraits, true, false, false>;

template<typename FormatTraits, bool Root, bool Pos, bool Offs,
         typename InputType>
class SinglePruningIterator : public DocIterator {
  using IteratorTraits = ScoreBoundTraits<FormatTraits>;
  using FieldTraits = IteratorTraitsImpl<FormatTraits, true, Pos, Offs>;

  class DefaultScoreBoundSource final : public ScoreBoundSource {
   public:
    Attribute* GetMutable(TypeInfo::type_id) noexcept final { return nullptr; }
    void Read(DataInput& in, size_t size) final {
      while (size--) {
        in.ReadByte();
      }
    }
    void Set(ScoreBound) noexcept final {}
  };

 public:
  static_assert(doc_limits::kBlockSize % kScoreBlock == 0,
                "kBlockSize must be a multiple of kScoreBlock");

  explicit SinglePruningIterator() = default;

  ScoreFunction PrepareScore(const PrepareScoreContext& ctx) final {
    SDB_ASSERT(ctx.scorer);
    if (auto bound_source = ctx.scorer->PrepareScoreBoundSource()) {
      auto bound_func = ctx.scorer->PrepareScorer({
        .segment = *ctx.segment,
        .field = _field,
        .doc_attrs = *bound_source,
        .stats = _stats,
        .boost = _boost,
      });
      _skip.SetScoreBoundScorer(std::move(bound_func),
                                         std::move(bound_source));
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

  void Prepare(const PostingCookie& meta, const IteratorFieldOptions& options,
               const IndexInput* doc_in, const IndexInput* skip_in);

  void SetSkipBoundsBelow(doc_id_t max) noexcept {
    _skip.SetSkipBoundsBelow(max);
  }

  IRS_NO_INLINE Attribute* GetMutable(TypeInfo::type_id type) noexcept final {
    if (type == irs::Type<ScoreThresholdAttr>::id()) {
      return &_skip.Threshold();
    }
    return irs::GetMutable(_attrs, type);
  }

  IRS_FORCE_INLINE doc_id_t advance() final { return seek(value() + 1); }

  IRS_FORCE_INLINE doc_id_t seek(doc_id_t target) final;

  IRS_FORCE_INLINE doc_id_t LazySeek(doc_id_t target) final {
    return seek(target);
  }

  uint32_t count() final {
    _doc = doc_limits::eof();
    const auto left_in_leaf = std::exchange(_left_in_leaf, 0);
    const auto left_in_list = std::exchange(_left_in_list, 0);
    return left_in_leaf + left_in_list;
  }

  uint32_t EmitScoredDocs(doc_id_t* out, score_t* scores, doc_id_t max,
                          const ScoreFunction& scorer,
                          ColumnArgsFetcher* fetcher, doc_id_t min) final {
    // Single-term entry only (multi-term drives the children's
    // ForEachScoredBlock directly, so this never runs for them). Position to
    // the window exactly as max_score does for its essentials; fires once, on
    // the first (fresh) window where value() is still unpositioned.
    seek(min);
    uint32_t count = 0;
    ForEachScoredBlock(
      scorer, fetcher, max,
      [&]<size_t N>(std::span<doc_id_t, N> docs, const score_t* block)
        IRS_FORCE_INLINE {
          std::memcpy(out + count, docs.data(), docs.size() * sizeof(doc_id_t));
          std::memcpy(scores + count, block, docs.size() * sizeof(score_t));
          count += static_cast<uint32_t>(docs.size());
        });
    return count;
  }

  uint32_t EmitDocs(doc_id_t* out, doc_id_t min, doc_id_t max) final {
    return EmitDocsImpl(*this, out, min, max);
  }

  void Collect(const ScoreFunction& scorer, ColumnArgsFetcher& fetcher,
               ScoreCollector& collector) final;

  void FetchScoreArgs(uint16_t index) final {
    SDB_ASSERT(index < kScoreBlock);
    SDB_ASSERT(_left_in_leaf < doc_limits::kBlockSize);
    _freq_block[index] = *(std::end(_freqs) - _left_in_leaf - 1);
  }

  void Init(const PostingCookie& cookie) noexcept {
    _field = cookie.field;
    _stats = cookie.stats;
    _boost = cookie.boost;
  }

 private:
  // The term's skip entries, one per doc block, as a flat column read.
  //
  // The old shape had a skip level per granularity and pruned by dropping
  // whole levels whose bound fell under the threshold. There is one
  // granularity now -- the doc block -- so pruning is block-max: a seek walks
  // past blocks whose bound cannot beat the threshold and stops at the first
  // one that can.
  class SkipBounds {
   public:
    void SetScoreBoundScorer(ScoreFunction func,
                             ScoreBoundSource::ptr bound_source) noexcept {
      _bound_func = std::move(func);
      _bound_source = std::move(bound_source);
      // The real scorer arrives after `Prepare`, so the term's bound has to
      // be turned into a score again.
      RecomputeGlobal();
    }

    void SetSkipBoundsBelow(doc_id_t max) noexcept { _skip_bounds_below = max; }
    doc_id_t SkipBoundsBelow() const noexcept { return _skip_bounds_below; }
    ScoreThresholdAttr& Threshold() noexcept { return _threshold; }
    void EnsureSorted() const noexcept {}

    // Kept so the call sites read the same as before; there are no levels to
    // count, only whether this term has entries at all.
    size_t NumLevels() const noexcept { return _entry_last >= _entry_first; }

    void Prepare(const IteratorFieldOptions& opts, const PostingMeta& state,
                 uint64_t doc_origin, InputType& skip_in) {
      SDB_ASSERT(state.docs_count > doc_limits::kBlockSize);
      _idx = SkipColumnIndex{opts.skip.columns, FieldTraits::Position(),
                             FieldTraits::Offset()};
      _cols.Prepare(opts.skip.dir, opts.skip.count, opts.skip.columns,
                    opts.skip.origin, skip_in);
      _doc_origin = doc_origin;
      _docs_total = state.docs_count;
      _entry_first = state.first_entry;
      _entry_last =
        _entry_first +
        math::DivCeil32(state.docs_count, doc_limits::kBlockSize) - 1;
      _next = _entry_first;
      _upper = doc_limits::invalid();
      // Entry 0 carries the term's bound rather than block 0's own.
      _root.freq = _cols.Get(_idx.bfreq, _entry_first);
      _root.delta =
        _idx.has_norm ? _cols.Get(_idx.bdelta, _entry_first) : uint32_t{0};
      _has_root = _idx.has_bound;
      RecomputeGlobal();
    }

    uint64_t DocStart() {
      SDB_ASSERT(NumLevels());
      return _doc_origin + _cols.GetOnce(_idx.docoff, _entry_first);
    }

    IRS_FORCE_INLINE doc_id_t UpperBound() const noexcept { return _upper; }

    // The read path also advances a block at a time without seeking, which
    // leaves the resolved block behind the one being read -- and then every
    // question about the current window has to scan forward to catch up.
    // Blocks are only ever left behind, so catching up is O(1).
    void SyncTo(uint64_t block) {
      const auto k = _entry_first + block;
      if (k <= _cur || k > _entry_last) {
        return;
      }
      _cur = k;
      _next = k + 1;
      _upper = _cols.Get(_idx.docs, k);
      _score = Bound(k);
    }

    IRS_FORCE_INLINE bool IsLessThanUpperBound(doc_id_t target) const noexcept {
      if constexpr (Root) {
        return _upper < target || _score <= _threshold.value;
      } else {
        return _upper < target;
      }
    }

    SkipState& State() noexcept { return _state; }

    // The caller seeks to the window's start and then asks what the term can
    // score anywhere in it. Inside the resolved block that is just its bound;
    // when the window reaches past it, the answer is the largest bound among
    // the blocks the window actually touches -- which the column already
    // holds, so it is exact rather than the coarse level v1 fell back to.
    // Inside the block the reader has reached, that block's own bound. Past
    // it, the term's bound, which entry 0 carries.
    IRS_FORCE_INLINE score_t GetMaxScore(doc_id_t doc) noexcept {
      return doc <= _upper ? _score : _global_max_score;
    }

    // Positions at the first block that can hold `target` and, when pruning,
    // whose bound can still beat the threshold. Returns how many documents
    // remain from that block on, 0 once the term is exhausted.
    uint32_t Seek(doc_id_t target) {
      auto k = SkipColumnSeek(_cols, _next, _entry_last + 1, target);
      if constexpr (Root) {
        // Step past blocks that cannot beat the threshold. Each step reads
        // columns already decoded for the group.
        while (k <= _entry_last && Bound(k) <= _threshold.value) {
          ++k;
        }
      }
      if (k > _entry_last) [[unlikely]] {
        _upper = doc_limits::eof();
        _score = std::numeric_limits<score_t>::max();
        _next = _entry_last + 1;
        return 0;
      }

      const auto block = k - _entry_first;
      _state.doc = block == 0 ? doc_limits::invalid()
                              : _cols.Get(_idx.docs, k - 1);
      _state.doc_ptr = _doc_origin + _cols.Get(_idx.docoff, k);
      if constexpr (IteratorTraits::Position()) {
        _state.pos_ptr = _pos_start + _cols.Get(_idx.posoff, k);
        _state.pos_offset = _cols.Get(_idx.posslot, k);
        if constexpr (IteratorTraits::Offset()) {
          _state.pay_ptr = _pay_start + _cols.Get(_idx.payoff, k);
        }
      }
      _upper = _cols.Get(_idx.docs, k);
      _score = Bound(k);
      _cur = k;
      _next = k + 1;
      return _docs_total -
             static_cast<uint32_t>(block) * doc_limits::kBlockSize;
    }

   private:
    void RecomputeGlobal() noexcept {
      if (!_has_root || !_bound_source) {
        _global_max_score = std::numeric_limits<score_t>::max();
        return;
      }
      _bound_source->Set(_root);
      _global_max_score = _bound_func.Score();
    }

    // What block `k` can score.
    score_t BlockBound(uint64_t k) {
      if (!_idx.has_bound) {
        return std::numeric_limits<score_t>::max();
      }
      ScoreBound bound;
      bound.freq = _cols.Get(_idx.bfreq, k);
      if (_idx.has_norm) {
        bound.delta = _cols.Get(_idx.bdelta, k);
      }
      SDB_ASSERT(_bound_source);
      _bound_source->Set(bound);
      return _bound_func.Score();
    }

    // The same, or "unbounded" for a block below the range the caller cares
    // about, which is not worth evaluating just to skip past it.
    score_t Bound(uint64_t k) {
      if (_skip_bounds_below && _cols.Get(_idx.docs, k) < _skip_bounds_below)
        [[unlikely]] {
        return std::numeric_limits<score_t>::max();
      }
      return BlockBound(k);
    }

    SkipColumnsReader<InputType> _cols;
    SkipColumnIndex _idx{2, false, false};
    SkipState _state;
    ScoreFunction _bound_func;
    ScoreBoundSource::ptr _bound_source;
    ScoreThresholdAttr _threshold;
    uint64_t _doc_origin = 0;
    uint64_t _pos_start = 0;
    uint64_t _pay_start = 0;
    uint64_t _entry_first = 1;
    uint64_t _entry_last = 0;
    uint64_t _cur = 0;
    uint64_t _next = 0;
    uint32_t _docs_total = 0;
    doc_id_t _upper = doc_limits::eof();
    doc_id_t _skip_bounds_below = 0;
    score_t _score = std::numeric_limits<score_t>::max();
    score_t _global_max_score = std::numeric_limits<score_t>::max();
    ScoreBound _root;
    bool _has_root = false;
  };

 public:
  score_t GetMaxScore(doc_id_t doc) noexcept {
    if (_skip.NumLevels()) [[likely]] {
      _skip.SyncTo(CurrentBlock());
    }
    return _skip.GetMaxScore(doc);
  }

  doc_id_t SeekToBlock(doc_id_t target) {
    target = ShallowSeekToBlock(target);
    if (!doc_limits::eof(target)) {
      _doc = _skip.State().doc;
    }
    return target;
  }

  doc_id_t ShallowSeekToBlock(doc_id_t target) {
    if (!_skip.NumLevels()) [[unlikely]] {
      return doc_limits::eof();
    }
    const auto upper_bound = _skip.UpperBound();
    if (upper_bound >= target) {
      return upper_bound;
    }
    const auto bounds_below = _skip.SkipBoundsBelow();
    _skip.SetSkipBoundsBelow(std::max(bounds_below, target));
    _left_in_list = _skip.Seek(target);
    _skip.SetSkipBoundsBelow(bounds_below);
    _left_in_leaf = 0;
    _needs_reposition = true;
    return _skip.UpperBound();
  }

  std::pair<doc_id_t, bool> FillBlock(
    const doc_id_t min, const doc_id_t max,
    uint64_t* IRS_RESTRICT const doc_mask, FillBlockScoreContext score,
    [[maybe_unused]] FillBlockMatchContext match) final;

  template<typename Visitor>
  void ForEachScoredBlock(const ScoreFunction& scorer,
                          ColumnArgsFetcher* fetcher, doc_id_t max,
                          Visitor&& visit);

  template<typename DocsBuffer, typename ScoresBuffer>
  void ScoreCandidates(DocsBuffer& cand_docs, ScoresBuffer& cand_scores,
                       const ScoreFunction& scorer, ColumnArgsFetcher* fetcher,
                       bool required, doc_id_t window_max);

 private:
  IRS_FORCE_INLINE InputType& GetDocIn() const noexcept {
    return sdb::basics::downCast<InputType>(*this->_doc_in);
  }

  template<size_t N>
  IRS_FORCE_INLINE score_t* ScoreBlock(std::span<const doc_id_t, N> docs,
                                       const ScoreFunction& score,
                                       ColumnArgsFetcher* fetcher);

  IRS_FORCE_INLINE void ReadBlock(doc_id_t prev_doc);

  template<size_t N>
  void ProcessBatch(std::span<const doc_id_t, N> docs, const doc_id_t min,
                    uint64_t* IRS_RESTRICT doc_mask,
                    FillBlockScoreContext score);

  using Attributes = AttributesImpl<IteratorTraits>;

  FieldProperties _field;
  const byte_type* _stats = nullptr;
  score_t _boost = kNoBoost;

  ABSL_CACHELINE_ALIGNED uint32_t _enc_buf[doc_limits::kBlockSize];
  ABSL_CACHELINE_ALIGNED uint32_t _freqs[doc_limits::kBlockSize];
  ABSL_CACHELINE_ALIGNED doc_id_t _docs[doc_limits::kBlockSize];
#ifdef __AVX2__
  [[maybe_unused]] doc_id_t _placeholder_for_bitset_materialize[8];
#endif
  doc_id_t _max_in_leaf = doc_limits::invalid();
  uint32_t _left_in_leaf = 0;
  uint32_t _left_in_list = 0;
  bool _needs_reposition = false;
  IndexInput::ptr _doc_in;
  Attributes _attrs;
  SkipBounds _skip;
  IndexInput::ptr _skip_own;
  uint32_t _docs_total = 0;

  // Index of the doc block the read path has reached, derived rather than
  // tracked: `_left_in_list` already says how far along the term we are.
  IRS_FORCE_INLINE uint64_t CurrentBlock() const noexcept {
    const auto read = _docs_total - _left_in_list;
    return read == 0 ? 0 : (read - 1) / doc_limits::kBlockSize;
  }
  uint32_t _freq_block[kScoreBlock];
};

// TODO(gnusi): Deduplicate ScoreBlock and Collect at least
template<typename IteratorTraits, bool Root, bool Pos, bool Offs,
         typename InputType>
template<size_t N>
score_t*
SinglePruningIterator<IteratorTraits, Root, Pos, Offs, InputType>::ScoreBlock(
  std::span<const doc_id_t, N> docs, const ScoreFunction& score,
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
         typename InputType>
void SinglePruningIterator<IteratorTraits, Root, Pos, Offs, InputType>::Collect(
  const ScoreFunction& scorer, ColumnArgsFetcher& fetcher,
  ScoreCollector& collector) {
  ResolveScoreCollector(collector, [&](auto& collector) IRS_FORCE_INLINE {
    auto process_block = [&]<size_t N>(size_t left_in_leaf) IRS_FORCE_INLINE {
      std::span<const doc_id_t, N> docs{std::end(_docs) - left_in_leaf,
                                        left_in_leaf};
      const auto* scores = ScoreBlock(docs, scorer, &fetcher);
      if constexpr (N == std::dynamic_extent) {
        for (size_t i = 0; i != docs.size(); ++i) {
          collector.Add(scores[i], docs[i]);
        }
      } else {
        collector.AddDocs(docs.data(), docs.size(), scores);
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
      if (last_doc + 1 > _skip.UpperBound()) {
        _left_in_list = _skip.Seek(last_doc + 1);
        if (_left_in_list == 0) [[unlikely]] {
          // Nothing left that can beat the threshold, so the term is done.
          _left_in_leaf = 0;
          break;
        }
        auto& state = _skip.State();
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
         typename InputType>
template<size_t N>
void SinglePruningIterator<IteratorTraits, Root, Pos, Offs, InputType>::
  ProcessBatch(std::span<const doc_id_t, N> docs, const doc_id_t min,
               uint64_t* IRS_RESTRICT doc_mask, FillBlockScoreContext score) {
  auto* IRS_RESTRICT const score_window = score.score_window;
  const score_t* IRS_RESTRICT score_ptr =
    ScoreBlock(docs, *score.score, score.fetcher);

  static constexpr auto kBits = BitsRequired<uint64_t>();
  const auto* const data = docs.data();
  VisitDocs<N>(static_cast<uint32_t>(docs.size()),
               [&](uint32_t i) IRS_FORCE_INLINE {
                 const size_t offset = data[i] - min;
                 SetBit(doc_mask[offset / kBits], offset % kBits);
                 Merge<ScoreMergeType::Sum>(score_window[offset], score_ptr[i]);
               });
}

template<typename IteratorTraits, bool Root, bool Pos, bool Offs,
         typename InputType>
template<typename Visitor>
void SinglePruningIterator<IteratorTraits, Root, Pos, Offs, InputType>::
  ForEachScoredBlock(const ScoreFunction& scorer, ColumnArgsFetcher* fetcher,
                     doc_id_t max, Visitor&& visit) {
  if (value() >= max) [[unlikely]] {
    return;
  }

  auto emit = [&]<size_t N>(std::span<doc_id_t, N> docs) IRS_FORCE_INLINE {
    visit(docs,
          ScoreBlock(std::span<const doc_id_t, N>{docs}, scorer, fetcher));
  };

  if (_needs_reposition && _left_in_list != 0) [[unlikely]] {
    _needs_reposition = false;
    auto& state = _skip.State();
    if (state.doc_ptr) [[likely]] {
      GetDocIn().Seek(state.doc_ptr);
    }
    ReadBlock(state.doc);
    const auto min = value();
    const auto* first =
      std::find_if(std::end(_docs) - _left_in_leaf, std::end(_docs),
                   [&](doc_id_t doc) { return doc >= min; });
    SDB_ASSERT(first != std::end(_docs));
    _doc = *first;
    _left_in_leaf = static_cast<uint32_t>(std::end(_docs) - first) - 1;
  }

  SDB_ASSERT(_left_in_leaf < kPostingBlock);
  SDB_ASSERT(*(std::end(_docs) - _left_in_leaf - 1) == value());
  // Head: what is left of the block already decoded, whole when value() is its
  // first doc. Read `last` before emitting -- `visit` may compact in place.
  doc_id_t last = *(std::end(_docs) - 1);
  {
    const auto count = _left_in_leaf + 1;
    if (last >= max) {
      _left_in_leaf = count;
      goto for_each_block_tail;
    }
    if (count == kPostingBlock) {
      goto for_each_block_full;
    }
    emit(std::span<doc_id_t>{std::end(_docs) - count, count});
  }

  for (;;) {
    if (_left_in_list == 0) [[unlikely]] {
      _left_in_leaf = 0;
      goto for_each_block_done;
    }
    ReadBlock(last);
    last = *(std::end(_docs) - 1);
    if (last >= max || _left_in_leaf != kPostingBlock) {
      goto for_each_block_tail;
    }
  for_each_block_full:
    emit(std::span<doc_id_t, kPostingBlock>{std::begin(_docs), kPostingBlock});
  }

for_each_block_tail: {
  auto* begin = std::end(_docs) - _left_in_leaf;
  auto* tail_end = std::find_if(begin, std::end(_docs),
                                [&](doc_id_t doc) { return doc >= max; });
  _left_in_leaf = static_cast<uint32_t>(std::end(_docs) - tail_end);
  if (tail_end != begin) {
    emit(std::span<doc_id_t>{begin, tail_end});
  }
}

for_each_block_done:
  if (_left_in_leaf > 0) {
    _doc = *(std::end(_docs) - _left_in_leaf);
    --_left_in_leaf;
  } else {
    _doc = doc_limits::eof();
  }

  if constexpr (IteratorTraits::Frequency()) {
    std::get<FreqBlockAttr>(_attrs).value = _freq_block;
  }
}

template<typename IteratorTraits, bool Root, bool Pos, bool Offs,
         typename InputType>
template<typename DocsBuffer, typename ScoresBuffer>
void SinglePruningIterator<IteratorTraits, Root, Pos, Offs, InputType>::
  ScoreCandidates(DocsBuffer& cand_docs, ScoresBuffer& cand_scores,
                  const ScoreFunction& scorer, ColumnArgsFetcher* fetcher,
                  bool required, doc_id_t window_max) {
  SDB_ASSERT(!cand_docs.empty());

  size_t out = 0;  // compacted output index (used when required=true)
  SetSkipBoundsBelow(window_max);

  Finally unset = [&] noexcept {
    SetSkipBoundsBelow(0);
    if (required) {
      cand_docs.resize(out);
      cand_scores.resize(out);
    }
  };

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

  // Reposition if needed (same logic as ForEachScoredBlock).
  if (_needs_reposition && _left_in_list != 0) [[unlikely]] {
    _needs_reposition = false;
    auto& state = _skip.State();
    if (state.doc_ptr) [[likely]] {
      GetDocIn().Seek(state.doc_ptr);
    }
    ReadBlock(state.doc);
    const auto min = value();
    const auto* first =
      std::find_if(std::end(_docs) - _left_in_leaf, std::end(_docs),
                   [&](doc_id_t doc) { return doc >= min; });
    SDB_ASSERT(first != std::end(_docs));
    _doc = *first;
    _left_in_leaf = static_cast<uint32_t>(std::end(_docs) - first) - 1;
  }

  SDB_ASSERT(_left_in_leaf < kPostingBlock);
  SDB_ASSERT(*(std::end(_docs) - _left_in_leaf - 1) == value());

  {
    const auto count = _left_in_leaf + 1;
    if (*(std::end(_docs) - 1) >= max) {
      _left_in_leaf = count;
      goto score_cand_tail;
    }
    find_in_block(std::end(_docs) - count, std::end(_docs));
    if (cand_idx >= cand_count) {
      goto score_cand_done;
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
      if (next_cand > _skip.UpperBound()) {
        _left_in_list = _skip.Seek(next_cand);
        if (_left_in_list == 0) [[unlikely]] {
          // Nothing left that can beat the threshold, so the term is done.
          _left_in_leaf = 0;
          goto score_cand_done;
        }
        auto& state = _skip.State();
        if (state.doc_ptr) [[likely]] {
          GetDocIn().Seek(state.doc_ptr);
        }
        ReadBlock(state.doc);
      } else {
        ReadBlock(*(std::end(_docs) - 1));
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
    std::get<FreqBlockAttr>(_attrs).value = _freq_block;
  }
}

template<typename IteratorTraits, bool Root, bool Pos, bool Offs,
         typename InputType>
std::pair<doc_id_t, bool>
SinglePruningIterator<IteratorTraits, Root, Pos, Offs, InputType>::FillBlock(
  const doc_id_t min, const doc_id_t max, uint64_t* IRS_RESTRICT const doc_mask,
  FillBlockScoreContext score, FillBlockMatchContext match) {
  SDB_ASSERT(!IteratorTraits::Position());
  SDB_ASSERT(min < max);
  SDB_ASSERT(value() >= min);
  SDB_ASSERT(score.score && !score.score->IsDefault());
  SDB_ASSERT(score.merge_type == ScoreMergeType::Sum);
  SDB_ASSERT(doc_mask != nullptr);

  // Iterator already past window -- nothing to do.
  if (value() >= max) [[unlikely]] {
    return std::pair{_doc, true};
  }

  if (_needs_reposition && _left_in_list != 0) [[unlikely]] {
    _needs_reposition = false;
    auto& state = _skip.State();
    if (state.doc_ptr) [[likely]] {
      GetDocIn().Seek(state.doc_ptr);
    }
    ReadBlock(state.doc);
    const auto* first =
      std::find_if(std::end(_docs) - _left_in_leaf, std::end(_docs),
                   [&](doc_id_t doc) { return doc >= min; });
    SDB_ASSERT(first != std::end(_docs));
    _doc = *first;
    _left_in_leaf = static_cast<uint32_t>(std::end(_docs) - first) - 1;
  }

  SDB_ASSERT(_left_in_leaf < kPostingBlock);
  SDB_ASSERT(*(std::end(_docs) - _left_in_leaf - 1) == value());

  doc_id_t last = *(std::end(_docs) - 1);
  {
    const auto count = _left_in_leaf + 1;
    if (last >= max) {
      _left_in_leaf = count;
      goto fill_block_tail;
    }
    if (count == kPostingBlock) {
      goto fill_block_full;
    }
    ProcessBatch(std::span<const doc_id_t>{std::end(_docs) - count, count}, min,
                 doc_mask, score);
  }

  for (;;) {
    if (_left_in_list == 0) [[unlikely]] {
      _left_in_leaf = 0;
      goto fill_block_done;
    }
    ReadBlock(last);
    last = *(std::end(_docs) - 1);
    if (last >= max || _left_in_leaf != kPostingBlock) {
      goto fill_block_tail;
    }
  fill_block_full:
    ProcessBatch(std::span<const doc_id_t, kPostingBlock>{std::begin(_docs),
                                                          kPostingBlock},
                 min, doc_mask, score);
  }

fill_block_tail: {
  const auto* begin = std::end(_docs) - _left_in_leaf;
  const auto* tail_end = std::find_if(begin, std::cend(_docs),
                                      [&](doc_id_t doc) { return doc >= max; });
  if (tail_end != begin) {
    ProcessBatch(std::span{begin, tail_end}, min, doc_mask, score);
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
    std::get<FreqBlockAttr>(_attrs).value = _freq_block;
  }
  return std::pair{_doc, false};
}

template<typename IteratorTraits, bool Root, bool Pos, bool Offs,
         typename InputType>
doc_id_t SinglePruningIterator<IteratorTraits, Root, Pos, Offs,
                               InputType>::seek(doc_id_t target) {
  if (target <= _doc) [[unlikely]] {
    return _doc;
  }

  if (_skip.IsLessThanUpperBound(target)) [[unlikely]] {
    SeekToBlock(target);
  }

  // Position from skip state if no decoded docs remain.
  if (_left_in_leaf == 0) [[unlikely]] {
    if (_left_in_list == 0) [[unlikely]] {
      return _doc = doc_limits::eof();
    }

    if (_needs_reposition) {
      _needs_reposition = false;
      auto& state = _skip.State();
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
         typename InputType>
void SinglePruningIterator<FormatTraits, Root, Pos, Offs, InputType>::Prepare(
  const PostingCookie& meta, const IteratorFieldOptions& opts,
  const IndexInput* doc_in, const IndexInput* skip_in) {
  Init(meta);

  // Set default bound with max score so no blocks are ever pruned
  _skip.SetScoreBoundScorer(
    ScoreFunction::Constant(std::numeric_limits<score_t>::max()),
    std::make_unique<DefaultScoreBoundSource>());

  const auto& term_state = *meta.cookie;
  std::get<CostAttr>(_attrs).reset(term_state.docs_count);

  if (term_state.docs_count > 1) {
    _left_in_list = term_state.docs_count;
    SDB_ASSERT(_left_in_leaf == 0);
    SDB_ASSERT(_max_in_leaf == doc_limits::invalid());

    SDB_ASSERT(!_doc_in);
    _doc_in = doc_in->Reopen();

    if (!_doc_in) {
      SDB_ERROR(IRESEARCH, "Failed to reopen document input");
      throw IoError("failed to reopen document input");
    }

    std::get<FreqBlockAttr>(_attrs).value = _freq_block;

    if (term_state.docs_count > doc_limits::kBlockSize) {
      if (skip_in == nullptr || opts.skip.Empty()) [[unlikely]] {
        throw IndexError{
          "while preparing postings, error: term needs skip columns the "
          "field does not have"};
      }
      _skip_own = skip_in->Reopen();
      if (!_skip_own) {
        SDB_ERROR(IRESEARCH, "Failed to reopen skip input");
        throw IoError("failed to reopen skip input");
      }
      _skip.Prepare(opts, term_state, opts.skip.doc_origin,
                    sdb::basics::downCast<InputType>(*_skip_own));
      _docs_total = term_state.docs_count;
      // A long term keeps no `doc_start`: entry 0 describes doc block 0.
      GetDocIn().Seek(_skip.DocStart());
    } else {
      GetDocIn().Seek(term_state.doc_start);
    }
    SDB_ASSERT(!GetDocIn().IsEOF());
  } else {
    SDB_ASSERT(term_state.docs_count == 1);
    auto* doc = std::end(_docs) - 1;
    *doc = term_state.doc;

    *(std::end(_freqs) - 1) = term_state.freq;
    _freq_block[0] = term_state.freq;
    std::get<FreqBlockAttr>(_attrs).value = _freq_block;

    _left_in_list = 0;
    _left_in_leaf = 1;
    _max_in_leaf = *doc;
  }

  SDB_ASSERT(term_state.freq);

  if (1 < term_state.docs_count &&
      term_state.docs_count < doc_limits::kBlockSize) {
    // A term of one block keeps its score bound ahead of that block.
    irs::SkipScoreBounds(true, GetDocIn());
  }
}

template<typename FormatTraits, bool Root, bool Pos, bool Offs,
         typename InputType>
void SinglePruningIterator<FormatTraits, Root, Pos, Offs, InputType>::ReadBlock(
  doc_id_t prev_doc) {
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

}  // namespace irs
