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

#include "basics/empty.hpp"
#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/formats/formats.hpp"
#include "iresearch/formats/posting/iterator_pos.hpp"
#include "iresearch/formats/posting/skip_column.hpp"
#include "iresearch/index/iterators.hpp"
#include "iresearch/search/cost.hpp"
#include "iresearch/utils/attribute_helper.hpp"
#include "pg/sql_exception_macro.h"

namespace irs {

template<typename IteratorTraits>
class PostingIteratorBase : public DocIterator {
 public:
  static_assert(doc_limits::kBlockSize % kScoreBlock == 0,
                "kBlockSize must be a multiple of kScoreBlock");

  uint32_t RemainingDocs() const noexcept {
    return _left_in_leaf + _left_in_list;
  }

  std::span<const doc_id_t> NextLeafBlock() {
    static_assert(!IteratorTraits::Frequency());
    if (_left_in_leaf == 0) [[unlikely]] {
      if (_left_in_list == 0) [[unlikely]] {
        return {};
      }
      ReadLeaf(_doc);
    }
    const auto left = std::exchange(_left_in_leaf, 0);
    _doc = *(std::end(_docs) - 1);
    return {std::end(_docs) - left, left};
  }

  IRS_NO_INLINE Attribute* GetMutable(TypeInfo::type_id type) noexcept final {
    return irs::GetMutable(_attrs, type);
  }

  IRS_FORCE_INLINE doc_id_t advance() final;

  IRS_FORCE_INLINE doc_id_t seek(doc_id_t target) final;

  IRS_FORCE_INLINE doc_id_t LazySeek(doc_id_t target) final;

  uint32_t count() final {
    _doc = doc_limits::eof();
    const auto left_in_leaf = std::exchange(_left_in_leaf, 0);
    const auto left_in_list = std::exchange(_left_in_list, 0);
    return left_in_leaf + left_in_list;
  }

  uint32_t EmitDocs(doc_id_t* out, doc_id_t min, doc_id_t max) final {
    if constexpr (IteratorTraits::Position()) {
      return DocIterator::EmitDocsImpl(*this, out, min, max);
    } else {
      uint32_t n = 0;
      auto doc = seek(min);
      auto left_in_leaf = _left_in_leaf;
      const auto* const end = std::end(_docs);
      while (doc < max) {
        const auto* const it = end - left_in_leaf - 1;
        if (_max_in_leaf >= max) {
          auto* dst = out + n;
          const auto* p = it;
          do {
            *dst++ = *p++;
          } while (*p < max);
          n = static_cast<uint32_t>(dst - out);
          doc = *p;
          left_in_leaf = static_cast<uint32_t>(end - p) - 1;
          break;
        }
        const auto count = left_in_leaf + 1;
        if (count == doc_limits::kBlockSize) [[likely]] {
          std::memcpy(out + n, it, doc_limits::kBlockSize * sizeof(doc_id_t));
        } else {
          std::memcpy(out + n, it, count * sizeof(doc_id_t));
        }
        n += count;
        if (_left_in_list == 0) [[unlikely]] {
          doc = doc_limits::eof();
          left_in_leaf = 0;
          break;
        }
        ReadLeaf(end[-1]);
        left_in_leaf = _left_in_leaf - 1;
        doc = *(end - _left_in_leaf);
      }
      _doc = doc;
      _left_in_leaf = left_in_leaf;
      return n;
    }
  }

  uint32_t EmitScoredDocs(doc_id_t* out, score_t* scores, doc_id_t max,
                          const ScoreFunction& scorer,
                          ColumnArgsFetcher* fetcher, doc_id_t min) final {
    if constexpr (IteratorTraits::Position()) {
      return DocIterator::EmitScoredDocsImpl(*this, out, scores, max, scorer,
                                             fetcher, min);
    } else {
      uint32_t n = 0;
      auto doc = seek(min);
      auto left_in_leaf = _left_in_leaf;
      const auto* const end = std::end(_docs);
      auto emit = [&]<size_t N>(const doc_id_t* it,
                                uint32_t count) IRS_FORCE_INLINE {
        const auto* s =
          ScoreBlock(std::span<const doc_id_t, N>{it, count}, scorer, fetcher);
        std::memcpy(out + n, it, count * sizeof(doc_id_t));
        std::memcpy(scores + n, s, count * sizeof(score_t));
        n += count;
      };
      while (doc < max) {
        const auto* const it = end - left_in_leaf - 1;
        if (_max_in_leaf >= max) {
          const auto* p = it;
          do {
            ++p;
          } while (*p < max);
          emit.template operator()<std::dynamic_extent>(
            it, static_cast<uint32_t>(p - it));
          doc = *p;
          left_in_leaf = static_cast<uint32_t>(end - p) - 1;
          break;
        }
        const auto count = left_in_leaf + 1;
        if (count == kPostingBlock) [[likely]] {
          emit.template operator()<kPostingBlock>(it, kPostingBlock);
        } else {
          emit.template operator()<std::dynamic_extent>(it, count);
        }
        if (_left_in_list == 0) [[unlikely]] {
          doc = doc_limits::eof();
          left_in_leaf = 0;
          break;
        }
        ReadLeaf(end[-1]);
        left_in_leaf = _left_in_leaf - 1;
        doc = *(end - _left_in_leaf);
      }
      _doc = doc;
      _left_in_leaf = left_in_leaf;
      if constexpr (IteratorTraits::Frequency()) {
        std::get<FreqBlockAttr>(_attrs).value = _freq_block;
      }
      return n;
    }
  }

  ScoreFunction PrepareScore(const PrepareScoreContext& ctx) final {
    SDB_ASSERT(ctx.scorer);
    return ctx.scorer->PrepareScorer({
      .segment = *ctx.segment,
      .field = _field,
      .doc_attrs = *this,
      .fetcher = ctx.fetcher,
      .stats = _stats,
      .boost = _boost,
    });
  }

  void Collect(const ScoreFunction& scorer, ColumnArgsFetcher& fetcher,
               ScoreCollector& collector) final;

  IRS_FORCE_INLINE uint32_t GetFreq() const final {
    if constexpr (IteratorTraits::Frequency()) {
      SDB_ASSERT(_left_in_leaf < doc_limits::kBlockSize);
      return *(std::end(_freqs) - _left_in_leaf - 1);
    } else {
      return 0;
    }
  }

  IRS_FORCE_INLINE void FetchScoreArgs(uint16_t index) final {
    if constexpr (IteratorTraits::Frequency()) {
      SDB_ASSERT(index < kScoreBlock);
      _freq_block[index] = GetFreq();
    }
  }

  IRS_FORCE_INLINE void Init(const PostingCookie& cookie) noexcept {
    _field = cookie.field;
    _stats = cookie.stats;
    _boost = cookie.boost;
  }

 protected:
  using Position = PositionImpl<IteratorTraits>;
  using Attributes =
    std::conditional_t<IteratorTraits::Position(),
                       std::tuple<FreqBlockAttr, CostAttr, Position>,
                       AttributesImpl<IteratorTraits>>;

  virtual void ReadLeaf(doc_id_t prev_doc) = 0;
  virtual bool SeekToLeaf(doc_id_t target) = 0;

  template<size_t N>
  IRS_FORCE_INLINE const score_t* ScoreBlock(std::span<const doc_id_t, N> docs,
                                             const ScoreFunction& score,
                                             ColumnArgsFetcher* fetcher);

  template<ScoreMergeType MergeType, bool TrackMatch, size_t N>
  bool ProcessBatch(std::span<const doc_id_t, N> docs, const doc_id_t min,
                    uint64_t* IRS_RESTRICT doc_mask,
                    [[maybe_unused]] FillBlockScoreContext score,
                    [[maybe_unused]] FillBlockMatchContext match);

  FieldProperties _field;
  const byte_type* _stats = nullptr;
  score_t _boost = kNoBoost;

  ABSL_CACHELINE_ALIGNED uint32_t _enc_buf[doc_limits::kBlockSize];
  [[no_unique_address]] ABSL_CACHELINE_ALIGNED utils::Need<
    IteratorTraits::Frequency(), uint32_t[doc_limits::kBlockSize]> _freqs;
  ABSL_CACHELINE_ALIGNED doc_id_t _docs[doc_limits::kBlockSize];
#ifdef __AVX2__
  [[maybe_unused]] doc_id_t _placeholder_for_bitset_materialize[8];
#endif
  doc_id_t _max_in_leaf = doc_limits::invalid();
  uint32_t _left_in_leaf = 0;
  uint32_t _left_in_list = 0;
  IndexInput::ptr _doc_in;
  Attributes _attrs;
  // TODO(gnusi) we don't need collected freqs if we don't compute score
  // But for positions we need freqs, even without score
  [[no_unique_address]] utils::Need<IteratorTraits::Frequency(),
                                    uint32_t[kScoreBlock]> _freq_block;
};

template<typename IteratorTraits>
doc_id_t PostingIteratorBase<IteratorTraits>::advance() {
  if (_left_in_leaf == 0) [[unlikely]] {
    if (_left_in_list == 0) [[unlikely]] {
      return _doc = doc_limits::eof();
    }

    ReadLeaf(_doc);
  }

  _doc = *(std::end(_docs) - _left_in_leaf);

  if constexpr (IteratorTraits::Position()) {
    auto& pos = std::get<Position>(_attrs);
    const auto freq = *(std::end(_freqs) - _left_in_leaf);
    pos.Notify(freq, freq);
    pos.Clear();
  }

  --_left_in_leaf;
  return _doc;
}

template<typename IteratorTraits>
doc_id_t PostingIteratorBase<IteratorTraits>::seek(doc_id_t target) {
  if (target <= _doc) [[unlikely]] {
    return _doc;
  }

  if (_max_in_leaf < target && !SeekToLeaf(target)) [[unlikely]] {
    _left_in_leaf = 0;
    return _doc = doc_limits::eof();
  }

  [[maybe_unused]] uint32_t notify = 0;
  for (auto left_in_leaf = _left_in_leaf; left_in_leaf != 0; --left_in_leaf) {
    const auto doc = *(std::end(_docs) - left_in_leaf);

    if constexpr (IteratorTraits::Position()) {
      notify += *(std::end(_freqs) - left_in_leaf);
    }

    if (target <= doc) {
      if constexpr (IteratorTraits::Position()) {
        auto& pos = std::get<Position>(_attrs);
        pos.Notify(*(std::end(_freqs) - left_in_leaf), notify);
        pos.Clear();
      }

      _left_in_leaf = left_in_leaf - 1;
      return _doc = doc;
    }
  }

  _left_in_leaf = 0;
  return _doc = doc_limits::eof();
}

template<typename IteratorTraits>
doc_id_t PostingIteratorBase<IteratorTraits>::LazySeek(doc_id_t target) {
  if (target <= _doc) [[unlikely]] {
    return _doc;
  }

  auto seal = [&] IRS_FORCE_INLINE {
    _left_in_leaf = 0;
    return _doc = doc_limits::eof();
  };

  if (_max_in_leaf < target && !SeekToLeaf(target)) [[unlikely]] {
    return seal();
  }

  if constexpr (IteratorTraits::Position()) {
    static constexpr uint32_t kGroup = 8;

    auto left_in_leaf = _left_in_leaf;
    const auto* doc = std::end(_docs) - left_in_leaf;
    const auto* freq = std::end(_freqs) - left_in_leaf;
    uint32_t notify = 0;

    const auto found = [&](uint32_t i) IRS_FORCE_INLINE {
      auto& pos = std::get<Position>(_attrs);
      pos.Notify(freq[i], notify + freq[i]);
      pos.Clear();
      _left_in_leaf = left_in_leaf - i - 1;
      return _doc = doc[i];
    };

    for (; left_in_leaf >= kGroup;
         left_in_leaf -= kGroup, doc += kGroup, freq += kGroup) {
      uint32_t below = 0;
      uint32_t skipped = 0;
      for (uint32_t i = 0; i != kGroup; ++i) {
        const auto lower = static_cast<uint32_t>(doc[i] < target);
        below += lower;
        skipped += freq[i] * lower;
      }
      notify += skipped;
      if (below != kGroup) {
        return found(below);
      }
    }

    for (uint32_t i = 0; i != left_in_leaf; ++i) {
      if (target <= doc[i]) {
        return found(i);
      }
      notify += freq[i];
    }
  } else {
    auto next = [&](uint32_t left_in_leaf, doc_id_t doc) IRS_FORCE_INLINE {
      _left_in_leaf = left_in_leaf - 1;
      return _doc = doc;
    };

    if (_left_in_list != 0) [[likely]] {
      auto it =
        BranchlessLowerBound<doc_limits::kBlockSize>(std::begin(_docs), target);
      return next(std::end(_docs) - it, *it);
    }

    for (auto left_in_leaf = _left_in_leaf; left_in_leaf != 0; --left_in_leaf) {
      const auto doc = *(std::end(_docs) - left_in_leaf);
      if (target <= doc) {
        return next(left_in_leaf, doc);
      }
    }
  }

  return seal();
}

template<typename IteratorTraits>
void PostingIteratorBase<IteratorTraits>::Collect(const ScoreFunction& scorer,
                                                  ColumnArgsFetcher& fetcher,
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

    if (const auto left_in_leaf = std::exchange(_left_in_leaf, 0))
      [[unlikely]] {
      process_block.template operator()<std::dynamic_extent>(left_in_leaf);
    } else {
      *(std::end(_docs) - 1) = _doc;
    }

    while (_left_in_list >= kPostingBlock) {
      ReadLeaf(*(std::end(_docs) - 1));
      process_block.template operator()<kPostingBlock>(kPostingBlock);
    }

    if (_left_in_list) {
      ReadLeaf(*(std::end(_docs) - 1));
      process_block.template operator()<std::dynamic_extent>(
        std::exchange(_left_in_leaf, 0));
    }
  });

  _doc = doc_limits::eof();
}

template<typename IteratorTraits>
template<size_t N>
const score_t* PostingIteratorBase<IteratorTraits>::ScoreBlock(
  std::span<const doc_id_t, N> docs, const ScoreFunction& score,
  ColumnArgsFetcher* fetcher) {
  if constexpr (N == kPostingBlock) {
    SDB_ASSERT(std::data(_docs) == docs.data());
    if (fetcher) {
      fetcher->FetchPostingBlock(docs);
    }
    if constexpr (IteratorTraits::Frequency()) {
      std::get<FreqBlockAttr>(_attrs).value = std::begin(_freqs);
    }
    auto* p = reinterpret_cast<score_t*>(std::end(_enc_buf) - N);
    score.ScorePostingBlock(p);
    return p;
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
    auto* p = reinterpret_cast<score_t*>(std::end(_enc_buf) - docs.size());
    score.Score(p, docs.size());
    return p;
  }
}

template<typename IteratorTraits>
template<ScoreMergeType MergeType, bool TrackMatch, size_t N>
bool PostingIteratorBase<IteratorTraits>::ProcessBatch(
  std::span<const doc_id_t, N> docs, const doc_id_t min,
  uint64_t* IRS_RESTRICT doc_mask, [[maybe_unused]] FillBlockScoreContext score,
  [[maybe_unused]] FillBlockMatchContext match) {
  [[maybe_unused]] auto* IRS_RESTRICT const score_window = score.score_window;
  [[maybe_unused]] const score_t* IRS_RESTRICT score_ptr;
  if constexpr (MergeType != ScoreMergeType::Noop) {
    score_ptr = ScoreBlock(docs, *score.score, score.fetcher);
  }

  static constexpr auto kBits = BitsRequired<uint64_t>();
  const auto* const data = docs.data();
  [[maybe_unused]] bool empty = true;
  VisitDocs<N>(
    static_cast<uint32_t>(docs.size()), [&](uint32_t i) IRS_FORCE_INLINE {
      const size_t offset = data[i] - min;
      if constexpr (TrackMatch) {
        const bool has_match = ++match.matches[offset] >= match.min_match_count;
        SetBit(doc_mask[offset / kBits], offset % kBits, has_match);
        empty &= !has_match;
      } else {
        SetBit(doc_mask[offset / kBits], offset % kBits);
      }
      if constexpr (MergeType != ScoreMergeType::Noop) {
        Merge<MergeType>(score_window[offset], score_ptr[i]);
      }
    });
  if constexpr (TrackMatch) {
    return empty;
  }
  return false;
}

// Iterator over posting list.
// IteratorTraits defines requested features.
// FieldTraits defines requested features.
template<typename IteratorTraits, typename FieldTraits, bool HasScoreBounds,
         typename InputType>
class PostingIteratorImpl : public PostingIteratorBase<IteratorTraits> {
  static_assert((IteratorTraits::Features() & FieldTraits::Features()) ==
                IteratorTraits::Features());

  using Base = PostingIteratorBase<IteratorTraits>;
  using typename Base::Position;

  static_assert(doc_limits::kBlockSize % kScoreBlock == 0,
                "kBlockSize must be a multiple of kScoreBlock");

 public:
  PostingIteratorImpl() = default;

  void Prepare(const PostingCookie& meta, const IteratorFieldOptions& options,
               const IndexInput* doc_in, const IndexInput* pos_in,
               const IndexInput* pay_in, const IndexInput* skip_in,
               bool score_prune = false);

  std::pair<doc_id_t, bool> FillBlock(const doc_id_t min, const doc_id_t max,
                                      uint64_t* IRS_RESTRICT const doc_mask,
                                      FillBlockScoreContext score,
                                      FillBlockMatchContext match) final;

 private:
  IRS_FORCE_INLINE InputType& GetDocIn() const noexcept {
    return sdb::basics::downCast<InputType>(*this->_doc_in);
  }

  IRS_FORCE_INLINE void ReadTail(doc_id_t prev_doc);
  IRS_FORCE_INLINE void ReadBlock(doc_id_t prev_doc);
  IRS_FORCE_INLINE void ReadLeaf(doc_id_t prev_doc) final;

  IRS_FORCE_INLINE void SkipLeafFreqs(uint32_t tail) {
    if constexpr (FieldTraits::Frequency()) {
      if (tail == doc_limits::kBlockSize) {
        IteratorTraits::SkipBlock(GetDocIn());
      }
    }
  }

  IRS_FORCE_INLINE void ReadLeafFreqs(uint32_t tail) {
    if constexpr (IteratorTraits::Frequency()) {
      IteratorTraits::ReadTail(tail, GetDocIn(), this->_enc_buf, this->_freqs);
    } else {
      SkipLeafFreqs(tail);
    }
  }

  bool SeekToLeaf(doc_id_t target) final;

  // Index of the doc block the read path is about to reach. Derived rather
  // than tracked: `_left_in_list` already says exactly how far along the
  // term's documents we are.
  IRS_FORCE_INLINE uint64_t NextBlock() const noexcept {
    return (_docs_total - this->_left_in_list) / doc_limits::kBlockSize;
  }

  IndexInput::ptr _skip_own;
  SkipColumnsReader<InputType> _cols;
  SkipColumnIndex _idx{2, false, false};
  // The term's entries are `[_entry_first, _entry_last]`, one per doc block.
  uint64_t _entry_first{};
  uint64_t _entry_last{};
  uint64_t _doc_origin{};
  uint64_t _pos_start{};
  uint64_t _pay_start{};
  uint32_t _docs_total{};
};

template<typename IteratorTraits, typename FieldTraits, bool HasScoreBounds,
         typename InputType>
void PostingIteratorImpl<IteratorTraits, FieldTraits, HasScoreBounds,
                         InputType>::Prepare(const PostingCookie& meta,
                                             const IteratorFieldOptions& opts,
                                             const IndexInput* doc_in,
                                             const IndexInput* pos_in,
                                             const IndexInput* pay_in,
                                             const IndexInput* skip_in,
                                             bool score_prune) {
  this->Init(meta);

  const auto& term_state = *meta.cookie;
  std::get<CostAttr>(this->_attrs).reset(term_state.docs_count);

  SDB_ASSERT(this->_left_in_leaf == 0);
  SDB_ASSERT(this->_max_in_leaf == doc_limits::invalid());

  if (term_state.docs_count > 1) {
    this->_left_in_list = term_state.docs_count;

    SDB_ASSERT(!this->_doc_in);
    this->_doc_in = doc_in->Reopen();  // Reopen thread-safe stream

    if (!this->_doc_in) {
      SDB_ERROR(IRESEARCH, "Failed to reopen document input");
      throw IoError("failed to reopen document input");
    }

    if constexpr (IteratorTraits::Frequency()) {
      std::get<FreqBlockAttr>(this->_attrs).value = this->_freq_block;
    }

    if (term_state.docs_count > doc_limits::kBlockSize) {
      // A long term keeps no `doc_start`: entry 0 describes doc block 0, so
      // it is where the term's `.doc` data begins.
      if (skip_in == nullptr || opts.skip.Empty()) [[unlikely]] {
        throw IndexError{absl::StrCat(
          "while preparing postings, error: term of ", term_state.docs_count,
          " docs needs skip columns; skip_in=", skip_in != nullptr,
          " count=", opts.skip.count, " columns=", opts.skip.columns,
          " dir=", opts.skip.dir != nullptr, " first_entry=",
          term_state.first_entry)};
      }
      _skip_own = skip_in->Reopen();
      if (!_skip_own) {
        SDB_ERROR(IRESEARCH, "Failed to reopen skip input");
        throw IoError("failed to reopen skip input");
      }
      _idx = SkipColumnIndex{opts.skip.columns, FieldTraits::Position(),
                             FieldTraits::Offset()};
      _cols.Prepare(opts.skip.dir, opts.skip.count, opts.skip.columns,
                    opts.skip.origin,
                    sdb::basics::downCast<InputType>(*_skip_own));
      _doc_origin = opts.skip.doc_origin;
      _pos_start = term_state.pos_start;
      _pay_start = term_state.pay_start;
      _docs_total = term_state.docs_count;
      _entry_first = term_state.first_entry;
      _entry_last =
        _entry_first +
        math::DivCeil32(term_state.docs_count, doc_limits::kBlockSize) - 1;
      SDB_ASSERT(_entry_last < opts.skip.count);
      GetDocIn().Seek(_doc_origin + _cols.GetOnce(_idx.docoff, _entry_first));
    } else {
      GetDocIn().Seek(term_state.doc_start);
    }
    SDB_ASSERT(!GetDocIn().IsEOF());
  } else {
    SDB_ASSERT(term_state.docs_count == 1);
    auto* doc = std::end(this->_docs) - 1;
    *doc = term_state.doc;
    if constexpr (IteratorTraits::Frequency()) {
      *(std::end(this->_freqs) - 1) = term_state.freq;
      this->_freq_block[0] = term_state.freq;
      std::get<FreqBlockAttr>(this->_attrs).value = this->_freq_block;
    }
    this->_left_in_list = 0;
    this->_left_in_leaf = 1;
    this->_max_in_leaf = *doc;
  }

  SDB_ASSERT(!IteratorTraits::Frequency() || term_state.freq);
  if constexpr (IteratorTraits::Position()) {
    static_assert(IteratorTraits::Frequency());

    const DocState state{
      .pos_in = pos_in,
      .pay_in = pay_in,
      .term_state = &term_state,
      .enc_buf = this->_enc_buf,
    };

    std::get<Position>(this->_attrs).template Prepare<InputType>(state);
  }

  if (1 < term_state.docs_count &&
      term_state.docs_count < doc_limits::kBlockSize && !score_prune) {
    // A term of one block keeps its score bound ahead of that block.
    irs::SkipScoreBounds(HasScoreBounds, GetDocIn());
  }
}

template<typename IteratorTraits, typename FieldTraits, bool HasScoreBounds,
         typename InputType>
std::pair<doc_id_t, bool>
PostingIteratorImpl<IteratorTraits, FieldTraits, HasScoreBounds,
                    InputType>::FillBlock(const doc_id_t min,
                                          const doc_id_t max,
                                          uint64_t* IRS_RESTRICT const doc_mask,
                                          FillBlockScoreContext score,
                                          FillBlockMatchContext match) {
  SDB_ASSERT(min < max);
  SDB_ASSERT(this->value() >= min);
  // value() was consumed by advance/seek/previous FillBlock
  // but still sits in _docs just before the leftover range
  SDB_ASSERT(this->_left_in_leaf < kPostingBlock);
  if constexpr (!IteratorTraits::Position()) {
    if (this->value() >= max) [[unlikely]] {
      return std::pair{this->_doc, true};
    }
    if (!score.score || score.score->IsDefault()) {
      score.merge_type = ScoreMergeType::Noop;
    }

    return ResolveBool(match.matches, [&]<bool TrackMatch> {
      return ResolveMergeType(score.merge_type, [&]<ScoreMergeType MergeType> {
        bool empty = true;
        doc_id_t last = *(std::end(this->_docs) - 1);

        // leftover from previous call
        {
          SDB_ASSERT(*(std::end(this->_docs) - this->_left_in_leaf - 1) ==
                     this->value());
          const auto count = this->_left_in_leaf + 1;
          if (last >= max) {
            this->_left_in_leaf = count;
            goto fill_block_tail;
          }
          if (count == kPostingBlock) {
            goto fill_block_full;
          }
          empty &= this->template ProcessBatch<MergeType, TrackMatch>(
            std::span<const doc_id_t>{std::end(this->_docs) - count, count},
            min, doc_mask, score, match);
        }

        // full blocks only
        for (;;) {
          if constexpr (!TrackMatch && MergeType == ScoreMergeType::Noop) {
            if (this->_left_in_list == 0) [[unlikely]] {
              this->_left_in_leaf = 0;
              goto fill_block_done;
            }
            SDB_ASSERT(!IteratorTraits::Frequency());
            const auto tail =
              std::min(this->_left_in_list, doc_limits::kBlockSize);
            const auto base = last;
            SDB_ASSERT(base >= min);
            const auto leaf = IteratorTraits::ReadTailForFill(
              tail, GetDocIn(), this->_enc_buf, this->_docs, base);
            this->_max_in_leaf = leaf.max;
            this->_left_in_leaf = tail;
            this->_left_in_list -= tail;
            if (leaf.Maskable()) {
              const auto live = IteratorTraits::MaskLeaf(
                leaf, base, tail, min, max, doc_mask, std::end(this->_docs));
              empty &= live == tail;
              SkipLeafFreqs(tail);
              if (live == 0) {
                *(std::end(this->_docs) - 1) = leaf.max;
                last = leaf.max;
                continue;
              }
              this->_left_in_leaf = live;
              goto fill_block_done;
            }
            SkipLeafFreqs(tail);
            last = leaf.max;
            if (last >= max || tail != doc_limits::kBlockSize) {
              goto fill_block_tail;
            }
          } else {
            if (this->_left_in_list < doc_limits::kBlockSize) [[unlikely]] {
              if (this->_left_in_list == 0) {
                this->_left_in_leaf = 0;
                goto fill_block_done;
              }
              ReadTail(last);
              goto fill_block_tail;
            }
            ReadBlock(last);
            last = this->_max_in_leaf;
            if (last >= max) {
              goto fill_block_tail;
            }
          }
        fill_block_full:
          empty &= this->template ProcessBatch<MergeType, TrackMatch>(
            std::span<const doc_id_t, kPostingBlock>{std::begin(this->_docs),
                                                     kPostingBlock},
            min, doc_mask, score, match);
        }

      fill_block_tail: {
        const auto* begin = std::end(this->_docs) - this->_left_in_leaf;
        const auto* tail_end =
          std::find_if(begin, std::cend(this->_docs),
                       [&](doc_id_t doc) { return doc >= max; });
        if (tail_end != begin) {
          empty &= this->template ProcessBatch<MergeType, TrackMatch>(
            std::span{begin, tail_end}, min, doc_mask, score, match);
        }
        this->_left_in_leaf =
          static_cast<uint32_t>(std::end(this->_docs) - tail_end);
      }

      fill_block_done:
        if (this->_left_in_leaf > 0) {
          this->_doc = *(std::end(this->_docs) - this->_left_in_leaf);
          --this->_left_in_leaf;
        } else {
          this->_doc = doc_limits::eof();
        }

        if constexpr (IteratorTraits::Frequency()) {
          std::get<FreqBlockAttr>(this->_attrs).value = this->_freq_block;
        }
        return std::pair{this->_doc, empty};
      });
    });
  } else {
    SDB_ASSERT(false);
    return std::pair{this->_doc, true};
  }
}

template<typename IteratorTraits, typename FieldTraits, bool HasScoreBounds,
         typename InputType>
void PostingIteratorImpl<IteratorTraits, FieldTraits, HasScoreBounds,
                         InputType>::ReadTail(doc_id_t prev_doc) {
  const auto tail = this->_left_in_list;
  SDB_ASSERT(tail < doc_limits::kBlockSize);
  IteratorTraits::ReadTailDelta(tail, GetDocIn(), this->_enc_buf, this->_docs,
                                prev_doc);
  this->_max_in_leaf = *(std::end(this->_docs) - 1);
  this->_left_in_leaf = tail;
  this->_left_in_list = 0;
  ReadLeafFreqs(tail);
}

template<typename IteratorTraits, typename FieldTraits, bool HasScoreBounds,
         typename InputType>
void PostingIteratorImpl<IteratorTraits, FieldTraits, HasScoreBounds,
                         InputType>::ReadBlock(doc_id_t prev_doc) {
  IteratorTraits::ReadBlockDelta(GetDocIn(), this->_enc_buf, this->_docs,
                                 prev_doc);
  this->_max_in_leaf = *(std::end(this->_docs) - 1);
  this->_left_in_leaf = doc_limits::kBlockSize;
  this->_left_in_list -= doc_limits::kBlockSize;
  ReadLeafFreqs(doc_limits::kBlockSize);
}

template<typename IteratorTraits, typename FieldTraits, bool HasScoreBounds,
         typename InputType>
void PostingIteratorImpl<IteratorTraits, FieldTraits, HasScoreBounds,
                         InputType>::ReadLeaf(doc_id_t prev_doc) {
  if (this->_left_in_list >= doc_limits::kBlockSize) [[likely]] {
    ReadBlock(prev_doc);
  } else {
    ReadTail(prev_doc);
  }
}

template<typename IteratorTraits, typename FieldTraits, bool HasScoreBounds,
         typename InputType>
bool PostingIteratorImpl<IteratorTraits, FieldTraits, HasScoreBounds,
                         InputType>::SeekToLeaf(doc_id_t target) {
  if constexpr (!IteratorTraits::Position()) {
    // Close enough that decoding the next leaf beats addressing a new block.
    if (target - this->_max_in_leaf <= doc_limits::kBlockSize) [[unlikely]] {
      if (this->_left_in_list == 0) [[unlikely]] {
        return false;
      }
      ReadLeaf(this->_max_in_leaf);
      return true;
    }
  }

  if (_docs_total == 0) [[unlikely]] {
    // One doc block, so there is nothing to address: read on or run out.
    if (this->_left_in_list == 0) [[unlikely]] {
      return false;
    }
    ReadLeaf(this->_max_in_leaf);
    return true;
  }

  // Entries are ordered by the max doc of their block, and seeks only move
  // forward, so gallop from where the read path has reached.
  const auto first = _entry_first + NextBlock();
  const auto k =
    SkipColumnSeek(_cols, first, _entry_last + 1, target);
  if (k > _entry_last) [[unlikely]] {
    this->_left_in_list = 0;
    return false;
  }

  const auto block = k - _entry_first;
  const auto skipped = static_cast<uint32_t>(block) * doc_limits::kBlockSize;
  SDB_ASSERT(skipped < _docs_total);
  this->_left_in_list = _docs_total - skipped;

  // Block `k`'s deltas are based on the last doc before it, which is the max
  // doc of block `k - 1` -- entry `k - 1`, usually in the same group.
  const auto prev_doc = block == 0 ? doc_limits::invalid()
                                   : _cols.Get(_idx.docs, k - 1);

  GetDocIn().Seek(_doc_origin + _cols.Get(_idx.docoff, k));
  if constexpr (IteratorTraits::Position()) {
    SkipState state;
    state.doc = prev_doc;
    state.pos_ptr = _pos_start + _cols.Get(_idx.posoff, k);
    state.pos_offset = _cols.Get(_idx.posslot, k);
    if constexpr (IteratorTraits::Offset()) {
      state.pay_ptr = _pay_start + _cols.Get(_idx.payoff, k);
    }
    std::get<Position>(this->_attrs).template Prepare<InputType>(state);
  }

  ReadLeaf(prev_doc);
  return true;
}

}  // namespace irs
