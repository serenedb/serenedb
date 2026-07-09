////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2016 by EMC Corporation, All Rights Reserved
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
/// Copyright holder is EMC Corporation
///
/// @author Andrey Abramov
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <absl/functional/function_ref.h>
#ifdef __AVX2__
#include <immintrin.h>
#endif

#include <algorithm>
#include <bit>

#include "basics/assert.h"
#include "basics/bit_utils.hpp"
#include "basics/down_cast.h"
#include "basics/memory.hpp"
#include "basics/shared.hpp"
#include "basics/system-compiler.h"
#include "iresearch/formats/seek_cookie.hpp"
#include "iresearch/index/index_features.hpp"
#include "iresearch/index/scan_filter.hpp"
#include "iresearch/search/column_collector.hpp"
#include "iresearch/search/score_function.hpp"
#include "iresearch/search/scorer.hpp"
#include "iresearch/utils/attribute_provider.hpp"
#include "iresearch/utils/iterator.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs {

struct PrepareScoreContext {
  const Scorer* scorer = nullptr;
  const SubReader* segment = nullptr;
  ColumnArgsFetcher* fetcher = nullptr;
};

struct FillBlockScoreContext {
  const ScoreFunction* score = nullptr;
  ColumnArgsFetcher* fetcher = nullptr;
  score_t* IRS_RESTRICT score_window = nullptr;
  ScoreMergeType merge_type = ScoreMergeType::Noop;
};

struct FillBlockMatchContext {
  uint32_t* IRS_RESTRICT matches = 0;
  size_t min_match_count = 0;
};

class ScoreCollector {
 public:
  enum class Tag {
    NthPartition,
    NthPartitionAsc,
    NthPartitionFiltered,
    NthPartitionAscFiltered,
    Generic,
  };

  IRS_FORCE_INLINE Tag GetTag() const noexcept { return _tag; }

  virtual void Add(score_t score, doc_id_t doc) = 0;

  virtual void AddWindow(const score_t* scores, const uint64_t* mask,
                         doc_id_t min, size_t num_blocks, bool clear_score) = 0;

  virtual void AddDocs(const doc_id_t* docs, size_t count,
                       const score_t* scores) = 0;

 protected:
  explicit ScoreCollector(Tag tag) noexcept : _tag{tag} {}

  ~ScoreCollector() = default;

 private:
  Tag _tag;
};

struct ScoreDoc {
  score_t score = 0.0f;
  doc_id_t doc = doc_limits::eof();
  uint32_t segment_idx = 0;

  bool operator==(const ScoreDoc& other) const = default;
};

enum class Order { ASC, DESC };

// TODO(mbkkt) Try to make it autovectorized,
// otherwise try to use xsimd/neon specific intrinsics
//
// Templated on whether a pushed scan filter can be present, so a scan with no
// table filter pays nothing for the filter machinery: the Filtered=false
// instantiation is the plain top-k collector -- noexcept hot paths, no filter
// branches, byte-identical to the pre-filter collector -- while Filtered=true
// adds the batched MatchesBatch gating. The scan picks the instantiation from
// whether the query has any pushed table filter.
template<Order O, bool Filtered>
class NthPartitionScoreCollectorT final : public ScoreCollector {
 public:
  explicit NthPartitionScoreCollectorT(score_t& score_threshold, size_t k,
                                       std::span<ScoreDoc> hits) noexcept
    : ScoreCollector{Filtered
                       ? (O == Order::ASC ? Tag::NthPartitionAscFiltered
                                          : Tag::NthPartitionFiltered)
                       : (O == Order::ASC ? Tag::NthPartitionAsc
                                          : Tag::NthPartition)},
      _score_threshold{&score_threshold},
      _hits_it{hits.data()},
      _hits_begin{hits.data()},
      _hits_pivot{hits.data() + k},
      _hits_end{hits.data() + hits.size()} {
    SDB_ASSERT(2 * k == hits.size());
  }

  void SetScoreThreshold(score_t& score_threshold) noexcept {
    if constexpr (O == Order::ASC) {
      SDB_ASSERT(score_threshold >= *_score_threshold);
    } else {
      SDB_ASSERT(score_threshold <= *_score_threshold);
    }
    score_threshold = *_score_threshold;
    _score_threshold = &score_threshold;
  }

  IRS_FORCE_INLINE void Add(score_t score,
                            doc_id_t doc) noexcept(!Filtered) final {
    ++_count;
    if constexpr (Filtered) {
      if (_filter != nullptr) {
        TryPushFiltered(score, doc);
        return;
      }
    }
    TryPushNoFilter(score, doc);
  }

  void SetSegment(uint32_t idx) noexcept { _current_segment = idx; }

  void SetFilter(ScanFilter* filter) noexcept
    requires Filtered
  {
    _filter = filter;
  }

  bool HasFilter() const noexcept {
    if constexpr (Filtered) {
      return _filter != nullptr;
    } else {
      return false;
    }
  }

  IRS_FORCE_INLINE size_t AcceptedCount() const noexcept {
    return _hits_it - _hits_begin;
  }

  IRS_FORCE_INLINE uint64_t TotalMatches() const noexcept { return _count; }

  IRS_FORCE_INLINE void AddWindow(const score_t* scores, const uint64_t* mask,
                                  doc_id_t min, size_t num_blocks,
                                  bool clear_score) noexcept(!Filtered) final {
#ifdef __AVX2__
    auto threshold = _mm256_set1_ps(*_score_threshold);
#endif
    if constexpr (Filtered) {
      doc_id_t bdocs[kFilterBatch];
      score_t bscores[kFilterBatch];
      size_t bn = 0;
      for (size_t i = 0; i < num_blocks; ++i) {
        auto word = mask[i];
        if (word == 0) [[likely]] {
          continue;
        }
        _count += std::popcount(word);
        const score_t* IRS_RESTRICT const score_base =
          scores + i * BitsRequired<uint64_t>();
#ifdef __AVX2__
        word &= GetScoreMask(score_base, threshold);
#endif
        const doc_id_t doc_base = min + i * BitsRequired<uint64_t>();
        if (_filter != nullptr) {
          if (bn + BitsRequired<uint64_t>() > kFilterBatch) {
            PushFiltered(bdocs, bscores, bn);
            bn = 0;
#ifdef __AVX2__
            threshold = _mm256_set1_ps(*_score_threshold);
#endif
          }
          for (uint64_t w = word; w != 0; w = PopBit(w)) {
            const doc_id_t bit = std::countr_zero(w);
            bdocs[bn] = doc_base + bit;
            bscores[bn] = score_base[bit];
            ++bn;
          }
        } else {
          while (word != 0) {
            const doc_id_t bit = std::countr_zero(word);
            word = PopBit(word);
#ifdef __AVX2__
            if (PushNoFilter(score_base[bit], doc_base + bit)) {
              threshold = _mm256_set1_ps(*_score_threshold);
              word &= GetScoreMask(score_base, threshold);
            }
#else
            TryPushNoFilter(score_base[bit], doc_base + bit);
#endif
          }
        }
        if (clear_score) {
          std::memset(const_cast<score_t*>(score_base), 0,
                      BitsRequired<uint64_t>() * sizeof(score_t));
        }
      }
      PushFiltered(bdocs, bscores, bn);
    } else {
      for (size_t i = 0; i < num_blocks; ++i) {
        auto word = mask[i];
        if (word == 0) [[likely]] {
          continue;
        }
        _count += std::popcount(word);
        const score_t* IRS_RESTRICT const score_base =
          scores + i * BitsRequired<uint64_t>();
#ifdef __AVX2__
        word &= GetScoreMask(score_base, threshold);
#endif
        const doc_id_t doc_base = min + i * BitsRequired<uint64_t>();
        while (word != 0) {
          const doc_id_t bit = std::countr_zero(word);
          word = PopBit(word);
#ifdef __AVX2__
          if (PushNoFilter(score_base[bit], doc_base + bit)) {
            threshold = _mm256_set1_ps(*_score_threshold);
            word &= GetScoreMask(score_base, threshold);
          }
#else
          TryPushNoFilter(score_base[bit], doc_base + bit);
#endif
        }
        if (clear_score) {
          std::memset(const_cast<score_t*>(score_base), 0,
                      BitsRequired<uint64_t>() * sizeof(score_t));
        }
      }
    }
  }

  IRS_FORCE_INLINE void AddDocs(
    const doc_id_t* docs, size_t count,
    const score_t* scores) noexcept(!Filtered) final {
    _count += count;

    if constexpr (Filtered) {
      if (_filter != nullptr) {
        doc_id_t bdocs[kFilterBatch];
        score_t bscores[kFilterBatch];
        size_t bn = 0;
        for (size_t i = 0; i < count; ++i) {
          if (Accept(scores[i])) {
            bdocs[bn] = docs[i];
            bscores[bn] = scores[i];
            if (++bn == kFilterBatch) {
              PushFiltered(bdocs, bscores, bn);
              bn = 0;
            }
          }
        }
        PushFiltered(bdocs, bscores, bn);
        return;
      }
    }

    size_t i = 0;

#ifdef __AVX2__
    auto threshold = _mm256_set1_ps(*_score_threshold);
    for (; i + 8 <= count; i += 8) {
      auto scores_vec = _mm256_loadu_ps(scores + i);
      auto cmp = _mm256_cmp_ps(scores_vec, threshold, kCmpPred);
      auto pass = static_cast<unsigned>(_mm256_movemask_ps(cmp));

      while (pass) {
        const int bit = std::countr_zero(pass);
        pass = PopBit(pass);
        const score_t score = scores[i + bit];
        if (PushNoFilter(score, docs[i + bit])) {
          threshold = _mm256_set1_ps(*_score_threshold);
          cmp = _mm256_cmp_ps(scores_vec, threshold, kCmpPred);
          pass &= static_cast<unsigned>(_mm256_movemask_ps(cmp));
        }
      }
    }
#endif

    for (; i < count; ++i) {
      TryPushNoFilter(scores[i], docs[i]);
    }
  }

 private:
  // Candidates gathered for one filter call before pushes resume; bounds
  // theta staleness while amortizing the per-call filter costs.
  static constexpr size_t kFilterBatch = 256;

  IRS_FORCE_INLINE bool Accept(score_t score) const noexcept {
    if constexpr (O == Order::ASC) {
      return score < *_score_threshold;
    } else {
      return score > *_score_threshold;
    }
  }

  // Filter a gathered batch of threshold-beating candidates and push the
  // survivors; the threshold may move while the filter runs, so it is
  // re-checked per doc.
  IRS_FORCE_INLINE void PushFiltered(const doc_id_t* docs,
                                     const score_t* scores, size_t n)
    requires Filtered
  {
    SDB_ASSERT(n <= kFilterBatch);
    if (n == 0) {
      return;
    }
    bool pass[kFilterBatch];
    _filter->MatchesBatch({docs, n}, pass);
    for (size_t j = 0; j < n; ++j) {
      if (pass[j] && Accept(scores[j])) {
        PushUnchecked(scores[j], docs[j]);
      }
    }
  }

  IRS_FORCE_INLINE void TryPushFiltered(score_t score, doc_id_t doc)
    requires Filtered
  {
    if (Accept(score)) {
      SDB_ASSERT(_filter);
      bool pass;
      _filter->MatchesBatch({&doc, 1}, &pass);
      if (pass) {
        PushUnchecked(score, doc);
      }
    }
  }

  IRS_FORCE_INLINE void TryPushNoFilter(score_t score, doc_id_t doc) noexcept {
    if (Accept(score)) {
      PushUnchecked(score, doc);
    }
  }

  IRS_FORCE_INLINE bool PushNoFilter(score_t score, doc_id_t doc) noexcept {
    SDB_ASSERT(Accept(score));
    return PushUnchecked(score, doc);
  }

  IRS_FORCE_INLINE bool PushUnchecked(score_t score, doc_id_t doc) noexcept {
    *_hits_it = {score, doc, _current_segment};
    ++_hits_it;
    if (_hits_it != _hits_end) {
      return false;
    }
    _hits_it = _hits_pivot;
    std::nth_element(_hits_begin, _hits_pivot, _hits_end,
                     [](const ScoreDoc& l, const ScoreDoc& r) {
                       if constexpr (O == Order::ASC) {
                         return l.score < r.score;
                       } else {
                         return l.score > r.score;
                       }
                     });
    *_score_threshold = _hits_pivot->score;
    return true;
  }

#ifdef __AVX2__
  static constexpr int kCmpPred = O == Order::ASC ? _CMP_LT_OQ : _CMP_GT_OQ;

  IRS_FORCE_INLINE uint64_t GetScoreMask(const score_t* IRS_RESTRICT scores,
                                         __m256 threshold) const noexcept {
    uint64_t mask = 0;
    for (int i = 0; i < 64; i += 8) {
      const uint64_t bits = _mm256_movemask_ps(
        _mm256_cmp_ps(_mm256_loadu_ps(scores + i), threshold, kCmpPred));
      mask |= bits << i;
    }
    return mask;
  }
#endif

  uint64_t _count = 0;
  uint32_t _current_segment = 0;
  // Only ever set on the Filtered instantiation (SetFilter requires Filtered);
  // referenced solely from Filtered-guarded paths, so it costs the plain
  // collector nothing in its hot loop.
  ScanFilter* _filter = nullptr;
  score_t* IRS_RESTRICT _score_threshold = nullptr;
  ScoreDoc* IRS_RESTRICT _hits_it;
  ScoreDoc* IRS_RESTRICT const _hits_begin;
  ScoreDoc* IRS_RESTRICT const _hits_pivot;
  ScoreDoc* IRS_RESTRICT const _hits_end;
};

// Filtered=false: plain top-k collector, zero filter overhead. Filtered=true:
// adds MatchesBatch gating. The scan selects one at init from whether the
// query pushed any table filter.
template<Order O>
using NthPartitionScoreCollector = NthPartitionScoreCollectorT<O, false>;
template<Order O>
using NthPartitionFilteredScoreCollector = NthPartitionScoreCollectorT<O, true>;

template<typename F>
IRS_FORCE_INLINE auto ResolveScoreCollector(ScoreCollector& collector, F&& f) {
  switch (collector.GetTag()) {
    case ScoreCollector::Tag::NthPartition:
      return std::forward<F>(f)(
        sdb::basics::downCast<NthPartitionScoreCollectorT<Order::DESC, false>>(
          collector));
    case ScoreCollector::Tag::NthPartitionAsc:
      return std::forward<F>(f)(
        sdb::basics::downCast<NthPartitionScoreCollectorT<Order::ASC, false>>(
          collector));
    case ScoreCollector::Tag::NthPartitionFiltered:
      return std::forward<F>(f)(
        sdb::basics::downCast<NthPartitionScoreCollectorT<Order::DESC, true>>(
          collector));
    case ScoreCollector::Tag::NthPartitionAscFiltered:
      return std::forward<F>(f)(
        sdb::basics::downCast<NthPartitionScoreCollectorT<Order::ASC, true>>(
          collector));
    case ScoreCollector::Tag::Generic:
      return std::forward<F>(f)(collector);
    default:
      SDB_UNREACHABLE();
  }
}

// An iterator providing sequential and random access to a posting list
//
// After creation iterator is in uninitialized state:
//   - `value()` returns `doc_limits::invalid()` or `doc_limits::eof()`
// `seek()` to:
//   - `doc_limits::invalid()` is undefined and implementation dependent
//   - `doc_limits::eof()` must always return `doc_limits::eof()`
// Once iterator is exhausted:
//   - `next()` must constantly return `false`
//   - `seek()` to any value must return `doc_limits::eof()`
//   - `value()` must return `doc_limits::eof()`
struct DocIterator : AttributeProvider {
  using ptr = memory::managed_ptr<DocIterator>;

  [[nodiscard]] static DocIterator::ptr empty() noexcept;

  IRS_FORCE_INLINE const doc_id_t& value() const noexcept { return _doc; }

  virtual doc_id_t advance() = 0;

  // Position iterator at a specified target and returns current value
  // (for more information see class description)
  virtual doc_id_t seek(doc_id_t target) = 0;

  // If target is in the iterator: returns target and value() == target.
  // If target isn't in the iterator: value() is unchanged (no advance).
  // If target <= value(): returns target
  virtual doc_id_t LazySeek(doc_id_t target) { return seek(target); }

  virtual void Collect(const ScoreFunction& scorer, ColumnArgsFetcher& fetcher,
                       ScoreCollector& collector) = 0;

  virtual void FetchScoreArgs(uint16_t index) {}

  virtual ScoreFunction PrepareScore(const PrepareScoreContext& ctx) {
    return {};
  }

  virtual uint32_t count() = 0;

  // Emit ascending matched doc-ids in [value(), max) into `out`, return the
  // count. Bitset-free counterpart of FillBlock for unscored scans.
  // Preconditions: value() < max and value() is positioned (as for FillBlock);
  //   `out` has room for max - value() ids.
  // Postcondition: value() == first doc >= max (or eof()), as for FillBlock.
  virtual uint32_t EmitDocs(doc_id_t* out, doc_id_t max) = 0;

  // Scored counterpart of EmitDocs: also emit each match's score into `scores`
  // (parallel to `out`) via the block-scoring path (mirrors Collect), no
  // bitset. Emits [min, max): value() may sit before the window (a
  // block-positioned sub-iterator), so docs < min are skipped, not emitted.
  virtual uint32_t EmitScoredDocs(doc_id_t* out, score_t* scores, doc_id_t max,
                                  const ScoreFunction& scorer,
                                  ColumnArgsFetcher* fetcher, doc_id_t min) = 0;

  // Fill a bitmap window [min, max) with documents from this iterator.
  // Preconditions:
  //   - min < max
  //   - value() >= min (iterator must be positioned at or after window start)
  // For each doc in [value(), max):
  //   - Sets bit (doc - min) in mask
  //   - If TrackMatch: increments match count, sets bit only when threshold met
  //   - If scoring: accumulates scores into score.score_window
  // Returns {next_doc, empty}:
  //   - next_doc: first doc >= max (next unprocessed), or eof() if exhausted
  //   - empty: true if no documents matched (always false when !TrackMatch)
  // Postcondition:
  //   - value() == next_doc
  virtual std::pair<doc_id_t, bool> FillBlock(doc_id_t min, doc_id_t max,
                                              uint64_t* mask,
                                              FillBlockScoreContext score,
                                              FillBlockMatchContext match) = 0;

  virtual uint32_t GetFreq() const {
    SDB_ASSERT(false);
    return 0;
  }

 protected:
  mutable doc_id_t _doc = doc_limits::invalid();

  template<typename S>
  IRS_FORCE_INLINE static uint32_t CountImpl(S& self) {
    uint32_t count = 0;
    while (!doc_limits::eof(self.S::advance())) {
      ++count;
    }
    return count;
  }

  // `self.S::advance()` is a qualified (non-virtual) call, so when `self` is
  // the concrete iterator (from a derived override) advance() inlines instead
  // of dispatching -- the whole point of the *Impl(*this) devirtualisation.
  template<typename S>
  IRS_FORCE_INLINE static uint32_t EmitDocsImpl(S& self, doc_id_t* out,
                                                doc_id_t max) {
    uint32_t n = 0;
    for (auto doc = self.value(); doc < max; doc = self.S::advance()) {
      out[n++] = doc;
    }
    return n;
  }

  template<typename S>
  IRS_FORCE_INLINE static uint32_t EmitScoredDocsImpl(
    S& self, doc_id_t* out, score_t* scores, doc_id_t max,
    const ScoreFunction& scorer, ColumnArgsFetcher* fetcher, doc_id_t min) {
    uint32_t n = 0;
    auto doc = self.value();
    while (doc < min) {  // value() may be behind the window; skip up to min
      doc = self.S::advance();
    }
    while (doc < max) {
      const uint32_t start = n;
      for (; n - start != kScoreBlock && doc < max; ++n) {
        out[n] = doc;
        self.S::FetchScoreArgs(n - start);
        doc = self.S::advance();
      }
      if (fetcher != nullptr) {
        fetcher->Fetch(std::span<const doc_id_t>{out + start, n - start});
      }
      scorer.Score(scores + start, n - start);
    }
    return n;
  }

  template<typename S>
  IRS_FORCE_INLINE static void CollectImpl(S& self, const ScoreFunction& scorer,
                                           ColumnArgsFetcher& fetcher,
                                           ScoreCollector& c) {
    ABSL_CACHELINE_ALIGNED std::array<score_t, kScoreBlock> scores;
    ABSL_CACHELINE_ALIGNED std::array<doc_id_t, kScoreBlock> docs;
    ResolveScoreCollector(c, [&](auto& collector) IRS_FORCE_INLINE {
      while (true) {
        for (size_t i = 0; i != kScoreBlock; ++i) {
          const auto doc = self.S::advance();
          if (doc_limits::eof(doc)) [[unlikely]] {
            if (i != 0) {
              fetcher.Fetch(std::span<const doc_id_t>{docs.data(), i});
              scorer.Score(scores.data(), i);
              for (size_t j = 0; j < i; ++j) {
                collector.Add(scores[j], docs[j]);
              }
            }
            return;
          }
          docs[i] = doc;
          self.S::FetchScoreArgs(i);
        }
        fetcher.Fetch(docs);
        scorer.ScoreBlock(scores.data());
        collector.AddDocs(docs.data(), kScoreBlock, scores.data());
      }
    });
  }

  template<typename S>
  IRS_FORCE_INLINE static std::pair<doc_id_t, bool> FillBlockImpl(
    S& self, doc_id_t min, doc_id_t max, uint64_t* mask,
    FillBlockScoreContext score, FillBlockMatchContext match) {
    if (!score.score || score.score->IsDefault()) {
      score.merge_type = ScoreMergeType::Noop;
    }

    return ResolveMergeType(score.merge_type, [&]<ScoreMergeType MergeType> {
      return ResolveBool(match.matches != nullptr, [&]<bool TrackMatch> {
        return FillBlockImpl<MergeType, TrackMatch>(self, min, max, mask, score,
                                                    match);
      });
    });
  }

 private:
  template<ScoreMergeType MergeType, bool TrackMatch, typename S>
  static std::pair<doc_id_t, bool> FillBlockImpl(
    S& self, const doc_id_t min, const doc_id_t max,
    uint64_t* IRS_RESTRICT mask, [[maybe_unused]] FillBlockScoreContext score,
    [[maybe_unused]] FillBlockMatchContext match) {
    SDB_ASSERT(min < max);
    SDB_ASSERT(self.value() >= min);

    [[maybe_unused]] std::array<score_t, kScoreBlock> score_buf;
    [[maybe_unused]] std::array<doc_id_t, kScoreBlock> score_hits;
    [[maybe_unused]] uint16_t score_index = 0;
    [[maybe_unused]] auto flush_score = [&](size_t n) {
      if constexpr (MergeType != ScoreMergeType::Noop) {
        SDB_ASSERT(n);
        SDB_ASSERT(score.score);

        if (score.fetcher) {
          score.fetcher->Fetch(std::span<const doc_id_t>{score_hits.data(), n});
        }
        if (n == kScoreBlock) [[likely]] {
          score.score->ScoreBlock(score_buf.data());
        } else {
          score.score->Score(score_buf.data(), n);
        }
        Merge<MergeType>(score.score_window, score_hits.data(), min,
                         score_buf.data(), n);
        score_index = 0;
      }
    };

    bool empty = true;
    auto doc = self.value();
    for (; doc < max; doc = self.S::advance()) {
      SDB_ASSERT(doc >= min);
      const auto offset = doc - min;

      if constexpr (TrackMatch) {
        SDB_ASSERT(match.matches);
        const bool has_match = ++match.matches[offset] >= match.min_match_count;
        SetBit(mask[offset / BitsRequired<uint64_t>()],
               offset % BitsRequired<uint64_t>(), has_match);
        empty &= !has_match;
      } else {
        SetBit(mask[offset / BitsRequired<uint64_t>()],
               offset % BitsRequired<uint64_t>());
        empty = false;
      }

      if constexpr (MergeType != ScoreMergeType::Noop) {
        score_hits[score_index] = doc;
        self.S::FetchScoreArgs(score_index);
        ++score_index;

        if (score_index == kScoreBlock) {
          flush_score(kScoreBlock);
        }
      }
    }

    if constexpr (MergeType != ScoreMergeType::Noop) {
      if (score_index) {
        flush_score(score_index);
      }
    }

    return {doc, empty};
  }
};

#define IRS_DOC_ITERATOR_DEFAULTS                                              \
  uint32_t count() final { return irs::DocIterator::CountImpl(*this); }        \
                                                                               \
  void Collect(const irs::ScoreFunction& scorer,                               \
               irs::ColumnArgsFetcher& fetcher,                                \
               irs::ScoreCollector& collector) final {                         \
    irs::DocIterator::CollectImpl(*this, scorer, fetcher, collector);          \
  }                                                                            \
                                                                               \
  uint32_t EmitDocs(irs::doc_id_t* out, irs::doc_id_t max) final {             \
    return irs::DocIterator::EmitDocsImpl(*this, out, max);                    \
  }                                                                            \
                                                                               \
  uint32_t EmitScoredDocs(irs::doc_id_t* out, irs::score_t* scores,            \
                          irs::doc_id_t max, const irs::ScoreFunction& scorer, \
                          irs::ColumnArgsFetcher* fetcher, irs::doc_id_t min)  \
    final {                                                                    \
    return irs::DocIterator::EmitScoredDocsImpl(*this, out, scores, max,       \
                                                scorer, fetcher, min);         \
  }                                                                            \
                                                                               \
  std::pair<irs::doc_id_t, bool> FillBlock(                                    \
    irs::doc_id_t min, irs::doc_id_t max, uint64_t* mask,                      \
    irs::FillBlockScoreContext score, irs::FillBlockMatchContext match)        \
    final {                                                                    \
    return irs::DocIterator::FillBlockImpl(*this, min, max, mask, score,       \
                                           match);                             \
  }

// EmitDocs/EmitScoredDocs only, for subclasses that already define bespoke
// count/Collect/FillBlock.
#define IRS_DOC_ITERATOR_EMIT_DEFAULTS                                         \
  uint32_t EmitDocs(irs::doc_id_t* out, irs::doc_id_t max) final {             \
    return irs::DocIterator::EmitDocsImpl(*this, out, max);                    \
  }                                                                            \
  uint32_t EmitScoredDocs(irs::doc_id_t* out, irs::score_t* scores,            \
                          irs::doc_id_t max, const irs::ScoreFunction& scorer, \
                          irs::ColumnArgsFetcher* fetcher, irs::doc_id_t min)  \
    final {                                                                    \
    return irs::DocIterator::EmitScoredDocsImpl(*this, out, scores, max,       \
                                                scorer, fetcher, min);         \
  }

// Everything except count(), for subclasses with a bespoke count() but default
// Collect/FillBlock/EmitDocs/EmitScoredDocs.
#define IRS_DOC_ITERATOR_DEFAULTS_NO_COUNT                                     \
  void Collect(const irs::ScoreFunction& scorer,                               \
               irs::ColumnArgsFetcher& fetcher,                                \
               irs::ScoreCollector& collector) final {                         \
    irs::DocIterator::CollectImpl(*this, scorer, fetcher, collector);          \
  }                                                                            \
                                                                               \
  uint32_t EmitDocs(irs::doc_id_t* out, irs::doc_id_t max) final {             \
    return irs::DocIterator::EmitDocsImpl(*this, out, max);                    \
  }                                                                            \
                                                                               \
  uint32_t EmitScoredDocs(irs::doc_id_t* out, irs::score_t* scores,            \
                          irs::doc_id_t max, const irs::ScoreFunction& scorer, \
                          irs::ColumnArgsFetcher* fetcher, irs::doc_id_t min)  \
    final {                                                                    \
    return irs::DocIterator::EmitScoredDocsImpl(*this, out, scores, max,       \
                                                scorer, fetcher, min);         \
  }                                                                            \
                                                                               \
  std::pair<irs::doc_id_t, bool> FillBlock(                                    \
    irs::doc_id_t min, irs::doc_id_t max, uint64_t* mask,                      \
    irs::FillBlockScoreContext score, irs::FillBlockMatchContext match)        \
    final {                                                                    \
    return irs::DocIterator::FillBlockImpl(*this, min, max, mask, score,       \
                                           match);                             \
  }

// Same as `DocIterator` but also support `reset()` operation
struct ResettableDocIterator : DocIterator {
  using ptr = memory::managed_ptr<ResettableDocIterator>;

  [[nodiscard]] static ResettableDocIterator::ptr empty() noexcept;

  // Reset iterator to initial state
  virtual void reset() = 0;
};

struct TermIterator : Iterator<bytes_view, AttributeProvider> {
  using ptr = memory::managed_ptr<TermIterator>;

  [[nodiscard]] static TermIterator::ptr empty() noexcept;

  // Read term attributes
  virtual void read() = 0;

  // Return iterator over the associated posting list with the requested
  // features.
  [[nodiscard]] virtual DocIterator::ptr postings(
    IndexFeatures features) const = 0;
};

// Represents a result of seek operation
enum class SeekResult {
  // Exact value is found
  Found = 0,
  // Exact value is not found, an iterator is positioned at the next
  // greater value.
  NotFound,
  // No value greater than a target found, eof
  End,
};

// An iterator providing random and sequential access to term
// dictionary.
struct SeekTermIterator : TermIterator {
  using ptr = memory::managed_ptr<SeekTermIterator>;

  [[nodiscard]] static SeekTermIterator::ptr empty() noexcept;

  // Position iterator at a value that is not less than the specified
  // one. Returns seek result.
  virtual SeekResult seek_ge(bytes_view value) = 0;

  // Position iterator at a value that is not less than the specified
  // one. Returns `true` on success, `false` otherwise.
  // Caller isn't allowed to read iterator value in case if this method
  // returned `false`.
  virtual bool seek(bytes_view value) = 0;

  // Returns seek cookie of the current term value.
  [[nodiscard]] virtual SeekCookie::ptr cookie() const = 0;
};

// Position iterator to the specified target and returns current value
// of the iterator. Returns `false` if iterator exhausted, `true` otherwise.
template<typename Iterator, typename T, typename Less = std::less<T>>
bool seek(Iterator& it, const T& target, Less less = Less()) {
  const auto step = [&] {
    if constexpr (requires { it.next(); }) {
      return it.next();
    } else {
      return !doc_limits::eof(it.advance());
    }
  };
  bool next = true;
  while (less(it.value(), target) && (next = step())) {
  }
  return next;
}

// Position iterator to the specified min term or to the next term
// after the min term depending on the specified `Include` value.
// Returns true in case if iterator has been successfully positioned,
// false otherwise.
template<bool Include>
bool seek_min(SeekTermIterator& it, bytes_view min) {
  const auto res = it.seek_ge(min);

  return SeekResult::End != res &&
         (Include || SeekResult::Found != res || it.next());
}

// Position iterator `count` items after the current position.
// Returns true if the iterator has been successfully positioned
template<typename Iterator>
bool skip(Iterator& itr, size_t count) {
  while (count--) {
    if (!itr.next()) {
      return false;
    }
  }

  return true;
}

}  // namespace irs
