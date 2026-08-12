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
#include "iresearch/formats/posting_meta.hpp"
#include "iresearch/index/index_features.hpp"
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
    Loser,
    Generic,
  };

  IRS_FORCE_INLINE Tag GetTag() const noexcept { return _tag; }

  virtual void Add(score_t score, doc_id_t doc) = 0;

  virtual void ConsumeWindow(score_t* scores, uint64_t* mask, doc_id_t min,
                             size_t num_blocks) = 0;

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

// TODO(mbkkt) Try to make it autovectorized,
// otherwise try to use xsimd/neon specific intrinsics
template<typename Derived>
class ScoreCollectorBase : public ScoreCollector {
 public:
  void SetScoreThreshold(score_t& score_threshold) noexcept {
    SDB_ASSERT(score_threshold <= *_score_threshold);
    score_threshold = *_score_threshold;
    _score_threshold = &score_threshold;
  }

  // The one place the threshold moves, and it only ever rises. Two things
  // raise it and neither knows about the other: this collector's own k-th
  // whenever it has k hits, and the scan seeding a segment with the best k-th
  // any worker has published. The hits are not dropped at that seed, so hits
  // accepted under the older threshold stay in and the next k-th can be below
  // the seeded value -- routing every raise through here is what keeps that
  // lower k-th from writing the threshold back down. SetScoreThreshold
  // asserts on it.
  IRS_FORCE_INLINE void RaiseScoreThreshold(score_t score_threshold) noexcept {
    *_score_threshold = std::max(*_score_threshold, score_threshold);
  }

  void SetSegment(uint32_t idx) noexcept { _current_segment = idx; }

  IRS_FORCE_INLINE uint64_t TotalMatches() const noexcept { return _count; }

  IRS_FORCE_INLINE void Add(score_t score, doc_id_t doc) noexcept final {
    ++_count;
    TryPush(*_score_threshold, score, doc);
  }

  IRS_FORCE_INLINE void ConsumeWindow(score_t* scores, uint64_t* mask,
                                      doc_id_t min,
                                      size_t num_blocks) noexcept final {
    score_t threshold = *_score_threshold;
    for (size_t i = 0; i < num_blocks; ++i) {
      auto word = mask[i];
      if (word == 0) [[likely]] {
        continue;
      }
      mask[i] = 0;

      _count += std::popcount(word);
      auto* IRS_RESTRICT const score_base =
        scores + i * BitsRequired<uint64_t>();
#ifdef __AVX2__
      word &= GetScoreMask(score_base, threshold);
#endif
      const doc_id_t doc_base = min + i * BitsRequired<uint64_t>();

      while (word != 0) {
        const doc_id_t bit = std::countr_zero(word);
        word = PopBit(word);
        TryPush(threshold, score_base[bit], doc_base + bit);
      }

      std::memset(score_base, 0, BitsRequired<uint64_t>() * sizeof(score_t));
    }
    *_score_threshold = threshold;
  }

  IRS_FORCE_INLINE void AddDocs(const doc_id_t* docs, size_t count,
                                const score_t* scores) noexcept final {
    _count += count;
    score_t threshold = *_score_threshold;
    size_t i = 0;
#ifdef __AVX2__
    for (; i + 8 <= count; i += 8) {
      auto pass = static_cast<unsigned>(_mm256_movemask_ps(_mm256_cmp_ps(
        _mm256_loadu_ps(scores + i), _mm256_set1_ps(threshold), kCmpPred)));
      while (pass != 0) {
        const int bit = std::countr_zero(pass);
        pass = PopBit(pass);
        TryPush(threshold, scores[i + bit], docs[i + bit]);
      }
    }
#endif
    for (; i < count; ++i) {
      TryPush(threshold, scores[i], docs[i]);
    }
    *_score_threshold = threshold;
  }

 protected:
  ScoreCollectorBase(Tag tag, score_t& score_threshold) noexcept
    : ScoreCollector{tag}, _score_threshold{&score_threshold} {}

  uint64_t _count = 0;
  uint32_t _current_segment = 0;
  score_t* IRS_RESTRICT _score_threshold;

 private:
  IRS_FORCE_INLINE Derived& Self() noexcept {
    return static_cast<Derived&>(*this);
  }

  IRS_FORCE_INLINE void TryPush(score_t& threshold, score_t score,
                                doc_id_t doc) noexcept {
    if (score > threshold) {
      Self().Push(threshold, score, doc);
    }
  }

#ifdef __AVX2__
  static constexpr int kCmpPred = _CMP_GT_OQ;

  IRS_FORCE_INLINE static uint64_t GetScoreMask(
    const score_t* IRS_RESTRICT scores, score_t threshold) noexcept {
    const auto v = _mm256_set1_ps(threshold);
    uint64_t mask = 0;
    for (int i = 0; i < 64; i += 8) {
      const uint64_t bits = _mm256_movemask_ps(
        _mm256_cmp_ps(_mm256_loadu_ps(scores + i), v, kCmpPred));
      mask |= bits << i;
    }
    return mask;
  }
#endif
};

class LoserScoreCollector final
  : public ScoreCollectorBase<LoserScoreCollector> {
  using Base = ScoreCollectorBase<LoserScoreCollector>;
  friend Base;

 public:
  LoserScoreCollector(score_t& score_threshold, std::span<ScoreDoc> hits)
    : Base{Tag::Loser, score_threshold},
      _hits{hits.data()},
      _k{hits.size()},
      _tree(hits.size()) {
    SDB_ASSERT(!hits.empty());
  }

  IRS_FORCE_INLINE size_t AcceptedCount() const noexcept { return _size; }

 private:
  struct Node {
    score_t score;
    uint32_t leaf;
  };

  static constexpr uint32_t kNone = std::numeric_limits<uint32_t>::max();

  IRS_FORCE_INLINE size_t Match(uint32_t leaf) const noexcept {
    return (_k + leaf) >> 1;
  }

  void Build() noexcept {
    Node* IRS_RESTRICT const tree = _tree.data();
    for (size_t i = 1; i < _k; ++i) {
      tree[i].leaf = kNone;
    }
    for (uint32_t leaf = 0; leaf != _k; ++leaf) {
      Node cur{_hits[leaf].score, leaf};
      for (size_t node = Match(leaf); node != 0; node >>= 1) {
        Node& slot = tree[node];
        if (slot.leaf == kNone) {
          slot = cur;
          cur.leaf = kNone;
          break;
        }
        if (slot.score < cur.score) {
          std::swap(slot, cur);
        }
      }
      if (cur.leaf != kNone) {
        _root = cur;
      }
    }
  }

  IRS_FORCE_INLINE void Replace(ScoreDoc hit) noexcept {
    Node* IRS_RESTRICT const tree = _tree.data();
    const uint32_t leaf = _root.leaf;
    _hits[leaf] = hit;
    Node cur{hit.score, leaf};
    for (size_t node = Match(leaf); node != 0; node >>= 1) {
      const Node loser = tree[node];
      const bool win = loser.score < cur.score;
      tree[node] = win ? cur : loser;
      cur = win ? loser : cur;
    }
    _root = cur;
  }

  IRS_FORCE_INLINE void Push(score_t& threshold, score_t score,
                             doc_id_t doc) noexcept {
    if (_size != _k) [[unlikely]] {
      _hits[_size++] = {score, doc, _current_segment};
      if (_size != _k) {
        return;
      }
      Build();
    } else {
      Replace({score, doc, _current_segment});
    }
    threshold = std::max(threshold, _root.score);
  }

  ScoreDoc* IRS_RESTRICT _hits;
  size_t _k;
  std::vector<Node> _tree;
  size_t _size = 0;
  Node _root{};
};

template<typename F>
IRS_FORCE_INLINE auto ResolveScoreCollector(ScoreCollector& collector, F&& f) {
  switch (collector.GetTag()) {
    case ScoreCollector::Tag::Loser:
      return std::forward<F>(f)(
        sdb::basics::downCast<LoserScoreCollector>(collector));
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

  // Emit ascending matched doc-ids in [min, max) into `out`, return the count.
  // Bitset-free counterpart of FillBlock for unscored scans. Self-positioning
  // like EmitScoredDocs: value() may sit before the window (or be
  // unpositioned), so docs < min are skipped -- the caller does NOT prime the
  // iterator. Preconditions: min < max; `out` has room for max - min ids.
  // Postcondition: value() == first doc >= max (or eof()), as for FillBlock.
  virtual uint32_t EmitDocs(doc_id_t* out, doc_id_t min, doc_id_t max) = 0;

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

  template<typename S>
  IRS_FORCE_INLINE static uint32_t EmitDocsImpl(S& self, doc_id_t* out,
                                                doc_id_t min, doc_id_t max) {
    uint32_t n = 0;
    for (auto doc = self.S::seek(min); doc < max; doc = self.S::advance()) {
      out[n++] = doc;
    }
    return n;
  }

  template<typename S>
  IRS_FORCE_INLINE static uint32_t EmitScoredDocsImpl(
    S& self, doc_id_t* out, score_t* scores, doc_id_t max,
    const ScoreFunction& scorer, ColumnArgsFetcher* fetcher, doc_id_t min) {
    uint32_t n = 0;
    auto doc = self.S::seek(min);
    while (true) {
      const uint32_t start = n;
      for (scores_size_t i = 0; i != kScoreBlock; ++i) {
        if (doc >= max) [[unlikely]] {
          if (i != 0) {
            if (fetcher) {
              fetcher->Fetch(std::span<const doc_id_t>{out + start, i});
            }
            scorer.Score(scores + start, i);
          }
          return n;
        }
        out[n++] = doc;
        self.S::FetchScoreArgs(i);
        doc = self.S::advance();
      }
      if (fetcher) {
        fetcher->FetchScoreBlock(
          std::span<const doc_id_t, kScoreBlock>{out + start, kScoreBlock});
      }
      scorer.ScoreBlock(scores + start);
    }
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
        fetcher.FetchScoreBlock(docs);
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

#define IRS_DOC_ITERATOR_FILL_BLOCK                                      \
  std::pair<irs::doc_id_t, bool> FillBlock(                              \
    irs::doc_id_t min, irs::doc_id_t max, uint64_t* mask,                \
    irs::FillBlockScoreContext score, irs::FillBlockMatchContext match)  \
    final {                                                              \
    return irs::DocIterator::FillBlockImpl(*this, min, max, mask, score, \
                                           match);                       \
  }

#define IRS_DOC_ITERATOR_COUNT \
  uint32_t count() final { return irs::DocIterator::CountImpl(*this); }

#define IRS_DOC_ITERATOR_COLLECT                                      \
  void Collect(const irs::ScoreFunction& scorer,                      \
               irs::ColumnArgsFetcher& fetcher,                       \
               irs::ScoreCollector& collector) final {                \
    irs::DocIterator::CollectImpl(*this, scorer, fetcher, collector); \
  }

#define IRS_DOC_ITERATOR_EMIT_DOCS                                            \
  uint32_t EmitDocs(irs::doc_id_t* out, irs::doc_id_t min, irs::doc_id_t max) \
    final {                                                                   \
    return irs::DocIterator::EmitDocsImpl(*this, out, min, max);              \
  }

#define IRS_DOC_ITERATOR_EMIT_SCORED_DOCS                                      \
  uint32_t EmitScoredDocs(irs::doc_id_t* out, irs::score_t* scores,            \
                          irs::doc_id_t max, const irs::ScoreFunction& scorer, \
                          irs::ColumnArgsFetcher* fetcher, irs::doc_id_t min)  \
    final {                                                                    \
    return irs::DocIterator::EmitScoredDocsImpl(*this, out, scores, max,       \
                                                scorer, fetcher, min);         \
  }

#define IRS_DOC_ITERATOR_DEFAULTS \
  IRS_DOC_ITERATOR_FILL_BLOCK     \
  IRS_DOC_ITERATOR_COUNT          \
  IRS_DOC_ITERATOR_COLLECT        \
  IRS_DOC_ITERATOR_EMIT_DOCS      \
  IRS_DOC_ITERATOR_EMIT_SCORED_DOCS

// What a field writer reads: each term hands over its posting list whole,
// which is what `PostingsWriter::Write` consumes. The write-side walk never
// asks what a term's postings cost, so it owes no record -- a separate
// contract rather than a mode of `TermIterator`.
struct TermOnlyIterator : Iterator<bytes_view, AttributeProvider> {
  using ptr = memory::managed_ptr<TermOnlyIterator>;

  // Return iterator over the associated posting list with the requested
  // features.
  [[nodiscard]] virtual DocIterator::ptr postings(
    IndexFeatures features) const = 0;
};

struct TermIterator : TermOnlyIterator {
  using ptr = memory::managed_ptr<TermIterator>;

  // Where the current term's postings live and how big they are. Decoding
  // happens here, once, so statistics collection and posting-list construction
  // share one parse of the entry. Answers the term the iterator stands on, so
  // it is called per term -- a pointer kept across a move reports the term it
  // was taken on. The reference is the iterator's own storage and dies with the
  // next move; copy to keep it.
  [[nodiscard]] virtual const PostingMeta& cookie() const = 0;
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

  // Position iterator at `value` exactly. Returns `true` when the dictionary
  // holds it, `false` otherwise -- and on `false` the iterator is left wherever
  // answering took it, so its value must not be read.
  //
  // Not `seek_ge` plus a comparison: a miss is answered as soon as the block
  // that would hold the key says it has no terms, where `seek_ge` still has to
  // read the block after it to have somewhere to stand. Every exact-match
  // filter goes through here for that reason.
  virtual bool seek(bytes_view value) = 0;
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
