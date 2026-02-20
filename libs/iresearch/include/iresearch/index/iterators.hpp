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

#include "basics/assert.h"
#include "basics/down_cast.h"
#include "basics/memory.hpp"
#include "basics/shared.hpp"
#include "basics/system-compiler.h"
#include "iresearch/formats/seek_cookie.hpp"
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
  ColumnArgsFetcher* fetcher;
};

struct FillBlockScoreContext {
  const ScoreFunction* score = nullptr;
  ColumnArgsFetcher* fetcher = nullptr;
  score_t* score_window = nullptr;
  ScoreMergeType merge_type = ScoreMergeType::Noop;
};

struct FillBlockMatchContext {
  uint32_t* matches = 0;
  size_t min_match_count = 0;
};

class ScoreCollector {
 public:
  enum class Tag {
    NthPartition,
    Generic,
  };

  IRS_FORCE_INLINE Tag GetTag() const noexcept { return _tag; }

  virtual void Add(score_t score, doc_id_t doc) = 0;

 protected:
  explicit ScoreCollector(Tag tag) noexcept : _tag{tag} {}

  ~ScoreCollector() = default;

 private:
  Tag _tag;
};

using ScoreDoc = std::pair<score_t, doc_id_t>;

class NthPartitionScoreCollector final : public ScoreCollector {
 public:
  explicit NthPartitionScoreCollector(score_t& score_threshold, size_t k,
                                      std::span<ScoreDoc> hits) noexcept
    : ScoreCollector{Tag::NthPartition},
      _score_threshold{&score_threshold},
      _hits_it{hits.data()},
      _hits_begin{hits.data()},
      _hits_pivot{hits.data() + k},
      _hits_end{hits.data() + hits.size()} {
    SDB_ASSERT(2 * k == hits.size());
  }

  void SetScoreThreshold(score_t& score_threshold) noexcept {
    SDB_ASSERT(score_threshold <= *_score_threshold);
    score_threshold = *_score_threshold;
    _score_threshold = &score_threshold;
  }

  IRS_FORCE_INLINE void Add(score_t score, doc_id_t doc) final {
    ++_count;
    if (score <= *_score_threshold) {
      return;
    }
    *_hits_it = {score, doc};
    ++_hits_it;
    if (_hits_it != _hits_end) {
      return;
    }
    _hits_it = _hits_pivot;
    std::nth_element(_hits_begin, _hits_pivot, _hits_end,
                     [](const auto& l, const auto& r) {
                       return std::get<score_t>(l) > std::get<score_t>(r);
                     });
    *_score_threshold = std::get<score_t>(*_hits_pivot);
  }

  IRS_FORCE_INLINE uint64_t Finalize() {
    std::sort(_hits_begin, _hits_end, [](const auto& l, const auto& r) {
      return std::get<score_t>(l) > std::get<score_t>(r);
    });
    return _count;
  }

 private:
  uint64_t _count = 0;
  score_t* IRS_RESTRICT _score_threshold = nullptr;
  ScoreDoc* IRS_RESTRICT _hits_it;
  ScoreDoc* IRS_RESTRICT const _hits_begin;
  ScoreDoc* IRS_RESTRICT const _hits_pivot;
  ScoreDoc* IRS_RESTRICT const _hits_end;
};

template<typename F>
IRS_FORCE_INLINE auto ResolveScoreCollector(ScoreCollector& collector, F&& f) {
  switch (collector.GetTag()) {
    case ScoreCollector::Tag::NthPartition:
      return std::forward<F>(f)(
        sdb::basics::downCast<NthPartitionScoreCollector>(collector));
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

  virtual doc_id_t value() const noexcept = 0;

  virtual doc_id_t advance() = 0;

  // deprecated: use advance() instead
  IRS_FORCE_INLINE bool next(this auto& self) {
    return !doc_limits::eof(self.advance());
  }

  // Position iterator at a specified target and returns current value
  // (for more information see class description)
  virtual doc_id_t seek(doc_id_t target) = 0;

  virtual void Collect(const ScoreFunction& scorer, ColumnArgsFetcher& fetcher,
                       ScoreCollector& collector) {
    CollectImpl(*this, scorer, fetcher, collector);
  }

  virtual void FetchScoreArgs(uint16_t index) {}

  virtual ScoreFunction PrepareScore(const PrepareScoreContext& ctx) {
    return {};
  }

  virtual uint32_t count() { return CountImpl(*this); }

  // TODO(mbkkt) Maybe implement FillBlock for all iterators?
  virtual std::pair<doc_id_t, bool> FillBlock(doc_id_t min, doc_id_t max,
                                              uint64_t* mask,
                                              FillBlockScoreContext score,
                                              FillBlockMatchContext match) {
    return FillBlockImpl(*this, min, max, mask, score, match);
  }

 protected:
  IRS_FORCE_INLINE static uint32_t CountImpl(auto& self) {
    uint32_t count = 0;
    while (self.next()) {
      ++count;
    }
    return count;
  }

  IRS_FORCE_INLINE static void CollectImpl(auto& self,
                                           const ScoreFunction& scorer,
                                           ColumnArgsFetcher& fetcher,
                                           ScoreCollector& c) {
    ABSL_CACHELINE_ALIGNED std::array<score_t, kScoreBlock> scores;
    ABSL_CACHELINE_ALIGNED std::array<doc_id_t, kScoreBlock> docs;
    ResolveScoreCollector(c, [&](auto& collector) IRS_FORCE_INLINE {
      while (true) {
        for (size_t i = 0; i != kScoreBlock; ++i) {
          const auto doc = self.advance();
          if (doc_limits::eof(doc)) [[unlikely]] {
            if (i != 0) {
              fetcher.Fetch({docs.data(), i});
              scorer.Score(scores.data(), i);
              for (size_t j = 0; j < i; ++j) {
                collector.Add(scores[j], docs[j]);
              }
            }
            return;
          }
          docs[i] = doc;
          self.FetchScoreArgs(i);
        }
        fetcher.Fetch(docs);
        scorer.ScoreBlock(scores.data());
        for (size_t j = 0; j != kScoreBlock; ++j) {
          collector.Add(scores[j], docs[j]);
        }
      }
    });
  }

  IRS_FORCE_INLINE static std::pair<doc_id_t, bool> FillBlockImpl(
    auto& self, doc_id_t min, doc_id_t max, uint64_t* mask,
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
  template<ScoreMergeType MergeType, bool TrackMatch>
  static std::pair<doc_id_t, bool> FillBlockImpl(
    auto& self, const doc_id_t min, const doc_id_t max,
    uint64_t* IRS_RESTRICT mask, [[maybe_unused]] FillBlockScoreContext score,
    [[maybe_unused]] FillBlockMatchContext match) {
    SDB_ASSERT(min < max);

    [[maybe_unused]] std::array<score_t, kScoreBlock> score_buf;
    [[maybe_unused]] std::array<doc_id_t, kScoreBlock> score_hits;
    [[maybe_unused]] uint16_t score_index = 0;
    [[maybe_unused]] auto flush_score = [&](size_t n) {
      if constexpr (MergeType != ScoreMergeType::Noop) {
        SDB_ASSERT(n);
        SDB_ASSERT(score.score);

        if (score.fetcher) {
          score.fetcher->Fetch(std::span{score_hits.data(), n});
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

    auto value = self.value();
    while (value < min) {
      value = self.advance();
    }
    bool empty = true;
    for (; value < max; value = self.advance()) {
      SDB_ASSERT(value >= min);
      const auto offset = value - min;

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
        score_hits[score_index] = value;
        self.FetchScoreArgs(score_index);
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

    return {value, empty};
  }
};

// Same as `DocIterator` but also support `reset()` operation
struct ResettableDocIterator : DocIterator {
  using ptr = memory::managed_ptr<ResettableDocIterator>;

  [[nodiscard]] static ResettableDocIterator::ptr empty() noexcept;

  // Reset iterator to initial state
  virtual void reset() = 0;
};

struct TermReader;

// An iterator providing sequential and random access to indexed fields
struct FieldIterator : Iterator<const TermReader&> {
  using ptr = memory::managed_ptr<FieldIterator>;

  [[nodiscard]] static FieldIterator::ptr empty() noexcept;

  // Position iterator at a specified target.
  // Return if the target is found, false otherwise.
  virtual bool seek(std::string_view target) = 0;
};

struct ColumnReader;

// An iterator providing sequential and random access to stored columns.
struct ColumnIterator : Iterator<const ColumnReader&> {
  using ptr = memory::managed_ptr<ColumnIterator>;

  [[nodiscard]] static ColumnIterator::ptr empty() noexcept;

  // Position iterator at a specified target.
  // Return if the target is found, false otherwise.
  virtual bool seek(std::string_view name) = 0;
};

// An iterator providing sequential access to term dictionary
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
  bool next = true;
  while (less(it.value(), target) && static_cast<bool>(next = it.next())) {
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
