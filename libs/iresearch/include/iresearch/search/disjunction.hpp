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

#include <absl/algorithm/container.h>

#include <bit>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <iresearch/formats/sparse_bitmap.hpp>
#include <iresearch/search/boolean_filter.hpp>
#include <iresearch/search/score.hpp>
#include <iresearch/search/score_function.hpp>
#include <iresearch/search/scorer.hpp>
#include <iresearch/utils/attribute_provider.hpp>
#include <limits>
#include <memory>
#include <utility>
#include <vector>

#include "basics/empty.hpp"
#include "basics/std.hpp"
#include "basics/system-compiler.h"
#include "conjunction.hpp"
#include "iresearch/index/iterators.hpp"
#include "iresearch/utils/type_limits.hpp"

// Disjunction is template for Adapter instead of direct use of ScoreAdapter
// only because of variadic phrase
namespace irs {
namespace detail {

template<size_t Size>
struct MinMatchBuffer {
  explicit MinMatchBuffer(size_t min_match_count) noexcept
    : _min_match_count(std::max(size_t(1), min_match_count)) {}

  uint32_t match_count(size_t i) const noexcept {
    SDB_ASSERT(i < Size);
    return _match_count[i];
  }

  uint32_t count() const noexcept {
    uint32_t count = 0;
    for (const auto match_count : _match_count) {
      count += match_count >= _min_match_count;
    }
    return count;
  }

  bool inc(size_t i) noexcept { return ++_match_count[i] < _min_match_count; }

  void clear() noexcept {
    std::memset(_match_count.data(), 0, sizeof _match_count);
  }

  void min_match_count(size_t min_match_count) noexcept {
    _min_match_count = std::max(min_match_count, _min_match_count);
  }
  size_t min_match_count() const noexcept { return _min_match_count; }

 private:
  size_t _min_match_count;
  std::array<uint32_t, Size> _match_count;
};

template<size_t Size>
class ScoreBuffer {
 public:
  score_t* get(size_t i) noexcept {
    SDB_ASSERT(i < Size);
    return &_buf[i];
  }

  score_t* data() noexcept { return _buf.data(); }

 private:
  std::array<score_t, Size> _buf;
};

struct SubScoresCtx : SubScores {
  size_t unscored = 0;

  size_t Size() const noexcept { return unscored + scores.size(); }

  void Clear() noexcept {
    scores.clear();
    sum_score = 0.f;
    unscored = 0;
  }
};

inline bool MakeSubScores(const auto& itrs, SubScoresCtx& scores) {
  SDB_ASSERT(scores.Size() == 0);
  scores.scores.reserve(itrs.size());
  for (auto& it : itrs) {
    const auto& score = it.score();
    if (score.IsDefault()) {
      ++scores.unscored;
      continue;
    }
    scores.scores.emplace_back(&score);
    const auto tail = score.max.tail;
    if (tail == std::numeric_limits<score_t>::max()) {
      return false;
    }
    scores.sum_score += tail;
  }
  absl::c_sort(scores.scores, [](const auto* lhs, const auto* rhs) noexcept {
    return lhs->max.tail > rhs->max.tail;
  });
  SDB_ASSERT(scores.Size() == itrs.size());
  return !scores.scores.empty();
}

}  // namespace detail

template<typename Adapter>
using IteratorVisitor = bool (*)(void*, Adapter&);

template<typename Adapter>
struct CompoundDocIterator : DocIterator {
  virtual void visit(void* ctx, IteratorVisitor<Adapter>) = 0;
};

// Wrapper around regular DocIterator to conform CompoundDocIterator API
template<typename Adapter>
class UnaryDisjunction : public CompoundDocIterator<Adapter> {
 public:
  UnaryDisjunction(Adapter it) : _it{std::move(it)} {}

  Attribute* GetMutable(TypeInfo::type_id type) noexcept final {
    return _it.GetMutable(type);
  }

  doc_id_t value() const noexcept final { return _it.value(); }

  doc_id_t advance() final { return _it.advance(); }

  doc_id_t seek(doc_id_t target) final { return _it.seek(target); }

  uint32_t count() final { return _it.count(); }

  void visit(void* ctx, IteratorVisitor<Adapter> visitor) final {
    SDB_ASSERT(ctx);
    SDB_ASSERT(visitor);
    visitor(ctx, _it);
  }

  void CollectData(uint16_t index) final { _it.CollectData(index); }

  uint32_t collect(std::span<doc_id_t> docs) final { return _it.collect(docs); }

 private:
  Adapter _it;
};

class DisjunctionScoreState : public ScoreCtx {
 public:
  void Collect(size_t i, uint16_t index) noexcept {
    SDB_ASSERT(i < _hits.size());
    _hits[i].PushBack(index);
  }

  uint16_t Index(size_t i) const noexcept {
    SDB_ASSERT(i < _hits.size());
    return static_cast<uint16_t>(_hits[i].Size());
  }

  template<ScoreMergeType MergeType, typename A, size_t N>
  ScoreFunction PrepareScore(std::span<A, N> itrs, uint16_t score_block,
                             auto min) {
    SDB_ASSERT(score_block);
    _sources.reserve(itrs.size());
    _sources.append_range(ToScores(itrs));
    if (_sources.empty()) {
      return ScoreFunction::Default();
    }

    _buf = std::make_unique<uint16_t[]>(_sources.size() * score_block);
    _scores = std::make_unique<score_t[]>(score_block);

    _hits.resize(_sources.size());
    auto p = _buf.get();
    for (auto& hit : _hits) {
      hit.Reset(p);
      p += score_block;
    }

    return {this,
            [](ScoreCtx* ctx, score_t* res, size_t n) noexcept {
              auto& self = static_cast<DisjunctionScoreState&>(*ctx);
              for (const auto& [hit, source] :
                   std::views::zip(self._hits, self._sources)) {
                const auto hits = hit.Size();
                if (hits == 0) {
                  continue;
                }

                // TODO(gnusi): we can score directly into res if hits == n and
                // it's a first iterator
                source->Score(self._scores.get(), hits);
                if (hits == n) {
                  Merge<MergeType>(res, self._scores.get(), hits);
                } else {
                  Merge<MergeType>(res, hit.Data(), self._scores.get(), hits);
                }
                hit.Clear();
              }
            },
            min};
  }

 private:
  std::vector<const ScoreFunction*> _sources;
  std::vector<Vector<uint16_t>> _hits;
  std::unique_ptr<uint16_t[]> _buf;
  std::unique_ptr<score_t[]> _scores;
};

template<ScoreMergeType MergeType>
class DisjunctionBase {
 public:
  static constexpr auto kMergeType = MergeType;
  static constexpr bool kHasScore = kMergeType != ScoreMergeType::Noop;

 protected:
  template<typename A, size_t N>
  void PrepareScore(ScoreAttr& score, std::span<A, N> itrs,
                    uint16_t score_block, auto min) {
    if constexpr (kHasScore) {
      score = _scores.template PrepareScore<MergeType>(itrs, score_block, min);
    }
  }

  [[no_unique_address]] utils::Need<kHasScore, DisjunctionScoreState> _scores;
};

template<typename Adapter, ScoreMergeType MergeType>
class BasicDisjunction : public CompoundDocIterator<Adapter>,
                         private DisjunctionBase<MergeType> {
  using Base = DisjunctionBase<MergeType>;

 public:
  using adapter = Adapter;
  using Base::kHasScore;
  using Base::kMergeType;

  BasicDisjunction(adapter&& lhs, adapter&& rhs, uint16_t score_block = 0)
    : BasicDisjunction{std::move(lhs), std::move(rhs),
                       [this] noexcept {
                         return CostAttr::extract(_itrs[0], 0) +
                                CostAttr::extract(_itrs[1], 0);
                       },
                       score_block, ResolveOverloadTag{}} {}

  BasicDisjunction(Adapter lhs, Adapter rhs, CostAttr::Type est,
                   uint16_t score_block)
    : BasicDisjunction{std::move(lhs), std::move(rhs), est, score_block,
                       ResolveOverloadTag{}} {}

  Attribute* GetMutable(TypeInfo::type_id type) noexcept final {
    return irs::GetMutable(_attrs, type);
  }

  doc_id_t value() const noexcept final {
    return std::get<DocAttr>(_attrs).value;
  }

  doc_id_t advance() final {
    NextImpl(_itrs[0]);
    NextImpl(_itrs[1]);

    auto& doc_value = std::get<DocAttr>(_attrs).value;
    return doc_value = std::min(_itrs[0].value(), _itrs[1].value());
  }

  doc_id_t seek(doc_id_t target) final {
    auto& doc_value = std::get<DocAttr>(_attrs).value;

    if (target <= doc_value) [[unlikely]] {
      return doc_value;
    }

    if (SeekImpl(_itrs[0], target) || SeekImpl(_itrs[1], target)) {
      return doc_value = target;
    }

    return doc_value = std::min(_itrs[0].value(), _itrs[1].value());
  }

  uint32_t count() final {
    uint32_t count = 0;
    auto lhs_value = _itrs[0].value();
    auto rhs_value = _itrs[1].value();
    while (true) {
      if (lhs_value < rhs_value) {
        lhs_value = _itrs[0].advance();
      } else if (rhs_value < lhs_value) {
        rhs_value = _itrs[1].advance();
      } else {
        lhs_value = _itrs[0].advance();
        rhs_value = _itrs[1].advance();
      }
      if (doc_limits::eof(lhs_value) && doc_limits::eof(rhs_value)) {
        return count;
      }
      ++count;
    }
  }

  void visit(void* ctx, IteratorVisitor<Adapter> visitor) final {
    SDB_ASSERT(ctx);
    SDB_ASSERT(visitor);

    auto& doc_value = std::get<DocAttr>(_attrs).value;
    // assume that seek or next has been called
    SDB_ASSERT(_itrs[0].value() >= doc_value);

    if (_itrs[0].value() == doc_value && !visitor(ctx, _itrs[0])) {
      return;
    }

    SeekImpl(_itrs[1], doc_value);
    if (_itrs[1].value() == doc_value) {
      visitor(ctx, _itrs[1]);
    }
  }

  uint32_t collect(std::span<doc_id_t> docs) final {
    return DocIterator::Collect(*this, docs);
  }

  void CollectData(uint16_t index) final {
    if constexpr (Base::kHasScore) {
      CollectIterator(0, index);
      CollectIterator(1, index);
    }
  }

 private:
  struct ResolveOverloadTag {};

  template<typename Estimation>
  BasicDisjunction(Adapter lhs, Adapter rhs, Estimation&& estimation,
                   uint16_t score_block, ResolveOverloadTag)
    : _itrs{std::move(lhs), std::move(rhs)} {
    std::get<CostAttr>(_attrs).reset(std::forward<Estimation>(estimation));

    PrepareScore(false, false, score_block);
  }

  void PrepareScore(bool /*wand*/, bool /*strict*/, uint16_t score_block) {
    if constexpr (Base::kHasScore) {
      auto& score = std::get<irs::ScoreAttr>(_attrs);

      if (_itrs[0].score().IsDefault() && _itrs[1].score().IsDefault()) {
        SDB_ASSERT(score.IsDefault());
        score = ScoreFunction::Default();
      } else if (!_itrs[0].score().IsDefault() &&
                 !_itrs[1].score().IsDefault()) {
        Base::PrepareScore(score, std::span{_itrs}, score_block,
                           ScoreFunction::NoopMin);
      } else {
        const auto& it = _itrs[_itrs[0].score().IsDefault() ? 1 : 0];
        score.Reset(*it.score().Ctx(), it.score().Func(),
                    ScoreFunction::NoopMin);
      }
    }
  }

  bool SeekImpl(Adapter& it, doc_id_t target) {
    return it.value() < target && target == it.seek(target);
  }

  void NextImpl(Adapter& it) {
    auto& doc_value = std::get<DocAttr>(_attrs).value;
    const auto value = it.value();

    if (doc_value == value) {
      it.advance();
    } else if (value < doc_value) {
      it.seek(doc_value + doc_id_t(!doc_limits::eof(doc_value)));
    }
  }

  void CollectIterator(size_t i, uint16_t index) {
    static_assert(Base::kHasScore);
    SDB_ASSERT(i < _itrs.size());
    auto& it = _itrs[i];
    if (it.score().IsDefault()) {
      return;
    }

    auto& doc_value = std::get<DocAttr>(_attrs).value;
    auto value = it.value();

    if (value < doc_value) {
      value = it.seek(doc_value);
    }

    if (value == doc_value) {
      it.CollectData(this->_scores.Index(i));
      this->_scores.Collect(i, index);
    }
  }

  using Attributes = std::tuple<DocAttr, ScoreAttr, CostAttr>;

  mutable std::array<adapter, 2> _itrs;
  Attributes _attrs;
};

// Disjunction optimized for a small number of iterators.
// Implements a linear search based disjunction.
// ----------------------------------------------------------------------------
//  Unscored iterators   Scored iterators
//   [0]   [1]   [2]   |   [3]    [4]     [5]
//    ^                |    ^                    ^
//    |                |    |                    |
//   begin             |   scored               end
//                     |   begin
// ----------------------------------------------------------------------------
template<typename Adapter, ScoreMergeType MergeType>
class SmallDisjunction : public CompoundDocIterator<Adapter>,
                         private DisjunctionBase<MergeType> {
 public:
  using Adapters = std::vector<Adapter>;

  static constexpr auto kMergeType = MergeType;
  static constexpr bool kHasScore = kMergeType != ScoreMergeType::Noop;

  SmallDisjunction(Adapters&& itrs, CostAttr::Type est, uint16_t score_block)
    : SmallDisjunction{std::move(itrs), est, score_block,
                       ResolveOverloadTag()} {}

  explicit SmallDisjunction(Adapters&& itrs, uint16_t score_block = 0)
    : SmallDisjunction{std::move(itrs),
                       [&] noexcept {
                         return std::accumulate(
                           _begin, _end, CostAttr::Type{0},
                           [](CostAttr::Type lhs, const Adapter& rhs) noexcept {
                             return lhs + CostAttr::extract(rhs, 0);
                           });
                       },
                       score_block, ResolveOverloadTag()} {}

  Attribute* GetMutable(TypeInfo::type_id type) noexcept final {
    return irs::GetMutable(_attrs, type);
  }

  doc_id_t value() const noexcept final {
    return std::get<DocAttr>(_attrs).value;
  }

  bool next_iterator_impl(Adapter& it) {
    auto& doc_value = std::get<DocAttr>(_attrs).value;
    const auto value = it.value();

    if (value == doc_value) {
      return !doc_limits::eof(it.advance());
    } else if (value < doc_value) {
      return !doc_limits::eof(it.seek(doc_value + 1));
    }

    return true;
  }

  doc_id_t advance() final {
    auto& doc_value = std::get<DocAttr>(_attrs).value;

    if (doc_limits::eof(doc_value)) {
      return doc_value;
    }

    doc_id_t min = doc_limits::eof();

    for (auto begin = _begin; begin != _end;) {
      auto& it = *begin;
      if (!next_iterator_impl(it)) {
        if (!remove_iterator(begin)) {
          return doc_value = doc_limits::eof();
        }
      } else {
        min = std::min(min, it.value());
        ++begin;
      }
    }

    return doc_value = min;
  }

  doc_id_t seek(doc_id_t target) final {
    auto& doc_value = std::get<DocAttr>(_attrs).value;

    if (doc_limits::eof(doc_value)) {
      return doc_value;
    }

    doc_id_t min = doc_limits::eof();

    for (auto begin = _begin; begin != _end;) {
      auto& it = *begin;

      if (it.value() < target) {
        const auto value = it.seek(target);

        if (value == target) {
          return doc_value = value;
        } else if (doc_limits::eof(value)) {
          if (!remove_iterator(begin)) {
            // exhausted
            return doc_value = doc_limits::eof();
          }
          continue;  // don't need to increment 'begin' here
        }
      }

      min = std::min(min, it.value());
      ++begin;
    }

    return doc_value = min;
  }

  uint32_t count() final { return DocIterator::Count(*this); }

  void visit(void* ctx, IteratorVisitor<Adapter> visitor) final {
    SDB_ASSERT(ctx);
    SDB_ASSERT(visitor);
    auto& doc_value = std::get<DocAttr>(_attrs).value;
    hitch_all_iterators();
    for (auto it = _begin; it != _end; ++it) {
      if (it->value() == doc_value && !visitor(ctx, *it)) {
        return;
      }
    }
  }

  void CollectData(uint16_t index) final {
    if constexpr (kHasScore) {
      const auto doc = std::get<DocAttr>(_attrs).value;

      size_t i = 0;
      for (auto begin = _scored_begin, end = _end; begin != end; ++begin) {
        auto value = begin->value();

        if (value < doc) {
          value = (*begin).seek(doc);
        }

        if (value == doc) {
          (*begin).CollectData(index);
          this->_scores.Collect(i++, index);
        }
      }
    }
  }

 private:
  struct ResolveOverloadTag {};

  template<typename Estimation>
  SmallDisjunction(Adapters&& itrs, Estimation&& estimation,
                   uint16_t score_block, ResolveOverloadTag)
    : _itrs(itrs.size()),
      _scored_begin(_itrs.begin()),
      _begin(_scored_begin),
      _end(_itrs.end()) {
    std::get<CostAttr>(_attrs).reset(std::forward<Estimation>(estimation));

    if (_itrs.empty()) {
      std::get<DocAttr>(_attrs).value = doc_limits::eof();
    }

    auto rbegin = _itrs.rbegin();
    for (auto& it : itrs) {
      if (it.score().IsDefault()) {
        *_scored_begin = std::move(it);
        ++_scored_begin;
      } else {
        *rbegin = std::move(it);
        ++rbegin;
      }
    }

    if constexpr (kHasScore) {
      this->PrepareScore(std::get<ScoreAttr>(_attrs),
                         std::span{_scored_begin, _end}, score_block,
                         ScoreFunction::NoopMin);
    }
  }

  uint32_t collect(std::span<doc_id_t> docs) final {
    return DocIterator::Collect(*this, docs);
  }

  bool remove_iterator(typename Adapters::iterator it) {
    if (it->score().IsDefault()) {
      std::swap(*it, *_begin);
      ++_begin;
    } else {
      std::swap(*it, *(--_end));
    }

    return _begin != _end;
  }

  void hitch_all_iterators() {
    auto& doc_value = std::get<DocAttr>(_attrs).value;

    if (_last_hitched_doc == doc_value) {
      return;  // nothing to do
    }
    for (auto begin = _begin; begin != _end; ++begin) {
      auto& it = *begin;
      if (it.value() < doc_value && doc_limits::eof(it.seek(doc_value))) {
        [[maybe_unused]] auto r = remove_iterator(begin);
        SDB_ASSERT(r);
      }
    }
    _last_hitched_doc = doc_value;
  }

  using Attributes = std::tuple<DocAttr, ScoreAttr, CostAttr>;
  using Iterator = typename Adapters::iterator;

  doc_id_t _last_hitched_doc{doc_limits::invalid()};
  Adapters _itrs;
  Iterator _scored_begin;  // beginning of scored doc iterator range
  Iterator _begin;         // beginning of unscored doc iterators range
  Iterator _end;           // end of scored doc iterator range
  Attributes _attrs;
};

// Heapsort-based disjunction
// ----------------------------------------------------------------------------
//   [0]   <-- begin
//   [1]      |
//   [2]      | head (min doc_id heap)
//   ...      |
//   [n-1] <-- end
//   [n]   <-- lead (accepted iterator)
// ----------------------------------------------------------------------------
template<typename Adapter, ScoreMergeType MergeType>
class Disjunction : public CompoundDocIterator<Adapter>,
                    private DisjunctionBase<MergeType> {
 public:
  using Adapters = std::vector<Adapter>;
  using Heap = std::vector<size_t>;
  using Iterator = Heap::iterator;

  static constexpr auto kMergeType = MergeType;
  static constexpr bool kHasScore = kMergeType != ScoreMergeType::Noop;
  static constexpr size_t kSmallDisjunctionUpperBound = 5;

  Disjunction(Adapters&& itrs, CostAttr::Type est, uint16_t score_block)
    : Disjunction{std::move(itrs), est, score_block, ResolveOverloadTag()} {}

  explicit Disjunction(Adapters&& itrs, uint16_t score_block = 0)
    : Disjunction{std::move(itrs),
                  [&] noexcept {
                    return absl::c_accumulate(
                      _itrs, CostAttr::Type{0},
                      [](CostAttr::Type lhs, const Adapter& rhs) noexcept {
                        return lhs + CostAttr::extract(rhs, 0);
                      });
                  },
                  score_block, ResolveOverloadTag{}} {}

  Attribute* GetMutable(TypeInfo::type_id type) noexcept final {
    return irs::GetMutable(_attrs, type);
  }

  doc_id_t value() const noexcept final {
    return std::get<DocAttr>(_attrs).value;
  }

  doc_id_t advance() final {
    auto& doc_value = std::get<DocAttr>(_attrs).value;

    if (doc_limits::eof(doc_value)) {
      return doc_value;
    }

    while (lead().value() <= doc_value) {
      const auto target = lead().value() == doc_value
                            ? lead().advance()
                            : lead().seek(doc_value + 1);
      const bool exhausted = doc_limits::eof(target);

      if (exhausted && !remove_lead()) {
        return doc_value = doc_limits::eof();
      }

      refresh_lead();
    }

    return doc_value = lead().value();
  }

  doc_id_t seek(doc_id_t target) final {
    auto& doc_value = std::get<DocAttr>(_attrs).value;

    if (doc_limits::eof(doc_value)) {
      return doc_value;
    }

    while (lead().value() < target) {
      const auto value = lead().seek(target);

      if (doc_limits::eof(value) && !remove_lead()) {
        return doc_value = doc_limits::eof();
      } else if (value != target) {
        refresh_lead();
      }
    }

    return doc_value = lead().value();
  }

  uint32_t count() final { return DocIterator::Count(*this); }

  uint32_t collect(std::span<doc_id_t> docs) final {
    return DocIterator::Collect(*this, docs);
  }

  void CollectData(uint16_t index) final {
    if constexpr (kHasScore) {
      SDB_ASSERT(!_heap.empty());

      const auto its = hitch_all_iterators();

      if (const auto doc = std::get<DocAttr>(_attrs).value;
          top().value() == doc) {
        irstd::heap::ForEachIf(
          its.first, its.second,
          [&](const size_t it) noexcept {
            SDB_ASSERT(it < _itrs.size());
            return _itrs[it].value() == doc;
          },
          [&](size_t it) {
            SDB_ASSERT(it < _itrs.size());
            if (auto& score = _itrs[it].score(); !score.IsDefault()) {
              _itrs[it].CollectData(index);
              this->_scores.Collect(it, index);  // TODO(gnusi): fix index
            }
          });
      }
    }
  }

  void visit(void* ctx, IteratorVisitor<Adapter> visitor) final {
    SDB_ASSERT(ctx);
    SDB_ASSERT(visitor);
    if (_heap.empty()) {
      return;
    }
    hitch_all_iterators();
    auto& lead = _itrs[_heap.back()];
    auto cont = visitor(ctx, lead);
    if (cont && _heap.size() > 1) {
      auto value = lead.value();
      irstd::heap::ForEachIf(
        _heap.cbegin(), _heap.cend() - 1,
        [this, value, &cont](const size_t it) {
          SDB_ASSERT(it < _itrs.size());
          return cont && _itrs[it].value() == value;
        },
        [this, ctx, visitor, &cont](const size_t it) {
          SDB_ASSERT(it < _itrs.size());
          cont = visitor(ctx, _itrs[it]);
        });
    }
  }

 private:
  struct ResolveOverloadTag {};

  using Attributes = std::tuple<DocAttr, ScoreAttr, CostAttr>;

  template<typename Estimation>
  Disjunction(Adapters&& itrs, Estimation&& estimation, uint16_t score_block,
              ResolveOverloadTag)
    : _itrs{std::move(itrs)} {
    // since we are using heap in order to determine next document,
    // in order to avoid useless make_heap call we expect that all
    // iterators are equal here
    // SDB_ASSERT(irstd::AllEqual(itrs_.begin(), itrs_.end()));
    std::get<CostAttr>(_attrs).reset(std::forward<Estimation>(estimation));

    if (_itrs.empty()) {
      std::get<DocAttr>(_attrs).value = doc_limits::eof();
    }

    // prepare external heap
    _heap.resize(_itrs.size());
    absl::c_iota(_heap, size_t{0});

    if constexpr (kHasScore) {
      this->PrepareScore(std::get<irs::ScoreAttr>(_attrs), std::span{_itrs},
                         score_block, ScoreFunction::NoopMin);
    }
  }

  template<typename Iterator>
  void push(Iterator begin, Iterator end) noexcept {
    std::push_heap(begin, end, [&](const auto lhs, const auto rhs) noexcept {
      SDB_ASSERT(lhs < _itrs.size());
      SDB_ASSERT(rhs < _itrs.size());
      return _itrs[lhs].value() > _itrs[rhs].value();
    });
  }

  template<typename Iterator>
  void pop(Iterator begin, Iterator end) noexcept {
    std::pop_heap(begin, end, [&](const auto lhs, const auto rhs) noexcept {
      SDB_ASSERT(lhs < _itrs.size());
      SDB_ASSERT(rhs < _itrs.size());
      return _itrs[lhs].value() > _itrs[rhs].value();
    });
  }

  // Removes lead iterator.
  // Returns true - if the disjunction condition still can be satisfied,
  // false - otherwise.
  bool remove_lead() noexcept {
    _heap.pop_back();

    if (!_heap.empty()) {
      pop(_heap.begin(), _heap.end());
      return true;
    }

    return false;
  }

  void refresh_lead() noexcept {
    auto begin = _heap.begin(), end = _heap.end();
    push(begin, end);
    pop(begin, end);
  }

  Adapter& lead() noexcept {
    SDB_ASSERT(!_heap.empty());
    SDB_ASSERT(_heap.back() < _itrs.size());
    return _itrs[_heap.back()];
  }

  Adapter& top() noexcept {
    SDB_ASSERT(!_heap.empty());
    SDB_ASSERT(_heap.front() < _itrs.size());
    return _itrs[_heap.front()];
  }

  std::pair<Iterator, Iterator> hitch_all_iterators() {
    // hitch all iterators in head to the lead (current doc_)
    SDB_ASSERT(!_heap.empty());
    auto begin = _heap.begin(), end = _heap.end() - 1;

    auto& doc_value = std::get<DocAttr>(_attrs).value;
    while (begin != end && top().value() < doc_value) {
      const auto value = top().seek(doc_value);

      if (doc_limits::eof(value)) {
        // remove top
        pop(begin, end);
        std::swap(*--end, _heap.back());
        _heap.pop_back();
      } else {
        // refresh top
        pop(begin, end);
        push(begin, end);
      }
    }
    return {begin, end};
  }

  Adapters _itrs;
  Heap _heap;
  Attributes _attrs;
};

enum class MatchType {
  Match,
  MinMatchFast,
  MinMatch,
};

template<MatchType MinMatch, bool SeekReadahead, size_t NumBlocks = 8>
struct BlockDisjunctionTraits {
  // "false" - iterator is used for min match filtering,
  // "true" - otherwise
  static constexpr bool kMinMatchEarlyPruning =
    MatchType::MinMatchFast == MinMatch;

  // "false" - iterator is used for min match filtering,
  // "true" - otherwise
  static constexpr bool kMinMatch =
    kMinMatchEarlyPruning || MatchType::Match != MinMatch;

  // Use readahead buffer for random access
  static constexpr bool kSeekReadahead = SeekReadahead;

  // Size of the readhead buffer in blocks
  static constexpr size_t kNumBlocks = NumBlocks;
};

// The implementation reads ahead 64*NumBlocks documents.
// It isn't optimized for conjunction case when the requested min match
// count equals to a number of input iterators.
// It's better to to use a dedicated "conjunction" iterator.
template<typename Adapter, ScoreMergeType MergeType, typename Traits>
class BlockDisjunction : public DocIterator, private ScoreCtx {
 public:
  using Adapters = std::vector<Adapter>;

  static constexpr auto kMergeType = MergeType;
  static constexpr bool kHasScore = kMergeType != ScoreMergeType::Noop;

  BlockDisjunction(Adapters&& itrs, CostAttr::Type est, uint16_t score_block)
    : BlockDisjunction{std::move(itrs), 1, detail::SubScoresCtx{}, est,
                       score_block} {}

  BlockDisjunction(Adapters&& itrs, size_t min_match_count,
                   detail::SubScoresCtx&& scores, CostAttr::Type est,
                   uint16_t score_block)
    : BlockDisjunction{
        std::move(itrs),   min_match_count, est,
        std::move(scores), score_block,     ResolveOverloadTag{}} {}

  explicit BlockDisjunction(Adapters&& itrs, uint16_t score_block = 0)
    : BlockDisjunction{std::move(itrs), 1, {}, score_block} {}

  BlockDisjunction(Adapters&& itrs, size_t min_match_count,
                   detail::SubScoresCtx&& scores, uint16_t score_block)
    : BlockDisjunction{std::move(itrs),
                       min_match_count,
                       [&] noexcept {
                         return absl::c_accumulate(
                           _itrs, CostAttr::Type{0},
                           [](CostAttr::Type lhs, const Adapter& rhs) noexcept {
                             return lhs + CostAttr::extract(rhs, 0);
                           });
                       },
                       std::move(scores),
                       score_block,
                       ResolveOverloadTag{}} {}

  size_t MatchCount() const noexcept { return _match_count; }

  Attribute* GetMutable(TypeInfo::type_id type) noexcept final {
    return irs::GetMutable(_attrs, type);
  }

  doc_id_t value() const noexcept final {
    return std::get<DocAttr>(_attrs).value;
  }

  doc_id_t advance() final {
    auto& doc_value = std::get<DocAttr>(_attrs).value;

    do {
      while (_cur == 0) {
        if (_begin >= std::end(_mask)) {
          if (Refill()) {
            SDB_ASSERT(_cur);
            break;
          }

          _match_count = 0;
          return doc_value = doc_limits::eof();
        }

        _cur = *_begin++;
        _doc_base += BitsRequired<uint64_t>();
        if constexpr (Traits::kMinMatch || kHasScore) {
          _buf_offset += BitsRequired<uint64_t>();
        }
      }

      const size_t offset = std::countr_zero(_cur);
      irs::UnsetBit(_cur, offset);

      [[maybe_unused]] const size_t buf_offset = _buf_offset + offset;

      if constexpr (Traits::kMinMatch) {
        _match_count = _match_buf.match_count(buf_offset);

        if (_match_count < _match_buf.min_match_count()) {
          continue;
        }
      }

      doc_value = _doc_base + doc_id_t(offset);

      return doc_value;
    } while (Traits::kMinMatch);
    SDB_UNREACHABLE();
  }

  doc_id_t seek(doc_id_t target) final {
    auto& doc_value = std::get<DocAttr>(_attrs).value;

    if (target <= doc_value) [[unlikely]] {
      return doc_value;
    }

    if (target < _max) {
      const doc_id_t block_base = (_max - kWindow);

      target -= block_base;
      const doc_id_t block_offset = target / kBlockSize;

      _doc_base = block_base + block_offset * kBlockSize;
      _begin = _mask + block_offset + 1;

      SDB_ASSERT(_begin > std::begin(_mask) && _begin <= std::end(_mask));
      _cur = _begin[-1] & ((~UINT64_C(0)) << target % kBlockSize);

      return advance();
    }

    doc_value = doc_limits::eof();

    if constexpr (Traits::kMinMatch) {
      _match_count = 0;
    }

    VisitAndPurge([&](auto& it) {
      const auto value = it.seek(target);

      if (doc_limits::eof(value)) {
        // exhausted
        return false;
      }

      if (value < doc_value) {
        doc_value = value;
        if constexpr (Traits::kMinMatch) {
          _match_count = 1;
        }
      } else if constexpr (Traits::kMinMatch) {
        if (target == value) {
          ++_match_count;
        }
      }

      return true;
    });

    if (_itrs.empty()) {
      _match_count = 0;
      return doc_value = doc_limits::eof();
    }

    SDB_ASSERT(!doc_limits::eof(doc_value));
    _cur = 0;
    _begin = std::end(_mask);  // enforce "refill()" for upcoming "next()"
    _max = doc_value;

    if constexpr (Traits::kSeekReadahead) {
      _min = doc_value;
      return advance();
    } else {
      _min = doc_value + 1;
      _buf_offset = 0;

      if constexpr (Traits::kMinMatch) {
        if (_match_count < _match_buf.min_match_count()) {
          return advance();
        }
      }

      if constexpr (kHasScore) {
        for (auto& it : _itrs) {
          // TODO(gnusi): we have to set a new score/collect function to support
          // seek
          if (!it.score().IsDefault() && doc_value == it.value()) {
            it.CollectData(_score_buf.stream.Index());
            _score_buf.stream.CollectData(0);  // TODO(gnusi): fix index
          }
        }
      }
      return doc_value;
    }
  }

  uint32_t count() final {
    uint32_t count = 0;

    while (_cur != 0 && next()) [[unlikely]] {
      ++count;
    }

    while (Refill()) {
      if constexpr (Traits::kMinMatch) {
        count += _match_buf.count();
      } else {
        for (const auto word : _mask) {
          count += std::popcount(word);
        }
      }
    }

    _match_count = 0;
    std::get<DocAttr>(_attrs).value = doc_limits::eof();
    return count;
  }

  void CollectData(uint16_t index) final {
    if constexpr (kHasScore) {
      SDB_ASSERT(_buf_offset < std::numeric_limits<uint16_t>::max());
      _score_buf.CollectData(static_cast<uint16_t>(_buf_offset), index);
    }
  }

 private:
  static constexpr doc_id_t kBlockSize = BitsRequired<uint64_t>();

  static constexpr doc_id_t kNumBlocks =
    static_cast<doc_id_t>(std::max(size_t(1), Traits::kNumBlocks));

  static constexpr doc_id_t kWindow = kBlockSize * kNumBlocks;

  static_assert(kBlockSize * size_t(kNumBlocks) <
                std::numeric_limits<doc_id_t>::max());

  using Attributes = std::tuple<DocAttr, ScoreAttr, CostAttr>;

  struct ResolveOverloadTag {};

  uint32_t collect(std::span<doc_id_t> docs) final {
    return DocIterator::Collect(*this, docs);
  }

  template<typename Estimation>
  BlockDisjunction(Adapters&& itrs, size_t min_match_count,
                   Estimation&& estimation, detail::SubScoresCtx&& scores,
                   uint16_t score_block, ResolveOverloadTag)
    : _itrs(std::move(itrs)),
      _match_count(_itrs.empty() ? size_t(0)
                                 : static_cast<size_t>(!Traits::kMinMatch)),
      _match_buf(min_match_count),
      _scores(std::move(scores)) {
    std::get<CostAttr>(_attrs).reset(std::forward<Estimation>(estimation));

    if (_itrs.empty()) {
      std::get<DocAttr>(_attrs).value = doc_limits::eof();
    }

    if constexpr (kHasScore) {
      this->PrepareScore(score_block);
    }

    if (Traits::kMinMatch && min_match_count > 1) {
      // sort subnodes in ascending order by their cost
      // FIXME(gnusi) don't use extract
      absl::c_sort(_itrs, [](const auto& lhs, const auto& rhs) noexcept {
        return CostAttr::extract(lhs, 0) < CostAttr::extract(rhs, 0);
      });

      // FIXME(gnusi): fix estimation, we have to estimate only min_match
      // iterators
    }
  }

  template<typename Visitor>
  void VisitAndPurge(Visitor visitor) {
    auto* begin = _itrs.data();
    auto* end = _itrs.data() + _itrs.size();

    while (begin != end) {
      if (!visitor(*begin)) {
        // TODO(mbkkt) It looks good, but only for wand case
        // scores_.unscored -= begin->score().IsDefault();
        irstd::SwapRemove(_itrs, begin);
        --end;

        if constexpr (Traits::kMinMatchEarlyPruning) {
          // we don't need precise match count
          if (_itrs.size() < _match_buf.min_match_count()) {
            // can't fulfill min match requirement anymore
            _itrs.clear();
            return;
          }
        }
      } else {
        ++begin;
      }
    }

    if constexpr (Traits::kMinMatch && !Traits::kMinMatchEarlyPruning) {
      // we need precise match count, so can't break earlier
      if (_itrs.size() < _match_buf.min_match_count()) {
        // can't fulfill min match requirement anymore
        _itrs.clear();
        return;
      }
    }
  }

  void Reset() noexcept {
    std::memset(_mask, 0, sizeof _mask);
    if constexpr (Traits::kMinMatch) {
      _match_buf.clear();
    }
  }

  bool Refill() {
    if (_itrs.empty()) {
      return false;
    }

    if constexpr (!Traits::kMinMatch) {
      Reset();
    }

    bool empty = true;

    if constexpr (kHasScore) {
      // TODO(gnusi): can we avoid that?
      _score_buf.stream.ClearWindow();
    }

    do {
      if constexpr (Traits::kMinMatch) {
        // in min match case we need to clear
        // internal buffers on every iteration
        Reset();
      }

      _doc_base = _min;
      _max = _min + kWindow;
      _min = doc_limits::eof();

      VisitAndPurge([this, &empty](auto& it) mutable {
        // FIXME
        // for min match case we can skip the whole block if
        // we can't satisfy match_buf_.min_match_count() conditions, namely
        // if constexpr (Traits::kMinMatch) {
        //  if (empty && (&it + (match_buf_.min_match_count() -
        //  match_buf_.max_match_count()) < (itrs_.data() + itrs_.size()))) {
        //    // skip current block
        //    return true;
        //  }
        //}

        if constexpr (kHasScore) {
          if (!it.score().IsDefault()) {
            _score_buf.stream.SetScore(it.score());
            const auto r = this->Refill<true>(it, empty);
            _score_buf.stream.Finish();
            return r;
          }
        }

        return this->Refill<false>(it, empty);
      });
    } while (empty && !_itrs.empty());

    if (empty) {
      // exhausted
      SDB_ASSERT(_itrs.empty());
      return false;
    }

    _cur = *_mask;
    _begin = _mask + 1;
    if constexpr (Traits::kMinMatch || kHasScore) {
      _buf_offset = 0;
    }
    while (!_cur) {
      _cur = *_begin++;
      _doc_base += BitsRequired<uint64_t>();
      if constexpr (Traits::kMinMatch || kHasScore) {
        _buf_offset += BitsRequired<uint64_t>();
      }
    }
    SDB_ASSERT(_cur);

    return true;
  }

  template<bool Score>
  bool Refill(Adapter& it, bool& empty) {
    auto value = it.value();

    // disjunction is 1 step next behind, that may happen:
    // - before the very first next()
    // - after seek() in case of 'kSeekReadahead == false'
    if (value < _doc_base) {
      value = it.advance();
    }

    while (!doc_limits::eof(value)) {
      if (value >= _max) {
        _min = std::min(value, _min);
        return true;
      }

      const auto offset = value - _doc_base;

      SetBit(_mask[offset / kBlockSize], offset % kBlockSize);

      if constexpr (Score) {
        it.CollectData(_score_buf.stream.Index());
        _score_buf.stream.CollectData(offset);
      }

      if constexpr (Traits::kMinMatch) {
        empty &= _match_buf.inc(offset);
      } else {
        empty = false;
      }
      value = it.advance();
    }
    return false;  // exhausted
  }

  static_assert(kWindow <= std::numeric_limits<uint16_t>::max());

  class ScoreStream {
   public:
    void Init(uint16_t score_block) {
      _hits.Realloc(score_block);
      _scores.Realloc(score_block);
    }

    uint16_t Index() const noexcept {
      return static_cast<uint16_t>(_hits.Size());
    }

    void CollectData(uint16_t hit) {
      _hits.PushBack(hit);

      if (_hits.Size() == _hits.Capacity()) {
        Flush();
      }
    }

    void ClearWindow() noexcept {
      std::memset(_score_window.data(), 0, sizeof(score_t) * kWindow);
    }

    void Finish() {
      if (_hits.Size()) {
        Flush();
      }
    }

    auto GetScore(uint16_t index) noexcept { return _scores.Data()[index]; }

    auto GetHits() noexcept { return std::span{_hits.Data(), _hits.Size()}; }

    void SetScore(const ScoreFunction& score) noexcept {
      _score = &score;
      SDB_ASSERT(_hits.Size() == 0);
    }

    void Flush() {
      SDB_ASSERT(_hits.Size());

      _score->Score(_scores.Data(), _hits.Size());
      // TODO(gnusi): can optimize if dense
      Merge<MergeType>(_score_window.data(), _hits.Data(), _scores.Data(),
                       _hits.Size());
      _hits.Clear();
    }

   private:
    std::array<score_t, kWindow> _score_window;
    FixedBuffer<uint16_t> _hits;
    FixedBuffer<score_t> _scores;
    const ScoreFunction* _score = nullptr;
  };

  struct ScoreState : ScoreCtx {
    // TODO(gnuis): it's big allocate on heap?
    ScoreStream stream;
    std::unique_ptr<score_t[]> result;

    void CollectData(uint16_t offset, uint16_t index) noexcept {
      result[index] = stream.GetScore(offset);
    }

    ScoreFunction PrepareScore(ScoreCtx* ctx, uint16_t score_block, auto min) {
      result = std::make_unique<score_t[]>(score_block);
      stream.Init(score_block);

      return {this,
              [](ScoreCtx* ctx, score_t* res, size_t n) noexcept {
                auto& self = static_cast<BlockDisjunction&>(*ctx);
                std::memcpy(res, self._score_buf.result.get(),
                            n * sizeof(score_t));
              },
              min};
    }
  };

  ScoreFunction PrepareScore(uint16_t score_block) {
    auto& score = std::get<ScoreAttr>(_attrs);
    auto min = ScoreFunction::NoopMin;
    if (!_scores.scores.empty()) {
      score.max.leaf = score.max.tail = _scores.sum_score;
      min = [](ScoreCtx* ctx, score_t arg) noexcept {
        auto& self = static_cast<BlockDisjunction&>(*ctx);
        if (self._scores.Size() != self._itrs.size()) [[unlikely]] {
          self._scores.Clear();
          detail::MakeSubScores(self._itrs, self._scores);
          auto& score = std::get<ScoreAttr>(self._attrs);
          // TODO(mbkkt) We cannot change tail now
          // Because it needs to recompute sum_score for our parent iterator
          score.max.leaf /* = score.max.tail */ = self._scores.sum_score;
        }
        auto it = self._scores.scores.begin();
        auto end = self._scores.scores.end();
        [[maybe_unused]] size_t min_match = 0;
        [[maybe_unused]] score_t sum = 0.f;
        while (it != end) {
          auto next = end;
          if constexpr (Traits::kMinMatch) {
            if (arg > sum) {  // TODO(mbkkt) strict wand: >=
              ++min_match;
              next = it + 1;
            }
            sum += (*it)->max.tail;
          }
          const auto others = self._scores.sum_score - (*it)->max.tail;
          if (arg > others) {
            // For common usage `arg - others <= (*it)->max.tail` -- is true
            (*it)->Min(arg - others);
            next = it + 1;
          }
          it = next;
        }
        if constexpr (Traits::kMinMatch) {
          self._match_buf.min_match_count(min_match);
        }
      };

      score = _score_buf.PrepareScore(this, score_block, min);
    }
  }

  uint64_t _mask[kNumBlocks]{};
  Adapters _itrs;
  uint64_t* _begin{std::end(_mask)};
  uint64_t _cur{};
  doc_id_t _doc_base{doc_limits::invalid()};
  doc_id_t _min{doc_limits::min()};      // base doc id for the next mask
  doc_id_t _max{doc_limits::invalid()};  // max doc id in the current mask
  Attributes _attrs;
  size_t _match_count;
  size_t _buf_offset{};  // offset within a buffer
  [[no_unique_address]] utils::Need<kHasScore, ScoreState> _score_buf;
  [[no_unique_address]] utils::Need<Traits::kMinMatch,
                                    detail::MinMatchBuffer<kWindow>> _match_buf;
  // TODO(mbkkt) We don't need scores_ for not wand,
  // but we don't want to generate more functions, than necessary
  [[no_unique_address]] utils::Need<kHasScore, detail::SubScoresCtx> _scores;
};

template<typename Adapter, ScoreMergeType MergeType>
using DisjunctionIterator =
  BlockDisjunction<Adapter, MergeType,
                   BlockDisjunctionTraits<MatchType::Match, false>>;

template<typename Adapter, ScoreMergeType MergeType>
using MinMatchIterator =
  BlockDisjunction<Adapter, MergeType,
                   BlockDisjunctionTraits<MatchType::MinMatch, false>>;

template<typename T>
struct RebindIterator;

template<typename Adapter, ScoreMergeType MergeType>
struct RebindIterator<Disjunction<Adapter, MergeType>> {
  using Unary = UnaryDisjunction<Adapter>;
  using Basic = BasicDisjunction<Adapter, MergeType>;
  using Small = SmallDisjunction<Adapter, MergeType>;
  using Wand = void;
};

template<typename Adapter, ScoreMergeType MergeType>
struct RebindIterator<DisjunctionIterator<Adapter, MergeType>> {
  using Unary = void;  // block disjunction doesn't support visitor
  using Basic = BasicDisjunction<Adapter, MergeType>;
  using Small = void;  // block disjunction always faster than small
  using Wand = DisjunctionIterator<Adapter, MergeType>;
};

template<typename Adapter, ScoreMergeType MergeType>
struct RebindIterator<MinMatchIterator<Adapter, MergeType>> {
  using Disjunction = DisjunctionIterator<Adapter, MergeType>;
  using Wand = MinMatchIterator<Adapter, MergeType>;
};

// Returns disjunction iterator created from the specified sub iterators
template<typename Disjunction, typename... Args>
DocIterator::ptr MakeDisjunction(WandContext ctx,
                                 typename Disjunction::Adapters&& itrs,
                                 Args&&... args) {
  const auto size = itrs.size();

  if (0 == size) {
    // Empty or unreachable search criteria
    return DocIterator::empty();
  }

  if (1 == size) {
    using UnaryDisjunction = typename RebindIterator<Disjunction>::Unary;
    if constexpr (std::is_void_v<UnaryDisjunction>) {
      return std::move(itrs.front());
    } else {
      SDB_ASSERT(!ctx.Enabled());
      return memory::make_managed<UnaryDisjunction>(std::move(itrs.front()));
    }
  }

  using BasicDisjunction = typename RebindIterator<Disjunction>::Basic;
  if constexpr (!std::is_void_v<BasicDisjunction>) {
    if (2 == size) {
      // 2-way disjunction
      return memory::make_managed<BasicDisjunction>(
        std::move(itrs.front()), std::move(itrs.back()),
        std::forward<Args>(args)...);
    }
  }

  using SmallDisjunction = typename RebindIterator<Disjunction>::Small;
  if constexpr (!std::is_void_v<SmallDisjunction>) {
    if (size <= Disjunction::kSmallDisjunctionUpperBound) {
      return memory::make_managed<SmallDisjunction>(
        std::move(itrs), std::forward<Args>(args)...);
    }
  }

  using Wand = typename RebindIterator<Disjunction>::Wand;
  if constexpr (!std::is_void_v<Wand>) {
    if (ctx.Enabled()) {
      // TODO(mbkkt) root optimization
      // TODO(mbkkt) block wand/maxscore optimization
      detail::SubScoresCtx scores;
      if (detail::MakeSubScores(itrs, scores)) {
        return memory::make_managed<Wand>(std::move(itrs), size_t{1},
                                          std::move(scores),
                                          std::forward<Args>(args)...);
      }
    }
  }

  return memory::make_managed<Disjunction>(std::move(itrs),
                                           std::forward<Args>(args)...);
}

// Returns weak conjunction iterator created from the specified sub iterators
template<typename WeakConjunction, typename... Args>
DocIterator::ptr MakeWeakDisjunction(WandContext ctx,
                                     typename WeakConjunction::Adapters&& itrs,
                                     size_t min_match, uint16_t score_block,
                                     Args&&... args) {
  // This case must be handled by a caller, we're unable to process it here
  SDB_ASSERT(min_match > 0);

  const auto size = itrs.size();

  if (0 == size || min_match > size) {
    // Empty or unreachable search criteria
    return DocIterator::empty();
  }

  if (1 == min_match) {
    // Pure disjunction
    using Disjunction = typename RebindIterator<WeakConjunction>::Disjunction;
    return MakeDisjunction<Disjunction>(ctx, std::move(itrs), score_block,
                                        std::forward<Args>(args)...);
  }

  if (min_match == size) {
    // Pure conjunction
    return MakeConjunction<WeakConjunction::kMergeType>(ctx, std::move(itrs),
                                                        score_block);
  }

  if (ctx.Enabled()) {
    detail::SubScoresCtx scores;
    if (detail::MakeSubScores(itrs, scores)) {
      // TODO(mbkkt) root optimization
      // TODO(mbkkt) block wand/maxscore optimization
      using Wand = typename RebindIterator<WeakConjunction>::Wand;
      return memory::make_managed<Wand>(std::move(itrs), min_match,
                                        std::move(scores), score_block,
                                        std::forward<Args>(args)...);
    }
  }

  return memory::make_managed<WeakConjunction>(
    std::move(itrs), min_match, detail::SubScoresCtx{}, score_block,
    std::forward<Args>(args)...);
}

}  // namespace irs
