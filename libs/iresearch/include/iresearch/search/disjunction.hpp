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

#include "basics/std.hpp"
#include "basics/system-compiler.h"
#include "conjunction.hpp"
#include "iresearch/index/iterators.hpp"
#include "iresearch/utils/type_limits.hpp"

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

  void clear() noexcept { std::memset(_match_count, 0, sizeof _match_count); }

  void min_match_count(size_t min_match_count) noexcept {
    _min_match_count = std::max(min_match_count, _min_match_count);
  }
  size_t min_match_count() const noexcept { return _min_match_count; }

 private:
  size_t _min_match_count;
  uint32_t _match_count[Size];
};

class ScoreBuffer {
 public:
  ScoreBuffer(size_t num_buckets, size_t size)
    : _bucket_size{num_buckets * sizeof(score_t)},
      _buf_size{_bucket_size * size},
      _buf{!_buf_size ? nullptr : new byte_type[_buf_size]} {
    if (_buf) {
      std::memset(data(), 0, this->size());
    }
  }

  score_t* get(size_t i) noexcept {
    SDB_ASSERT(!_buf || _bucket_size * i < _buf_size);
    return reinterpret_cast<score_t*>(_buf.get() + _bucket_size * i);
  }

  score_t* data() noexcept { return reinterpret_cast<score_t*>(_buf.get()); }

  size_t size() const noexcept { return _buf_size; }

  size_t bucket_size() const noexcept { return _bucket_size; }

 private:
  size_t _bucket_size;
  size_t _buf_size;
  std::unique_ptr<byte_type[]> _buf;
};

struct EmptyScoreBuffer {
  explicit EmptyScoreBuffer(size_t, size_t) noexcept {}

  score_t* data() noexcept { return nullptr; }
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
    // FIXME(gnus): remove const cast
    auto* score = const_cast<irs::ScoreAttr*>(it.score);
    SDB_ASSERT(score);  // ensured by ScoreAdapter
    if (score->IsDefault()) {
      ++scores.unscored;
      continue;
    }
    scores.scores.emplace_back(score);
    const auto tail = score->max.tail;
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

// Wrapper around regular DocIterator to conform compound_doc_iterator API
template<typename DocIteratorImpl,
         typename Adapter = ScoreAdapter<DocIteratorImpl>>
class UnaryDisjunction : public CompoundDocIterator<Adapter> {
 public:
  using doc_iterator_t = Adapter;

  UnaryDisjunction(doc_iterator_t&& it) : _it(std::move(it)) {}

  Attribute* GetMutable(TypeInfo::type_id type) noexcept final {
    return _it->GetMutable(type);
  }

  doc_id_t value() const noexcept final { return _it.doc->value; }

  doc_id_t advance() final { return _it->advance(); }

  doc_id_t seek(doc_id_t target) final { return _it->seek(target); }

  uint32_t count() final { return _it->count(); }

  void visit(void* ctx, IteratorVisitor<Adapter> visitor) final {
    SDB_ASSERT(ctx);
    SDB_ASSERT(visitor);
    visitor(ctx, _it);
  }

 private:
  doc_iterator_t _it;
};

// Disjunction optimized for two iterators.
template<typename DocIteratorImpl, typename Merger,
         typename Adapter = ScoreAdapter<DocIteratorImpl>>
class BasicDisjunction : public CompoundDocIterator<Adapter>,
                         private Merger,
                         private ScoreCtx {
 public:
  using adapter = Adapter;

  BasicDisjunction(adapter&& lhs, adapter&& rhs, Merger&& merger = Merger{})
    : BasicDisjunction{std::move(lhs), std::move(rhs), std::move(merger),
                       [this]() noexcept {
                         return CostAttr::extract(_lhs, 0) +
                                CostAttr::extract(_rhs, 0);
                       },
                       ResolveOverloadTag{}} {}

  BasicDisjunction(adapter&& lhs, adapter&& rhs, Merger&& merger,
                   CostAttr::Type est)
    : BasicDisjunction{std::move(lhs), std::move(rhs), std::move(merger), est,
                       ResolveOverloadTag{}} {}

  Attribute* GetMutable(TypeInfo::type_id type) noexcept final {
    return irs::GetMutable(_attrs, type);
  }

  doc_id_t value() const noexcept final {
    return std::get<DocAttr>(_attrs).value;
  }

  doc_id_t advance() final {
    next_iterator_impl(_lhs);
    next_iterator_impl(_rhs);

    auto& doc_value = std::get<DocAttr>(_attrs).value;
    return doc_value = std::min(_lhs.value(), _rhs.value());
  }

  doc_id_t seek(doc_id_t target) final {
    auto& doc_value = std::get<DocAttr>(_attrs).value;

    if (target <= doc_value) [[unlikely]] {
      return doc_value;
    }

    if (seek_iterator_impl(_lhs, target) || seek_iterator_impl(_rhs, target)) {
      return doc_value = target;
    }

    return doc_value = std::min(_lhs.value(), _rhs.value());
  }

  uint32_t count() final {
    uint32_t count = -1;
    auto lhs_value = _lhs.value();
    auto rhs_value = _rhs.value();
    while (!doc_limits::eof(lhs_value) || !doc_limits::eof(rhs_value)) {
      if (lhs_value < rhs_value) {
        lhs_value = _lhs->advance();
      } else if (rhs_value < lhs_value) {
        rhs_value = _rhs->advance();
      } else {
        lhs_value = _lhs->advance();
        rhs_value = _rhs->advance();
      }
      ++count;
    }
    if (count == -1) {
      return 0;
    }
    return count;
  }

  void visit(void* ctx, IteratorVisitor<Adapter> visitor) final {
    SDB_ASSERT(ctx);
    SDB_ASSERT(visitor);

    auto& doc_value = std::get<DocAttr>(_attrs).value;
    // assume that seek or next has been called
    SDB_ASSERT(_lhs.doc->value >= doc_value);

    if (_lhs.value() == doc_value && !visitor(ctx, _lhs)) {
      return;
    }

    seek_iterator_impl(_rhs, doc_value);
    if (_rhs.value() == doc_value) {
      visitor(ctx, _rhs);
    }
  }

 private:
  struct ResolveOverloadTag {};

  template<typename Estimation>
  BasicDisjunction(adapter&& lhs, adapter&& rhs, Merger&& merger,
                   Estimation&& estimation, ResolveOverloadTag)
    : Merger{std::move(merger)}, _lhs(std::move(lhs)), _rhs(std::move(rhs)) {
    std::get<CostAttr>(_attrs).reset(std::forward<Estimation>(estimation));

    if constexpr (kHasScore<Merger>) {
      prepare_score(false, false);
    }
  }

  void prepare_score(bool /*wand*/, bool /*strict*/) {
    SDB_ASSERT(Merger::size());
    SDB_ASSERT(_lhs.score && _rhs.score);  // must be ensure by the adapter

    auto& score = std::get<irs::ScoreAttr>(_attrs);

    const bool lhs_score_empty = _lhs.score->IsDefault();
    const bool rhs_score_empty = _rhs.score->IsDefault();

    if (!lhs_score_empty && !rhs_score_empty) {
      // both sub-iterators have score
      score.Reset(*this, [](ScoreCtx* ctx, score_t* res) noexcept {
        auto& self = *static_cast<BasicDisjunction*>(ctx);
        auto& merger = static_cast<Merger&>(self);
        self.score_iterator_impl(self._lhs, res);
        self.score_iterator_impl(self._rhs, merger.temp());
        merger(res, merger.temp());
      });
    } else if (!lhs_score_empty) {
      // only left sub-iterator has score
      score.Reset(*this, [](ScoreCtx* ctx, score_t* res) noexcept {
        auto& self = *static_cast<BasicDisjunction*>(ctx);
        return self.score_iterator_impl(self._lhs, res);
      });
    } else if (!rhs_score_empty) {
      // only right sub-iterator has score
      score.Reset(*this, [](ScoreCtx* ctx, score_t* res) noexcept {
        auto& self = *static_cast<BasicDisjunction*>(ctx);
        return self.score_iterator_impl(self._rhs, res);
      });
    } else {
      SDB_ASSERT(score.IsDefault());
      score = ScoreFunction::Default(Merger::size());
    }
  }

  bool seek_iterator_impl(adapter& it, doc_id_t target) {
    return it.value() < target && target == it->seek(target);
  }

  void next_iterator_impl(adapter& it) {
    auto& doc_value = std::get<DocAttr>(_attrs).value;
    const auto value = it.value();

    if (doc_value == value) {
      it->advance();
    } else if (value < doc_value) {
      it->seek(doc_value + doc_id_t(!doc_limits::eof(doc_value)));
    }
  }

  void score_iterator_impl(adapter& it, score_t* res) {
    auto& doc_value = std::get<DocAttr>(_attrs).value;
    auto value = it.value();

    if (value < doc_value) {
      value = it->seek(doc_value);
    }

    if (value == doc_value) {
      (*it.score)(res);
    } else {
      std::memset(res, 0, Merger::byte_size());
    }
  }

  using Attributes = std::tuple<DocAttr, ScoreAttr, CostAttr>;

  mutable adapter _lhs;
  mutable adapter _rhs;
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
template<typename DocIteratorImpl, typename Merger,
         typename Adapter = ScoreAdapter<DocIteratorImpl>>
class SmallDisjunction : public CompoundDocIterator<Adapter>,
                         private Merger,
                         private ScoreCtx {
 public:
  using adapter = Adapter;
  using DocIterators = std::vector<adapter>;

  SmallDisjunction(DocIterators&& itrs, Merger&& merger, CostAttr::Type est)
    : SmallDisjunction{std::move(itrs), std::move(merger), est,
                       ResolveOverloadTag()} {}

  explicit SmallDisjunction(DocIterators&& itrs, Merger&& merger = Merger{})
    : SmallDisjunction{std::move(itrs), std::move(merger),
                       [this]() noexcept {
                         return std::accumulate(
                           _begin, _end, CostAttr::Type(0),
                           [](CostAttr::Type lhs, const adapter& rhs) noexcept {
                             return lhs + CostAttr::extract(rhs, 0);
                           });
                       },
                       ResolveOverloadTag()} {}

  Attribute* GetMutable(TypeInfo::type_id type) noexcept final {
    return irs::GetMutable(_attrs, type);
  }

  doc_id_t value() const noexcept final {
    return std::get<DocAttr>(_attrs).value;
  }

  bool next_iterator_impl(adapter& it) {
    auto& doc_value = std::get<DocAttr>(_attrs).value;
    const auto value = it.value();

    if (value == doc_value) {
      return it->next();
    } else if (value < doc_value) {
      return !doc_limits::eof(it->seek(doc_value + 1));
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
        const auto value = it->seek(target);

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
    for (auto begin = _begin; begin != _end; ++begin) {
      auto& it = *begin;
      if (it->value() == doc_value && !visitor(ctx, it)) {
        return;
      }
    }
  }

 private:
  struct ResolveOverloadTag {};

  template<typename Estimation>
  SmallDisjunction(DocIterators&& itrs, Merger&& merger,
                   Estimation&& estimation, ResolveOverloadTag)
    : Merger{std::move(merger)},
      _itrs(itrs.size()),
      _scored_begin(_itrs.begin()),
      _begin(_scored_begin),
      _end(_itrs.end()) {
    std::get<CostAttr>(_attrs).reset(std::forward<Estimation>(estimation));

    if (_itrs.empty()) {
      std::get<DocAttr>(_attrs).value = doc_limits::eof();
    }

    auto rbegin = _itrs.rbegin();
    for (auto& it : itrs) {
      if (it.score->IsDefault()) {
        *_scored_begin = std::move(it);
        ++_scored_begin;
      } else {
        *rbegin = std::move(it);
        ++rbegin;
      }
    }

    if constexpr (kHasScore<Merger>) {
      prepare_score();
    }
  }

  void prepare_score() {
    SDB_ASSERT(Merger::size());

    auto& score = std::get<irs::ScoreAttr>(_attrs);

    // prepare score
    if (_scored_begin != _end) {
      score.Reset(*this, [](irs::ScoreCtx* ctx, score_t* res) noexcept {
        auto& self = *static_cast<SmallDisjunction*>(ctx);
        auto& merger = static_cast<Merger&>(self);
        const auto doc = std::get<DocAttr>(self._attrs).value;

        std::memset(res, 0, merger.byte_size());
        for (auto begin = self._scored_begin, end = self._end; begin != end;
             ++begin) {
          auto value = begin->value();

          if (value < doc) {
            value = (*begin)->seek(doc);
          }

          if (value == doc) {
            (*begin->score)(merger.temp());
            merger(res, merger.temp());
          }
        }
      });
    } else {
      SDB_ASSERT(score.IsDefault());
      score = ScoreFunction::Default(Merger::size());
    }
  }

  bool remove_iterator(typename DocIterators::iterator it) {
    if (it->score->IsDefault()) {
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
      if (it.value() < doc_value && doc_limits::eof(it->seek(doc_value))) {
        [[maybe_unused]] auto r = remove_iterator(begin);
        SDB_ASSERT(r);
      }
    }
    _last_hitched_doc = doc_value;
  }

  using Attributes = std::tuple<DocAttr, ScoreAttr, CostAttr>;
  using Iterator = typename DocIterators::iterator;

  doc_id_t _last_hitched_doc{doc_limits::invalid()};
  DocIterators _itrs;
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
template<typename DocIteratorImpl, typename Merger,
         typename Adapter = ScoreAdapter<DocIteratorImpl>,
         bool EnableUnary = false>
class Disjunction : public CompoundDocIterator<Adapter>,
                    private Merger,
                    private ScoreCtx {
 public:
  using adapter = Adapter;
  using DocIterators = std::vector<adapter>;
  using heap_container = std::vector<size_t>;
  using heap_iterator = heap_container::iterator;

  static constexpr bool kEnableUnary = EnableUnary;
  static constexpr size_t kSmallDisjunctionUpperBound = 5;

  Disjunction(DocIterators&& itrs, Merger&& merger, CostAttr::Type est)
    : Disjunction{std::move(itrs), std::move(merger), est,
                  ResolveOverloadTag()} {}

  explicit Disjunction(DocIterators&& itrs, Merger&& merger = Merger{})
    : Disjunction{std::move(itrs), std::move(merger),
                  [this]() noexcept {
                    return absl::c_accumulate(
                      _itrs, CostAttr::Type(0),
                      [](CostAttr::Type lhs, const adapter& rhs) noexcept {
                        return lhs + CostAttr::extract(rhs, 0);
                      });
                  },
                  ResolveOverloadTag{}} {}

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
                            ? lead()->advance()
                            : lead()->seek(doc_value + 1);
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
      const auto value = lead()->seek(target);

      if (doc_limits::eof(value) && !remove_lead()) {
        return doc_value = doc_limits::eof();
      } else if (value != target) {
        refresh_lead();
      }
    }

    return doc_value = lead().value();
  }

  uint32_t count() final { return DocIterator::Count(*this); }

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
  Disjunction(DocIterators&& itrs, Merger&& merger, Estimation&& estimation,
              ResolveOverloadTag)
    : Merger{std::move(merger)}, _itrs{std::move(itrs)} {
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

    if constexpr (kHasScore<Merger>) {
      prepare_score();
    }
  }

  void prepare_score() {
    SDB_ASSERT(Merger::size());

    auto& score = std::get<irs::ScoreAttr>(_attrs);

    score.Reset(*this, [](ScoreCtx* ctx, score_t* res) noexcept {
      auto& self = *static_cast<Disjunction*>(ctx);
      SDB_ASSERT(!self._heap.empty());

      const auto its = self.hitch_all_iterators();

      if (auto& score = *self.lead().score; !score.IsDefault()) {
        score(res);
      } else {
        std::memset(res, 0, self.byte_size());
      }
      if (const auto doc = std::get<DocAttr>(self._attrs).value;
          self.top().value() == doc) {
        irstd::heap::ForEachIf(
          its.first, its.second,
          [&self, doc](const size_t it) noexcept {
            SDB_ASSERT(it < self._itrs.size());
            return self._itrs[it].value() == doc;
          },
          [&self, res](size_t it) {
            SDB_ASSERT(it < self._itrs.size());
            if (auto& score = *self._itrs[it].score; !score.IsDefault()) {
              auto& merger = static_cast<Merger&>(self);
              score(merger.temp());
              merger(res, merger.temp());
            }
          });
      }
    });
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

  adapter& lead() noexcept {
    SDB_ASSERT(!_heap.empty());
    SDB_ASSERT(_heap.back() < _itrs.size());
    return _itrs[_heap.back()];
  }

  adapter& top() noexcept {
    SDB_ASSERT(!_heap.empty());
    SDB_ASSERT(_heap.front() < _itrs.size());
    return _itrs[_heap.front()];
  }

  std::pair<heap_iterator, heap_iterator> hitch_all_iterators() {
    // hitch all iterators in head to the lead (current doc_)
    SDB_ASSERT(!_heap.empty());
    auto begin = _heap.begin(), end = _heap.end() - 1;

    auto& doc_value = std::get<DocAttr>(_attrs).value;
    while (begin != end && top().value() < doc_value) {
      const auto value = top()->seek(doc_value);

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

  DocIterators _itrs;
  heap_container _heap;
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
template<typename DocIteratorImpl, typename Merger, typename Traits,
         typename Adapter = ScoreAdapter<DocIteratorImpl>>
class BlockDisjunction : public DocIterator, private Merger, private ScoreCtx {
 public:
  using traits_type = Traits;
  using adapter = Adapter;
  using DocIterators = std::vector<adapter>;

  BlockDisjunction(DocIterators&& itrs, Merger&& merger, CostAttr::Type est)
    : BlockDisjunction{std::move(itrs), 1, std::move(merger),
                       detail::SubScoresCtx{}, est} {}

  BlockDisjunction(DocIterators&& itrs, size_t min_match_count, Merger&& merger,
                   detail::SubScoresCtx&& scores, CostAttr::Type est)
    : BlockDisjunction{std::move(itrs),   min_match_count,
                       std::move(merger), est,
                       std::move(scores), ResolveOverloadTag()} {}

  explicit BlockDisjunction(DocIterators&& itrs, Merger&& merger = Merger{})
    : BlockDisjunction{std::move(itrs), 1, std::move(merger)} {}

  BlockDisjunction(DocIterators&& itrs, size_t min_match_count,
                   Merger&& merger = Merger{},
                   detail::SubScoresCtx&& scores = {})
    : BlockDisjunction{std::move(itrs),
                       min_match_count,
                       std::move(merger),
                       [this]() noexcept {
                         return absl::c_accumulate(
                           _itrs, CostAttr::Type{0},
                           [](CostAttr::Type lhs, const adapter& rhs) noexcept {
                             return lhs + CostAttr::extract(rhs, 0);
                           });
                       },
                       std::move(scores),
                       ResolveOverloadTag()} {}

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
        if constexpr (traits_type::kMinMatch || kHasScore<Merger>) {
          _buf_offset += BitsRequired<uint64_t>();
        }
      }

      const size_t offset = std::countr_zero(_cur);
      irs::UnsetBit(_cur, offset);

      [[maybe_unused]] const size_t buf_offset = _buf_offset + offset;

      if constexpr (traits_type::kMinMatch) {
        _match_count = _match_buf.match_count(buf_offset);

        if (_match_count < _match_buf.min_match_count()) {
          continue;
        }
      }

      doc_value = _doc_base + doc_id_t(offset);
      if constexpr (kHasScore<Merger>) {
        _score_value = _score_buf.get(buf_offset);
      }

      return doc_value;
    } while (traits_type::kMinMatch);
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

    if constexpr (traits_type::kMinMatch) {
      _match_count = 0;
    }

    VisitAndPurge([&](auto& it) {
      const auto value = it->seek(target);

      if (doc_limits::eof(value)) {
        // exhausted
        return false;
      }

      if (value < doc_value) {
        doc_value = value;
        if constexpr (traits_type::kMinMatch) {
          _match_count = 1;
        }
      } else if constexpr (traits_type::kMinMatch) {
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

    if constexpr (traits_type::kSeekReadahead) {
      _min = doc_value;
      return advance();
    } else {
      _min = doc_value + 1;
      _buf_offset = 0;

      if constexpr (traits_type::kMinMatch) {
        if (_match_count < _match_buf.min_match_count()) {
          return advance();
        }
      }

      if constexpr (kHasScore<Merger>) {
        std::memset(_score_buf.data(), 0, _score_buf.bucket_size());
        for (auto& it : _itrs) {
          SDB_ASSERT(it.score);
          if (!it.score->IsDefault() && doc_value == it->value()) {
            auto& merger = static_cast<Merger&>(*this);
            (*it.score)(merger.temp());
            merger(_score_buf.data(), merger.temp());
          }
        }

        _score_value = _score_buf.data();
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
      if constexpr (traits_type::kMinMatch) {
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

 private:
  static constexpr doc_id_t kBlockSize = BitsRequired<uint64_t>();

  static constexpr doc_id_t kNumBlocks =
    static_cast<doc_id_t>(std::max(size_t(1), traits_type::kNumBlocks));

  static constexpr doc_id_t kWindow = kBlockSize * kNumBlocks;

  static_assert(kBlockSize * size_t(kNumBlocks) <
                std::numeric_limits<doc_id_t>::max());

  // FIXME(gnusi): stack based score_buffer for constant cases
  using ScoreBufferType =
    std::conditional_t<kHasScore<Merger>, detail::ScoreBuffer,
                       detail::EmptyScoreBuffer>;

  using MinMatchBufferType =
    utils::Need<traits_type::kMinMatch, detail::MinMatchBuffer<kWindow>>;

  using Attributes = std::tuple<DocAttr, ScoreAttr, CostAttr>;

  struct ResolveOverloadTag {};

  template<typename Estimation>
  BlockDisjunction(DocIterators&& itrs, size_t min_match_count, Merger&& merger,
                   Estimation&& estimation, detail::SubScoresCtx&& scores,
                   ResolveOverloadTag)
    : Merger{std::move(merger)},
      _itrs(std::move(itrs)),
      _match_count(_itrs.empty()
                     ? size_t(0)
                     : static_cast<size_t>(!traits_type::kMinMatch)),
      _score_buf(Merger::size(), kWindow),
      _match_buf(min_match_count),
      _scores(std::move(scores)) {
    std::get<CostAttr>(_attrs).reset(std::forward<Estimation>(estimation));

    if (_itrs.empty()) {
      std::get<DocAttr>(_attrs).value = doc_limits::eof();
    }

    if constexpr (kHasScore<Merger>) {
      SDB_ASSERT(Merger::size());
      auto& score = std::get<irs::ScoreAttr>(_attrs);
      auto min = ScoreFunction::DefaultMin;
      if (!_scores.scores.empty()) {
        score.max.leaf = score.max.tail = _scores.sum_score;
        min = [](ScoreCtx* ctx, score_t arg) noexcept {
          auto& self = static_cast<BlockDisjunction&>(*ctx);
          if (self._scores.Size() != self._itrs.size()) [[unlikely]] {
            self._scores.Clear();
            detail::MakeSubScores(self._itrs, self._scores);
            auto& score = std::get<irs::ScoreAttr>(self._attrs);
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
            if constexpr (traits_type::kMinMatch) {
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
          if constexpr (traits_type::kMinMatch) {
            self._match_buf.min_match_count(min_match);
          }
        };
      }

      score.Reset(
        *this,
        [](ScoreCtx* ctx, score_t* res) noexcept {
          auto& self = static_cast<BlockDisjunction&>(*ctx);
          std::memcpy(res, self._score_value,
                      static_cast<Merger&>(self).byte_size());
        },
        min);
    }

    if (traits_type::kMinMatch && min_match_count > 1) {
      // sort subnodes in ascending order by their cost
      // FIXME(gnusi) don't use extract
      absl::c_sort(_itrs, [](const adapter& lhs, const adapter& rhs) noexcept {
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
        // scores_.unscored -= begin->score->IsDefault();
        irstd::SwapRemove(_itrs, begin);
        --end;

        if constexpr (traits_type::kMinMatchEarlyPruning) {
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

    if constexpr (traits_type::kMinMatch &&
                  !traits_type::kMinMatchEarlyPruning) {
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
    if constexpr (kHasScore<Merger>) {
      _score_value = _score_buf.data();
      std::memset(_score_buf.data(), 0, _score_buf.size());
    }
    if constexpr (traits_type::kMinMatch) {
      _match_buf.clear();
    }
  }

  bool Refill() {
    if (_itrs.empty()) {
      return false;
    }

    if constexpr (!traits_type::kMinMatch) {
      Reset();
    }

    bool empty = true;

    do {
      if constexpr (traits_type::kMinMatch) {
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
        // if constexpr (traits_type::kMinMatch) {
        //  if (empty && (&it + (match_buf_.min_match_count() -
        //  match_buf_.max_match_count()) < (itrs_.data() + itrs_.size()))) {
        //    // skip current block
        //    return true;
        //  }
        //}

        if constexpr (kHasScore<Merger>) {
          SDB_ASSERT(Merger::size());
          if (!it.score->IsDefault()) {
            return this->Refill<true>(it, empty);
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
    if constexpr (traits_type::kMinMatch || kHasScore<Merger>) {
      _buf_offset = 0;
    }
    while (!_cur) {
      _cur = *_begin++;
      _doc_base += BitsRequired<uint64_t>();
      if constexpr (traits_type::kMinMatch || kHasScore<Merger>) {
        _buf_offset += BitsRequired<uint64_t>();
      }
    }
    SDB_ASSERT(_cur);

    return true;
  }

  template<bool Score>
  bool Refill(adapter& it, bool& empty) {
    SDB_ASSERT(it.doc);
    const auto* doc = &it.doc->value;

    // disjunction is 1 step next behind, that may happen:
    // - before the very first next()
    // - after seek() in case of 'kSeekReadahead == false'
    if ((*doc < _doc_base && !it->next()) || doc_limits::eof(*doc)) {
      // exhausted
      return false;
    }

    for (;;) {
      const auto value = *doc;

      if (value >= _max) {
        _min = std::min(value, _min);
        return true;
      }

      const size_t offset{value - _doc_base};

      irs::SetBit(_mask[offset / kBlockSize], offset % kBlockSize);

      if constexpr (Score) {
        SDB_ASSERT(it.score);
        auto& merger = static_cast<Merger&>(*this);
        (*it.score)(merger.temp());
        merger(_score_buf.get(offset), merger.temp());
      }

      if constexpr (traits_type::kMinMatch) {
        empty &= _match_buf.inc(offset);
      } else {
        empty = false;
      }

      if (!it->next()) {
        // exhausted
        return false;
      }
    }
  }

  uint64_t _mask[kNumBlocks]{};
  DocIterators _itrs;
  uint64_t* _begin{std::end(_mask)};
  uint64_t _cur{};
  doc_id_t _doc_base{doc_limits::invalid()};
  doc_id_t _min{doc_limits::min()};      // base doc id for the next mask
  doc_id_t _max{doc_limits::invalid()};  // max doc id in the current mask
  Attributes _attrs;
  size_t _match_count;
  size_t _buf_offset{};  // offset within a buffer
  [[no_unique_address]] ScoreBufferType _score_buf;
  [[no_unique_address]] MinMatchBufferType _match_buf;
  // TODO(mbkkt) We don't need scores_ for not wand,
  // but we don't want to generate more functions, than necessary
  detail::SubScoresCtx _scores;
  const score_t* _score_value{_score_buf.data()};
};

template<typename DocIteratorImpl, typename Merger,
         typename Adapter = ScoreAdapter<DocIteratorImpl>>
using DisjunctionIterator =
  BlockDisjunction<DocIteratorImpl, Merger,
                   BlockDisjunctionTraits<MatchType::Match, false>, Adapter>;

template<typename DocIteratorImpl, typename Merger,
         typename Adapter = ScoreAdapter<DocIteratorImpl>>
using MinMatchIterator =
  BlockDisjunction<DocIteratorImpl, Merger,
                   BlockDisjunctionTraits<MatchType::MinMatch, false>, Adapter>;

template<typename T>
struct RebindIterator;

template<typename DocIteratorImpl, typename Merger, typename Adapter>
struct RebindIterator<Disjunction<DocIteratorImpl, Merger, Adapter>> {
  using Unary = UnaryDisjunction<DocIteratorImpl, Adapter>;
  using Basic = BasicDisjunction<DocIteratorImpl, Merger, Adapter>;
  using Small = SmallDisjunction<DocIteratorImpl, Merger, Adapter>;
  using Wand = void;
};

template<typename DocIteratorImpl, typename Merger, typename Adapter>
struct RebindIterator<DisjunctionIterator<DocIteratorImpl, Merger, Adapter>> {
  using Unary = void;  // block disjunction doesn't support visitor
  using Basic = BasicDisjunction<DocIteratorImpl, Merger, Adapter>;
  using Small = void;  // block disjunction always faster than small
  using Wand = DisjunctionIterator<DocIteratorImpl, Merger, Adapter>;
};

template<typename DocIteratorImpl, typename Merger, typename Adapter>
struct RebindIterator<MinMatchIterator<DocIteratorImpl, Merger, Adapter>> {
  using Disjunction = DisjunctionIterator<DocIteratorImpl, Merger, Adapter>;
  using Wand = MinMatchIterator<DocIteratorImpl, Merger, Adapter>;
};

// Returns disjunction iterator created from the specified sub iterators
template<typename Disjunction, typename Merger, typename... Args>
DocIterator::ptr MakeDisjunction(WandContext ctx,
                                 typename Disjunction::DocIterators&& itrs,
                                 Merger&& merger, Args&&... args) {
  const auto size = itrs.size();

  if (0 == size) {
    // Empty or unreachable search criteria
    return DocIterator::empty();
  }

  if (1 == size) {
    // Single sub-query
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
        std::forward<Merger>(merger), std::forward<Args>(args)...);
    }
  }

  using SmallDisjunction = typename RebindIterator<Disjunction>::Small;
  if constexpr (!std::is_void_v<SmallDisjunction>) {
    if (size <= Disjunction::kSmallDisjunctionUpperBound) {
      return memory::make_managed<SmallDisjunction>(
        std::move(itrs), std::forward<Merger>(merger),
        std::forward<Args>(args)...);
    }
  }

  using Wand = typename RebindIterator<Disjunction>::Wand;
  if constexpr (!std::is_void_v<Wand>) {
    if (ctx.Enabled()) {
      // TODO(mbkkt) root optimization
      // TODO(mbkkt) block wand/maxscore optimization
      detail::SubScoresCtx scores;
      if (detail::MakeSubScores(itrs, scores)) {
        return memory::make_managed<Wand>(
          std::move(itrs), size_t{1}, std::forward<Merger>(merger),
          std::move(scores), std::forward<Args>(args)...);
      }
    }
  }

  return memory::make_managed<Disjunction>(
    std::move(itrs), std::forward<Merger>(merger), std::forward<Args>(args)...);
}

// Returns weak conjunction iterator created from the specified sub iterators
template<typename WeakConjunction, typename Merger, typename... Args>
DocIterator::ptr MakeWeakDisjunction(
  WandContext ctx, typename WeakConjunction::DocIterators&& itrs,
  size_t min_match, Merger&& merger, Args&&... args) {
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

    return MakeDisjunction<Disjunction>(ctx, std::move(itrs),
                                        std::forward<Merger>(merger),
                                        std::forward<Args>(args)...);
  }

  if (min_match == size) {
    // Pure conjunction
    return MakeConjunction(ctx, std::forward<Merger>(merger), std::move(itrs));
  }

  if (ctx.Enabled()) {
    detail::SubScoresCtx scores;
    if (detail::MakeSubScores(itrs, scores)) {
      // TODO(mbkkt) root optimization
      // TODO(mbkkt) block wand/maxscore optimization
      using Wand = typename RebindIterator<WeakConjunction>::Wand;
      return memory::make_managed<Wand>(
        std::move(itrs), min_match, std::forward<Merger>(merger),
        std::move(scores), std::forward<Args>(args)...);
    }
  }

  return memory::make_managed<WeakConjunction>(
    std::move(itrs), min_match, std::forward<Merger>(merger),
    detail::SubScoresCtx{}, std::forward<Args>(args)...);
}

}  // namespace irs
