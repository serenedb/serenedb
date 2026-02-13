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

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <utility>
#include <vector>

#include "basics/empty.hpp"
#include "basics/std.hpp"
#include "conjunction.hpp"
#include "iresearch/index/iterators.hpp"
#include "iresearch/utils/attribute_provider.hpp"
#include "iresearch/utils/type_limits.hpp"

// Disjunction is template for Adapter instead of direct use of ScoreAdapter
// only because of variadic phrase
namespace irs {

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

 private:
  Adapter _it;
};

template<typename Adapter>
class BasicDisjunction : public CompoundDocIterator<Adapter> {
 public:
  using adapter = Adapter;
  static constexpr auto kMergeType = ScoreMergeType::Noop;
  static constexpr bool kHasScore = kMergeType != ScoreMergeType::Noop;

  BasicDisjunction(adapter&& lhs, adapter&& rhs)
    : BasicDisjunction{std::move(lhs), std::move(rhs),
                       [this] noexcept {
                         return CostAttr::extract(_itrs[0], 0) +
                                CostAttr::extract(_itrs[1], 0);
                       },
                       ResolveOverloadTag{}} {}

  BasicDisjunction(Adapter lhs, Adapter rhs, CostAttr::Type est)
    : BasicDisjunction{std::move(lhs), std::move(rhs), est,
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

 private:
  struct ResolveOverloadTag {};

  template<typename Estimation>
  BasicDisjunction(Adapter lhs, Adapter rhs, Estimation&& estimation,
                   ResolveOverloadTag)
    : _itrs{std::move(lhs), std::move(rhs)} {
    std::get<CostAttr>(_attrs).reset(std::forward<Estimation>(estimation));
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

  using Attributes = std::tuple<DocAttr, CostAttr>;

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
template<typename Adapter>
class SmallDisjunction : public CompoundDocIterator<Adapter> {
 public:
  using Adapters = std::vector<Adapter>;

  static constexpr auto kMergeType = ScoreMergeType::Noop;
  static constexpr bool kHasScore = kMergeType != ScoreMergeType::Noop;

  SmallDisjunction(Adapters&& itrs, CostAttr::Type est)
    : SmallDisjunction{std::move(itrs), est, ResolveOverloadTag{}} {}
  explicit SmallDisjunction(Adapters&& itrs)
    : SmallDisjunction{std::move(itrs),
                       [&] noexcept {
                         return std::accumulate(
                           _begin, _end, CostAttr::Type{0},
                           [](CostAttr::Type lhs, const Adapter& rhs) noexcept {
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

 private:
  struct ResolveOverloadTag {};

  template<typename Estimation>
  SmallDisjunction(Adapters&& itrs, Estimation&& estimation, ResolveOverloadTag)
    : _itrs(itrs.size()), _begin(_itrs.begin()), _end(_itrs.end()) {
    std::get<CostAttr>(_attrs).reset(std::forward<Estimation>(estimation));

    if (_itrs.empty()) {
      std::get<DocAttr>(_attrs).value = doc_limits::eof();
    }

    auto rbegin = _itrs.rbegin();
    for (auto& it : itrs) {
      *rbegin = std::move(it);
      ++rbegin;
    }
  }

  bool remove_iterator(typename Adapters::iterator it) {
    std::swap(*it, *_begin);
    ++_begin;

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

  using Attributes = std::tuple<DocAttr, CostAttr>;
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
template<typename Adapter>
class Disjunction : public CompoundDocIterator<Adapter> {
 public:
  using Adapters = std::vector<Adapter>;
  using Heap = std::vector<size_t>;
  using Iterator = Heap::iterator;

  static constexpr auto kMergeType = ScoreMergeType::Noop;
  static constexpr bool kHasScore = kMergeType != ScoreMergeType::Noop;
  static constexpr size_t kSmallDisjunctionUpperBound = 5;

  Disjunction(Adapters&& itrs, CostAttr::Type est)
    : Disjunction{std::move(itrs), est, ResolveOverloadTag()} {}

  explicit Disjunction(Adapters&& itrs)
    : Disjunction{std::move(itrs),
                  [&] noexcept {
                    return absl::c_accumulate(
                      _itrs, CostAttr::Type{0},
                      [](CostAttr::Type lhs, const Adapter& rhs) noexcept {
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

  using Attributes = std::tuple<DocAttr, CostAttr>;

  template<typename Estimation>
  Disjunction(Adapters&& itrs, Estimation&& estimation, ResolveOverloadTag)
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

template<typename T>
struct RebindIterator;

template<typename Adapter>
struct RebindIterator<Disjunction<Adapter>> {
  using Unary = UnaryDisjunction<Adapter>;
  using Basic = BasicDisjunction<Adapter>;
  using Small = SmallDisjunction<Adapter>;
  using Wand = void;
};

struct CostAdapter : ScoreAdapter {
  explicit CostAdapter(DocIterator::ptr it) noexcept
    : ScoreAdapter{std::move(it)} {
    // TODO(mbkkt) 0 instead of kMax?
    est = CostAttr::extract(*this, CostAttr::kMax);
  }

  CostAttr::Type est;
};

using CostAdapters = std::vector<CostAdapter>;

// Heapsort-based "weak and" iterator
// -----------------------------------------------------------------------------
//      [0] <-- begin
//      [1]      |
//      [2]      | head (min doc_id, cost heap)
//      [3]      |
//      [4] <-- lead_
// c ^  [5]      |
// o |  [6]      | lead (list of accepted iterators)
// s |  ...      |
// t |  [n] <-- end
// -----------------------------------------------------------------------------
class MinMatchDisjunction : public DocIterator {
 private:
  // Returns reference to the top of the head
  auto& Top() noexcept {
    SDB_ASSERT(!_heap.empty());
    SDB_ASSERT(_heap.front() < _itrs.size());
    return _itrs[_heap.front()];
  }

  // Returns the first iterator in the lead group
  auto Lead() noexcept {
    SDB_ASSERT(_lead <= _heap.size());
    return _heap.end() - _lead;
  }

 public:
  MinMatchDisjunction(CostAdapters&& itrs, size_t min_match_count)
    : _itrs{std::move(itrs)},
      _min_match_count{std::clamp(min_match_count, size_t{1}, _itrs.size())},
      _lead{_itrs.size()} {
    SDB_ASSERT(!_itrs.empty());
    SDB_ASSERT(_min_match_count >= 1 && _min_match_count <= _itrs.size());

    // sort subnodes in ascending order by their cost
    absl::c_sort(_itrs, [](const auto& lhs, const auto& rhs) noexcept {
      return lhs.est < rhs.est;
    });

    std::get<CostAttr>(_attrs).reset([this]() noexcept {
      return absl::c_accumulate(
        _itrs, CostAttr::Type{0},
        [](CostAttr::Type lhs, const auto& rhs) noexcept {
          return lhs + rhs.est;
        });
    });

    // prepare external heap
    _heap.resize(_itrs.size());
    absl::c_iota(_heap, size_t{0});
  }

  Attribute* GetMutable(TypeInfo::type_id id) noexcept final {
    return irs::GetMutable(_attrs, id);
  }

  doc_id_t value() const noexcept final {
    return std::get<DocAttr>(_attrs).value;
  }

  doc_id_t advance() final {
    auto& doc_value = std::get<DocAttr>(_attrs).value;

    if (doc_limits::eof(doc_value)) {
      return doc_value;
    }

    while (CheckSize()) {
      // start next iteration. execute next for all lead iterators
      // and move them to head
      if (!PopLead()) {
        return doc_value = doc_limits::eof();
      }

      // make step for all head iterators less or equal current doc (doc_)
      while (Top().value() <= doc_value) {
        const auto target = Top().value() == doc_value
                              ? Top().advance()
                              : Top().seek(doc_value + 1);
        const bool exhausted = doc_limits::eof(target);

        if (exhausted && !RemoveTop()) {
          return doc_value = doc_limits::eof();
        }
        RefreshTop();
      }

      // count equal iterators
      const auto top = Top().value();

      do {
        AddLead();
        if (_lead >= _min_match_count) {
          return doc_value = top;
        }
      } while (top == Top().value());
    }

    return doc_value = doc_limits::eof();
  }

  doc_id_t seek(doc_id_t target) final {
    auto& doc_value = std::get<DocAttr>(_attrs).value;

    if (target <= doc_value) [[unlikely]] {
      return doc_value;
    }

    // execute seek for all lead iterators and
    // move one to head if it doesn't hit the target
    for (auto it = Lead(), end = _heap.end(); it != end;) {
      SDB_ASSERT(*it < _itrs.size());
      const auto doc = _itrs[*it].seek(target);

      if (doc_limits::eof(doc)) {
        --_lead;

        // iterator exhausted
        if (!RemoveLead(it)) {
          return doc_value = doc_limits::eof();
        }

        it = Lead();
        end = _heap.end();
      } else {
        if (doc != target) {
          // move back to head
          PushHead(it);
          --_lead;
        }
        ++it;
      }
    }

    // check if we still satisfy search criteria
    if (_lead >= _min_match_count) {
      return doc_value = target;
    }

    // main search loop
    for (;; target = Top().value()) {
      while (Top().value() <= target) {
        const auto doc = Top().seek(target);

        if (doc_limits::eof(doc)) {
          // iterator exhausted
          if (!RemoveTop()) {
            return doc_value = doc_limits::eof();
          }
        } else if (doc == target) {
          // valid iterator, doc == target
          AddLead();
          if (_lead >= _min_match_count) {
            return doc_value = target;
          }
        } else {
          // invalid iterator, doc != target
          RefreshTop();
        }
      }

      // can't find enough iterators equal to target here.
      // start next iteration. execute next for all lead iterators
      // and move them to head
      if (!PopLead()) {
        return doc_value = doc_limits::eof();
      }
    }
  }

  uint32_t count() final { return Count(*this); }

  // Calculates total count of matched iterators. This value could be
  // greater than required min_match. All matched iterators points
  // to current matched document after this call.
  // Returns total matched iterators count.
  size_t MatchCount() {
    PushValidToLead();
    return _lead;
  }

 private:
  using Attributes = std::tuple<DocAttr, CostAttr>;

  // Push all valid iterators to lead.
  void PushValidToLead() {
    auto& doc_value = std::get<DocAttr>(_attrs).value;

    for (auto lead = Lead(), begin = _heap.begin();
         lead != begin && Top().value() <= doc_value;) {
      // hitch head
      if (Top().value() == doc_value) {
        // got hit here
        AddLead();
        --lead;
      } else {
        if (doc_limits::eof(Top().seek(doc_value))) {
          // iterator exhausted
          RemoveTop();
          lead = Lead();
        } else {
          RefreshTop();
        }
      }
    }
  }

  template<typename Iterator>
  void Push(Iterator begin, Iterator end) noexcept {
    std::push_heap(begin, end, [&](const auto lhs, const auto rhs) noexcept {
      SDB_ASSERT(lhs < _itrs.size());
      SDB_ASSERT(rhs < _itrs.size());
      const auto& lhs_it = _itrs[lhs];
      const auto& rhs_it = _itrs[rhs];
      const auto lhs_doc = lhs_it.value();
      const auto rhs_doc = rhs_it.value();
      return lhs_doc > rhs_doc ||
             (lhs_doc == rhs_doc && lhs_it.est > rhs_it.est);
    });
  }

  template<typename Iterator>
  void Pop(Iterator begin, Iterator end) noexcept {
    std::pop_heap(begin, end, [&](const auto lhs, const auto rhs) noexcept {
      SDB_ASSERT(lhs < _itrs.size());
      SDB_ASSERT(rhs < _itrs.size());
      const auto& lhs_it = _itrs[lhs];
      const auto& rhs_it = _itrs[rhs];
      const auto lhs_doc = lhs_it.value();
      const auto rhs_doc = rhs_it.value();
      return lhs_doc > rhs_doc ||
             (lhs_doc == rhs_doc && lhs_it.est > rhs_it.est);
    });
  }

  // Performs a step for each iterator in lead group and pushes it to the head.
  // Returns true - if the min_match_count_ condition still can be satisfied,
  // false - otherwise
  bool PopLead() {
    for (auto it = Lead(), end = _heap.end(); it != end;) {
      SDB_ASSERT(*it < _itrs.size());
      if (doc_limits::eof(_itrs[*it].advance())) {
        --_lead;

        // remove iterator
        if (!RemoveLead(it)) {
          return false;
        }

        it = Lead();
        end = _heap.end();
      } else {
        // push back to head
        Push(_heap.begin(), ++it);
        --_lead;
      }
    }

    return true;
  }

  // Removes an iterator from the specified position in lead group
  // without moving iterators after the specified iterator.
  // Returns true - if the min_match_count_ condition still can be satisfied,
  // false - otherwise.
  template<typename Iterator>
  bool RemoveLead(Iterator it) noexcept {
    if (&*it != &_heap.back()) {
      std::swap(*it, _heap.back());
    }
    _heap.pop_back();
    return CheckSize();
  }

  // Removes iterator from the top of the head without moving
  // iterators after the specified iterator.
  // Returns true - if the min_match_count_ condition still can be satisfied,
  // false - otherwise.
  bool RemoveTop() noexcept {
    auto lead = Lead();
    Pop(_heap.begin(), lead);
    return RemoveLead(--lead);
  }

  // Refresh the value of the top of the head.
  void RefreshTop() noexcept {
    auto lead = Lead();
    Pop(_heap.begin(), lead);
    Push(_heap.begin(), lead);
  }

  // Push the specified iterator from lead group to the head
  // without movinh iterators after the specified iterator.
  template<typename Iterator>
  void PushHead(Iterator it) noexcept {
    Iterator lead = Lead();
    if (it != lead) {
      std::swap(*it, *lead);
    }
    ++lead;
    Push(_heap.begin(), lead);
  }

  // Returns true - if the min_match_count_ condition still can be satisfied,
  // false - otherwise.
  bool CheckSize() const noexcept { return _heap.size() >= _min_match_count; }

  // Adds iterator to the lead group
  void AddLead() {
    Pop(_heap.begin(), Lead());
    ++_lead;
  }

  CostAdapters _itrs;  // sub iterators
  std::vector<size_t> _heap;
  size_t _min_match_count;  // minimum number of hits
  size_t _lead;             // number of iterators in lead group
  Attributes _attrs;
};

}  // namespace irs
