////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2017 ArangoDB GmbH, Cologne, Germany
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
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
///
/// @author Andrey Abramov
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include "disjunction.hpp"

namespace irs {

template<typename DocIteratorImpl = DocIterator::ptr>
struct CostAdapter : ScoreAdapter<DocIteratorImpl> {
  explicit CostAdapter(DocIteratorImpl&& it) noexcept
    : ScoreAdapter<DocIteratorImpl>{std::move(it)} {
    // TODO(mbkkt) 0 instead of kMax?
    est = CostAttr::extract(*this->it, CostAttr::kMax);
  }

  CostAdapter(CostAdapter&&) noexcept = default;
  CostAdapter& operator=(CostAdapter&&) noexcept = default;

  CostAttr::Type est{};
};

using CostAdapters = std::vector<CostAdapter<>>;

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
template<typename Merger>
class MinMatchDisjunction : public DocIterator,
                            protected Merger,
                            protected ScoreCtx {
 public:
  MinMatchDisjunction(CostAdapters&& itrs, size_t min_match_count,
                      Merger&& merger = {})
    : Merger{std::move(merger)},
      _itrs{std::move(itrs)},
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

    if constexpr (kHasScore<Merger>) {
      PrepareScore();
    }
  }

  Attribute* GetMutable(TypeInfo::type_id id) noexcept final {
    return irs::GetMutable(_attrs, id);
  }

  doc_id_t value() const final { return std::get<DocAttr>(_attrs).value; }

  bool next() final {
    auto& doc_value = std::get<DocAttr>(_attrs).value;

    if (doc_limits::eof(doc_value)) {
      return false;
    }

    while (CheckSize()) {
      // start next iteration. execute next for all lead iterators
      // and move them to head
      if (!PopLead()) {
        doc_value = doc_limits::eof();
        return false;
      }

      // make step for all head iterators less or equal current doc (doc_)
      while (Top().value() <= doc_value) {
        const bool exhausted = Top().value() == doc_value
                                 ? !Top()->next()
                                 : doc_limits::eof(Top()->seek(doc_value + 1));

        if (exhausted && !RemoveTop()) {
          doc_value = doc_limits::eof();
          return false;
        }
        RefreshTop();
      }

      // count equal iterators
      const auto top = Top().value();

      do {
        AddLead();
        if (_lead >= _min_match_count) {
          return !doc_limits::eof(doc_value = top);
        }
      } while (top == Top().value());
    }

    doc_value = doc_limits::eof();
    return false;
  }

  doc_id_t seek(doc_id_t target) final {
    auto& doc_value = std::get<DocAttr>(_attrs).value;

    if (target <= doc_value) {
      return doc_value;
    }

    // execute seek for all lead iterators and
    // move one to head if it doesn't hit the target
    for (auto it = Lead(), end = _heap.end(); it != end;) {
      SDB_ASSERT(*it < _itrs.size());
      const auto doc = _itrs[*it]->seek(target);

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
        const auto doc = Top()->seek(target);

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

  // Calculates total count of matched iterators. This value could be
  // greater than required min_match. All matched iterators points
  // to current matched document after this call.
  // Returns total matched iterators count.
  size_t MatchCount() {
    PushValidToLead();
    return _lead;
  }

 private:
  using Attributes = std::tuple<DocAttr, CostAttr, ScoreAttr>;

  void PrepareScore() {
    SDB_ASSERT(Merger::size());

    auto& score = std::get<irs::ScoreAttr>(_attrs);

    score.Reset(*this, [](ScoreCtx* ctx, score_t* res) noexcept {
      auto& self = static_cast<MinMatchDisjunction&>(*ctx);
      SDB_ASSERT(!self._heap.empty());

      self.PushValidToLead();

      // score lead iterators
      std::memset(res, 0, static_cast<Merger&>(self).byte_size());
      std::for_each(self.Lead(), self._heap.end(), [&self, res](size_t it) {
        SDB_ASSERT(it < self._itrs.size());
        if (auto& score = *self._itrs[it].score; !score.IsDefault()) {
          auto& merger = static_cast<Merger&>(self);
          score(merger.temp());
          merger(res, merger.temp());
        }
      });
    });
  }

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
        if (doc_limits::eof(Top()->seek(doc_value))) {
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
      if (!_itrs[*it]->next()) {
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
