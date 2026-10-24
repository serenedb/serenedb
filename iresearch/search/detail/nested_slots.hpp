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

#include <absl/base/optimization.h>

#include <memory>
#include <span>
#include <tuple>
#include <utility>

#include "iresearch/search/filters/nested_filter.hpp"
#include "iresearch/search/lead/node.hpp"
#include "iresearch/search/scorers/make_window.hpp"
#include "iresearch/search/scorers/score_args.hpp"
#include "iresearch/search/scorers/score_function.hpp"
#include "iresearch/search/scorers/scorer.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::detail {

class PlainChild {
 public:
  explicit PlainChild(lead::Node::ptr child) noexcept
    : _child{std::move(child)} {
    SDB_ASSERT(_child);
  }

  doc_id_t Value() const noexcept { return _doc; }

  doc_id_t Seek(doc_id_t target) { return _doc = _child->Seek(target); }

  doc_id_t Next() { return _doc = _child->Next(); }

  void Restart() noexcept {}

  void Take(doc_id_t) noexcept {}

 private:
  lead::Node::ptr _child;
  doc_id_t _doc = doc_limits::invalid();
};

class ScoredChild {
 public:
  ScoredChild(lead::Node::ptr child, std::unique_ptr<ColumnArgsFetcher> fetcher,
              ScoreMergeType merge)
    : _child{std::move(child)}, _fetcher{std::move(fetcher)}, _merge{merge} {
    SDB_ASSERT(_child);
    SDB_ASSERT(_fetcher);
    SDB_ASSERT(_merge != ScoreMergeType::Noop);
    _score = _child->PrepareScore();
  }

  ScoredChild(ScoredChild&&) = delete;
  ScoredChild& operator=(ScoredChild&&) = delete;

  doc_id_t Value() const noexcept { return _doc; }

  doc_id_t Seek(doc_id_t target) { return _doc = _child->Seek(target); }

  doc_id_t Next() { return _doc = _child->Next(); }

  void Restart() noexcept {
    _held = 0;
    _sum = 0;
  }

  void Take(doc_id_t doc) {
    _docs[_held] = doc;
    _child->FetchScoreArgs(_held);
    if (++_held == kScoreBlock) {
      Flush();
    }
  }

  void Settle(uint32_t slot) {
    if (_held != 0) {
      Flush();
    }
    SDB_ASSERT(slot < kScoreBlock);
    _parents[slot] = _sum;
  }

  ScoreFunction PrepareScore() {
    return detail::MakeWindowScore(_merge, _parents);
  }

 private:
  void Flush() {
    _fetcher->Fetch(std::span<const doc_id_t>{_docs, _held});
    _score.Score(_batch, _held);
    irs::ResolveMergeType(_merge, [&]<ScoreMergeType Merge> {
      for (scores_size_t i = 0; i != _held; ++i) {
        irs::Merge<Merge>(_sum, _batch[i]);
      }
    });
    _held = 0;
  }

  ABSL_CACHELINE_ALIGNED score_t _parents[kScoreBlock]{};
  ABSL_CACHELINE_ALIGNED score_t _batch[kScoreBlock];
  ABSL_CACHELINE_ALIGNED doc_id_t _docs[kScoreBlock];
  lead::Node::ptr _child;
  std::unique_ptr<ColumnArgsFetcher> _fetcher;
  ScoreFunction _score;
  doc_id_t _doc = doc_limits::invalid();
  score_t _sum = 0;
  scores_size_t _held = 0;
  ScoreMergeType _merge;
};

struct NoneRule {
  static bool Accept(auto& child, doc_id_t first, doc_id_t parent) {
    return child.Seek(first) >= parent;
  }

  static doc_id_t Skip(auto&) noexcept { return doc_limits::invalid(); }

  static void Settle(auto& child, doc_id_t, uint32_t slot) {
    child.Settle(slot);
  }
};

struct AnyRule {
  static bool Accept(auto& child, doc_id_t first, doc_id_t parent) {
    return child.Seek(first) < parent;
  }

  static doc_id_t Skip(auto& child) noexcept { return child.Value(); }

  static void Settle(auto& child, doc_id_t parent, uint32_t slot) {
    for (auto doc = child.Value(); doc < parent; doc = child.Next()) {
      child.Take(doc);
    }
    child.Settle(slot);
  }
};

class MinRule {
 public:
  explicit MinRule(doc_id_t min) noexcept : _min{min} {}

  bool Accept(auto& child, doc_id_t first, doc_id_t parent) const {
    auto doc = child.Seek(first);
    for (auto left = _min; left != 0; --left) {
      if (doc >= parent) {
        return false;
      }
      child.Take(doc);
      doc = child.Next();
    }
    return true;
  }

  doc_id_t Skip(auto& child) const noexcept {
    return _min != 0 ? child.Value() : doc_limits::invalid();
  }

  static void Settle(auto& child, doc_id_t parent, uint32_t slot) {
    for (auto doc = child.Value(); doc < parent; doc = child.Next()) {
      child.Take(doc);
    }
    child.Settle(slot);
  }

 private:
  doc_id_t _min;
};

class RangeRule {
 public:
  explicit RangeRule(Match range) noexcept : _range{range} {
    SDB_ASSERT(_range.min <= _range.max);
  }

  bool Accept(auto& child, doc_id_t first, doc_id_t parent) const {
    doc_id_t count = 0;
    for (auto doc = child.Seek(first); doc < parent; doc = child.Next()) {
      if (++count > _range.max) {
        return false;
      }
      child.Take(doc);
    }
    return count >= _range.min;
  }

  doc_id_t Skip(auto& child) const noexcept {
    return _range.min != 0 ? child.Value() : doc_limits::invalid();
  }

  static void Settle(auto& child, doc_id_t, uint32_t slot) {
    child.Settle(slot);
  }

 private:
  Match _range;
};

class PredRule {
 public:
  explicit PredRule(lead::Node::ptr pred) noexcept : _pred{std::move(pred)} {
    SDB_ASSERT(_pred);
  }

  bool Accept(auto& child, doc_id_t first, doc_id_t parent) const {
    auto doc = child.Seek(first);
    if (doc >= parent || doc != _pred->Seek(first)) {
      return false;
    }
    child.Take(doc);
    while (true) {
      const auto want = _pred->Next();
      if (want >= parent) {
        return true;
      }
      doc = child.Next();
      if (doc != want) {
        return false;
      }
      child.Take(doc);
    }
  }

  static doc_id_t Skip(auto&) noexcept { return doc_limits::invalid(); }

  static void Settle(auto& child, doc_id_t, uint32_t slot) {
    child.Settle(slot);
  }

 private:
  lead::Node::ptr _pred;
};

template<typename Child, typename Rule>
class NestedSlots {
 public:
  template<typename ChildArgs>
  NestedSlots(std::piecewise_construct_t, ParentDocs::ptr parent,
              ChildArgs&& child, Rule rule)
    : _child{std::make_from_tuple<Child>(std::forward<ChildArgs>(child))},
      _rule{std::move(rule)},
      _parent{std::move(parent)} {
    SDB_ASSERT(_parent);
  }

  doc_id_t Seek(doc_id_t target) { return _doc = _parent->Seek(target); }

  doc_id_t Probe(doc_id_t target) { return _doc = _parent->Seek(target); }

  doc_id_t Next(doc_id_t) {
    const auto skip = _rule.Skip(_child);
    if (doc_limits::eof(skip)) {
      return doc_limits::eof();
    }
    if (skip > _doc) {
      return _doc = _parent->Seek(skip + 1);
    }
    return _doc = _parent->Next();
  }

  bool Match(doc_id_t parent) {
    _child.Restart();
    return _rule.Accept(_child, _parent->Prev() + 1, parent);
  }

  void Settle(uint32_t slot) { _rule.Settle(_child, _doc, slot); }

  ScoreFunction PrepareScore() { return _child.PrepareScore(); }

 private:
  Child _child;
  [[no_unique_address]] Rule _rule;
  ParentDocs::ptr _parent;
  doc_id_t _doc = doc_limits::invalid();
};

template<typename Slots>
class NestedScored {
 public:
  template<typename... Args>
  explicit NestedScored(Args&&... args) : _slots{std::forward<Args>(args)...} {}

  NestedScored(NestedScored&&) = delete;
  NestedScored& operator=(NestedScored&&) = delete;

  doc_id_t Value() const noexcept { return _doc; }

  doc_id_t Next() { return Converge(_slots.Next(_doc)); }

  doc_id_t Seek(doc_id_t target) {
    if (target <= _doc) {
      return _doc;
    }
    return Converge(_slots.Seek(target));
  }

  doc_id_t Probe(doc_id_t target) {
    if (target <= _doc) {
      return _doc;
    }
    if (target < _bound) {
      return _bound;
    }
    if (const auto probe = _slots.Probe(target); probe != target) {
      return _bound = probe;
    }
    if (!_slots.Match(target)) {
      return _bound = target + 1;
    }
    return _doc = target;
  }

  void FetchScoreArgs(uint32_t slot) { _slots.Settle(slot); }

  ScoreFunction PrepareScore() { return _slots.PrepareScore(); }

 private:
  doc_id_t Converge(doc_id_t target) {
    while (!doc_limits::eof(target)) {
      if (_slots.Match(target)) {
        return _doc = target;
      }
      target = _slots.Next(target);
    }
    return _doc = target;
  }

  Slots _slots;
  doc_id_t _doc = doc_limits::invalid();
  doc_id_t _bound = doc_limits::min();
};

}  // namespace irs::detail
