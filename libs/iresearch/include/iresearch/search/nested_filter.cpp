////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2022 ArangoDB GmbH, Cologne, Germany
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

#include "nested_filter.hpp"

#include <absl/base/optimization.h>
#include <absl/functional/overload.h>

#include <cstdint>
#include <memory>
#include <span>
#include <tuple>
#include <utility>
#include <variant>

#include "iresearch/search/common/node_of.hpp"
#include "iresearch/search/common/score/make_window.hpp"
#include "iresearch/search/count/plan.hpp"
#include "iresearch/search/count/walk.hpp"
#include "iresearch/search/docs/plan.hpp"
#include "iresearch/search/docs/walk.hpp"
#include "iresearch/search/fill/walk.hpp"
#include "iresearch/search/lead/constant_scored.hpp"
#include "iresearch/search/lead/impl.hpp"
#include "iresearch/search/prepared_state_visitor.hpp"
#include "iresearch/search/probe/constant_scored.hpp"
#include "iresearch/search/probe/impl.hpp"
#include "iresearch/search/query_builder_impl.hpp"
#include "iresearch/search/score_function.hpp"
#include "iresearch/search/scored/detail/walk.hpp"
#include "iresearch/search/scorer.hpp"
#include "iresearch/search/top/detail/walk.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs {
namespace {

static_assert(std::variant_size_v<ByNestedOptions::MatchType> == 2);

const Scorer* GetOrder(const ByNestedOptions::MatchType& match,
                       const Scorer* scorer) noexcept {
  return std::visit(
    absl::Overload{[&](Match v) noexcept -> const Scorer* {
                     return kMatchNone == v ? nullptr : scorer;
                   },
                   [scorer](const MatchProvider&) noexcept -> const Scorer* {
                     return scorer;
                   }},
    match);
}

bool IsValid(const ByNestedOptions::MatchType& match) noexcept {
  return std::visit(
    absl::Overload{[](Match v) noexcept { return v.min <= v.max; },
                   [](const MatchProvider& v) { return nullptr != v; }},
    match);
}

struct EmptyParentsNode : ParentDocs {
  doc_id_t Advance() final { return _doc = doc_limits::eof(); }

  doc_id_t Seek(doc_id_t) final { return _doc = doc_limits::eof(); }

  doc_id_t Prev() const noexcept final { return doc_limits::invalid(); }
};

struct EmptyDocsNode : lead::Node {
  doc_id_t Advance() final { return _doc = doc_limits::eof(); }

  doc_id_t Seek(doc_id_t) final { return _doc = doc_limits::eof(); }
};

lead::Node::ptr EmptyDocs() {
  return memory::make_managed<lead::Node, EmptyDocsNode>();
}

ParentDocs::ptr EmptyParentDocs() {
  return memory::make_managed<ParentDocs, EmptyParentsNode>();
}

class PlainChild {
 public:
  explicit PlainChild(lead::Node::ptr child) noexcept
    : _child{std::move(child)} {
    SDB_ASSERT(_child);
  }

  doc_id_t Value() const noexcept { return _child->Value(); }

  doc_id_t Seek(doc_id_t target) { return _child->Seek(target); }

  doc_id_t Advance() { return _child->Advance(); }

  void Restart() noexcept {}

  void Take(doc_id_t) noexcept {}

 private:
  lead::Node::ptr _child;
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

  doc_id_t Value() const noexcept { return _child->Value(); }

  doc_id_t Seek(doc_id_t target) { return _child->Seek(target); }

  doc_id_t Advance() { return _child->Advance(); }

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
    SDB_ASSERT(slot < doc_limits::kBlockSize);
    _parents[slot] = _sum;
  }

  ScoreFunction PrepareScore() {
    return search::MakeWindowScore(_merge, _parents);
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

  ABSL_CACHELINE_ALIGNED score_t _parents[doc_limits::kBlockSize]{};
  ABSL_CACHELINE_ALIGNED score_t _batch[kScoreBlock];
  ABSL_CACHELINE_ALIGNED doc_id_t _docs[kScoreBlock];
  lead::Node::ptr _child;
  std::unique_ptr<ColumnArgsFetcher> _fetcher;
  ScoreFunction _score;
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
    for (auto doc = child.Value(); doc < parent; doc = child.Advance()) {
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
      doc = child.Advance();
    }
    return true;
  }

  doc_id_t Skip(auto& child) const noexcept {
    return _min != 0 ? child.Value() : doc_limits::invalid();
  }

  static void Settle(auto& child, doc_id_t parent, uint32_t slot) {
    for (auto doc = child.Value(); doc < parent; doc = child.Advance()) {
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
    for (auto doc = child.Seek(first); doc < parent; doc = child.Advance()) {
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
      const auto want = _pred->Advance();
      if (want >= parent) {
        return true;
      }
      doc = child.Advance();
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

  doc_id_t Seek(doc_id_t target) { return _parent->Seek(target); }

  doc_id_t Probe(doc_id_t target) { return _parent->Seek(target); }

  doc_id_t Next(doc_id_t) {
    const auto skip = _rule.Skip(_child);
    if (doc_limits::eof(skip)) {
      return doc_limits::eof();
    }
    if (skip > _parent->Value()) {
      return _parent->Seek(skip + 1);
    }
    return _parent->Advance();
  }

  bool Match(doc_id_t parent) {
    _child.Restart();
    return _rule.Accept(_child, _parent->Prev() + 1, parent);
  }

  void Settle(uint32_t slot) { _rule.Settle(_child, _parent->Value(), slot); }

  ScoreFunction PrepareScore() { return _child.PrepareScore(); }

 private:
  Child _child;
  [[no_unique_address]] Rule _rule;
  ParentDocs::ptr _parent;
};

template<typename Slots>
class NestedScored {
 public:
  template<typename... Args>
  explicit NestedScored(Args&&... args) : _slots{std::forward<Args>(args)...} {}

  NestedScored(NestedScored&&) = delete;
  NestedScored& operator=(NestedScored&&) = delete;

  doc_id_t Value() const noexcept { return _doc; }

  doc_id_t Advance() { return Converge(_slots.Next(_doc)); }

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

template<bool None, typename Visitor>
auto ResolveRule(const SubReader& segment,
                 const ByNestedOptions::MatchType& match, Visitor&& visitor) {
  return std::visit(absl::Overload{[&](Match v) {
                                     if constexpr (None) {
                                       if (v == kMatchNone) {
                                         return visitor(NoneRule{});
                                       }
                                     } else {
                                       SDB_ASSERT(v != kMatchNone);
                                     }
                                     if (v == kMatchAny) {
                                       return visitor(AnyRule{});
                                     }
                                     if (v.IsMinMatch()) {
                                       return visitor(MinRule{v.min});
                                     }
                                     return visitor(RangeRule{v});
                                   },
                                   [&](const MatchProvider& v) {
                                     auto pred = v(segment);
                                     if (!pred) {
                                       return visitor(PredRule{EmptyDocs()});
                                     }
                                     return visitor(PredRule{std::move(pred)});
                                   }},
                    match);
}

template<template<typename> class Impl, typename Result, typename Rule,
         typename... Head>
Result MakeNestedDocs(ParentDocs::ptr parent, lead::Node::ptr child, Rule rule,
                      Head&&... head) {
  using Slots = NestedSlots<PlainChild, Rule>;
  using Node = search::TwoPhaseFor<Result, Slots>;
  return memory::make_managed<Impl<Node>>(
    std::forward<Head>(head)..., std::piecewise_construct, std::move(parent),
    std::forward_as_tuple(std::move(child)), std::move(rule));
}

template<template<typename> class Impl, typename Result, typename Rule,
         typename... Head>
Result MakeNestedScored(ParentDocs::ptr parent, lead::Node::ptr child,
                        std::unique_ptr<ColumnArgsFetcher> fetcher,
                        ScoreMergeType merge, Rule rule, Head&&... head) {
  using Slots = NestedSlots<ScoredChild, Rule>;
  return memory::make_managed<Impl<NestedScored<Slots>>>(
    std::forward<Head>(head)..., std::piecewise_construct, std::move(parent),
    std::forward_as_tuple(std::move(child), std::move(fetcher), merge),
    std::move(rule));
}

}  // namespace

class ByNestedQuery : public QueryBuilderImpl<ByNestedQuery> {
 public:
  static uint32_t EstimateOf(const SubReader& segment,
                             const ByNestedOptions::MatchType& match,
                             const QueryBuilder& child) noexcept {
    const auto docs = static_cast<uint32_t>(segment.docs_count());
    const auto* const range = std::get_if<Match>(&match);
    if (range == nullptr || range->min == 0) {
      return docs;
    }
    return std::min(docs, child.EstimateMax());
  }

  ByNestedQuery(const SubReader& segment, ParentProvider parent,
                QueryBuilder::ptr&& child, ScoreMergeType merge_type,
                ByNestedOptions::MatchType match, score_t constant) noexcept
    : QueryBuilderImpl{segment, EstimateOf(segment, match, *child),
                       QueryKind::Other},
      _parent{std::move(parent)},
      _child{std::move(child)},
      _match{std::move(match)},
      _merge_type{merge_type},
      _constant{constant} {
    SDB_ASSERT(_parent);
    SDB_ASSERT(_child);
    SDB_ASSERT(IsValid(_match));
  }

  void Visit(PreparedStateVisitor& visitor, score_t boost) const final {
    if (!visitor.Visit(*this, boost)) {
      return;
    }

    SDB_ASSERT(_child);
    _child->Visit(visitor, boost);
  }

  score_t Boost() const noexcept final { return kNoBoost; }

  ParentDocs::ptr Parents() const {
    auto parent = _parent(_segment);
    if (!parent) {
      return EmptyParentDocs();
    }
    return parent;
  }

  const QueryBuilder& Child() const noexcept { return *_child; }

  const ByNestedOptions::MatchType& MatchKind() const noexcept {
    return _match;
  }

  ScoreMergeType Merge() const noexcept { return _merge_type; }

  score_t Constant() const noexcept { return IsNoneMatch() ? _constant : 0.F; }

  bool ScoresChildren() const noexcept {
    return _merge_type != ScoreMergeType::Noop && !IsNoneMatch() &&
           !QueryBuilder::IsEmpty(*_child);
  }

 private:
  bool IsNoneMatch() const noexcept {
    const auto* range = std::get_if<Match>(&_match);
    return range != nullptr && *range == kMatchNone;
  }

  ParentProvider _parent;
  QueryBuilder::ptr _child;
  ByNestedOptions::MatchType _match;
  ScoreMergeType _merge_type;
  score_t _constant;
};

namespace {

lead::Node::ptr ChildDocs(const ByNestedQuery& query) {
  auto child = query.Child().PlanLead({});
  if (!child && QueryBuilder::IsEmpty(query.Child())) {
    return EmptyDocs();
  }
  return child;
}

template<template<typename> class Impl, typename Result, typename... Head>
Result PlanNestedDocs(const ByNestedQuery& query, Head&&... head) {
  auto child = ChildDocs(query);
  if (!child) {
    return {};
  }
  auto parent = query.Parents();
  return ResolveRule<true>(query.Segment(), query.MatchKind(),
                           [&]<typename Rule>(Rule rule) -> Result {
                             return MakeNestedDocs<Impl, Result>(
                               std::move(parent), std::move(child),
                               std::move(rule), std::forward<Head>(head)...);
                           });
}

template<template<typename> class Impl, typename Result, typename... Head>
Result PlanNestedScored(const ByNestedQuery& query, search::ScoredCtx ctx,
                        Head&&... head) {
  auto fetcher = std::make_unique<ColumnArgsFetcher>();
  ctx.fetcher = fetcher.get();
  auto child = query.Child().PlanLead(ctx);
  if (!child) {
    return {};
  }
  auto parent = query.Parents();
  const auto merge = query.Merge();
  return ResolveRule<false>(query.Segment(), query.MatchKind(),
                            [&]<typename Rule>(Rule rule) -> Result {
                              return MakeNestedScored<Impl, Result>(
                                std::move(parent), std::move(child),
                                std::move(fetcher), merge, std::move(rule),
                                std::forward<Head>(head)...);
                            });
}

}  // namespace
namespace count {

Root::ptr Make(const ByNestedQuery& query, const Context& ctx) {
  if (ctx.table != nullptr) {
    auto node = lead::Make(query);
    if (!node) {
      return {};
    }
    return MakeShape<Walk, lead::Erased>(ctx, std::move(node));
  }
  return PlanNestedDocs<PlainWalk, Root::ptr>(query, utils::Empty{});
}

}  // namespace count
namespace docs {

Root::ptr Make(const ByNestedQuery& query, const Context& ctx) {
  if (ctx.table != nullptr) {
    return PlanNestedDocs<FilteredWalk, Root::ptr>(query, ctx.table);
  }
  return PlanNestedDocs<PlainWalk, Root::ptr>(query, utils::Empty{});
}

}  // namespace docs
namespace lead {

Node::ptr Make(const ByNestedQuery& query) {
  return PlanNestedDocs<Impl, Node::ptr>(query);
}

Node::ptr Make(const ByNestedQuery& query, const ScoredCtx& ctx) {
  if (query.ScoresChildren()) {
    return PlanNestedScored<Impl, Node::ptr>(query, ctx);
  }
  auto node = Make(query);
  if (!node) {
    return {};
  }
  using Node = ConstantScored<Erased>;
  return memory::make_managed<Impl<Node>>(query.Constant(), std::move(node));
}

}  // namespace lead
namespace probe {

Node::ptr Make(const ByNestedQuery& query, uint64_t) {
  return PlanNestedDocs<Impl, Node::ptr>(query);
}

Node::ptr Make(const ByNestedQuery& query, const ScoredCtx& ctx, uint64_t) {
  if (query.ScoresChildren()) {
    return PlanNestedScored<Impl, Node::ptr>(query, ctx);
  }
  auto node = PlanNestedDocs<Impl, Node::ptr>(query);
  if (!node) {
    return {};
  }
  using Node = ConstantScored<Erased>;
  return memory::make_managed<Impl<Node>>(query.Constant(), std::move(node));
}

}  // namespace probe
namespace fill {

Node::ptr Make(const ByNestedQuery& query) {
  return PlanNestedDocs<ByWalkDocs, Node::ptr>(query);
}

Node::ptr Make(const ByNestedQuery& query, const ScoredCtx& ctx,
               ScoreMergeType merge) {
  if (query.ScoresChildren()) {
    return PlanNestedScored<ByWalkScored, Node::ptr>(query, ctx, merge,
                                                     nullptr);
  }
  auto node = lead::Make(query);
  if (!node) {
    return {};
  }
  using Node = lead::ConstantScored<lead::Erased>;
  return memory::make_managed<ByWalkScored<Node>>(
    merge, ctx.fetcher, query.Constant(), std::move(node));
}

}  // namespace fill
namespace scored {

Root::ptr Make(const ByNestedQuery& query, const Context& ctx) {
  if (query.ScoresChildren()) {
    if (ctx.table != nullptr) {
      return PlanNestedScored<FilteredWalk, Root::ptr>(query, ScoredOf(ctx),
                                                       ctx.table, ctx.fetcher);
    }
    return PlanNestedScored<PlainWalk, Root::ptr>(query, ScoredOf(ctx),
                                                  utils::Empty{}, ctx.fetcher);
  }
  auto node = lead::Make(query);
  if (!node) {
    return {};
  }
  return MakeShape<detail::ConstantWalk, lead::Erased>(
    ctx, query.Constant(), lead::Erased{std::move(node)});
}

}  // namespace scored
namespace top {

Root::ptr Make(const ByNestedQuery& query, const Context& ctx) {
  if (query.ScoresChildren()) {
    if (ctx.table != nullptr) {
      return PlanNestedScored<FilteredWalk, Root::ptr>(query, ScoredOf(ctx),
                                                       ctx.table, ctx.fetcher);
    }
    return PlanNestedScored<PlainWalk, Root::ptr>(query, ScoredOf(ctx),
                                                  utils::Empty{}, ctx.fetcher);
  }
  auto node = lead::Make(query);
  if (!node) {
    return {};
  }
  return MakeShape<detail::ConstantWalk, lead::Erased>(
    ctx, query.Constant(), lead::Erased{std::move(node)});
}

}  // namespace top

PrepareCollector::ptr ByNestedFilter::MakeCollectorImpl(
  const Scorer* scorer, StatsArena& stats, uint32_t threads) const {
  auto& [parent, child, match, merge_type] = options();

  if (!parent || !child || !IsValid(match)) {
    return nullptr;
  }

  auto compound = std::make_unique<CompoundCollector>(scorer);
  const auto* const order = GetOrder(match, scorer);
  compound->Add(order != nullptr ? child->MakeCollector(*order, stats, threads)
                                 : nullptr);
  return compound;
}

QueryBuilder::ptr ByNestedFilter::PrepareSegment(
  const SubReader& segment, const PrepareContext& ctx) const {
  auto& [parent, child, match, merge_type] = options();

  if (!parent || !child || !IsValid(match)) {
    return QueryBuilder::Empty();
  }

  auto* compound =
    ctx.collector != nullptr ? ctx.collector->AsCompound() : nullptr;
  SDB_ASSERT(ctx.collector == nullptr || compound != nullptr);

  const auto sub_boost = ctx.boost * GetBoost();

  PrepareContext child_ctx = ctx;
  child_ctx.boost = sub_boost;
  child_ctx.collector = compound ? compound->Child(0) : nullptr;

  auto prepared_child = child->PrepareSegment(segment, child_ctx);

  if (!prepared_child) {
    return QueryBuilder::Empty();
  }

  auto query = memory::make_tracked<ByNestedQuery>(
    ctx.memory, segment, parent, std::move(prepared_child), merge_type, match,
    sub_boost);
  query->SetStats(ctx.Record());
  return query;
}

}  // namespace irs
