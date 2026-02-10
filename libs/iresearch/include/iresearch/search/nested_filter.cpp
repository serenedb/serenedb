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

#include <absl/functional/overload.h>

#include <cstdint>
#include <limits>
#include <span>
#include <tuple>
#include <utility>
#include <variant>

#include "basics/empty.hpp"
#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/search/cost.hpp"
#include "iresearch/search/prepared_state_visitor.hpp"
#include "iresearch/search/prev_doc.hpp"
#include "iresearch/search/score.hpp"
#include "iresearch/search/score_function.hpp"
#include "iresearch/search/scorer.hpp"
#include "iresearch/utils/attribute_helper.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace {

using namespace irs;

static_assert(std::variant_size_v<ByNestedOptions::MatchType> == 2);

const Scorers& GetOrder(const ByNestedOptions::MatchType& match,
                        const Scorers& ord) noexcept {
  return std::visit(
    absl::Overload{[&](Match v) noexcept -> const Scorers& {
                     return kMatchNone == v ? Scorers::kUnordered : ord;
                   },
                   [&ord](const DocIteratorProvider&) noexcept
                     -> const Scorers& { return ord; }},
    match);
}

bool IsValid(const ByNestedOptions::MatchType& match) noexcept {
  return std::visit(
    absl::Overload{[](Match v) noexcept { return v.min <= v.max; },
                   [](const DocIteratorProvider& v) {
                     {
                       return nullptr != v;
                     }
                   }},
    match);
}

class NoneMatcher;

template<typename Matcher>
class ChildToParentJoin : public DocIterator, private Matcher {
 public:
  ChildToParentJoin(DocIterator::ptr&& parent, const PrevDocAttr& prev_parent,
                    DocIterator::ptr&& child, Matcher&& matcher) noexcept
    : Matcher{std::move(matcher)},
      _parent{std::move(parent)},
      _child{std::move(child)},
      _prev_parent{&prev_parent} {
    SDB_ASSERT(_parent);
    SDB_ASSERT(prev_parent);
    SDB_ASSERT(_child);

    std::get<AttributePtr<DocAttr>>(_attrs) =
      irs::GetMutable<DocAttr>(_parent.get());
    SDB_ASSERT(std::get<AttributePtr<DocAttr>>(_attrs).ptr);

    _child_doc = irs::get<DocAttr>(*_child);

    std::get<AttributePtr<CostAttr>>(_attrs) =
      irs::GetMutable<CostAttr>(_child.get());
  }

  Attribute* GetMutable(TypeInfo::type_id id) noexcept final {
    return irs::GetMutable(_attrs, id);
  }

  doc_id_t value() const noexcept final {
    return std::get<AttributePtr<DocAttr>>(_attrs).ptr->value;
  }

  doc_id_t advance() final {
    const auto parent = _parent->advance();
    return SeekInternal(parent);
  }

  doc_id_t seek(doc_id_t target) final {
    if (const auto doc = value(); target <= doc) [[unlikely]] {
      return doc;
    }
    const auto parent = _parent->seek(target);
    return SeekInternal(parent);
  }

  uint32_t count() final { return Count(*this); }

  ScoreFunction PrepareScore(const PrepareScoreContext& ctx) final;

  uint32_t Collect(const ScoreFunction& scorer, ColumnCollector& columns,
                   std::span<doc_id_t, kScoreBlock> docs,
                   std::span<score_t, kScoreBlock> scores) final {
    SDB_ASSERT(kScoreBlock <= docs.size());
    return DocIterator::Collect(*this, scorer, columns, docs, scores);
  }

  void FetchScoreArgs(uint16_t index) final {
    if constexpr (Matcher::kHasScore) {
      Matcher::CollectDataImpl();
    }
  }

 private:
  friend Matcher;

  using Attributes = std::tuple<AttributePtr<DocAttr>, AttributePtr<CostAttr>>;

  // Returns min possible first child given the current parent.
  doc_id_t FirstChildApprox() const {
    SDB_ASSERT(!doc_limits::eof((*_prev_parent)()));
    return (*_prev_parent)() + 1;
  }

  doc_id_t SeekInternal(doc_id_t parent) {
    if (doc_limits::eof(parent)) [[unlikely]] {
      return doc_limits::eof();
    }
    for (doc_id_t first_child = _child->seek(FirstChildApprox());
         (first_child = Matcher::Accept(first_child, parent));
         first_child = _child->seek(FirstChildApprox())) {
      parent = _parent->seek(first_child);

      if (doc_limits::eof(parent) ||
          (parent == first_child && !_parent->next())) {  // Skip parent docs
        return doc_limits::eof();
      }
    }

    return value();
  }

  DocIterator::ptr _parent;
  DocIterator::ptr _child;
  Attributes _attrs;
  const PrevDocAttr* _prev_parent{};
  const DocAttr* _child_doc{};
};

template<typename Matcher>
ScoreFunction ChildToParentJoin<Matcher>::PrepareScore(
  const PrepareScoreContext& ctx) {
  if constexpr (std::is_same_v<Matcher, NoneMatcher>) {
    return Matcher::PrepareMatcherScore();
  } else if constexpr (!Matcher::kHasScore) {
    return ScoreFunction::Default();
  } else {
    if (!ctx.scorer || !this->_child_doc) {
      return ScoreFunction::Default();
    }

    auto child_ctx = ctx;
    child_ctx.collector = &this->_scores.collector;
    auto child_score = _child->PrepareScore(child_ctx);

    if (child_score.IsDefault()) {
      return ScoreFunction::Default();
    }

    this->_scores.child_score = std::move(child_score);
    return Matcher::PrepareMatcherScore();
  }
}

class NoneMatcher {
 public:
  using JoinType = ChildToParentJoin<NoneMatcher>;

  static constexpr bool kHasScore = false;

  NoneMatcher(score_t none_boost) noexcept : _boost{none_boost} {}

  constexpr doc_id_t Accept(const doc_id_t child,
                            const doc_id_t parent) const noexcept {
    SDB_ASSERT(!doc_limits::eof(parent));
    return child < parent ? parent + 1 : 0;
  }

  ScoreFunction PrepareMatcherScore() const {
    return ScoreFunction::Constant(_boost);
  }

 private:
  score_t _boost;
};

template<ScoreMergeType MergeType>
struct NestedScoreContext {
  ScoreFunction child_score;
  ColumnCollector collector;
  std::array<score_t, kScoreBlock> parent_scores{};
  std::array<score_t, kScoreBlock> child_temp;
  std::array<doc_id_t, kScoreBlock> child_docs;
  score_t current_parent_score = 0;
  uint16_t child_idx = 0;
  uint16_t parent_idx = 0;

  void CollectChild(auto& child_it) {
    child_docs[child_idx] = child_it.value();
    child_it.FetchScoreArgs(child_idx++);
    if (child_idx == kScoreBlock) {
      FlushChildBatch();
    }
  }

  void FinishParent() {
    if (child_idx) {
      FlushChildBatch();
    }
    parent_scores[parent_idx++] = current_parent_score;
    current_parent_score = 0;
  }

  void DiscardParent() {
    child_idx = 0;
    current_parent_score = 0;
  }

  void FlushChildBatch() {
    SDB_ASSERT(child_idx);
    collector.Collect(std::span{child_docs.data(), child_idx});
    child_score.Score(child_temp.data(), child_idx);
    for (uint16_t i = 0; i < child_idx; ++i) {
      Merge<MergeType>(current_parent_score, child_temp[i]);
    }
    child_idx = 0;
  }

  void Score(score_t* res, size_t n) {
    std::memcpy(res, parent_scores.data(), n * sizeof(score_t));
    parent_idx = 0;
  }
};

template<ScoreMergeType MergeType>
class MatcherBase : public ScoreCtx {
 protected:
  static constexpr auto kMergeType = MergeType;
  static constexpr bool kHasScore = kMergeType != ScoreMergeType::Noop;

  ScoreFunction PrepareMatcherScore() {
    static_assert(kHasScore);
    return {this,
            [](ScoreCtx* ctx, score_t* res, size_t n) noexcept {
              SDB_ASSERT(ctx);
              SDB_ASSERT(res);
              auto& self = static_cast<MatcherBase&>(*ctx);
              self._scores.Score(res, n);
            },
            ScoreFunction::NoopMin};
  }

  void CollectChild(auto& it) {
    if constexpr (kHasScore) {
      _scores.CollectChild(it);
    }
  }

  void FinishParent() {
    if constexpr (kHasScore) {
      _scores.FinishParent();
    }
  }

  void DiscardParent() {
    if constexpr (kHasScore) {
      _scores.DiscardParent();
    }
  }

  [[no_unique_address]] utils::Need<kHasScore, NestedScoreContext<MergeType>>
    _scores;
};

template<ScoreMergeType MergeType>
class AnyMatcher : protected MatcherBase<MergeType> {
 public:
  using JoinType = ChildToParentJoin<AnyMatcher<MergeType>>;

  constexpr doc_id_t Accept(const doc_id_t child,
                            const doc_id_t parent) const noexcept {
    SDB_ASSERT(!doc_limits::eof(parent));
    return child < parent ? 0 : child;
  }

  void CollectDataImpl() {
    if constexpr (MatcherBase<MergeType>::kHasScore) {
      auto& self = static_cast<JoinType&>(*this);
      auto& child = *self._child;
      const auto parent_doc = self.value();

      child.Collect([&](doc_id_t doc) -> bool {
        if (doc >= parent_doc) {
          return false;
        }
        this->CollectChild(child);
        return true;
      });
      this->FinishParent();
    }
  }
};

template<ScoreMergeType MergeType>
class PredMatcher : protected MatcherBase<MergeType> {
 public:
  using JoinType = ChildToParentJoin<PredMatcher<MergeType>>;

  static constexpr auto kMergeType = MergeType;
  static constexpr bool kHasScore = kMergeType != ScoreMergeType::Noop;

  explicit PredMatcher(DocIterator::ptr&& pred) noexcept
    : _pred{std::move(pred)} {
    if (!_pred) [[unlikely]] {
      _pred = DocIterator::empty();
    }

    _pred_doc = irs::get<DocAttr>(*_pred);
    SDB_ASSERT(_pred_doc);
  }

  doc_id_t Accept(const doc_id_t first_child, const doc_id_t parent) {
    SDB_ASSERT(!doc_limits::eof(parent));

    if (first_child > parent) {
      return first_child;
    }

    auto& self = static_cast<JoinType&>(*this);

    if (first_child != _pred->seek(self.FirstChildApprox())) {
      return parent + 1;
    }

    auto& child = *self._child;

    this->CollectChild(child);

    while (true) {
      const auto pred_doc = _pred->advance();
      if (parent <= pred_doc) {
        return doc_limits::invalid();
      }
      SDB_ASSERT(!doc_limits::eof(pred_doc));

      const auto child_doc = child.advance();
      if (pred_doc != child_doc) {
        this->DiscardParent();
        return parent + 1;
      }
      SDB_ASSERT(!doc_limits::eof(child_doc));

      this->CollectChild(child);
    }
  }

  void CollectDataImpl() { this->FinishParent(); }

 private:
  DocIterator::ptr _pred;
  const DocAttr* _pred_doc;
};

template<ScoreMergeType MergeType>
class RangeMatcher : protected MatcherBase<MergeType> {
 public:
  using JoinType = ChildToParentJoin<RangeMatcher<MergeType>>;

  static constexpr auto kMergeType = MergeType;
  static constexpr bool kHasScore = kMergeType != ScoreMergeType::Noop;

  RangeMatcher(Match match) noexcept : _match{match} {
    // This case is handled by MinMatcher
    SDB_ASSERT(_match != Match{0});
  }

  doc_id_t Accept(const doc_id_t first_child, const doc_id_t parent) {
    SDB_ASSERT(!doc_limits::eof(parent));

    const auto [min, max] = _match;
    SDB_ASSERT(min <= max);

    if (first_child > parent) {
      if (min == 0) {
        return 0;
      }
      return first_child;
    }

    auto& self = static_cast<JoinType&>(*this);
    auto& child = *self._child;

    // Already matched the first child
    doc_id_t count = 1;

    this->CollectChild(child);

    while (child.advance() < parent) {
      if (++count > max) {
        this->DiscardParent();
        return parent + 1;
      }

      this->CollectChild(child);
    }

    if (min <= count) {
      return 0;
    }

    this->DiscardParent();
    return parent + 1;
  }

  void CollectDataImpl() { this->FinishParent(); }

  const Match& Range() const noexcept { return _match; }

 private:
  const Match _match;
};

template<ScoreMergeType MergeType>
class MinMatcher : protected MatcherBase<MergeType> {
 public:
  using JoinType = ChildToParentJoin<MinMatcher<MergeType>>;

  static constexpr auto kMergeType = MergeType;
  static constexpr bool kHasScore = kMergeType != ScoreMergeType::Noop;

  MinMatcher(doc_id_t min) noexcept : _min{min} {}

  doc_id_t Accept(const doc_id_t first_child, const doc_id_t parent) {
    SDB_ASSERT(!doc_limits::eof(parent));

    if (0 == _min) {
      return 0;
    }

    if (first_child > parent) {
      return first_child;
    }

    doc_id_t count = _min - 1;

    if (!count) {
      return 0;
    }

    auto& self = static_cast<JoinType&>(*this);
    auto& child = *self._child;

    this->CollectChild(child);

    while (child.advance() < parent) {
      this->CollectChild(child);

      if (!--count) {
        return 0;
      }
    }

    this->DiscardParent();
    return parent + 1;
  }

  void CollectDataImpl() {
    if constexpr (kHasScore) {
      auto& self = static_cast<JoinType&>(*this);
      auto& child = *self._child;
      const auto parent_doc = self.value();

      child.Collect([&](doc_id_t doc) {
        if (doc >= parent_doc) {
          return false;
        }
        this->CollectChild(child);
        return true;
      });
      this->FinishParent();
    }
  }

  Match Range() const noexcept { return Match{_min}; }

 private:
  const doc_id_t _min;
};

template<ScoreMergeType MergeType, typename Visitor>
auto ResolveMatchType(const SubReader& segment,
                      const ByNestedOptions::MatchType& match,
                      score_t none_boost, Visitor&& visitor) {
  return std::visit(
    absl::Overload{[&](Match v) {
                     if (v == kMatchNone) {
                       return visitor(NoneMatcher{none_boost});
                     } else if (v == kMatchAny) {
                       return visitor(AnyMatcher<MergeType>{});
                     } else if (v.IsMinMatch()) {
                       SDB_ASSERT(doc_limits::eof(v.max));
                       return visitor(MinMatcher<MergeType>{v.min});
                     } else {
                       return visitor(RangeMatcher<MergeType>{v});
                     }
                   },
                   [&](const DocIteratorProvider& v) {
                     return visitor(PredMatcher<MergeType>{v(segment)});
                   }},
    match);
}

}  // namespace

namespace irs {

class ByNestedQuery : public Filter::Query {
 public:
  ByNestedQuery(DocIteratorProvider parent, Query::ptr&& child,
                ScoreMergeType merge_type, ByNestedOptions::MatchType match,
                score_t none_boost) noexcept
    : _parent{std::move(parent)},
      _child{std::move(child)},
      _match{std::move(match)},
      _merge_type{merge_type},
      _none_boost{none_boost} {
    SDB_ASSERT(_parent);
    SDB_ASSERT(_child);
    SDB_ASSERT(IsValid(_match));
  }

  DocIterator::ptr execute(const ExecutionContext& ctx) const final;

  void visit(const SubReader& segment, PreparedStateVisitor& visitor,
             score_t boost) const final {
    // TODO(mbkkt) maybe use none_boost for NoneMatcher?
    // boost *= this->Boost();

    if (!visitor.Visit(*this, boost)) {
      return;
    }

    SDB_ASSERT(_child);
    _child->visit(segment, visitor, boost);
  }

  score_t Boost() const noexcept final { return kNoBoost; }

 private:
  DocIteratorProvider _parent;
  Query::ptr _child;
  ByNestedOptions::MatchType _match;
  ScoreMergeType _merge_type;
  score_t _none_boost;
};

DocIterator::ptr ByNestedQuery::execute(const ExecutionContext& ctx) const {
  auto& rdr = ctx.segment;
  auto& ord = ctx.scorers;

  auto parent = _parent(rdr);

  if (!parent || doc_limits::eof(parent->value())) [[unlikely]] {
    return DocIterator::empty();
  }

  const auto* prev = irs::get<PrevDocAttr>(*parent);

  if (!prev || !*prev) [[unlikely]] {
    return DocIterator::empty();
  }

  auto child = _child->execute({.segment = rdr,
                                .scorers = GetOrder(_match, ord),
                                .ctx = ctx.ctx,
                                // TODO(mbkkt) wand for nested?
                                .wand = {}});

  if (!child) [[unlikely]] {
    return DocIterator::empty();
  }

  return ResolveMergeType(
    _merge_type, [&]<ScoreMergeType MergeType>() -> DocIterator::ptr {
      return ResolveMatchType<MergeType>(
        rdr, _match, _none_boost,
        [&]<typename M>(M&& matcher) -> DocIterator::ptr {
          if constexpr (std::is_same_v<NoneMatcher, M>) {
            if (doc_limits::eof(child->value()) && ord.empty()) {
              return std::move(parent);
            }
          } else if constexpr (std::is_same_v<MinMatcher<MergeType>, M> ||
                               std::is_same_v<RangeMatcher<MergeType>, M>) {
            // When min=0 and child has no matches, every parent matches
            // with score 0 — return parent directly for efficiency
            if (Match{0} == matcher.Range() &&
                doc_limits::eof(child->value())) {
              return std::move(parent);
            }
          } else {
            if (doc_limits::eof(child->value())) {
              return DocIterator::empty();
            }
          }

          return memory::make_managed<ChildToParentJoin<M>>(
            std::move(parent), *prev, std::move(child), std::move(matcher));
        });
    });
}

Filter::Query::ptr ByNestedFilter::prepare(const PrepareContext& ctx) const {
  auto& [parent, child, match, merge_type] = options();

  if (!parent || !child || !IsValid(match)) {
    return Query::empty();
  }

  const auto sub_boost = ctx.boost * Boost();

  auto prepared_child = child->prepare({
    .index = ctx.index,
    .memory = ctx.memory,
    .scorers = GetOrder(match, ctx.scorers),
    .ctx = ctx.ctx,
    .boost = sub_boost,
  });

  if (!prepared_child) {
    return Query::empty();
  }

  return memory::make_tracked<ByNestedQuery>(
    ctx.memory, parent, std::move(prepared_child), merge_type, match,
    /*none_boost*/ sub_boost);
}

}  // namespace irs
