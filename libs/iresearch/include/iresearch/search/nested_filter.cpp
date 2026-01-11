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

#include <iresearch/search/score_function.hpp>
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

class ScorerWrapper : public DocIterator {
 public:
  explicit ScorerWrapper(DocIterator::ptr it, ScoreFunction&& score) noexcept
    : _it{std::move(it)} {
    SDB_ASSERT(_it);
    _score = std::move(score);
  }

  Attribute* GetMutable(TypeInfo::type_id id) final {
    if (irs::Type<ScoreAttr>::id() == id) {
      return &_score;
    }

    return _it->GetMutable(id);
  }

  doc_id_t value() const final { return _it->value(); }

  doc_id_t advance() final { return _it->advance(); }

  doc_id_t seek(doc_id_t target) final { return _it->seek(target); }

  doc_id_t shallow_seek(doc_id_t target) final {
    return _it->shallow_seek(target);
  }

  uint32_t count() final { return _it->count(); }

 private:
  DocIterator::ptr _it;
  ScoreAttr _score;
};

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

    if constexpr (Matcher::kHasScore) {
      PrepareScore();
    }
  }

  Attribute* GetMutable(TypeInfo::type_id id) final {
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

  uint32_t collect(std::span<doc_id_t> docs) noexcept {
    const ScoreFunction* score;
    if constexpr (Matcher::kHasScore) {
      score = &std::get<ScoreAttr>(_attrs);
    }
    return Collect(*this, docs, [&] {
      if constexpr (Matcher::kHasScore) {
        score->Collect();
      }
    });
  }

 private:
  friend Matcher;

  using Attributes =
    std::tuple<AttributePtr<DocAttr>, AttributePtr<CostAttr>, ScoreAttr>;

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

  void PrepareScore();

  DocIterator::ptr _parent;
  DocIterator::ptr _child;
  Attributes _attrs;
  const PrevDocAttr* _prev_parent{};
  const DocAttr* _child_doc{};
};

template<typename Matcher>
void ChildToParentJoin<Matcher>::PrepareScore() {
  auto& score = std::get<ScoreAttr>(_attrs);
  if constexpr (!Matcher::kHasScore) {
    score = ScoreFunction::Default();
  } else {
    this->_child_score = irs::get<ScoreAttr>(*_child);

    if (!std::is_same_v<Matcher, NoneMatcher> &&
        (this->_child_doc == nullptr || this->_child_score == nullptr ||
         this->_child_score->IsDefault())) {
      score = ScoreFunction::Default();
    } else {
      static_assert(Matcher::kHasScore);
      score = static_cast<Matcher&>(*this).PrepareScore();
    }
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

  ScoreFunction PrepareScore() const { return ScoreFunction::Constant(_boost); }

 private:
  score_t _boost;
};

struct NestedScoreContext {
  std::vector<uint32_t> childs_score_count{0};
  std::vector<score_t> scores;

  template<ScoreMergeType MergeType>
  void Score(score_t* res, const ScoreFunction& scorer) {
    scores.reserve(index);
    scorer.Score(scores.data());

    size_t i = 0;
    auto* score = scores.data();
    for (size_t count : childs_score_count) {
      Merge<MergeType>(&res[i], std::span{score, count});
      score += count;
      ++i;
    }
    index = 0;
  }
  void Collect() { ++childs_score_count.back(); }
  void Next() { childs_score_count.emplace_back(); }

  size_t index = 0;
};

template<ScoreMergeType MergeType>
class MatcherBase : public ScoreCtx {
 protected:
  static constexpr auto kMergeType = MergeType;
  static constexpr bool kHasScore = kMergeType != ScoreMergeType::Noop;

  static ScoreFunction NestedScore(ScoreCtx* ctx, auto collect, auto min) {
    static_assert(kHasScore);
    return {*ctx,
            [](ScoreCtx* ctx, score_t* res) noexcept {
              SDB_ASSERT(ctx);
              SDB_ASSERT(res);
              auto& self = static_cast<MatcherBase&>(*ctx);
              self._scores.template Score<kMergeType>(res, *self._child_score);
            },
            collect, min};
  }

  void Collect() {
    static_assert(kHasScore);
    _child_score->Collect();
    _scores.Collect();
  }

  utils::Need<kHasScore, NestedScoreContext> _scores;
  [[no_unique_address]] irs::utils::Need<kHasScore, const ScoreAttr*>
    _child_score{};
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

  ScoreFunction PrepareScore() {
    return this->NestedScore(
      this,
      [](ScoreCtx* ctx) noexcept {
        auto& self = static_cast<JoinType&>(*ctx);
        auto& child = *self._child;
        const auto parent_doc = self.value();

        self._scores.Collect();
        while (child.advance() < parent_doc) {
          self._scores.Collect();
        }
        self._scores.Next();
      },
      ScoreFunction::NoopMin);
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

    Finally next = [&] noexcept {
      if constexpr (kHasScore) {
        this->_scores.Next();
      }
    };

    if constexpr (kHasScore) {
      this->_scores.Collect();
    }

    while (true) {
      const auto pred_doc = _pred->advance();
      if (parent <= pred_doc) {
        return doc_limits::invalid();
      }
      SDB_ASSERT(!doc_limits::eof(pred_doc));

      const auto child_doc = child.advance();
      if (pred_doc != child_doc) {
        return parent + 1;
      }
      SDB_ASSERT(!doc_limits::eof(child_doc));

      if constexpr (kHasScore) {
        this->_scores.Collect();
      }
    }
  }

  ScoreFunction PrepareScore() noexcept {
    static_assert(kHasScore);
    return this->NestedScore(this, ScoreFunction::NoopCollect,
                             ScoreFunction::NoopMin);
  }

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
        // we are not able to find any childs
        if constexpr (kHasScore) {
          this->_scores.Next();
        }
        return 0;
      }

      return first_child;
    }

    auto& self = static_cast<JoinType&>(*this);
    auto& child = *self._child;

    // Already matched the first child
    doc_id_t count = 1;

    Finally next = [&] noexcept {
      if constexpr (kHasScore) {
        this->_scores.Next();
      }
    };

    if constexpr (kHasScore) {
      this->_scores.Collect();
    }

    while (child.advance() < parent) {
      if (++count > max) {
        return parent + 1;
      }

      if constexpr (kHasScore) {
        this->_scores.Collect();
      }
    }

    return min <= count ? 0 : parent + 1;
  }

  ScoreFunction PrepareScore() noexcept {
    static_assert(kHasScore);
    return this->NestedScore(this, ScoreFunction::NoopCollect,
                             ScoreFunction::NoopMin);
  }

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
      // we might not be able to find any childs
      if constexpr (kHasScore) {
        this->_scores.Next();
      }
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

    Finally next = [&] noexcept {
      if constexpr (kHasScore) {
        this->_scores.Next();
      }
    };

    if constexpr (kHasScore) {
      this->_scores.Collect();
    }

    while (child.advance() < parent) {
      if (!--count) {
        return 0;
      }

      if constexpr (kHasScore) {
        this->_scores.Collect();
      }
    }

    return count ? parent + 1 : 0;
  }

  ScoreFunction PrepareScore() noexcept {
    static_assert(kHasScore);

    return this->NestedScore(
      this,
      [](ScoreCtx* ctx) noexcept {
        auto& self = static_cast<JoinType&>(*ctx);
        auto& child = *self._child;
        const auto parent_doc = self.value();
        const auto* child_doc = self._child_doc;

        while (child_doc->value < parent_doc) {
          self._scores.Collect();
          if (!child.next()) {
            break;
          }
        }
        self._scores.Next();
      },
      ScoreFunction::NoopMin);
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
            if (doc_limits::eof(child->value())) {  // Match all parents
              if constexpr (MergeType != ScoreMergeType::Noop) {
                auto func = ScoreFunction::Constant(_none_boost);
                auto* score = irs::GetMutable<ScoreAttr>(parent.get());
                if (!score) [[unlikely]] {
                  return memory::make_managed<ScorerWrapper>(std::move(parent),
                                                             std::move(func));
                }
                *score = std::move(func);
              }
              return std::move(parent);
            }
          } else if constexpr (std::is_same_v<MinMatcher<MergeType>, M> ||
                               std::is_same_v<RangeMatcher<MergeType>, M>) {
            // Unordered case for the range [0..EOF] is the equivalent to
            // matching all parents
            if constexpr (MergeType == ScoreMergeType::Noop) {
              if (Match{0} == matcher.Range() &&
                  doc_limits::eof(child->value())) {
                return std::move(parent);
              }
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
