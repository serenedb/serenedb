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

#include <span>
#include <type_traits>
#include <utility>
#include <vector>

#include "iresearch/search/detail/collect.hpp"
#include "iresearch/search/detail/fill_posting_scored.hpp"
#include "iresearch/search/detail/plain_scored.hpp"
#include "iresearch/search/detail/plan.hpp"
#include "iresearch/search/detail/posting_count.hpp"
#include "iresearch/search/detail/posting_fill.hpp"
#include "iresearch/search/detail/posting_window_scored.hpp"
#include "iresearch/search/detail/resolve.hpp"
#include "iresearch/search/fill/impl.hpp"
#include "iresearch/search/fill/set_leaves.hpp"
#include "iresearch/search/scorers/score_args.hpp"
#include "iresearch/search/scorers/score_policy.hpp"
#include "iresearch/search/scorers/scorer.hpp"
#include "iresearch/utils/empty.hpp"

namespace irs::detail {

struct NoScoring {
  static constexpr const Scorer* scorer = nullptr;
  static constexpr score_t boost = kNoBoost;
  static constexpr Terms uniformity = Terms::Constant;
  static constexpr ScoreMergeType merge = ScoreMergeType::Noop;
};

struct TermScoring {
  const Scorer* scorer = nullptr;
  score_t boost = kNoBoost;
  Terms uniformity = Terms::Mixed;
  ScoreRecipe recipe;
  ScoreMergeType merge = ScoreMergeType::Noop;
};

template<typename Scoring>
inline constexpr bool kScores = std::is_same_v<Scoring, TermScoring>;

template<typename Scoring>
auto ScoreOf(const Scoring& scoring, score_t absorbed) noexcept {
  if constexpr (kScores<Scoring>) {
    return Scored{scoring.merge, absorbed};
  } else {
    return utils::Empty{};
  }
}

template<typename Result, typename Scoring, typename Make>
Result ResolveCountLeaves(const IndexInput& doc, const Scoring& scoring,
                          Make&& make) {
  if constexpr (kScores<Scoring>) {
    return ResolveCountScored<Result>(doc, scoring.uniformity >= Terms::Scored,
                                      scoring.merge, make);
  } else {
    return ResolveInput(doc, [&]<typename Input> -> Result {
      return make
        .template operator()<PostingCount<Input>, PostingCount<Input>>();
    });
  }
}

template<typename Term>
bool ScoresOf(const Term& term, const Scorer* scorer) noexcept {
  if constexpr (std::same_as<Term, PostingClause>) {
    return term.stats.stats != nullptr;
  } else {
    return scorer != nullptr && term.stats != nullptr;
  }
}

template<typename Term>
size_t ScoredCount(std::span<const Term> terms, const Scorer* scorer) noexcept {
  size_t count = 0;
  for (size_t i = 0; i != terms.size(); ++i) {
    count += static_cast<size_t>(ScoresOf(terms[i], scorer));
  }
  return count;
}

inline bool ScoresPerDocTerm(const PostingClause& posting) noexcept {
  SDB_ASSERT(posting.state.reader != nullptr);
  const auto& own = *posting.state.reader;
  return posting.state.cookie.docs_count != 1 &&
         posting.stats.stats != nullptr && DocOf(own) != nullptr &&
         FreqOf(own) && ScoresPerDoc(posting.stats.scorer);
}

inline constexpr auto kPlanFillDocs = [](const QueryBuilder& child) {
  return child.PlanFill({}, ScoreMergeType::Noop);
};

template<typename Term, typename PlanChild>
bool CollectDense(std::span<const Term> terms,
                  std::span<const QueryBuilder::ptr> filters,
                  const TermReader* field, const IndexInput*& doc,
                  std::vector<FillNode::ptr>& rest, PlanChild&& plan) {
  if (!terms.empty()) {
    doc = DocOf(FieldOf(terms.front(), field));
    if (doc == nullptr) {
      return false;
    }
  }
  rest.reserve(filters.size());
  for (const auto& child : filters) {
    SDB_ASSERT(child);
    SDB_ASSERT(child->Kind() != QueryKind::Empty);
    SDB_ASSERT(child->Kind() != QueryKind::All);
    auto node = plan(*child);
    if (!node) {
      return false;
    }
    rest.emplace_back(std::move(node));
  }
  return true;
}

template<typename Plain, typename Term>
void PreparePlain(Plain& leaf, const Term& term, const TermReader* field,
                  const IndexInput& doc) {
  const auto& own = FieldOf(term, field);
  const auto& meta = CookieOf(term);
  leaf.Prepare(meta, doc, LayoutOf(own), meta.docs_count != 1 && BoundsOf(own),
               meta.docs_count != 1 && FreqOf(own));
}

template<typename Leaf, typename Term>
void PrepareScored(Leaf& leaf, const Term& term, const TermReader* field,
                   const IndexInput& doc, const TermScoring& scoring) {
  const auto clause = ClauseOf(term, field, scoring.scorer, scoring.boost);
  SDB_ASSERT(clause.state.reader != nullptr);
  const auto& meta = clause.state.cookie;
  leaf.Prepare(meta, doc,
               meta.docs_count != 1 && BoundsOf(*clause.state.reader),
               *scoring.recipe.segment, *clause.state.reader,
               scoring.recipe.Args(clause.stats, clause.boost));
}

template<typename Result, typename Leaf, typename Plain, typename Term,
         typename Scoring, typename Make>
Result BuildWindowTerms(std::span<const Term> terms, const TermReader* field,
                        const IndexInput* input, const Scoring& scoring,
                        Make&& make) {
  SDB_ASSERT(!terms.empty());
  const auto& doc = *input;
  const auto plain = [&](Plain& leaf, size_t i) {
    PreparePlain(leaf, terms[i], field, doc);
  };
  if constexpr (!kScores<Scoring>) {
    return make.template operator()<fill::SetLeaves<Plain>>(terms.size(),
                                                            plain);
  } else {
    const auto scored = [&](Leaf& leaf, const Term& term) {
      PrepareScored(leaf, term, field, doc, scoring);
    };
    const auto scored_count = ScoredCount(terms, scoring.scorer);
    if (scored_count == terms.size()) {
      return make.template operator()<fill::SetLeaves<Leaf>>(
        terms.size(), [&](Leaf& leaf, size_t i) { scored(leaf, terms[i]); });
    }
    if (scored_count == 0) {
      return make.template operator()<fill::SetLeaves<Plain>>(terms.size(),
                                                              plain);
    }
    return make.template operator()<fill::MixedSetLeaves<Leaf, Plain>>(
      std::piecewise_construct,
      std::forward_as_tuple(scored_count,
                            [&, next = size_t{0}](Leaf& leaf, size_t) mutable {
                              while (!ScoresOf(terms[next], scoring.scorer)) {
                                ++next;
                              }
                              scored(leaf, terms[next++]);
                            }),
      std::forward_as_tuple(terms.size() - scored_count,
                            [&, next = size_t{0}](Plain& leaf, size_t) mutable {
                              while (ScoresOf(terms[next], scoring.scorer)) {
                                ++next;
                              }
                              PreparePlain(leaf, terms[next++], field, doc);
                            }));
  }
}

template<typename Result, typename Leaf, typename Plain, typename Term,
         typename Scoring, typename Make>
Result BuildWindowMix(std::span<const Term> terms, const TermReader* field,
                      const IndexInput* input, std::vector<FillNode::ptr>& rest,
                      const Scoring& scoring, Make&& make) {
  SDB_ASSERT(!terms.empty());
  if (rest.empty()) {
    return BuildWindowTerms<Result, Leaf, Plain>(terms, field, input, scoring,
                                                 make);
  }
  const auto& doc = *input;
  const auto count = terms.size();
  return make.template operator()<fill::SetLeaves<fill::Erased>>(
    count + rest.size(), [&](fill::Erased& leaf, size_t i) {
      if (i >= count) {
        leaf = fill::Erased{std::move(rest[i - count])};
        return;
      }
      const auto& own = FieldOf(terms[i], field);
      const auto& meta = CookieOf(terms[i]);
      const auto bounds = meta.docs_count != 1 && BoundsOf(own);
      if constexpr (kScores<Scoring>) {
        const auto clause =
          ClauseOf(terms[i], field, scoring.scorer, scoring.boost);
        if (clause.stats.stats != nullptr) {
          leaf = fill::Erased{memory::make_managed<fill::Impl<Leaf>>(
            meta, doc, bounds, *scoring.recipe.segment, own,
            scoring.recipe.Args(clause.stats, clause.boost))};
          return;
        }
      }
      leaf = fill::Erased{memory::make_managed<fill::Impl<Plain>>(
        meta, doc, LayoutOf(own), bounds, meta.docs_count != 1 && FreqOf(own))};
    });
}

inline fill::Node::ptr ScoredTermOf(const PostingClause& term,
                                    const ScoreRecipe& recipe,
                                    ScoreMergeType merge) {
  SDB_ASSERT(term.state.reader != nullptr);
  const auto& own = *term.state.reader;
  SDB_ASSERT(DocOf(own) != nullptr);
  const auto& doc = *DocOf(own);
  const auto& meta = term.state.cookie;
  const auto bounds = meta.docs_count != 1 && BoundsOf(own);
  if (term.stats.stats == nullptr) {
    return ResolveInput(doc, [&]<typename Input> -> fill::Node::ptr {
      return memory::make_managed<fill::Impl<PlainFillScored<Input>>>(
        meta, doc, LayoutOf(own), bounds, meta.docs_count != 1 && FreqOf(own));
    });
  }
  return ResolveFillScored<fill::Node::ptr>(
    doc, FreqOf(own) && ScoresPerDoc(term.stats.scorer), merge,
    [&]<typename Leaf, typename Plain> -> fill::Node::ptr {
      return memory::make_managed<fill::Impl<Leaf>>(
        meta, doc, bounds, *recipe.segment, own,
        recipe.Args(term.stats, term.boost));
    });
}

template<typename Result, typename Term, typename Scoring, typename Make>
Result BuildWindowSet(std::span<const Term> terms, const TermReader* field,
                      const IndexInput* doc, std::vector<FillNode::ptr>& rest,
                      const Scoring& scoring, Make&& make) {
  SDB_ASSERT(!terms.empty() || !rest.empty());
  if (terms.empty()) {
    return make.template operator()<fill::SetLeaves<fill::Erased>>(
      rest.size(), [&](fill::Erased& leaf, size_t i) {
        leaf = fill::Erased{std::move(rest[i])};
      });
  }
  if constexpr (!kScores<Scoring>) {
    return ResolveInput(*doc, [&]<typename Input> -> Result {
      using Leaf = PostingFill<Input>;
      return BuildWindowMix<Result, Leaf, Leaf>(terms, field, doc, rest,
                                                scoring, make);
    });
  } else {
    if (scoring.uniformity != Terms::Mixed) {
      return ResolveFillScored<Result>(
        *doc, scoring.uniformity >= Terms::Scored, scoring.merge,
        [&]<typename Leaf, typename Plain> -> Result {
          return BuildWindowMix<Result, Leaf, Plain>(terms, field, doc, rest,
                                                     scoring, make);
        });
    }
    std::vector<FillNode::ptr> leaves;
    leaves.reserve(terms.size() + rest.size());
    for (size_t i = 0; i != terms.size(); ++i) {
      auto node =
        ScoredTermOf(ClauseOf(terms[i], field, scoring.scorer, scoring.boost),
                     scoring.recipe, scoring.merge);
      if (!node) {
        return {};
      }
      leaves.emplace_back(std::move(node));
    }
    for (auto& node : rest) {
      leaves.emplace_back(std::move(node));
    }
    return make.template operator()<fill::SetLeaves<fill::Erased>>(
      leaves.size(), [&](fill::Erased& leaf, size_t i) {
        leaf = fill::Erased{std::move(leaves[i])};
      });
  }
}

}  // namespace irs::detail
