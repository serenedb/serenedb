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
#include <utility>
#include <vector>

#include "iresearch/search/common/collect.hpp"
#include "iresearch/search/common/fill_posting_scored.hpp"
#include "iresearch/search/common/plain_scored.hpp"
#include "iresearch/search/common/plan.hpp"
#include "iresearch/search/common/posting_count_scored.hpp"
#include "iresearch/search/common/resolve.hpp"
#include "iresearch/search/fill/impl.hpp"
#include "iresearch/search/fill/posting_scored.hpp"
#include "iresearch/search/fill/set_leaves.hpp"
#include "iresearch/search/lead/posting_scored.hpp"
#include "iresearch/search/probe/posting_scored.hpp"

namespace irs::search {

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

template<typename Term, typename PlanChild>
bool CollectDenseScored(std::span<const Term> terms,
                        std::span<const QueryBuilder::ptr> filters,
                        const TermReader* field, const IndexInput*& doc,
                        std::vector<fill::Node::ptr>& rest, PlanChild&& plan) {
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

template<typename Result, typename Make>
Result BuildScoredErased(std::vector<fill::Node::ptr>& rest, Make&& make) {
  return make.template operator()<fill::SetLeaves<fill::Erased>>(
    rest.size(), [&](fill::Erased& leaf, size_t i) {
      leaf = fill::Erased{std::move(rest[i])};
    });
}

template<typename Result, typename Leaf, typename Plain, typename Term,
         typename Make>
Result BuildScoredTerms(std::span<const Term> terms, const TermReader* field,
                        const Scorer* scorer, score_t boost,
                        const IndexInput* input, const ScoreRecipe& recipe,
                        Make&& make) {
  SDB_ASSERT(!terms.empty());
  const auto& doc = *input;
  const auto scored_count = ScoredCount(terms, scorer);

  const auto scored = [&](Leaf& leaf, const Term& term) {
    const auto clause = ClauseOf(term, field, scorer, boost);
    SDB_ASSERT(clause.state.reader != nullptr);
    const auto& meta = clause.state.cookie;
    leaf.Prepare(meta, doc,
                 meta.docs_count != 1 && BoundsOf(*clause.state.reader),
                 *recipe.segment, *clause.state.reader,
                 recipe.Args(clause.stats, clause.boost));
  };
  const auto plain = [&](Plain& leaf, const Term& term) {
    const auto& own = FieldOf(term, field);
    const auto& meta = CookieOf(term);
    leaf.Prepare(meta, doc, meta.docs_count != 1 && BoundsOf(own),
                 meta.docs_count != 1 && FreqOf(own));
  };

  if (scored_count == terms.size()) {
    return make.template operator()<fill::SetLeaves<Leaf>>(
      terms.size(), [&](Leaf& leaf, size_t i) { scored(leaf, terms[i]); });
  }
  if (scored_count == 0) {
    return make.template operator()<fill::SetLeaves<Plain>>(
      terms.size(), [&](Plain& leaf, size_t i) { plain(leaf, terms[i]); });
  }
  return make.template operator()<fill::MixedSetLeaves<Leaf, Plain>>(
    std::piecewise_construct,
    std::forward_as_tuple(scored_count,
                          [&, next = size_t{0}](Leaf& leaf, size_t) mutable {
                            while (!ScoresOf(terms[next], scorer)) {
                              ++next;
                            }
                            scored(leaf, terms[next++]);
                          }),
    std::forward_as_tuple(terms.size() - scored_count,
                          [&, next = size_t{0}](Plain& leaf, size_t) mutable {
                            while (ScoresOf(terms[next], scorer)) {
                              ++next;
                            }
                            plain(leaf, terms[next++]);
                          }));
}

template<typename Result, typename Leaf, typename Plain, typename Term,
         typename Make>
Result BuildScoredSet(std::span<const Term> terms, const TermReader* field,
                      const Scorer* scorer, score_t boost,
                      const IndexInput* input,
                      std::vector<fill::Node::ptr>& rest,
                      const ScoreRecipe& recipe, Make&& make) {
  SDB_ASSERT(!terms.empty());
  if (rest.empty()) {
    return BuildScoredTerms<Result, Leaf, Plain, Term>(
      terms, field, scorer, boost, input, recipe, make);
  }
  const auto& doc = *input;
  const auto count = terms.size();
  return make.template operator()<fill::SetLeaves<fill::Erased>>(
    count + rest.size(), [&](fill::Erased& leaf, size_t i) {
      if (i >= count) {
        leaf = fill::Erased{std::move(rest[i - count])};
        return;
      }
      const auto clause = ClauseOf(terms[i], field, scorer, boost);
      const auto& meta = clause.state.cookie;
      const auto& own = *clause.state.reader;
      const auto bounds = meta.docs_count != 1 && BoundsOf(own);
      if (clause.stats.stats != nullptr) {
        leaf = fill::Erased{memory::make_managed<fill::Impl<Leaf>>(
          meta, doc, bounds, *recipe.segment, own,
          recipe.Args(clause.stats, clause.boost))};
        return;
      }
      leaf = fill::Erased{memory::make_managed<fill::Impl<Plain>>(
        meta, doc, bounds, meta.docs_count != 1 && FreqOf(own))};
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
        meta, doc, bounds, meta.docs_count != 1 && FreqOf(own));
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

template<typename Result, typename Term, typename Make>
Result BuildScoredWindow(std::span<const Term> terms, const TermReader* field,
                         const Scorer* scorer, score_t boost,
                         const IndexInput* doc,
                         std::vector<fill::Node::ptr>& rest, Terms uniformity,
                         const ScoreRecipe& recipe, ScoreMergeType merge,
                         Make&& make) {
  if (terms.empty()) {
    return BuildScoredErased<Result>(rest, make);
  }
  if (uniformity != Terms::Mixed) {
    return ResolveFillScored<Result>(
      *doc, uniformity >= Terms::Scored, merge,
      [&]<typename Leaf, typename Plain> -> Result {
        return BuildScoredSet<Result, Leaf, Plain, Term>(
          terms, field, scorer, boost, doc, rest, recipe, make);
      });
  }
  std::vector<fill::Node::ptr> leaves;
  leaves.reserve(terms.size() + rest.size());
  for (size_t i = 0; i != terms.size(); ++i) {
    auto node =
      ScoredTermOf(ClauseOf(terms[i], field, scorer, boost), recipe, merge);
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

}  // namespace irs::search
