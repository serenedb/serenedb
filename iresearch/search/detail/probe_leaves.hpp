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

#include <limits>
#include <span>
#include <type_traits>
#include <utility>
#include <vector>

#include "iresearch/utils/memory.hpp"
#include "iresearch/utils/shared.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/detail/collect.hpp"
#include "iresearch/search/detail/plan.hpp"
#include "iresearch/search/detail/posting_probe.hpp"
#include "iresearch/search/detail/resolve.hpp"
#include "iresearch/search/probe/impl.hpp"
#include "iresearch/search/probe/make.hpp"
#include "iresearch/search/probe/posting_scored.hpp"
#include "iresearch/search/queries/term_state.hpp"
#include "iresearch/search/scorers/score_args.hpp"

namespace irs::detail {

enum class ProbeOrder : uint8_t {
  Narrowest,
  Densest,
};

template<typename Term, typename Visit>
bool VisitClauses(std::span<const Term> terms,
                  std::span<const QueryBuilder::ptr> filters,
                  const TermReader* field, const Scorer* scorer, score_t boost,
                  ProbeOrder order, Visit&& visit) {
  return VisitOrderedOf(
    terms, filters, order != ProbeOrder::Densest, 0,
    std::numeric_limits<size_t>::max(),
    [&](const Term& term) {
      return visit(ClauseOf(term, field, scorer, boost), nullptr);
    },
    [&](const QueryBuilder& child) {
      return visit(PostingClause{TermState{nullptr, PostingMeta{}}}, &child);
    });
}

template<typename Term>
bool ConcreteClauses(std::span<const Term> terms,
                     std::span<const QueryBuilder::ptr> filters,
                     const TermReader* field, const IndexInput*& doc) {
  if (!filters.empty() || terms.empty()) {
    return false;
  }
  doc = DocOf(FieldOf(terms.front(), field));
  return doc != nullptr;
}

template<typename Result, typename Term, typename Make>
Result BuildProbeLeaves(std::span<const Term> terms,
                        std::span<const QueryBuilder::ptr> filters,
                        const TermReader* field, const SubReader& segment,
                        uint64_t interrogations, ProbeOrder order,
                        Make&& make) {
  SDB_ASSERT(!terms.empty() || !filters.empty());
  const IndexInput* doc = nullptr;
  if (ConcreteClauses(terms, filters, field, doc)) {
    return ResolveInput(*doc, [&]<typename Input> -> Result {
      using Leaf = PostingProbe<Input>;
      return make.template operator()<Leaf>(
        terms.size(), [&](Leaf& leaf, size_t i) {
          const auto& own = FieldOf(terms[i], field);
          leaf.Prepare(CookieOf(terms[i]), *DocOf(own), LayoutOf(own),
                       BoundsOf(own));
        });
    });
  }
  std::vector<probe::Erased> leaves;
  leaves.reserve(terms.size() + filters.size());
  const auto ask = [&](const PostingClause& posting,
                       const QueryBuilder* child) noexcept {
    auto node = ProbeOf(posting, child, segment, interrogations);
    if (!node) {
      return false;
    }
    leaves.emplace_back(std::move(node));
    return true;
  };
  if (!VisitClauses(terms, filters, field, nullptr, kNoBoost, order, ask)) {
    return {};
  }
  return make.template operator()<probe::Erased>(
    leaves.size(),
    [&](probe::Erased& leaf, size_t i) { leaf = std::move(leaves[i]); });
}

template<typename Term>
bool ConcreteOptionals(std::span<const Term> terms,
                       std::span<const QueryBuilder::ptr> filters,
                       Terms uniformity, const TermReader* field,
                       const Scorer* scorer, const IndexInput*& doc) {
  if (!filters.empty() || terms.empty() || uniformity < Terms::Scored) {
    return false;
  }
  for (size_t i = 0; i != terms.size(); ++i) {
    SDB_ASSERT(CookieOf(terms[i]).docs_count != 0);
    if (ClauseOf(terms[i], field, scorer).stats.stats == nullptr) {
      return false;
    }
  }
  doc = DocOf(FieldOf(terms.front(), field));
  return doc != nullptr;
}

template<typename Result, typename Term, typename ProbeClause, typename Make>
Result BuildOptionalLeaves(std::span<const Term> terms,
                           std::span<const QueryBuilder::ptr> filters,
                           Terms uniformity, const TermReader* field,
                           const Scorer* scorer, score_t boost,
                           const SubReader& segment, const ScoreRecipe& recipe,
                           uint64_t interrogations, ProbeClause&& clause,
                           Make&& make,
                           ProbeOrder order = ProbeOrder::Densest) {
  SDB_ASSERT(!terms.empty() || !filters.empty());
  const IndexInput* doc = nullptr;
  if (ConcreteOptionals(terms, filters, uniformity, field, scorer, doc)) {
    return ResolveInput(*doc, [&]<typename Input> -> Result {
      using Leaf = PostingProbeScored<Input>;
      return make.template operator()<Leaf>(terms.size(), [&](Leaf& leaf,
                                                              size_t i) {
        const auto posting = ClauseOf(terms[i], field, scorer, boost);
        leaf.Prepare(posting.state.cookie, *doc, segment, *posting.state.reader,
                     recipe.Args(posting.stats, posting.boost));
      });
    });
  }
  std::vector<probe::Erased> leaves;
  leaves.reserve(terms.size() + filters.size());
  const auto ask = [&](const PostingClause& posting,
                       const QueryBuilder* child) noexcept {
    auto node = clause(posting, child, interrogations);
    if (!node) {
      return false;
    }
    leaves.emplace_back(std::move(node));
    return true;
  };
  if (!VisitClauses(terms, filters, field, scorer, boost, order, ask)) {
    return {};
  }
  return make.template operator()<probe::Erased>(
    leaves.size(),
    [&](probe::Erased& leaf, size_t i) { leaf = std::move(leaves[i]); });
}

}  // namespace irs::detail
