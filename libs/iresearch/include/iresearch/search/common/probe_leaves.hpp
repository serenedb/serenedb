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
#include <utility>
#include <vector>

#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/common/collect.hpp"
#include "iresearch/search/common/plan.hpp"
#include "iresearch/search/common/posting_probe.hpp"
#include "iresearch/search/common/resolve.hpp"
#include "iresearch/search/probe/impl.hpp"

namespace irs::search {

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

}  // namespace irs::search
