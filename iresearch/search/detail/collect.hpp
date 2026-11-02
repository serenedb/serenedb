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

#include <algorithm>
#include <limits>
#include <span>
#include <tuple>
#include <utility>
#include <vector>

#include "iresearch/search/detail/plan.hpp"
#include "iresearch/search/detail/posting_fill.hpp"
#include "iresearch/search/detail/posting_probe.hpp"
#include "iresearch/search/detail/resolve.hpp"
#include "iresearch/search/fill/impl.hpp"
#include "iresearch/search/fill/make.hpp"
#include "iresearch/search/fill/set_leaves.hpp"
#include "iresearch/search/lead/impl.hpp"
#include "iresearch/search/lead/make.hpp"
#include "iresearch/search/lead/posting_docs.hpp"
#include "iresearch/search/probe/impl.hpp"
#include "iresearch/search/probe/leaves.hpp"
#include "iresearch/search/probe/make.hpp"

namespace irs::detail {

inline LeadNode::ptr LeadOf(const PostingClause& posting,
                            const QueryBuilder* child,
                            const SubReader& segment) {
  return child == nullptr ? lead::MakePostingDocs(posting, segment)
                          : child->PlanLead({});
}

inline ProbeNode::ptr ProbeOf(const PostingClause& posting,
                              const QueryBuilder* child,
                              const SubReader& segment,
                              uint64_t interrogations) {
  return child == nullptr ? probe::MakePostingDocs(posting, segment)
                          : child->PlanProbe({}, interrogations);
}

inline FillNode::ptr FillOf(const PostingClause& posting,
                            const QueryBuilder* child,
                            const SubReader& segment) {
  return child == nullptr ? fill::MakePostingDocs(posting, segment)
                          : child->PlanFill({}, ScoreMergeType::Noop);
}

inline auto ProbeClauseOf(const SubReader& segment) noexcept {
  return [&segment](const PostingClause& posting, const QueryBuilder* child,
                    uint64_t reach) {
    return ProbeOf(posting, child, segment, reach);
  };
}

inline constexpr auto kPlanLeadDocs = [](const QueryBuilder& child) {
  return child.PlanLead({});
};

template<typename Term>
bool CollectFills(std::span<const Term> terms,
                  std::span<const QueryBuilder::ptr> filters,
                  const TermReader* field, const SubReader& segment,
                  std::vector<FillNode::ptr>& nodes) {
  nodes.reserve(nodes.size() + terms.size() + filters.size());
  const auto take = [&](FillNode::ptr node) {
    if (!node) {
      return false;
    }
    nodes.emplace_back(std::move(node));
    return true;
  };
  return VisitOrderedOf(
    terms, filters, false, 0, std::numeric_limits<size_t>::max(),
    [&](const Term& term) {
      return take(FillOf(ClauseOf(term, field), nullptr, segment));
    },
    [&](const QueryBuilder& child) {
      return take(child.PlanFill({}, ScoreMergeType::Noop));
    });
}

template<typename Term>
uint64_t IncludeCandidates(std::span<const Term> terms,
                           std::span<const QueryBuilder::ptr> filters,
                           const SubReader& segment) noexcept {
  const auto docs_count = static_cast<doc_id_t>(segment.docs_count());
  uint64_t candidates = docs_count;
  if (!terms.empty()) {
    candidates =
      std::min(candidates, uint64_t{CookieOf(terms.front()).docs_count});
  }
  if (!filters.empty()) {
    SDB_ASSERT(filters.front());
    candidates = std::min(candidates, ChildCandidates(*filters.front()));
  }
  return candidates;
}

template<typename Term>
uint64_t LeadCandidates(std::span<const Term> terms,
                        std::span<const QueryBuilder::ptr> filters,
                        doc_id_t docs_count) noexcept {
  uint64_t candidates = 0;
  for (size_t i = 0; i != terms.size(); ++i) {
    candidates += CookieOf(terms[i]).docs_count;
  }
  for (const auto& filter : filters) {
    SDB_ASSERT(filter);
    candidates += ChildCandidates(*filter);
  }
  return candidates;
}

template<typename Term>
bool DenseConjunction(std::span<const Term> terms,
                      doc_id_t segment_docs_count) noexcept {
  return terms.size() >= 2 && CookieOf(terms.front()).docs_count >=
                                segment_docs_count / kDensityThresholdInverse;
}

template<typename Term>
bool WindowTerms(std::span<const Term> terms,
                 std::span<const QueryBuilder::ptr> filters,
                 const TermReader* field, const IndexInput*& doc) {
  if (terms.empty() || !filters.empty()) {
    return false;
  }
  doc = DocOf(FieldOf(terms.front(), field));
  return doc != nullptr;
}

}  // namespace irs::detail
