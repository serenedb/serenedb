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

#include "iresearch/search/common/plan.hpp"
#include "iresearch/search/common/posting_fill.hpp"
#include "iresearch/search/common/posting_probe.hpp"
#include "iresearch/search/common/resolve.hpp"
#include "iresearch/search/fill/impl.hpp"
#include "iresearch/search/fill/make.hpp"
#include "iresearch/search/fill/set_leaves.hpp"
#include "iresearch/search/lead/impl.hpp"
#include "iresearch/search/lead/make.hpp"
#include "iresearch/search/lead/posting_docs.hpp"
#include "iresearch/search/probe/impl.hpp"
#include "iresearch/search/probe/make.hpp"
#include "iresearch/search/probe/sparse_conjunction_docs.hpp"

namespace irs::search {

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

template<typename Term>
uint64_t IncludeCandidates(std::span<const Term> terms,
                           std::span<const QueryBuilder::ptr> filters,
                           const SubReader& segment) noexcept {
  uint64_t candidates = segment.docs_count();
  if (!terms.empty()) {
    candidates =
      std::min(candidates, uint64_t{CookieOf(terms.front()).docs_count});
  }
  if (!filters.empty()) {
    SDB_ASSERT(filters.front());
    candidates = std::min(candidates, uint64_t{filters.front()->EstimateMax()});
  }
  return candidates;
}

template<typename Term>
bool CollectDense(std::span<const Term> terms,
                  std::span<const QueryBuilder::ptr> filters,
                  const TermReader* field, const IndexInput*& doc,
                  std::vector<FillNode::ptr>& rest) {
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
    auto node = child->PlanFill({}, ScoreMergeType::Noop);
    if (!node) {
      return false;
    }
    rest.emplace_back(std::move(node));
  }
  return true;
}

template<typename Result, typename Term, typename Make>
Result BuildDense(std::span<const Term> terms, const TermReader* field,
                  const IndexInput* input, std::vector<FillNode::ptr>& rest,
                  Make&& make) {
  SDB_ASSERT(!terms.empty() || !rest.empty());
  if (terms.empty()) {
    return make.template operator()<fill::SetLeaves<fill::Erased>>(
      rest.size(), [&](fill::Erased& leaf, size_t i) {
        leaf = fill::Erased{std::move(rest[i])};
      });
  }
  const auto& doc = *input;
  return ResolveInput(doc, [&]<typename Input> -> Result {
    using Leaf = PostingFill<Input>;
    if (rest.empty()) {
      return make.template operator()<fill::SetLeaves<Leaf>>(
        terms.size(), [&](Leaf& leaf, size_t i) {
          const auto& own = FieldOf(terms[i], field);
          const auto& meta = CookieOf(terms[i]);
          leaf.Prepare(meta, doc, meta.docs_count != 1 && BoundsOf(own),
                       meta.docs_count != 1 && FreqOf(own));
        });
    }
    const auto count = terms.size();
    return make.template operator()<fill::SetLeaves<fill::Erased>>(
      count + rest.size(), [&](fill::Erased& leaf, size_t i) {
        if (i < count) {
          const auto& own = FieldOf(terms[i], field);
          const auto& meta = CookieOf(terms[i]);
          leaf = fill::Erased{memory::make_managed<fill::Impl<Leaf>>(
            meta, doc, meta.docs_count != 1 && BoundsOf(own),
            meta.docs_count != 1 && FreqOf(own))};
          return;
        }
        leaf = fill::Erased{std::move(rest[i - count])};
      });
  });
}

template<typename Result, typename Term, bool Probed = false, typename Make>
Result BuildConjunction(std::span<const Term> terms,
                        std::span<const QueryBuilder::ptr> filters,
                        const TermReader* field, const SubReader& segment,
                        uint64_t interrogations, Make&& make) {
  SDB_ASSERT(terms.size() + filters.size() > 1);
  const bool head_term = HeadIsTerm(terms, filters);
  const auto rest = head_term ? terms.subspan(1) : terms;
  const auto rest_filters = head_term ? filters : filters.subspan(1);
  const auto rest_size = rest.size() + rest_filters.size();
  const uint64_t lead_estimate = head_term ? CookieOf(terms.front()).docs_count
                                           : filters.front()->EstimateMax();
  const uint64_t reach =
    Probed ? std::min<uint64_t>(interrogations, lead_estimate) : lead_estimate;

  const bool concrete = rest_size != 0 && rest_filters.empty();

  const auto build_concrete =
    [&]<typename Input, typename Head>(auto&& head) -> Result {
    using Probe = PostingProbe<Input>;
    return ResolveArity<kTailArity, kTailFloor>(
      rest.size(), [&]<size_t N> -> Result {
        if constexpr (N == 1) {
          const auto& one = rest.front();
          const auto& own = FieldOf(one, field);
          return make.template operator()<Head, Probe>(
            std::forward<decltype(head)>(head),
            std::forward_as_tuple(CookieOf(one), *DocOf(own), LayoutOf(own),
                                  BoundsOf(own)));
        } else if constexpr (N != 0) {
          using Tail = probe::SparseConjunctionDocs<Probe, N>;
          return [&]<size_t... I>(std::index_sequence<I...>) {
            return make.template operator()<Head, Tail>(
              std::forward<decltype(head)>(head),
              std::forward_as_tuple(
                std::piecewise_construct,
                std::forward_as_tuple(CookieOf(rest[I]),
                                      *DocOf(FieldOf(rest[I], field)),
                                      LayoutOf(FieldOf(rest[I], field)),
                                      BoundsOf(FieldOf(rest[I], field)))...));
          }(std::make_index_sequence<N>{});
        } else {
          return make
            .template operator()<Head, probe::SparseConjunctionDocs<Probe>>(
              std::forward<decltype(head)>(head),
              std::forward_as_tuple(rest.size(), [&](Probe& probe, size_t i) {
                const auto& own = FieldOf(rest[i], field);
                probe.Prepare(CookieOf(rest[i]), *DocOf(own), LayoutOf(own),
                              BoundsOf(own));
              }));
        }
      });
  };

  const auto erased = [&]<typename Head>(auto&& head) -> Result {
    std::vector<ProbeNode::ptr> probes;
    probes.reserve(rest_size);
    const auto take = [&](ProbeNode::ptr node) {
      if (!node) {
        return false;
      }
      probes.emplace_back(std::move(node));
      return true;
    };
    if (!VisitOrderedOf(
          rest, rest_filters, true, 0, std::numeric_limits<size_t>::max(),
          [&](const Term& term) {
            return take(
              ProbeOf(ClauseOf(term, field), nullptr, segment, reach));
          },
          [&](const QueryBuilder& child) {
            return take(child.PlanProbe({}, reach));
          })) {
      return {};
    }
    return ResolveArity<kTailArity, kTailFloor>(
      probes.size(), [&]<size_t N> -> Result {
        if constexpr (N == 1) {
          return make.template operator()<Head, probe::Erased>(
            std::forward<decltype(head)>(head),
            std::forward_as_tuple(std::move(probes.front())));
        } else if constexpr (N != 0) {
          using Tail = probe::SparseConjunctionDocs<probe::Erased, N>;
          return [&]<size_t... I>(std::index_sequence<I...>) {
            return make.template operator()<Head, Tail>(
              std::forward<decltype(head)>(head),
              std::forward_as_tuple(
                std::piecewise_construct,
                std::forward_as_tuple(std::move(probes[I]))...));
          }(std::make_index_sequence<N>{});
        } else {
          using Tail = probe::SparseConjunctionDocs<probe::Erased>;
          return make.template operator()<Head, Tail>(
            std::forward<decltype(head)>(head),
            std::forward_as_tuple(probes.size(),
                                  [&](probe::Erased& leaf, size_t i) {
                                    leaf = probe::Erased{std::move(probes[i])};
                                  }));
        }
      });
  };

  const auto build = [&]<typename Input, typename Head>(auto&& head) -> Result {
    if (!concrete) {
      return erased.template operator()<Head>(
        std::forward<decltype(head)>(head));
    }
    return build_concrete.template operator()<Input, Head>(
      std::forward<decltype(head)>(head));
  };

  const auto build_erased_lead = [&]<typename Head>(auto&& head) -> Result {
    if (!concrete) {
      return erased.template operator()<Head>(
        std::forward<decltype(head)>(head));
    }
    return ResolveInput(
      *DocOf(FieldOf(rest.front(), field)), [&]<typename Input> -> Result {
        return build_concrete.template operator()<Input, Head>(
          std::forward<decltype(head)>(head));
      });
  };

  if constexpr (Probed) {
    auto node = head_term ? ProbeOf(ClauseOf(terms.front(), field), nullptr,
                                    segment, interrogations)
                          : filters.front()->PlanProbe({}, interrogations);
    if (!node) {
      return {};
    }
    return build_erased_lead.template operator()<probe::Erased>(
      std::forward_as_tuple(std::move(node)));
  } else if (head_term) {
    const auto& own = FieldOf(terms.front(), field);
    return ResolveInput(*DocOf(own), [&]<typename Input> -> Result {
      using Head = PostingLead<Input>;
      return build.template operator()<Input, Head>(std::forward_as_tuple(
        CookieOf(terms.front()), *DocOf(own), LayoutOf(own), BoundsOf(own)));
    });
  } else {
    auto node = filters.front()->PlanLead({});
    if (!node) {
      return {};
    }
    return build_erased_lead.template operator()<lead::Erased>(
      std::forward_as_tuple(std::move(node)));
  }
}

}  // namespace irs::search
