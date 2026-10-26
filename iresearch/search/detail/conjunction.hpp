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

#include <absl/algorithm/container.h>

#include <limits>
#include <span>
#include <tuple>
#include <utility>
#include <vector>

#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/detail/collect.hpp"
#include "iresearch/search/detail/posting_probe.hpp"
#include "iresearch/search/detail/resolve.hpp"
#include "iresearch/search/detail/window_set.hpp"
#include "iresearch/search/lead/impl.hpp"
#include "iresearch/search/lead/posting_docs.hpp"
#include "iresearch/search/lead/posting_scored.hpp"
#include "iresearch/search/probe/impl.hpp"
#include "iresearch/search/probe/leaves.hpp"
#include "iresearch/search/probe/posting_scored.hpp"
#include "iresearch/search/scorers/score_args.hpp"

namespace irs::detail {

template<typename Result, typename Term, typename Scoring, typename ProbeClause,
         typename LeadClause, typename Make>
Result BuildConjunction(std::span<const Term> terms,
                        std::span<const QueryBuilder::ptr> filters,
                        const TermReader* field, const SubReader& segment,
                        const Scoring& scoring, ProbeClause&& probe_clause,
                        LeadClause&& lead_clause, Make&& make) {
  constexpr bool kScored = kScores<Scoring>;
  SDB_ASSERT(!terms.empty() || !filters.empty());
  const bool head_term = HeadIsTerm(terms, filters);
  const auto rest = head_term ? terms.subspan(1) : terms;
  const auto rest_filters = head_term ? filters : filters.subspan(1);
  const auto rest_size = rest.size() + rest_filters.size();
  const uint64_t reach =
    kScored ? HeadCandidates(terms, filters) : HeadEstimate(terms, filters);
  const auto clause = [&](const Term& term) {
    return ClauseOf(term, field, scoring.scorer, scoring.boost);
  };
  const auto probe_args = [&](const Term& term) {
    const auto& own = FieldOf(term, field);
    if constexpr (kScored) {
      const auto posting = clause(term);
      return std::tuple<const PostingMeta&, const IndexInput&, const SubReader&,
                        const TermReader&, ScoreArgs>{
        CookieOf(term), *DocOf(own), segment, own,
        scoring.recipe.Args(posting.stats, posting.boost)};
    } else {
      return std::tuple<const PostingMeta&, const IndexInput&, IndexFeatures,
                        bool>{CookieOf(term), *DocOf(own), LayoutOf(own),
                              BoundsOf(own)};
    }
  };

  const bool concrete =
    rest_size != 0 && rest_filters.empty() &&
    (!kScored || absl::c_all_of(rest, [&](const Term& term) {
      const auto one = clause(term);
      return one.stats.stats != nullptr && FreqOf(*one.state.reader) &&
             ScoresPerDoc(one.stats.scorer);
    }));

  const auto build_concrete =
    [&]<typename Input, typename Head>(auto&& head) -> Result {
    using Probe = std::conditional_t<kScored, PostingProbeScored<Input>,
                                     PostingProbe<Input>>;
    return ResolveArity<kTailArity, kTailFloor>(
      rest.size(), [&]<size_t N> -> Result {
        if constexpr (N == 1) {
          return make.template operator()<Head, Probe>(
            std::forward<decltype(head)>(head), probe_args(rest.front()));
        } else if constexpr (N != 0) {
          return [&]<size_t... I>(std::index_sequence<I...>) {
            return make.template operator()<Head, probe::AndLeaves<Probe, N>>(
              std::forward<decltype(head)>(head),
              std::forward_as_tuple(std::piecewise_construct,
                                    probe_args(rest[I])...));
          }(std::make_index_sequence<N>{});
        } else {
          return make.template operator()<Head, probe::AndLeaves<Probe>>(
            std::forward<decltype(head)>(head),
            std::forward_as_tuple(rest.size(), [&](Probe& probe, size_t i) {
              std::apply([&](const auto&... args) { probe.Prepare(args...); },
                         probe_args(rest[i]));
            }));
        }
      });
  };

  const auto build_erased = [&]<typename Head>(auto&& head) -> Result {
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
            return take(probe_clause(clause(term), nullptr, reach));
          },
          [&](const QueryBuilder& child) {
            return take(probe_clause(
              PostingClause{TermState{nullptr, PostingMeta{}}}, &child, reach));
          })) {
      return {};
    }
    const auto dynamic = [&] -> Result {
      return make.template operator()<Head, probe::AndLeaves<probe::Erased>>(
        std::forward<decltype(head)>(head),
        std::forward_as_tuple(probes.size(),
                              [&](probe::Erased& leaf, size_t i) {
                                leaf = probe::Erased{std::move(probes[i])};
                              }));
    };
    if constexpr (kScored) {
      if (probes.size() != 1) {
        return dynamic();
      }
      return make.template operator()<Head, probe::Erased>(
        std::forward<decltype(head)>(head),
        std::forward_as_tuple(std::move(probes.front())));
    } else {
      return ResolveArity<kTailArity, kTailFloor>(
        probes.size(), [&]<size_t N> -> Result {
          if constexpr (N == 1) {
            return make.template operator()<Head, probe::Erased>(
              std::forward<decltype(head)>(head),
              std::forward_as_tuple(std::move(probes.front())));
          } else if constexpr (N != 0) {
            return [&]<size_t... I>(std::index_sequence<I...>) {
              return make
                .template operator()<Head, probe::AndLeaves<probe::Erased, N>>(
                  std::forward<decltype(head)>(head),
                  std::forward_as_tuple(
                    std::piecewise_construct,
                    std::forward_as_tuple(std::move(probes[I]))...));
            }(std::make_index_sequence<N>{});
          } else {
            return dynamic();
          }
        });
    }
  };

  const auto build_opaque = [&]<typename Head>(auto&& head) -> Result {
    if (rest_size == 0) {
      return make.template operator()<Head, probe::NoLeaves>(
        std::forward<decltype(head)>(head), std::forward_as_tuple());
    }
    return build_erased.template operator()<Head>(
      std::forward<decltype(head)>(head));
  };

  const auto build = [&]<typename Input, typename Head>(auto&& head) -> Result {
    if (!concrete) {
      return build_opaque.template operator()<Head>(
        std::forward<decltype(head)>(head));
    }
    return build_concrete.template operator()<Input, Head>(
      std::forward<decltype(head)>(head));
  };

  const auto build_erased_lead = [&](LeadNode::ptr node) -> Result {
    if (!node) {
      return {};
    }
    if (!concrete) {
      return build_opaque.template operator()<lead::Erased>(
        std::forward_as_tuple(std::move(node)));
    }
    return ResolveInput(
      *DocOf(FieldOf(rest.front(), field)), [&]<typename Input> -> Result {
        return build_concrete.template operator()<Input, lead::Erased>(
          std::forward_as_tuple(std::move(node)));
      });
  };

  if (!head_term) {
    return build_erased_lead(lead_clause(*filters.front()));
  }
  const auto& own = FieldOf(terms.front(), field);
  if constexpr (kScored) {
    const auto posting = clause(terms.front());
    if (!FreqOf(own) || !ScoresPerDoc(posting.stats.scorer)) {
      return build_erased_lead(
        lead::MakePostingScored(posting, segment, scoring.recipe));
    }
    return ResolveInput(*DocOf(own), [&]<typename Input> -> Result {
      return build.template operator()<Input, PostingLeadScored<Input>>(
        std::forward_as_tuple(
          CookieOf(terms.front()), *DocOf(own), segment, own,
          scoring.recipe.Args(posting.stats, posting.boost)));
    });
  } else {
    return ResolveInput(*DocOf(own), [&]<typename Input> -> Result {
      return build.template operator()<Input, PostingLead<Input>>(
        std::forward_as_tuple(CookieOf(terms.front()), *DocOf(own),
                              LayoutOf(own), BoundsOf(own)));
    });
  }
}

}  // namespace irs::detail
