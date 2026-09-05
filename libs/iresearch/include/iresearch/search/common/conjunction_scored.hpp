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
#include <array>
#include <limits>
#include <span>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/common/collect.hpp"
#include "iresearch/search/common/resolve.hpp"
#include "iresearch/search/common/score_args.hpp"
#include "iresearch/search/lead/impl.hpp"
#include "iresearch/search/lead/posting_scored.hpp"
#include "iresearch/search/probe/impl.hpp"
#include "iresearch/search/probe/posting_scored.hpp"
#include "iresearch/search/probe/sparse_conjunction_docs.hpp"
namespace irs::search {

template<typename Result, typename Term, typename MakeProbeClause,
         typename MakeLeadClause, typename Make>
Result BuildScoredConjunction(std::span<const Term> terms,
                              std::span<const QueryBuilder::ptr> filters,
                              const TermReader* field, const Scorer* scorer,
                              score_t boost, const SubReader& segment,
                              const ScoreRecipe& recipe,
                              MakeProbeClause&& probe_clause,
                              MakeLeadClause&& lead_clause, Make&& make) {
  SDB_ASSERT(!terms.empty() || !filters.empty());
  const bool head_term = HeadIsTerm(terms, filters);
  const auto rest = head_term ? terms.subspan(1) : terms;
  const auto rest_filters = head_term ? filters : filters.subspan(1);
  const auto rest_size = rest.size() + rest_filters.size();
  const uint64_t reach = HeadEstimate(terms, filters);
  const auto clause = [&](const Term& term) {
    return ClauseOf(term, field, scorer, boost);
  };
  const auto args = [&](const PostingClause& posting) {
    return recipe.Args(posting.stats, posting.boost);
  };

  const bool concrete = rest_size != 0 && rest_filters.empty() &&
                        std::ranges::all_of(rest, [&](const Term& term) {
                          const auto one = clause(term);
                          return one.stats.stats != nullptr &&
                                 FreqOf(*one.state.reader) &&
                                 ScoresPerDoc(one.stats.scorer);
                        });

  const auto build_concrete =
    [&]<typename Input, typename Head>(auto&& head) -> Result {
    using Probe = PostingProbeScored<Input>;
    return ResolveArity<kTailArity, kTailFloor>(
      rest.size(), [&]<size_t N> -> Result {
        if constexpr (N == 1) {
          const auto posting = clause(rest.front());
          return make.template operator()<Head, Probe>(
            std::forward<decltype(head)>(head),
            std::forward_as_tuple(posting.state.cookie,
                                  *DocOf(*posting.state.reader), segment,
                                  *posting.state.reader, args(posting)));
        } else if constexpr (N != 0) {
          using Tail = probe::SparseConjunctionDocs<Probe, N>;
          return [&]<size_t... I>(std::index_sequence<I...>) {
            return make.template operator()<Head, Tail>(
              std::forward<decltype(head)>(head),
              std::forward_as_tuple(
                std::piecewise_construct,
                std::forward_as_tuple(clause(rest[I]).state.cookie,
                                      *DocOf(*clause(rest[I]).state.reader),
                                      segment, *clause(rest[I]).state.reader,
                                      args(clause(rest[I])))...));
          }(std::make_index_sequence<N>{});
        } else {
          return make
            .template operator()<Head, probe::SparseConjunctionDocs<Probe>>(
              std::forward<decltype(head)>(head),
              std::forward_as_tuple(rest.size(), [&](Probe& probe, size_t i) {
                const auto posting = clause(rest[i]);
                probe.Prepare(posting.state.cookie,
                              *DocOf(*posting.state.reader), segment,
                              *posting.state.reader, args(posting));
              }));
        }
      });
  };

  const auto build_erased = [&]<typename Head>(auto&& head) -> Result {
    std::vector<probe::Erased> probes;
    probes.reserve(rest_size);
    const auto take = [&](probe::Node::ptr node) {
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
    if (probes.size() == 1) {
      return make.template operator()<Head, probe::Erased>(
        std::forward<decltype(head)>(head),
        std::forward_as_tuple(std::move(probes.front())));
    }
    using Tail = probe::SparseConjunctionDocs<probe::Erased>;
    return make.template operator()<Head, Tail>(
      std::forward<decltype(head)>(head),
      std::forward_as_tuple(probes.size(), [&](probe::Erased& leaf, size_t i) {
        leaf = std::move(probes[i]);
      }));
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
    if (rest_size == 0 || !concrete) {
      return build_opaque.template operator()<Head>(
        std::forward<decltype(head)>(head));
    }
    return build_concrete.template operator()<Input, Head>(
      std::forward<decltype(head)>(head));
  };

  const auto build_erased_lead = [&]<typename Head>(auto&& head) -> Result {
    if (rest_size == 0 || !concrete) {
      return build_opaque.template operator()<Head>(
        std::forward<decltype(head)>(head));
    }
    return ResolveInput(
      *DocOf(*clause(rest.front()).state.reader),
      [&]<typename Input> -> Result {
        return build_concrete.template operator()<Input, Head>(
          std::forward<decltype(head)>(head));
      });
  };

  if (head_term) {
    const auto posting = clause(terms.front());
    SDB_ASSERT(posting.state.reader != nullptr);
    const auto& own = *posting.state.reader;
    if (!FreqOf(own) || !ScoresPerDoc(posting.stats.scorer)) {
      auto node = lead::MakePostingScored(posting, segment, recipe);
      if (!node) {
        return {};
      }
      return build_erased_lead.template operator()<lead::Erased>(
        std::forward_as_tuple(std::move(node)));
    }
    return ResolveInput(*DocOf(own), [&]<typename Input> -> Result {
      using Head = PostingLeadScored<Input>;
      return build.template operator()<Input, Head>(std::forward_as_tuple(
        posting.state.cookie, *DocOf(own), segment, own, args(posting)));
    });
  }
  auto node = lead_clause(*filters.front());
  if (!node) {
    return {};
  }
  return build_erased_lead.template operator()<lead::Erased>(
    std::forward_as_tuple(std::move(node)));
}

}  // namespace irs::search
