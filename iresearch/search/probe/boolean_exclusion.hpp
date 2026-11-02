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
#include <cstdint>
#include <span>
#include <tuple>
#include <utility>

#include "iresearch/search/detail/exclusion_of.hpp"
#include "iresearch/search/detail/with_mask.hpp"
#include "iresearch/search/probe/all_docs.hpp"
#include "iresearch/search/probe/boolean_planner.hpp"

namespace irs::probe {

template<typename Scoring>
Node::ptr MakeExclusion(const BooleanQuery& query, uint64_t interrogations,
                        const Scoring& scoring, const detail::ScoredCtx& ctx,
                        score_t absorbed) {
  const auto& segment = query.Segment();
  const std::span must = query.Terms(Occur::Must);
  const std::span must_filters = query.Queries(Occur::Must);
  const std::span should = query.Terms(Occur::Should);
  const std::span should_filters = query.Queries(Occur::Should);
  const std::span excludes = query.Terms(Occur::MustNot);
  const std::span exclude_filters = query.Queries(Occur::MustNot);
  const auto min_match = query.MinShouldMatch();
  SDB_ASSERT(!excludes.empty() || !exclude_filters.empty());
  const bool no_must = must.empty() && must_filters.empty();
  const uint64_t docs_count = segment.docs_count();
  uint64_t lead = detail::IncludeCandidates(must, must_filters, segment);
  if (no_must && min_match != 0) {
    lead = std::min(docs_count,
                    detail::LeadCandidates(should, should_filters,
                                           static_cast<doc_id_t>(docs_count)));
  }
  const auto reach = std::max<uint64_t>(
    1, docs_count == 0 ? 0 : interrogations * lead / docs_count);
  if constexpr (detail::kScores<Scoring>) {
    if (absorbed == 0 && min_match == 0 && should.empty() &&
        should_filters.empty() && must.size() == 1 && must_filters.empty() &&
        detail::ScoresPerDocTerm(must.front())) {
      const auto& posting = must.front();
      const auto& own = *posting.state.reader;
      const auto& doc = *detail::DocOf(own);
      return detail::ResolveInput(doc, [&]<typename Input> -> Node::ptr {
        return detail::BuildExcludeSideOf<Node::ptr, Input>(
          excludes, exclude_filters, nullptr, segment, reach, lead,
          [&]<typename Exclude>(auto&& excluded) -> Node::ptr {
            return MakeSparse<detail::PostingProbeScored<Input>, utils::Empty,
                              Exclude>(
              detail::Inherited{},
              std::forward_as_tuple(
                posting.state.cookie, doc, segment, own,
                scoring.recipe.Args(posting.stats, posting.boost)),
              std::forward_as_tuple(),
              std::forward<decltype(excluded)>(excluded));
          });
      });
    }
    auto include = MakeIncluded(query, interrogations, scoring, ctx, absorbed);
    if (!include) {
      return {};
    }
    return detail::BuildExcludeSide<Node::ptr>(
      excludes, exclude_filters, nullptr, segment, reach, lead,
      [&]<typename Exclude>(auto&& excluded) -> Node::ptr {
        return MakeSparse<Erased, utils::Empty, Exclude>(
          detail::Inherited{}, std::forward_as_tuple(std::move(include)),
          std::forward_as_tuple(), std::forward<decltype(excluded)>(excluded));
      });
  } else {
    Node::ptr optional;
    if (min_match != 0) {
      optional = MakeOptional(should, should_filters, min_match, segment,
                              no_must ? interrogations : reach, scoring, ctx,
                              score_t{0});
      if (!optional) {
        return {};
      }
    }
    const auto excluded = [&]<typename Musts, typename Optional>(
                            auto&& musts, auto&& optional_args) -> Node::ptr {
      return detail::BuildExcludeSide<Node::ptr>(
        excludes, exclude_filters, nullptr, segment, reach, lead,
        [&]<typename Exclude>(auto&& exclude) -> Node::ptr {
          return MakeSparse<Musts, Optional, Exclude>(
            utils::Empty{}, std::forward<decltype(musts)>(musts),
            std::forward<decltype(optional_args)>(optional_args),
            std::forward<decltype(exclude)>(exclude));
        });
    };
    if (no_must) {
      if (!optional) {
        if (detail::OnlyMask(excludes, exclude_filters)) {
          return MakeLiveDocs(segment);
        }
        return excluded.template operator()<AllDocs, utils::Empty>(
          std::forward_as_tuple(segment), std::forward_as_tuple());
      }
      return excluded.template operator()<utils::Empty, Erased>(
        std::forward_as_tuple(), std::forward_as_tuple(std::move(optional)));
    }
    return BuildMusts(
      must, must_filters, segment, interrogations, scoring, ctx,
      [&]<typename Musts>(auto&& musts) -> Node::ptr {
        if (!optional) {
          return excluded.template operator()<Musts, utils::Empty>(
            std::forward<decltype(musts)>(musts), std::forward_as_tuple());
        }
        return excluded.template operator()<Musts, Erased>(
          std::forward<decltype(musts)>(musts),
          std::forward_as_tuple(std::move(optional)));
      });
  }
}

}  // namespace irs::probe
