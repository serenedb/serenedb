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

#include <utility>

#include "iresearch/search/detail/make.hpp"
#include "iresearch/search/scorers/all_docs_score.hpp"

namespace irs::detail {

template<typename Family, typename Kind, typename Query>
typename Family::Result MakeDocs(const Query& query,
                                 const typename Family::DocsContext& ctx) {
  return Build<typename Family::Result, false>(
    Kind{}, query, Family::Interrogations(ctx),
    [&]<typename Slots>(auto&&... args) -> typename Family::Result {
      return Family::template Docs<
        typename Family::template TwoPhaseDocs<Slots>>(
        ctx, std::forward<decltype(args)>(args)...);
    });
}

template<typename Family, typename Kind, typename Query>
typename Family::Result MakeConstant(const Query& query,
                                     const typename Family::ScoredContext& ctx,
                                     score_t value) {
  return Build<typename Family::Result, false>(
    Kind{}, query, Family::Interrogations(ctx),
    [&]<typename Slots>(auto&&... args) -> typename Family::Result {
      return Family::template Constant<
        typename Family::template TwoPhaseDocs<Slots>>(
        ctx, value, std::forward<decltype(args)>(args)...);
    });
}

template<typename Family, typename Kind, typename Query>
typename Family::Result MakeScored(const Query& query,
                                   const typename Family::ScoredContext& ctx) {
  const auto& scoring = Family::Scoring(ctx);
  if constexpr (Kind::kConstant) {
    return MakeConstant<Family, Kind>(query, ctx,
                                      AllDocsScoreOf(query, scoring));
  } else {
    const auto record = query.Stats(scoring);
    const ScoreArgs args{.scorer = record.scorer,
                         .stats = record.stats,
                         .fetcher = scoring.fetcher,
                         .boost = query.Boost()};
    if (const auto value = ConstantOf(Kind{}, query, args)) {
      return MakeConstant<Family, Kind>(query, ctx, *value);
    }
    return Build<typename Family::Result, true>(
      Kind{}, query, Family::Interrogations(ctx),
      [&]<typename Slots>(auto&&... rest) -> typename Family::Result {
        return Family::template Scored<
          typename Family::template TwoPhaseScored<Slots>>(
          ctx, query.Segment(), FieldOf(Kind{}, query), args,
          std::forward<decltype(rest)>(rest)...);
      });
  }
}

}  // namespace irs::detail
