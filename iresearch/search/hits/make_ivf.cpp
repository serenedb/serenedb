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

#include "iresearch/search/detail/vector_of.hpp"
#include "iresearch/search/hits/make.hpp"

namespace irs::hits {

Root::ptr Make(const RangeVectorQuery& query, const Context& ctx) {
  auto inner = irs::detail::InnerProbe(query);
  if (query.Inner() != nullptr && !inner) {
    return {};
  }
  const auto record = query.Stats(ScoredOf(ctx));
  const irs::detail::ScoreArgs score{.scorer = record.scorer,
                                     .stats = record.stats,
                                     .fetcher = &ctx.fetcher,
                                     .boost = query.Boost()};
  return ResolveBool(query.Inclusive(), [&]<bool Inclusive>() -> Root::ptr {
    return ResolveBool(query.Rescored(), [&]<bool Rescore>() -> Root::ptr {
      if (ctx.table != nullptr) {
        return irs::detail::MakeVectorScored<FilteredWalk, Root::ptr,
                                             irs::detail::RadiusGate<Inclusive>,
                                             Rescore, lead::TwoPhaseScored>(
          query, *query.State().reader, score, query.Threshold(),
          std::move(inner), ctx.table, ctx.fetcher);
      }
      return irs::detail::MakeVectorScored<PlainWalk, Root::ptr,
                                           irs::detail::RadiusGate<Inclusive>,
                                           Rescore, lead::TwoPhaseScored>(
        query, *query.State().reader, score, query.Threshold(),
        std::move(inner), utils::Empty{}, ctx.fetcher);
    });
  });
}

Root::ptr Make(const KnnVectorQuery& query, const Context& ctx) {
  auto inner = irs::detail::InnerProbe(query);
  if (query.Inner() != nullptr && !inner) {
    return {};
  }
  const auto record = query.Stats(ScoredOf(ctx));
  const irs::detail::ScoreArgs score{.scorer = record.scorer,
                                     .stats = record.stats,
                                     .fetcher = &ctx.fetcher,
                                     .boost = query.Boost()};
  const auto& field = *query.State().reader;
  return ResolveBool(query.Rescored(), [&]<bool Rescore>() -> Root::ptr {
    if (ctx.table != nullptr) {
      return irs::detail::MakeVectorScored<FilteredWalk, Root::ptr,
                                           irs::detail::AcceptAll, Rescore,
                                           lead::TwoPhaseScored>(
        query, field, score, irs::detail::Unbounded(), std::move(inner),
        ctx.table, ctx.fetcher);
    }
    return irs::detail::MakeVectorScored<PlainWalk, Root::ptr,
                                         irs::detail::AcceptAll, Rescore,
                                         lead::TwoPhaseScored>(
      query, field, score, irs::detail::Unbounded(), std::move(inner),
      utils::Empty{}, ctx.fetcher);
  });
}

}  // namespace irs::hits
