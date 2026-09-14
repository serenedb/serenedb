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
#include "iresearch/search/fill/make.hpp"

namespace irs::fill {

Node::ptr Make(const RangeVectorQuery& query) {
  auto inner = detail::InnerProbe(query);
  if (query.Inner() != nullptr && !inner) {
    return {};
  }
  return ResolveBool(query.Inclusive(), [&]<bool Inclusive>() -> Node::ptr {
    return detail::MakeVectorDocs<
      ByWalkDocs, Node::ptr, detail::RadiusGate<Inclusive>, lead::TwoPhaseDocs>(
      query, query.Threshold(), std::move(inner));
  });
}

Node::ptr Make(const RangeVectorQuery& query, const detail::ScoredCtx& ctx,
               ScoreMergeType merge) {
  auto inner = detail::InnerProbe(query);
  if (query.Inner() != nullptr && !inner) {
    return {};
  }
  const auto record = query.Stats(ctx);
  const detail::ScoreArgs score{.scorer = record.scorer,
                                .stats = record.stats,
                                .fetcher = ctx.fetcher,
                                .boost = query.Boost()};
  return ResolveBool(query.Inclusive(), [&]<bool Inclusive>() -> Node::ptr {
    return ResolveBool(query.Rescored(), [&]<bool Rescore>() -> Node::ptr {
      return detail::MakeVectorScored<ByWalkScored, Node::ptr,
                                      detail::RadiusGate<Inclusive>, Rescore,
                                      lead::TwoPhaseScored>(
        query, *query.State().reader, score, query.Threshold(),
        std::move(inner), merge, *ctx.fetcher);
    });
  });
}

}  // namespace irs::fill
