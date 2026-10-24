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

#include <memory>
#include <tuple>
#include <utility>

#include "iresearch/search/detail/make_query.hpp"
#include "iresearch/search/detail/nested_of.hpp"
#include "iresearch/search/queries/nested_query.hpp"
#include "iresearch/search/queries/query_builder_impl.tpp"

namespace irs::detail {

template<typename Family, typename Ctx>
typename Family::Result MakeQuery(const ByNestedQuery& query, const Ctx& ctx) {
  if constexpr (!kScoredPlan<Family, Ctx>) {
    return PlanNestedDocs<Family>(
      query, [&]<typename Node>(auto&&... args) -> typename Family::Result {
        return Family::template Docs<Node>(
          ctx, std::forward<decltype(args)>(args)...);
      });
  } else {
    if (!query.ScoresChildren()) {
      return PlanNestedDocs<Family>(
        query, [&]<typename Node>(auto&&... args) -> typename Family::Result {
          return Family::template Constant<Node>(
            ctx, query.Constant(), std::forward<decltype(args)>(args)...);
        });
    }
    auto fetcher = std::make_unique<ColumnArgsFetcher>();
    ScoredCtx scoring = Family::Scoring(ctx);
    scoring.fetcher = fetcher.get();
    auto child = query.Child().PlanLead(scoring);
    if (!child) {
      return {};
    }
    auto parent = query.Parents();
    const auto merge = query.Merge();
    return ResolveRule<false>(
      query.Segment(), query.MatchKind(),
      [&]<typename Rule>(Rule rule) -> typename Family::Result {
        using Node = NestedScored<NestedSlots<ScoredChild, Rule>>;
        return Family::template Scored<Node>(
          ctx, std::piecewise_construct, std::move(parent),
          std::forward_as_tuple(std::move(child), std::move(fetcher), merge),
          std::move(rule));
      });
  }
}

}  // namespace irs::detail
namespace irs {

template class QueryBuilderImpl<ByNestedQuery>;

}  // namespace irs
