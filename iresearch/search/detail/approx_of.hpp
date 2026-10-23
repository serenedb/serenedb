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

#include "iresearch/search/detail/plan.hpp"
#include "iresearch/search/queries/approx_query.hpp"
#include "iresearch/search/scorers/all_docs_score.hpp"

namespace irs::detail {

template<typename Family>
typename Family::Result MakeApprox(const ApproxQuery& query,
                                   const typename Family::ScoredContext& ctx) {
  using Result = typename Family::Result;
  auto node = PlanApprox<Result>(query.Approx(), Family::Interrogations(ctx));
  if (!node) {
    return {};
  }
  return Family::template Constant<ApproxNode<Result>>(
    ctx, AllDocsScoreOf(query, Family::Scoring(ctx)), std::move(node));
}

}  // namespace irs::detail
