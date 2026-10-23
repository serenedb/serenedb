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

#include "iresearch/search/probe/boolean_planner.hpp"

namespace irs::probe {

Node::ptr Make(const BooleanQuery& query, const detail::ScoredCtx& ctx,
               uint64_t interrogations) {
  const auto& segment = query.Segment();
  return MakeBoolean(query, interrogations,
                     detail::TermScoring{
                       .recipe = {.segment = &segment, .fetcher = ctx.fetcher},
                       .merge = query.MergeType(),
                     },
                     ctx);
}

}  // namespace irs::probe
