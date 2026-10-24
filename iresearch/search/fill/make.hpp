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

#include <cstdint>
#include <span>
#include <vector>

#include "iresearch/search/detail/plan.hpp"
#include "iresearch/search/detail/scored_context.hpp"
#include "iresearch/search/fill/node.hpp"
#include "iresearch/search/queries/term_state.hpp"
#include "iresearch/utils/assert.hpp"

namespace irs::fill {

Node::ptr MakePostingDocs(const detail::PostingClause& posting,
                          const SubReader& segment);
Node::ptr MakePostingScored(const detail::PostingClause& posting,
                            const SubReader& segment,
                            const detail::ScoredCtx& ctx, ScoreMergeType merge);

Node::ptr MakeSinglePostingDocs(const detail::PostingClause& posting);
Node::ptr MakeSinglePostingScored(const detail::PostingClause& posting,
                                  const SubReader& segment,
                                  const detail::ScoredCtx& ctx,
                                  ScoreMergeType merge);

Node::ptr MakeAllDocs(const SubReader& segment);
Node::ptr MakeAllScored(const SubReader& segment, ScoreMergeType merge,
                        score_t score);

}  // namespace irs::fill
