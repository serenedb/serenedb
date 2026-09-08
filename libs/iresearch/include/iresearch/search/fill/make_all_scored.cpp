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

#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/common/all_docs_score.hpp"
#include "iresearch/search/fill/all_docs.hpp"
#include "iresearch/search/fill/constant_scored.hpp"
#include "iresearch/search/fill/impl.hpp"
#include "iresearch/search/fill/plan.hpp"

namespace irs::fill {

Node::ptr MakeAllScored(const SubReader& segment, const ScoredCtx& ctx,
                        const search::StatsRecord& record, ScoreMergeType merge,
                        score_t boost) {
  const auto value =
    search::AllDocsScore(segment, ScoreArgs{.scorer = record.scorer,
                                            .stats = record.stats,
                                            .fetcher = ctx.fetcher,
                                            .boost = boost});
  return memory::make_managed<Impl<ConstantScored<AllDocs>>>(merge, value,
                                                             segment);
}

Node::ptr MakeAllScored(const SubReader& segment, ScoreMergeType merge,
                        score_t score) {
  return memory::make_managed<Impl<ConstantScored<AllDocs>>>(merge, score,
                                                             segment);
}

}  // namespace irs::fill
