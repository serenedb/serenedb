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

#include <utility>

#include "iresearch/search/common/all_docs_score.hpp"
#include "iresearch/search/common/scored_context.hpp"
#include "iresearch/search/fill/plan.hpp"
#include "iresearch/search/fill/walk.hpp"
#include "iresearch/search/lead/constant_scored.hpp"
#include "iresearch/search/lead/impl.hpp"
#include "iresearch/search/lead/make.hpp"
#include "iresearch/search/wildcard_ngram_filter.hpp"

namespace irs::fill {

Node::ptr MakeWildcardNgramScored(const WildcardNgramQuery& query,
                                  const ScoredCtx& ctx, ScoreMergeType merge) {
  SDB_ASSERT(query.Kind() != QueryKind::Empty);
  auto node = lead::MakeWildcardNgramDocs(query);
  if (!node) {
    return {};
  }
  const auto record = query.Stats(ctx);
  const auto value =
    search::AllDocsScore(query.Segment(), ScoreArgs{.scorer = record.scorer,
                                                    .stats = record.stats,
                                                    .fetcher = ctx.fetcher,
                                                    .boost = query.Boost()});
  using Node = lead::ConstantScored<lead::Erased>;
  return memory::make_managed<ByWalkScored<Node>>(merge, value,
                                                  std::move(node));
}

}  // namespace irs::fill
