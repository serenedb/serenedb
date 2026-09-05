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
#include "iresearch/search/common/collect.hpp"
#include "iresearch/search/fill/constant_scored.hpp"
#include "iresearch/search/fill/impl.hpp"
#include "iresearch/search/fill/make.hpp"
#include "iresearch/search/fill/single_posting_docs.hpp"
#include "iresearch/search/states/term_state.hpp"

namespace irs::fill {

Node::ptr MakeSinglePostingScored(const search::PostingClause& posting,
                                  const SubReader& segment,
                                  const ScoredCtx& ctx, ScoreMergeType merge) {
  SDB_ASSERT(posting.state.cookie.docs_count == 1);
  SDB_ASSERT(posting.state.reader != nullptr);
  const auto& meta = posting.state.cookie;
  const auto doc = doc_limits::min() + meta.doc_delta;
  if (posting.stats.stats == nullptr) {
    return memory::make_managed<Impl<SingleDocs>>(doc);
  }
  const auto value =
    search::SingleDocScore(segment, *posting.state.reader, doc, meta.freq,
                           search::ScoreArgs{.scorer = posting.stats.scorer,
                                             .stats = posting.stats.stats,
                                             .fetcher = ctx.fetcher,
                                             .boost = posting.boost});
  return memory::make_managed<Impl<ConstantScored<SingleDocs>>>(merge, value,
                                                                doc);
}

}  // namespace irs::fill
