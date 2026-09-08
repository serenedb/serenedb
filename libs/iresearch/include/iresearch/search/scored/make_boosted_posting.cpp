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

#include <span>

#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/boolean_query.hpp"
#include "iresearch/search/common/all_docs_score.hpp"
#include "iresearch/search/common/resolve.hpp"
#include "iresearch/search/scored/boosted_posting.hpp"
#include "iresearch/search/scored/make.hpp"

namespace irs::scored {

Root::ptr MakeBoostedPosting(const BooleanQuery& query,
                             const SubReader& segment, const Context& ctx,
                             ScoreMergeType merge, score_t absorbed) {
  if (merge != ScoreMergeType::Sum || absorbed != 0 ||
      query.MinShouldMatch() != 0) {
    return {};
  }
  const std::span must = query.Terms(Occur::Must);
  const std::span should = query.Terms(Occur::Should);
  if (must.size() != 1 || should.size() != 1 ||
      !query.Queries(Occur::Must).empty() ||
      !query.Queries(Occur::Should).empty()) {
    return {};
  }
  const auto& lead = must.front();
  const auto& boost = should.front();
  const auto& meta = lead.state.cookie;
  const auto& boost_meta = boost.state.cookie;
  if (meta.docs_count <= 1 || meta.docs_count < boost_meta.docs_count ||
      lead.stats.stats == nullptr || boost.stats.stats == nullptr) {
    return {};
  }
  SDB_ASSERT(lead.state.reader != nullptr);
  SDB_ASSERT(boost.state.reader != nullptr);
  const auto& own = *lead.state.reader;
  const auto& boost_own = *boost.state.reader;
  const auto* const doc = search::DocOf(own);
  if (doc == nullptr || search::DocOf(boost_own) != doc ||
      !search::FreqOf(own) || !search::FreqOf(boost_own)) {
    return {};
  }
  const ScoreArgs args{.scorer = lead.stats.scorer,
                       .stats = lead.stats.stats,
                       .fetcher = &ctx.fetcher,
                       .boost = lead.boost};
  const ScoreArgs boost_args{.scorer = boost.stats.scorer,
                             .stats = boost.stats.stats,
                             .fetcher = &ctx.fetcher,
                             .boost = boost.boost};
  if (search::ConstantOf(segment, own, args) ||
      search::ConstantOf(segment, boost_own, boost_args)) {
    return {};
  }
  return search::ResolveInput(*doc, [&]<typename Input> -> Root::ptr {
    return MakePrepared(ctx, [&](auto table) -> Root::ptr {
      auto root = memory::make_managed<BoostedPosting<Input, decltype(table)>>(
        table, ctx.fetcher);
      root->Prepare(meta, *doc, segment, own, args, search::LayoutOf(own),
                    search::BoundsOf(own), boost_meta, boost_own, boost_args);
      return root;
    });
  });
}

}  // namespace irs::scored
