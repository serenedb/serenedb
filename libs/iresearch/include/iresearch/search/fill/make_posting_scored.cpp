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
#include "iresearch/search/scorers/all_docs_score.hpp"
#include "iresearch/search/detail/collect.hpp"
#include "iresearch/search/detail/fill_posting_scored.hpp"
#include "iresearch/search/detail/posting_fill.hpp"
#include "iresearch/search/fill/constant_scored.hpp"
#include "iresearch/search/fill/impl.hpp"
#include "iresearch/search/fill/make.hpp"

namespace irs::fill {

Node::ptr MakePostingScored(const detail::PostingClause& posting,
                            const SubReader& segment, const detail::ScoredCtx& ctx,
                            ScoreMergeType merge) {
  SDB_ASSERT(posting.state.cookie.docs_count != 0);
  SDB_ASSERT(posting.state.reader != nullptr);
  const auto& own = *posting.state.reader;
  const auto& meta = posting.state.cookie;
  const auto& doc = *detail::DocOf(own);
  const auto bounds = detail::BoundsOf(own);
  const auto freq = detail::FreqOf(own);

  if (posting.stats.stats == nullptr) {
    return detail::ResolveInput(doc, [&]<typename Input> -> Node::ptr {
      using Leaf = detail::PlainFillScored<Input>;
      return memory::make_managed<Impl<Leaf>>(meta, doc, bounds, freq);
    });
  }

  if (const auto constant =
        detail::ConstantOf(segment, own,
                           detail::ScoreArgs{.scorer = posting.stats.scorer,
                                             .stats = posting.stats.stats,
                                             .fetcher = ctx.fetcher,
                                             .boost = posting.boost})) {
    const auto value = *constant;
    return detail::ResolveInput(doc, [&]<typename Input> -> Node::ptr {
      using Approx = detail::PostingFill<Input>;
      using Node = ConstantScored<Approx>;
      return memory::make_managed<Impl<Node>>(merge, value, meta, doc, bounds,
                                              freq);
    });
  }

  return detail::ResolveFillScored<Node::ptr>(
    doc, freq && ScoresPerDoc(posting.stats.scorer), merge,
    [&]<typename Leaf, typename> -> Node::ptr {
      return memory::make_managed<Impl<Leaf>>(
        meta, doc, bounds, segment, own,
        detail::ScoreArgs{.scorer = posting.stats.scorer,
                          .stats = posting.stats.stats,
                          .fetcher = ctx.fetcher,
                          .boost = posting.boost});
    });
}

}  // namespace irs::fill
