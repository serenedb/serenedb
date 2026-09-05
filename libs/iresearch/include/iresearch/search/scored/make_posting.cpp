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
#include "iresearch/search/common/resolve.hpp"
#include "iresearch/search/lead/impl.hpp"
#include "iresearch/search/lead/plan.hpp"
#include "iresearch/search/scored/detail/walk.hpp"
#include "iresearch/search/scored/make.hpp"
#include "iresearch/search/scored/posting.hpp"

namespace irs::scored {

Root::ptr MakePosting(const PostingClause& posting, const SubReader& segment,
                      const Context& ctx) {
  const auto& meta = posting.state.cookie;
  SDB_ASSERT(meta.docs_count > 1, "a single document has its own unit");
  SDB_ASSERT(posting.state.reader != nullptr);
  const auto& own = *posting.state.reader;
  const auto* const doc = search::DocOf(own);
  const ScoreArgs args{.scorer = posting.stats.scorer,
                       .stats = posting.stats.stats,
                       .fetcher = &ctx.fetcher,
                       .boost = posting.boost};

  if (const auto value = search::ConstantOf(segment, own, args)) {
    return lead::ResolvePostingDocs<Root::ptr>(
      posting, [&]<typename Leaf>(auto&&... rest) -> Root::ptr {
        return MakeShape<detail::ConstantWalk, Leaf>(
          ctx, *value, std::forward<decltype(rest)>(rest)...);
      });
  }
  return search::ResolveInput(*doc, [&]<typename Input> -> Root::ptr {
    return MakePrepared(ctx, [&](auto table) -> Root::ptr {
      auto root = memory::make_managed<Posting<Input, decltype(table)>>(table);
      root->Prepare(meta, *doc, segment, own, args, search::LayoutOf(own),
                    search::BoundsOf(own));
      return root;
    });
  });
}

}  // namespace irs::scored
