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

#include "basics/debugging.h"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/common/resolve.hpp"
#include "iresearch/search/top/make.hpp"
#include "iresearch/search/top/pruned_posting.hpp"
#include "pg/sql_exception_macro.h"

namespace irs::top {

Root::ptr MakePrunedPosting(const PostingClause& posting,
                            const SubReader& segment, const Context& ctx) {
  SDB_ASSERT(posting.state.reader != nullptr);
  if (posting.stats.stats == nullptr) {
    return {};
  }
  const auto& meta = posting.state.cookie;
  if (meta.docs_count <= doc_limits::kBlockSize) {
    return {};
  }
  const auto& own = *posting.state.reader;
  if (!search::BoundsOf(own) || !search::FreqOf(own)) {
    return {};
  }
  if (!HasScoreBounds(posting.stats.scorer)) {
    return {};
  }
  SDB_IF_FAILURE("irs::PruningIterator") {
    THROW_SQL_ERROR(ERR_MSG("intentional debug error"));
  }
  return search::ResolveInput(
    *search::DocOf(own), [&]<typename Input> -> Root::ptr {
      return MakeShape<PrunedPosting, Input>(
        ctx, meta, *search::DocOf(own), search::LayoutOf(own), segment, own,
        ScoreArgs{.scorer = posting.stats.scorer,
                  .stats = posting.stats.stats,
                  .fetcher = &ctx.fetcher,
                  .boost = posting.boost});
    });
}

}  // namespace irs::top
