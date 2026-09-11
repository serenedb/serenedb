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
#include <tuple>
#include <utility>

#include "basics/debugging.h"
#include "basics/empty.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/detail/exclusion_of.hpp"
#include "iresearch/search/detail/resolve.hpp"
#include "iresearch/search/scorers/scorer.hpp"
#include "iresearch/search/top/make.hpp"
#include "iresearch/search/top/pruned_posting.hpp"
#include "pg/sql_exception_macro.h"

namespace irs::top {

Root::ptr MakePrunedPosting(
  const irs::detail::PostingClause& posting,
  std::span<const irs::detail::PostingClause> excludes,
  std::span<const QueryBuilder::ptr> exclude_filters, const SubReader& segment,
  const Context& ctx) {
  SDB_ASSERT(posting.state.reader != nullptr);
  if (posting.stats.stats == nullptr) {
    return {};
  }
  const auto& meta = posting.state.cookie;
  if (meta.docs_count <= doc_limits::kBlockSize) {
    return {};
  }
  const auto& own = *posting.state.reader;
  if (!irs::detail::BoundsOf(own) || !irs::detail::FreqOf(own)) {
    return {};
  }
  if (!HasScoreBounds(posting.stats.scorer)) {
    return {};
  }
  SDB_IF_FAILURE("irs::PruningIterator") {
    THROW_SQL_ERROR(ERR_MSG("intentional debug error"));
  }
  const auto& doc = *irs::detail::DocOf(own);
  const irs::detail::ScoreArgs args{.scorer = posting.stats.scorer,
                                    .stats = posting.stats.stats,
                                    .fetcher = &ctx.fetcher,
                                    .boost = posting.boost};
  return irs::detail::ResolveInput(doc, [&]<typename Input> -> Root::ptr {
    if (excludes.empty() && exclude_filters.empty()) {
      return MakeShape<PrunedPosting, Input, utils::Empty>(
        ctx, std::forward_as_tuple(), meta, doc, irs::detail::LayoutOf(own),
        segment, own, args);
    }
    return irs::detail::BuildBlockExcludesOf<Root::ptr, Input>(
      excludes, exclude_filters, nullptr, segment,
      PrunedCandidates(meta.docs_count, ctx), meta.docs_count,
      [&]<typename Exclude>(auto&& negated) -> Root::ptr {
        return MakeShape<PrunedPosting, Input, Exclude>(
          ctx, std::forward<decltype(negated)>(negated), meta, doc,
          irs::detail::LayoutOf(own), segment, own, args);
      });
  });
}

Root::ptr MakePrunedPosting(const irs::detail::PostingClause& posting,
                            const SubReader& segment, const Context& ctx) {
  return MakePrunedPosting(posting, {}, {}, segment, ctx);
}

}  // namespace irs::top
