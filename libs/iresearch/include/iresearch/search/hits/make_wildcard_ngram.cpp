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

#include "iresearch/search/scorers/all_docs_score.hpp"
#include "iresearch/search/detail/wildcard_ngram_of.hpp"
#include "iresearch/search/hits/walk.hpp"
#include "iresearch/search/hits/make.hpp"
#include "iresearch/search/filters/wildcard_ngram_filter.hpp"

namespace irs::hits {

Root::ptr MakeWildcardNGram(const WildcardNGramQuery& query,
                            const Context& ctx) {
  SDB_ASSERT(query.Kind() != QueryKind::Empty);
  const auto record = query.Stats(ScoredOf(ctx));
  const auto value = irs::detail::AllDocsScore(
    query.Segment(), irs::detail::ScoreArgs{.scorer = record.scorer,
                                       .stats = record.stats,
                                       .fetcher = &ctx.fetcher,
                                       .boost = query.Boost()});
  if (ctx.table != nullptr) {
    return irs::detail::MakeWildcardNGram<FilteredConstantWalk, Root::ptr>(
      query, 0, ctx.table, value);
  }
  return irs::detail::MakeWildcardNGram<PlainConstantWalk, Root::ptr>(
    query, 0, utils::Empty{}, value);
}

}  // namespace irs::hits
