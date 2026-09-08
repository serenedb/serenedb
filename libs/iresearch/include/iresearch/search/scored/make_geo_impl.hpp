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

#include <utility>

#include "iresearch/search/common/all_docs_score.hpp"
#include "iresearch/search/common/geo_of.hpp"
#include "iresearch/search/geo_query.hpp"
#include "iresearch/search/scored/detail/walk.hpp"
#include "iresearch/search/scored/make.hpp"

namespace irs::scored {

template<typename Parser, typename Acceptor>
Root::ptr Make(const GeoQuery<Parser, Acceptor>& query, const Context& ctx) {
  SDB_ASSERT(query.Kind() != QueryKind::Empty);
  const auto record = query.Stats(ScoredOf(ctx));
  const auto value =
    search::AllDocsScore(query.Segment(), ScoreArgs{.scorer = record.scorer,
                                                    .stats = record.stats,
                                                    .fetcher = &ctx.fetcher,
                                                    .boost = query.Boost()});
  if (ctx.table != nullptr) {
    return search::MakeGeo<FilteredConstantWalk, Root::ptr>(query, 0, ctx.table,
                                                            value);
  }
  return search::MakeGeo<PlainConstantWalk, Root::ptr>(query, 0, utils::Empty{},
                                                       value);
}

}  // namespace irs::scored
