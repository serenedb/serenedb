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
#include "iresearch/search/scored/all.hpp"
#include "iresearch/search/scored/make.hpp"

namespace irs::scored {

Root::ptr MakeAll(const SubReader& segment, const Context& ctx,
                  const search::StatsRecord& record, score_t boost) {
  return MakePrepared(ctx, [&](auto table) -> Root::ptr {
    auto root = memory::make_managed<All<decltype(table)>>(
      table, ctx.fetcher, static_cast<doc_id_t>(segment.docs_count()));
    root->Prepare(segment, ScoreArgs{.scorer = record.scorer,
                                     .stats = record.stats,
                                     .fetcher = &ctx.fetcher,
                                     .boost = boost});
    return root;
  });
}

Root::ptr MakeAll(const SubReader& segment, const Context& ctx, score_t score) {
  return MakeShape<All>(ctx, ctx.fetcher,
                        static_cast<doc_id_t>(segment.docs_count()), score);
}

}  // namespace irs::scored
