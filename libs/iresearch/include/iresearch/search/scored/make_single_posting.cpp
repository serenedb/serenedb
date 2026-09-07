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
#include "iresearch/search/common/resolve.hpp"
#include "iresearch/search/scored/make.hpp"
#include "iresearch/search/scored/single_posting.hpp"

namespace irs::scored {

Root::ptr MakeSinglePosting(const PostingClause& posting,
                            const SubReader& segment, const Context& ctx) {
  SDB_ASSERT(posting.state.cookie.docs_count == 1);
  SDB_ASSERT(posting.state.reader != nullptr);
  auto root = memory::make_managed<SinglePosting>();
  root->Prepare(posting.state.cookie, segment, *posting.state.reader,
                ScoreArgs{.scorer = posting.stats.scorer,
                          .stats = posting.stats.stats,
                          .fetcher = &ctx.fetcher,
                          .boost = posting.boost});
  return root;
}

}  // namespace irs::scored
