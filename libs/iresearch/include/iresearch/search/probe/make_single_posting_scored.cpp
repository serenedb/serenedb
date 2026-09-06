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

#include <utility>

#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/common/all_docs_score.hpp"
#include "iresearch/search/common/resolve.hpp"
#include "iresearch/search/probe/constant_scored.hpp"
#include "iresearch/search/probe/impl.hpp"
#include "iresearch/search/probe/make.hpp"
#include "iresearch/search/probe/single_posting.hpp"

namespace irs::probe {

Node::ptr MakeSinglePostingScored(const search::PostingClause& posting,
                                  const SubReader& segment,
                                  const ScoreRecipe& recipe) {
  const auto& meta = posting.state.cookie;
  SDB_ASSERT(meta.docs_count == 1);
  SDB_ASSERT(posting.state.reader != nullptr);
  const auto doc = doc_limits::min() + meta.doc_delta;
  const auto value =
    posting.stats.stats != nullptr
      ? search::SingleDocScore(segment, *posting.state.reader, doc, meta.freq,
                               recipe.Args(posting.stats, posting.boost))
      : score_t{0};
  using Node = ConstantScored<SinglePostingDocs>;
  return memory::make_managed<Impl<Node>>(value, meta);
}

}  // namespace irs::probe
