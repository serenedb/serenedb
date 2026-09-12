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
#include "iresearch/search/detail/plain_scored.hpp"
#include "iresearch/search/detail/resolve.hpp"
#include "iresearch/search/probe/constant_scored.hpp"
#include "iresearch/search/probe/impl.hpp"
#include "iresearch/search/probe/make.hpp"
#include "iresearch/search/probe/plan.hpp"
#include "iresearch/search/probe/posting_scored.hpp"
#include "iresearch/search/scorers/all_docs_score.hpp"

namespace irs::probe {

Node::ptr MakePostingScored(const detail::PostingClause& posting,
                            const SubReader& segment,
                            const detail::ScoreRecipe& recipe) {
  const auto& meta = posting.state.cookie;
  SDB_ASSERT(meta.docs_count != 0);
  if (meta.docs_count == 1) {
    return MakeSinglePostingScored(posting, segment, recipe);
  }
  SDB_ASSERT(posting.state.reader != nullptr);
  const auto& own = *posting.state.reader;
  const auto scores = posting.stats.stats != nullptr;
  if (scores) {
    if (const auto value = detail::ConstantOf(
          segment, own, recipe.Args(posting.stats, posting.boost))) {
      return ResolvePostingDocs<Node::ptr>(
        posting, [&]<typename Leaf>(auto&&... args) -> Node::ptr {
          return memory::make_managed<Impl<ConstantScored<Leaf>>>(
            *value, std::forward<decltype(args)>(args)...);
        });
    }
  }
  return detail::ResolveInput(
    *detail::DocOf(own), [&]<typename Input> -> Node::ptr {
      if (!scores) {
        using Leaf = detail::PlainProbeScored<Input>;
        return memory::make_managed<Impl<Leaf>>(meta, *detail::DocOf(own),
                                                detail::LayoutOf(own),
                                                detail::BoundsOf(own));
      }
      using Leaf = detail::PostingProbeScored<Input>;
      return memory::make_managed<Impl<Leaf>>(
        meta, *detail::DocOf(own), segment, own,
        recipe.Args(posting.stats, posting.boost));
    });
}

}  // namespace irs::probe
