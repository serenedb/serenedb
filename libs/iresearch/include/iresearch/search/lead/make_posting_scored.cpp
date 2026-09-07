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

#include <optional>

#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/common/all_docs_score.hpp"
#include "iresearch/search/lead/constant_scored.hpp"
#include "iresearch/search/lead/impl.hpp"
#include "iresearch/search/lead/plan.hpp"
#include "iresearch/search/lead/recipe_scored.hpp"

namespace irs::lead {

Node::ptr MakePostingScored(const PostingClause& posting,
                            const SubReader& segment,
                            const ScoreRecipe& recipe) {
  const auto& meta = posting.state.cookie;
  SDB_ASSERT(meta.docs_count != 0);
  const auto args = recipe.Args(posting.stats, posting.boost);

  const auto constant = [&]() -> std::optional<score_t> {
    if (posting.stats.stats == nullptr) {
      return score_t{0};
    }
    SDB_ASSERT(posting.state.reader != nullptr);
    if (meta.docs_count == 1) {
      return search::SingleDocScore(segment, *posting.state.reader,
                                    doc_limits::min() + meta.doc_delta,
                                    meta.freq, args);
    }
    return search::ConstantOf(segment, *posting.state.reader, args);
  }();

  if (constant) {
    return ResolvePostingDocs<Node::ptr>(
      posting, [&]<typename Leaf>(auto&&... leaf_args) -> Node::ptr {
        return memory::make_managed<Impl<ConstantScored<Leaf>>>(
          *constant, std::forward<decltype(leaf_args)>(leaf_args)...);
      });
  }
  SDB_ASSERT(posting.state.reader != nullptr);
  const auto& own = *posting.state.reader;
  if (!search::FreqOf(own)) {
    return ResolvePostingDocs<Node::ptr>(
      posting, [&]<typename Leaf>(auto&&... leaf_args) -> Node::ptr {
        return memory::make_managed<Impl<RecipeScored<Leaf>>>(
          segment, own, args, std::forward<decltype(leaf_args)>(leaf_args)...);
      });
  }
  const auto& doc = *search::DocOf(own);
  return ResolveInput(doc, [&]<typename Input> -> Node::ptr {
    using Leaf = PostingLeadScored<Input>;
    return memory::make_managed<Impl<Leaf>>(meta, doc, segment, own, args);
  });
}

}  // namespace irs::lead
