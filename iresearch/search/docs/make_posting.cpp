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

#include "iresearch/search/docs/family.hpp"
#include "iresearch/search/docs/plan.hpp"
#include "iresearch/search/docs/posting.hpp"
#include "iresearch/search/docs/single_posting.hpp"
#include "iresearch/search/lead/plan.hpp"
#include "iresearch/search/queries/term_query.hpp"

namespace irs::docs {

Root::ptr Family::MakePosting(const detail::PostingClause& posting,
                              const SubReader&, const Context&,
                              ScoreMergeType) {
  const auto& meta = posting.state.cookie;
  SDB_ASSERT(meta.docs_count != 0);
  if (meta.docs_count == 1) {
    return memory::make_managed<SinglePosting>(doc_limits::min() +
                                               meta.doc_delta);
  }
  const auto& own = *posting.state.reader;
  const auto& in = *detail::DocOf(own);
  return detail::ResolveInput(in, [&]<typename Input> -> Root::ptr {
    auto root = memory::make_managed<Posting<Input>>();
    root->Prepare(meta, in, detail::LayoutOf(own), detail::BoundsOf(own),
                  detail::FreqOf(own));
    return root;
  });
}

}  // namespace irs::docs
