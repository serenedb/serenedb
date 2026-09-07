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

#include "iresearch/search/docs/plan.hpp"
#include "iresearch/search/docs/posting.hpp"
#include "iresearch/search/lead/plan.hpp"
#include "iresearch/search/term_query.hpp"

namespace irs::docs {

Root::ptr MakePosting(const search::PostingClause& posting, const SubReader&,
                      const Context& ctx) {
  const auto& meta = posting.state.cookie;
  SDB_ASSERT(meta.docs_count != 0);
  if (meta.docs_count == 1) {
    return MakeSinglePosting(doc_limits::min() + meta.doc_delta, ctx);
  }
  const auto& own = *posting.state.reader;
  const auto& in = *search::DocOf(own);
  return ResolveInput(in, [&]<typename Input> -> Root::ptr {
    const auto make = [&](auto table) -> Root::ptr {
      auto root = memory::make_managed<Posting<Input, decltype(table)>>(table);
      root->Prepare(meta, in, search::LayoutOf(own), search::BoundsOf(own),
                    search::FreqOf(own));
      return root;
    };
    if (ctx.table != nullptr) {
      return make(ctx.table);
    }
    return make(utils::Empty{});
  });
}

Root::ptr Make(const TermQuery& query, const Context& ctx) {
  return MakePosting(search::PostingClause{query.State()}, query.Segment(),
                     ctx);
}

}  // namespace irs::docs
