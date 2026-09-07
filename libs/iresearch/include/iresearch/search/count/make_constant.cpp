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
#include "iresearch/search/all_filter.hpp"
#include "iresearch/search/count/constant.hpp"
#include "iresearch/search/count/plan.hpp"
#include "iresearch/search/count/walk.hpp"
#include "iresearch/search/lead/impl.hpp"
#include "iresearch/search/lead/make.hpp"
#include "iresearch/search/term_query.hpp"

namespace irs::count {

Root::ptr MakeConstant(uint64_t count) {
  return memory::make_managed<Constant>(count);
}

Root::ptr MakeTerm(const search::PostingClause& posting,
                   const SubReader& segment, const Context& ctx) {
  if (ctx.table == nullptr) {
    return MakeConstant(posting.state.cookie.docs_count);
  }
  auto node = lead::MakePostingDocs(posting, segment);
  if (!node) {
    return {};
  }
  return MakeShape<Walk, lead::Erased>(ctx, std::move(node));
}

Root::ptr MakeAll(const SubReader& segment, const Context& ctx) {
  if (ctx.table == nullptr) {
    return MakeConstant(segment.live_docs_count());
  }
  auto node = lead::MakeAllDocs(segment);
  if (!node) {
    return {};
  }
  return MakeShape<Walk, lead::Erased>(ctx, std::move(node));
}

Root::ptr Make(const TermQuery& query, const Context& ctx) {
  return MakeTerm(search::PostingClause{query.State()}, query.Segment(), ctx);
}

Root::ptr Make(const AllQuery& query, const Context& ctx) {
  return MakeAll(query.Segment(), ctx);
}

}  // namespace irs::count
