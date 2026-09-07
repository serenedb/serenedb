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

#include <span>
#include <utility>
#include <vector>

#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/common/window_of.hpp"
#include "iresearch/search/count/plan.hpp"
#include "iresearch/search/count/subtract.hpp"

namespace irs::count {

Root::ptr MakeSubtractConjunction(std::span<const search::PostingClause> terms,
                                  std::span<const QueryBuilder::ptr> filters,
                                  const SubReader& segment,
                                  const Context& ctx) {
  SDB_ASSERT(terms.size() + filters.size() == 2);
  const IndexInput* doc = nullptr;
  if (!search::WindowTerms(terms, filters, nullptr, doc)) {
    return {};
  }
  const auto docs_count = static_cast<doc_id_t>(segment.docs_count());
  auto disjunction =
    MakeDisjunctionOfTerms(terms, nullptr, *doc, docs_count, ctx);
  if (!disjunction) {
    return {};
  }
  const uint64_t total = uint64_t{terms.front().state.cookie.docs_count} +
                         uint64_t{terms.back().state.cookie.docs_count};
  return memory::make_managed<Subtract>(total, std::move(disjunction));
}

}  // namespace irs::count
