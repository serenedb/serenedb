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

#include <vector>

#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/common/conjunction_bitset.hpp"
#include "iresearch/search/common/exclusion_bitset.hpp"
#include "iresearch/search/count/bitset.hpp"
#include "iresearch/search/count/make_boolean.hpp"

namespace irs::count {

Root::ptr MakeBitsetDisjunction(std::span<const search::PostingClause> terms,
                                const IndexInput* doc,
                                std::vector<FillNode::ptr>& rest,
                                doc_id_t docs_count, const Context& ctx) {
  if (terms.empty() ||
      !search::TakeBitset<Root::ptr>(terms, *doc, docs_count)) {
    return {};
  }
  return search::MakeBitsetWith<Root::ptr>(terms, nullptr, *doc, docs_count,
                                           std::move(rest), ctx.table);
}

Root::ptr MakeBitsetConjunction(std::span<const search::PostingClause> terms,
                                std::span<const QueryBuilder::ptr> filters,
                                const SubReader& segment, const Context& ctx) {
  return search::MakeConjunctionBitset<Root::ptr>(terms, filters, nullptr,
                                                  segment, ctx.table);
}

Root::ptr MakeBitsetExclusion(
  std::span<const search::PostingClause> terms,
  std::span<const QueryBuilder::ptr> filters,
  std::span<const search::PostingClause> exclude_terms,
  std::span<const QueryBuilder::ptr> exclude_filters, const SubReader& segment,
  uint64_t candidates, const Context& ctx) {
  return search::MakeExclusionBitset<Root::ptr>(
    terms, filters, nullptr, exclude_terms, exclude_filters, nullptr, segment,
    candidates, ctx.table);
}

}  // namespace irs::count
