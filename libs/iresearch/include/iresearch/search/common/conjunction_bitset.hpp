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

#pragma once

#include <limits>
#include <span>
#include <utility>
#include <vector>

#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/common/bitset_build.hpp"
#include "iresearch/search/common/bitset_of.hpp"
#include "iresearch/search/common/clause_terms.hpp"
#include "iresearch/search/common/collect.hpp"
#include "iresearch/search/common/plan.hpp"
#include "iresearch/search/common/resolve.hpp"

namespace irs::search {

template<typename Term>
bool CollectConjunctionBuckets(std::span<const Term> terms,
                               std::span<const QueryBuilder::ptr> filters,
                               const TermReader* field, BitsetBuckets& out) {
  out.must.reserve(terms.size() + filters.size());
  return VisitOrderedOf(
    terms, filters, true, 0, std::numeric_limits<size_t>::max(),
    [&](const Term& term) {
      out.must.emplace_back().emplace_back(ClauseOf(term, field));
      return true;
    },
    [&](const QueryBuilder& child) {
      return ClauseTerms(child, out.must.emplace_back());
    });
}

template<typename Result, typename Term>
Result MakeConjunctionBitset(std::span<const Term> terms,
                             std::span<const QueryBuilder::ptr> filters,
                             const TermReader* field, const SubReader& segment,
                             TableFilter* table) {
  SDB_ASSERT(terms.size() + filters.size() > 1);
  const auto* const doc = SegmentDoc(segment);
  if (doc == nullptr) {
    return {};
  }
  BitsetBuckets buckets;
  if (!CollectConjunctionBuckets(terms, filters, field, buckets)) {
    return {};
  }
  const auto docs_count = static_cast<doc_id_t>(segment.docs_count());
  if (!TakeConjunctionFold(buckets, *doc, docs_count,
                           HeadEstimate(terms, filters))) {
    return {};
  }
  return MakeBitsetNode<Result>(std::move(buckets), *doc, docs_count, table);
}

}  // namespace irs::search
