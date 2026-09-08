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

#include <utility>
#include <vector>

#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/common/bitset_build.hpp"
#include "iresearch/search/common/bitset_of.hpp"
#include "iresearch/search/common/boolean_groups.hpp"
#include "iresearch/search/common/collect.hpp"
#include "iresearch/search/common/conjunction_bitset.hpp"
#include "iresearch/search/common/exclusion_bitset.hpp"
#include "iresearch/search/common/plan.hpp"
#include "iresearch/search/common/resolve.hpp"

namespace irs::search {

template<typename Result>
Result MakeBooleanBitset(const BooleanGroups& groups, const SubReader& segment,
                         TableFilter* table) {
  const auto docs_count = static_cast<doc_id_t>(segment.docs_count());
  if (groups.must.empty() && groups.must_filters.empty()) {
    if (groups.should.empty() || !groups.must_not.empty() ||
        !groups.must_not_filters.empty()) {
      return {};
    }
    const auto* const doc = DocOf(FieldOf(groups.should.front(), nullptr));
    if (doc == nullptr ||
        !TakeBitset<Result>(groups.should, *doc, docs_count)) {
      return {};
    }
    auto buckets = DisjunctionBuckets(groups.should, nullptr);
    if (groups.should_fills != nullptr) {
      buckets.fills = std::move(*groups.should_fills);
    }
    return MakeBitsetNode<Result>(std::move(buckets), *doc, docs_count, table);
  }
  if (!groups.should.empty() || !groups.should_filters.empty()) {
    return {};
  }
  SDB_ASSERT(groups.must.size() + groups.must_filters.size() > 1 ||
             !groups.must_not.empty() || !groups.must_not_filters.empty());
  const auto* const doc = SegmentDoc(segment);
  if (doc == nullptr) {
    return {};
  }
  BitsetBuckets buckets;
  if (!CollectConjunctionBuckets(groups.must, groups.must_filters, nullptr,
                                 buckets)) {
    return {};
  }
  if (groups.must_not.empty() && groups.must_not_filters.empty()) {
    if (!TakeConjunctionFold(buckets, *doc, docs_count,
                             HeadEstimate(groups.must, groups.must_filters))) {
      return {};
    }
    return MakeBitsetNode<Result>(std::move(buckets), *doc, docs_count, table);
  }
  uint64_t fill_docs = 0;
  if (!CollectExcludeBuckets(groups.must_not, groups.must_not_filters, nullptr,
                             buckets, fill_docs)) {
    return {};
  }
  const auto candidates =
    IncludeCandidates(groups.must, groups.must_filters, segment);
  if (!TakeExclusionFold(buckets, fill_docs, *doc, docs_count, candidates)) {
    return {};
  }
  return MakeBitsetNode<Result>(std::move(buckets), *doc, docs_count, table);
}

}  // namespace irs::search
