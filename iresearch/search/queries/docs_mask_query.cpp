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

#include "iresearch/search/queries/docs_mask_query.hpp"

#include <utility>

#include "iresearch/index/index_meta.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/filters/all_filter.hpp"
#include "iresearch/search/queries/boolean_query.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/memory.hpp"

namespace irs {
namespace {

uint32_t MaskedCount(const SubReader& segment) noexcept {
  const auto& meta = segment.Meta();
  SDB_ASSERT(meta.live_docs_count <= meta.docs_count);
  return meta.docs_count - meta.live_docs_count;
}

}  // namespace

QueryBuilder::ptr WithDocsMask(QueryBuilder::ptr query,
                               const SubReader& segment,
                               IResourceManager& memory,
                               PrepareCollector* collector, bool needs_terms) {
  const auto masked = MaskedCount(segment);
  if (masked == 0 || (query && QueryBuilder::IsEmpty(*query))) {
    return query;
  }
  if (!query) {
    const All all;
    query = all.PrepareSegment(segment, {.memory = memory});
  }
  BooleanBuilder builder{
    segment, memory, 0, kNoBoost, ScoreMergeType::Sum, collector, needs_terms};
  builder.Add(std::move(query), Occur::Must);
  builder.Add(memory::make_tracked<DocsMaskQuery>(memory, segment, masked),
              Occur::MustNot);
  return builder.Finish();
}

}  // namespace irs
