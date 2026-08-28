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

#include "iresearch/search/vector_similarity_filter.hpp"

#include <span>
#include <utility>

#include "basics/memory.hpp"
#include "iresearch/search/collectors.hpp"
#include "iresearch/search/vector_filter_util.hpp"
#include "iresearch/search/vector_similarity_query.hpp"

namespace irs {

QueryBuilder::ptr ByVectorSimilarity::PrepareSegment(
  const SubReader& segment, const PrepareContext& ctx) const {
  const auto& opts = options();
  VectorState state{ctx.memory};
  QueryBuilder::ptr inner;
  if (!PrepareVectorState(segment, ctx, field_id(), opts, opts.nprobe, state,
                          inner)) {
    return QueryBuilder::Empty();
  }

  return memory::make_tracked<KnnVectorQuery>(
    ctx.memory, segment, std::move(state), std::span<const float>{opts.query},
    opts.metric, ctx.boost * GetBoost(), std::move(inner));
}

PrepareCollector::ptr ByVectorSimilarity::MakeCollectorImpl(
  const Scorer* scorer) const {
  return std::make_unique<AllCollector>(scorer);
}

}  // namespace irs
