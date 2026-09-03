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

#include "iresearch/search/vector_radius_filter.hpp"

#include "basics/assert.h"
#include "iresearch/search/ann_index.hpp"
#include "iresearch/search/collectors.hpp"

namespace irs {

QueryBuilder::ptr ByRadius::PrepareSegment(const SubReader& segment,
                                           const PrepareContext& ctx) const {
  const auto& opts = options();
  const auto* ann = segment.Ann(opts.centroids_id);
  if (!ann) {
    return QueryBuilder::Empty();
  }
  SDB_ASSERT(ann->SupportsRange());
  auto sub_ctx = ctx;
  sub_ctx.Boost(GetBoost());
  return ann->PrepareRange(segment, sub_ctx, opts, opts.radius, opts.inclusive,
                           0);
}

PrepareCollector::ptr ByRadius::MakeCollectorImpl(const Scorer* scorer) const {
  return std::make_unique<AllCollector>(scorer);
}

}  // namespace irs
