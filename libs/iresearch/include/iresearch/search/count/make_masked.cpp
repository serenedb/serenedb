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

#include <utility>

#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/count/plan.hpp"
#include "iresearch/search/count/sparse_exclusion.hpp"
#include "iresearch/search/filter.hpp"
#include "iresearch/search/lead/impl.hpp"
#include "iresearch/search/probe/mask_docs.hpp"

namespace irs::count {

Root::ptr MakeMasked(const QueryBuilder& query, const Context& ctx) {
  const auto* docs_mask = query.Segment().docs_mask();
  SDB_ASSERT(docs_mask != nullptr);
  auto node = query.PlanLead({});
  if (!node) {
    return {};
  }
  return MakeShape<SparseExclusion, lead::Erased, probe::MaskDocs>(
    ctx, std::piecewise_construct, std::forward_as_tuple(std::move(node)),
    std::forward_as_tuple(*docs_mask));
}

}  // namespace irs::count
