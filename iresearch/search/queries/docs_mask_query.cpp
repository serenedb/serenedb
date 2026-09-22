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
#include "iresearch/search/fill/docs_mask.hpp"
#include "iresearch/search/fill/impl.hpp"
#include "iresearch/search/filters/all_filter.hpp"
#include "iresearch/search/probe/docs_mask.hpp"
#include "iresearch/search/probe/impl.hpp"
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

class MaskQuery : public QueryBuilder {
 public:
  MaskQuery(const SubReader& segment, uint32_t masked) noexcept
    : QueryBuilder{segment, masked, QueryKind::Other} {}

  probe::Node::ptr PlanProbe(const detail::ScoredCtx&, uint64_t) const final {
    return memory::make_managed<probe::Impl<probe::DocsMask>>(_segment);
  }

  fill::Node::ptr PlanFill(const detail::ScoredCtx&,
                           ScoreMergeType) const final {
    return memory::make_managed<fill::Impl<fill::DocsMask>>(_segment);
  }

  count::Root::ptr PlanCount(const count::Context&) const final { return {}; }
  docs::Root::ptr PlanDocs(const docs::Context&) const final { return {}; }
  hits::Root::ptr PlanScored(const hits::Context&) const final { return {}; }
  top::Root::ptr PlanTop(const top::Context&) const final { return {}; }
  lead::Node::ptr PlanLead(const detail::ScoredCtx&) const final { return {}; }

  void Visit(PreparedStateVisitor&, score_t) const final {}

  score_t Boost() const noexcept final { return kNoBoost; }
};

}  // namespace

QueryBuilder::ptr WithDocsMask(QueryBuilder::ptr query,
                               const SubReader& segment,
                               IResourceManager& memory,
                               PrepareCollector* collector, bool needs_terms) {
  const auto masked = MaskedCount(segment);
  if (!query || masked == 0 || (query && QueryBuilder::IsEmpty(*query))) {
    return query;
  }

  BooleanBuilder builder{
    segment, memory, 0, kNoBoost, ScoreMergeType::Sum, collector, needs_terms};
  builder.Add(std::move(query), Occur::Must);
  builder.Add(memory::make_tracked<MaskQuery>(memory, segment, masked),
              Occur::MustNot);
  return builder.Finish();
}

}  // namespace irs
