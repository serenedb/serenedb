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

#include <vector>

#include "basics/memory.hpp"
#include "iresearch/formats/column/column_reader.hpp"
#include "iresearch/formats/formats.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/vector_similarity_query.hpp"

namespace irs {

QueryBuilder::ptr ByRadius::PrepareSegment(const SubReader& segment,
                                           const PrepareContext& ctx) const {
  const auto& opts = options();
  if (opts.query.empty() || !field_limits::valid(opts.postings_id)) {
    return QueryBuilder::Empty();
  }

  const auto* postings = segment.field(opts.postings_id);
  const auto* vector_col = segment.Column(field_id());
  const auto* col_reader = segment.GetColReader();
  if (!postings || !vector_col || !col_reader) {
    return QueryBuilder::Empty();
  }
  if (opts.query.size() != vector_col->ArraySize()) {
    return QueryBuilder::Empty();
  }

  auto terms = postings->iterator(SeekMode::NORMAL);
  if (!terms) {
    return QueryBuilder::Empty();
  }
  const auto* term_meta = irs::get<TermMeta>(*terms);

  VectorState state{ctx.memory};
  state.reader = postings;
  state.vector_column = vector_col;

  // Range search probes every cluster: a doc within the ball may be assigned
  // to any cell (cell-radius pruning needs the not-yet-persisted radii).
  CostAttr::Type estimation = 0;
  while (terms->next()) {
    terms->read();
    if (term_meta) {
      estimation += term_meta->docs_count;
    }
    state.cookies.emplace_back(terms->cookie());
  }
  state.estimation = estimation;

  if (state.cookies.empty()) {
    return QueryBuilder::Empty();
  }

  QueryBuilder::ptr inner;
  if (opts.inner) {
    auto inner_ctx = ctx;
    inner_ctx.collector = nullptr;
    inner = opts.inner->PrepareSegment(segment, inner_ctx);
    if (!inner) {
      return QueryBuilder::Empty();
    }
  }

  return memory::make_tracked<RangeVectorQuery>(
    ctx.memory, segment, std::move(state), std::vector<float>{opts.query},
    opts.metric, opts.radius, opts.inclusive, ctx.boost * Boost(),
    std::move(inner));
}

}  // namespace irs
