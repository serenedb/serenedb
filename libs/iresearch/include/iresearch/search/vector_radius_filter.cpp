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

#include <array>
#include <cmath>
#include <span>
#include <vector>

#include "basics/memory.hpp"
#include "iresearch/formats/column/column_reader.hpp"
#include "iresearch/formats/formats.hpp"
#include "iresearch/formats/ivf/centroids.hpp"
#include "iresearch/formats/ivf/ivf_reader.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/vector_filter_util.hpp"
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

  const auto* ivf = field_limits::valid(opts.centroids_id)
                      ? segment.Ivf(opts.centroids_id)
                      : nullptr;
  auto idx_in = segment.ReopenIvf();
  const bool prunable =
    (opts.metric == VectorMetric::L2Sqr || opts.metric == VectorMetric::L1) &&
    ivf != nullptr && !ivf->Empty() && idx_in != nullptr &&
    opts.query.size() == ivf->Dimension();

  CostAttr::Type estimation = 0;
  if (!prunable) {
    while (terms->next()) {
      terms->read();
      if (term_meta) {
        estimation += term_meta->docs_count;
      }
      state.cookies.emplace_back(terms->cookie());
    }
  } else {
    const auto dist = ResolveVectorDistance(opts.metric);
    const auto* q = reinterpret_cast<const byte_type*>(opts.query.data());
    const auto d16 = static_cast<uint16_t>(opts.query.size());
    const float rt =
      opts.metric == VectorMetric::L2Sqr ? std::sqrt(opts.radius) : opts.radius;
    std::array<byte_type, kCentroidTermWidth> term_buf{};
    ivf->ForEachFineCluster(
      *idx_in, [&](uint32_t fine_id, const byte_type* centroid, float radius) {
        const float dqc = dist(q, centroid, d16);
        const float dqc_lin =
          opts.metric == VectorMetric::L2Sqr ? std::sqrt(dqc) : dqc;
        if (dqc_lin - radius > rt) {
          return;
        }
        if (!SeekClusterTerm(*terms, fine_id, term_buf)) {
          return;
        }
        if (term_meta) {
          estimation += term_meta->docs_count;
        }
        state.cookies.emplace_back(terms->cookie());
      });
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
    ctx.memory, segment, std::move(state), std::span<const float>{opts.query},
    opts.metric, opts.radius, opts.inclusive, ctx.boost * Boost(),
    std::move(inner));
}

}  // namespace irs
