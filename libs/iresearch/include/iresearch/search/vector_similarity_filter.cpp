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

#include <algorithm>
#include <array>
#include <limits>
#include <vector>

#include "basics/memory.hpp"
#include "iresearch/formats/formats.hpp"
#include "iresearch/formats/index/idx_reader.hpp"
#include "iresearch/formats/ivf/ivf_reader.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/vector_similarity_query.hpp"

namespace irs {

Filter::Query::ptr ByVectorSimilarity::prepare(
  const PrepareContext& ctx) const {
  const auto& opts = options();
  if (opts.query.empty() || opts.nprobe == 0 ||
      !field_limits::valid(opts.centroids_id) ||
      !field_limits::valid(opts.postings_id)) {
    return Query::empty();
  }

  const auto dist = ResolveVectorDistance(opts.metric);
  VectorStates states{ctx.memory, ctx.index.size()};

  std::vector<uint32_t> probes;
  for (const auto& segment : ctx.index) {
    const auto* postings = segment.field(opts.postings_id);
    const auto* ivf = segment.Ivf(opts.centroids_id);
    const auto* vector_col = segment.Column(field_id());
    if (!postings || !ivf || !vector_col) {
      continue;
    }

    if (ivf->nlist == 0 || opts.query.size() != ivf->d) {
      continue;
    }
    const IvfCentroids centroids{
      .data = ivf->centroids.data(), .nlist = ivf->nlist, .d = ivf->d};

    probes.clear();
    SelectNearestCentroids(opts.query.data(), centroids, opts.nprobe, dist,
                           VectorMetricNearestIsLargest(opts.metric), probes);
    if (probes.empty()) {
      continue;
    }
    // Seek the cluster terms in ascending centroid-id (== ascending term) order
    // so the dictionary cursor only moves forward.
    std::sort(probes.begin(), probes.end());

    auto terms = postings->iterator(SeekMode::NORMAL);
    if (!terms) {
      continue;
    }
    const auto* term_meta = irs::get<TermMeta>(*terms);

    auto& state = states.insert(segment);
    state.reader = postings;
    state.vector_column = vector_col;

    std::array<byte_type, kCentroidTermWidth> term_buf{};
    CostAttr::Type estimation = 0;
    for (const uint32_t c : probes) {
      EncodeCentroidTerm(c, term_buf.data());
      if (!terms->seek(bytes_view{term_buf.data(), term_buf.size()})) {
        continue;
      }
      terms->read();
      if (term_meta) {
        estimation += term_meta->docs_count;
      }
      state.cookies.emplace_back(terms->cookie());
    }
    state.estimation = estimation;
  }

  if (states.empty()) {
    return Query::empty();
  }

  Filter::Query::ptr inner;
  if (opts.inner) {
    auto inner_ctx = ctx;
    inner_ctx.scorer = nullptr;
    inner = opts.inner->prepare(inner_ctx);
    if (!inner) {
      return Query::empty();
    }
  }

  return memory::make_tracked<VectorSimilarityQuery>(
    ctx.memory, std::move(states), std::vector<float>{opts.query}, opts.metric,
    std::numeric_limits<float>::infinity(), /*inclusive=*/true,
    ctx.boost * Boost(), std::move(inner));
}

}  // namespace irs
