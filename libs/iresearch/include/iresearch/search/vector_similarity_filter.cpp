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
#include <cmath>
#include <unordered_map>
#include <vector>

#include "basics/memory.hpp"
#include "iresearch/formats/formats.hpp"
#include "iresearch/formats/index/idx_reader.hpp"
#include "iresearch/formats/ivf/centroids.hpp"
#include "iresearch/formats/ivf/ivf_reader.hpp"
#include "iresearch/formats/posting/common.hpp"
#include "iresearch/index/index_features.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/vector_similarity_query.hpp"

namespace irs {

QueryBuilder::ptr ByVectorSimilarity::PrepareSegment(
  const SubReader& segment, const PrepareContext& ctx) const {
  const auto& opts = options();
  if (opts.query.empty() || opts.nprobe == 0 ||
      !field_limits::valid(opts.centroids_id) ||
      !field_limits::valid(opts.postings_id)) {
    return QueryBuilder::Empty();
  }

  const auto* postings = segment.field(opts.postings_id);
  const auto* ivf = segment.Ivf(opts.centroids_id);
  const auto* vector_col = segment.Column(field_id());
  if (!postings || !ivf || !vector_col || ivf->Empty() ||
      opts.query.size() != ivf->Dimension()) {
    return QueryBuilder::Empty();
  }

  const uint32_t n1 = static_cast<uint32_t>(std::max<double>(
    1.0, std::ceil(std::sqrt(static_cast<double>(opts.nprobe)))));
  const uint32_t n2 = n1;

  std::vector<uint32_t> l1_ids;
  ivf->SearchL1(opts.query, n1, l1_ids);
  if (l1_ids.empty()) {
    return QueryBuilder::Empty();
  }

  auto idx_in = segment.ReopenIvf();
  if (!idx_in) {
    return QueryBuilder::Empty();
  }

  const bool has_pay = IndexFeatures::None !=
                       (postings->meta().index_features & IndexFeatures::Pay);
  const bool metric_ok = opts.metric == VectorMetric::L2Sqr ||
                         opts.metric == VectorMetric::InnerProduct;
  const VectorQuantization quant =
    (has_pay && metric_ok && !ivf->QuantStats().empty())
      ? opts.quant
      : VectorQuantization::None;
  const bool pq = quant == VectorQuantization::PQ;
  const uint32_t d = ivf->Dimension();

  std::vector<uint32_t> fine_ids;
  std::unordered_map<uint32_t, uint32_t> fine_centroid_off;
  std::vector<float> probed_centroids;
  L2BodyView body;
  for (const uint32_t l1 : l1_ids) {
    ivf->ReadL2Body(*idx_in, l1, body);
    ivf->SearchL2(opts.query, body, n2, fine_ids);
    if (pq) {
      const auto* cv = reinterpret_cast<const float*>(body.l2_centroids);
      for (uint32_t s = 0; s < body.n_l2; ++s) {
        const uint32_t id = body.fine_ids[s];
        if (fine_centroid_off.emplace(id, probed_centroids.size()).second) {
          const float* cen = cv + static_cast<size_t>(s) * d;
          probed_centroids.insert(probed_centroids.end(), cen, cen + d);
        }
      }
    }
  }
  std::sort(fine_ids.begin(), fine_ids.end());
  fine_ids.erase(std::unique(fine_ids.begin(), fine_ids.end()), fine_ids.end());
  if (fine_ids.empty()) {
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
  state.quant = quant;
  state.d = d;
  if (quant != VectorQuantization::None) {
    const auto stats = ivf->QuantStats();
    state.quant_stats.assign(stats.begin(), stats.end());
  }

  std::array<byte_type, kCentroidTermWidth> term_buf{};
  CostAttr::Type estimation = 0;
  for (const uint32_t c : fine_ids) {
    EncodeCentroidTerm(c, term_buf.data());
    if (!terms->seek(bytes_view{term_buf.data(), term_buf.size()})) {
      continue;
    }
    terms->read();
    if (term_meta) {
      estimation += term_meta->docs_count;
    }
    if (quant != VectorQuantization::None) {
      state.pay_starts.push_back(
        static_cast<const TermMetaImpl*>(term_meta)->pay_start);
      state.cluster_counts.push_back(term_meta->docs_count);
    }
    if (pq) {
      const auto it = fine_centroid_off.find(c);
      const float* cen = it != fine_centroid_off.end()
                           ? probed_centroids.data() + it->second
                           : nullptr;
      if (cen != nullptr) {
        state.cluster_centroids.insert(state.cluster_centroids.end(), cen,
                                       cen + d);
      } else {
        state.cluster_centroids.insert(state.cluster_centroids.end(), d, 0.f);
      }
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

  return memory::make_tracked<KnnVectorQuery>(
    ctx.memory, segment, std::move(state), std::vector<float>{opts.query},
    opts.metric, ctx.boost * Boost(), std::move(inner));
}

}  // namespace irs
