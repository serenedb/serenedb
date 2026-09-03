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

#include <array>
#include <cstdint>
#include <memory>
#include <span>
#include <vector>

#include "iresearch/formats/formats.hpp"
#include "iresearch/formats/ivf/centroids.hpp"
#include "iresearch/formats/ivf/ivf_reader.hpp"
#include "iresearch/formats/ivf/quantizer.hpp"
#include "iresearch/formats/posting_meta.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/ann_index.hpp"
#include "iresearch/search/filter.hpp"
#include "iresearch/search/states/vector_state.hpp"
#include "iresearch/utils/string.hpp"
#include "iresearch/utils/vector.hpp"

namespace irs {

inline bool SeekClusterTerm(auto& terms, uint32_t cluster_id,
                            std::span<byte_type, kCentroidTermWidth> term_buf) {
  EncodeCentroidTerm(cluster_id, term_buf.data());
  return terms.seek(bytes_view{term_buf.data(), term_buf.size()});
}

inline std::shared_ptr<const QuantizerCodebook> ReadQuantizerCodebook(
  const CentroidsTree& ivf, IndexInput& idx_in, VectorQuantization quant,
  uint32_t d, VectorMetric metric, std::span<const float> query) {
  idx_in.Seek(ivf.QuantStatsOffset());
  const auto stats_size = static_cast<size_t>(idx_in.ReadI64());
  std::span<const byte_type> stats;
  bstring owned;
  if (const byte_type* p = idx_in.ReadVolatile(stats_size)) {
    stats = {p, stats_size};
  } else {
    owned.resize(stats_size);
    idx_in.ReadData(owned.data(), stats_size);
    stats = owned;
  }
  auto quant_stats = MakeQuantizerStats(quant, d, stats, metric);
  return quant_stats ? quant_stats->MakeCodebook(query) : nullptr;
}

inline bool PrepareVectorState(const CentroidsTree& ivf,
                               const SubReader& segment,
                               const PrepareContext& ctx,
                               const VectorFilterOptions& opts, uint32_t nprobe,
                               VectorState& state,
                               QueryBuilder::ptr& inner_query,
                               uint32_t max_search_fanout = 1) {
  if (opts.query.empty() || nprobe == 0 ||
      !field_limits::valid(opts.centroids_id) ||
      !field_limits::valid(opts.postings_id)) {
    return false;
  }

  const auto* postings = segment.field(opts.postings_id);
  if (!postings || ivf.Empty() || opts.query.size() != ivf.Dim()) {
    return false;
  }

  auto idx_in = segment.ReopenAnn();
  if (!idx_in) {
    return false;
  }

  const auto d = static_cast<uint32_t>(ivf.Dim());

  std::vector<float> normalized_query;
  std::span<const float> query = opts.query;
  if (opts.metric == VectorMetric::Cosine) {
    normalized_query.resize(query.size());
    vector::L2Space<float, float, float>::Normalize(
      reinterpret_cast<const byte_type*>(query.data()),
      static_cast<uint16_t>(d), normalized_query.data());
    query = normalized_query;
  }

  auto codebook =
    ReadQuantizerCodebook(ivf, *idx_in, opts.quant, d, opts.metric, query);
  if (!codebook) {
    return false;
  }
  const bool needs_centroids = QuantizerNeedsCentroid(opts.quant);

  std::vector<uint32_t> fine_ids;
  std::vector<float> probed_centroids;
  ivf.Search(query, *idx_in, nprobe, fine_ids,
             needs_centroids ? &probed_centroids : nullptr, max_search_fanout);
  if (fine_ids.empty()) {
    return false;
  }

  auto terms = postings->iterator();
  if (!terms) {
    return false;
  }

  state.reader = postings;
  state.vector_column = segment.Column(opts.centroids_id);
  state.quant = opts.quant;
  state.d = d;
  state.codebook = std::move(codebook);

  state.cookies.reserve(fine_ids.size());
  state.pay_starts.reserve(fine_ids.size());
  state.pay_lanes.reserve(fine_ids.size());
  state.cluster_counts.reserve(fine_ids.size());
  if (needs_centroids) {
    state.cluster_centroids.reserve(fine_ids.size() * d);
  }

  std::array<byte_type, kCentroidTermWidth> term_buf{};
  CostAttr::Type estimation = 0;
  for (size_t i = 0; i < fine_ids.size(); ++i) {
    if (!SeekClusterTerm(*terms, fine_ids[i], term_buf)) {
      continue;
    }
    const auto& meta = terms->cookie();
    estimation += meta.docs_count;
    state.pay_starts.push_back(meta.pay_start);
    state.pay_lanes.push_back(meta.pos_offset);
    state.cluster_counts.push_back(meta.docs_count);
    if (needs_centroids) {
      const float* cen = probed_centroids.data() + i * d;
      state.cluster_centroids.insert(state.cluster_centroids.end(), cen,
                                     cen + d);
    }
    state.cookies.emplace_back(meta);
  }
  state.estimation = estimation;

  if (state.cookies.empty()) {
    return false;
  }

  return PrepareInnerFilter(opts.inner, segment, ctx, inner_query);
}

}  // namespace irs
