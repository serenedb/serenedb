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

#include "iresearch/formats/hnsw/hnsw_reader.hpp"

#include <algorithm>
#include <cstring>
#include <limits>
#include <span>
#include <utility>

#include "iresearch/formats/hnsw/hnsw_bases.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/queries/hnsw_query.hpp"
#include "iresearch/store/data_input.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/memory.hpp"
#include "iresearch/utils/misc.hpp"

namespace irs {
namespace {

std::vector<float> NormalizedQuery(std::span<const float> query,
                                   VectorMetric metric, uint32_t d) {
  std::vector<float> q{query.begin(), query.end()};
  if (metric == VectorMetric::Cosine) {
    std::vector<float> normalized(q.size());
    duckdb::L2NormalizeOp::Operation(q.data(), normalized.data(), d);
    q = std::move(normalized);
  }
  return q;
}

}  // namespace

HnswHeader HnswIndex::ReadHeader(IndexInput& in) {
  HnswHeader h;
  h.d = static_cast<uint32_t>(in.ReadI32());
  h.metric = static_cast<VectorMetric>(in.ReadI32());
  h.quant = static_cast<VectorQuantization>(in.ReadI32());
  h.ef_construction = static_cast<uint32_t>(in.ReadI32());
  h.record_size = static_cast<uint32_t>(in.ReadI32());
  h.rows = static_cast<uint64_t>(in.ReadI64());
  h.rotated = in.ReadByte() != 0;
  return h;
}

std::shared_ptr<const HnswData> HnswIndex::Load(
  const SubReader& segment) const {
  absl::call_once(_once, [&] {
    auto in = segment.ReopenAnn();
    if (!in) {
      return;
    }
    in->Seek(_meta.offset);
    IRS_IGNORE(ReadHeader(*in));
    auto data = std::make_shared<HnswData>();
    data->graph = HnswGraph::Deserialize(*in);
    if (_header.quant == VectorQuantization::None) {
      data->vectors.resize(static_cast<size_t>(_header.rows) * _header.d);
      if (!data->vectors.empty()) {
        in->ReadData(reinterpret_cast<byte_type*>(data->vectors.data()),
                     data->vectors.size() * sizeof(float));
      }
      if (_header.rotated) {
        std::vector<float> rotation(size_t{_header.d} * _header.d);
        in->ReadData(reinterpret_cast<byte_type*>(rotation.data()),
                     rotation.size() * sizeof(float));
        data->rotation =
          _bases
            ? _bases->Share(_field, std::move(rotation))
            : std::make_shared<const std::vector<float>>(std::move(rotation));
        data->norms.resize(static_cast<size_t>(_header.rows));
        in->ReadData(reinterpret_cast<byte_type*>(data->norms.data()),
                     data->norms.size() * sizeof(float));
      }
    } else {
      const auto stats_size = static_cast<size_t>(in->ReadI64());
      bstring stats;
      stats.resize(stats_size);
      if (stats_size != 0) {
        in->ReadData(stats.data(), stats_size);
      }
      data->stats = MakeQuantizerStats(_header.quant, _header.d, stats,
                                       EffectiveQuantMetric(_header.metric),
                                       /*row_major=*/true);
      if (!data->stats) {
        return;
      }
      if (QuantizerNeedsCentroid(_header.quant)) {
        data->centroid.resize(_header.d);
        in->ReadData(reinterpret_cast<byte_type*>(data->centroid.data()),
                     data->centroid.size() * sizeof(float));
      }
      data->codes.resize(static_cast<size_t>(_header.rows) *
                         _header.record_size);
      if (!data->codes.empty()) {
        in->ReadData(data->codes.data(), data->codes.size());
      }
    }
    _data = std::move(data);
  });
  return _data;
}

namespace {

std::shared_ptr<const PanoramaQuery> MakePanoramaQuery(
  const HnswData& data, std::span<const float> query, AnnQueryCache* cache) {
  if (!data.rotation) {
    return nullptr;
  }
  return cache ? cache->Get(data.rotation, query)
               : AnnQueryCache{}.Get(data.rotation, query);
}

}  // namespace

QueryBuilder::ptr HnswIndex::PrepareKnn(const SubReader& segment,
                                        const PrepareContext& ctx,
                                        const VectorFilterOptions& opts,
                                        uint32_t /*effort*/,
                                        AnnQueryCache* cache) const {
  SDB_ASSERT(opts.query.size() == _header.d);
  if (Empty()) {
    return QueryBuilder::Empty();
  }
  auto data = Load(segment);
  if (!data || data->graph.Empty()) {
    return QueryBuilder::Empty();
  }
  auto query = NormalizedQuery(opts.query, opts.metric, _header.d);
  auto panorama = MakePanoramaQuery(*data, query, cache);
  auto codebook = data->stats ? data->stats->MakeCodebook(query) : nullptr;
  SDB_ASSERT(!data->stats || codebook);
  SDB_ASSERT(opts.ef_search != 0);
  const auto ef = std::max(opts.ef_search, opts.min_ef);
  QueryBuilder::ptr inner;
  if (!PrepareInnerFilter(opts.inner, segment, ctx, inner)) {
    return QueryBuilder::Empty();
  }
  auto built = memory::make_tracked<HnswQuery>(
    ctx.memory, segment, std::move(data), std::move(codebook), std::move(query),
    opts.metric, _header.d, _header.record_size, ef, kHnswNoThreshold,
    /*max_results=*/size_t{0}, /*inclusive=*/false, ctx.boost, std::move(inner),
    opts.hnsw_filter_mode, opts.hnsw_column_filter, /*exact=*/nullptr,
    std::move(panorama));
  built->SetStats(ctx.Record());
  return built;
}

QueryBuilder::ptr HnswIndex::PrepareRange(const SubReader& segment,
                                          const PrepareContext& ctx,
                                          const VectorFilterOptions& opts,
                                          float radius, bool inclusive,
                                          uint32_t /*effort*/,
                                          AnnQueryCache* cache) const {
  SDB_ASSERT(opts.query.size() == _header.d);
  if (Empty()) {
    return QueryBuilder::Empty();
  }
  auto data = Load(segment);
  if (!data || data->graph.Empty()) {
    return QueryBuilder::Empty();
  }
  auto query = NormalizedQuery(opts.query, opts.metric, _header.d);
  auto panorama = MakePanoramaQuery(*data, query, cache);
  auto codebook = data->stats ? data->stats->MakeRangeCodebook(query) : nullptr;
  SDB_ASSERT(!data->stats || codebook);
  const auto* exact = data->stats ? segment.Column(opts.centroids_id) : nullptr;
  const bool angular = opts.metric == VectorMetric::InnerProduct ||
                       opts.metric == VectorMetric::Cosine;
  const score_t threshold = angular ? radius : -radius;
  QueryBuilder::ptr inner;
  if (!PrepareInnerFilter(opts.inner, segment, ctx, inner)) {
    return QueryBuilder::Empty();
  }
  auto built = memory::make_tracked<HnswQuery>(
    ctx.memory, segment, std::move(data), std::move(codebook), std::move(query),
    opts.metric, _header.d, _header.record_size, /*ef=*/uint32_t{0}, threshold,
    static_cast<size_t>(_header.rows), inclusive, ctx.boost, std::move(inner),
    opts.hnsw_filter_mode, opts.hnsw_column_filter, exact, std::move(panorama));
  built->SetStats(ctx.Record());
  return built;
}

AnnExactCodes HnswIndex::PrepareExact(const SubReader& segment,
                                      std::span<const float> query,
                                      VectorMetric metric) const {
  if (Empty() || query.size() != _header.d ||
      (metric == VectorMetric::Cosine) !=
        (_header.metric == VectorMetric::Cosine)) {
    return {};
  }
  auto data = Load(segment);
  if (!data || !data->stats || data->codes.empty()) {
    return {};
  }
  auto bound = data->stats->MakeCodeBound(
    NormalizedQuery(query, metric, _header.d), metric);
  if (!bound) {
    return {};
  }
  AnnExactCodes codes{.bound = std::move(bound),
                      .codes = data->codes.data(),
                      .levels = data->graph.Levels(),
                      .record_size = _header.record_size};
  codes.owner = std::move(data);
  return codes;
}

}  // namespace irs
