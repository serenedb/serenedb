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

#include "basics/assert.h"
#include "basics/memory.hpp"
#include "basics/misc.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/queries/hnsw_query.hpp"
#include "iresearch/store/data_input.hpp"

namespace irs {
namespace {

std::vector<float> NormalizedQuery(const VectorFilterOptions& opts,
                                   uint32_t d) {
  std::vector<float> q{opts.query.begin(), opts.query.end()};
  if (opts.metric == VectorMetric::Cosine) {
    std::vector<float> normalized(q.size());
    vector::L2Space<float, float, float>::Normalize(
      reinterpret_cast<const byte_type*>(q.data()), static_cast<uint16_t>(d),
      normalized.data());
    q = std::move(normalized);
  }
  return q;
}

}  // namespace

HnswHeader HnswIndex::ReadHeader(IndexInput& in) {
  HnswHeader h;
  h.version = static_cast<uint32_t>(in.ReadI32());
  h.d = static_cast<uint32_t>(in.ReadI32());
  h.metric = static_cast<VectorMetric>(in.ReadI32());
  h.quant = static_cast<VectorQuantization>(in.ReadI32());
  h.ef_construction = static_cast<uint32_t>(in.ReadI32());
  h.record_size = static_cast<uint32_t>(in.ReadI32());
  h.rows = static_cast<uint64_t>(in.ReadI64());
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

QueryBuilder::ptr HnswIndex::PrepareKnn(const SubReader& segment,
                                        const PrepareContext& ctx,
                                        const VectorFilterOptions& opts,
                                        uint32_t /*effort*/) const {
  SDB_ASSERT(opts.query.size() == _header.d);
  if (Empty()) {
    return QueryBuilder::Empty();
  }
  auto data = Load(segment);
  if (!data || data->graph.Empty()) {
    return QueryBuilder::Empty();
  }
  auto query = NormalizedQuery(opts, _header.d);
  auto codebook = data->stats ? data->stats->MakeCodebook(query) : nullptr;
  SDB_ASSERT(!data->stats || codebook);
  SDB_ASSERT(opts.ef_search != 0);
  const auto ef = std::max(opts.ef_search, opts.min_ef);
  auto built = memory::make_tracked<HnswQuery>(
    ctx.memory, segment, std::move(data), std::move(codebook), std::move(query),
    opts.metric, _header.d, _header.record_size, ef, kHnswNoThreshold,
    /*max_results=*/0, /*inclusive=*/false, ctx.boost);
  built->SetStats(ctx.Record());
  return built;
}

QueryBuilder::ptr HnswIndex::PrepareRange(const SubReader& segment,
                                          const PrepareContext& ctx,
                                          const VectorFilterOptions& opts,
                                          float radius, bool inclusive,
                                          uint32_t /*effort*/) const {
  SDB_ASSERT(opts.query.size() == _header.d);
  if (Empty()) {
    return QueryBuilder::Empty();
  }
  auto data = Load(segment);
  if (!data || data->graph.Empty()) {
    return QueryBuilder::Empty();
  }
  auto query = NormalizedQuery(opts, _header.d);
  auto codebook = data->stats ? data->stats->MakeCodebook(query) : nullptr;
  SDB_ASSERT(!data->stats || codebook);
  const bool angular = opts.metric == VectorMetric::InnerProduct ||
                       opts.metric == VectorMetric::Cosine;
  const score_t threshold = angular ? radius : -radius;
  auto built = memory::make_tracked<HnswQuery>(
    ctx.memory, segment, std::move(data), std::move(codebook), std::move(query),
    opts.metric, _header.d, _header.record_size, /*ef=*/0, threshold,
    static_cast<size_t>(_header.rows), inclusive, ctx.boost);
  built->SetStats(ctx.Record());
  return built;
}

}  // namespace irs
