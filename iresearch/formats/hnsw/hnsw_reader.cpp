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
#include "iresearch/formats/ivf/ivf_reader.hpp"
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
    const auto read_floats = [&](std::vector<float>& out, size_t count) {
      out.resize(count);
      if (count != 0) {
        in->ReadData(reinterpret_cast<byte_type*>(out.data()),
                     count * sizeof(float));
      }
    };
    const auto read_rotation = [&] {
      std::vector<float> rotation;
      read_floats(rotation, size_t{_header.d} * _header.d);
      data->rotation =
        _bases
          ? _bases->Share(_field, std::move(rotation))
          : std::make_shared<const std::vector<float>>(std::move(rotation));
    };
    if (_header.quant == VectorQuantization::None) {
      read_floats(data->vectors, static_cast<size_t>(_header.rows) * _header.d);
      if (_header.rotated) {
        read_rotation();
        read_floats(data->norms, static_cast<size_t>(_header.rows));
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
        read_floats(data->centroid, _header.d);
      }
      data->codes.resize(static_cast<size_t>(_header.rows) *
                         _header.record_size);
      if (!data->codes.empty()) {
        in->ReadData(data->codes.data(), data->codes.size());
      }
      if (_header.rotated) {
        read_rotation();
        data->byte_rotation = _bases ? _bases->Bytes(data->rotation)
                                     : MakeByteBasis(*data->rotation);
        read_floats(data->tails, static_cast<size_t>(_header.rows) *
                                   HnswCodeTails(_header.d));
      }
    }
    _data = std::move(data);
  });
  return _data;
}

QueryBuilder::ptr HnswIndex::PrepareKnn(const SubReader& segment,
                                        const PrepareContext& ctx,
                                        const VectorFilterOptions& opts,
                                        uint32_t /*effort*/,
                                        AnnQueryCache* cache) const {
  SDB_ASSERT(opts.ef_search != 0);
  return PrepareSearch(segment, ctx, opts,
                       {.ef = std::max(opts.ef_search, opts.min_ef)}, cache);
}

QueryBuilder::ptr HnswIndex::PrepareRange(const SubReader& segment,
                                          const PrepareContext& ctx,
                                          const VectorFilterOptions& opts,
                                          float radius, bool inclusive,
                                          uint32_t /*effort*/,
                                          AnnQueryCache* cache) const {
  return PrepareSearch(segment, ctx, opts,
                       {.threshold = RadiusThreshold(opts.metric, radius),
                        .max_results = static_cast<size_t>(_header.rows),
                        .inclusive = inclusive},
                       cache);
}

QueryBuilder::ptr HnswIndex::PrepareSearch(const SubReader& segment,
                                           const PrepareContext& ctx,
                                           const VectorFilterOptions& opts,
                                           HnswSearchLimits limits,
                                           AnnQueryCache* cache) const {
  SDB_ASSERT(opts.query.size() == _header.d);
  if (Empty()) {
    return QueryBuilder::Empty();
  }
  auto data = Load(segment);
  if (!data || data->graph.Empty()) {
    return QueryBuilder::Empty();
  }
  QueryBuilder::ptr inner;
  if (!PrepareInnerFilter(opts.inner, segment, ctx, inner)) {
    return QueryBuilder::Empty();
  }
  auto built = memory::make_tracked<HnswQuery>(
    ctx.memory, segment, std::move(data), opts, _header.record_size, limits,
    ctx.boost, std::move(inner), cache);
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
  const auto normalized = NormalizedQuery(query, metric, _header.d);
  const auto panorama =
    data->rotation
      ? RotateQuery(nullptr, data->rotation, data->byte_rotation, normalized)
      : nullptr;
  auto bound = data->stats->MakeCodeBound(
    panorama ? panorama->rotated : normalized, metric);
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
