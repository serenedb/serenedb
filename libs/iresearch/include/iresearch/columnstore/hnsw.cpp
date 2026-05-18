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

#include "iresearch/columnstore/hnsw.hpp"

#include <faiss/impl/DistanceComputer.h>
#include <faiss/impl/ResultHandler.h>

#include <algorithm>
#include <cmath>
#include <duckdb/common/types.hpp>
#include <duckdb/common/types/vector.hpp>
#include <duckdb/common/vector/array_vector.hpp>
#include <duckdb/storage/table/column_segment.hpp>
#include <duckdb/storage/table/scan_state.hpp>

#include "basics/assert.h"
#include "basics/exceptions.h"
#include "iresearch/columnstore/column_reader.hpp"
#include "iresearch/columnstore/read_context.hpp"
#include "iresearch/formats/column/hnsw_index.hpp"
#include "iresearch/store/data_input.hpp"
#include "iresearch/store/data_output.hpp"
#include "iresearch/utils/vector.hpp"

namespace irs::columnstore {
namespace {

float ComputeNegativeInnerProduct(const byte_type* l, const byte_type* r,
                                  uint16_t d) {
  return -irs::vector::DotProductImpl<float, float>::Compute(l, r, d);
}

float ComputeCosine(const byte_type* l, const byte_type* r, uint16_t d) {
  const auto [ll, lr, rr] =
    irs::vector::CosineDistanceImpl<float, float, float>::Compute(l, r, d);
  const float denom = std::sqrt(ll) * std::sqrt(rr);
  return denom == 0.f ? 1.f : 1.f - lr / denom;
}

auto ResolveDistanceFunction(HNSWMetric metric) {
  switch (metric) {
    case HNSWMetric::L2Sqr:
      return irs::vector::L2Space<float, float, float>::Dist;
    case HNSWMetric::NegativeIP:
      return ComputeNegativeInnerProduct;
    case HNSWMetric::L1:
      return irs::vector::L1Space<float, float, float>::Dist;
    case HNSWMetric::Cosine:
      return ComputeCosine;
  }
  SDB_UNREACHABLE();
}

struct ColumnDistance final : public faiss::DistanceComputer {
  ColumnDistance(HNSWInfo info, ChunkedVectorCache* cache) noexcept
    : dim{info.d}, dist{ResolveDistanceFunction(info.metric)}, cache{cache} {}

  void set_query(const float* x) final { q = x; }

  float operator()(faiss::idx_t id) final {
    const auto* slice =
      cache->Get(static_cast<uint64_t>(id - doc_limits::min()));
    return dist(reinterpret_cast<const byte_type*>(q),
                reinterpret_cast<const byte_type*>(slice),
                static_cast<uint16_t>(dim));
  }

  float symmetric_dis(faiss::idx_t i, faiss::idx_t j) final {
    const auto* a = cache->Pin(static_cast<uint64_t>(i - doc_limits::min()));
    const auto* b = cache->Get(static_cast<uint64_t>(j - doc_limits::min()));
    const auto r =
      dist(reinterpret_cast<const byte_type*>(a),
           reinterpret_cast<const byte_type*>(b), static_cast<uint16_t>(dim));
    cache->Unpin();
    return r;
  }

  int32_t dim;
  float (*dist)(const byte_type*, const byte_type*, uint16_t);
  ChunkedVectorCache* cache;
  const float* q = nullptr;
};

}  // namespace

HNSWWriter::HNSWWriter(HNSWInfo info)
  : _info{std::move(info)}, _hnsw{std::make_shared<faiss::HNSW>(_info.m)} {
  _hnsw->efConstruction = _info.ef_construction;
}

HNSWWriter::~HNSWWriter() = default;

void HNSWWriter::Build(const ColumnReader& vector_column, ReadContext& ctx) {
  const auto* child = vector_column.Child();
  SDB_ASSERT(child);
  const auto array_size = vector_column.ArraySize();
  const auto rows = vector_column.RowCount();

  // faiss node N == iresearch doc_id (1-based) for row (N - 1).
  const auto graph_nodes = rows + doc_limits::min();
  faiss::VisitedTable vt{static_cast<int>(graph_nodes)};
  auto& hnsw = *_hnsw;
  hnsw.prepare_level_tab(graph_nodes, false);

  ChunkedVectorCache cache;
  cache.Rebind(*child, array_size, ctx);
  ColumnDistance dis{_info, &cache};

  const uint64_t chunk_rows =
    std::max<uint64_t>(kChunkSizeFloats / std::max<uint64_t>(array_size, 1), 1);
  const auto add_row = [&](uint64_t row, const float* base, uint64_t base_row) {
    dis.set_query(base + (row - base_row) * array_size);
    const faiss::idx_t id = static_cast<faiss::idx_t>(row + doc_limits::min());
    const int level = hnsw.levels[id] - 1;
    vt.advance();
    hnsw.add_with_locks(dis, level, id, vt, false);
  };

  if (!vector_column.HasValidity()) {
    // Fast path: every row is valid -- no validity Vector / Scan needed.
    for (uint64_t start = 0; start < rows; start += chunk_rows) {
      const uint64_t take = std::min<uint64_t>(chunk_rows, rows - start);
      const float* base = cache.Pin(start);
      for (uint64_t k = 0; k < take; ++k) {
        add_row(start + k, base, start);
      }
      cache.Unpin();
    }
    return;
  }

  duckdb::Vector vbatch{vector_column.Type(), /*capacity=*/0};
  vbatch.BufferMutable().GetValidityMask().Initialize(STANDARD_VECTOR_SIZE);
  ColumnReader::RangeScan vscan{vector_column, ctx, /*validity_side=*/true};
  for (uint64_t start = 0; start < rows; start += chunk_rows) {
    const uint64_t take = std::min<uint64_t>(chunk_rows, rows - start);
    const float* base = cache.Pin(start);
    uint64_t sub = 0;
    while (sub < take) {
      const auto vtake =
        std::min<duckdb::idx_t>(take - sub, STANDARD_VECTOR_SIZE);
      vscan.Scan(start + sub, vtake, vbatch, /*out_offset=*/0);
      const auto& vmask = vbatch.Buffer().GetValidityMask();
      for (uint64_t k = 0; k < vtake; ++k) {
        if (vmask.RowIsValid(k)) {
          add_row(start + sub + k, base, start);
        }
      }
      sub += vtake;
    }
    cache.Unpin();
  }
}

void HNSWWriter::Serialize(DataOutput& out) { irs::WriteHNSW(out, *_hnsw); }

HNSWReader::HNSWReader(field_id id, std::shared_ptr<const faiss::HNSW> hnsw,
                       HNSWInfo info, const ColumnReader& vector_column)
  : _id{id},
    _info{std::move(info)},
    _vector_column{vector_column},
    _hnsw{std::move(hnsw)} {
  SDB_ENSURE(vector_column.ArraySize() == static_cast<uint64_t>(_info.d),
             sdb::ERROR_INTERNAL, "columnstore::HNSWReader: ARRAY size ",
             vector_column.ArraySize(), " does not match HNSWInfo.d ", _info.d);
}

HNSWReader::~HNSWReader() = default;

void HNSWReader::Search(HNSWSearchContext& ctx) const {
  const auto& hnsw = *_hnsw;
  ColumnDistance dis{_info, &ctx.cache};
  dis.set_query(reinterpret_cast<const float*>(ctx.info.query));

  HNSWSegmentResultHandler res{ctx.segment_id, ctx.handler,
                               ctx.info.global_threshold, ctx.docs_mask};
  ctx.vt.visited.resize(hnsw.levels.size(), 0);
  ctx.vt.advance();
  res.begin(0, false);
  hnsw.search(dis, nullptr, res, ctx.vt, &ctx.info.params);
}

void HNSWReader::RangeSearch(HNSWRangeSearchContext& ctx) const {
  const auto& hnsw = *_hnsw;
  ColumnDistance dis{_info, &ctx.cache};
  dis.set_query(reinterpret_cast<const float*>(ctx.info.query));

  HNSWRangeSegmentResultHandler res{ctx.segment_id, ctx.handler, ctx.docs_mask};
  ctx.vt.visited.resize(hnsw.levels.size(), 0);
  ctx.vt.advance();
  res.begin(0);
  hnsw.search(dis, nullptr, res, ctx.vt, &ctx.info.params);
}

ChunkedVectorCache& HNSWReader::PrepareCache(ChunkedVectorCache& slot,
                                             ReadContext& ctx) const {
  const auto* child = _vector_column.Child();
  SDB_ASSERT(child);
  slot.Rebind(*child, _vector_column.ArraySize(), ctx);
  return slot;
}

}  // namespace irs::columnstore
