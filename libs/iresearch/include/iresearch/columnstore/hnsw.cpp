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
#include <array>
#include <cmath>
#include <duckdb/common/types.hpp>
#include <duckdb/common/types/vector.hpp>
#include <duckdb/common/vector/array_vector.hpp>
#include <duckdb/storage/table/column_segment.hpp>
#include <duckdb/storage/table/scan_state.hpp>

#include "basics/assert.h"
#include "basics/containers/node_hash_map.h"
#include "basics/containers/s3fifo.h"
#include "basics/exceptions.h"
#include "iresearch/columnstore/column_reader.hpp"
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

// Distance computer over the ARRAY child column. doc_id space is 1-based
// (faiss::idx_t == iresearch doc_id == row + doc_limits::min()), so the
// access key is (id - doc_limits::min()). set_query stores the
// caller-owned query pointer; dis(id) fetches one slice and forwards
// it to dist(). symmetric_dis(i, j) needs both slices to coexist for
// the dist() call -- pin the i-side chunk before fetching j so the
// second cache->Get's eviction can't free `a` out from under us.
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

void HNSWWriter::Build(const ColumnReader& vector_column) {
  const auto* child = vector_column.Child();
  SDB_ASSERT(child);
  const auto array_size = vector_column.ArraySize();
  const auto rows = vector_column.RowCount();

  // faiss node N == iresearch doc_id (1-based) for row (N - 1).
  const auto graph_nodes = rows + doc_limits::min();
  faiss::VisitedTable vt{static_cast<int>(graph_nodes)};
  auto& hnsw = *_hnsw;
  hnsw.prepare_level_tab(graph_nodes, false);

  ChunkedVectorCache cache{*child, array_size};
  ColumnDistance dis{_info, &cache};

  const uint64_t chunk_rows =
    std::max<uint64_t>(kChunkSizeFloats / std::max<uint64_t>(array_size, 1), 1);
  for (uint64_t start = 0; start < rows; start += chunk_rows) {
    const uint64_t take = std::min<uint64_t>(chunk_rows, rows - start);
    const float* base = cache.Pin(start);
    for (uint64_t k = 0; k < take; ++k) {
      dis.set_query(base + k * array_size);
      const faiss::idx_t id =
        static_cast<faiss::idx_t>(start + k + doc_limits::min());
      const int level = hnsw.levels[id] - 1;
      vt.advance();
      hnsw.add_with_locks(dis, level, id, vt, false);
    }
    cache.Unpin();
  }
}

void HNSWWriter::Serialize(DataOutput& out) { irs::WriteHNSW(out, *_hnsw); }

HNSWReader::HNSWReader(field_id id, std::shared_ptr<faiss::HNSW> hnsw,
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
                               ctx.info.global_threshold};
  ctx.vt.visited.resize(hnsw.levels.size(), 0);
  ctx.vt.advance();
  res.begin(0, false);
  hnsw.search(dis, nullptr, res, ctx.vt, &ctx.info.params);
}

void HNSWReader::RangeSearch(HNSWRangeSearchContext& ctx) const {
  const auto& hnsw = *_hnsw;
  ColumnDistance dis{_info, &ctx.cache};
  dis.set_query(reinterpret_cast<const float*>(ctx.info.query));

  HNSWRangeSegmentResultHandler res{ctx.segment_id, ctx.handler};
  ctx.vt.visited.resize(hnsw.levels.size(), 0);
  ctx.vt.advance();
  res.begin(0);
  hnsw.search(dis, nullptr, res, ctx.vt, &ctx.info.params);
}

ChunkedVectorCache& HNSWReader::PrepareCache(ChunkedVectorCache& slot) const {
  const auto* child = _vector_column.Child();
  SDB_ASSERT(child);
  slot.Rebind(*child, _vector_column.ArraySize());
  return slot;
}

void ChunkedVectorCache::Rebind(const ColumnReader& child,
                                uint64_t array_size) {
  SDB_ASSERT(_pin_depth == 0);
  _slots.Clear();
  _rgs.Clear();
  _chunk_index.clear();
  _rg_index.clear();
  _locate_hint = {};
  _child = &child;
  _d = array_size;
  _chunk_rows =
    std::max<uint64_t>(kChunkSizeFloats / std::max<uint64_t>(_d, 1), 1);
}

ChunkSlot* ChunkedVectorCache::FindOrLoad(uint64_t row) {
  const uint64_t chunk_id = row / _chunk_rows;
  auto [it, inserted] = _chunk_index.try_emplace(
    chunk_id, static_cast<duckdb::idx_t>(_chunk_rows * _d));
  if (!inserted) {
    it->second.Touch();
    return &it->second;
  }
  ChunkSlot& slot = it->second;
  slot.chunk_id = chunk_id;
  slot.pinned = 0;
  Load(slot, chunk_id);
  _slots.Insert(slot);
  while (!_chunk_evicted.empty()) {
    auto& s = _chunk_evicted.front();
    _chunk_evicted.pop_front();
    _chunk_index.erase(s.chunk_id);
  }
  return &slot;
}

RgSlot& ChunkedVectorCache::GetRgSlot(size_t rg) {
  auto [it, inserted] = _rg_index.try_emplace(rg);
  if (!inserted) {
    it->second.Touch();
    return it->second;
  }
  RgSlot& slot = it->second;
  slot.rg = rg;
  slot.cursor = ColumnReader::ScanCursor{_child->OpenSegment(rg)};
  _rgs.Insert(slot);
  while (!_rg_evicted.empty()) {
    auto& s = _rg_evicted.front();
    _rg_evicted.pop_front();
    _rg_index.erase(s.rg);
  }
  return slot;
}

void ChunkedVectorCache::Load(ChunkSlot& slot, uint64_t chunk_id) {
  const uint64_t total_rows = _child->RowCount() / _d;
  const uint64_t start_row = chunk_id * _chunk_rows;
  const uint64_t take = std::min<uint64_t>(_chunk_rows, total_rows - start_row);
  const uint64_t start_elem = start_row * _d;
  const uint64_t take_elems = take * _d;

  uint64_t produced = 0;
  while (produced < take_elems) {
    const uint64_t pos = start_elem + produced;
    _locate_hint = _child->Locate(pos, _locate_hint);
    const uint64_t in_rg = pos - _locate_hint.begin;
    const uint64_t to_take =
      std::min<uint64_t>(take_elems - produced, _locate_hint.end - pos);

    auto& rgs = GetRgSlot(_locate_hint.rg);
    if (in_rg < rgs.cursor.Position()) {
      rgs.cursor.Rewind();
    }
    rgs.cursor.SeekTo(in_rg);
    rgs.cursor.Scan(to_take, slot.data, static_cast<duckdb::idx_t>(produced));
    produced += to_take;
  }
}

bool ChunkEvictor::operator()(ChunkSlot& slot) const noexcept {
  if (slot.pinned != 0) {
    return false;
  }
  self->_chunk_evicted.push_back(slot);
  return true;
}

bool RgEvictor::operator()(RgSlot& slot) const noexcept {
  self->_rg_evicted.push_back(slot);
  return true;
}

}  // namespace irs::columnstore
