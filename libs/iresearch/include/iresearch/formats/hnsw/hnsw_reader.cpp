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

#include <faiss/impl/DistanceComputer.h>
#include <faiss/impl/ResultHandler.h>

#include <algorithm>
#include <cmath>
#include <duckdb/common/types.hpp>
#include <duckdb/common/types/vector.hpp>
#include <utility>

#include "basics/assert.h"
#include "basics/exceptions.h"
#include "basics/system-compiler.h"
#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/formats/column/column_reader.hpp"
#include "iresearch/formats/column/read_context.hpp"
#include "iresearch/formats/formats.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/store/data_input.hpp"
#include "iresearch/store/data_output.hpp"
#include "iresearch/utils/attribute_provider.hpp"
#include "iresearch/utils/vector.hpp"

namespace irs {
namespace {

template<typename T>
void ReadVector(IndexInput& in, T& vec) {
  uint32_t size = irs::read<uint32_t>(in);
  vec.resize(size);
  SDB_ASSERT(size != 0);
  in.ReadData(reinterpret_cast<byte_type*>(vec.data()),
              sizeof(*vec.data()) * size);
}

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

void ReadHNSW(IndexInput& in, faiss::HNSW& hnsw) {
  ReadVector(in, hnsw.assign_probas);
  ReadVector(in, hnsw.cum_nneighbor_per_level);
  ReadVector(in, hnsw.levels);
  ReadVector(in, hnsw.offsets);
  ReadVector(in, hnsw.neighbors);

  hnsw.entry_point = irs::read<int32_t>(in);
  hnsw.max_level = irs::read<int>(in);
  hnsw.efConstruction = irs::read<int>(in);
  hnsw.efSearch = irs::read<int>(in);
}

uint64_t PackSegmentWithDoc(uint32_t segment, doc_id_t doc) {
  return (static_cast<uint64_t>(segment) << 32) | static_cast<uint64_t>(doc);
}

std::pair<uint32_t, doc_id_t> UnpackSegmentWithDoc(uint64_t id) {
  uint32_t segment = static_cast<uint32_t>(id >> 32);
  doc_id_t doc =
    static_cast<doc_id_t>(id & std::numeric_limits<uint32_t>::max());
  return {segment, doc};
}

ColumnDistanceBase::ColumnDistanceBase(HNSWMetric metric, int32_t dim)
  : _dist{ResolveDistanceFunction(metric)}, _dim{dim} {}

const float* ColumnDistanceBase::LoadData(faiss::idx_t id,
                                          ResettableDocIterator::ptr& it) {
  it->reset();
  doc_id_t doc = static_cast<doc_id_t>(id);
  auto next_doc = it->seek(doc);
  if (next_doc != doc) {
    SDB_THROW(sdb::ERROR_INTERNAL, "Failed to load vector data for doc: ", doc);
  }
  SDB_ASSERT(doc == next_doc);
  auto* payload = irs::get<PayAttr>(*it);
  SDB_ASSERT(payload);
  SDB_ASSERT(payload->value.size() == _dim * sizeof(float));
  return reinterpret_cast<const float*>(payload->value.data());
}

ColumnSearchDistance::ColumnSearchDistance(ResettableDocIterator::ptr&& it,
                                           HNSWInfo info)
  : ColumnDistanceBase{info.metric, info.d}, _it{std::move(it)} {}

float ColumnSearchDistance::operator()(faiss::idx_t id) {
  const float* data = LoadData(id, _it);
  SDB_ASSERT(_dist);
  const auto* lhs = reinterpret_cast<const irs::byte_type*>(_q);
  const auto* rhs = reinterpret_cast<const irs::byte_type*>(data);
  const auto d = static_cast<uint16_t>(_dim);
  return _dist(lhs, rhs, d);
}

ColumnIndexDistance::ColumnIndexDistance(ResettableDocIterator::ptr&& lit,
                                         ResettableDocIterator::ptr&& rit,
                                         HNSWInfo info)
  : ColumnDistanceBase{info.metric, info.d},
    _lit{std::move(lit)},
    _rit{std::move(rit)} {}

float ColumnIndexDistance::operator()(faiss::idx_t id) {
  const float* data = LoadData(id, _lit);
  SDB_ASSERT(_dist);
  const auto* lhs = reinterpret_cast<const irs::byte_type*>(_q);
  const auto* rhs = reinterpret_cast<const irs::byte_type*>(data);
  const auto d = static_cast<uint16_t>(_dim);
  return _dist(lhs, rhs, d);
}

float ColumnIndexDistance::symmetric_dis(faiss::idx_t i, faiss::idx_t j) {
  const float* data_i = LoadData(i, _lit);
  const float* data_j = LoadData(j, _rit);
  SDB_ASSERT(_dist);
  const auto* lhs = reinterpret_cast<const irs::byte_type*>(data_i);
  const auto* rhs = reinterpret_cast<const irs::byte_type*>(data_j);
  const auto d = static_cast<uint16_t>(_dim);
  return _dist(lhs, rhs, d);
}

HnswReader::HnswReader(field_id id, std::shared_ptr<const faiss::HNSW> hnsw,
                       HNSWInfo info, const ColumnReader& vector_column)
  : _id{id},
    _info{std::move(info)},
    _vector_column{vector_column},
    _hnsw{std::move(hnsw)} {
  SDB_ENSURE(vector_column.ArraySize() == static_cast<uint64_t>(_info.d),
             sdb::ERROR_INTERNAL, "HnswReader: ARRAY size ",
             vector_column.ArraySize(), " does not match HNSWInfo.d ", _info.d);
}

HnswReader::~HnswReader() = default;

void HnswReader::Search(HNSWSearchContext& ctx) const {
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

void HnswReader::RangeSearch(HNSWRangeSearchContext& ctx) const {
  const auto& hnsw = *_hnsw;
  ColumnDistance dis{_info, &ctx.cache};
  dis.set_query(reinterpret_cast<const float*>(ctx.info.query));

  HNSWRangeSegmentResultHandler res{ctx.segment_id, ctx.handler, ctx.docs_mask};
  ctx.vt.visited.resize(hnsw.levels.size(), 0);
  ctx.vt.advance();
  res.begin(0);
  hnsw.search(dis, nullptr, res, ctx.vt, &ctx.info.params);
}

ChunkedVectorCache& HnswReader::PrepareCache(ChunkedVectorCache& slot,
                                             ReadContext& ctx) const {
  const auto* child = _vector_column.Child();
  SDB_ASSERT(child);
  slot.Rebind(*child, _vector_column.ArraySize(), ctx);
  return slot;
}

}  // namespace irs
