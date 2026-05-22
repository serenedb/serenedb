////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
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

#include "iresearch/formats/column/hnsw_index.hpp"

#include <cmath>

#include "basics/system-compiler.h"
#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/formats/formats.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/utils/attribute_provider.hpp"
#include "iresearch/utils/vector.hpp"

namespace irs {
namespace {

template<typename T>
void WriteVector(DataOutput& out, const T& vec) {
  out.WriteU32(vec.size());
  SDB_ASSERT(vec.size() != 0);
  out.WriteBytes(reinterpret_cast<const byte_type*>(vec.data()),
                 sizeof(*vec.data()) * vec.size());
}

template<typename T>
void ReadVector(IndexInput& in, T& vec) {
  uint32_t size = irs::read<uint32_t>(in);
  vec.resize(size);
  SDB_ASSERT(size != 0);
  in.ReadBytes(reinterpret_cast<byte_type*>(vec.data()),
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
    case HNSWMetric::Cosine: {
      return ComputeCosine;
    }
    default:
      SDB_UNREACHABLE();
  }
}

}  // namespace

void WriteHNSW(DataOutput& out, const faiss::HNSW& hnsw) {
  WriteVector(out, hnsw.assign_probas);
  WriteVector(out, hnsw.cum_nneighbor_per_level);
  WriteVector(out, hnsw.levels);
  WriteVector(out, hnsw.offsets);
  WriteVector(out, hnsw.neighbors);

  out.WriteU32(hnsw.entry_point);
  out.WriteU32(hnsw.max_level);
  out.WriteU32(hnsw.efConstruction);
  out.WriteU32(hnsw.efSearch);
}

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

}  // namespace irs
