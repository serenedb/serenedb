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

#include "iresearch/formats/ivf/ivf_reader.hpp"

#include <cmath>
#include <cstring>
#include <memory>
#include <span>
#include <vector>

#include "basics/assert.h"
#include "basics/exceptions.h"
#include "iresearch/formats/column/read_context.hpp"
#include "iresearch/utils/type_limits.hpp"
#include "iresearch/utils/vector.hpp"

namespace irs {
namespace {

float CosineSimilarity(const byte_type* l, const byte_type* r, uint16_t d) {
  const auto [ll, lr, rr] =
    vector::CosineDistanceImpl<float, float, float>::Compute(l, r, d);
  const float denom = std::sqrt(ll) * std::sqrt(rr);
  return denom == 0.f ? 0.f : lr / denom;
}

class RawVectorReader final : public VectorBlockReader {
 public:
  RawVectorReader(const ColumnReader& vector_column,
                  const ColReader& col_reader, uint32_t d)
    : _read_ctx{col_reader}, _vreader{vector_column, _read_ctx}, _d{d} {}

  void SetQuery(std::span<const float> query, VectorMetric metric) final {
    _query.assign(query.begin(), query.end());
    _dist = ResolveVectorDistance(metric);
  }

  void StartCluster(uint64_t, size_t) final {}

  void ComputeBlock(const doc_id_t* docs, uint32_t, size_t count, score_t boost,
                    score_t* out) final {
    SDB_ASSERT(_dist);
    const auto* q = reinterpret_cast<const byte_type*>(_query.data());
    const auto d = static_cast<uint16_t>(_d);
    for (size_t i = 0; i < count; ++i) {
      const float* v = _vreader.ReadDoc(docs[i]);
      out[i] = _dist(q, reinterpret_cast<const byte_type*>(v), d) * boost;
    }
  }

 private:
  ReadContext _read_ctx;
  IvfVectorReader _vreader;
  VectorDistanceFn _dist = nullptr;
  std::vector<float> _query;
  uint32_t _d;
};

}  // namespace

VectorDistanceFn ResolveVectorDistance(VectorMetric metric) {
  switch (metric) {
    case VectorMetric::L2Sqr:
      return &vector::L2Space<float, float, float>::Dist;
    case VectorMetric::L1:
      return &vector::L1Space<float, float, float>::Dist;
    case VectorMetric::InnerProduct:
      return &vector::DotProductImpl<float, float>::Compute;
    case VectorMetric::Cosine:
      return &CosineSimilarity;
  }
  SDB_THROW(sdb::ERROR_NOT_IMPLEMENTED, "unsupported IVF vector metric");
}

bool VectorMetricNearestIsLargest(VectorMetric metric) noexcept {
  switch (metric) {
    case VectorMetric::InnerProduct:
    case VectorMetric::Cosine:
      return true;
    case VectorMetric::L2Sqr:
    case VectorMetric::L1:
      return false;
  }
  return false;
}

IvfVectorReader::IvfVectorReader(const ColumnReader& vector_column,
                                 ReadContext& ctx)
  : _d{static_cast<uint32_t>(vector_column.ArraySize())},
    _scan{*vector_column.Child(), ctx},
    _buf{duckdb::LogicalType::FLOAT, static_cast<duckdb::idx_t>(_d)},
    _scratch(_d) {
  SDB_ASSERT(vector_column.Child());
}

const float* IvfVectorReader::ReadDoc(doc_id_t doc) {
  const uint64_t row = static_cast<uint64_t>(doc) - doc_limits::min();
  _scan.Scan(row * _d, _d, _buf, /*out_offset=*/0);
  const float* p = duckdb::FlatVector::GetData<float>(_buf);
  std::memcpy(_scratch.data(), p, static_cast<size_t>(_d) * sizeof(float));
  return _scratch.data();
}

std::unique_ptr<VectorBlockReader> MakeRawVectorReader(
  const ColumnReader& vector_column, const ColReader& col_reader, uint32_t d) {
  return std::make_unique<RawVectorReader>(vector_column, col_reader, d);
}

}  // namespace irs
