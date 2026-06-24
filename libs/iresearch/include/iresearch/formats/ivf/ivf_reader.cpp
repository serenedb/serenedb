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

#include <algorithm>
#include <cmath>
#include <cstring>

#include "basics/assert.h"
#include "basics/exceptions.h"
#include "iresearch/formats/column/read_context.hpp"
#include "iresearch/utils/type_limits.hpp"
#include "iresearch/utils/vector.hpp"

namespace irs {
namespace {

float Dot(const byte_type* l, const byte_type* r, uint16_t d) {
  return vector::DotProductImpl<float, float>::Compute(l, r, d);
}

float CosineSimilarity(const byte_type* l, const byte_type* r, uint16_t d) {
  const auto [ll, lr, rr] =
    vector::CosineDistanceImpl<float, float, float>::Compute(l, r, d);
  const float denom = std::sqrt(ll) * std::sqrt(rr);
  return denom == 0.f ? 0.f : lr / denom;
}

}  // namespace

VectorDistanceFn ResolveVectorDistance(VectorMetric metric) {
  switch (metric) {
    case VectorMetric::L2Sqr:
      return &vector::L2Space<float, float, float>::Dist;
    case VectorMetric::L1:
      return &vector::L1Space<float, float, float>::Dist;
    case VectorMetric::InnerProduct:
      return &Dot;
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

void SelectNearestCentroids(const float* query, const IvfCentroids& centroids,
                            uint32_t nprobe, VectorDistanceFn dist,
                            bool nearest_is_largest,
                            std::vector<uint32_t>& out) {
  const uint32_t nlist = centroids.nlist;
  if (nlist == 0) {
    return;
  }
  nprobe = std::min<uint32_t>(std::max<uint32_t>(nprobe, 1), nlist);

  const auto* q = reinterpret_cast<const byte_type*>(query);
  const auto d = static_cast<uint16_t>(centroids.d);

  std::vector<std::pair<float, uint32_t>> scored;
  scored.reserve(nlist);
  for (uint32_t c = 0; c < nlist; ++c) {
    const auto* cv = reinterpret_cast<const byte_type*>(centroids.Centroid(c));
    scored.emplace_back(dist(q, cv, d), c);
  }

  const auto mid = scored.begin() + nprobe;
  std::partial_sort(
    scored.begin(), mid, scored.end(),
    [nearest_is_largest](const auto& l, const auto& r) noexcept {
      return nearest_is_largest ? l.first > r.first : l.first < r.first;
    });

  out.reserve(out.size() + nprobe);
  for (auto it = scored.begin(); it != mid; ++it) {
    out.push_back(it->second);
  }
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

}  // namespace irs
