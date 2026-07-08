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

bool VectorMetricIsAngular(VectorMetric metric) noexcept {
  return metric == VectorMetric::InnerProduct || metric == VectorMetric::Cosine;
}

IvfVectorReader::IvfVectorReader(const ColumnReader& vector_column,
                                 ReadContext& ctx)
  : _d{static_cast<uint32_t>(vector_column.ArraySize())},
    _child{vector_column.Child()},
    _ctx{&ctx},
    _scan{vector_column.Child()->InitScan(ctx)},
    _buf{duckdb::LogicalType::FLOAT, static_cast<duckdb::idx_t>(_d)} {
  SDB_ASSERT(vector_column.Child());
}

void IvfVectorReader::ReadInto(uint64_t start, uint64_t count) {
  if (start < _pos) {
    _scan = _child->InitScan(*_ctx);
    _pos = 0;
  }
  if (start > _pos) {
    _child->Skip(_scan, static_cast<duckdb::idx_t>(start - _pos));
    _pos = start;
  }
  uint64_t done = 0;
  while (done < count) {
    const auto n =
      _child->ScanCount(_scan, _buf, static_cast<duckdb::idx_t>(count - done),
                        static_cast<duckdb::idx_t>(done));
    SDB_ASSERT(n > 0);
    done += n;
    _pos += n;
  }
}

const float* IvfVectorReader::ReadDoc(doc_id_t doc) {
  const uint64_t row = static_cast<uint64_t>(doc) - doc_limits::min();
  ReadInto(row * _d, _d);
  return duckdb::FlatVector::GetData<float>(_buf);
}

const float* IvfVectorReader::ReadDocBatch(doc_id_t first, size_t count) {
  SDB_ASSERT(count >= 1);
  const uint64_t row0 = static_cast<uint64_t>(first) - doc_limits::min();
  const size_t total = count * _d;
  _buf.Reserve(total);
  ReadInto(row0 * _d, total);
  return duckdb::FlatVector::GetData<float>(_buf);
}

}  // namespace irs
