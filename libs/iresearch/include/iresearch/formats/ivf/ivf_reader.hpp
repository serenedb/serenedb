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

#pragma once

#include <cstdint>
#include <duckdb/common/types/vector.hpp>
#include <vector>

#include "iresearch/formats/column/column_reader.hpp"
#include "iresearch/index/column_info.hpp"
#include "iresearch/types.hpp"

namespace irs {

class ReadContext;

// Centroid ids are stored as fixed-width big-endian uint32 terms so that FST
// term order matches numeric centroid order. Reader and writer must agree on
// this encoding.
inline constexpr size_t kCentroidTermWidth = 4;

inline void EncodeCentroidTerm(uint32_t id, byte_type* out) noexcept {
  out[0] = static_cast<byte_type>(id >> 24);
  out[1] = static_cast<byte_type>(id >> 16);
  out[2] = static_cast<byte_type>(id >> 8);
  out[3] = static_cast<byte_type>(id);
}

using VectorDistanceFn = float (*)(const byte_type*, const byte_type*, uint16_t);

VectorDistanceFn ResolveVectorDistance(VectorMetric metric);

bool VectorMetricNearestIsLargest(VectorMetric metric) noexcept;

// Non-owning view over the trained codebook of one segment, read back from the
// `.idx` IVF entry. `data` is the nlist x d row-major matrix.
struct IvfCentroids {
  const float* data = nullptr;
  uint32_t nlist = 0;
  uint32_t d = 0;

  const float* Centroid(uint32_t c) const noexcept {
    return data + static_cast<size_t>(c) * d;
  }
};

// Appends to `out` the ids of the `nprobe` centroids nearest to `query`
// (ascending centroid id within the kept set is not guaranteed; order is
// nearest-first). `query` holds `centroids.d` floats.
void SelectNearestCentroids(const float* query, const IvfCentroids& centroids,
                            uint32_t nprobe, VectorDistanceFn dist,
                            bool nearest_is_largest, std::vector<uint32_t>& out);

// Forward-only reader of per-doc fp32 vectors from an ARRAY(FLOAT) column, used
// for exact distance rerank. Docs from a cluster-union disjunction arrive in
// ascending order, so a single forward cursor over the flat element child
// suffices. Not thread-safe; one instance per executing iterator.
class IvfVectorReader {
 public:
  IvfVectorReader(const ColumnReader& vector_column, ReadContext& ctx);

  uint32_t Dimension() const noexcept { return _d; }

  // Returns a pointer to `Dimension()` floats for `doc`, valid until the next
  // call. `doc` must not be smaller than the previously requested doc.
  const float* ReadDoc(doc_id_t doc);

 private:
  uint32_t _d;
  ColumnReader::RangeScan _scan;
  duckdb::Vector _buf;
  std::vector<float> _scratch;
};

}  // namespace irs
