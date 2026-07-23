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

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <duckdb/common/vector/array_vector.hpp>
#include <span>
#include <vector>

#include "iresearch/formats/column/column_reader.hpp"
#include "iresearch/index/column_info.hpp"

namespace irs {

class ReadContext;

void NormalizeRows(float* data, size_t n, uint32_t d);

enum class ClusteringAlgo { Auto, Lloyd, FlatSuperKMeans };

std::vector<float> MakeRotation(uint32_t d, uint32_t seed);

std::vector<float> TrainCentroids(VectorMetric metric, const float* data,
                                  size_t n, uint32_t k, uint32_t d,
                                  uint32_t seed, uint32_t niter = 8,
                                  uint32_t nredo = 1,
                                  ClusteringAlgo algo = ClusteringAlgo::Auto,
                                  const float* rotation = nullptr);

void AssignNearestGrouped(VectorMetric metric, std::span<const float> centroids,
                          size_t d, std::span<float> data,
                          std::span<size_t> ids, std::span<size_t> perm = {},
                          std::span<std::span<const float>> gathered = {});

// Streams the vector column row-batch by row-batch. `vector_column` is the
// parent ARRAY(FLOAT,d) reader; each batch is materialized through the modern
// reader API (Scan resets its own scratch, so no cross-batch segment
// accumulation) and the sink receives the batch's flat float child plus the
// per-row validity mask.
template<typename Sink>
void StreamRowBatches(const ColumnReader& vector_column, uint64_t rows,
                      ReadContext& ctx, Sink&& sink) {
  ColumnReader::VectorScratch scratch{vector_column.Type()};
  auto scan = vector_column.InitScan(ctx);
  for (uint64_t first = 0; first < rows; first += STANDARD_VECTOR_SIZE) {
    const auto n = static_cast<duckdb::idx_t>(
      std::min<uint64_t>(STANDARD_VECTOR_SIZE, rows - first));
    auto& out = scratch.Reset();
    vector_column.Scan(scan, out, n);
    sink(first, n,
         duckdb::FlatVector::GetData<float>(
           duckdb::ArrayVector::GetChildMutable(out)),
         duckdb::FlatVector::Validity(out));
  }
}

}  // namespace irs
