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
#include <span>
#include <vector>

#include "iresearch/formats/column/column_reader.hpp"
#include "iresearch/index/column_info.hpp"

namespace irs {

class ReadContext;

void NormalizeRows(float* data, size_t n, uint32_t d);

enum class ClusteringAlgo { Auto, Lloyd, FlatSuperKMeans, Hskm };

struct HskmHierarchy {
  std::vector<float> meso;
  std::vector<std::vector<float>> fine;
};

std::vector<float> MakeRotation(uint32_t d, uint32_t seed);

HskmHierarchy RunHskmHierarchical(const float* data, size_t n, uint32_t k,
                                  uint32_t d, uint32_t seed,
                                  const float* rotation = nullptr);

bool HskmQualifies(VectorMetric metric, uint32_t k, uint32_t d);

std::vector<float> TrainCentroids(VectorMetric metric, const float* data,
                                  size_t n, uint32_t k, uint32_t d,
                                  uint32_t seed, uint32_t niter = 8,
                                  uint32_t nredo = 1,
                                  ClusteringAlgo algo = ClusteringAlgo::Auto,
                                  const float* rotation = nullptr);

template<VectorMetric Metric>
uint32_t NearestCentroidT(const float* v, const float* centroids, uint32_t k,
                          uint32_t d) noexcept;

template<VectorMetric Metric>
void AssignNearestT(const float* data, size_t n, const float* centroids,
                    uint32_t k, uint32_t d, std::vector<uint32_t>& out,
                    std::span<std::span<const float>> nearest_centroids = {});

uint32_t NearestCentroid(VectorMetric metric, const float* v,
                         const float* centroids, uint32_t k, uint32_t d);

void AssignNearest(VectorMetric metric, const float* data, size_t n,
                   const float* centroids, uint32_t k, uint32_t d,
                   std::vector<uint32_t>& out,
                   std::span<std::span<const float>> nearest_centroids = {});

void AssignNearestGrouped(VectorMetric metric, std::span<const float> centroids,
                          size_t d, std::span<float> data,
                          std::span<size_t> ids, std::span<size_t> perm = {},
                          std::span<std::span<const float>> gathered = {});

std::vector<bool> ReadValidity(const ColumnReader& vector_column, uint64_t rows,
                               ReadContext& ctx);

template<typename Sink>
void StreamRowBatches(const ColumnReader& child, uint64_t rows, uint32_t d,
                      ReadContext& ctx, Sink&& sink) {
  const auto rows_per_batch =
    static_cast<duckdb::idx_t>(std::max<uint64_t>(1, STANDARD_VECTOR_SIZE / d));
  duckdb::Vector batch{duckdb::LogicalType::FLOAT,
                       static_cast<duckdb::idx_t>(rows_per_batch) * d};
  auto scan = child.InitScan(ctx);
  for (uint64_t first = 0; first < rows; first += rows_per_batch) {
    const auto n = static_cast<duckdb::idx_t>(
      std::min<uint64_t>(rows_per_batch, rows - first));
    child.ScanCount(scan, batch, static_cast<duckdb::idx_t>(n) * d, 0);
    sink(first, n, duckdb::FlatVector::GetData<float>(batch));
  }
}

}  // namespace irs
