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

#include "iresearch/formats/ivf/clustering.hpp"

#include <superkmeans/distance_computers/batch_computers.h>
#include <superkmeans/superkmeans.h>

#include <algorithm>
#include <cmath>
#include <limits>

#include "iresearch/formats/ivf/ivf_reader.hpp"
#include "iresearch/types.hpp"

namespace irs {
namespace {

uint32_t NearestImpl(VectorDistanceFn dist, bool nearest_is_largest,
                     const float* v, const float* centroids, uint32_t k,
                     uint32_t d) noexcept {
  const auto* q = reinterpret_cast<const byte_type*>(v);
  const auto dd = static_cast<uint16_t>(d);
  uint32_t best = 0;
  float best_score = nearest_is_largest ? -std::numeric_limits<float>::max()
                                        : std::numeric_limits<float>::max();
  for (uint32_t s = 0; s < k; ++s) {
    const auto* cv = reinterpret_cast<const byte_type*>(
      centroids + static_cast<size_t>(s) * d);
    const float score = dist(q, cv, dd);
    if (Better(nearest_is_largest, score, best_score)) {
      best_score = score;
      best = s;
    }
  }
  return best;
}

void ComputeL2NormsRowMajor(const float* data, size_t n, uint32_t d,
                            float* out) noexcept {
  for (size_t i = 0; i < n; ++i) {
    const float* row = data + i * d;
    float sum = 0.f;
    for (uint32_t j = 0; j < d; ++j) {
      sum += row[j] * row[j];
    }
    out[i] = sum;
  }
}

}  // namespace

void NormalizeRows(float* data, size_t n, uint32_t d) {
  for (size_t i = 0; i < n; ++i) {
    float* row = data + i * d;
    float sum = 0.f;
    for (uint32_t j = 0; j < d; ++j) {
      sum += row[j] * row[j];
    }
    if (sum == 0.f) {
      continue;
    }
    const float inv_norm = 1.f / std::sqrt(sum);
    for (uint32_t j = 0; j < d; ++j) {
      row[j] *= inv_norm;
    }
  }
}

std::vector<float> TrainCentroids(VectorMetric metric, const float* data,
                                  size_t n, uint32_t k, uint32_t d,
                                  uint32_t seed, uint32_t niter,
                                  uint32_t nredo) {
  skmeans::SuperKMeansConfig cfg;
  cfg.iters = 1;
  cfg.seed = seed;
  cfg.angular = VectorMetricIsAngular(metric);
  cfg.sampling_fraction = 1.0f;
  cfg.data_already_rotated = true;
  cfg.max_points_per_cluster =
    std::max<uint32_t>(256, static_cast<uint32_t>((n + k - 1) / k));

  skmeans::SuperKMeans kmeans(k, d, cfg);
  return kmeans.Train(data, n);
}

uint32_t NearestCentroid(VectorMetric metric, const float* v,
                         const float* centroids, uint32_t k, uint32_t d) {
  return NearestImpl(ResolveVectorDistance(metric),
                     VectorMetricNearestIsLargest(metric), v, centroids, k, d);
}

void AssignNearest(VectorMetric metric, const float* data, size_t n,
                   const float* centroids, uint32_t k, uint32_t d,
                   std::vector<uint32_t>& out) {
  const size_t base = out.size();
  out.resize(base + n);
  if (n == 0) {
    return;
  }
  if (metric == VectorMetric::L1 || k == 0) {
    const auto dist = ResolveVectorDistance(metric);
    const bool nearest_is_largest = VectorMetricNearestIsLargest(metric);
    for (size_t i = 0; i < n; ++i) {
      out[base + i] =
        NearestImpl(dist, nearest_is_largest, data + i * d, centroids, k, d);
    }
    return;
  }

  using BatchComputer = skmeans::BatchComputer<skmeans::DistanceFunction::l2,
                                               skmeans::Quantization::f32>;
  std::vector<float> norms_x(n);
  std::vector<float> norms_y(k);
  ComputeL2NormsRowMajor(data, n, d, norms_x.data());
  ComputeL2NormsRowMajor(centroids, k, d, norms_y.data());
  std::vector<float> out_distances(n);
  const size_t tile_n = std::min<size_t>(n, skmeans::X_BATCH_SIZE);
  const size_t tile_k = std::min<size_t>(k, skmeans::Y_BATCH_SIZE);
  std::vector<float> tmp_distances(tile_n * tile_k);
  BatchComputer::FindNearestNeighbor(
    data, centroids, n, k, d, norms_x.data(), norms_y.data(), out.data() + base,
    out_distances.data(), tmp_distances.data());
}

std::vector<bool> ReadValidity(const ColumnReader& vector_column, uint64_t rows,
                               ReadContext& ctx) {
  std::vector<bool> valid(rows, true);
  const ColumnReader* validity = vector_column.Validity();
  if (!validity) {
    return valid;
  }
  duckdb::Vector vbatch{duckdb::LogicalType{duckdb::LogicalTypeId::VALIDITY},
                        duckdb::idx_t{0}};
  vbatch.BufferMutable().GetValidityMask().Initialize(STANDARD_VECTOR_SIZE);
  auto vscan = validity->InitScan(ctx);
  for (uint64_t start = 0; start < rows; start += STANDARD_VECTOR_SIZE) {
    const auto take =
      std::min<duckdb::idx_t>(STANDARD_VECTOR_SIZE, rows - start);
    validity->Scan(vscan, vbatch, take);
    const auto& mask = vbatch.Buffer().GetValidityMask();
    for (uint64_t k = 0; k < take; ++k) {
      valid[start + k] = mask.RowIsValid(k);
    }
  }
  return valid;
}

}  // namespace irs
