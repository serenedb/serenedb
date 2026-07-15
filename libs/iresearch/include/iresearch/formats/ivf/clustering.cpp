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

#include <faiss/Clustering.h>
#include <faiss/IndexFlat.h>
#include <faiss/SuperKMeans.h>
#include <faiss/utils/distances.h>

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

void ConfigureClusteringParams(faiss::ClusteringParameters& cp, uint32_t niter,
                               uint32_t nredo, uint32_t seed, size_t n,
                               uint32_t k) {
  cp.niter = static_cast<int>(niter);
  cp.nredo = static_cast<int>(nredo);
  cp.seed = static_cast<int>(seed);
  cp.min_points_per_centroid = 1;
  cp.max_points_per_centroid = std::max<int>(cp.max_points_per_centroid,
                                             static_cast<int>((n + k - 1) / k));
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
  if (VectorMetricIsAngular(metric)) {
    faiss::ClusteringParameters cp;
    ConfigureClusteringParams(cp, niter, nredo, seed, n, k);
    cp.spherical = true;

    std::vector<float> normalized(data, data + static_cast<size_t>(n) * d);
    NormalizeRows(normalized.data(), n, d);
    faiss::Clustering clus(static_cast<int>(d), static_cast<int>(k), cp);
    faiss::IndexFlatIP index(static_cast<int>(d));
    clus.train(static_cast<faiss::idx_t>(n), normalized.data(), index);
    return std::move(clus.centroids);
  }

  constexpr uint32_t kSuperKMeansMinD = 32;
  if (d >= kSuperKMeansMinD) {
    faiss::SuperKMeansParameters cp;
    ConfigureClusteringParams(cp, niter, nredo, seed, n, k);
    faiss::SuperKMeans kmeans(static_cast<int>(d), static_cast<int>(k), cp);
    kmeans.train(static_cast<faiss::idx_t>(n), data);
    return std::move(kmeans.centroids);
  }

  faiss::ClusteringParameters cp;
  ConfigureClusteringParams(cp, niter, nredo, seed, n, k);
  faiss::Clustering clus(static_cast<int>(d), static_cast<int>(k), cp);
  faiss::IndexFlatL2 index(static_cast<int>(d));
  clus.train(static_cast<faiss::idx_t>(n), data, index);
  return std::move(clus.centroids);
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

  std::vector<int64_t> indexes(n);
  std::vector<float> distances(n);
  faiss::knn_L2sqr(data, centroids, d, n, k, 1, distances.data(),
                   indexes.data());
  for (size_t i = 0; i < n; ++i) {
    out[base + i] = static_cast<uint32_t>(indexes[i]);
  }
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
