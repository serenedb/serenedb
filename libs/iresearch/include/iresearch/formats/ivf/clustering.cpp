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
    if (nearest_is_largest ? score > best_score : score < best_score) {
      best_score = score;
      best = s;
    }
  }
  return best;
}

}  // namespace

void NormalizeRows(float* data, size_t n, uint32_t d) {
  for (size_t i = 0; i < n; ++i) {
    float* row = data + i * d;
    float sum = 0.f;
    for (uint32_t j = 0; j < d; ++j) {
      sum += row[j] * row[j];
    }
    if (sum <= 0.f) {
      continue;
    }
    const float inv = 1.f / std::sqrt(sum);
    for (uint32_t j = 0; j < d; ++j) {
      row[j] *= inv;
    }
  }
}

std::vector<float> TrainCentroids(VectorMetric metric, const float* data,
                                  size_t n, uint32_t k, uint32_t d,
                                  uint32_t seed) {
  const bool angular =
    metric == VectorMetric::InnerProduct || metric == VectorMetric::Cosine;

  faiss::ClusteringParameters cp;
  cp.niter = 25;
  cp.seed = static_cast<int>(seed);
  cp.spherical = angular;
  cp.min_points_per_centroid = 1;
  cp.max_points_per_centroid = static_cast<int>(std::max<size_t>(
    static_cast<size_t>(cp.max_points_per_centroid), (n + k - 1) / k));
  cp.init_method = faiss::ClusteringInitMethod::RANDOM;

  faiss::Clustering clus(static_cast<int>(d), static_cast<int>(k), cp);
  if (angular) {
    faiss::IndexFlatIP index(static_cast<int>(d));
    clus.train(static_cast<faiss::idx_t>(n), data, index);
  } else {
    faiss::IndexFlatL2 index(static_cast<int>(d));
    clus.train(static_cast<faiss::idx_t>(n), data, index);
  }
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
  const auto dist = ResolveVectorDistance(metric);
  const bool nearest_is_largest = VectorMetricNearestIsLargest(metric);
  out.reserve(out.size() + n);
  for (size_t i = 0; i < n; ++i) {
    out.push_back(
      NearestImpl(dist, nearest_is_largest, data + i * d, centroids, k, d));
  }
}

}  // namespace irs
