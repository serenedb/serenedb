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

#include <superkmeans/superkmeans.h>

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

// Use
std::vector<float> TrainCentroids(VectorMetric metric, const float* data,
                                  size_t n, uint32_t k, uint32_t d,
                                  uint32_t seed) {
  skmeans::SuperKMeansConfig cfg;
  cfg.n_threads = 1;
  cfg.sampling_fraction = 1.0f;  // already sampled
  cfg.seed = seed;
  cfg.angular =
    metric == VectorMetric::InnerProduct || metric == VectorMetric::Cosine;
  skmeans::SuperKMeans<skmeans::Quantization::f32,
                       skmeans::DistanceFunction::l2>
    km{k, d, cfg};
  return km.Train(data, n);
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
