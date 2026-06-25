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

#include <cmath>
#include <vector>

#include "iresearch/formats/ivf/clustering.hpp"
#include "tests_shared.hpp"

using namespace irs;

namespace {

float Norm(const float* v, uint32_t d) {
  float sum = 0.f;
  for (uint32_t j = 0; j < d; ++j) {
    sum += v[j] * v[j];
  }
  return std::sqrt(sum);
}

// Two well-separated blobs, both far from the origin so any subset-mean keeps a
// large norm (lets the L2/L1 "not unit" assertion hold regardless of which
// points land in which cluster).
const std::vector<float> kBlobs2x4{
  8.f, 8.f, 0.f,  0.f,  7.f, 9.f, 0.f,  0.f,   9.f, 7.f, 0.f,
  0.f, 8.f, 8.5f, 0.f,  0.f, 8.f, -8.f, 0.f,   0.f, 7.f, -9.f,
  0.f, 0.f, 9.f,  -7.f, 0.f, 0.f, 8.f,  -8.5f, 0.f, 0.f,
};

}  // namespace

TEST(clustering_test, normalize_rows) {
  std::vector<float> data{3.f, 4.f, 5.f, 12.f, 0.f, 0.f};
  NormalizeRows(data.data(), /*n=*/3, /*d=*/2);
  EXPECT_NEAR(data[0], 0.6f, 1e-6f);
  EXPECT_NEAR(data[1], 0.8f, 1e-6f);
  EXPECT_NEAR(Norm(data.data() + 2, 2), 1.f, 1e-6f);
  // Zero row left untouched (no division by zero).
  EXPECT_EQ(data[4], 0.f);
  EXPECT_EQ(data[5], 0.f);
}

TEST(clustering_test, train_centroids_norms_per_metric) {
  constexpr uint32_t d = 4;
  constexpr uint32_t k = 2;

  // IP / Cosine: spherical (angular) -> every centroid unit-norm.
  for (const VectorMetric m :
       {VectorMetric::InnerProduct, VectorMetric::Cosine}) {
    const auto c =
      TrainCentroids(m, kBlobs2x4.data(), /*n=*/8, k, d, /*seed=*/42);
    ASSERT_EQ(c.size(), static_cast<size_t>(k) * d);
    for (uint32_t i = 0; i < k; ++i) {
      EXPECT_NEAR(Norm(c.data() + static_cast<size_t>(i) * d, d), 1.f, 1e-3f);
    }
  }

  // L2 / L1: plain means -> centroids keep the data's (large) magnitude.
  for (const VectorMetric m : {VectorMetric::L2Sqr, VectorMetric::L1}) {
    const auto c =
      TrainCentroids(m, kBlobs2x4.data(), /*n=*/8, k, d, /*seed=*/42);
    ASSERT_EQ(c.size(), static_cast<size_t>(k) * d);
    for (uint32_t i = 0; i < k; ++i) {
      EXPECT_GT(Norm(c.data() + static_cast<size_t>(i) * d, d), 2.f);
    }
  }
}

TEST(clustering_test, nearest_centroid_metric_direction) {
  constexpr uint32_t d = 4;
  // Two unit centroids along orthogonal axes.
  const std::vector<float> centroids{1.f, 0.f, 0.f, 0.f, 0.f, 1.f, 0.f, 0.f};

  // IP / Cosine pick the largest score: the aligned centroid, regardless of the
  // query's magnitude (direction-invariance).
  for (const VectorMetric m :
       {VectorMetric::InnerProduct, VectorMetric::Cosine}) {
    const std::vector<float> big_x{100.f, 0.f, 0.f, 0.f};
    const std::vector<float> tiny_x{0.01f, 0.f, 0.f, 0.f};
    const std::vector<float> y{0.f, 5.f, 0.f, 0.f};
    EXPECT_EQ(NearestCentroid(m, big_x.data(), centroids.data(), 2, d), 0u);
    EXPECT_EQ(NearestCentroid(m, tiny_x.data(), centroids.data(), 2, d), 0u);
    EXPECT_EQ(NearestCentroid(m, y.data(), centroids.data(), 2, d), 1u);
  }
}

TEST(clustering_test, nearest_centroid_l1_vs_l2_disagree) {
  constexpr uint32_t d = 4;
  const std::vector<float> p{0.f, 0.f, 0.f, 0.f};
  // cA: L1=3, L2=9 ; cB: L1=4, L2=8. L2 prefers cB, L1 prefers cA.
  const std::vector<float> centroids{3.f, 0.f, 0.f, 0.f, 2.f, 2.f, 0.f, 0.f};
  EXPECT_EQ(
    NearestCentroid(VectorMetric::L2Sqr, p.data(), centroids.data(), 2, d), 1u);
  EXPECT_EQ(NearestCentroid(VectorMetric::L1, p.data(), centroids.data(), 2, d),
            0u);
}
