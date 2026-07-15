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
#include <random>
#include <vector>

#include "basics/system-compiler.h"
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

float MetricScore(VectorMetric m, const float* x, const float* y, uint32_t d) {
  const auto* lx = reinterpret_cast<const byte_type*>(x);
  const auto* ly = reinterpret_cast<const byte_type*>(y);
  const auto dd = static_cast<uint16_t>(d);
  switch (m) {
    case VectorMetric::L2Sqr:
      return ComputeDistance<VectorMetric::L2Sqr>(lx, ly, dd);
    case VectorMetric::L1:
      return ComputeDistance<VectorMetric::L1>(lx, ly, dd);
    case VectorMetric::InnerProduct:
      return ComputeDistance<VectorMetric::InnerProduct>(lx, ly, dd);
    case VectorMetric::Cosine:
      return ComputeDistance<VectorMetric::Cosine>(lx, ly, dd);
  }
  SDB_UNREACHABLE();
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

TEST(clustering_test, train_centroids_separates_blobs) {
  constexpr uint32_t d = 4;
  constexpr uint32_t k = 2;
  const auto c =
    TrainCentroids(VectorMetric::L2Sqr, kBlobs2x4.data(), /*n=*/8, k, d,
                   /*seed=*/42);
  ASSERT_EQ(c.size(), static_cast<size_t>(k) * d);

  const float* a = c.data();
  const float* b = c.data() + d;
  EXPECT_GT(std::abs(a[1] - b[1]), 10.f);
  for (const float* cen : {a, b}) {
    EXPECT_NEAR(cen[0], 8.f, 1.5f);
    EXPECT_GT(std::abs(cen[1]), 6.f);
    EXPECT_NEAR(cen[2], 0.f, 1.f);
    EXPECT_NEAR(cen[3], 0.f, 1.f);
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

TEST(clustering_test, assign_nearest_all_metrics) {
  constexpr uint32_t d = 4;
  const std::vector<float> centroids{1.f, 0.f, 0.f, 0.f, 0.f, 1.f, 0.f, 0.f};

  for (const VectorMetric m :
       {VectorMetric::InnerProduct, VectorMetric::Cosine}) {
    const std::vector<float> data{100.f, 0.f, 0.f, 0.f, 0.01f, 0.f,
                                  0.f,   0.f, 0.f, 5.f, 0.f,   0.f};
    std::vector<uint32_t> out;
    AssignNearest(m, data.data(), 3, centroids.data(), 2, d, out);
    ASSERT_EQ(out.size(), 3u);
    EXPECT_EQ(out[0], 0u);
    EXPECT_EQ(out[1], 0u);
    EXPECT_EQ(out[2], 1u);
  }

  const std::vector<float> centroids2{3.f, 0.f, 0.f, 0.f, 2.f, 2.f, 0.f, 0.f};
  const std::vector<float> p{0.f, 0.f, 0.f, 0.f};

  std::vector<uint32_t> out_l2;
  AssignNearest(VectorMetric::L2Sqr, p.data(), 1, centroids2.data(), 2, d,
                out_l2);
  ASSERT_EQ(out_l2.size(), 1u);
  EXPECT_EQ(out_l2[0], 1u);

  std::vector<uint32_t> out_l1;
  AssignNearest(VectorMetric::L1, p.data(), 1, centroids2.data(), 2, d, out_l1);
  ASSERT_EQ(out_l1.size(), 1u);
  EXPECT_EQ(out_l1[0], 0u);

  std::vector<uint32_t> out_k1;
  AssignNearest(VectorMetric::L2Sqr, p.data(), 1, centroids2.data(), 1, d,
                out_k1);
  ASSERT_EQ(out_k1.size(), 1u);
  EXPECT_EQ(out_k1[0], 0u);

  std::vector<uint32_t> out_k0;
  AssignNearest(VectorMetric::L2Sqr, p.data(), 1, centroids2.data(), 0, d,
                out_k0);
  ASSERT_EQ(out_k0.size(), 1u);
  EXPECT_EQ(out_k0[0], 0u);
}

TEST(clustering_test, assign_nearest_matches_scalar_reference) {
  constexpr uint32_t d = 4;
  constexpr uint32_t k = 6;
  const std::vector<float> blob_centers{
    50.f, 0.f, 0.f, 0.f,  0.f,   50.f, 0.f, 0.f, 0.f, 0.f,   50.f, 0.f,
    0.f,  0.f, 0.f, 50.f, -50.f, 0.f,  0.f, 0.f, 0.f, -50.f, 0.f,  0.f,
  };
  ASSERT_EQ(blob_centers.size(), static_cast<size_t>(k) * d);

  std::mt19937 gen{42};
  std::normal_distribution<float> jitter{0.f, 0.1f};
  std::uniform_int_distribution<uint32_t> pick_blob{0, k - 1};
  const auto sample_near_blobs = [&](size_t n) {
    std::vector<float> out(n * d);
    for (size_t i = 0; i < n; ++i) {
      const uint32_t b = pick_blob(gen);
      for (uint32_t j = 0; j < d; ++j) {
        out[i * d + j] = blob_centers[b * d + j] + jitter(gen);
      }
    }
    return out;
  };

  for (const VectorMetric m : {VectorMetric::L2Sqr, VectorMetric::InnerProduct,
                               VectorMetric::Cosine, VectorMetric::L1}) {
    const auto train_sample = sample_near_blobs(300);
    const auto centroids =
      TrainCentroids(m, train_sample.data(), train_sample.size() / d, k, d,
                     /*seed=*/7);
    ASSERT_EQ(centroids.size(), static_cast<size_t>(k) * d);

    for (const size_t n : {size_t{1}, size_t{50}, size_t{300}}) {
      const auto queries = sample_near_blobs(n);

      std::vector<uint32_t> batched;
      AssignNearest(m, queries.data(), n, centroids.data(), k, d, batched);
      ASSERT_EQ(batched.size(), n);

      for (size_t i = 0; i < n; ++i) {
        const float* q = queries.data() + i * d;
        const uint32_t expected = NearestCentroid(m, q, centroids.data(), k, d);
        if (batched[i] == expected) {
          continue;
        }
        // GEMM (||x||²+||y||²-2·x·y) and the direct per-pair distance sum in
        // different order, so a genuine near-tie between two centroids can
        // legitimately flip which one wins -- only flag a real mismatch.
        const float got_score =
          MetricScore(m, q, centroids.data() + batched[i] * d, d);
        const float expected_score =
          MetricScore(m, q, centroids.data() + expected * d, d);
        const float tol = std::max(1e-3f, std::abs(expected_score) * 1e-3f);
        EXPECT_NEAR(got_score, expected_score, tol)
          << "metric=" << static_cast<int>(m) << " n=" << n << " row=" << i
          << " batched=" << batched[i] << " expected=" << expected;
      }
    }
  }
}
