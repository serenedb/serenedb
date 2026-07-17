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

#include <algorithm>
#include <cmath>
#include <cstring>
#include <limits>
#include <vector>

#include "iresearch/formats/ivf/quantizer.hpp"
#include "iresearch/store/data_output.hpp"
#include "iresearch/store/memory_directory.hpp"
#include "tests_shared.hpp"

using namespace irs;

namespace {

// Builds a writer, trains it on `n_train` copies of the 3 canonical
// `points` (so k-means has >= ksub=16 samples to work with), then encodes
// exactly `points` (n=3) as a single fast-scan cluster and returns the
// scores from a fresh reader positioned on that cluster.
std::array<score_t, 3> PqRoundtrip(uint32_t d, uint32_t pq_m,
                                   VectorMetric metric,
                                   const std::vector<float>& centroid,
                                   const std::vector<float>& points,
                                   const std::vector<float>& query) {
  auto writer = MakeQuantizerWriter(VectorQuantization::PQ, d, metric, pq_m,
                                    /*pq_niter=*/0, /*nb_bits=*/0);
  EXPECT_EQ(writer->Kind(), VectorQuantization::PQ);
  writer->SetClusterCentroid(centroid.data());

  std::vector<float> residual_train;
  constexpr size_t kCopiesPerPoint = 6;
  for (size_t c = 0; c < kCopiesPerPoint; ++c) {
    for (size_t p = 0; p < 3; ++p) {
      for (uint32_t j = 0; j < d; ++j) {
        residual_train.push_back(points[p * d + j] - centroid[j]);
      }
    }
  }
  writer->Train(residual_train.data(), residual_train.size() / d);

  SimpleMemoryAccounter memory;
  MemoryFile file{memory};
  uint64_t pay_start;
  {
    MemoryIndexOutput out{file};
    pay_start = out.Position();
    writer->BeginCluster(3);
    writer->EncodeCluster(out, points.data(), 3);
    writer->FinishCluster(out);
    out.Flush();
  }

  auto codebook = MakeQuantizerCodebook(VectorQuantization::PQ, d,
                                        writer->StatsBytes(), query, metric);
  EXPECT_NE(codebook, nullptr);

  auto reader =
    MakeQuantizerReader(codebook, std::make_unique<MemoryIndexInput>(file));
  EXPECT_NE(reader, nullptr);
  reader->StartCluster(pay_start, 3, centroid.data());

  std::array<score_t, 3> scores{};
  reader->ComputeBlock(0, 3, scores.data());
  return scores;
}

}  // namespace

class rabitq_quantizer_test : public ::testing::TestWithParam<uint32_t> {};

// The FWHT rotation pads d to a power of two; verify the encode+query roundtrip
// ranking is preserved across power-of-two AND non-power-of-two dimensions, and
// that the stats blob no longer carries a dense rotation matrix.
TEST_P(rabitq_quantizer_test, roundtrip_ranking_across_dims) {
  const uint32_t d = GetParam();
  constexpr uint32_t nb_bits = 8;
  const VectorMetric metric = VectorMetric::L2Sqr;
  const std::vector<float> centroid(d, 0.f);

  std::vector<float> points(3 * static_cast<size_t>(d), 0.f);
  points[0] = 1.f;                            // p0 (nearest)
  points[static_cast<size_t>(d)] = 4.f;       // p1
  points[2 * static_cast<size_t>(d)] = 20.f;  // p2 (farthest)
  constexpr size_t n = 3;

  auto writer = MakeQuantizerWriter(VectorQuantization::RaBitQ, d, metric,
                                    /*pq_m=*/0, /*pq_niter=*/0, nb_bits);
  ASSERT_NE(writer, nullptr);
  EXPECT_EQ(writer->Kind(), VectorQuantization::RaBitQ);
  EXPECT_EQ(writer->StatsBytes().size(), 2 * sizeof(uint32_t));
  writer->SetClusterCentroid(centroid.data());

  SimpleMemoryAccounter memory;
  MemoryFile file{memory};
  uint64_t pay_start;
  {
    MemoryIndexOutput out{file};
    pay_start = out.Position();
    writer->EncodeCluster(out, points.data(), n);
    out.Flush();
  }

  std::vector<float> query(d, 0.f);
  query[0] = 1.5f;
  auto codebook = MakeQuantizerCodebook(VectorQuantization::RaBitQ, d,
                                        writer->StatsBytes(), query, metric);
  ASSERT_NE(codebook, nullptr);

  auto reader =
    MakeQuantizerReader(codebook, std::make_unique<MemoryIndexInput>(file));
  ASSERT_NE(reader, nullptr);
  reader->StartCluster(pay_start, n, centroid.data());

  std::array<score_t, n> scores{};
  reader->ComputeBlock(0, n, scores.data());

  EXPECT_GT(scores[0], scores[1]);
  EXPECT_GT(scores[1], scores[2]);
}

INSTANTIATE_TEST_SUITE_P(dims, rabitq_quantizer_test,
                         ::testing::Values(8u, 32u, 96u, 128u, 1536u));

TEST(rabitq_quantizer_test, roundtrip_ranking_matches_exact_l2) {
  constexpr uint32_t d = 8;
  constexpr uint32_t nb_bits = 8;
  const VectorMetric metric = VectorMetric::L2Sqr;
  const std::vector<float> centroid(d, 0.f);

  // Well-separated points along one axis: distances to centroid 1, 3, 18.
  const std::vector<float> points{
    /*p0*/ 1.f,  0.f, 0.f, 0.f, 0.f, 0.f, 0.f, 0.f,
    /*p1*/ 4.f,  0.f, 0.f, 0.f, 0.f, 0.f, 0.f, 0.f,
    /*p2*/ 20.f, 0.f, 0.f, 0.f, 0.f, 0.f, 0.f, 0.f,
  };
  constexpr size_t n = 3;

  auto writer = MakeQuantizerWriter(VectorQuantization::RaBitQ, d, metric,
                                    /*pq_m=*/0, /*pq_niter=*/0, nb_bits);
  ASSERT_NE(writer, nullptr);
  writer->SetClusterCentroid(centroid.data());

  SimpleMemoryAccounter memory;
  MemoryFile file{memory};
  uint64_t pay_start;
  {
    MemoryIndexOutput out{file};
    pay_start = out.Position();
    writer->EncodeCluster(out, points.data(), n);
    out.Flush();
  }

  // Query closest to p0 (distance 0.5), then p1 (2.5), then p2 (18.5).
  const std::vector<float> query{1.5f, 0.f, 0.f, 0.f, 0.f, 0.f, 0.f, 0.f};
  auto codebook = MakeQuantizerCodebook(VectorQuantization::RaBitQ, d,
                                        writer->StatsBytes(), query, metric);
  ASSERT_NE(codebook, nullptr);

  auto reader =
    MakeQuantizerReader(codebook, std::make_unique<MemoryIndexInput>(file));
  ASSERT_NE(reader, nullptr);
  reader->StartCluster(pay_start, n, centroid.data());

  std::array<score_t, n> scores{};
  reader->ComputeBlock(0, n, scores.data());

  // L2 scores are negated distances (larger = nearer), so p0 > p1 > p2.
  EXPECT_GT(scores[0], scores[1]);
  EXPECT_GT(scores[1], scores[2]);
}

TEST(rabitq_quantizer_test, roundtrip_ranking_matches_exact_inner_product) {
  constexpr uint32_t d = 8;
  constexpr uint32_t nb_bits = 8;
  const VectorMetric metric = VectorMetric::InnerProduct;
  const std::vector<float> centroid(d, 0.f);

  const std::vector<float> points{
    /*p0 aligned with query*/ 2.f,
    0.f,
    0.f,
    0.f,
    0.f,
    0.f,
    0.f,
    0.f,
    /*p1 orthogonal*/ 0.f,
    2.f,
    0.f,
    0.f,
    0.f,
    0.f,
    0.f,
    0.f,
    /*p2 opposed*/ -2.f,
    0.f,
    0.f,
    0.f,
    0.f,
    0.f,
    0.f,
    0.f,
  };
  constexpr size_t n = 3;

  auto writer = MakeQuantizerWriter(VectorQuantization::RaBitQ, d, metric,
                                    /*pq_m=*/0, /*pq_niter=*/0, nb_bits);
  ASSERT_NE(writer, nullptr);
  writer->SetClusterCentroid(centroid.data());

  SimpleMemoryAccounter memory;
  MemoryFile file{memory};
  uint64_t pay_start;
  {
    MemoryIndexOutput out{file};
    pay_start = out.Position();
    writer->EncodeCluster(out, points.data(), n);
    out.Flush();
  }

  const std::vector<float> query{3.f, 0.f, 0.f, 0.f, 0.f, 0.f, 0.f, 0.f};
  auto codebook = MakeQuantizerCodebook(VectorQuantization::RaBitQ, d,
                                        writer->StatsBytes(), query, metric);
  ASSERT_NE(codebook, nullptr);

  auto reader =
    MakeQuantizerReader(codebook, std::make_unique<MemoryIndexInput>(file));
  ASSERT_NE(reader, nullptr);
  reader->StartCluster(pay_start, n, centroid.data());

  std::array<score_t, n> scores{};
  reader->ComputeBlock(0, n, scores.data());

  // IP: higher raw value means a larger inner product with the query.
  // Exact order by <query, p_i> is p0 (6) > p1 (0) > p2 (-6).
  EXPECT_GT(scores[0], scores[1]);
  EXPECT_GT(scores[1], scores[2]);
}

TEST(pq_quantizer_test, roundtrip_ranking_matches_exact_l2) {
  constexpr uint32_t d = 8;
  constexpr uint32_t pq_m = 2;
  const VectorMetric metric = VectorMetric::L2Sqr;
  const std::vector<float> centroid(d, 0.f);

  // Well-separated points along one axis: distances to centroid 1, 4, 20.
  const std::vector<float> points{
    /*p0*/ 1.f,  0.f, 0.f, 0.f, 0.f, 0.f, 0.f, 0.f,
    /*p1*/ 4.f,  0.f, 0.f, 0.f, 0.f, 0.f, 0.f, 0.f,
    /*p2*/ 20.f, 0.f, 0.f, 0.f, 0.f, 0.f, 0.f, 0.f,
  };
  // Query closest to p0 (distance 0.25), then p1 (6.25), then p2 (342.25).
  const std::vector<float> query{1.5f, 0.f, 0.f, 0.f, 0.f, 0.f, 0.f, 0.f};

  const auto scores = PqRoundtrip(d, pq_m, metric, centroid, points, query);

  // L2 scores are negated distances (larger = nearer), so p0 > p1 > p2.
  EXPECT_GT(scores[0], scores[1]);
  EXPECT_GT(scores[1], scores[2]);
}

TEST(pq_quantizer_test, roundtrip_ranking_matches_exact_inner_product) {
  constexpr uint32_t d = 8;
  constexpr uint32_t pq_m = 2;
  const VectorMetric metric = VectorMetric::InnerProduct;
  const std::vector<float> centroid(d, 0.f);

  const std::vector<float> points{
    /*p0 aligned with query*/ 2.f,
    0.f,
    0.f,
    0.f,
    0.f,
    0.f,
    0.f,
    0.f,
    /*p1 orthogonal*/ 0.f,
    2.f,
    0.f,
    0.f,
    0.f,
    0.f,
    0.f,
    0.f,
    /*p2 opposed*/ -2.f,
    0.f,
    0.f,
    0.f,
    0.f,
    0.f,
    0.f,
    0.f,
  };
  const std::vector<float> query{3.f, 0.f, 0.f, 0.f, 0.f, 0.f, 0.f, 0.f};

  const auto scores = PqRoundtrip(d, pq_m, metric, centroid, points, query);

  // IP: higher raw value means a larger inner product with the query.
  // Exact order by <query, p_i> is p0 (6) > p1 (0) > p2 (-6).
  EXPECT_GT(scores[0], scores[1]);
  EXPECT_GT(scores[1], scores[2]);
}

// A cluster spanning more than one 32-vector fast-scan SIMD block (bbs=32),
// with an odd M (M=3, rounded up to nsq=4 for packing/scanning) to exercise
// the padding subquantizer. Near/far points are interleaved so both blocks
// mix the two groups instead of neatly separating along the block boundary.
TEST(pq_quantizer_test, cluster_spans_multiple_fastscan_blocks_with_odd_m) {
  constexpr uint32_t d = 9;
  constexpr uint32_t pq_m = 3;
  constexpr size_t kNear = 20;
  constexpr size_t kFar = 20;
  constexpr size_t n = kNear + kFar;
  const VectorMetric metric = VectorMetric::L2Sqr;
  const std::vector<float> centroid(d, 0.f);

  std::vector<float> points(n * d, 0.f);
  std::vector<bool> is_near(n);
  for (size_t i = 0; i < n; ++i) {
    is_near[i] = (i % 2 == 0);
    points[i * d] = is_near[i] ? 1.f : 100.f;
  }

  auto writer = MakeQuantizerWriter(VectorQuantization::PQ, d, metric, pq_m,
                                    /*pq_niter=*/0, /*nb_bits=*/0);
  ASSERT_NE(writer, nullptr);
  writer->SetClusterCentroid(centroid.data());
  writer->Train(points.data(), n);

  SimpleMemoryAccounter memory;
  MemoryFile file{memory};
  uint64_t pay_start;
  {
    MemoryIndexOutput out{file};
    pay_start = out.Position();
    writer->BeginCluster(n);
    writer->EncodeCluster(out, points.data(), n);
    writer->FinishCluster(out);
    out.Flush();
  }

  const std::vector<float> query{1.5f, 0.f, 0.f, 0.f, 0.f, 0.f, 0.f, 0.f, 0.f};
  auto codebook = MakeQuantizerCodebook(VectorQuantization::PQ, d,
                                        writer->StatsBytes(), query, metric);
  ASSERT_NE(codebook, nullptr);

  auto reader =
    MakeQuantizerReader(codebook, std::make_unique<MemoryIndexInput>(file));
  ASSERT_NE(reader, nullptr);
  reader->StartCluster(pay_start, n, centroid.data());

  std::vector<score_t> scores(n);
  reader->ComputeBlock(0, n, scores.data());

  score_t min_near = std::numeric_limits<score_t>::infinity();
  score_t max_far = -std::numeric_limits<score_t>::infinity();
  for (size_t i = 0; i < n; ++i) {
    if (is_near[i]) {
      min_near = std::min(min_near, scores[i]);
    } else {
      max_far = std::max(max_far, scores[i]);
    }
  }
  // L2 scores are negated distances (larger = nearer). Near/far are separated
  // by two orders of magnitude, so quantization noise shouldn't be able to
  // flip the group ordering even though it can perturb individual scores
  // within a group.
  EXPECT_GT(min_near, max_far);
}
