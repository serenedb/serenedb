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
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <utility>
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
    writer->Encode(out, points.data(), 3);
    writer->Finish(out);
    out.Flush();
  }

  auto stats =
    MakeQuantizerStats(VectorQuantization::PQ, d, writer->StatsBytes(), metric);
  EXPECT_NE(stats, nullptr);
  auto codebook = stats->MakeCodebook(query);
  EXPECT_NE(codebook, nullptr);

  auto reader = MakeQuantizerReader(
    codebook, std::make_unique<MemoryIndexInput>(file), pay_start);
  EXPECT_NE(reader, nullptr);
  reader->StartCluster(/*first_lane=*/0, 3, centroid.data());

  std::array<score_t, 3> scores{};
  reader->ComputeBlock(0, 3, scores.data());
  return scores;
}

struct LaneCase {
  VectorQuantization quant;
  uint32_t pq_m;
  uint32_t nb_bits;
  std::string_view name;
};

constexpr uint32_t kLaneDim = 8;
// Two magnitudes eight orders of magnitude apart in squared distance, so a
// document from outside the cluster cannot be mistaken for one inside it.
constexpr float kFar = 10000.f;

// Documents of the one stream: those inside `[first_lane, first_lane + n)` are
// near, and spread over eight positions of a second dimension the far ones
// leave alone -- so neighbouring lanes decode to different scores and a
// shifted read shows. The rest are far along the first dimension only, which
// keeps the near documents apart under a quantizer trained on both.
std::vector<float> MakeLaneCorpus(size_t docs, size_t first_lane, size_t n) {
  std::vector<float> out(docs * kLaneDim, 0.f);
  for (size_t i = 0; i < docs; ++i) {
    const bool inside = i >= first_lane && i < first_lane + n;
    out[i * kLaneDim] = inside ? 1.f : kFar;
    out[i * kLaneDim + 1] = inside ? static_cast<float>(i % 8) : 0.f;
  }
  return out;
}

std::vector<score_t> ReadLanes(
  QuantizerWriter& writer, const std::shared_ptr<const QuantizerCodebook>& cb,
  std::span<const float> corpus, const std::vector<float>& centroid,
  uint64_t first_lane, size_t n) {
  SimpleMemoryAccounter memory;
  MemoryFile file{memory};
  uint64_t pay_base;
  {
    MemoryIndexOutput out{file};
    pay_base = out.Position();
    writer.SetClusterCentroid(centroid.data());
    writer.Encode(out, corpus.data(), corpus.size() / kLaneDim);
    writer.Finish(out);
    out.Flush();
  }
  auto reader =
    MakeQuantizerReader(cb, std::make_unique<MemoryIndexInput>(file), pay_base);
  EXPECT_NE(reader, nullptr);
  std::vector<score_t> scores(n, 0.f);
  reader->StartCluster(first_lane, n, centroid.data());
  reader->ComputeBlock(0, n, scores.data());
  return scores;
}

}  // namespace

class quantizer_lane_test : public ::testing::TestWithParam<LaneCase> {};

// A cluster is a lane range of a stream that keeps running past both of its
// ends, so the read has to mask both edges: the scores it hands back must be
// exactly the ones the same documents produce when they are the whole stream,
// and never a neighbour's.
TEST_P(quantizer_lane_test, cluster_reads_only_its_own_lanes) {
  const auto& param = GetParam();
  constexpr size_t kDocs = 200;
  const std::vector<float> centroid(kLaneDim, 0.f);
  const std::vector<float> query{0.5f, -1.f, 0.f, 0.f, 0.f, 0.f, 0.f, 0.f};

  // First lane / length pairs: pack-aligned and not, shorter and longer than a
  // pack, straddling one boundary and several, and the stream's own tail.
  const std::vector<std::pair<size_t, size_t>> ranges{
    {0, 5},   {1, 3},  {31, 2},   {32, 32},       {33, 40},
    {96, 64}, {63, 1}, {64, 128}, {kDocs - 3, 3}, {0, kDocs},
  };

  for (const auto& [first_lane, n] : ranges) {
    ASSERT_LE(first_lane + n, kDocs);
    const auto corpus = MakeLaneCorpus(kDocs, first_lane, n);
    // The same documents as the whole stream: their codes and factors do not
    // depend on the lane they land on, so the scores must match exactly.
    const std::vector<float> alone(
      corpus.begin() + static_cast<std::ptrdiff_t>(first_lane * kLaneDim),
      corpus.begin() +
        static_cast<std::ptrdiff_t>((first_lane + n) * kLaneDim));

    auto writer = MakeQuantizerWriter(param.quant, kLaneDim,
                                      VectorMetric::L2Sqr, param.pq_m,
                                      /*pq_niter=*/0, param.nb_bits);
    ASSERT_NE(writer, nullptr);
    writer->SetClusterCentroid(centroid.data());
    writer->Train(corpus.data(), kDocs);
    auto stats = MakeQuantizerStats(param.quant, kLaneDim, writer->StatsBytes(),
                                    VectorMetric::L2Sqr);
    ASSERT_NE(stats, nullptr);
    auto cb = stats->MakeCodebook(query);
    ASSERT_NE(cb, nullptr);

    const auto actual = ReadLanes(*writer, cb, corpus, centroid, first_lane, n);
    const auto expected = ReadLanes(*writer, cb, alone, centroid, 0, n);
    ASSERT_EQ(expected.size(), actual.size());
    for (size_t i = 0; i != n; ++i) {
      EXPECT_EQ(expected[i], actual[i])
        << param.name << " lane " << (first_lane + i);
    }

    // Independent of the two reads agreeing: every score belongs to a near
    // document. What a far one decodes to under this same quantizer is the
    // band, so the check needs no assumption about quantization error. A
    // cluster that is the whole stream has no neighbour to leak in, and its
    // quantizer never saw a far document.
    if (n != kDocs) {
      std::vector<float> far_corpus(4 * kLaneDim, 0.f);
      for (size_t i = 0; i != 4; ++i) {
        far_corpus[i * kLaneDim] = kFar;
      }
      const auto far_scores =
        ReadLanes(*writer, cb, far_corpus, centroid, 0, 4);
      const score_t max_far =
        *std::max_element(far_scores.begin(), far_scores.end());
      for (size_t i = 0; i != n; ++i) {
        EXPECT_GT(actual[i], max_far)
          << param.name << " lane " << (first_lane + i);
      }
    }
    // And the check has to be able to see a one-lane slip: the same length
    // read one lane over, at either edge, must not decode to the same scores.
    if (first_lane + n < kDocs) {
      EXPECT_NE(ReadLanes(*writer, cb, corpus, centroid, first_lane + 1, n),
                actual)
        << param.name << " forward of " << first_lane;
    }
    if (first_lane > 0) {
      EXPECT_NE(ReadLanes(*writer, cb, corpus, centroid, first_lane - 1, n),
                actual)
        << param.name << " back of " << first_lane;
    }
  }
}

INSTANTIATE_TEST_SUITE_P(
  quantizers, quantizer_lane_test,
  ::testing::Values(LaneCase{VectorQuantization::SQ8, 0, 0, "sq8"},
                    LaneCase{VectorQuantization::SQ4, 0, 0, "sq4"},
                    LaneCase{VectorQuantization::PQ, 4, 0, "pq"},
                    LaneCase{VectorQuantization::RaBitQ, 0, 1, "rabitq1"},
                    LaneCase{VectorQuantization::RaBitQ, 0, 8, "rabitq8"}),
  [](const ::testing::TestParamInfo<LaneCase>& info) {
    return std::string{info.param.name};
  });

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
    writer->Encode(out, points.data(), n);
    writer->Finish(out);
    out.Flush();
  }

  std::vector<float> query(d, 0.f);
  query[0] = 1.5f;
  auto stats = MakeQuantizerStats(VectorQuantization::RaBitQ, d,
                                  writer->StatsBytes(), metric);
  ASSERT_NE(stats, nullptr);
  auto codebook = stats->MakeCodebook(query);
  ASSERT_NE(codebook, nullptr);

  auto reader = MakeQuantizerReader(
    codebook, std::make_unique<MemoryIndexInput>(file), pay_start);
  ASSERT_NE(reader, nullptr);
  reader->StartCluster(/*first_lane=*/0, n, centroid.data());

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
    writer->Encode(out, points.data(), n);
    writer->Finish(out);
    out.Flush();
  }

  // Query closest to p0 (distance 0.5), then p1 (2.5), then p2 (18.5).
  const std::vector<float> query{1.5f, 0.f, 0.f, 0.f, 0.f, 0.f, 0.f, 0.f};
  auto stats = MakeQuantizerStats(VectorQuantization::RaBitQ, d,
                                  writer->StatsBytes(), metric);
  ASSERT_NE(stats, nullptr);
  auto codebook = stats->MakeCodebook(query);
  ASSERT_NE(codebook, nullptr);

  auto reader = MakeQuantizerReader(
    codebook, std::make_unique<MemoryIndexInput>(file), pay_start);
  ASSERT_NE(reader, nullptr);
  reader->StartCluster(/*first_lane=*/0, n, centroid.data());

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
    writer->Encode(out, points.data(), n);
    writer->Finish(out);
    out.Flush();
  }

  const std::vector<float> query{3.f, 0.f, 0.f, 0.f, 0.f, 0.f, 0.f, 0.f};
  auto stats = MakeQuantizerStats(VectorQuantization::RaBitQ, d,
                                  writer->StatsBytes(), metric);
  ASSERT_NE(stats, nullptr);
  auto codebook = stats->MakeCodebook(query);
  ASSERT_NE(codebook, nullptr);

  auto reader = MakeQuantizerReader(
    codebook, std::make_unique<MemoryIndexInput>(file), pay_start);
  ASSERT_NE(reader, nullptr);
  reader->StartCluster(/*first_lane=*/0, n, centroid.data());

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
    writer->Encode(out, points.data(), n);
    writer->Finish(out);
    out.Flush();
  }

  const std::vector<float> query{1.5f, 0.f, 0.f, 0.f, 0.f, 0.f, 0.f, 0.f, 0.f};
  auto stats =
    MakeQuantizerStats(VectorQuantization::PQ, d, writer->StatsBytes(), metric);
  ASSERT_NE(stats, nullptr);
  auto codebook = stats->MakeCodebook(query);
  ASSERT_NE(codebook, nullptr);

  auto reader = MakeQuantizerReader(
    codebook, std::make_unique<MemoryIndexInput>(file), pay_start);
  ASSERT_NE(reader, nullptr);
  reader->StartCluster(/*first_lane=*/0, n, centroid.data());

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
