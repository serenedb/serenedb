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
#include <functional>
#include <limits>
#include <memory>
#include <random>
#include <utility>
#include <vector>

#include "basics/misc.hpp"
#include "iresearch/formats/ivf/clustering.hpp"
#include "iresearch/formats/ivf/panorama.hpp"
#include "iresearch/formats/ivf/quantizer.hpp"
#include "iresearch/search/score_function.hpp"
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

  auto stats =
    MakeQuantizerStats(VectorQuantization::PQ, d, writer->StatsBytes(), metric);
  EXPECT_NE(stats, nullptr);
  auto codebook = stats->MakeCodebook(query);
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

class none_quantizer_test : public ::testing::TestWithParam<VectorMetric> {};

TEST_P(none_quantizer_test, roundtrip_is_bit_exact) {
  const VectorMetric metric = GetParam();
  constexpr uint32_t d = 5;
  constexpr size_t n = 7;
  constexpr size_t kFirstBatch = 4;

  std::vector<float> points(n * d);
  for (size_t i = 0; i < points.size(); ++i) {
    points[i] = static_cast<float>(i % 13) * 0.375f - 2.f;
  }
  const std::vector<float> query{0.5f, -1.25f, 3.f, 0.f, 2.75f};

  auto writer = MakeQuantizerWriter(VectorQuantization::None, d, metric,
                                    /*pq_m=*/0, /*pq_niter=*/0, /*nb_bits=*/0);
  ASSERT_NE(writer, nullptr);
  EXPECT_EQ(writer->Kind(), VectorQuantization::None);
  EXPECT_EQ(writer->CodeSize(), d * sizeof(float));
  EXPECT_TRUE(writer->StatsBytes().empty());

  SimpleMemoryAccounter memory;
  MemoryFile file{memory};
  uint64_t pay_start;
  {
    MemoryIndexOutput out{file};
    out.WriteByte(0xAB);
    out.WriteByte(0xCD);
    out.WriteByte(0xEF);
    pay_start = out.Position();
    ASSERT_NE(pay_start % alignof(float), 0);
    writer->BeginCluster(n);
    writer->EncodeCluster(out, points.data(), kFirstBatch);
    writer->EncodeCluster(out, points.data() + kFirstBatch * d,
                          n - kFirstBatch);
    writer->FinishCluster(out);
    out.Flush();
    EXPECT_EQ(out.Position() - pay_start, n * d * sizeof(float));
  }

  auto stats = MakeQuantizerStats(VectorQuantization::None, d,
                                  writer->StatsBytes(), metric);
  ASSERT_NE(stats, nullptr);
  EXPECT_EQ(stats->Kind(), VectorQuantization::None);
  auto codebook = stats->MakeCodebook(query);
  ASSERT_NE(codebook, nullptr);

  auto reader =
    MakeQuantizerReader(codebook, std::make_unique<MemoryIndexInput>(file));
  ASSERT_NE(reader, nullptr);
  reader->StartCluster(pay_start, n, /*centroid=*/nullptr);

  std::vector<score_t> scores(n);
  reader->ComputeBlock(0, n, scores.data());

  ResolveEnum<VectorMetric>(metric, [&]<VectorMetric M> {
    for (size_t i = 0; i < n; ++i) {
      const score_t want = ComputeDistance<M>(
        query.data(), points.data() + i * d, static_cast<uint16_t>(d));
      EXPECT_EQ(scores[i], want) << "row " << i;
    }
  });

  std::vector<score_t> tail(n - kFirstBatch);
  reader->ComputeBlock(kFirstBatch, tail.size(), tail.data());
  for (size_t i = 0; i < tail.size(); ++i) {
    EXPECT_EQ(tail[i], scores[kFirstBatch + i]) << "row " << i;
  }
}

INSTANTIATE_TEST_SUITE_P(metrics, none_quantizer_test,
                         ::testing::Values(VectorMetric::L2Sqr,
                                           VectorMetric::InnerProduct,
                                           VectorMetric::Cosine,
                                           VectorMetric::L1));

class panorama_quantizer_test : public ::testing::TestWithParam<VectorMetric> {
};

namespace {

// Rows with variance decaying along a random orthonormal basis, so a PCA
// rotation has something to concentrate, plus cluster offsets so the k-th best
// tightens the way it does inside probed clusters.
std::vector<float> MakePanoramaData(uint32_t d, size_t n, uint32_t seed) {
  auto basis = MakeRotation(d, seed);
  std::mt19937 rng{seed};
  std::normal_distribution<float> nd{0.f, 1.f};
  std::vector<float> coef(d);
  std::vector<float> out(n * d, 0.f);
  for (size_t i = 0; i < n; ++i) {
    const float shift = static_cast<float>(i % 8) * 0.25f;
    for (uint32_t j = 0; j < d; ++j) {
      coef[j] = (nd(rng) + shift) / static_cast<float>(j + 1);
    }
    float* row = out.data() + i * d;
    for (uint32_t j = 0; j < d; ++j) {
      const float* b = basis.data() + size_t{j} * d;
      for (uint32_t t = 0; t < d; ++t) {
        row[t] += coef[j] * b[t];
      }
    }
  }
  return out;
}

struct PanoramaIndex {
  SimpleMemoryAccounter memory;
  MemoryFile file;
  std::unique_ptr<QuantizerWriter> writer;
  uint64_t pay_start = 0;
  size_t n = 0;

  PanoramaIndex() : file{memory} {}
};

// Trains and encodes `points` as one cluster, in several EncodeCluster batches
// so records that straddle a batch boundary are covered.
void BuildPanorama(PanoramaIndex& index, uint32_t d, VectorMetric metric,
                   const std::vector<float>& points) {
  index.n = points.size() / d;
  index.writer = MakeQuantizerWriter(VectorQuantization::None, d, metric,
                                     /*pq_m=*/0, /*pq_niter=*/0, /*nb_bits=*/0);
  ASSERT_NE(index.writer, nullptr);
  index.writer->Train(points.data(), index.n);
  ASSERT_FALSE(index.writer->StatsBytes().empty())
    << "the rotation must have been trained for d=" << d;
  EXPECT_EQ(index.writer->StatsBytes().size(),
            2 * sizeof(uint32_t) + size_t{d} * d * sizeof(float));
  EXPECT_EQ(index.writer->CodeSize(),
            (d + panorama::Levels(d)) * sizeof(float));

  MemoryIndexOutput out{index.file};
  index.pay_start = out.Position();
  index.writer->BeginCluster(index.n);
  constexpr size_t kBatch = 97;
  for (size_t b = 0; b < index.n; b += kBatch) {
    const size_t m = std::min(kBatch, index.n - b);
    index.writer->EncodeCluster(out, points.data() + b * d, m);
  }
  index.writer->FinishCluster(out);
  out.Flush();
  EXPECT_EQ(out.Position() - index.pay_start,
            index.n * panorama::RecordSize(d, panorama::Levels(d)));
}

std::unique_ptr<QuantizerReader> OpenPanorama(PanoramaIndex& index, uint32_t d,
                                              VectorMetric metric,
                                              const std::vector<float>& query) {
  auto stats = MakeQuantizerStats(VectorQuantization::None, d,
                                  index.writer->StatsBytes(), metric);
  EXPECT_NE(stats, nullptr);
  auto codebook = stats->MakeCodebook(query);
  EXPECT_NE(codebook, nullptr);
  auto reader = MakeQuantizerReader(
    codebook, std::make_unique<MemoryIndexInput>(index.file));
  EXPECT_NE(reader, nullptr);
  reader->StartCluster(index.pay_start, index.n, /*centroid=*/nullptr);
  return reader;
}

}  // namespace

// Without a bound threshold the progressive scan must reproduce the distance a
// full raw-basis scan computes -- up to the rounding a rotation introduces.
TEST_P(panorama_quantizer_test, unpruned_matches_raw_basis) {
  const VectorMetric metric = GetParam();
  constexpr uint32_t d = 64;
  constexpr size_t n = 600;
  auto points = MakePanoramaData(d, n, 3);
  auto query = MakePanoramaData(d, 1, 77);

  PanoramaIndex index;
  BuildPanorama(index, d, metric, points);
  ASSERT_FALSE(::testing::Test::HasFatalFailure());
  auto reader = OpenPanorama(index, d, metric, query);
  ASSERT_NE(reader, nullptr);

  std::vector<score_t> scores(n);
  for (size_t b = 0; b < n; b += kPostingBlock) {
    const size_t m = std::min<size_t>(kPostingBlock, n - b);
    reader->ComputeBlock(b, m, scores.data() + b);
  }

  ResolveEnum<VectorMetric>(metric, [&]<VectorMetric M> {
    for (size_t i = 0; i < n; ++i) {
      const score_t want = ComputeDistance<M>(
        query.data(), points.data() + i * d, static_cast<uint16_t>(d));
      EXPECT_NEAR(scores[i], want, 2e-3 * (1.f + std::fabs(want)))
        << "row " << i;
    }
  });
}

// The whole contract: a pruned candidate is one that could not have entered the
// top-k anyway, so pruning must not change the result. Runs the deployed loop
// -- blocks of kPostingBlock, threshold refreshed between blocks.
TEST_P(panorama_quantizer_test, pruning_preserves_topk) {
  const VectorMetric metric = GetParam();
  constexpr uint32_t d = 64;
  constexpr size_t n = 4096;
  constexpr size_t k = 10;
  auto points = MakePanoramaData(d, n, 5);

  PanoramaIndex index;
  BuildPanorama(index, d, metric, points);
  ASSERT_FALSE(::testing::Test::HasFatalFailure());

  for (uint32_t qi = 0; qi < 8; ++qi) {
    auto query = MakePanoramaData(d, 1, 1000 + qi);

    const auto topk = [&](bool prune) {
      auto reader = OpenPanorama(index, d, metric, query);
      score_t threshold = std::numeric_limits<score_t>::lowest();
      if (prune) {
        reader->SetPruningThreshold(&threshold);
      }
      std::vector<score_t> block(kPostingBlock);
      std::vector<std::pair<score_t, size_t>> best;
      for (size_t b = 0; b < n; b += kPostingBlock) {
        const size_t m = std::min<size_t>(kPostingBlock, n - b);
        reader->ComputeBlock(b, m, block.data());
        for (size_t i = 0; i < m; ++i) {
          if (block[i] > threshold) {
            best.emplace_back(block[i], b + i);
            std::push_heap(best.begin(), best.end(), std::greater<>{});
            if (best.size() > k) {
              std::pop_heap(best.begin(), best.end(), std::greater<>{});
              best.pop_back();
              threshold = best.front().first;
            }
          }
        }
      }
      std::sort(best.begin(), best.end(), std::greater<>{});
      return best;
    };

    const auto want = topk(false);
    const auto got = topk(true);
    ASSERT_EQ(want.size(), got.size()) << "query " << qi;
    for (size_t i = 0; i < want.size(); ++i) {
      EXPECT_EQ(want[i].second, got[i].second)
        << "query " << qi << " rank " << i << ": " << want[i].first << " vs "
        << got[i].first;
      EXPECT_EQ(want[i].first, got[i].first) << "query " << qi << " rank " << i;
    }
  }
}

// A candidate is only ever dropped when its true score cannot clear the
// threshold, and what the kernel reports instead is an upper bound on it.
TEST_P(panorama_quantizer_test, bound_never_drops_a_live_candidate) {
  const VectorMetric metric = GetParam();
  constexpr uint32_t d = 128;
  constexpr size_t n = 512;
  constexpr uint32_t levels = panorama::Levels(d);
  auto points = MakePanoramaData(d, n, 9);
  auto query = MakePanoramaData(d, 1, 21);

  auto rotation = TrainPcaRotation(points.data(), n, d);
  ASSERT_FALSE(rotation.empty());
  std::vector<float> rotated(points.size());
  ApplyRotation(rotation.data(), points.data(), rotated.data(), n, d);
  std::vector<float> rq(d);
  ApplyRotation(rotation.data(), query.data(), rq.data(), 1, d);

  std::vector<float> q_tails(levels);
  panorama::ComputeTails(rq.data(), d, levels, q_tails.data());
  const panorama::Query q{.data = rq.data(),
                          .tails = q_tails.data(),
                          .norm = std::sqrt(q_tails.front())};

  std::vector<float> record(panorama::RecordFloats(d, levels));
  uint64_t scanned = 0;
  uint64_t pruned = 0;
  ResolveEnum<VectorMetric>(metric, [&]<VectorMetric M> {
    for (size_t i = 0; i < n; ++i) {
      const float* y = rotated.data() + i * d;
      panorama::ComputeTails(y, d, levels, record.data());
      std::copy_n(y, d, record.data() + levels);

      const score_t exact = panorama::ProgressiveScore<M>(
        q, record.data(), d, levels, std::numeric_limits<score_t>::lowest());

      for (int step = -4; step <= 4; ++step) {
        const score_t threshold =
          exact + static_cast<score_t>(step) * 0.05f * (1.f + std::fabs(exact));
        const score_t got = panorama::ProgressiveScore<M, true>(
          q, record.data(), d, levels, threshold, &scanned);
        EXPECT_GE(got, exact - 1e-4f * (1.f + std::fabs(exact)))
          << "row " << i << " step " << step;
        if (got != exact) {
          ++pruned;
          EXPECT_LE(exact, threshold) << "row " << i << " step " << step;
        }
      }
    }
  });
  EXPECT_GT(pruned, 0u) << "the sweep must exercise the pruning branch";
  EXPECT_LT(scanned, uint64_t{n} * 9 * d)
    << "pruning must read fewer dims than a full scan";
}

// L1 is absent on purpose: a rotation does not preserve it, so the writer
// declines to train and the payload stays row-major.
INSTANTIATE_TEST_SUITE_P(metrics, panorama_quantizer_test,
                         ::testing::Values(VectorMetric::L2Sqr,
                                           VectorMetric::InnerProduct,
                                           VectorMetric::Cosine));

TEST(panorama_writer_test, l1_and_small_dims_decline_the_rotation) {
  constexpr uint32_t d = 64;
  constexpr size_t n = 600;
  auto points = MakePanoramaData(d, n, 13);

  auto l1 = MakeQuantizerWriter(VectorQuantization::None, d, VectorMetric::L1,
                                /*pq_m=*/0, /*pq_niter=*/0, /*nb_bits=*/0);
  ASSERT_NE(l1, nullptr);
  l1->Train(points.data(), n);
  EXPECT_TRUE(l1->StatsBytes().empty());
  EXPECT_EQ(l1->CodeSize(), d * sizeof(float));

  constexpr uint32_t small = panorama::kMinDim - 1;
  auto narrow = MakePanoramaData(small, n, 17);
  auto tiny = MakeQuantizerWriter(VectorQuantization::None, small,
                                  VectorMetric::L2Sqr, /*pq_m=*/0,
                                  /*pq_niter=*/0, /*nb_bits=*/0);
  ASSERT_NE(tiny, nullptr);
  tiny->Train(narrow.data(), n);
  EXPECT_TRUE(tiny->StatsBytes().empty());

  // Below d rows there is no basis to estimate, so it declines as well.
  auto starved = MakeQuantizerWriter(VectorQuantization::None, d,
                                     VectorMetric::L2Sqr, /*pq_m=*/0,
                                     /*pq_niter=*/0, /*nb_bits=*/0);
  ASSERT_NE(starved, nullptr);
  starved->Train(points.data(), d - 1);
  EXPECT_TRUE(starved->StatsBytes().empty());
}

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
    writer->BeginCluster(n);
    writer->EncodeCluster(out, points.data(), n);
    writer->FinishCluster(out);
    out.Flush();
  }

  std::vector<float> query(d, 0.f);
  query[0] = 1.5f;
  auto stats = MakeQuantizerStats(VectorQuantization::RaBitQ, d,
                                  writer->StatsBytes(), metric);
  ASSERT_NE(stats, nullptr);
  auto codebook = stats->MakeCodebook(query);
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
    writer->BeginCluster(n);
    writer->EncodeCluster(out, points.data(), n);
    writer->FinishCluster(out);
    out.Flush();
  }

  // Query closest to p0 (distance 0.5), then p1 (2.5), then p2 (18.5).
  const std::vector<float> query{1.5f, 0.f, 0.f, 0.f, 0.f, 0.f, 0.f, 0.f};
  auto stats = MakeQuantizerStats(VectorQuantization::RaBitQ, d,
                                  writer->StatsBytes(), metric);
  ASSERT_NE(stats, nullptr);
  auto codebook = stats->MakeCodebook(query);
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
    writer->BeginCluster(n);
    writer->EncodeCluster(out, points.data(), n);
    writer->FinishCluster(out);
    out.Flush();
  }

  const std::vector<float> query{3.f, 0.f, 0.f, 0.f, 0.f, 0.f, 0.f, 0.f};
  auto stats = MakeQuantizerStats(VectorQuantization::RaBitQ, d,
                                  writer->StatsBytes(), metric);
  ASSERT_NE(stats, nullptr);
  auto codebook = stats->MakeCodebook(query);
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

namespace {

// Writes `points` as one RaBitQ cluster around `centroid` and returns the
// scores for `query`.
std::vector<score_t> RaBitQRoundtrip(uint32_t d, uint32_t nb_bits,
                                     VectorMetric metric,
                                     const std::vector<float>& centroid,
                                     const std::vector<float>& points,
                                     const std::vector<float>& query) {
  const size_t n = points.size() / d;
  auto writer = MakeQuantizerWriter(VectorQuantization::RaBitQ, d, metric,
                                    /*pq_m=*/0, /*pq_niter=*/0, nb_bits);
  EXPECT_NE(writer, nullptr);
  writer->SetClusterCentroid(centroid.data());

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

  auto stats = MakeQuantizerStats(VectorQuantization::RaBitQ, d,
                                  writer->StatsBytes(), metric);
  EXPECT_NE(stats, nullptr);
  auto codebook = stats->MakeCodebook(query);
  EXPECT_NE(codebook, nullptr);

  auto reader =
    MakeQuantizerReader(codebook, std::make_unique<MemoryIndexInput>(file));
  EXPECT_NE(reader, nullptr);
  reader->StartCluster(pay_start, n, centroid.data());

  std::vector<score_t> scores(n);
  reader->ComputeBlock(0, n, scores.data());
  return scores;
}

}  // namespace

TEST(rabitq_quantizer_test, one_bit_ranking_l2_nonzero_centroid) {
  constexpr uint32_t d = 8;
  const VectorMetric metric = VectorMetric::L2Sqr;
  const std::vector<float> centroid(d, 5.f);

  std::vector<float> points(3 * static_cast<size_t>(d), 5.f);
  points[0] = 6.f;
  points[d] = 9.f;
  points[2 * d] = 25.f;
  std::vector<float> query(d, 5.f);
  query[0] = 6.5f;

  const auto scores =
    RaBitQRoundtrip(d, /*nb_bits=*/1, metric, centroid, points, query);

  EXPECT_GT(scores[0], scores[1]);
  EXPECT_GT(scores[1], scores[2]);
}

TEST(rabitq_quantizer_test, one_bit_ranking_inner_product_nonzero_centroid) {
  constexpr uint32_t d = 8;
  const VectorMetric metric = VectorMetric::InnerProduct;
  const std::vector<float> centroid(d, 5.f);

  std::vector<float> points(3 * static_cast<size_t>(d), 5.f);
  points[0] = 7.f;
  points[d] = 4.f;
  points[2 * d] = -2.f;
  std::vector<float> query(d, 0.f);
  query[0] = 3.f;

  const auto scores =
    RaBitQRoundtrip(d, /*nb_bits=*/1, metric, centroid, points, query);

  EXPECT_GT(scores[0], scores[1]);
  EXPECT_GT(scores[1], scores[2]);
}

TEST(rabitq_quantizer_test, one_bit_scores_comparable_across_clusters) {
  constexpr uint32_t d = 8;
  const VectorMetric metric = VectorMetric::L2Sqr;

  std::vector<float> c1(d, 5.f);
  std::vector<float> c2(d, 5.f);
  c2[0] = 105.f;

  std::vector<float> points1(2 * static_cast<size_t>(d), 5.f);
  points1[0] = 6.f;
  points1[d] = 9.f;
  std::vector<float> points2(2 * static_cast<size_t>(d), 5.f);
  points2[0] = 101.f;
  points2[d] = 104.f;

  auto writer = MakeQuantizerWriter(VectorQuantization::RaBitQ, d, metric,
                                    /*pq_m=*/0, /*pq_niter=*/0, /*nb_bits=*/1);
  ASSERT_NE(writer, nullptr);

  SimpleMemoryAccounter memory;
  MemoryFile file{memory};
  uint64_t pay_start1;
  uint64_t pay_start2;
  {
    MemoryIndexOutput out{file};
    pay_start1 = out.Position();
    writer->SetClusterCentroid(c1.data());
    writer->BeginCluster(2);
    writer->EncodeCluster(out, points1.data(), 2);
    writer->FinishCluster(out);
    pay_start2 = out.Position();
    writer->SetClusterCentroid(c2.data());
    writer->BeginCluster(2);
    writer->EncodeCluster(out, points2.data(), 2);
    writer->FinishCluster(out);
    out.Flush();
  }

  std::vector<float> query(d, 5.f);
  query[0] = 7.f;
  auto stats = MakeQuantizerStats(VectorQuantization::RaBitQ, d,
                                  writer->StatsBytes(), metric);
  ASSERT_NE(stats, nullptr);
  auto codebook = stats->MakeCodebook(query);
  ASSERT_NE(codebook, nullptr);

  auto reader =
    MakeQuantizerReader(codebook, std::make_unique<MemoryIndexInput>(file));
  ASSERT_NE(reader, nullptr);

  std::array<score_t, 4> scores{};
  reader->StartCluster(pay_start1, 2, c1.data());
  reader->ComputeBlock(0, 2, scores.data());
  reader->StartCluster(pay_start2, 2, c2.data());
  reader->ComputeBlock(0, 2, scores.data() + 2);

  EXPECT_GT(scores[0], scores[1]);
  EXPECT_GT(scores[1], scores[2]);
  EXPECT_GT(scores[2], scores[3]);
}

TEST(rabitq_quantizer_test, multibit_selective_refine_above_pool) {
  constexpr uint32_t d = 8;
  constexpr size_t n = 80;
  const VectorMetric metric = VectorMetric::L2Sqr;
  const std::vector<float> centroid(d, 5.f);

  std::vector<float> points(n * static_cast<size_t>(d), 5.f);
  std::vector<bool> is_near(n);
  for (size_t i = 0; i < n; ++i) {
    is_near[i] = (i % 2 == 0);
    points[i * d] = is_near[i] ? 6.f : 105.f;
  }
  std::vector<float> query(d, 5.f);
  query[0] = 6.5f;

  const auto scores =
    RaBitQRoundtrip(d, /*nb_bits=*/8, metric, centroid, points, query);

  score_t min_near = std::numeric_limits<score_t>::infinity();
  score_t max_far = -std::numeric_limits<score_t>::infinity();
  for (size_t i = 0; i < n; ++i) {
    if (is_near[i]) {
      min_near = std::min(min_near, scores[i]);
    } else {
      max_far = std::max(max_far, scores[i]);
    }
  }
  EXPECT_GT(min_near, max_far);
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

TEST(pq_quantizer_test, l2_ranking_with_nonzero_centroid) {
  constexpr uint32_t d = 8;
  constexpr uint32_t pq_m = 2;
  const VectorMetric metric = VectorMetric::L2Sqr;
  const std::vector<float> centroid(d, 5.f);

  std::vector<float> points(3 * static_cast<size_t>(d), 5.f);
  points[0] = 6.f;
  points[d] = 9.f;
  points[2 * d] = 25.f;
  std::vector<float> query(d, 5.f);
  query[0] = 6.5f;

  const auto scores = PqRoundtrip(d, pq_m, metric, centroid, points, query);

  EXPECT_GT(scores[0], scores[1]);
  EXPECT_GT(scores[1], scores[2]);
  EXPECT_LT(scores[0], 0.f);
}

TEST(pq_quantizer_test, l2_scores_comparable_across_clusters) {
  constexpr uint32_t d = 8;
  constexpr uint32_t pq_m = 2;
  const VectorMetric metric = VectorMetric::L2Sqr;

  const std::vector<float> c1(d, 0.f);
  std::vector<float> c2(d, 0.f);
  c2[0] = 100.f;

  std::vector<float> points1(2 * static_cast<size_t>(d), 0.f);
  points1[0] = 1.f;
  points1[d] = 4.f;
  std::vector<float> points2(2 * static_cast<size_t>(d), 0.f);
  points2[0] = 96.f;
  points2[d] = 99.f;

  auto writer = MakeQuantizerWriter(VectorQuantization::PQ, d, metric, pq_m,
                                    /*pq_niter=*/0, /*nb_bits=*/0);
  ASSERT_NE(writer, nullptr);

  std::vector<float> train;
  constexpr size_t kCopies = 5;
  for (size_t c = 0; c < kCopies; ++c) {
    for (const float r : {1.f, 4.f, -4.f, -1.f}) {
      std::vector<float> v(d, 0.f);
      v[0] = r;
      train.insert(train.end(), v.begin(), v.end());
    }
  }
  writer->Train(train.data(), train.size() / d);

  SimpleMemoryAccounter memory;
  MemoryFile file{memory};
  uint64_t pay_start1;
  uint64_t pay_start2;
  {
    MemoryIndexOutput out{file};
    pay_start1 = out.Position();
    writer->SetClusterCentroid(c1.data());
    writer->BeginCluster(2);
    writer->EncodeCluster(out, points1.data(), 2);
    writer->FinishCluster(out);
    pay_start2 = out.Position();
    writer->SetClusterCentroid(c2.data());
    writer->BeginCluster(2);
    writer->EncodeCluster(out, points2.data(), 2);
    writer->FinishCluster(out);
    out.Flush();
  }

  std::vector<float> query(d, 0.f);
  query[0] = 2.f;
  auto stats =
    MakeQuantizerStats(VectorQuantization::PQ, d, writer->StatsBytes(), metric);
  ASSERT_NE(stats, nullptr);
  auto codebook = stats->MakeCodebook(query);
  ASSERT_NE(codebook, nullptr);

  auto reader =
    MakeQuantizerReader(codebook, std::make_unique<MemoryIndexInput>(file));
  ASSERT_NE(reader, nullptr);

  std::array<score_t, 4> scores{};
  reader->StartCluster(pay_start1, 2, c1.data());
  reader->ComputeBlock(0, 2, scores.data());
  reader->StartCluster(pay_start2, 2, c2.data());
  reader->ComputeBlock(0, 2, scores.data() + 2);

  EXPECT_GT(scores[0], scores[1]);
  EXPECT_GT(scores[1], scores[2]);
  EXPECT_GT(scores[2], scores[3]);
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
  auto stats =
    MakeQuantizerStats(VectorQuantization::PQ, d, writer->StatsBytes(), metric);
  ASSERT_NE(stats, nullptr);
  auto codebook = stats->MakeCodebook(query);
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
