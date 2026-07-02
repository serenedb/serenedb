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
#include <cstring>
#include <vector>

#include "iresearch/formats/ivf/quantizer.hpp"
#include "iresearch/store/data_output.hpp"
#include "iresearch/store/memory_directory.hpp"
#include "tests_shared.hpp"

using namespace irs;

namespace {

// StatsBytes() layout for RaBitQ, as written by RaBitQuantizerWriter:
// [uint32 nb_bits][uint32 d][float rotation[d*d]] (row-major).
struct RaBitQStatsView {
  uint32_t nb_bits;
  uint32_t d;
  const float* rotation;
};

RaBitQStatsView ParseRaBitQStats(std::span<const byte_type> stats) {
  RaBitQStatsView v{};
  std::memcpy(&v.nb_bits, stats.data(), sizeof(v.nb_bits));
  std::memcpy(&v.d, stats.data() + sizeof(v.nb_bits), sizeof(v.d));
  v.rotation =
    reinterpret_cast<const float*>(stats.data() + 2 * sizeof(uint32_t));
  return v;
}

float Dot(const float* a, const float* b, uint32_t d) {
  float acc = 0.f;
  for (uint32_t i = 0; i < d; ++i) {
    acc += a[i] * b[i];
  }
  return acc;
}

}  // namespace

class rabitq_quantizer_test : public ::testing::TestWithParam<uint32_t> {};

TEST_P(rabitq_quantizer_test, rotation_is_orthogonal) {
  const uint32_t d = GetParam();
  auto writer =
    MakeQuantizerWriter(VectorQuantization::RaBitQ, d, VectorMetric::L2Sqr,
                        /*pq_m=*/0, /*nb_bits=*/1);
  ASSERT_NE(writer, nullptr);
  EXPECT_EQ(writer->Kind(), VectorQuantization::RaBitQ);

  const auto view = ParseRaBitQStats(writer->StatsBytes());
  ASSERT_EQ(view.nb_bits, 1u);
  ASSERT_EQ(view.d, d);

  for (uint32_t i = 0; i < d; ++i) {
    const float* row_i = view.rotation + static_cast<size_t>(i) * d;
    const float norm = std::sqrt(Dot(row_i, row_i, d));
    EXPECT_NEAR(norm, 1.f, 1e-3f) << "row " << i << " not unit-norm";
    for (uint32_t j = i + 1; j < d; ++j) {
      const float* row_j = view.rotation + static_cast<size_t>(j) * d;
      EXPECT_NEAR(Dot(row_i, row_j, d), 0.f, 1e-3f)
        << "rows " << i << "/" << j << " not orthogonal";
    }
  }
}

INSTANTIATE_TEST_SUITE_P(dims, rabitq_quantizer_test,
                        ::testing::Values(8u, 32u, 128u));

TEST(rabitq_quantizer_test, roundtrip_ranking_matches_exact_l2) {
  constexpr uint32_t d = 8;
  constexpr uint32_t nb_bits = 8;
  const VectorMetric metric = VectorMetric::L2Sqr;
  const std::vector<float> centroid(d, 0.f);

  // Well-separated points along one axis: distances to centroid 1, 3, 18.
  const std::vector<float> points{
    /*p0*/ 1.f, 0.f, 0.f, 0.f, 0.f, 0.f, 0.f, 0.f,
    /*p1*/ 4.f, 0.f, 0.f, 0.f, 0.f, 0.f, 0.f, 0.f,
    /*p2*/ 20.f, 0.f, 0.f, 0.f, 0.f, 0.f, 0.f, 0.f,
  };
  constexpr size_t n = 3;

  auto writer = MakeQuantizerWriter(VectorQuantization::RaBitQ, d, metric,
                                    /*pq_m=*/0, nb_bits);
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
  reader->ComputeBlock(0, n, /*boost=*/1.f, scores.data());

  // L2: lower score means closer. Exact order is p0 < p1 < p2.
  EXPECT_LT(scores[0], scores[1]);
  EXPECT_LT(scores[1], scores[2]);
}

TEST(rabitq_quantizer_test, roundtrip_ranking_matches_exact_inner_product) {
  constexpr uint32_t d = 8;
  constexpr uint32_t nb_bits = 8;
  const VectorMetric metric = VectorMetric::InnerProduct;
  const std::vector<float> centroid(d, 0.f);

  const std::vector<float> points{
    /*p0 aligned with query*/ 2.f, 0.f, 0.f, 0.f, 0.f, 0.f, 0.f, 0.f,
    /*p1 orthogonal*/ 0.f, 2.f, 0.f, 0.f, 0.f, 0.f, 0.f, 0.f,
    /*p2 opposed*/ -2.f, 0.f, 0.f, 0.f, 0.f, 0.f, 0.f, 0.f,
  };
  constexpr size_t n = 3;

  auto writer = MakeQuantizerWriter(VectorQuantization::RaBitQ, d, metric,
                                    /*pq_m=*/0, nb_bits);
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
  reader->ComputeBlock(0, n, /*boost=*/1.f, scores.data());

  // IP: higher raw value means a larger inner product with the query.
  // Exact order by <query, p_i> is p0 (6) > p1 (0) > p2 (-6).
  EXPECT_GT(scores[0], scores[1]);
  EXPECT_GT(scores[1], scores[2]);
}
