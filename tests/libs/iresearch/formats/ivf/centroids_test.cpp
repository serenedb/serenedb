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

#include <cstring>
#include <span>
#include <vector>

#include "iresearch/formats/ivf/centroids.hpp"
#include "iresearch/store/data_output.hpp"
#include "iresearch/store/memory_directory.hpp"
#include "tests_shared.hpp"

using namespace irs;

namespace {

// One Layer-2 body: sub-centroids and the global fine ids they map to.
struct L2BodySpec {
  std::vector<float> centroids;    // n_l2 * d row-major
  std::vector<uint32_t> fine_ids;  // n_l2
};

// Serializes a full two-layer centroid entry (bodies first, resident trailer
// last) exactly as IvfBuilder::Build does, returning the resident offset/size.
struct Serialized {
  uint64_t resident_offset = 0;
  uint64_t resident_size = 0;
};

Serialized WriteEntry(IndexOutput& out, VectorMetric metric, uint32_t d,
                      const std::vector<float>& l1_centroids,
                      const std::vector<L2BodySpec>& bodies) {
  const auto n_l1 = static_cast<uint32_t>(bodies.size());
  std::vector<uint64_t> body_offsets;
  body_offsets.reserve(n_l1);
  for (const auto& b : bodies) {
    body_offsets.push_back(out.Position());
    const auto n_l2 = static_cast<uint32_t>(b.fine_ids.size());
    out.WriteU32(n_l2);
    out.WriteData(reinterpret_cast<const byte_type*>(b.centroids.data()),
                  static_cast<size_t>(n_l2) * d * sizeof(float));
    out.WriteData(reinterpret_cast<const byte_type*>(b.fine_ids.data()),
                  static_cast<size_t>(n_l2) * sizeof(uint32_t));
  }
  Serialized s;
  s.resident_offset = out.Position();
  TwoLayerCentroids::WriteFooter(out, metric, d, n_l1,
                                 std::span<const float>{l1_centroids},
                                 std::span<const uint64_t>{body_offsets});
  s.resident_size = out.Position() - s.resident_offset;
  return s;
}

}  // namespace

TEST(two_layer_centroids_test, roundtrip_and_search) {
  SimpleMemoryAccounter memory;
  MemoryFile file{memory};

  constexpr uint32_t d = 2;
  const std::vector<float> l1{/*c0*/ 0.f, 0.f, /*c1*/ 10.f, 10.f};
  const std::vector<L2BodySpec> bodies{
    // cell 0: two fine clusters near origin
    L2BodySpec{.centroids = {0.f, 0.f, 1.f, 1.f}, .fine_ids = {0, 1}},
    // cell 1: two fine clusters near (10,10)
    L2BodySpec{.centroids = {10.f, 10.f, 11.f, 11.f}, .fine_ids = {2, 3}},
  };

  Serialized s;
  {
    MemoryIndexOutput out{file};
    s = WriteEntry(out, VectorMetric::L2Sqr, d, l1, bodies);
    out.Flush();
  }
  ASSERT_EQ(s.resident_size, TwoLayerCentroids::FooterSize(d, 2));

  MemoryIndexInput in{file};
  in.Seek(s.resident_offset);
  auto centroids = TwoLayerCentroids::Deserialize(in, s.resident_size);

  // Resident layer-1 deserialized correctly; no layer-2 bytes were read.
  EXPECT_EQ(centroids.Dimension(), d);
  EXPECT_EQ(centroids.L1Count(), 2u);
  EXPECT_EQ(centroids.Metric(), VectorMetric::L2Sqr);
  EXPECT_FALSE(centroids.Empty());

  // Layer-1 selection.
  {
    const std::vector<float> q{0.2f, 0.2f};
    std::vector<uint32_t> l1_ids;
    centroids.SearchL1(q, /*n1=*/1, l1_ids);
    ASSERT_EQ(l1_ids.size(), 1u);
    EXPECT_EQ(l1_ids[0], 0u);  // nearest L1 is c0
  }
  {
    const std::vector<float> q{9.5f, 9.5f};
    std::vector<uint32_t> l1_ids;
    centroids.SearchL1(q, /*n1=*/1, l1_ids);
    ASSERT_EQ(l1_ids.size(), 1u);
    EXPECT_EQ(l1_ids[0], 1u);  // nearest L1 is c1
  }
  {
    const std::vector<float> q{0.2f, 0.2f};
    std::vector<uint32_t> l1_ids;
    centroids.SearchL1(q, /*n1=*/5, l1_ids);  // clamped to L1Count()
    ASSERT_EQ(l1_ids.size(), 2u);
  }

  // Lazy layer-2 reads match what was written.
  L2BodyView body;
  centroids.ReadL2Body(in, /*l1_id=*/0, body);
  ASSERT_EQ(body.n_l2, 2u);
  ASSERT_EQ(body.fine_ids.size(), 2u);
  EXPECT_EQ(body.fine_ids[0], 0u);
  EXPECT_EQ(body.fine_ids[1], 1u);
  {
    std::vector<float> c(body.n_l2 * d);
    std::memcpy(c.data(), body.l2_centroids, c.size() * sizeof(float));
    EXPECT_EQ(c, (std::vector<float>{0.f, 0.f, 1.f, 1.f}));
  }

  // SearchL2 within cell 0: query near (0,0) -> fine 0; near (1,1) -> fine 1.
  {
    std::vector<uint32_t> fine;
    centroids.SearchL2({std::vector<float>{0.1f, 0.1f}}, body, /*n2=*/1, fine);
    ASSERT_EQ(fine.size(), 1u);
    EXPECT_EQ(fine[0], 0u);
  }
  {
    std::vector<uint32_t> fine;
    centroids.SearchL2({std::vector<float>{0.9f, 0.9f}}, body, /*n2=*/1, fine);
    ASSERT_EQ(fine.size(), 1u);
    EXPECT_EQ(fine[0], 1u);
  }
  {
    std::vector<uint32_t> fine;
    centroids.SearchL2({std::vector<float>{0.5f, 0.5f}}, body, /*n2=*/5, fine);
    ASSERT_EQ(fine.size(), 2u);  // clamped to n_l2
  }

  // Cell 1 maps to fine ids {2, 3}.
  centroids.ReadL2Body(in, /*l1_id=*/1, body);
  ASSERT_EQ(body.n_l2, 2u);
  EXPECT_EQ(body.fine_ids[0], 2u);
  EXPECT_EQ(body.fine_ids[1], 3u);
  {
    std::vector<uint32_t> fine;
    centroids.SearchL2({std::vector<float>{10.1f, 10.1f}}, body, /*n2=*/1,
                       fine);
    ASSERT_EQ(fine.size(), 1u);
    EXPECT_EQ(fine[0], 2u);
  }

  // End-to-end navigation: nearest fine cluster to a query near (11,11).
  {
    const std::vector<float> q{11.f, 11.f};
    std::vector<uint32_t> l1_ids;
    centroids.SearchL1(q, /*n1=*/1, l1_ids);
    ASSERT_EQ(l1_ids.size(), 1u);
    centroids.ReadL2Body(in, l1_ids[0], body);
    std::vector<uint32_t> fine;
    centroids.SearchL2(q, body, /*n2=*/1, fine);
    ASSERT_EQ(fine.size(), 1u);
    EXPECT_EQ(fine[0], 3u);
  }
}

TEST(two_layer_centroids_test, inner_product_nearest_is_largest) {
  SimpleMemoryAccounter memory;
  MemoryFile file{memory};

  constexpr uint32_t d = 2;
  const std::vector<float> l1{/*c0*/ 1.f, 0.f, /*c1*/ 0.f, 1.f};
  const std::vector<L2BodySpec> bodies{
    L2BodySpec{.centroids = {1.f, 0.f}, .fine_ids = {0}},
    L2BodySpec{.centroids = {0.f, 1.f}, .fine_ids = {1}},
  };

  Serialized s;
  {
    MemoryIndexOutput out{file};
    s = WriteEntry(out, VectorMetric::InnerProduct, d, l1, bodies);
    out.Flush();
  }

  MemoryIndexInput in{file};
  in.Seek(s.resident_offset);
  auto centroids = TwoLayerCentroids::Deserialize(in, s.resident_size);
  EXPECT_EQ(centroids.Metric(), VectorMetric::InnerProduct);

  // Query aligned with c1 -> largest inner product picks L1 cell 1.
  const std::vector<float> q{0.f, 5.f};
  std::vector<uint32_t> l1_ids;
  centroids.SearchL1(q, /*n1=*/1, l1_ids);
  ASSERT_EQ(l1_ids.size(), 1u);
  EXPECT_EQ(l1_ids[0], 1u);
}
