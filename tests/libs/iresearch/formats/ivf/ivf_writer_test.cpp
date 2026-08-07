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

#include <cstdint>
#include <vector>

#include "iresearch/formats/ivf/ivf_writer.hpp"
#include "tests_shared.hpp"

using namespace irs;

namespace {

constexpr uint64_t kRows = 250000;
constexpr uint32_t kGranularity = 32;
constexpr uint64_t kMinPosting = 64;
constexpr uint64_t kMaxPosting = 8192;

// Mirrors QuantizerWriter::ScanCostBytes() for the shipped quantizers, so the
// resolver can be exercised without building a real writer.
uint64_t Sq8ScanBytes(uint32_t d) { return uint64_t{8} * d * 12 / 16; }
uint64_t Sq4ScanBytes(uint32_t d) { return uint64_t{9} * d * 12 / 16; }

uint64_t PqScanBytes(uint32_t d) {
  const uint64_t m = d / 2;  // kTargetDsub = 2
  const uint64_t nsq = m + (m & 1);
  return m * 4 / 8 + nsq * 16 / 32;
}

uint64_t RaBitQScanBytes(uint32_t d) {
  uint64_t rd = 1;
  while (rd < d) {
    rd <<= 1;
  }
  const uint64_t nsq_raw = rd / 4;
  const uint64_t nsq = nsq_raw + (nsq_raw & 1);
  return nsq / 2 + 8 + sizeof(float) + nsq * 16 / 32;
}

}  // namespace

TEST(ivf_tree_shape_test, auto_shape_admits_a_single_flat_level) {
  for (const uint32_t d : {384u, 768u, 1024u, 1536u}) {
    for (const uint64_t scan : {Sq8ScanBytes(d), Sq4ScanBytes(d),
                                PqScanBytes(d), RaBitQScanBytes(d)}) {
      const auto shape = ResolveIvfTreeShape(kRows, d, scan, 0, 0);
      EXPECT_GE(shape.max_centroids * shape.posting_size, kRows);
      EXPECT_EQ(0U, shape.posting_size % kGranularity);
      EXPECT_GE(shape.posting_size, kMinPosting);
      EXPECT_LE(shape.posting_size, kMaxPosting);
    }
  }
}

TEST(ivf_tree_shape_test, posting_size_is_dimension_invariant) {
  // code_size is proportional to d for sq8, sq4 and pq, so d cancels out of
  // 4*d / ScanCostBytes() exactly.
  for (const auto& fn : std::vector<uint64_t (*)(uint32_t)>{
         &Sq8ScanBytes, &Sq4ScanBytes, &PqScanBytes}) {
    const auto base = ResolveIvfTreeShape(kRows, 384, fn(384), 0, 0);
    for (const uint32_t d : {768u, 1024u, 1536u}) {
      EXPECT_EQ(base.posting_size,
                ResolveIvfTreeShape(kRows, d, fn(d), 0, 0).posting_size);
    }
  }
}

TEST(ivf_tree_shape_test, fast_scan_wants_larger_postings_than_scalar) {
  // A FastScan scan is roughly an order of magnitude cheaper per vector, so the
  // routing term dominates for longer and the optimum sits higher.
  const auto sq8 = ResolveIvfTreeShape(kRows, 1536, Sq8ScanBytes(1536), 0, 0);
  const auto pq = ResolveIvfTreeShape(kRows, 1536, PqScanBytes(1536), 0, 0);
  EXPECT_GT(pq.posting_size, sq8.posting_size);
}

TEST(ivf_tree_shape_test, posting_size_grows_with_sqrt_of_rows) {
  const auto scan = Sq8ScanBytes(1536);
  const auto small = ResolveIvfTreeShape(250000, 1536, scan, 0, 0);
  const auto large = ResolveIvfTreeShape(4000000, 1536, scan, 0, 0);
  // 16x the rows is 4x the posting size, up to the rounding granularity.
  EXPECT_NEAR(static_cast<double>(large.posting_size),
              4.0 * static_cast<double>(small.posting_size),
              static_cast<double>(kGranularity));
}

TEST(ivf_tree_shape_test, pinned_posting_size_sizes_the_root_to_match) {
  const auto shape =
    ResolveIvfTreeShape(kRows, 1536, Sq8ScanBytes(1536), 512, 0);
  EXPECT_EQ(512U, shape.posting_size);
  EXPECT_GE(shape.max_centroids * shape.posting_size, kRows);
}

TEST(ivf_tree_shape_test, a_deep_tree_needs_max_centroids_pinned_too) {
  // A small posting size alone no longer produces a multi-level tree: the root
  // widens to match it. Driving the leaf count past the fanout cap now takes an
  // explicit max_centroids, which is what the multi-level sqllogic tests pin.
  const auto flat = ResolveIvfTreeShape(10000, 2, 0, 2, 0);
  EXPECT_EQ(5000U, flat.max_centroids);

  const auto deep = ResolveIvfTreeShape(10000, 2, 0, 2, 1024);
  EXPECT_EQ(1024U, deep.max_centroids);
  EXPECT_LT(deep.max_centroids * deep.posting_size, 10000U);
}

TEST(ivf_tree_shape_test, pinned_narrow_root_raises_the_posting_size) {
  const auto scan = Sq8ScanBytes(1536);
  const auto autos = ResolveIvfTreeShape(kRows, 1536, scan, 0, 0);
  const auto pinned = ResolveIvfTreeShape(kRows, 1536, scan, 0, 64);
  EXPECT_EQ(64U, pinned.max_centroids);
  EXPECT_GT(pinned.posting_size, autos.posting_size);
  EXPECT_GE(pinned.max_centroids * pinned.posting_size, kRows);
}

TEST(ivf_tree_shape_test, pinned_wide_root_leaves_the_cost_optimum_alone) {
  const auto scan = Sq8ScanBytes(1536);
  const auto autos = ResolveIvfTreeShape(kRows, 1536, scan, 0, 0);
  const auto pinned = ResolveIvfTreeShape(kRows, 1536, scan, 0, 100000);
  EXPECT_EQ(autos.posting_size, pinned.posting_size);
}

TEST(ivf_tree_shape_test, pinning_both_is_honoured_verbatim) {
  // The escape hatch that forces a deep tree: with both pinned the one-shot
  // split condition need not hold, which is how the multi-level read path is
  // exercised.
  const auto shape = ResolveIvfTreeShape(kRows, 1536, Sq8ScanBytes(1536), 2, 4);
  EXPECT_EQ(2U, shape.posting_size);
  EXPECT_EQ(4U, shape.max_centroids);
}

TEST(ivf_tree_shape_test, degenerate_inputs_stay_in_range) {
  const auto tiny = ResolveIvfTreeShape(1, 1536, Sq8ScanBytes(1536), 0, 0);
  EXPECT_GE(tiny.posting_size, kMinPosting);
  EXPECT_GE(tiny.max_centroids, 1U);

  // An unquantized column reports no scan cost; 4*d is used for both terms.
  const auto raw = ResolveIvfTreeShape(kRows, 1536, 0, 0, 0);
  EXPECT_GE(raw.posting_size, kMinPosting);
  EXPECT_LE(raw.posting_size, kMaxPosting);
  EXPECT_GE(raw.max_centroids * raw.posting_size, kRows);
}
