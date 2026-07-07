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

#include "iresearch/formats/ivf/ivf_writer.hpp"

#include "tests_shared.hpp"

using namespace irs;

TEST(ivf_writer_test, resolve_centroid_shape_brute_force_below_threshold) {
  for (const uint64_t valid_count : {500u, 999u}) {
    const auto shape = IvfBuilder::ResolveCentroidShape(valid_count, IvfInfo{});
    EXPECT_EQ(shape.kind, CentroidShapeKind::BruteForce);
    EXPECT_EQ(shape.total_target, 1u);
    EXPECT_EQ(shape.n_l1, 1u);
    EXPECT_EQ(shape.n_l2_target, 1u);
  }
}

TEST(ivf_writer_test, resolve_centroid_shape_brute_force_ignores_nlist_override) {
  const auto shape =
    IvfBuilder::ResolveCentroidShape(500, IvfInfo{.nlist = 50});
  EXPECT_EQ(shape.kind, CentroidShapeKind::BruteForce);
  EXPECT_EQ(shape.total_target, 1u);
  EXPECT_EQ(shape.n_l1, 1u);
  EXPECT_EQ(shape.n_l2_target, 1u);
}

TEST(ivf_writer_test, resolve_centroid_shape_flat_at_lower_boundary) {
  const auto shape = IvfBuilder::ResolveCentroidShape(1000, IvfInfo{});
  EXPECT_EQ(shape.kind, CentroidShapeKind::Flat);
  EXPECT_EQ(shape.n_l1, 1u);
  EXPECT_EQ(shape.n_l2_target, shape.total_target);
  EXPECT_GT(shape.n_l2_target, 1u);
}

TEST(ivf_writer_test, resolve_centroid_shape_flat_mid_range) {
  const auto shape = IvfBuilder::ResolveCentroidShape(5000, IvfInfo{});
  EXPECT_EQ(shape.kind, CentroidShapeKind::Flat);
  EXPECT_EQ(shape.n_l1, 1u);
  EXPECT_EQ(shape.n_l2_target, shape.total_target);
  EXPECT_GT(shape.n_l2_target, 1u);
}

TEST(ivf_writer_test, resolve_centroid_shape_flat_honors_explicit_nlist) {
  const auto shape =
    IvfBuilder::ResolveCentroidShape(5000, IvfInfo{.nlist = 77});
  EXPECT_EQ(shape.kind, CentroidShapeKind::Flat);
  EXPECT_EQ(shape.n_l1, 1u);
  EXPECT_EQ(shape.total_target, 77u);
  EXPECT_EQ(shape.n_l2_target, 77u);
}

TEST(ivf_writer_test, resolve_centroid_shape_flat_at_upper_boundary) {
  const auto shape = IvfBuilder::ResolveCentroidShape(255999, IvfInfo{});
  EXPECT_EQ(shape.kind, CentroidShapeKind::Flat);
  EXPECT_EQ(shape.n_l1, 1u);
  EXPECT_EQ(shape.n_l2_target, shape.total_target);
  EXPECT_GT(shape.n_l2_target, 1u);
}

TEST(ivf_writer_test, resolve_centroid_shape_two_layer_at_threshold) {
  const auto shape = IvfBuilder::ResolveCentroidShape(256000, IvfInfo{});
  EXPECT_EQ(shape.kind, CentroidShapeKind::TwoLayer);
  EXPECT_GT(shape.n_l1, 1u);
  EXPECT_GT(shape.n_l2_target, 1u);
}

TEST(ivf_writer_test, resolve_centroid_shape_two_layer_large) {
  const auto shape = IvfBuilder::ResolveCentroidShape(300000, IvfInfo{});
  EXPECT_EQ(shape.kind, CentroidShapeKind::TwoLayer);
  EXPECT_GT(shape.n_l1, 1u);
  EXPECT_GT(shape.n_l2_target, 1u);
}
