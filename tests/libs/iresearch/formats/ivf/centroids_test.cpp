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
#include <array>
#include <span>
#include <vector>

#include "iresearch/formats/ivf/centroids.hpp"
#include "iresearch/store/data_output.hpp"
#include "iresearch/store/memory_directory.hpp"
#include "tests_shared.hpp"

using namespace irs;

namespace {

// Exclusive prefix-sum over per-centroid child counts -- same logic as
// CentroidsBuilder::Serialize's driver, duplicated here so tests can build
// trees by hand without going through CentroidsBuilder.
std::vector<size_t> ExclusiveScan(std::span<const size_t> counts) {
  std::vector<size_t> starts(counts.size());
  size_t running = 0;
  for (size_t i = 0; i < counts.size(); ++i) {
    starts[i] = running;
    running += counts[i];
  }
  return starts;
}

// Writes [IVFHeader][level][nodes...] exactly as CentroidsBuilder::Serialize
// does: `nodes` ordered coarsest-first, each node's body immediately
// followed by the next (finer) one, each node's own body grouped by its
// parent's cluster boundaries (root gets a single trivial group).
uint64_t WriteTree(IndexOutput& out, VectorMetric metric, uint32_t d,
                   std::span<const CentroidsNode> nodes) {
  const uint64_t offset = out.Position();
  IVFHeader{.metric = metric, .d = d}.Serialize(out);
  out.WriteU64(nodes.front().level);
  const std::array<size_t, 1> root_clusters{0};
  nodes.front().Serialize(out, root_clusters);
  for (size_t i = 1; i < nodes.size(); ++i) {
    nodes[i].Serialize(out, ExclusiveScan(nodes[i - 1].counts));
  }
  return offset;
}

}  // namespace

TEST(centroids_node_test, single_level_roundtrip_and_search) {
  SimpleMemoryAccounter memory;
  MemoryFile file{memory};

  constexpr uint32_t d = 2;
  CentroidsNode leaf{0, d};
  leaf.centroids = {0.f, 0.f, 10.f, 10.f, 20.f, 20.f, -10.f, -10.f};
  leaf.size = 4;

  uint64_t offset;
  uint64_t byte_size;
  {
    MemoryIndexOutput out{file};
    offset = WriteTree(out, VectorMetric::L2Sqr, d, {&leaf, 1});
    byte_size = out.Position() - offset;
    out.Flush();
  }

  MemoryIndexInput in{file};
  in.Seek(offset);
  auto tree = CentroidsTree::Deserialize(in, byte_size);
  EXPECT_EQ(tree.Dim(), d);
  EXPECT_EQ(tree.Metric(), VectorMetric::L2Sqr);
  EXPECT_FALSE(tree.Empty());
  EXPECT_TRUE(tree.QuantStats().empty());

  const std::vector<float> q{9.f, 9.f};
  std::vector<uint32_t> ids;
  std::vector<float> cens;

  // Nearest to q=(9,9) is c1=(10,10).
  tree.Search(q, in, /*nprobe=*/1, ids, &cens);
  ASSERT_EQ(ids.size(), 1u);
  EXPECT_EQ(ids[0], 1u);
  ASSERT_EQ(cens.size(), d);
  EXPECT_EQ(cens, (std::vector<float>{10.f, 10.f}));

  // nprobe=2 -> exactly the 2 nearest, no more.
  ids.clear();
  tree.Search(q, in, /*nprobe=*/2, ids, nullptr);
  ASSERT_EQ(ids.size(), 2u);

  // nprobe clamps to the total centroid count.
  ids.clear();
  tree.Search(q, in, /*nprobe=*/100, ids, nullptr);
  EXPECT_EQ(ids.size(), 4u);

  // nprobe=0 -> nothing.
  ids.clear();
  tree.Search(q, in, /*nprobe=*/0, ids, nullptr);
  EXPECT_TRUE(ids.empty());
}

TEST(centroids_node_test, two_level_roundtrip_and_search_respects_nprobe) {
  SimpleMemoryAccounter memory;
  MemoryFile file{memory};

  constexpr uint32_t d = 2;
  // Root groups leaf indices [0,2) under g0=(0.5,0.5) and [2,4) under
  // g1=(10.5,10.5) -- counts, not start indices.
  CentroidsNode root{1, d};
  root.centroids = {0.5f, 0.5f, 10.5f, 10.5f};
  root.counts = {2, 2};
  root.size = 2;

  CentroidsNode leaf{0, d};
  leaf.centroids = {0.f, 0.f, 1.f, 1.f, 10.f, 10.f, 11.f, 11.f};
  leaf.size = 4;

  uint64_t offset;
  uint64_t byte_size;
  {
    MemoryIndexOutput out{file};
    offset = WriteTree(out, VectorMetric::L2Sqr, d, {{root, leaf}});
    byte_size = out.Position() - offset;
    out.Flush();
  }

  MemoryIndexInput in{file};
  in.Seek(offset);
  auto tree = CentroidsTree::Deserialize(in, byte_size);
  EXPECT_EQ(tree.Dim(), d);

  {
    const std::vector<float> q{0.2f, 0.2f};
    std::vector<uint32_t> ids;
    tree.Search(q, in, /*nprobe=*/1, ids, nullptr);
    ASSERT_EQ(ids.size(), 1u);
    EXPECT_EQ(ids[0], 0u);
  }
  {
    const std::vector<float> q{9.5f, 9.5f};
    std::vector<uint32_t> ids;
    tree.Search(q, in, /*nprobe=*/1, ids, nullptr);
    ASSERT_EQ(ids.size(), 1u);
    EXPECT_EQ(ids[0], 2u);
  }
  {
    // nprobe=2 -> per-layer probe count is ceil(sqrt(2))=2 for this 2-level
    // tree, i.e. both root groups and both leaves within each are visited.
    const std::vector<float> q{0.4f, 0.4f};
    std::vector<uint32_t> ids;
    tree.Search(q, in, /*nprobe=*/2, ids, nullptr);
    ASSERT_EQ(ids.size(), 4u);
    std::sort(ids.begin(), ids.end());
    EXPECT_EQ(ids, (std::vector<uint32_t>{0, 1, 2, 3}));
  }
}

TEST(centroids_node_test, three_group_search_skips_unselected_groups) {
  // Three root groups of unequal size, written to disk grouped by parent
  // cluster (per-group [centroids][counts] blocks). Exercises fetching a
  // strict, non-contiguous subset of groups at the leaf level -- the batch
  // Deserialize must seek correctly past a skipped group's bytes rather
  // than assuming everything is read sequentially/contiguously.
  SimpleMemoryAccounter memory;
  MemoryFile file{memory};

  constexpr uint32_t d = 2;
  // g0 (2 leaves, near (0,0)), g1 (3 leaves, near (1000,1000), far away),
  // g2 (1 leaf, near (9,11)).
  CentroidsNode root{1, d};
  root.centroids = {0.f, 0.f, 1000.f, 1000.f, 9.f, 11.f};
  root.counts = {2, 3, 1};
  root.size = 3;

  CentroidsNode leaf{0, d};
  leaf.centroids = {
    0.f,    0.f,    1.f,    1.f,                   // g0's 2 leaves: ids 0,1
    1000.f, 1000.f, 1001.f, 1001.f, 999.f, 999.f,  // g1's 3 leaves: 2,3,4
    9.f,    11.f,                                  // g2's 1 leaf: id 5
  };
  leaf.size = 6;

  uint64_t offset;
  uint64_t byte_size;
  {
    MemoryIndexOutput out{file};
    offset = WriteTree(out, VectorMetric::L2Sqr, d, {{root, leaf}});
    byte_size = out.Position() - offset;
    out.Flush();
  }

  MemoryIndexInput in{file};
  in.Seek(offset);
  auto tree = CentroidsTree::Deserialize(in, byte_size);

  {
    // Nearest root group to (8,10) is g2 alone -- fetches the single leaf
    // at global index 5, which is NOT the first group on disk.
    const std::vector<float> q{8.f, 10.f};
    std::vector<uint32_t> ids;
    tree.Search(q, in, /*nprobe=*/1, ids, nullptr);
    ASSERT_EQ(ids.size(), 1u);
    EXPECT_EQ(ids[0], 5u);
  }
  {
    // g0 and g2 are both much closer to (5,5) than far-away g1 -- selects
    // two non-adjacent groups (g1's block in between must be skipped, not
    // read), and every leaf within both selected groups should come back.
    const std::vector<float> q{5.f, 5.f};
    std::vector<uint32_t> ids;
    tree.Search(q, in, /*nprobe=*/2, ids, nullptr);
    std::sort(ids.begin(), ids.end());
    EXPECT_EQ(ids, (std::vector<uint32_t>{0, 1, 5}));
  }
}

TEST(centroids_node_test, inner_product_nearest_is_largest) {
  SimpleMemoryAccounter memory;
  MemoryFile file{memory};

  constexpr uint32_t d = 2;
  CentroidsNode leaf{0, d};
  leaf.centroids = {1.f, 0.f, 0.f, 1.f};
  leaf.size = 2;

  uint64_t offset;
  uint64_t byte_size;
  {
    MemoryIndexOutput out{file};
    offset = WriteTree(out, VectorMetric::InnerProduct, d, {&leaf, 1});
    byte_size = out.Position() - offset;
    out.Flush();
  }

  MemoryIndexInput in{file};
  in.Seek(offset);
  auto tree = CentroidsTree::Deserialize(in, byte_size);
  EXPECT_EQ(tree.Metric(), VectorMetric::InnerProduct);

  // Query aligned with c1 -> largest inner product picks c1.
  const std::vector<float> q{0.f, 5.f};
  std::vector<uint32_t> ids;
  tree.Search(q, in, /*nprobe=*/1, ids, nullptr);
  ASSERT_EQ(ids.size(), 1u);
  EXPECT_EQ(ids[0], 1u);
}

TEST(centroids_tree_test, set_quant_stats_roundtrips) {
  SimpleMemoryAccounter memory;
  MemoryFile file{memory};

  constexpr uint32_t d = 2;
  CentroidsNode leaf{0, d};
  leaf.centroids = {0.f, 0.f};
  leaf.size = 1;

  uint64_t offset;
  uint64_t byte_size;
  {
    MemoryIndexOutput out{file};
    offset = WriteTree(out, VectorMetric::L2Sqr, d, {&leaf, 1});
    byte_size = out.Position() - offset;
    out.Flush();
  }

  MemoryIndexInput in{file};
  in.Seek(offset);
  auto tree = CentroidsTree::Deserialize(in, byte_size);
  EXPECT_TRUE(tree.QuantStats().empty());

  const bstring stats{reinterpret_cast<const byte_type*>("stats"), 5};
  tree.SetQuantStats(stats);
  EXPECT_EQ(tree.QuantStats(), stats);
}
