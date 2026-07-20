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

#include <faiss/utils/utils.h>

#include <algorithm>
#include <cmath>
#include <cstring>
#include <numeric>
#include <random>
#include <utility>
#include <vector>

#include "iresearch/formats/ivf/centroids.hpp"
#include "iresearch/formats/ivf/clustering.hpp"
#include "iresearch/store/data_output.hpp"
#include "iresearch/store/memory_directory.hpp"
#include "tests_shared.hpp"

using namespace irs;

namespace {

// Writes [IVFHeader][root level][layer blobs...] exactly as
// CentroidsBuilder::Serialize does: nodes coarsest-first, each layer's
// centroids followed by its child_offsets (size+1, absolute) unless it is the
// leaf layer.
uint64_t WriteTree(IndexOutput& out, VectorMetric metric, uint32_t d,
                   std::span<const CentroidsNode> nodes) {
  const uint64_t offset = out.Position();
  IVFHeader{.metric = metric, .d = d}.Serialize(out);
  out.WriteU64(nodes.front().level);
  for (const auto& node : nodes) {
    node.Serialize(out);
  }
  return offset;
}

// n_clusters well-separated blobs on a lattice, per_cluster points each with a
// tiny deterministic jitter. Returns row-major [n_clusters*per_cluster, d].
std::vector<float> MakeClusters(uint32_t d, size_t n_clusters,
                                size_t per_cluster) {
  std::mt19937 rng{123};
  std::uniform_real_distribution<float> jitter{-0.05f, 0.05f};
  std::vector<float> data;
  data.reserve(n_clusters * per_cluster * d);
  for (size_t c = 0; c < n_clusters; ++c) {
    for (size_t p = 0; p < per_cluster; ++p) {
      for (uint32_t j = 0; j < d; ++j) {
        // Clusters 1000 apart per dim-slot so routing is unambiguous.
        const float center = static_cast<float>((c >> j) & 0xf) * 1000.f;
        data.push_back(center + jitter(rng));
      }
    }
  }
  return data;
}

// n_clusters (<= d) direction-separated blobs: cluster c is dominant along axis
// c (value 100) with tiny positive noise, so every vector is far from the
// origin and normalizes to a distinct, well-separated direction -- the cosine
// analogue of MakeClusters (which places a blob at the origin, degenerate under
// cosine).
std::vector<float> MakeDirClusters(uint32_t d, size_t n_clusters,
                                   size_t per_cluster) {
  std::mt19937 rng{123};
  std::uniform_real_distribution<float> noise{0.f, 0.5f};
  std::vector<float> data;
  data.reserve(n_clusters * per_cluster * d);
  for (size_t c = 0; c < n_clusters; ++c) {
    for (size_t p = 0; p < per_cluster; ++p) {
      for (uint32_t j = 0; j < d; ++j) {
        data.push_back((j == c % d ? 100.f : 0.f) + noise(rng));
      }
    }
  }
  return data;
}

CentroidsBuildParams DeepParams() {
  return {.posting_size = 4, .min_centroids = 2, .max_centroids = 8};
}

// Build a genuine multi-level tree from an in-memory sample, then assert that
// the on-disk tree (Serialize -> Deserialize -> Search) routes every training
// vector to the exact same leaf id that the build-side AssignCentroids does.
TEST(centroids_builder_test, multilevel_build_search_id_consistency) {
  constexpr uint32_t d = 4;
  // per_cluster (3) <= posting_size (4): each well-separated blob lands in its
  // own leaf, so greedy build-descent and beam search route identically.
  const auto data = MakeClusters(d, /*n_clusters=*/64, /*per_cluster=*/3);
  const size_t n = data.size() / d;

  auto builder = CentroidsBuilder::CreateFromSample(
    data, d, VectorMetric::L2Sqr, DeepParams());
  const size_t n_clusters = builder.NumClusters();

  // Build-side assignment (reorders a copy; perm maps back to original rows).
  auto reordered = data;
  auto assigned =
    builder.AssignCentroids({reordered.data(), reordered.size()}, d);
  ASSERT_EQ(assigned.ids.size(), n);
  ASSERT_EQ(assigned.perm.size(), n);
  for (const size_t id : assigned.ids) {
    EXPECT_LT(id, n_clusters);
  }
  // perm is a permutation, and reordered row j is the original row perm[j].
  std::vector<char> seen(n, 0);
  for (size_t j = 0; j < n; ++j) {
    ASSERT_LT(assigned.perm[j], n);
    EXPECT_EQ(seen[assigned.perm[j]], 0);
    seen[assigned.perm[j]] = 1;
    const float* orig = data.data() + assigned.perm[j] * d;
    const float* row = reordered.data() + j * d;
    EXPECT_EQ(0, std::memcmp(orig, row, d * sizeof(float)));
  }

  // Map original row -> build-side leaf id.
  std::vector<size_t> build_id(n);
  for (size_t j = 0; j < n; ++j) {
    build_id[assigned.perm[j]] = assigned.ids[j];
  }

  SimpleMemoryAccounter memory;
  MemoryFile file{memory};
  uint64_t offset;
  uint64_t byte_size;
  {
    MemoryIndexOutput out{file};
    const auto span = builder.Serialize(out);
    offset = span.offset;
    byte_size = span.byte_size;
    out.Flush();
  }

  MemoryIndexInput in{file};
  in.Seek(offset);
  auto tree = CentroidsTree::Deserialize(in, byte_size);

  for (size_t i = 0; i < n; ++i) {
    const std::span<const float> q{data.data() + i * d, d};
    std::vector<uint32_t> ids;
    tree.Search(q, in, /*nprobe=*/1, ids, nullptr);
    ASSERT_EQ(ids.size(), 1u) << "row " << i;
    EXPECT_EQ(ids[0], build_id[i]) << "row " << i;
  }
}

// The optional gathered-centroid span returns, per vector, the same centroid
// Search reports for that vector's leaf.
TEST(centroids_builder_test, gathered_centroid_matches_search) {
  constexpr uint32_t d = 4;
  const auto data = MakeClusters(d, /*n_clusters=*/32, /*per_cluster=*/3);
  const size_t n = data.size() / d;

  auto builder = CentroidsBuilder::CreateFromSample(
    data, d, VectorMetric::L2Sqr, DeepParams());

  auto reordered = data;
  std::vector<std::span<const float>> gathered(n);
  auto assigned =
    builder.AssignCentroids({reordered.data(), reordered.size()}, d, gathered);

  SimpleMemoryAccounter memory;
  MemoryFile file{memory};
  uint64_t offset;
  uint64_t byte_size;
  {
    MemoryIndexOutput out{file};
    const auto span = builder.Serialize(out);
    offset = span.offset;
    byte_size = span.byte_size;
    out.Flush();
  }
  MemoryIndexInput in{file};
  in.Seek(offset);
  auto tree = CentroidsTree::Deserialize(in, byte_size);

  for (size_t j = 0; j < n; ++j) {
    ASSERT_EQ(gathered[j].size(), d);
    const std::span<const float> q{data.data() + assigned.perm[j] * d, d};
    std::vector<uint32_t> ids;
    std::vector<float> cens;
    tree.Search(q, in, /*nprobe=*/1, ids, &cens);
    ASSERT_EQ(ids.size(), 1u);
    ASSERT_EQ(cens.size(), d);
    EXPECT_EQ(0,
              std::memcmp(cens.data(), gathered[j].data(), d * sizeof(float)));
  }
}

// A leaf hanging directly off an interior layer is encoded as a zero-size child
// window (child_offsets[i+1] == child_offsets[i]); Search must emit it as a
// leaf candidate with the correct global id instead of descending.
TEST(centroids_node_test, zero_size_window_emits_early_leaf) {
  SimpleMemoryAccounter memory;
  MemoryFile file{memory};

  constexpr uint32_t d = 1;
  // Root row 0 is a leaf (window [0,0)); row 1 has two children in the leaf
  // layer. Global ids: root layer base 0 (rows 0,1), leaf layer base 2.
  CentroidsNode root{1, d};
  root.centroids = {0.f, 10.5f};
  root.child_offsets = {0, 0, 2};
  root.size = 2;
  CentroidsNode leaf{0, d};
  leaf.centroids = {10.f, 11.f};
  leaf.size = 2;

  std::vector<CentroidsNode> nodes;
  nodes.emplace_back(std::move(root));
  nodes.emplace_back(std::move(leaf));

  uint64_t offset;
  uint64_t byte_size;
  {
    MemoryIndexOutput out{file};
    offset = WriteTree(out, VectorMetric::L2Sqr, d, nodes);
    byte_size = out.Position() - offset;
    out.Flush();
  }

  MemoryIndexInput in{file};
  in.Seek(offset);
  auto tree = CentroidsTree::Deserialize(in, byte_size);

  // Query near the early leaf (root row 0) -> its global id 0.
  {
    const std::vector<float> q{0.2f};
    std::vector<uint32_t> ids;
    tree.Search(q, in, /*nprobe=*/1, ids, nullptr);
    ASSERT_EQ(ids.size(), 1u);
    EXPECT_EQ(ids[0], 0u);
  }
  // Query near the leaf-layer cells -> ids 2 / 3, never the early leaf.
  {
    const std::vector<float> q{10.1f};
    std::vector<uint32_t> ids;
    tree.Search(q, in, /*nprobe=*/1, ids, nullptr);
    ASSERT_EQ(ids.size(), 1u);
    EXPECT_EQ(ids[0], 2u);
  }
  {
    const std::vector<float> q{11.1f};
    std::vector<uint32_t> ids;
    tree.Search(q, in, /*nprobe=*/1, ids, nullptr);
    ASSERT_EQ(ids.size(), 1u);
    EXPECT_EQ(ids[0], 3u);
  }
  // Large nprobe surfaces every leaf (early leaf included).
  {
    const std::vector<float> q{5.f};
    std::vector<uint32_t> ids;
    tree.Search(q, in, /*nprobe=*/100, ids, nullptr);
    EXPECT_EQ(ids.size(), 3u);
  }
}

// A dataset too small to split builds a single real centroids node (the mean),
// not an empty tree: NumClusters()==1, every vector routes to cluster 0 with
// the mean as its gathered centroid, and the on-disk tree searches back to id
// 0. This is the centroid residual quantizers (PQ/RaBitQ) rely on.
TEST(centroids_builder_test, single_cluster_has_mean_centroid) {
  constexpr uint32_t d = 4;
  // 10 vectors << default posting_size -> root is a leaf.
  std::vector<float> data;
  for (size_t i = 0; i < 10; ++i) {
    for (uint32_t j = 0; j < d; ++j) {
      data.push_back(static_cast<float>(i * d + j));
    }
  }
  const size_t n = data.size() / d;

  std::vector<float> mean(d, 0.f);
  for (size_t i = 0; i < n; ++i) {
    for (uint32_t j = 0; j < d; ++j) {
      mean[j] += data[i * d + j];
    }
  }
  for (float& m : mean) {
    m /= static_cast<float>(n);
  }

  auto builder =
    CentroidsBuilder::CreateFromSample(data, d, VectorMetric::L2Sqr, {});
  EXPECT_EQ(builder.NumClusters(), 1u);

  auto reordered = data;
  std::vector<std::span<const float>> cents(n);
  auto assigned =
    builder.AssignCentroids({reordered.data(), reordered.size()}, d, cents);
  for (size_t j = 0; j < n; ++j) {
    EXPECT_EQ(assigned.ids[j], 0u);
    ASSERT_EQ(cents[j].size(), d);
    for (uint32_t k = 0; k < d; ++k) {
      EXPECT_NEAR(cents[j][k], mean[k], 1e-3f) << "row " << j << " dim " << k;
    }
  }

  SimpleMemoryAccounter memory;
  MemoryFile file{memory};
  uint64_t offset;
  uint64_t byte_size;
  {
    MemoryIndexOutput out{file};
    const auto span = builder.Serialize(out);
    offset = span.offset;
    byte_size = span.byte_size;
    out.Flush();
  }
  MemoryIndexInput in{file};
  in.Seek(offset);
  auto tree = CentroidsTree::Deserialize(in, byte_size);
  const std::span<const float> q{data.data(), d};
  std::vector<uint32_t> ids;
  tree.Search(q, in, /*nprobe=*/1, ids, nullptr);
  ASSERT_EQ(ids.size(), 1u);
  EXPECT_EQ(ids[0], 0u);
}

// Cosine build must route consistently after the "normalize the sample once in
// the builder" change: the tree is trained/assigned on unit-normalized vectors
// while AssignCentroids/Search operate on the raw vectors (direction-only
// routing against unit centroids), and both must pick the same leaf.
TEST(centroids_builder_test, cosine_multilevel_build_search_id_consistency) {
  constexpr uint32_t d = 16;
  const auto data = MakeDirClusters(d, /*n_clusters=*/16, /*per_cluster=*/3);
  const size_t n = data.size() / d;

  auto builder = CentroidsBuilder::CreateFromSample(
    data, d, VectorMetric::Cosine, DeepParams());
  const size_t n_clusters = builder.NumClusters();

  auto reordered = data;
  auto assigned =
    builder.AssignCentroids({reordered.data(), reordered.size()}, d);
  ASSERT_EQ(assigned.ids.size(), n);
  std::vector<size_t> build_id(n);
  for (size_t j = 0; j < n; ++j) {
    ASSERT_LT(assigned.ids[j], n_clusters);
    build_id[assigned.perm[j]] = assigned.ids[j];
  }

  SimpleMemoryAccounter memory;
  MemoryFile file{memory};
  uint64_t offset;
  uint64_t byte_size;
  {
    MemoryIndexOutput out{file};
    const auto span = builder.Serialize(out);
    offset = span.offset;
    byte_size = span.byte_size;
    out.Flush();
  }
  MemoryIndexInput in{file};
  in.Seek(offset);
  auto tree = CentroidsTree::Deserialize(in, byte_size);

  for (size_t i = 0; i < n; ++i) {
    const std::span<const float> q{data.data() + i * d, d};
    std::vector<uint32_t> ids;
    tree.Search(q, in, /*nprobe=*/1, ids, nullptr);
    ASSERT_EQ(ids.size(), 1u) << "row " << i;
    EXPECT_EQ(ids[0], build_id[i]) << "row " << i;
  }
}

// Cosine at k >= 4096 takes the SuperKMeans branch (on already-normalized
// input) and re-normalizes the centroids; below that it uses spherical
// Clustering. Both must yield unit-norm centroids (empty clusters may stay
// zero).
TEST(clustering_test, cosine_centroids_are_unit_norm) {
  constexpr uint32_t d = 64;
  std::mt19937 rng{7};
  std::normal_distribution<float> g{0.f, 1.f};

  const auto train = [&](size_t n, uint32_t k) {
    std::vector<float> data(n * d);
    for (auto& x : data) {
      x = g(rng);
    }
    NormalizeRows(data.data(), n, d);
    auto centroids = TrainCentroids(VectorMetric::Cosine, data.data(), n, k, d,
                                    /*seed=*/1, /*niter=*/2);
    ASSERT_EQ(centroids.size(), static_cast<size_t>(k) * d);
    for (uint32_t c = 0; c < k; ++c) {
      float sum = 0.f;
      for (uint32_t j = 0; j < d; ++j) {
        const float v = centroids[c * d + j];
        sum += v * v;
      }
      const float norm = std::sqrt(sum);
      EXPECT_TRUE(norm < 1e-3f || std::abs(norm - 1.f) < 1e-3f)
        << "k=" << k << " centroid " << c << " norm " << norm;
    }
  };

  train(/*n=*/2000, /*k=*/64);     // spherical Clustering path
  train(/*n=*/16384, /*k=*/4096);  // SuperKMeans path (k >= threshold)
}

// Regression: a genuine >=3-layer tree (binary splits over 8 separated points
// -> root -> mid -> leaf-parent layers) must route every vector to the same
// leaf id on the deserialized tree as the build-side AssignCentroids. Exercises
// the descent leaf-id numbering across >=3 layers (the 2-layer case is covered
// by multilevel_build_search_id_consistency).
TEST(centroids_builder_test, three_level_build_search_id_consistency) {
  constexpr uint32_t d = 8;
  const auto data = MakeClusters(d, /*n_clusters=*/256, /*per_cluster=*/1);
  const size_t n = data.size() / d;

  const CentroidsBuildParams params{
    .posting_size = 1, .min_centroids = 2, .max_centroids = 4};
  auto builder =
    CentroidsBuilder::CreateFromSample(data, d, VectorMetric::L2Sqr, params);
  const size_t n_clusters = builder.NumClusters();

  auto reordered = data;
  auto assigned =
    builder.AssignCentroids({reordered.data(), reordered.size()}, d);
  ASSERT_EQ(assigned.ids.size(), n);
  std::vector<size_t> build_id(n);
  for (size_t j = 0; j < n; ++j) {
    ASSERT_LT(assigned.ids[j], n_clusters);
    build_id[assigned.perm[j]] = assigned.ids[j];
  }

  SimpleMemoryAccounter memory;
  MemoryFile file{memory};
  uint64_t offset;
  uint64_t byte_size;
  {
    MemoryIndexOutput out{file};
    const auto span = builder.Serialize(out);
    offset = span.offset;
    byte_size = span.byte_size;
    out.Flush();
  }
  MemoryIndexInput in{file};
  in.Seek(offset);
  auto tree = CentroidsTree::Deserialize(in, byte_size);

  for (size_t i = 0; i < n; ++i) {
    const std::span<const float> q{data.data() + i * d, d};
    std::vector<uint32_t> ids;
    tree.Search(q, in, /*nprobe=*/1, ids, nullptr);
    ASSERT_EQ(ids.size(), 1u) << "row " << i;
    EXPECT_EQ(ids[0], build_id[i]) << "row " << i;
  }
}

// Recall regression for the depth-independent search beam. On a genuine
// multi-level (>=3-layer) tree, Search(q, nprobe) must recover the true
// top-nprobe leaves ranked by exact centroid distance. The previous beam
// (3*nprobe^(1/L)) collapsed as the tree deepened and greedily pruned true
// leaves; the depth-independent beam (ceil(3*sqrt(nprobe)), floored to nprobe
// on >=3-layer trees) keeps recall exact here.
TEST(centroids_builder_test, multilevel_search_recall_matches_bruteforce) {
  constexpr uint32_t d = 8;
  // posting_size=1 with max_centroids=4 forces a deep (>=4-layer) tree whose
  // widest internal layer (64 nodes) is below the query nprobe, so a correct
  // beam visits every node and Search is exact.
  const auto data = MakeClusters(d, /*n_clusters=*/256, /*per_cluster=*/1);
  const size_t n = data.size() / d;

  const CentroidsBuildParams params{
    .posting_size = 1, .min_centroids = 2, .max_centroids = 4};
  auto builder =
    CentroidsBuilder::CreateFromSample(data, d, VectorMetric::L2Sqr, params);

  SimpleMemoryAccounter memory;
  MemoryFile file{memory};
  uint64_t offset;
  uint64_t byte_size;
  {
    MemoryIndexOutput out{file};
    const auto span = builder.Serialize(out);
    offset = span.offset;
    byte_size = span.byte_size;
    out.Flush();
  }
  MemoryIndexInput in{file};
  in.Seek(offset);
  auto tree = CentroidsTree::Deserialize(in, byte_size);

  // Enumerate every leaf id + centroid via an all-covering probe.
  std::vector<uint32_t> leaf_ids;
  std::vector<float> leaf_cens;
  tree.Search(std::span<const float>{data.data(), d}, in,
              static_cast<uint32_t>(n), leaf_ids, &leaf_cens);
  const size_t n_leaves = leaf_ids.size();
  ASSERT_GT(n_leaves, 1u);
  ASSERT_EQ(leaf_cens.size(), n_leaves * d);

  const auto l2 = [&](const float* a, const float* b) {
    float s = 0.f;
    for (uint32_t j = 0; j < d; ++j) {
      const float e = a[j] - b[j];
      s += e * e;
    }
    return s;
  };

  const uint32_t nprobe = 128;
  const uint32_t k =
    std::min<uint32_t>(nprobe, static_cast<uint32_t>(n_leaves));
  size_t hit = 0;
  size_t total = 0;
  for (size_t i = 0; i < n; ++i) {
    const float* q = data.data() + i * d;
    std::vector<std::pair<float, uint32_t>> scored;
    scored.reserve(n_leaves);
    for (size_t l = 0; l < n_leaves; ++l) {
      scored.emplace_back(l2(q, leaf_cens.data() + l * d), leaf_ids[l]);
    }
    std::partial_sort(scored.begin(), scored.begin() + k, scored.end());

    std::vector<uint32_t> got;
    tree.Search(std::span<const float>{q, d}, in, nprobe, got, nullptr);
    for (uint32_t t = 0; t < k; ++t) {
      if (std::find(got.begin(), got.end(), scored[t].second) != got.end()) {
        ++hit;
      }
    }
    total += k;
  }
  const double recall = static_cast<double>(hit) / static_cast<double>(total);
  EXPECT_GE(recall, 0.999) << "multi-level Search recall vs brute force";
}

TEST(matrix_qr_test, blocked_qr_orthonormal_and_spanning) {
  std::mt19937 rng{7};
  std::normal_distribution<float> nd{0.f, 1.f};
  const std::vector<std::pair<int, int>> shapes = {
    {4, 4}, {9, 5}, {33, 17}, {64, 64}, {96, 64}, {256, 200}};
  for (const auto [m, n] : shapes) {
    std::vector<float> a(static_cast<size_t>(m) * n);
    for (auto& x : a) {
      x = nd(rng);
    }
    std::vector<float> q = a;
    faiss::matrix_qr(m, n, q.data());

    for (int i = 0; i < n; ++i) {
      for (int j = i; j < n; ++j) {
        double dot = 0.0;
        for (int r = 0; r < m; ++r) {
          dot += static_cast<double>(q[r + static_cast<size_t>(i) * m]) *
                 q[r + static_cast<size_t>(j) * m];
        }
        ASSERT_NEAR(dot, i == j ? 1.0 : 0.0, 2e-3)
          << "m=" << m << " n=" << n << " i=" << i << " j=" << j;
      }
    }

    for (int col = 0; col < n; col += std::max(1, n / 8)) {
      std::vector<double> coef(n, 0.0);
      for (int i = 0; i < n; ++i) {
        double c = 0.0;
        for (int r = 0; r < m; ++r) {
          c += static_cast<double>(q[r + static_cast<size_t>(i) * m]) *
               a[r + static_cast<size_t>(col) * m];
        }
        coef[i] = c;
      }
      for (int r = 0; r < m; ++r) {
        double p = 0.0;
        for (int i = 0; i < n; ++i) {
          p += coef[i] * q[r + static_cast<size_t>(i) * m];
        }
        const double want = a[r + static_cast<size_t>(col) * m];
        ASSERT_NEAR(p, want, 2e-3 * (1.0 + std::fabs(want)))
          << "m=" << m << " n=" << n << " col=" << col << " r=" << r;
      }
    }
  }
}

double KMeansObjective(const float* data, size_t n, const float* c, uint32_t k,
                       uint32_t d) {
  double total = 0.0;
  for (size_t i = 0; i < n; ++i) {
    const float* x = data + i * d;
    double best = 0.0;
    for (uint32_t j = 0; j < k; ++j) {
      const float* cj = c + static_cast<size_t>(j) * d;
      double s = 0.0;
      for (uint32_t t = 0; t < d; ++t) {
        const double diff = static_cast<double>(x[t]) - cj[t];
        s += diff * diff;
      }
      if (j == 0 || s < best) {
        best = s;
      }
    }
    total += best;
  }
  return total;
}

TEST(clustering_test, hskm_matches_kmeans_quality_and_is_deterministic) {
  const uint32_t d = 64;
  const size_t nclusters = 64, per = 20;
  auto data = MakeClusters(d, nclusters, per);
  const size_t n = nclusters * per;
  const uint32_t k = 64;

  auto hskm = TrainCentroids(VectorMetric::L2Sqr, data.data(), n, k, d,
                             /*seed=*/1234, /*niter=*/8, /*nredo=*/1,
                             ClusteringAlgo::Hskm);
  auto lloyd = TrainCentroids(VectorMetric::L2Sqr, data.data(), n, k, d,
                              /*seed=*/1234, /*niter=*/8, /*nredo=*/1,
                              ClusteringAlgo::Lloyd);

  ASSERT_EQ(hskm.size(), static_cast<size_t>(k) * d);
  for (float x : hskm) {
    ASSERT_TRUE(std::isfinite(x));
  }

  const double obj_hskm = KMeansObjective(data.data(), n, hskm.data(), k, d);
  const double obj_lloyd = KMeansObjective(data.data(), n, lloyd.data(), k, d);
  EXPECT_LE(obj_hskm, 1.5 * obj_lloyd)
    << "hskm=" << obj_hskm << " lloyd=" << obj_lloyd;

  auto hskm2 = TrainCentroids(VectorMetric::L2Sqr, data.data(), n, k, d, 1234,
                              8, 1, ClusteringAlgo::Hskm);
  ASSERT_EQ(hskm.size(), hskm2.size());
  EXPECT_EQ(
    0, std::memcmp(hskm.data(), hskm2.data(), hskm.size() * sizeof(float)));
}

TEST(clustering_test, hskm_hierarchical_structure_and_determinism) {
  const uint32_t d = 32;
  auto data = MakeClusters(d, /*n_clusters=*/1024, /*per_cluster=*/4);
  const size_t n = 1024 * 4;
  const uint32_t k = 1024;

  auto h = RunHskmHierarchical(data.data(), n, k, d, /*seed=*/99);

  size_t fine_total = 0;
  for (const auto& block : h.fine) {
    ASSERT_EQ(block.size() % d, 0u);
    fine_total += block.size() / d;
    for (float x : block) {
      ASSERT_TRUE(std::isfinite(x));
    }
  }
  EXPECT_EQ(fine_total, k);
  EXPECT_EQ(h.meso.size() / d, h.fine.size());
  EXPECT_GE(h.fine.size(), 1u);
  for (float x : h.meso) {
    ASSERT_TRUE(std::isfinite(x));
  }

  auto h2 = RunHskmHierarchical(data.data(), n, k, d, /*seed=*/99);
  ASSERT_EQ(h.meso.size(), h2.meso.size());
  EXPECT_EQ(0, std::memcmp(h.meso.data(), h2.meso.data(),
                           h.meso.size() * sizeof(float)));
  ASSERT_EQ(h.fine.size(), h2.fine.size());
  for (size_t g = 0; g < h.fine.size(); ++g) {
    ASSERT_EQ(h.fine[g].size(), h2.fine[g].size());
    EXPECT_EQ(0, std::memcmp(h.fine[g].data(), h2.fine[g].data(),
                             h.fine[g].size() * sizeof(float)));
  }
}

// A build large enough to trip the meso-reuse gate (d>=32, target leaves>=1024)
// yields a 2-level tree (meso coarse layer + fine leaves) whose deserialized
// Search routes every training vector to the same leaf id as build-side
// AssignCentroids. Well-separated blobs make greedy descent and beam search
// agree.
TEST(centroids_builder_test, meso_reuse_two_level_build_search_id_consistency) {
  constexpr uint32_t d = 32;
  const auto data = MakeClusters(d, /*n_clusters=*/1024, /*per_cluster=*/4);
  const size_t n = data.size() / d;
  const size_t target_leaves = 1024;  // ceil(n / posting_size)

  const CentroidsBuildParams params{
    .posting_size = 4, .min_centroids = 2, .max_centroids = 4096};
  auto builder =
    CentroidsBuilder::CreateFromSample(data, d, VectorMetric::L2Sqr, params);
  const size_t n_clusters = builder.NumClusters();
  // A flat build would have exactly `target_leaves` centroids; the extra rows
  // are the meso coarse layer, so this confirms meso-reuse actually fired.
  EXPECT_GT(n_clusters, target_leaves);

  auto reordered = data;
  auto assigned =
    builder.AssignCentroids({reordered.data(), reordered.size()}, d);
  ASSERT_EQ(assigned.ids.size(), n);
  std::vector<size_t> build_id(n);
  for (size_t j = 0; j < n; ++j) {
    ASSERT_LT(assigned.ids[j], n_clusters);
    build_id[assigned.perm[j]] = assigned.ids[j];
  }

  SimpleMemoryAccounter memory;
  MemoryFile file{memory};
  uint64_t offset;
  uint64_t byte_size;
  {
    MemoryIndexOutput out{file};
    const auto span = builder.Serialize(out);
    offset = span.offset;
    byte_size = span.byte_size;
    out.Flush();
  }
  MemoryIndexInput in{file};
  in.Seek(offset);
  auto tree = CentroidsTree::Deserialize(in, byte_size);

  size_t matches = 0;
  for (size_t i = 0; i < n; ++i) {
    const std::span<const float> q{data.data() + i * d, d};
    std::vector<uint32_t> ids;
    tree.Search(q, in, /*nprobe=*/1, ids, nullptr);
    ASSERT_EQ(ids.size(), 1u) << "row " << i;
    ASSERT_LT(ids[0], n_clusters) << "row " << i;
    matches += (ids[0] == build_id[i]);
  }
  // Greedy build-descent and beam Search agree for all but a few near-boundary
  // points (meso cells don't align exactly with fine cells). A leaf-numbering
  // or child-offset bug would instead break nearly every row.
  EXPECT_GE(matches, static_cast<size_t>(0.99 * static_cast<double>(n)));
}

TEST(clustering_test, hskm_angular_centroids_unit_norm) {
  const uint32_t d = 64;
  auto data = MakeClusters(d, 40, 20);
  const size_t n = 40 * 20;
  NormalizeRows(data.data(), n, d);
  const uint32_t k = 32;
  auto c =
    TrainCentroids(VectorMetric::Cosine, data.data(), n, k, d,
                   /*seed=*/7, /*niter=*/8, /*nredo=*/1, ClusteringAlgo::Hskm);
  ASSERT_EQ(c.size(), static_cast<size_t>(k) * d);
  for (uint32_t j = 0; j < k; ++j) {
    double s = 0.0;
    for (uint32_t t = 0; t < d; ++t) {
      const double v = c[static_cast<size_t>(j) * d + t];
      s += v * v;
    }
    EXPECT_NEAR(std::sqrt(s), 1.0, 1e-3) << "centroid " << j;
  }
}

}  // namespace
