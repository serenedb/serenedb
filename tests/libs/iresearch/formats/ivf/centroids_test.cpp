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
#include <duckdb.hpp>
#include <duckdb/common/vector/array_vector.hpp>
#include <limits>
#include <numeric>
#include <random>
#include <span>
#include <utility>
#include <vector>

#include "iresearch/formats/column/col_reader.hpp"
#include "iresearch/formats/column/col_writer.hpp"
#include "iresearch/formats/column/column_reader.hpp"
#include "iresearch/formats/column/read_context.hpp"
#include "iresearch/formats/ivf/centroids.hpp"
#include "iresearch/formats/ivf/clustering.hpp"
#include "iresearch/formats/ivf/quantizer.hpp"
#include "iresearch/store/data_output.hpp"
#include "iresearch/store/memory_directory.hpp"
#include "tests_shared.hpp"

using namespace irs;

namespace {

constexpr uint32_t kDefaultMaxFanout = 16;

// Writes [IVFHeader][root level][layer blobs...] exactly as
// CentroidsBuilder::Serialize does: nodes coarsest-first, each layer's
// centroids followed by its child_offsets (size+1, absolute) unless it is the
// leaf layer.
// Legacy row-major layout only: a rotated tree writes level-major bodies.
void WriteNode(IndexOutput& out, const CentroidsNode& node) {
  out.WriteU64(node.size);
  if (node.size != 0) {
    out.WriteData(reinterpret_cast<const byte_type*>(node.centroids.data()),
                  node.size * node.d * sizeof(float));
  }
  if (node.level > 0) {
    ASSERT_EQ(node.child_offsets.size(), node.size + 1);
    out.WriteData(reinterpret_cast<const byte_type*>(node.child_offsets.data()),
                  (node.size + 1) * sizeof(size_t));
  }
}

uint64_t WriteTree(IndexOutput& out, VectorMetric metric, uint32_t d,
                   std::span<const CentroidsNode> nodes) {
  const uint64_t offset = out.Position();
  IVFHeader{.metric = metric, .d = d}.Serialize(out);
  out.WriteU64(nodes.front().level);
  for (const auto& node : nodes) {
    WriteNode(out, node);
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
  return {.posting_size = 4, .max_centroids = 4};
}

void WriteVectorColumn(Directory& dir, duckdb::DatabaseInstance& db,
                       field_id id, uint32_t d, std::span<const float> data) {
  const uint64_t n = data.size() / d;
  const auto atype = duckdb::LogicalType::ARRAY(duckdb::LogicalType::FLOAT, d);
  ColWriter w{dir, "seg", db};
  auto& cw = w.OpenColumn(id, atype, /*skip_validity=*/false, /*rg_size=*/4096);
  uint64_t pos = 0;
  while (pos < n) {
    const auto take = std::min<duckdb::idx_t>(n - pos, STANDARD_VECTOR_SIZE);
    duckdb::Vector v{atype, STANDARD_VECTOR_SIZE};
    auto& child = duckdb::ArrayVector::GetChildMutable(v);
    auto* cd = duckdb::FlatVector::GetDataMutable<float>(child);
    auto& av = duckdb::FlatVector::ValidityMutable(v);
    auto& cv = duckdb::FlatVector::ValidityMutable(child);
    av.Reset(STANDARD_VECTOR_SIZE);
    cv.Reset(STANDARD_VECTOR_SIZE * d);
    for (duckdb::idx_t k = 0; k < take; ++k) {
      const uint64_t g = pos + k;
      for (uint32_t i = 0; i < d; ++i) {
        cd[k * d + i] = data[g * d + i];
      }
    }
    duckdb::FlatVector::SetSize(v, take);
    cw.Append(v, take);
    pos += take;
  }
  w.Commit(0);
}

std::vector<float> MakeRowMajor(uint32_t d, uint64_t n) {
  std::vector<float> data(n * d);
  for (uint64_t g = 0; g < n; ++g) {
    for (uint32_t i = 0; i < d; ++i) {
      data[g * d + i] = static_cast<float>(g * d + i);
    }
  }
  return data;
}

size_t BuiltRootSize(const CentroidsBuilder& builder) {
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
  return CentroidsTree::Deserialize(in, byte_size, false).RootSize();
}

void ExpectQueriesRouteToTrueNn(const CentroidsBuilder& builder,
                                std::span<const float> data, uint32_t d) {
  const size_t n = data.size() / d;

  std::vector<float> reordered(data.begin(), data.end());
  auto assigned =
    builder.AssignCentroids({reordered.data(), reordered.size()}, d);
  ASSERT_EQ(assigned.ids.size(), n);
  ASSERT_EQ(assigned.perm.size(), n);
  std::vector<size_t> row_cluster(n);
  for (size_t j = 0; j < n; ++j) {
    row_cluster[assigned.perm[j]] = assigned.ids[j];
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
  auto tree = CentroidsTree::Deserialize(in, byte_size, false);

  for (size_t q = 0; q < n; ++q) {
    const std::span<const float> query{data.data() + q * d, d};

    size_t nn = 0;
    float best = std::numeric_limits<float>::max();
    for (size_t r = 0; r < n; ++r) {
      float dist = 0.f;
      for (uint32_t i = 0; i < d; ++i) {
        const float diff = data[r * d + i] - query[i];
        dist += diff * diff;
      }
      if (dist < best) {
        best = dist;
        nn = r;
      }
    }

    std::vector<uint32_t> ids;
    tree.Search(query, in, /*nprobe=*/8, ids, nullptr, kDefaultMaxFanout);
    ASSERT_FALSE(ids.empty()) << "query " << q;
    const bool hit =
      std::find(ids.begin(), ids.end(),
                static_cast<uint32_t>(row_cluster[nn])) != ids.end();
    EXPECT_TRUE(hit) << "query " << q << " nn " << nn;
  }
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
  auto tree = CentroidsTree::Deserialize(in, byte_size, false);

  for (size_t i = 0; i < n; ++i) {
    const std::span<const float> q{data.data() + i * d, d};
    std::vector<uint32_t> ids;
    tree.Search(q, in, /*nprobe=*/1, ids, nullptr, kDefaultMaxFanout);
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
  auto tree = CentroidsTree::Deserialize(in, byte_size, false);

  for (size_t j = 0; j < n; ++j) {
    ASSERT_EQ(gathered[j].size(), d);
    const std::span<const float> q{data.data() + assigned.perm[j] * d, d};
    std::vector<uint32_t> ids;
    std::vector<float> cens;
    tree.Search(q, in, /*nprobe=*/1, ids, &cens, kDefaultMaxFanout);
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
  auto tree = CentroidsTree::Deserialize(in, byte_size, false);

  // Query near the early leaf (root row 0) -> its global id 0.
  {
    const std::vector<float> q{0.2f};
    std::vector<uint32_t> ids;
    tree.Search(q, in, /*nprobe=*/1, ids, nullptr, kDefaultMaxFanout);
    ASSERT_EQ(ids.size(), 1u);
    EXPECT_EQ(ids[0], 0u);
  }
  // Query near the leaf-layer cells -> ids 2 / 3, never the early leaf.
  {
    const std::vector<float> q{10.1f};
    std::vector<uint32_t> ids;
    tree.Search(q, in, /*nprobe=*/1, ids, nullptr, kDefaultMaxFanout);
    ASSERT_EQ(ids.size(), 1u);
    EXPECT_EQ(ids[0], 2u);
  }
  {
    const std::vector<float> q{11.1f};
    std::vector<uint32_t> ids;
    tree.Search(q, in, /*nprobe=*/1, ids, nullptr, kDefaultMaxFanout);
    ASSERT_EQ(ids.size(), 1u);
    EXPECT_EQ(ids[0], 3u);
  }
  // Large nprobe surfaces every leaf (early leaf included).
  {
    const std::vector<float> q{5.f};
    std::vector<uint32_t> ids;
    tree.Search(q, in, /*nprobe=*/100, ids, nullptr, kDefaultMaxFanout);
    EXPECT_EQ(ids.size(), 3u);
  }
}

// A dataset too small to split builds a single real centroids node (the mean),
// not an empty tree: NumClusters()==1, every vector routes to cluster 0 with
// the mean as its gathered centroid, and the on-disk tree searches back to id
// 0. This is the centroid residual quantizers (PQ/RaBitQ) rely on.
TEST(centroids_builder_test, single_cluster_has_mean_centroid) {
  constexpr uint32_t d = 4;
  // 10 vectors << posting_size -> root is a leaf.
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

  auto builder = CentroidsBuilder::CreateFromSample(
    data, d, VectorMetric::L2Sqr, {.posting_size = 1024});
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
  auto tree = CentroidsTree::Deserialize(in, byte_size, false);
  const std::span<const float> q{data.data(), d};
  std::vector<uint32_t> ids;
  tree.Search(q, in, /*nprobe=*/1, ids, nullptr, kDefaultMaxFanout);
  ASSERT_EQ(ids.size(), 1u);
  EXPECT_EQ(ids[0], 0u);
}

TEST(centroids_builder_test, create_small_dataset_builds_no_centroids) {
  constexpr uint32_t d = 4;
  constexpr uint64_t n = 8;
  constexpr field_id kVec = 1;
  const auto data = MakeRowMajor(d, n);

  duckdb::DuckDB db;
  MemoryDirectory dir;
  WriteVectorColumn(dir, *db.instance, kVec, d, data);

  ColReader r{dir, "seg", *db.instance};
  const auto* col = r.Column(kVec);
  ASSERT_NE(col, nullptr);

  auto builder = CentroidsBuilder::Create(*col, r.Ctx(), n, VectorMetric::L2Sqr,
                                          d, {.posting_size = 1024});
  EXPECT_EQ(builder.NumClusters(), 1u);
  EXPECT_EQ(BuiltRootSize(builder), 0u);
  ExpectQueriesRouteToTrueNn(builder, data, d);
}

TEST(centroids_builder_test,
     create_small_dataset_min_train_sample_one_centroid) {
  constexpr uint32_t d = 4;
  constexpr uint64_t n = 8;
  constexpr field_id kVec = 1;
  const auto data = MakeRowMajor(d, n);

  duckdb::DuckDB db;
  MemoryDirectory dir;
  WriteVectorColumn(dir, *db.instance, kVec, d, data);

  ColReader r{dir, "seg", *db.instance};
  const auto* col = r.Column(kVec);
  ASSERT_NE(col, nullptr);

  auto builder =
    CentroidsBuilder::Create(*col, r.Ctx(), n, VectorMetric::L2Sqr, d,
                             {.posting_size = 1024, .min_train_sample = 256});
  EXPECT_EQ(builder.NumClusters(), 1u);
  EXPECT_GE(BuiltRootSize(builder), 1u);
  ExpectQueriesRouteToTrueNn(builder, data, d);
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
  auto tree = CentroidsTree::Deserialize(in, byte_size, false);

  for (size_t i = 0; i < n; ++i) {
    const std::span<const float> q{data.data() + i * d, d};
    std::vector<uint32_t> ids;
    tree.Search(q, in, /*nprobe=*/1, ids, nullptr, kDefaultMaxFanout);
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

// Regression: a genuine >=3-layer tree must route every vector to the same leaf
// id on the deserialized tree as the build-side AssignCentroids. A small
// max_centroids square-roots the leaf count down over several layers,
// exercising the descent leaf-id numbering across >=3 layers (the 2-layer case
// is covered by multilevel_build_search_id_consistency).
TEST(centroids_builder_test, three_level_build_search_id_consistency) {
  constexpr uint32_t d = 8;
  const auto data = MakeClusters(d, /*n_clusters=*/256, /*per_cluster=*/1);
  const size_t n = data.size() / d;

  const CentroidsBuildParams params{.posting_size = 1, .max_centroids = 4};
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
  auto tree = CentroidsTree::Deserialize(in, byte_size, false);
  EXPECT_GE(tree.Levels(), 3u);
  EXPECT_LE(tree.RootSize(), 4u);

  for (size_t i = 0; i < n; ++i) {
    const std::span<const float> q{data.data() + i * d, d};
    std::vector<uint32_t> ids;
    tree.Search(q, in, /*nprobe=*/1, ids, nullptr, kDefaultMaxFanout);
    ASSERT_EQ(ids.size(), 1u) << "row " << i;
    EXPECT_EQ(ids[0], build_id[i]) << "row " << i;
  }
}

// Recall regression for the search fanout. On a genuine multi-level (>=3-layer)
// tree, Search(q, nprobe) must recover the true top-nprobe leaves ranked by
// exact centroid distance. An earlier scheme scaled the per-node fanout as
// 3*nprobe^(1/L), which collapsed as the tree deepened and greedily pruned true
// leaves. The fanout is now an explicit setting floored at nprobe, and Fanout()
// splits every interior node at least 2 ways, so no interior layer here holds
// more than n_leaves/2 == nprobe rows and the descent stays exhaustive.
TEST(centroids_builder_test, multilevel_search_recall_matches_bruteforce) {
  constexpr uint32_t d = 8;
  // posting_size=1 with a small max_centroids forces a deep tree whose every
  // internal layer stays below the query nprobe, so a correct fanout visits
  // every node and Search is exact.
  const auto data = MakeClusters(d, /*n_clusters=*/256, /*per_cluster=*/1);
  const size_t n = data.size() / d;

  const CentroidsBuildParams params{.posting_size = 1, .max_centroids = 4};
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
  auto tree = CentroidsTree::Deserialize(in, byte_size, false);

  // Enumerate every leaf id + centroid via an all-covering probe.
  std::vector<uint32_t> leaf_ids;
  std::vector<float> leaf_cens;
  tree.Search(std::span<const float>{data.data(), d}, in,
              static_cast<uint32_t>(n), leaf_ids, &leaf_cens,
              kDefaultMaxFanout);
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
    tree.Search(std::span<const float>{q, d}, in, nprobe, got, nullptr,
                kDefaultMaxFanout);
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

namespace {

// A 3-layer tree whose nearest root centroid does NOT lead to the nearest leaf:
// leaf 5.0 sits under the far root row (centroid 10), so a descent that keeps
// only the best child per node cannot reach it. Layers coarsest-first, global
// ids: root 0-1, L1 2-5, leaves 6-13.
std::vector<CentroidsNode> MakeGreedyTrapTree(uint32_t d) {
  CentroidsNode root{2, d};
  root.centroids = {0.f, 10.f};
  root.child_offsets = {0, 2, 4};
  root.size = 2;
  CentroidsNode mid{1, d};
  mid.centroids = {0.f, 1.f, 10.f, 11.f};
  mid.child_offsets = {0, 2, 4, 6, 8};
  mid.size = 4;
  CentroidsNode leaf{0, d};
  leaf.centroids = {0.f, 0.5f, 1.f, 1.5f, 5.f, 10.f, 11.f, 12.f};
  leaf.size = 8;

  std::vector<CentroidsNode> nodes;
  nodes.emplace_back(std::move(root));
  nodes.emplace_back(std::move(mid));
  nodes.emplace_back(std::move(leaf));
  return nodes;
}

}  // namespace

// max_search_fanout caps the children expanded per node, so it decides whether
// the descent can escape a wrong greedy turn. On MakeGreedyTrapTree with
// nprobe=1: fanout 1 follows the root's best child and lands on leaf 1.5
// (global id 9), while fanout 2 expands both root subtrees and finds the true
// nearest, leaf 5.0 (global id 10).
TEST(centroids_node_test, fanout_caps_children_per_node) {
  SimpleMemoryAccounter memory;
  MemoryFile file{memory};

  constexpr uint32_t d = 1;
  const auto nodes = MakeGreedyTrapTree(d);

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
  auto tree = CentroidsTree::Deserialize(in, byte_size, false);
  ASSERT_EQ(tree.Levels(), 3u);

  const std::vector<float> q{4.9f};
  {
    std::vector<uint32_t> ids;
    tree.Search(q, in, /*nprobe=*/1, ids, nullptr, /*max_search_fanout=*/1);
    ASSERT_EQ(ids.size(), 1u);
    EXPECT_EQ(ids[0], 9u);
  }
  {
    std::vector<uint32_t> ids;
    tree.Search(q, in, /*nprobe=*/1, ids, nullptr, /*max_search_fanout=*/2);
    ASSERT_EQ(ids.size(), 1u);
    EXPECT_EQ(ids[0], 10u);
  }
}

// The width is floored at the root-level-th root of nprobe, not at nprobe: it
// applies per node and compounds over _root.level expansion steps, so w^level
// is what has to reach nprobe. On a 3-layer tree that is sqrt(nprobe); on a
// 2-layer tree the single expansion step makes it nprobe itself, which is the
// shape where one expanded node yields one leaf candidate.
TEST(centroids_node_test, fanout_floored_at_root_of_nprobe) {
  SimpleMemoryAccounter memory;
  MemoryFile file{memory};

  constexpr uint32_t d = 1;
  const auto nodes = MakeGreedyTrapTree(d);

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
  auto tree = CentroidsTree::Deserialize(in, byte_size, false);
  ASSERT_EQ(tree.Levels(), 3u);

  // Two expansion steps: w^2 >= nprobe. Exact integer roots must not round up.
  EXPECT_EQ(tree.EffectiveFanout(/*nprobe=*/100, /*max_search_fanout=*/1), 10u);
  EXPECT_EQ(tree.EffectiveFanout(10000, 1), 100u);
  EXPECT_EQ(tree.EffectiveFanout(101, 1), 11u);
  EXPECT_EQ(tree.EffectiveFanout(1, 1), 1u);
  // An explicit width wider than the floor wins.
  EXPECT_EQ(tree.EffectiveFanout(100, 64), 64u);
  EXPECT_EQ(tree.EffectiveFanout(4, kDefaultMaxFanout), kDefaultMaxFanout);

  const std::vector<float> q{4.9f};
  std::vector<uint32_t> floored;
  std::vector<uint32_t> explicit_width;
  tree.Search(q, in, /*nprobe=*/4, floored, nullptr, /*max_search_fanout=*/1);
  tree.Search(q, in, /*nprobe=*/4, explicit_width, nullptr,
              /*max_search_fanout=*/2);
  EXPECT_EQ(floored, explicit_width);
  ASSERT_EQ(floored.size(), 4u);
  EXPECT_EQ(floored[0], 10u);
}

// A single expansion step means the floor is nprobe itself, so a 2-layer tree
// keeps supplying nprobe leaf candidates however small the setting is.
TEST(centroids_node_test, two_layer_tree_floors_at_nprobe) {
  SimpleMemoryAccounter memory;
  MemoryFile file{memory};

  constexpr uint32_t d = 1;
  CentroidsNode root{1, d};
  root.centroids = {0.f, 10.5f};
  root.child_offsets = {0, 1, 2};
  root.size = 2;
  CentroidsNode leaf{0, d};
  leaf.centroids = {0.f, 10.f};
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
  auto tree = CentroidsTree::Deserialize(in, byte_size, false);
  ASSERT_EQ(tree.Levels(), 2u);

  EXPECT_EQ(tree.EffectiveFanout(/*nprobe=*/1000, /*max_search_fanout=*/1),
            1000u);
  EXPECT_EQ(tree.EffectiveFanout(2, 1), 2u);
}

// Raising the fanout expands a superset of nodes at every layer, so the
// explored leaf set can only grow and recall against brute force can only rise.
// This is the property that makes an increase in the shipped default safe by
// construction. Once the fanout reaches the build-side max_centroids every
// retained node expands all of its children, so the descent is exhaustive and
// exact.
TEST(centroids_builder_test, wider_fanout_does_not_lower_recall) {
  constexpr uint32_t d = 8;
  constexpr size_t kMaxBuildFanout = 4;
  const auto data = MakeClusters(d, /*n_clusters=*/256, /*per_cluster=*/1);

  const CentroidsBuildParams params{.posting_size = 1,
                                    .max_centroids = kMaxBuildFanout};
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
  auto tree = CentroidsTree::Deserialize(in, byte_size, false);
  ASSERT_GE(tree.Levels(), 3u);

  // Enumerate every leaf id + centroid with a fanout wide enough to be
  // exhaustive.
  const auto n = static_cast<uint32_t>(data.size() / d);
  std::vector<uint32_t> leaf_ids;
  std::vector<float> leaf_cens;
  tree.Search(std::span<const float>{data.data(), d}, in, n, leaf_ids,
              &leaf_cens, /*max_search_fanout=*/n);
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

  // Queries off the training points, so a greedy descent has boundary cells to
  // get wrong -- querying the training rows would just replay the build's own
  // greedy assignment and hide the effect of the fanout.
  std::mt19937 rng{7};
  std::uniform_real_distribution<float> pos{0.f, 15000.f};
  std::vector<float> queries(200 * d);
  for (auto& v : queries) {
    v = pos(rng);
  }
  const size_t nq = queries.size() / d;

  const auto recall_at = [&](uint32_t fanout) {
    size_t hit = 0;
    for (size_t i = 0; i < nq; ++i) {
      const float* q = queries.data() + i * d;
      size_t best = 0;
      float best_dist = std::numeric_limits<float>::max();
      for (size_t l = 0; l < n_leaves; ++l) {
        const float dist = l2(q, leaf_cens.data() + l * d);
        if (dist < best_dist) {
          best_dist = dist;
          best = l;
        }
      }
      std::vector<uint32_t> got;
      tree.Search(std::span<const float>{q, d}, in, /*nprobe=*/1, got, nullptr,
                  fanout);
      if (got.size() == 1 && got[0] == leaf_ids[best]) {
        ++hit;
      }
    }
    return static_cast<double>(hit) / static_cast<double>(nq);
  };

  const double greedy = recall_at(1);
  double prev = greedy;
  for (const uint32_t fanout : {2u, 4u, 16u, 64u}) {
    const double recall = recall_at(fanout);
    EXPECT_GE(recall, prev) << "recall dropped at fanout " << fanout;
    prev = recall;
  }
  EXPECT_LT(greedy, 1.0) << "fanout=1 should miss boundary queries";
  EXPECT_DOUBLE_EQ(recall_at(static_cast<uint32_t>(kMaxBuildFanout)), 1.0);
}

// Rows drawn with variance decaying along a random orthonormal basis, so the
// energy is concentrated in a subspace that is not axis-aligned -- exactly the
// case a PCA rotation has to discover.
std::vector<float> MakeAnisotropic(uint32_t d, size_t n, uint32_t seed) {
  auto basis = MakeRotation(d, seed);
  std::mt19937 rng{seed};
  std::normal_distribution<float> nd{0.f, 1.f};
  std::vector<float> coef(d);
  std::vector<float> out(n * d, 0.f);
  for (size_t i = 0; i < n; ++i) {
    for (uint32_t j = 0; j < d; ++j) {
      coef[j] = nd(rng) / static_cast<float>(j + 1);
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

double TailEnergy(const float* data, size_t n, uint32_t d, uint32_t from) {
  double tail = 0.0;
  double total = 0.0;
  for (size_t i = 0; i < n; ++i) {
    const float* row = data + i * d;
    for (uint32_t j = 0; j < d; ++j) {
      const double sq = static_cast<double>(row[j]) * row[j];
      total += sq;
      if (j >= from) {
        tail += sq;
      }
    }
  }
  return total == 0.0 ? 0.0 : tail / total;
}

TEST(pca_rotation_test, orthonormal_preserves_distances_and_concentrates) {
  const uint32_t d = 64;
  const size_t n = 4096;
  auto data = MakeAnisotropic(d, n, 11);

  auto rotation = TrainPcaRotation(data.data(), n, d);
  ASSERT_EQ(rotation.A.size(), size_t{d} * d);

  for (uint32_t i = 0; i < d; ++i) {
    for (uint32_t j = i; j < d; ++j) {
      double dot = 0.0;
      for (uint32_t t = 0; t < d; ++t) {
        dot += static_cast<double>(rotation.A[size_t{i} * d + t]) *
               rotation.A[size_t{j} * d + t];
      }
      ASSERT_NEAR(dot, i == j ? 1.0 : 0.0, 2e-3) << "i=" << i << " j=" << j;
    }
  }

  std::vector<float> rotated(data.size());
  rotation.apply_noalloc(n, data.data(), rotated.data());

  for (size_t i = 0; i + 1 < 32; ++i) {
    const float* a = data.data() + i * d;
    const float* b = data.data() + (i + 1) * d;
    const float* ra = rotated.data() + i * d;
    const float* rb = rotated.data() + (i + 1) * d;
    double l2 = 0.0, rl2 = 0.0, ip = 0.0, rip = 0.0;
    for (uint32_t t = 0; t < d; ++t) {
      l2 += (a[t] - b[t]) * static_cast<double>(a[t] - b[t]);
      rl2 += (ra[t] - rb[t]) * static_cast<double>(ra[t] - rb[t]);
      ip += static_cast<double>(a[t]) * b[t];
      rip += static_cast<double>(ra[t]) * rb[t];
    }
    ASSERT_NEAR(rl2, l2, 1e-3 * (1.0 + l2)) << "row " << i;
    ASSERT_NEAR(rip, ip, 1e-3 * (1.0 + std::fabs(ip))) << "row " << i;
  }

  std::vector<float> one(d);
  rotation.apply_noalloc(1, data.data(), one.data());
  for (uint32_t t = 0; t < d; ++t) {
    ASSERT_NEAR(one[t], rotated[t], 1e-4) << "dim " << t;
  }

  const uint32_t from = d / 4;
  const double raw_tail = TailEnergy(data.data(), n, d, from);
  const double rot_tail = TailEnergy(rotated.data(), n, d, from);
  EXPECT_LT(rot_tail, raw_tail / 4)
    << "rotated tail " << rot_tail << " vs raw " << raw_tail;
}

TEST(pca_rotation_test, eigenvalues_are_descending) {
  const uint32_t d = 32;
  const size_t n = 2048;
  auto data = MakeAnisotropic(d, n, 5);
  auto rotation = TrainPcaRotation(data.data(), n, d);
  ASSERT_EQ(rotation.A.size(), size_t{d} * d);

  std::vector<float> rotated(data.size());
  rotation.apply_noalloc(n, data.data(), rotated.data());

  std::vector<double> energy(d, 0.0);
  for (size_t i = 0; i < n; ++i) {
    const float* row = rotated.data() + i * d;
    for (uint32_t j = 0; j < d; ++j) {
      energy[j] += static_cast<double>(row[j]) * row[j];
    }
  }
  for (uint32_t j = 0; j + 1 < d; ++j) {
    EXPECT_GE(energy[j] * 1.001 + 1e-6, energy[j + 1])
      << "dim " << j << ": " << energy[j] << " then " << energy[j + 1];
  }
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

TEST(clustering_test, superkmeans_matches_kmeans_quality_and_is_deterministic) {
  const uint32_t d = 64;
  const size_t nclusters = 64, per = 20;
  auto data = MakeClusters(d, nclusters, per);
  const size_t n = nclusters * per;
  const uint32_t k = 64;

  auto skm = TrainCentroids(VectorMetric::L2Sqr, data.data(), n, k, d,
                            /*seed=*/1234, /*niter=*/8, /*nredo=*/1,
                            ClusteringAlgo::FlatSuperKMeans);
  auto lloyd = TrainCentroids(VectorMetric::L2Sqr, data.data(), n, k, d,
                              /*seed=*/1234, /*niter=*/8, /*nredo=*/1,
                              ClusteringAlgo::Lloyd);

  ASSERT_EQ(skm.size(), static_cast<size_t>(k) * d);
  for (float x : skm) {
    ASSERT_TRUE(std::isfinite(x));
  }

  const double obj_skm = KMeansObjective(data.data(), n, skm.data(), k, d);
  const double obj_lloyd = KMeansObjective(data.data(), n, lloyd.data(), k, d);
  EXPECT_LE(obj_skm, 1.5 * obj_lloyd)
    << "skm=" << obj_skm << " lloyd=" << obj_lloyd;

  auto skm2 = TrainCentroids(VectorMetric::L2Sqr, data.data(), n, k, d, 1234, 8,
                             1, ClusteringAlgo::FlatSuperKMeans);
  ASSERT_EQ(skm.size(), skm2.size());
  EXPECT_EQ(0,
            std::memcmp(skm.data(), skm2.data(), skm.size() * sizeof(float)));
}

TEST(clustering_test, superkmeans_injected_rotation_matches_self_init) {
  const uint32_t d = 64;
  const size_t nclusters = 64, per = 20;
  auto data = MakeClusters(d, nclusters, per);
  const size_t n = nclusters * per;
  const uint32_t k = 64;
  const uint32_t seed = 1234;

  auto rotation = MakeRotation(d, seed);
  ASSERT_EQ(rotation.size(), static_cast<size_t>(d) * d);

  auto injected = TrainCentroids(
    VectorMetric::L2Sqr, data.data(), n, k, d, seed,
    /*niter=*/8, /*nredo=*/1, ClusteringAlgo::FlatSuperKMeans, rotation.data());
  auto self_init =
    TrainCentroids(VectorMetric::L2Sqr, data.data(), n, k, d, seed,
                   /*niter=*/8, /*nredo=*/1, ClusteringAlgo::FlatSuperKMeans,
                   /*rotation=*/nullptr);

  ASSERT_EQ(injected.size(), static_cast<size_t>(k) * d);
  ASSERT_EQ(injected.size(), self_init.size());
  EXPECT_EQ(0, std::memcmp(injected.data(), self_init.data(),
                           injected.size() * sizeof(float)));
}

// A d>=32 build (SuperKMeans path) with a max_centroids below the leaf count
// yields a genuine multi-level tree whose deserialized Search routes every
// training vector to the same leaf id as build-side AssignCentroids.
// Well-separated blobs make greedy descent and beam search agree.
TEST(centroids_builder_test,
     superkmeans_multilevel_build_search_id_consistency) {
  constexpr uint32_t d = 32;
  const auto data = MakeClusters(d, /*n_clusters=*/1024, /*per_cluster=*/4);
  const size_t n = data.size() / d;
  const size_t target_leaves = 1024;  // ceil(n / posting_size)

  const CentroidsBuildParams params{.posting_size = 4, .max_centroids = 32};
  auto builder =
    CentroidsBuilder::CreateFromSample(data, d, VectorMetric::L2Sqr, params);
  const size_t n_clusters = builder.NumClusters();
  // A flat build would have exactly `target_leaves` centroids; the extra rows
  // are the interior layers, so this confirms the tree really went multi-level.
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
  auto tree = CentroidsTree::Deserialize(in, byte_size, false);

  size_t matches = 0;
  for (size_t i = 0; i < n; ++i) {
    const std::span<const float> q{data.data() + i * d, d};
    std::vector<uint32_t> ids;
    tree.Search(q, in, /*nprobe=*/1, ids, nullptr, kDefaultMaxFanout);
    ASSERT_EQ(ids.size(), 1u) << "row " << i;
    ASSERT_LT(ids[0], n_clusters) << "row " << i;
    matches += (ids[0] == build_id[i]);
  }
  // Greedy build-descent and beam Search agree for all but a few near-boundary
  // points (interior cells don't align exactly with leaf cells). A leaf-
  // numbering or child-offset bug would instead break nearly every row.
  EXPECT_GE(matches, static_cast<size_t>(0.99 * static_cast<double>(n)));
}

TEST(clustering_test, superkmeans_angular_centroids_unit_norm) {
  const uint32_t d = 64;
  auto data = MakeClusters(d, 40, 20);
  const size_t n = 40 * 20;
  NormalizeRows(data.data(), n, d);
  const uint32_t k = 32;
  auto c = TrainCentroids(VectorMetric::Cosine, data.data(), n, k, d,
                          /*seed=*/7, /*niter=*/8, /*nredo=*/1,
                          ClusteringAlgo::FlatSuperKMeans);
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

// No node ever fans out past max_centroids, so the root is bounded by it: a
// large cap lets the root fan wide toward the leaf count (shallow), a small cap
// square-roots the leaf count down over extra layers (deep).
TEST(centroids_builder_test, root_fanout_capped_at_max_centroids) {
  constexpr uint32_t d = 8;
  const auto data = MakeClusters(d, /*n_clusters=*/256, /*per_cluster=*/4);

  const auto build = [&](size_t max_centroids) {
    const CentroidsBuildParams params{.posting_size = 4,
                                      .max_centroids = max_centroids};
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
    auto tree = CentroidsTree::Deserialize(in, byte_size, false);
    return std::pair{tree.Levels(), tree.RootSize()};
  };

  // 256 leaves <= cap -> root fans wide in one shot.
  const auto [levels_hi, root_hi] = build(/*max_centroids=*/4096);
  EXPECT_LE(root_hi, 4096u);
  EXPECT_GT(root_hi, 8u);
  // 256 leaves > cap -> root bounded by the cap, tree goes multi-level.
  const auto [levels_lo, root_lo] = build(/*max_centroids=*/8);
  EXPECT_LE(root_lo, 8u);
  EXPECT_GE(levels_lo, 2u);
  EXPECT_GE(levels_lo, levels_hi);
}

// Serializes a builder and reopens it the way IdxReader does: tree blob, then
// the rotation blob, whose presence is the only panorama signal.
struct PanoramaTree {
  SimpleMemoryAccounter memory;
  MemoryFile file{memory};
  std::optional<CentroidsTree> tree;
  std::optional<MemoryIndexInput> in;
};

void OpenPanoramaTree(PanoramaTree& out, const CentroidsBuilder& builder) {
  uint64_t offset = 0;
  uint64_t byte_size = 0;
  uint64_t rot_offset = 0;
  const auto rotation = builder.Rotation();
  {
    MemoryIndexOutput sink{out.file};
    const auto span = builder.Serialize(sink);
    offset = span.offset;
    byte_size = span.byte_size;
    rot_offset = sink.Position();
    if (!rotation.empty()) {
      sink.WriteData(reinterpret_cast<const byte_type*>(rotation.data()),
                     rotation.size() * sizeof(float));
    }
    sink.Flush();
  }
  out.in.emplace(out.file);
  out.in->Seek(offset);
  out.tree.emplace(
    CentroidsTree::Deserialize(*out.in, byte_size, !rotation.empty()));
  if (!rotation.empty()) {
    out.in->Seek(rot_offset);
    out.tree->ReadRotation(*out.in, rotation.size() * sizeof(float));
  }
}

CentroidsBuilder BuildRotated(std::span<const float> data, uint32_t d,
                              VectorMetric metric, size_t posting_size,
                              size_t max_centroids = 0) {
  const CentroidsBuildParams params{.posting_size = posting_size,
                                    .max_centroids = max_centroids,
                                    .rotate = true};
  return CentroidsBuilder::CreateFromSample(
    std::vector<float>{data.begin(), data.end()}, d, metric, params);
}

class panorama_centroids_test : public ::testing::TestWithParam<VectorMetric> {
};

// The exactness guard: pruning must return the identical top-nprobe list that
// the same rotated tree returns with the gate disabled. Compared against the
// unpruned *rotated* path, not the scalar one -- faiss accumulates L2 as
// |c|^2+|q|^2-2<c,q> from stored norms, which cancels differently than
// sum (q-c)^2 and would make a scalar comparison flaky.
TEST_P(panorama_centroids_test, prune_matches_unpruned) {
  constexpr uint32_t d = 128;
  const auto metric = GetParam();
  const auto data = MakeAnisotropic(d, /*n=*/4096, /*seed=*/7);

  auto builder = BuildRotated(data, d, metric, /*posting_size=*/8);
  ASSERT_NE(builder.NLevels(), 0u) << "panorama layout must be enabled";
  ASSERT_FALSE(builder.Rotation().empty());
  ASSERT_GT(builder.NumClusters(), 128u) << "need clusters > one batch";

  PanoramaTree opened;
  OpenPanoramaTree(opened, builder);
  auto& tree = *opened.tree;

  for (const uint32_t nprobe : {1u, 8u, 64u}) {
    for (size_t q = 0; q < 16; ++q) {
      const std::span<const float> query{data.data() + q * d, d};
      std::vector<uint32_t> pruned, full;
      tree.Search(query, *opened.in, nprobe, pruned, nullptr, kDefaultMaxFanout,
                  /*prune=*/true);
      tree.Search(query, *opened.in, nprobe, full, nullptr, kDefaultMaxFanout,
                  /*prune=*/false);
      EXPECT_EQ(pruned, full) << "metric " << static_cast<int>(metric)
                              << " nprobe " << nprobe << " query " << q;
    }
  }
}

INSTANTIATE_TEST_SUITE_P(panorama, panorama_centroids_test,
                         ::testing::Values(VectorMetric::L2Sqr,
                                           VectorMetric::InnerProduct,
                                           VectorMetric::Cosine));

// Node row counts that are not multiples of the 128-row batch exercise the
// short trailing batch, which the postings path never hits (it pads).
TEST(panorama_centroids_test, ragged_node_sizes_route_consistently) {
  constexpr uint32_t d = 64;
  for (const size_t posting_size : {3u, 5u, 17u}) {
    const auto data = MakeAnisotropic(d, /*n=*/2048, /*seed=*/11);
    auto builder = BuildRotated(data, d, VectorMetric::L2Sqr, posting_size);
    ASSERT_NE(builder.NLevels(), 0u) << "posting_size " << posting_size;

    std::vector<float> reordered(data.begin(), data.end());
    auto assigned =
      builder.AssignCentroids({reordered.data(), reordered.size()}, d);
    std::vector<size_t> row_cluster(data.size() / d);
    for (size_t j = 0; j < row_cluster.size(); ++j) {
      row_cluster[assigned.perm[j]] = assigned.ids[j];
    }

    PanoramaTree opened;
    OpenPanoramaTree(opened, builder);
    size_t hits = 0;
    for (size_t q = 0; q < row_cluster.size(); ++q) {
      std::vector<uint32_t> ids;
      opened.tree->Search({data.data() + q * d, d}, *opened.in, /*nprobe=*/32,
                          ids, nullptr, kDefaultMaxFanout);
      ASSERT_FALSE(ids.empty());
      hits += std::find(ids.begin(), ids.end(),
                        static_cast<uint32_t>(row_cluster[q])) != ids.end();
      if (q < 32) {
        std::vector<uint32_t> full;
        opened.tree->Search({data.data() + q * d, d}, *opened.in, /*nprobe=*/32,
                            full, nullptr, kDefaultMaxFanout,
                            /*prune=*/false);
        ASSERT_EQ(ids, full)
          << "posting_size " << posting_size << " query " << q;
      }
    }
    EXPECT_GT(hits, row_cluster.size() * 9 / 10)
      << "posting_size " << posting_size;
  }
}

// Every batch prunes against the gates its own entries compete in, so a node
// wider than one batch must prune its later batches against the per-node
// top-beam gate. Equality with the unpruned run can hold vacuously, so the
// stats have to show the interior gate firing.
void ExpectInteriorPruning(uint32_t d, size_t n, size_t posting_size,
                           VectorMetric metric, size_t min_levels,
                           size_t max_centroids) {
  auto data = MakeAnisotropic(d, n, /*seed=*/23);
  // Spherical clustering on un-normalized vectors degenerates: InnerProduct
  // otherwise builds a 10-12 level tree whose widest node is under one batch,
  // so interior pruning could never fire. Normalized input is IP's real case,
  // and it lands on the same shape Cosine gets.
  if (metric == VectorMetric::InnerProduct) {
    NormalizeRows(data.data(), n, d);
  }
  auto builder = BuildRotated(data, d, metric, posting_size, max_centroids);
  ASSERT_NE(builder.NLevels(), 0u);

  PanoramaTree opened;
  OpenPanoramaTree(opened, builder);
  auto& tree = *opened.tree;
  ASSERT_GE(tree.Levels(), min_levels);
  ASSERT_GT(tree.RootSize(), kPanoramaBatchSize)
    << "root must span batches, got " << tree.RootSize() << " clusters "
    << builder.NumClusters() << " levels " << tree.Levels();

  uint64_t slices = 0, scanned = 0;
  for (const uint32_t nprobe : {1u, 8u, 64u}) {
    for (size_t q = 0; q < 8; ++q) {
      const std::span<const float> query{data.data() + q * d, d};
      std::vector<uint32_t> pruned, full;
      CentroidsSearchStats stats;
      tree.Search(query, *opened.in, nprobe, pruned, nullptr, kDefaultMaxFanout,
                  /*prune=*/true, &stats);
      tree.Search(query, *opened.in, nprobe, full, nullptr, kDefaultMaxFanout,
                  /*prune=*/false);
      ASSERT_EQ(pruned, full) << "metric " << static_cast<int>(metric)
                              << " nprobe " << nprobe << " query " << q;
      slices += stats.node_slices;
      scanned += stats.node_slices_scanned;
    }
  }
  EXPECT_GT(slices, 0u);
  EXPECT_LT(scanned, slices) << "interior gate never pruned a slice";
}

TEST(panorama_centroids_test, interior_nodes_prune) {
  for (const auto metric : {VectorMetric::L2Sqr, VectorMetric::InnerProduct,
                            VectorMetric::Cosine}) {
    // max_centroids pins the pre-adaptive fanout cap: without it the builder
    // gives posting_size=1 one flat layer of 20000 leaves and there is no
    // interior work left to prune.
    ExpectInteriorPruning(/*d=*/64, /*n=*/8192, /*posting_size=*/8, metric,
                          /*min_levels=*/2, /*max_centroids=*/1024);
  }
}

// posting_size=2 leaves k-means groups ragged enough that a node carries both
// leaf and interior entries, which is what exercises the score-space min of the
// two bounds. The split within a node is data-dependent and not directly
// observable from outside, so only the depth is asserted.
TEST(panorama_centroids_test, mixed_nodes_prune) {
  ExpectInteriorPruning(/*d=*/64, /*n=*/40000, /*posting_size=*/2,
                        VectorMetric::L2Sqr, /*min_levels=*/3,
                        /*max_centroids=*/1024);
}

// A wide tree -- max_fanout raised until one layer holds everything -- is the
// shape panorama pays off most on, and the only one where the root itself is at
// level 0: every entry is a leaf, so the whole scan is one node's batches with
// the leaf gate tightening across them and no interior bound in play.
TEST(panorama_centroids_test, wide_single_layer_root_prunes) {
  constexpr uint32_t d = 128;
  const auto data = MakeAnisotropic(d, /*n=*/2048, /*seed=*/29);
  const CentroidsBuildParams params{
    .posting_size = 1, .max_centroids = 4096, .rotate = true};
  auto builder =
    CentroidsBuilder::CreateFromSample(data, d, VectorMetric::L2Sqr, params);
  ASSERT_NE(builder.NLevels(), 0u);

  PanoramaTree opened;
  OpenPanoramaTree(opened, builder);
  auto& tree = *opened.tree;
  ASSERT_EQ(tree.Levels(), 1u) << "max_fanout must collapse the tree";
  ASSERT_GT(tree.RootSize(), kPanoramaBatchSize)
    << "root must span batches, got " << tree.RootSize() << " clusters "
    << builder.NumClusters() << " levels " << tree.Levels();

  uint64_t slices = 0, scanned = 0;
  for (const uint32_t nprobe : {1u, 8u, 64u}) {
    for (size_t q = 0; q < 8; ++q) {
      std::vector<uint32_t> pruned, full;
      CentroidsSearchStats stats;
      tree.Search({data.data() + q * d, d}, *opened.in, nprobe, pruned, nullptr,
                  kDefaultMaxFanout, /*prune=*/true, &stats);
      tree.Search({data.data() + q * d, d}, *opened.in, nprobe, full, nullptr,
                  kDefaultMaxFanout, /*prune=*/false);
      ASSERT_EQ(pruned, full) << "nprobe " << nprobe << " query " << q;
      slices += stats.leaf_slices;
      scanned += stats.leaf_slices_scanned;
      EXPECT_EQ(stats.node_slices, 0u) << "a level-0 root has no interior work";
    }
  }
  EXPECT_LT(scanned, slices / 2) << "a wide node should prune most slices";
}

// ByRadius passes nprobe = UINT32_MAX. Neither gate can fill there, so both
// stay off: no allocation on k (a reserve(nprobe) is a ~16GB request), no
// push_heap per entry, and no pruning -- the result has to match an exhaustive
// finite probe.
TEST(panorama_centroids_test, radius_nprobe_returns_every_leaf) {
  constexpr uint32_t d = 64;
  const auto data = MakeAnisotropic(d, /*n=*/2048, /*seed=*/13);
  auto builder = BuildRotated(data, d, VectorMetric::L2Sqr, /*posting_size=*/8);
  ASSERT_NE(builder.NLevels(), 0u);

  PanoramaTree opened;
  OpenPanoramaTree(opened, builder);
  const std::span<const float> query{data.data(), d};

  // Every call needs the same search fanout, or the descent itself differs and
  // the comparison stops being about pruning: EffectiveFanout derives the beam
  // from nprobe, and these three deliberately disagree on nprobe.
  const auto all = static_cast<uint32_t>(builder.NumClusters());
  std::vector<uint32_t> radius, exhaustive, unpruned;
  opened.tree->Search(query, *opened.in, std::numeric_limits<uint32_t>::max(),
                      radius, nullptr, all);
  opened.tree->Search(query, *opened.in, all, exhaustive, nullptr, all);
  opened.tree->Search(query, *opened.in, std::numeric_limits<uint32_t>::max(),
                      unpruned, nullptr, all, /*prune=*/false);
  EXPECT_FALSE(radius.empty());
  EXPECT_EQ(radius, exhaustive);
  EXPECT_EQ(radius, unpruned);
}

// Every decline path must fall back to the byte-identical scalar layout.
TEST(panorama_centroids_test, declines_below_the_gate) {
  const auto scalar_bytes = [](uint32_t d, VectorMetric metric, size_t n,
                               bool rotate) {
    const auto data = MakeAnisotropic(d, n, /*seed=*/17);
    const CentroidsBuildParams params{.posting_size = 8, .rotate = rotate};
    auto builder = CentroidsBuilder::CreateFromSample(data, d, metric, params);
    SimpleMemoryAccounter memory;
    MemoryFile file{memory};
    uint64_t byte_size = 0;
    {
      MemoryIndexOutput out{file};
      byte_size = builder.Serialize(out).byte_size;
      out.Flush();
    }
    EXPECT_EQ(builder.Rotation().empty(), builder.NLevels() == 0)
      << "the rotation is the only panorama signal on disk";
    return std::tuple{builder.NLevels(), builder.Rotation().size(), byte_size};
  };

  // L1 is excluded, and so is d below kPanoramaMinDim.
  const auto [l1_levels, l1_rot, l1_size] =
    scalar_bytes(128, VectorMetric::L1, 2048, true);
  EXPECT_EQ(l1_levels, 0u);
  EXPECT_EQ(l1_rot, 0u);
  const auto [narrow_levels, narrow_rot, narrow_size] =
    scalar_bytes(32, VectorMetric::L2Sqr, 2048, true);
  EXPECT_EQ(narrow_levels, 0u);
  EXPECT_EQ(narrow_rot, 0u);

  // A tree with too few centroids for pruning to fire stays scalar, and its
  // bytes match a build that never asked to rotate.
  const auto [tiny_levels, tiny_rot, tiny_size] =
    scalar_bytes(128, VectorMetric::L2Sqr, 256, true);
  const auto [plain_levels, plain_rot, plain_size] =
    scalar_bytes(128, VectorMetric::L2Sqr, 256, false);
  EXPECT_EQ(tiny_levels, 0u);
  EXPECT_EQ(tiny_rot, 0u);
  EXPECT_EQ(tiny_size, plain_size);
  EXPECT_EQ(plain_levels, 0u);
}

}  // namespace
