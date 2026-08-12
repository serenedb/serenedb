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

#include "iresearch/formats/ivf/centroids.hpp"

#include <absl/algorithm/container.h>
#include <absl/random/random.h>
#include <faiss/impl/Panorama.h>

#include <algorithm>
#include <array>
#include <cmath>
#include <cstring>
#include <deque>
#include <duckdb/common/vector/array_vector.hpp>
#include <functional>
#include <limits>
#include <numeric>
#include <random>
#include <utility>

#include "iresearch/formats/ivf/clustering.hpp"
#include "iresearch/formats/ivf/ivf_reader.hpp"
#include "iresearch/formats/ivf/quantizer.hpp"
#include "iresearch/store/data_input.hpp"
#include "iresearch/store/data_output.hpp"
#include "pg/sql_exception_macro.h"

namespace irs {
namespace {

constexpr size_t kTrainSeed = 42;
constexpr uint64_t kSampleSegmentOversample = 4;
constexpr size_t kClusterIters = 15;
constexpr size_t kLeafClusterIters = 8;
constexpr size_t kClusterRedos = 1;

uint32_t CeilRoot(uint32_t target, uint32_t exp) noexcept {
  if (exp <= 1 || target <= 1) {
    return target;
  }
  auto w = static_cast<uint32_t>(std::ceill(
    std::pow(static_cast<double>(target), 1.0 / static_cast<double>(exp))));
  w = std::max<uint32_t>(w, 1);
  SDB_ASSERT(std::pow(static_cast<double>(w), static_cast<double>(exp)) >=
             target);
  return w;
}

size_t RecordFloats(size_t d, size_t n_levels) noexcept {
  return d + (n_levels != 0 ? n_levels + 1 : 0);
}

// Aligns the start of a layer body, and nothing else: a record is
// d + n_levels + 1 floats, which is odd for most d, so batches after the first
// land on 4-byte boundaries. The panorama kernels use unaligned loads.
size_t AlignBody(size_t pos) noexcept {
  return (pos + kPanoramaBodyAlign - 1) / kPanoramaBodyAlign *
         kPanoramaBodyAlign;
}

void RotateVector(const float* rotation, const float* q, float* out,
                  uint32_t d) {
  const auto width = static_cast<uint16_t>(d);
  const auto* qb = reinterpret_cast<const byte_type*>(q);
  for (uint32_t i = 0; i < d; ++i) {
    out[i] = vector::DotProductImpl<float, float>::Compute(
      reinterpret_cast<const byte_type*>(rotation + size_t{i} * d), qb, width);
  }
}

struct LayerLayout {
  size_t n_total;
  size_t body_start;
  size_t offsets_start;
  size_t record_floats;

  static LayerLayout Read(IndexInput& in, size_t d, size_t n_levels) {
    const size_t n_total = static_cast<size_t>(in.ReadI64());
    const size_t w = RecordFloats(d, n_levels);
    const auto pos = static_cast<size_t>(in.Position());
    const size_t body_start = n_levels != 0 ? AlignBody(pos) : pos;
    return {n_total, body_start, body_start + n_total * w * sizeof(float), w};
  }
  size_t CentroidPos(size_t start) const {
    return body_start + start * record_floats * sizeof(float);
  }
  size_t OffsetsPos(size_t start) const {
    return offsets_start + start * sizeof(size_t);
  }
  size_t FooterPos() const {
    return offsets_start + (n_total + 1) * sizeof(size_t);
  }
};

std::vector<float> GatherTrainingSample(const ColumnReader& vector_column,
                                        uint64_t rows, uint32_t d,
                                        ReadContext& ctx, uint64_t n_train,
                                        uint32_t seed) {
  std::vector<float> sample(static_cast<size_t>(n_train) * d);
  // TODO(codeworse): replace with PCG
  absl::InsecureBitGen rng(std::seed_seq{seed});
  uint64_t seen = 0;
  const auto reservoir_sink = [&](uint64_t /*first*/, duckdb::idx_t n,
                                  const float* data,
                                  const duckdb::ValidityMask& mask) {
    for (duckdb::idx_t k = 0; k < n; ++k) {
      if (!mask.RowIsValid(k)) {
        continue;
      }
      const float* v = data + static_cast<size_t>(k) * d;
      if (seen < n_train) {
        std::memcpy(sample.data() + seen * d, v, d * sizeof(float));
      } else {
        const uint64_t j =
          std::uniform_int_distribution<uint64_t>{0, seen}(rng);
        if (j < n_train) {
          std::memcpy(sample.data() + j * d, v, d * sizeof(float));
        }
      }
      ++seen;
    }
  };

  const auto* child = vector_column.Child();
  SDB_ASSERT(child);
  const size_t n_seg = child->DataRgCount();
  if (n_seg <= 1 || n_train * kSampleSegmentOversample >= rows) {
    StreamRowBatches(vector_column, rows, ctx, reservoir_sink);
  } else {
    std::vector<size_t> order(n_seg);
    std::iota(order.begin(), order.end(), size_t{0});
    absl::InsecureBitGen seg_rng(std::seed_seq{seed});
    std::shuffle(order.begin(), order.end(), seg_rng);

    ColumnReader::VectorScratch scratch{vector_column.Type()};
    auto scan = vector_column.InitScan(ctx);
    for (size_t i = 0; i < n_seg && seen < n_train; ++i) {
      const uint64_t w_begin = child->DataBlockFirstRow(order[i]);
      const uint64_t w_end = child->DataBlockFirstRow(order[i] + 1);
      const uint64_t r_lo = (w_begin + d - 1) / d;
      const uint64_t r_hi = w_end / d;
      if (r_lo >= r_hi) {
        continue;
      }
      if (r_lo < vector_column.GatherCursor(scan)) {
        scan = vector_column.InitScan(ctx);
      }
      if (const uint64_t cur = vector_column.GatherCursor(scan); r_lo > cur) {
        vector_column.Skip(scan, static_cast<duckdb::idx_t>(r_lo - cur));
      }
      for (uint64_t off = r_lo; off < r_hi;) {
        const auto n = static_cast<duckdb::idx_t>(
          std::min<uint64_t>(STANDARD_VECTOR_SIZE, r_hi - off));
        auto& out = scratch.Reset();
        vector_column.Scan(scan, out, n);
        reservoir_sink(off, n,
                       duckdb::FlatVector::GetData<float>(
                         duckdb::ArrayVector::GetChildMutable(out)),
                       duckdb::FlatVector::Validity(out));
        off += n;
      }
    }
  }
  sample.resize(std::min<uint64_t>(seen, n_train) * d);
  return sample;
}

struct BuildSettings {
  size_t posting_size;
  size_t max_centroids;
  VectorMetric metric;
  size_t niter;

  bool IsLeaf(size_t sample_size) const noexcept {
    return sample_size <= posting_size;
  }

  size_t Fanout(size_t sample_size) const noexcept {
    size_t f = (sample_size + posting_size - 1) / posting_size;
    while (f > max_centroids) {
      f = static_cast<size_t>(std::ceil(std::sqrt(static_cast<double>(f))));
    }
    return std::max<size_t>(2, f);
  }
};

size_t RemoveEmptyCentroids(std::vector<float>& centroids,
                            std::span<size_t> ids, size_t d) {
  size_t kept = 0;
  size_t prev = std::numeric_limits<size_t>::max();
  for (auto& id : ids) {
    SDB_ASSERT(prev == std::numeric_limits<size_t>::max() || id >= prev);
    SDB_ASSERT((id + 1) * d <= centroids.size());
    if (id != prev) {
      prev = id;
      if (kept != id) {
        std::copy_n(centroids.begin() + id * d, d,
                    centroids.begin() + kept * d);
      }
      ++kept;
    }
    id = kept - 1;
  }
  centroids.resize(kept * d);
  return kept;
}

auto BuildAndSplit(std::span<float> data, size_t d, std::span<size_t> ids,
                   size_t n_clusters, VectorMetric metric, size_t niter,
                   const float* rotation) {
  auto centroids = TrainCentroids(
    metric, data.data(), data.size() / d, static_cast<uint32_t>(n_clusters),
    static_cast<uint32_t>(d), kTrainSeed, static_cast<uint32_t>(niter),
    static_cast<uint32_t>(kClusterRedos), ClusteringAlgo::Auto, rotation);
  AssignNearestGrouped(metric, centroids, d, data, ids);
  RemoveEmptyCentroids(centroids, ids, d);
  return centroids;
}

template<typename Fn>
void ForEachGroup(std::span<const size_t> ids, size_t n_groups, Fn&& fn) {
  for (size_t i = 0, current = 0; i < n_groups; ++i) {
    const size_t start = current;
    while (current < ids.size() && ids[current] == i) {
      ++current;
    }
    fn(i, start, current - start);
  }
}

void Build(std::vector<CentroidsBuilder::Node>& nodes, std::span<float> data,
           size_t d, std::span<size_t> ids, const BuildSettings& settings) {
  const std::vector<float> rotation =
    MakeRotation(static_cast<uint32_t>(d), kTrainSeed);
  const float* rot = rotation.data();
  struct CentroidsEntry {
    size_t parent;
    std::span<float> sample;
    std::span<size_t> ids;
  };
  std::deque<CentroidsEntry> centroids_build = {CentroidsEntry{
    .parent = std::numeric_limits<size_t>::max(),
    .sample = data,
    .ids = ids,
  }};

  while (!centroids_build.empty()) {
    const auto entry = centroids_build.front();
    size_t sample_size = entry.sample.size() / d;
    centroids_build.pop_front();
    if (settings.IsLeaf(sample_size)) {
      if (entry.parent < nodes.size()) {
        nodes[entry.parent].leafs++;
        nodes[entry.parent].children.emplace_back(0);
      } else if (sample_size > 0) {
        auto centroids = TrainCentroids(
          settings.metric, entry.sample.data(), sample_size,
          /*k=*/1, static_cast<uint32_t>(d), kTrainSeed,
          static_cast<uint32_t>(kLeafClusterIters),
          static_cast<uint32_t>(kClusterRedos), ClusteringAlgo::Auto, rot);
        nodes.emplace_back(CentroidsBuilder::Node{
          .centroids = std::move(centroids), .children = {0}, .leafs = 1});
      }
      // centroid from parent will route to this posting
      continue;
    }
    const size_t n_clusters = settings.Fanout(sample_size);
    auto centroids = BuildAndSplit(entry.sample, d, entry.ids, n_clusters,
                                   settings.metric, settings.niter, rot);
    size_t n_built = centroids.size() / d;

    if (n_built == 1) {
      // Only one centroid
      centroids.resize(d * n_clusters);
      const auto c = std::span{centroids}.first(d);
      for (size_t c_id = 1; c_id < n_clusters; ++c_id) {
        absl::c_copy(c, centroids.begin() + c_id * d);
      }
      auto chunk = (sample_size + n_clusters - 1) / n_clusters;
      for (size_t i = 0; i < sample_size; ++i) {
        entry.ids[i] = i / chunk;
      }
      n_built = n_clusters;
    }

#ifdef SDB_DEV
    ForEachGroup(entry.ids, n_built, [&](size_t g, size_t, size_t count) {
      SDB_ASSERT(count < sample_size);
    });
#endif

    if (entry.parent < nodes.size()) {
      nodes[entry.parent].children.emplace_back(nodes.size());
    }
    nodes.emplace_back(
      CentroidsBuilder::Node{.centroids = std::move(centroids)});
    ForEachGroup(entry.ids, n_built, [&](size_t, size_t start, size_t count) {
      centroids_build.emplace_back(CentroidsEntry{
        .parent = nodes.size() - 1,
        .sample = entry.sample.subspan(start * d, count * d),
        .ids = entry.ids.subspan(start, count),
      });
    });
  }
  for (size_t i = nodes.size(); i--;) {
    for (auto&& child : nodes[i].children) {
      if (child == 0) {
        continue;
      }
      nodes[i].leafs += nodes[child].leafs;
    }
  }
}

}  // namespace

std::vector<CentroidsNode> CentroidsNode::Deserialize(
  IndexInput& in, size_t level, size_t d, std::span<const size_t> starts,
  std::span<const size_t> sizes, size_t n_levels) {
  SDB_ASSERT(starts.size() == sizes.size());
  const auto layout = LayerLayout::Read(in, d, n_levels);
  std::vector<CentroidsNode> nodes;
  nodes.reserve(starts.size());
  for (auto&& [start, size] : std::views::zip(starts, sizes)) {
    CentroidsNode node{level, d};
    node.size = size;

    in.Seek(layout.CentroidPos(start));
    node.centroids.resize(node.size * layout.record_floats);
    if (node.size != 0) {
      in.ReadData(reinterpret_cast<byte_type*>(node.centroids.data()),
                  node.size * layout.record_floats * sizeof(float));
    }

    if (level > 0) {
      node.child_offsets.resize(node.size + 1);
      in.Seek(layout.OffsetsPos(start));
      in.ReadData(reinterpret_cast<byte_type*>(node.child_offsets.data()),
                  (node.size + 1) * sizeof(size_t));
    }
    nodes.emplace_back(std::move(node));
  }
  if (level > 0) {
    in.Seek(layout.FooterPos());
  }
  return nodes;
}

std::vector<CentroidsNodeView> CentroidsNode::ReadLayer(
  IndexInput& in, size_t level, size_t d, std::span<const size_t> starts,
  std::span<const size_t> sizes, LayerBuffers& bufs, size_t& n_total,
  size_t n_levels) {
  SDB_ASSERT(starts.size() == sizes.size());
  const auto layout = LayerLayout::Read(in, d, n_levels);
  n_total = layout.n_total;
  std::vector<CentroidsNodeView> nodes;
  nodes.reserve(starts.size());
  bufs.centroids.reserve(bufs.centroids.size() + starts.size());
  bufs.child_offsets.reserve(bufs.child_offsets.size() + starts.size());
  for (auto&& [start, size] : std::views::zip(starts, sizes)) {
    CentroidsNodeView node;
    node.base = start;
    node.size = size;
    if (size == 0) {
      nodes.emplace_back(node);
      continue;
    }
    const uint64_t offset = layout.CentroidPos(start);
    const size_t floats = size * layout.record_floats;
    const size_t centroids_bytes = floats * sizeof(float);
    if (const byte_type* p = in.ReadStable(offset, centroids_bytes)) {
      node.centroids =
        std::span<const float>{reinterpret_cast<const float*>(p), floats};
    } else {
      auto& buf = bufs.centroids.emplace_back(floats);
      in.ReadData(offset, reinterpret_cast<byte_type*>(buf.data()),
                  centroids_bytes);
      node.centroids = std::span<const float>{buf.data(), floats};
    }
    if (level > 0) {
      auto& off = bufs.child_offsets.emplace_back(size + 1);
      in.ReadData(layout.OffsetsPos(start),
                  reinterpret_cast<byte_type*>(off.data()),
                  (size + 1) * sizeof(size_t));
      node.child_offsets = std::span<const size_t>{off.data(), size + 1};
    }
    nodes.emplace_back(node);
  }
  if (level > 0) {
    in.Seek(layout.FooterPos());
  }
  return nodes;
}

IVFHeader IVFHeader::Deserialize(IndexInput& in) {
  IVFHeader head;
  head.metric = static_cast<VectorMetric>(in.ReadByte());
  head.d = static_cast<uint32_t>(in.ReadI32());
  return head;
}

void IVFHeader::Serialize(IndexOutput& out) const {
  out.WriteByte(static_cast<byte_type>(metric));
  out.WriteU32(d);
}

CentroidsTree CentroidsTree::Deserialize(IndexInput& in, uint64_t byte_size,
                                         bool panorama) {
  auto head = IVFHeader::Deserialize(in);
  const uint32_t n_levels = panorama ? PanoramaLevels(head.d) : 0;
  const size_t level = static_cast<size_t>(in.ReadI64());
  const size_t n_total_pos = static_cast<size_t>(in.Position());
  const size_t n_total = static_cast<size_t>(in.ReadI64());
  in.Seek(n_total_pos);
  auto nodes =
    CentroidsNode::Deserialize(in, level, head.d, {0}, {n_total}, n_levels);
  auto node = std::move(nodes.front());
  const size_t next_level_offset = static_cast<size_t>(in.Position());
  CentroidsTree tree{std::move(head), std::move(node), next_level_offset};
  tree._n_levels = n_levels;
  return tree;
}

uint32_t CentroidsTree::EffectiveFanout(
  uint32_t nprobe, uint32_t max_search_fanout) const noexcept {
  return std::max(max_search_fanout,
                  CeilRoot(nprobe, static_cast<uint32_t>(_root.level)));
}

void CentroidsTree::ReadRotation(IndexInput& in, uint64_t byte_size) {
  SDB_ENSURE(byte_size == size_t{_head.d} * _head.d * sizeof(float));
  _rotation.resize(size_t{_head.d} * _head.d);
  in.ReadData(reinterpret_cast<byte_type*>(_rotation.data()), byte_size);
}

namespace {

struct Scored {
  float dist;
  size_t start;
  size_t count;
};

// Selection needs a total order, not just a score order. Duplicate centroids
// are real -- Build collapses a node whose points all land in one cluster by
// copying the winner into every slot -- so equal scores are common, and a
// pruned run commits its survivors in a different physical order than an
// unpruned one. Ordering by score alone would let the two disagree on which of
// the tied ids wins; every entry that ties with the cutoff survives pruning, so
// a tie-break on identity makes the result independent of how much was pruned.
constexpr auto kBetterScored = [](const Scored& l, const Scored& r) {
  return l.dist != r.dist ? l.dist > r.dist : l.start < r.start;
};

constexpr auto kBetterCandidate = [](const CentroidsNode::Candidate& l,
                                     const CentroidsNode::Candidate& r) {
  return l.dist != r.dist ? l.dist > r.dist : l.id < r.id;
};

constexpr float kNoBound = -std::numeric_limits<float>::infinity();

// Top-k bound over the scores of a result set. Every metric is scored "higher
// is better", so the min-heap front is the weakest score kept -- the value a
// further candidate has to beat to change the outcome.
struct Gate {
  std::vector<float> heap;
  size_t k = 0;
  bool on = false;

  void Push(float score) {
    if (!on) {
      return;
    }
    if (heap.size() < k) {
      heap.push_back(score);
      std::ranges::push_heap(heap, std::greater{});
    } else if (!heap.empty() && score > heap.front()) {
      std::ranges::pop_heap(heap, std::greater{});
      heap.back() = score;
      std::ranges::push_heap(heap, std::greater{});
    }
  }

  float Bound() const noexcept {
    return on && k != 0 && heap.size() == k ? heap.front() : kNoBound;
  }
  void Reset() noexcept { heap.clear(); }
};

// Per-query state for the centroid descent. The faiss scratch is allocated once
// and the leaf gate persists across nodes and layers -- carrying it is what
// makes progressive pruning pay for the level-major layout.
struct SearchCtx {
  std::span<const float> query;
  const float* query_cums = nullptr;
  size_t n_levels = 0;
  size_t d = 0;
  uint32_t beam = 0;
  // Only reachable from ScoreNodeScalar: a rotated tree stores centroids in the
  // PCA basis and cannot hand one back in the original basis.
  bool want_centroids = false;
  Gate leaf_gate;
  Gate node_gate;
  std::vector<CentroidsNode::Candidate> leaves;
  // Candidate::centroid and CentroidsNodeView::centroids alias these whenever a
  // layer cannot be read in place, so they outlive the whole descent. Growing
  // the outer vector moves the inner ones, which preserves their buffers.
  LayerBuffers bufs;
  std::vector<uint32_t> active;
  std::vector<uint8_t> byteset;
  std::vector<float> exact;
  std::vector<float> dots;
  faiss::PanoramaStats leaf_stats;
  faiss::PanoramaStats node_stats;
};

// Thresholds cannot be combined after conversion: PanoramaThreshold flips
// direction per metric, so a min over converted values is unsound for L2.
template<VectorMetric Metric>
float BatchThreshold(const SearchCtx& ctx, bool has_leaf, bool has_child) {
  float bound = std::numeric_limits<float>::infinity();
  if (has_leaf) {
    bound = std::min(bound, ctx.leaf_gate.Bound());
  }
  if (has_child) {
    bound = std::min(bound, ctx.node_gate.Bound());
  }
  return std::isfinite(bound) ? PanoramaThreshold(bound, Metric)
                              : PanoramaNoPrune(Metric);
}

// Serves every tree the writer left unrotated: needs_centroid quantizers, L1,
// d below kPanoramaMinDim, and trees too small to batch.
template<VectorMetric Metric>
void ScoreNodeScalar(SearchCtx& ctx, const CentroidsNodeView& node,
                     size_t level, size_t layer_base,
                     std::vector<Scored>& scored) {
  const auto d = static_cast<uint16_t>(ctx.d);
  for (size_t i = 0; i < node.size; ++i) {
    const auto centroid = node.centroids.subspan(i * ctx.d, ctx.d);
    const float dist =
      ComputeDistance<Metric>(ctx.query.data(), centroid.data(), d);
    if (level == 0 || node.child_offsets[i + 1] == node.child_offsets[i]) {
      auto& cand = ctx.leaves.emplace_back(dist, layer_base + node.base + i);
      if (ctx.want_centroids) {
        cand.centroid = centroid;
      }
    } else {
      scored.push_back({dist, node.child_offsets[i],
                        node.child_offsets[i + 1] - node.child_offsets[i]});
    }
  }
}

// A batch prunes against the gates its own entries compete in: leaves against
// the top-nprobe leaf gate, interior entries against this node's top-beam gate.
// Both are exact -- a full gate holds k scores better than the pruned entry's
// best case, and all k are already committed (to ctx.leaves, to scored), so the
// entry cannot enter either top-k under any tie-break.
template<VectorMetric Metric>
void ScoreNodePruned(SearchCtx& ctx, const CentroidsNodeView& node,
                     size_t level, size_t layer_base,
                     std::vector<Scored>& scored) {
  static constexpr auto kMetric = Metric == VectorMetric::L2Sqr
                                    ? faiss::METRIC_L2
                                    : faiss::METRIC_INNER_PRODUCT;
  using C =
    std::conditional_t<kMetric == faiss::METRIC_L2, faiss::CMax<float, int64_t>,
                       faiss::CMin<float, int64_t>>;
  const size_t w = RecordFloats(ctx.d, ctx.n_levels);
  auto& stats = level == 0 ? ctx.leaf_stats : ctx.node_stats;
  for (size_t off = 0; off < node.size;) {
    const size_t len = std::min<size_t>(kPanoramaBatchSize, node.size - off);
    bool has_leaf = level == 0;
    bool has_child = false;
    for (size_t i = off; level != 0 && i < off + len; ++i) {
      const bool leaf = node.child_offsets[i + 1] == node.child_offsets[i];
      has_leaf |= leaf;
      has_child |= !leaf;
    }
    const faiss::Panorama pano{ctx.d * sizeof(float), ctx.n_levels, len};
    const float* cums = node.centroids.data() + off * w;
    const auto* codes =
      reinterpret_cast<const uint8_t*>(cums + len * (ctx.n_levels + 1));
    const size_t alive = pano.template progressive_filter_batch<C, kMetric>(
      codes, cums, ctx.query.data(), ctx.query_cums, 0, len,
      /*sel=*/nullptr, /*ids=*/nullptr, /*use_sel=*/false, ctx.active,
      ctx.byteset, ctx.exact, ctx.dots,
      BatchThreshold<Metric>(ctx, has_leaf, has_child), stats);
    for (size_t a = 0; a < alive; ++a) {
      const size_t idx = ctx.active[a];
      SDB_ASSERT(idx < len);
      const size_t i = off + idx;
      const float dist =
        kMetric == faiss::METRIC_L2 ? -ctx.exact[idx] : ctx.exact[idx];
      if (level == 0 || node.child_offsets[i + 1] == node.child_offsets[i]) {
        ctx.leaves.emplace_back(dist, layer_base + node.base + i);
        ctx.leaf_gate.Push(dist);
      } else {
        scored.push_back({dist, node.child_offsets[i],
                          node.child_offsets[i + 1] - node.child_offsets[i]});
        ctx.node_gate.Push(dist);
      }
    }
    off += len;
  }
}

template<VectorMetric Metric>
void SearchLayer(SearchCtx& ctx, IndexInput& in, size_t level,
                 std::span<const CentroidsNodeView> nodes, size_t layer_base,
                 size_t layer_total) {
  SDB_ASSERT(!nodes.empty());
  std::vector<Scored> scored, kept;
  for (const auto& node : nodes) {
    scored.clear();
    // Each node keeps its own top-beam, so the interior bound cannot carry over
    // from the previous one. A node with fewer than beam interior entries never
    // fills its gate and is therefore never pruned, which keeps nth_element
    // seeing every entry it would have seen unpruned.
    ctx.node_gate.Reset();
    if (ctx.n_levels == 0) {
      ScoreNodeScalar<Metric>(ctx, node, level, layer_base, scored);
    } else {
      ScoreNodePruned<Metric>(ctx, node, level, layer_base, scored);
    }
    const auto k = std::min<size_t>(ctx.beam, scored.size());
    const auto mid = scored.begin() + k;
    std::ranges::nth_element(scored, mid, kBetterScored);
    kept.insert(kept.end(), scored.begin(), mid);
  }
  if (level == 0 || kept.empty()) {
    return;
  }
  // Visiting the most promising child first tightens the gate sooner, which is
  // the whole reason the next layer prunes at all.
  std::ranges::sort(kept, kBetterScored);
  std::vector<size_t> starts, sizes;
  starts.reserve(kept.size());
  sizes.reserve(kept.size());
  for (const auto& s : kept) {
    starts.emplace_back(s.start);
    sizes.emplace_back(s.count);
  }
  size_t n_total = 0;
  auto next = CentroidsNode::ReadLayer(in, level - 1, ctx.d, starts, sizes,
                                       ctx.bufs, n_total, ctx.n_levels);
  SearchLayer<Metric>(ctx, in, level - 1, next, layer_base + layer_total,
                      n_total);
}

}  // namespace

void CentroidsTree::Search(std::span<const float> query, IndexInput& in,
                           uint32_t nprobe, std::vector<uint32_t>& out_ids,
                           std::vector<float>* out_centroids,
                           uint32_t max_search_fanout, bool prune,
                           CentroidsSearchStats* out_stats) const {
  if (_root.size == 0) {
    out_ids.push_back(0);
    return;
  }
  SDB_ASSERT(max_search_fanout > 0);
  const auto fanout = EffectiveFanout(nprobe, max_search_fanout);
  if (_root.level > 0) {
    in.Seek(_next_level_offset);
  }
  const CentroidsNodeView root_view{
    .centroids = std::span<const float>{_root.centroids},
    .child_offsets = std::span<const size_t>{_root.child_offsets},
    .base = 0,
    .size = _root.size};
  // A rotated tree stores centroids in the PCA basis, so it cannot hand one
  // back in the original basis. The writer guarantees this by only rotating
  // when the quantizer does not need centroids.
  SDB_ASSERT(out_centroids == nullptr || _n_levels == 0);

  SearchCtx ctx;
  ctx.query = query;
  ctx.n_levels = _n_levels;
  ctx.d = _head.d;
  ctx.beam = fanout;
  ctx.want_centroids = out_centroids != nullptr;
  // ByRadius asks for every leaf, so neither gate can ever fill: keeping them
  // off skips a push_heap per entry on the one path that cannot prune.
  const bool gate_on = prune && nprobe != std::numeric_limits<uint32_t>::max();
  ctx.leaf_gate = {.k = nprobe, .on = gate_on};
  ctx.node_gate = {.k = fanout, .on = gate_on};

  std::vector<float> rotated, query_cums;
  if (_n_levels != 0) {
    SDB_ASSERT(_rotation.size() == size_t{_head.d} * _head.d);
    rotated.resize(_head.d);
    RotateVector(_rotation.data(), query.data(), rotated.data(), _head.d);
    ctx.query = rotated;
    query_cums.resize(_n_levels + 1);
    const faiss::Panorama pano{size_t{_head.d} * sizeof(float), _n_levels,
                               kPanoramaBatchSize};
    pano.compute_query_cum_sums(rotated.data(), query_cums.data());
    ctx.query_cums = query_cums.data();
    ctx.active.resize(kPanoramaBatchSize);
    ctx.byteset.resize(kPanoramaBatchSize);
    ctx.exact.resize(kPanoramaBatchSize);
    ctx.dots.resize(kPanoramaBatchSize);
  }

  const auto metric = EffectiveQuantMetric(_head.metric);
  irs::ResolveEnum<VectorMetric>(metric, [&]<VectorMetric Metric>() {
    SearchLayer<Metric>(ctx, in, _root.level, std::span{&root_view, 1}, 0,
                        _root.size);
    auto& leaves = ctx.leaves;
    const auto k = std::min<size_t>(nprobe, leaves.size());
    const auto mid = leaves.begin() + k;
    std::ranges::nth_element(leaves, mid, kBetterCandidate);
    std::ranges::sort(leaves.begin(), mid, kBetterCandidate);
    out_ids.reserve(out_ids.size() + k);
    if (out_centroids) {
      out_centroids->reserve(out_centroids->size() + k * _head.d);
    }
    for (auto it = leaves.begin(); it != mid; ++it) {
      out_ids.emplace_back(static_cast<uint32_t>(it->id));
      if (out_centroids) {
        out_centroids->append_range(it->centroid);
      }
    }
  });

  if (out_stats) {
    *out_stats = {.leaf_slices = ctx.leaf_stats.total_dims,
                  .leaf_slices_scanned = ctx.leaf_stats.total_dims_scanned,
                  .node_slices = ctx.node_stats.total_dims,
                  .node_slices_scanned = ctx.node_stats.total_dims_scanned};
  }
}

void CentroidsBuilder::BuildTree(std::vector<float> sample, size_t leaf_size,
                                 size_t max_centroids, bool rotate) {
  BuildSettings settings{
    .posting_size = std::max<size_t>(1, leaf_size),
    .max_centroids = max_centroids,
    .metric = _metric,
    .niter = kClusterIters,
  };
  const size_t n = sample.size() / _d;
  std::vector<size_t> ids(n);
  if (_metric == VectorMetric::Cosine) {
    NormalizeRows(sample.data(), n, _d);
  }
  Build(_nodes, sample, _d, ids, settings);
  _row_bases.resize(_nodes.size());
  for (size_t j = 0; j < _nodes.size(); ++j) {
    _row_bases[j] = _n_rows;
    _n_rows += _nodes[j].Rows(_d);
  }
  // Format-affecting and writer-only: once a rotation exists every layer body
  // is level-major, and the reader learns that from the rotation alone.
  // Scan-time constants carry no such commitment and stay freely tunable.
  if (rotate && PanoramaApplies(_metric, _d) && n >= _d &&
      _n_rows > kPanoramaBatchSize) {
    const size_t train =
      std::min(n, std::max<size_t>(kPcaTrainRows, size_t{8} * _d));
    _rotation = std::make_shared<const faiss::PCAMatrix>(
      TrainPcaRotation(sample.data(), train, _d));
    _n_levels = PanoramaLevels(_d);
  }
}

CentroidsBuilder CentroidsBuilder::BuildFromSample(
  std::vector<float> sample, uint32_t d, VectorMetric metric, size_t leaf_size,
  size_t max_centroids, bool rotate) {
  CentroidsBuilder builder;
  builder._metric = metric;
  builder._d = d;
  if (max_centroids == 0) {
    const size_t rows = d == 0 ? 0 : sample.size() / d;
    max_centroids = std::max<size_t>(
      1, (rows + leaf_size - 1) / std::max<size_t>(1, leaf_size));
  }
  builder.BuildTree(std::move(sample), leaf_size, max_centroids, rotate);
  return builder;
}

std::span<const float> CentroidsBuilder::Rotation() const noexcept {
  if (!_rotation) {
    return {};
  }
  return _rotation->A;
}

CentroidsBuilder CentroidsBuilder::Create(const ColumnReader& vector_column,
                                          ReadContext& ctx, size_t rows,
                                          VectorMetric metric, uint32_t d,
                                          const CentroidsBuildParams& params) {
  const size_t t = params.posting_size;
  SDB_ASSERT(t > 0);

  size_t sample_size = static_cast<size_t>(params.sample_factor * rows);
  sample_size = std::max<size_t>(sample_size, params.min_train_sample);
  sample_size = std::min<size_t>(sample_size, rows);

  const size_t tau =
    rows == 0
      ? t
      : std::max<size_t>(
          1, static_cast<size_t>(static_cast<double>(sample_size) / rows * t));
  auto sample =
    GatherTrainingSample(vector_column, rows, d, ctx, sample_size, kTrainSeed);
  return BuildFromSample(std::move(sample), d, metric, tau,
                         params.max_centroids, params.rotate);
}

CentroidsBuilder CentroidsBuilder::CreateFromSample(
  std::vector<float> sample, uint32_t d, VectorMetric metric,
  const CentroidsBuildParams& params) {
  SDB_ASSERT(params.posting_size > 0);
  return BuildFromSample(std::move(sample), d, metric, params.posting_size,
                         params.max_centroids, params.rotate);
}

CentroidsSpan CentroidsBuilder::Serialize(IndexOutput& out) const {
  const IVFHeader head{.metric = _metric, .d = _d};
  const size_t offset = static_cast<size_t>(out.Position());
  const auto span = [&] {
    return CentroidsSpan{
      .offset = offset,
      .byte_size = static_cast<size_t>(out.Position()) - offset};
  };
  head.Serialize(out);
  SDB_ASSERT(!_nodes.empty() || !_rotation);
  if (_nodes.empty()) {
    out.WriteU64(0);
    out.WriteU64(0);
    return span();
  }

  std::vector<size_t> depth(_nodes.size(), 0);
  for (size_t j = 0; j < _nodes.size(); ++j) {
    SDB_ASSERT(_nodes[j].children.size() == _nodes[j].Rows(_d));
    for (const size_t child : _nodes[j].children) {
      if (child != 0) {
        SDB_ASSERT(child > j);
        depth[child] = depth[j] + 1;
      }
    }
  }

  struct Layer {
    size_t first;
    size_t last;
    size_t rows;
  };
  std::vector<Layer> layers;
  for (size_t first = 0; first < _nodes.size();) {
    Layer layer{.first = first, .last = first, .rows = 0};
    while (layer.last < _nodes.size() && depth[layer.last] == depth[first]) {
      layer.rows += _nodes[layer.last].Rows(_d);
      ++layer.last;
    }
    first = layer.last;
    layers.emplace_back(layer);
  }

  out.WriteU64(layers.size() - 1);
  std::vector<size_t> offsets;
  std::vector<float> stage, cums, codes;
  const auto write_node = [&](const Node& node) {
    const size_t rows = node.Rows(_d);
    for (size_t off = 0; off < rows;) {
      const size_t len = std::min<size_t>(kPanoramaBatchSize, rows - off);
      faiss::Panorama pano{size_t{_d} * sizeof(float), _n_levels, len};
      SDB_ASSERT(pano.n_levels == _n_levels);
      stage.resize(len * size_t{_d});
      codes.resize(len * size_t{_d});
      cums.assign(len * (size_t{_n_levels} + 1), 0.f);
      _rotation->apply_noalloc(static_cast<faiss::idx_t>(len),
                               node.centroids.data() + off * _d, stage.data());
      pano.compute_cumulative_sums(cums.data(), 0, len, stage.data());
      pano.copy_codes_to_level_layout(
        reinterpret_cast<uint8_t*>(codes.data()), 0, len,
        reinterpret_cast<const uint8_t*>(stage.data()));
      out.WriteData(reinterpret_cast<const byte_type*>(cums.data()),
                    cums.size() * sizeof(float));
      out.WriteData(reinterpret_cast<const byte_type*>(codes.data()),
                    codes.size() * sizeof(float));
      off += len;
    }
  };
  for (size_t p = 0; p < layers.size(); ++p) {
    const auto& layer = layers[p];
    out.WriteU64(layer.rows);
    if (_n_levels != 0) {
      for (auto pos = static_cast<size_t>(out.Position()), end = AlignBody(pos);
           pos < end; ++pos) {
        out.WriteByte(0);
      }
      for (size_t j = layer.first; j < layer.last; ++j) {
        write_node(_nodes[j]);
      }
    } else {
      for (size_t j = layer.first; j < layer.last; ++j) {
        out.WriteData(
          reinterpret_cast<const byte_type*>(_nodes[j].centroids.data()),
          _nodes[j].centroids.size() * sizeof(float));
      }
    }
    if (p + 1 == layers.size()) {
      break;
    }
    offsets.clear();
    offsets.reserve(layer.rows + 1);
    offsets.push_back(0);
    size_t running = 0;
    for (size_t j = layer.first; j < layer.last; ++j) {
      for (const size_t child : _nodes[j].children) {
        running += child == 0 ? 0 : _nodes[child].Rows(_d);
        offsets.push_back(running);
      }
    }
    SDB_ASSERT(offsets.size() == layer.rows + 1);
    SDB_ASSERT(running == layers[p + 1].rows);
    out.WriteData(reinterpret_cast<const byte_type*>(offsets.data()),
                  offsets.size() * sizeof(size_t));
  }
  return span();
}

void CentroidsBuilder::AssignCentroidsImpl(
  size_t node_index, std::span<float> data, size_t d, std::span<size_t> ids,
  std::span<size_t> perm,
  std::span<std::span<const float>> centroids_out) const {
  const auto& node = _nodes[node_index];
  AssignNearestGrouped(_metric, node.centroids, d, data, ids, perm,
                       centroids_out);
  ForEachGroup(ids, node.Rows(d), [&](size_t i, size_t start, size_t count) {
    if (node.children[i] == 0) {
      absl::c_fill(ids.subspan(start, count), _row_bases[node_index] + i);
      return;
    }
    AssignCentroidsImpl(node.children[i], data.subspan(start * d, count * d), d,
                        ids.subspan(start, count),
                        perm.empty() ? perm : perm.subspan(start, count),
                        centroids_out.empty()
                          ? centroids_out
                          : centroids_out.subspan(start, count));
  });
}

AssignedCentroids CentroidsBuilder::AssignCentroids(
  std::span<float> data, size_t d,
  std::span<std::span<const float>> centroids_out) const {
  const size_t n = data.size() / d;
  AssignedCentroids result;
  result.ids.resize(n);
  result.perm.resize(n);
  absl::c_iota(result.perm, size_t{0});
  if (_nodes.empty()) {
    return result;
  }
  AssignCentroidsImpl(0, data, d, result.ids, result.perm, centroids_out);
  return result;
}

}  // namespace irs
