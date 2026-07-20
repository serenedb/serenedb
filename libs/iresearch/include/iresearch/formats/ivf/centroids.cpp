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

#include <algorithm>
#include <array>
#include <cmath>
#include <cstring>
#include <deque>
#include <functional>
#include <numeric>
#include <random>
#include <utility>

#include "iresearch/formats/ivf/clustering.hpp"
#include "iresearch/formats/ivf/ivf_reader.hpp"
#include "iresearch/store/data_input.hpp"
#include "iresearch/store/data_output.hpp"
#include "pg/sql_exception_macro.h"

namespace irs {
namespace {

constexpr double kBeamOverprobe = 3.0;
constexpr size_t kTrainSeed = 42;
constexpr uint64_t kSampleSegmentOversample = 4;
constexpr size_t kPostingSizeDefault = 1024;
constexpr size_t kMinCentroids = 64;
constexpr size_t kMaxCentroids = 4096;
constexpr size_t kTrainPointsPerLeaf = 64;
constexpr size_t kMaxTrainSample = 4ull * 1024 * 1024;
constexpr size_t kClusterIters = 15;

template<typename Sink>
void StreamSelectedRanges(const ColumnReader& child,
                          std::span<const std::pair<uint64_t, uint64_t>> ranges,
                          uint32_t d, ReadContext& ctx, Sink&& sink) {
  const auto rows_per_batch =
    static_cast<duckdb::idx_t>(std::max<uint64_t>(1, STANDARD_VECTOR_SIZE / d));
  duckdb::Vector batch{duckdb::LogicalType::FLOAT,
                       static_cast<duckdb::idx_t>(rows_per_batch) * d};
  RangeScanCursor cursor{child, ctx};
  for (const auto& [start, n_rows] : ranges) {
    for (uint64_t off = 0; off < n_rows; off += rows_per_batch) {
      const auto n = static_cast<duckdb::idx_t>(
        std::min<uint64_t>(rows_per_batch, n_rows - off));
      cursor.SeekTo((start + off) * d);
      cursor.Read(batch, static_cast<duckdb::idx_t>(n) * d, 0);
      sink(start + off, n, duckdb::FlatVector::GetData<float>(batch));
    }
  }
}

std::vector<float> GatherTrainingSample(const ColumnReader& child,
                                        uint64_t rows, uint32_t d,
                                        ReadContext& ctx,
                                        const std::vector<bool>& valid,
                                        uint64_t valid_count, uint64_t n_train,
                                        uint32_t seed) {
  std::vector<float> sample(static_cast<size_t>(n_train) * d);
  std::mt19937_64 rng{seed};
  uint64_t seen = 0;
  const auto reservoir_sink = [&](uint64_t first, duckdb::idx_t n,
                                  const float* p) {
    for (duckdb::idx_t k = 0; k < n; ++k) {
      if (!valid[first + k]) {
        continue;
      }
      const float* v = p + static_cast<size_t>(k) * d;
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

  const uint64_t target = (n_train > valid_count / kSampleSegmentOversample)
                            ? valid_count
                            : n_train * kSampleSegmentOversample;
  const size_t n_seg = child.DataRgCount();
  if (target >= valid_count || n_seg <= 1) {
    StreamRowBatches(child, rows, d, ctx, reservoir_sink);
    SDB_ASSERT(seen == valid_count);
  } else {
    std::vector<size_t> order(n_seg);
    std::iota(order.begin(), order.end(), size_t{0});
    std::mt19937_64 seg_rng{seed};
    std::shuffle(order.begin(), order.end(), seg_rng);

    std::vector<std::pair<uint64_t, uint64_t>> ranges;
    uint64_t valid_selected = 0;
    for (size_t i = 0; i < n_seg && valid_selected < target; ++i) {
      const uint64_t w_begin = child.DataBlockFirstRow(order[i]);
      const uint64_t w_end = child.DataBlockFirstRow(order[i] + 1);
      const uint64_t r_lo = (w_begin + d - 1) / d;
      const uint64_t r_hi = w_end / d;
      if (r_lo >= r_hi) {
        continue;
      }
      uint64_t vc = 0;
      for (uint64_t r = r_lo; r < r_hi; ++r) {
        vc += valid[r] ? 1 : 0;
      }
      if (vc == 0) {
        continue;
      }
      ranges.emplace_back(r_lo, r_hi - r_lo);
      valid_selected += vc;
    }
    std::sort(ranges.begin(), ranges.end());
    StreamSelectedRanges(child,
                         std::span<const std::pair<uint64_t, uint64_t>>{ranges},
                         d, ctx, reservoir_sink);
    SDB_ASSERT(seen >= n_train && seen <= valid_count);
  }
  return sample;
}

struct BuildSettings {
  size_t posting_size;
  size_t max_centroids;
  size_t min_centroids;
  VectorMetric metric;
  size_t niter;

  bool IsLeaf(size_t sample_size) const noexcept {
    return sample_size <= posting_size;
  }

  size_t ClusterSize(size_t sample_size) const noexcept {
    const double n = static_cast<double>(sample_size);
    const double tau = static_cast<double>(posting_size);
    size_t b;
    if (sample_size <= max_centroids * posting_size) {
      // cap-split: children land at ~tau (design NEW_CENTROIDS.md §3.2).
      b = std::clamp<size_t>(static_cast<size_t>(std::ceil(n / tau)), size_t{2},
                             max_centroids);
    } else {
      // sqrt-split (SKM): keep fan-out brute-forceable.
      b = std::clamp<size_t>(static_cast<size_t>(std::ceil(std::sqrt(n / tau))),
                             min_centroids, max_centroids);
    }
    return b;
  }
};

auto BuildAndSplit(std::span<float> data, size_t d, std::span<size_t> ids,
                   size_t n_clusters, VectorMetric metric, size_t niter,
                   const float* rotation) {
  auto centroids = TrainCentroids(
    metric, data.data(), data.size() / d, static_cast<uint32_t>(n_clusters),
    static_cast<uint32_t>(d), kTrainSeed, static_cast<uint32_t>(niter), 1u,
    ClusteringAlgo::Auto, rotation);
  AssignNearestGrouped(metric, centroids, d, data, ids);
  return centroids;
}

void BuildMesoReuse(std::vector<CentroidsBuilder::Node>& nodes,
                    std::span<const float> sample, size_t d,
                    size_t target_leaves, const BuildSettings& settings,
                    const float* rotation) {
  const size_t n = sample.size() / d;
  auto h =
    RunHskmHierarchical(sample.data(), n, static_cast<uint32_t>(target_leaves),
                        static_cast<uint32_t>(d), kTrainSeed, rotation);
  const size_t m = h.fine.size();
  if (settings.metric == VectorMetric::Cosine) {
    NormalizeRows(h.meso.data(), m, static_cast<uint32_t>(d));
    for (auto& block : h.fine) {
      NormalizeRows(block.data(), block.size() / d, static_cast<uint32_t>(d));
    }
  }
  const size_t meso_index = nodes.size();
  std::vector<size_t> meso_children;
  meso_children.reserve(m);
  for (size_t g = 0; g < m; ++g) {
    meso_children.emplace_back(meso_index + 1 + g);
  }
  nodes.emplace_back(CentroidsBuilder::Node{
    .centroids = std::move(h.meso), .children = std::move(meso_children)});
  for (size_t g = 0; g < m; ++g) {
    const size_t ki = h.fine[g].size() / d;
    nodes.emplace_back(
      CentroidsBuilder::Node{.centroids = std::move(h.fine[g]),
                             .children = std::vector<size_t>(ki, 0),
                             .leafs = ki});
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
          /*k=*/1, static_cast<uint32_t>(d), kTrainSeed, /*niter=*/8u,
          /*nredo=*/1u, ClusteringAlgo::Auto, rot);
        nodes.emplace_back(CentroidsBuilder::Node{
          .centroids = std::move(centroids), .children = {0}, .leafs = 1});
      }
      // centroid from parent will route to this posting
      continue;
    }
    if (entry.parent == std::numeric_limits<size_t>::max()) {
      const size_t target_leaves =
        (sample_size + settings.posting_size - 1) / settings.posting_size;
      if (HskmQualifies(settings.metric, static_cast<uint32_t>(target_leaves),
                        static_cast<uint32_t>(d))) {
        BuildMesoReuse(nodes, entry.sample, d, target_leaves, settings, rot);
        continue;
      }
    }
    size_t n_clusters = settings.ClusterSize(sample_size);
    auto centroids = BuildAndSplit(entry.sample, d, entry.ids, n_clusters,
                                   settings.metric, settings.niter, rot);
    const size_t n_built = centroids.size() / d;

    if (entry.parent < nodes.size()) {
      nodes[entry.parent].children.emplace_back(nodes.size());
    }
    nodes.emplace_back(
      CentroidsBuilder::Node{.centroids = std::move(centroids)});
    for (size_t i = 0, current = 0; i < n_built; ++i) {
      size_t start = current;
      while (current < entry.ids.size() && entry.ids[current] == i) {
        current++;
      }
      CentroidsEntry child{
        .parent = nodes.size() - 1,
        .sample = entry.sample.subspan(start * d, (current - start) * d),
        .ids = entry.ids.subspan(start, current - start),
      };
      centroids_build.emplace_back(std::move(child));
    }
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
  std::span<const size_t> sizes) {
  SDB_ASSERT(starts.size() == sizes.size());
  const size_t n_total = static_cast<size_t>(in.ReadI64());
  const size_t body_start = static_cast<size_t>(in.Position());
  const size_t offsets_start = body_start + n_total * d * sizeof(float);
  std::vector<CentroidsNode> nodes;
  nodes.reserve(starts.size());
  for (auto&& [start, size] : std::views::zip(starts, sizes)) {
    CentroidsNode node{level, d};
    node.size = size;

    in.Seek(body_start + start * d * sizeof(float));
    node.centroids.resize(node.size * d);
    if (node.size != 0) {
      in.ReadData(reinterpret_cast<byte_type*>(node.centroids.data()),
                  node.size * sizeof(node.centroids[0]) * d);
    }

    if (level > 0) {
      node.child_offsets.resize(node.size + 1);
      in.Seek(offsets_start + start * sizeof(size_t));
      in.ReadData(reinterpret_cast<byte_type*>(node.child_offsets.data()),
                  (node.size + 1) * sizeof(size_t));
    }
    nodes.emplace_back(std::move(node));
  }
  if (level > 0) {
    in.Seek(offsets_start + (n_total + 1) * sizeof(size_t));
  }
  return nodes;
}

std::vector<CentroidsNodeView> CentroidsNode::ReadLayer(
  IndexInput& in, size_t level, size_t d, std::span<const size_t> starts,
  std::span<const size_t> sizes, LayerBuffers& bufs, size_t& n_total) {
  SDB_ASSERT(starts.size() == sizes.size());
  n_total = static_cast<size_t>(in.ReadI64());
  const size_t body_start = static_cast<size_t>(in.Position());
  const uint64_t offsets_start = body_start + n_total * d * sizeof(float);
  std::vector<CentroidsNodeView> nodes;
  nodes.reserve(starts.size());
  bufs.centroids.reserve(starts.size());
  bufs.child_offsets.reserve(starts.size());
  for (auto&& [start, size] : std::views::zip(starts, sizes)) {
    CentroidsNodeView node;
    node.base = start;
    node.size = size;
    if (size == 0) {
      nodes.emplace_back(node);
      continue;
    }
    const uint64_t offset = body_start + start * d * sizeof(float);
    const size_t centroids_bytes = size * d * sizeof(float);
    if (const byte_type* p = in.ReadStable(offset, centroids_bytes)) {
      node.centroids =
        std::span<const float>{reinterpret_cast<const float*>(p), size * d};
    } else {
      auto& buf = bufs.centroids.emplace_back(size * d);
      in.ReadData(offset, reinterpret_cast<byte_type*>(buf.data()),
                  centroids_bytes);
      node.centroids = std::span<const float>{buf.data(), size * d};
    }
    if (level > 0) {
      auto& off = bufs.child_offsets.emplace_back(size + 1);
      in.ReadData(offsets_start + start * sizeof(size_t),
                  reinterpret_cast<byte_type*>(off.data()),
                  (size + 1) * sizeof(size_t));
      node.child_offsets = std::span<const size_t>{off.data(), size + 1};
    }
    nodes.emplace_back(node);
  }
  if (level > 0) {
    in.Seek(offsets_start + (n_total + 1) * sizeof(size_t));
  }
  return nodes;
}

void CentroidsNode::Serialize(IndexOutput& out) const {
  out.WriteU64(size);
  if (size != 0) {
    out.WriteData(reinterpret_cast<const byte_type*>(centroids.data()),
                  size * d * sizeof(centroids[0]));
  }
  if (level > 0) {
    SDB_ASSERT(child_offsets.size() == size + 1);
    out.WriteData(reinterpret_cast<const byte_type*>(child_offsets.data()),
                  (size + 1) * sizeof(size_t));
  }
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

CentroidsTree CentroidsTree::Deserialize(IndexInput& in, uint64_t byte_size) {
  auto head = IVFHeader::Deserialize(in);
  const size_t level = static_cast<size_t>(in.ReadI64());
  const size_t n_total_pos = static_cast<size_t>(in.Position());
  const size_t n_total = static_cast<size_t>(in.ReadI64());
  in.Seek(n_total_pos);
  auto nodes = CentroidsNode::Deserialize(in, level, head.d, {0}, {n_total});
  auto node = std::move(nodes.front());
  const size_t next_level_offset = static_cast<size_t>(in.Position());
  return {std::move(head), std::move(node), next_level_offset};
}

void CentroidsTree::Search(std::span<const float> query, IndexInput& in,
                           uint32_t nprobe, std::vector<uint32_t>& out_ids,
                           std::vector<float>* out_centroids) const {
  if (_root.size == 0) {
    out_ids.push_back(0);
    return;
  }
  auto beam = static_cast<uint32_t>(
    std::ceil(kBeamOverprobe * std::sqrt(static_cast<double>(nprobe))));
  if (_root.level >= 2) {
    beam = std::max(beam, nprobe);
  }
  if (_root.level > 0) {
    in.Seek(_next_level_offset);
  }
  const CentroidsNodeView root_view{
    .centroids = std::span<const float>{_root.centroids},
    .child_offsets = std::span<const size_t>{_root.child_offsets},
    .base = 0,
    .size = _root.size};
  std::vector<CentroidsNode::Candidate> leaves;
  irs::ResolveEnum<VectorMetric>(_head.metric, [&]<VectorMetric Metric>() {
    CentroidsNode::Search<Metric>(query, in, beam, out_centroids != nullptr,
                                  _root.level, std::span{&root_view, 1}, 0,
                                  _root.size, leaves);
    const auto k = std::min<size_t>(nprobe, leaves.size());
    const auto mid = leaves.begin() + k;
    std::ranges::partial_sort(leaves, mid, std::greater{},
                              &CentroidsNode::Candidate::dist);
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
}

void CentroidsBuilder::BuildTree(std::vector<float> sample, size_t leaf_size,
                                 size_t min_centroids, size_t max_centroids) {
  BuildSettings settings{
    .posting_size = std::max<size_t>(1, leaf_size),
    .max_centroids = max_centroids,
    .min_centroids = min_centroids,
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
    _n_rows += _nodes[j].centroids.size() / _d;
  }
}

CentroidsBuilder CentroidsBuilder::Create(const ColumnReader& vector_column,
                                          ReadContext& ctx, size_t rows,
                                          VectorMetric metric, uint32_t d,
                                          const CentroidsBuildParams& params) {
  CentroidsBuilder builder;
  builder._metric = metric;
  builder._d = d;
  const size_t t =
    params.posting_size != 0 ? params.posting_size : kPostingSizeDefault;
  const size_t min_c =
    params.min_centroids != 0 ? params.min_centroids : kMinCentroids;
  const size_t max_c =
    params.max_centroids != 0 ? params.max_centroids : kMaxCentroids;

  const auto* child = vector_column.Child();
  SDB_ASSERT(child);
  const auto valid = ReadValidity(vector_column, rows, ctx);
  size_t valid_count = 0;
  for (const bool v : valid) {
    valid_count += v;
  }

  size_t sample_size =
    params.sample_factor > 0
      ? static_cast<size_t>(params.sample_factor * valid_count)
      : (valid_count / t) * kTrainPointsPerLeaf;
  sample_size = std::max<size_t>(sample_size, params.min_train_sample);
  sample_size = std::min<size_t>(sample_size, kMaxTrainSample);
  sample_size = std::min<size_t>(sample_size, valid_count);

  const size_t tau =
    valid_count == 0
      ? t
      : std::max<size_t>(1,
                         static_cast<size_t>(static_cast<double>(sample_size) /
                                             valid_count * t));
  auto sample = GatherTrainingSample(*child, rows, d, ctx, valid, valid_count,
                                     sample_size, kTrainSeed);
  builder.BuildTree(std::move(sample), tau, min_c, max_c);
  return builder;
}

CentroidsBuilder CentroidsBuilder::CreateFromSample(
  std::vector<float> sample, uint32_t d, VectorMetric metric,
  const CentroidsBuildParams& params) {
  CentroidsBuilder builder;
  builder._metric = metric;
  builder._d = d;
  const size_t t =
    params.posting_size != 0 ? params.posting_size : kPostingSizeDefault;
  const size_t min_c =
    params.min_centroids != 0 ? params.min_centroids : kMinCentroids;
  const size_t max_c =
    params.max_centroids != 0 ? params.max_centroids : kMaxCentroids;
  builder.BuildTree(std::move(sample), t, min_c, max_c);
  return builder;
}

CentroidsSpan CentroidsBuilder::Serialize(IndexOutput& out) const {
  const IVFHeader head{.metric = _metric, .d = _d};
  const size_t offset = static_cast<size_t>(out.Position());
  head.Serialize(out);
  if (_nodes.empty()) {
    out.WriteU64(0);
    out.WriteU64(0);
    return {.offset = offset,
            .byte_size = static_cast<size_t>(out.Position()) - offset};
  }

  std::vector<size_t> depth(_nodes.size(), 0);
  for (size_t j = 0; j < _nodes.size(); ++j) {
    SDB_ASSERT(_nodes[j].children.size() == _nodes[j].centroids.size() / _d);
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
      layer.rows += _nodes[layer.last].centroids.size() / _d;
      ++layer.last;
    }
    first = layer.last;
    layers.emplace_back(layer);
  }

  out.WriteU64(layers.size() - 1);
  std::vector<size_t> offsets;
  for (size_t p = 0; p < layers.size(); ++p) {
    const auto& layer = layers[p];
    out.WriteU64(layer.rows);
    for (size_t j = layer.first; j < layer.last; ++j) {
      out.WriteData(
        reinterpret_cast<const byte_type*>(_nodes[j].centroids.data()),
        _nodes[j].centroids.size() * sizeof(float));
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
        running += child == 0 ? 0 : _nodes[child].centroids.size() / _d;
        offsets.push_back(running);
      }
    }
    SDB_ASSERT(offsets.size() == layer.rows + 1);
    SDB_ASSERT(running == layers[p + 1].rows);
    out.WriteData(reinterpret_cast<const byte_type*>(offsets.data()),
                  offsets.size() * sizeof(size_t));
  }
  return {.offset = offset,
          .byte_size = static_cast<size_t>(out.Position()) - offset};
}

void CentroidsBuilder::AssignCentroidsImpl(
  size_t node_index, std::span<float> data, size_t d, std::span<size_t> ids,
  std::span<size_t> perm,
  std::span<std::span<const float>> centroids_out) const {
  const auto& node = _nodes[node_index];
  AssignNearestGrouped(_metric, node.centroids, d, data, ids, perm,
                       centroids_out);
  for (size_t i = 0, current = 0; i < node.centroids.size() / d; ++i) {
    size_t start = current;
    while (current < ids.size() && ids[current] == i) {
      current++;
    }
    if (node.children[i] == 0) {
      absl::c_fill(ids.subspan(start, current - start),
                   _row_bases[node_index] + i);
      continue;
    }
    AssignCentroidsImpl(
      node.children[i], data.subspan(start * d, (current - start) * d), d,
      ids.subspan(start, current - start),
      perm.empty() ? perm : perm.subspan(start, current - start),
      centroids_out.empty() ? centroids_out
                            : centroids_out.subspan(start, current - start));
  }
}

AssignedCentroids CentroidsBuilder::AssignCentroids(
  std::span<float> data, size_t d,
  std::span<std::span<const float>> centroids_out) const {
  const size_t n = data.size() / d;
  AssignedCentroids result;
  result.ids.resize(n);
  result.perm.resize(n);
  std::iota(result.perm.begin(), result.perm.end(), size_t{0});
  if (_nodes.empty()) {
    return result;
  }
  AssignCentroidsImpl(0, data, d, result.ids, result.perm, centroids_out);
  return result;
}

}  // namespace irs
