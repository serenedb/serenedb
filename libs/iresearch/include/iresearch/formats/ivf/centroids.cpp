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

constexpr size_t kCentroidsFactor = 2;
constexpr size_t kCentroidsThreshold = (1 << 12);
constexpr size_t kTrainSeed = 42;
constexpr uint64_t kTrainPointsPerCentroid = 64;
constexpr uint64_t kSampleSegmentOversample = 4;

std::vector<size_t> SplitByCentroids(VectorMetric metric, std::span<float> data,
                                     std::span<const float> centroids,
                                     size_t d) {
  const size_t n = data.size() / d;
  const size_t c = centroids.size() / d;
  std::vector<size_t> counts(c, 0);
  if (n == 0 || c == 0) {
    return counts;
  }

  std::vector<uint32_t> assign;
  AssignNearest(metric, data.data(), n, centroids.data(),
                static_cast<uint32_t>(c), static_cast<uint32_t>(d), assign);

  for (const uint32_t a : assign) {
    ++counts[a];
  }

  std::vector<size_t> starts(c);
  {
    size_t running = 0;
    for (size_t i = 0; i < c; ++i) {
      starts[i] = running;
      running += counts[i];
    }
  }

  std::vector<float> reordered(data.size());
  std::vector<size_t> cursor = starts;
  for (size_t i = 0; i < n; ++i) {
    const uint32_t bucket = assign[i];
    const size_t pos = cursor[bucket]++;
    std::memcpy(reordered.data() + pos * d, data.data() + i * d,
                d * sizeof(float));
  }
  std::memcpy(data.data(), reordered.data(), data.size() * sizeof(float));
  return counts;
}

std::vector<size_t> ExclusiveScan(std::span<const size_t> counts) {
  std::vector<size_t> starts(counts.size());
  size_t running = 0;
  for (size_t i = 0; i < counts.size(); ++i) {
    starts[i] = running;
    running += counts[i];
  }
  return starts;
}

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

}  // namespace

std::vector<CentroidsNode> CentroidsNode::Deserialize(
  IndexInput& in, size_t level, size_t d, std::span<const size_t> starts,
  std::span<const size_t> sizes) {
  SDB_ASSERT(starts.size() == sizes.size());
  const size_t n_total = static_cast<size_t>(in.ReadI64());
  const size_t body_start = static_cast<size_t>(in.Position());
  const size_t stride = d * sizeof(float) + (level > 0 ? sizeof(size_t) : 0);
  std::vector<CentroidsNode> nodes;
  nodes.reserve(starts.size());
  for (auto&& [start, size] : std::views::zip(starts, sizes)) {
    CentroidsNode node{level, d};
    node.size = size;

    in.Seek(body_start + start * stride);
    node.centroids.resize(node.size * d);
    in.ReadData(reinterpret_cast<byte_type*>(node.centroids.data()),
                node.size * sizeof(node.centroids[0]) * d);

    if (level > 0) {
      node.counts.resize(node.size);
      in.ReadData(reinterpret_cast<byte_type*>(node.counts.data()),
                  node.size * sizeof(node.counts[0]));
    }
    nodes.emplace_back(std::move(node));
  }
  if (level > 0) {
    in.Seek(body_start + n_total * stride);
  }
  return nodes;
}

void CentroidsNode::Serialize(IndexOutput& out,
                              std::span<const size_t> clusters) const {
  out.WriteU64(size);
  SDB_ASSERT(!clusters.empty());
  for (size_t i = 0; i < clusters.size(); ++i) {
    const size_t length = i == clusters.size() - 1
                            ? size - clusters[i]
                            : clusters[i + 1] - clusters[i];

    out.WriteData(
      reinterpret_cast<const byte_type*>(centroids.data() + clusters[i] * d),
      length * d * sizeof(centroids[0]));
    if (level > 0) {
      out.WriteData(
        reinterpret_cast<const byte_type*>(counts.data() + clusters[i]),
        length * sizeof(counts[0]));
    }
  }
}

IVFHeader IVFHeader::Deserialize(IndexInput& in) {
  IVFHeader head;
  head.metric = static_cast<VectorMetric>(in.ReadByte());
  head.d = static_cast<uint32_t>(in.ReadI32());
  const size_t quant_size = static_cast<size_t>(in.ReadI64());
  head.quant_stats.resize(quant_size);
  in.ReadData(head.quant_stats.data(), quant_size);
  return head;
}

void IVFHeader::Serialize(IndexOutput& out) const {
  out.WriteByte(static_cast<byte_type>(metric));
  out.WriteU32(d);
  out.WriteU64(quant_stats.size());
  out.WriteData(quant_stats.data(), quant_stats.size());
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
  const size_t n_layers = _root.level + 1;
  const auto per_layer = static_cast<uint32_t>(std::ceil(std::pow(
    static_cast<double>(nprobe), 1.0 / static_cast<double>(n_layers))));
  if (_root.level > 0) {
    in.Seek(_next_level_offset);
  }
  const std::array<size_t, 1> root_base{0};
  irs::ResolveEnum<VectorMetric>(_head.metric, [&]<VectorMetric Metric>() {
    CentroidsNode::Search<Metric>(query, in, per_layer, out_ids, out_centroids,
                                  std::span{&_root, 1}, root_base);
  });
}

CentroidsBuilder CentroidsBuilder::Create(const ColumnReader& vector_column,
                                          ReadContext& ctx, size_t rows,
                                          VectorMetric metric, uint32_t d,
                                          uint64_t min_train_sample) {
  size_t n_clusters;
  CentroidsBuilder builder;
  builder._metric = metric;
  builder._d = d;
  {
    const auto* child = vector_column.Child();
    SDB_ASSERT(child);

    const auto valid = ReadValidity(vector_column, rows, ctx);
    uint64_t valid_count = 0;
    for (const bool v : valid) {
      valid_count += v;
    }

    n_clusters =
      valid_count == 0
        ? 0
        : std::clamp<size_t>(
            static_cast<size_t>(kCentroidsFactor *
                                std::sqrt(static_cast<double>(valid_count))),
            size_t{1}, valid_count);
    builder._n_clusters = n_clusters;

    uint64_t n_train =
      std::min<uint64_t>(valid_count, n_clusters * kTrainPointsPerCentroid);
    if (min_train_sample > 0) {
      n_train = std::min<uint64_t>(
        valid_count, std::max<uint64_t>(n_train, min_train_sample));
    }
    n_train = std::clamp<uint64_t>(
      n_train, std::min<uint64_t>(n_clusters, valid_count), valid_count);

    builder._sample = GatherTrainingSample(*child, rows, d, ctx, valid,
                                           valid_count, n_train, kTrainSeed);

    CentroidsNode leaf{0, d};

    leaf.centroids = n_clusters == 0
                       ? std::vector<float>{}
                       : TrainCentroids(metric, builder._sample.data(), n_train,
                                        n_clusters, d, kTrainSeed);
    leaf.size = leaf.centroids.size() / d;
    builder._nodes.emplace_back(std::move(leaf));
  }
  while (builder._nodes.back().size > kCentroidsThreshold) {
    auto current_centroids = std::span{builder._nodes.back().centroids};
    n_clusters = std::clamp<size_t>(
      static_cast<size_t>(kCentroidsFactor *
                          std::sqrt(static_cast<double>(n_clusters))),
      size_t{1}, n_clusters - 1);
    auto new_centroids = TrainCentroids(
      metric, current_centroids.data(), current_centroids.size() / d,
      static_cast<uint32_t>(n_clusters), d, kTrainSeed);
    CentroidsNode node{builder._nodes.back().level + 1, d};
    node.centroids = std::move(new_centroids);
    node.size = node.centroids.size() / d;
    builder._nodes.emplace_back(std::move(node));
  }
  absl::c_reverse(builder._nodes);

  return builder;
}

void CentroidsBuilder::Finish() {
  for (size_t i = 1; i < _nodes.size(); ++i) {
    _nodes[i - 1].counts = SplitByCentroids(_metric, _nodes[i].centroids,
                                            _nodes[i - 1].centroids, _d);
  }
}

CentroidsSpan CentroidsBuilder::Serialize(IndexOutput& out) const {
  const IVFHeader head{.metric = _metric, .d = _d};
  const size_t offset = static_cast<size_t>(out.Position());
  head.Serialize(out);
  out.WriteU64(_nodes[0].level);
  const std::array<size_t, 1> root_clusters{0};
  _nodes[0].Serialize(out, root_clusters);
  for (size_t i = 1; i < _nodes.size(); ++i) {
    _nodes[i].Serialize(out, ExclusiveScan(_nodes[i - 1].counts));
  }
  return {.offset = offset,
          .byte_size = static_cast<size_t>(out.Position()) - offset};
}

std::vector<uint32_t> CentroidsBuilder::AssignCentroids(
  std::span<const float> data, size_t d) const {
  const size_t n = data.size() / d;
  std::vector<uint32_t> ids;
  const auto& leaf = _nodes.back();
  AssignNearest(_metric, data.data(), n, leaf.centroids.data(),
                static_cast<uint32_t>(leaf.centroids.size() / d),
                static_cast<uint32_t>(d), ids);
  return ids;
}

}  // namespace irs
