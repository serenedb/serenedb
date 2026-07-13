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
#include <cmath>
#include <cstring>
#include <utility>

#include "basics/errors.h"
#include "basics/exceptions.h"
#include "iresearch/formats/ivf/clustering.hpp"
#include "iresearch/formats/ivf/ivf_reader.hpp"
#include "iresearch/store/data_input.hpp"
#include "iresearch/store/data_output.hpp"

namespace irs {
namespace {

constexpr size_t kCentroidsFactor = 2;
constexpr size_t kCentroidsThreshold = (1 << 12);
constexpr size_t kTrainSeed = 42;

std::vector<size_t> SplitByCentroids(VectorMetric metric, std::span<float> data,
                                     std::span<const float> centroids,
                                     size_t d) {
  const size_t n = data.size() / d;
  const size_t c = centroids.size() / d;
  std::vector<size_t> offsets(c, 0);
  if (n == 0 || c == 0) {
    return offsets;
  }

  std::vector<uint32_t> assign;
  AssignNearest(metric, data.data(), n, centroids.data(),
                static_cast<uint32_t>(c), static_cast<uint32_t>(d), assign);

  for (const uint32_t a : assign) {
    ++offsets[a];
  }
  size_t running = 0;
  for (auto& off : offsets) {
    const size_t count = std::exchange(off, running);
    running += count;
  }

  std::vector<float> reordered(data.size());
  std::vector<size_t> cursor = offsets;
  for (size_t i = 0; i < n; ++i) {
    const uint32_t bucket = assign[i];
    const size_t pos = cursor[bucket]++;
    std::memcpy(reordered.data() + pos * d, data.data() + i * d,
                d * sizeof(float));
  }
  std::memcpy(data.data(), reordered.data(), data.size() * sizeof(float));
  return offsets;
}

}  // namespace

CentroidsNode CentroidsNode::Deserialize(IndexInput& in, size_t level, size_t d,
                                         size_t offset, size_t size) {
  const size_t n_total = static_cast<size_t>(in.ReadI64());
  const size_t body_start = static_cast<size_t>(in.Position());
  CentroidsNode node{.size = size == 0 ? n_total - offset : size,
                     .level = level};
  if (level > 0) {
    node.next_level_offset = body_start +
                             n_total * sizeof(node.centroids[0]) * d +
                             n_total * sizeof(node.offsets[0]);
  }

  in.Seek(body_start + offset * d * sizeof(node.centroids[0]));
  node.centroids.resize(node.size * d);
  in.ReadData(reinterpret_cast<byte_type*>(node.centroids.data()),
              node.size * sizeof(node.centroids[0]) * d);

  if (level > 0) {
    const size_t offsets_start =
      body_start + n_total * sizeof(node.centroids[0]) * d;
    in.Seek(offsets_start + offset * sizeof(node.offsets[0]));
    node.offsets.resize(node.size);
    in.ReadData(reinterpret_cast<byte_type*>(node.offsets.data()),
                node.size * sizeof(node.offsets[0]));
  }
  return node;
}

void CentroidsNode::Serialize(IndexOutput& out) const {
  out.WriteU64(size);
  out.WriteData(reinterpret_cast<const byte_type*>(centroids.data()),
                centroids.size() * sizeof(centroids[0]));
  if (level > 0) {
    out.WriteData(reinterpret_cast<const byte_type*>(offsets.data()),
                  offsets.size() * sizeof(offsets[0]));
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
  CentroidsTree tree;
  tree._head = IVFHeader::Deserialize(in);
  size_t level = static_cast<size_t>(in.ReadI64());
  tree._root = CentroidsNode::Deserialize(in, level, tree._head.d);
  return tree;
}

void CentroidsTree::Search(std::span<const float> query, IndexInput& in,
                           uint32_t nprobe, std::vector<uint32_t>& out_ids,
                           std::vector<float>* out_centroids) const {
  const size_t n_layers = _root.level + 1;
  const auto per_layer = static_cast<uint32_t>(std::ceil(std::pow(
    static_cast<double>(nprobe), 1.0 / static_cast<double>(n_layers))));
  irs::ResolveEnum<VectorMetric>(_head.metric, [&]<VectorMetric Metric>() {
    _root.Search<Metric>(query, in, per_layer, out_ids, out_centroids);
  });
}

CentroidsBuilder CentroidsBuilder::Create(const ColumnReader& vector_column,
                                          ReadContext& ctx, size_t rows,
                                          VectorMetric metric, uint32_t d) {
  size_t n_clusters;
  CentroidsBuilder builder;
  builder._metric = metric;
  builder._d = d;
  {
    CentroidsNode leaf;
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

    std::vector<float> sample(valid_count * d);
    uint64_t seen = 0;
    StreamRowBatches(*child, rows, d, ctx,
                     [&](uint64_t first, duckdb::idx_t n, const float* p) {
                       for (duckdb::idx_t k = 0; k < n; ++k) {
                         if (!valid[first + k]) {
                           continue;
                         }
                         std::memcpy(sample.data() + seen * d,
                                     p + static_cast<size_t>(k) * d,
                                     d * sizeof(float));
                         ++seen;
                       }
                     });
    SDB_ASSERT(seen == valid_count);

    leaf.centroids = n_clusters == 0
                       ? std::vector<float>{}
                       : TrainCentroids(metric, sample.data(), valid_count,
                                        n_clusters, d, kTrainSeed);
    leaf.level = 0;
    leaf.size = leaf.centroids.size() / d;
    builder._nodes.emplace_back(std::move(leaf));
  }
  auto current_centroids = std::span{builder._nodes.back().centroids};
  while (current_centroids.size() > kCentroidsThreshold) {
    n_clusters = std::clamp<size_t>(
      static_cast<size_t>(kCentroidsFactor *
                          std::sqrt(static_cast<double>(n_clusters))),
      size_t{1}, n_clusters - 1);
    auto new_centroids = TrainCentroids(
      metric, current_centroids.data(), current_centroids.size() / d,
      static_cast<uint32_t>(n_clusters), d, kTrainSeed);
    CentroidsNode node;
    node.centroids = std::move(new_centroids);
    node.size = node.centroids.size() / d;
    node.level = builder._nodes.back().level + 1;
    builder._nodes.emplace_back(std::move(node));
    current_centroids = std::span{builder._nodes.back().centroids};
  }
  absl::c_reverse(builder._nodes);

  return builder;
}

void CentroidsBuilder::Finish() {
  for (size_t i = 1; i < _nodes.size(); ++i) {
    _nodes[i - 1].offsets = SplitByCentroids(_metric, _nodes[i].centroids,
                                             _nodes[i - 1].centroids, _d);
  }
}

CentroidsSpan CentroidsBuilder::Serialize(IndexOutput& out) const {
  const IVFHeader head{.metric = _metric, .d = _d};
  const size_t offset = static_cast<size_t>(out.Position());
  head.Serialize(out);
  out.WriteU64(_nodes[0].level);
  for (const auto& node : _nodes) {
    node.Serialize(out);
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
