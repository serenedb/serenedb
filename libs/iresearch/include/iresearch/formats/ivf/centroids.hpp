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

#pragma once

#include <cstdint>
#include <functional>
#include <span>
#include <vector>

#include "iresearch/index/column_info.hpp"
#include "iresearch/store/data_input.hpp"
#include "iresearch/types.hpp"
#include "iresearch/utils/string.hpp"
#include "iresearch/utils/vector.hpp"

namespace irs {

class IndexOutput;
class ColumnReader;
class ReadContext;

struct IVFHeader {
  VectorMetric metric;
  uint32_t d;
  bstring quant_stats;

  static IVFHeader Deserialize(IndexInput& in);
  void Serialize(IndexOutput& out) const;
};

struct CentroidsNode {
  std::vector<float> centroids;
  std::vector<size_t> counts;  // level > 0: child count of each centroid
  size_t size;
  size_t level;
  size_t d;

  CentroidsNode(size_t level, size_t d) : level{level}, d{d} {}

  static std::vector<CentroidsNode> Deserialize(IndexInput& in, size_t level,
                                                size_t d,
                                                std::span<const size_t> starts,
                                                std::span<const size_t> sizes);
  void Serialize(IndexOutput& out, std::span<const size_t> clusters) const;

  template<VectorMetric Metric>
  static void Search(std::span<const float> query, IndexInput& in,
                     uint32_t nprobe, std::vector<uint32_t>& out_ids,
                     std::vector<float>* out_centroids,
                     std::span<const CentroidsNode> nodes,
                     std::span<const size_t> bases) {
    SDB_ASSERT(!nodes.empty());
    const size_t level = nodes[0].level;
    const uint16_t d = query.size();
    const auto* q = reinterpret_cast<const byte_type*>(query.data());
    constexpr bool kOrder = VectorMetricNearestIsLargest(Metric);
    const auto by_dist = [](const auto& l, const auto& r) noexcept {
      if constexpr (kOrder) {
        return l.dist > r.dist;
      } else {
        return l.dist < r.dist;
      }
    };

    if (level == 0) {
      struct Scored {
        float dist;
        size_t id;
        std::span<const float> centroid;
      };
      std::vector<Scored> scored;
      for (size_t node_index = 0; node_index < nodes.size(); ++node_index) {
        const auto& node = nodes[node_index];
        scored.reserve(scored.size() + node.size);
        for (size_t i = 0; i < node.size; ++i) {
          const byte_type* c =
            reinterpret_cast<const byte_type*>(node.centroids.data() + d * i);
          scored.push_back({ComputeDistance<Metric>(q, c, d),
                            bases[node_index] + i,
                            std::span{node.centroids}.subspan(i * d, d)});
        }
      }
      const auto k = std::min<size_t>(nprobe * nodes.size(), scored.size());
      const auto mid = scored.begin() + k;
      std::partial_sort(scored.begin(), mid, scored.end(), by_dist);
      out_ids.reserve(out_ids.size() + k);
      if (out_centroids) {
        out_centroids->reserve(out_centroids->size() + k * d);
      }
      for (auto it = scored.begin(); it != mid; ++it) {
        out_ids.emplace_back(static_cast<uint32_t>(it->id));
        if (out_centroids) {
          out_centroids->append_range(it->centroid);
        }
      }
      return;
    }

    struct Scored {
      float dist;
      size_t start;
      size_t count;
    };
    std::vector<Scored> scored;
    for (size_t node_index = 0; node_index < nodes.size(); ++node_index) {
      const auto& node = nodes[node_index];
      scored.reserve(scored.size() + node.size);
      size_t start = bases[node_index];
      for (size_t i = 0; i < node.size; ++i) {
        const byte_type* c =
          reinterpret_cast<const byte_type*>(node.centroids.data() + d * i);
        scored.push_back(
          {ComputeDistance<Metric>(q, c, d), start, node.counts[i]});
        start += node.counts[i];
      }
    }
    if (scored.empty()) {
      return;
    }
    const auto k = std::min<size_t>(nprobe * nodes.size(), scored.size());
    const auto mid = scored.begin() + k;
    std::partial_sort(scored.begin(), mid, scored.end(), by_dist);
    std::vector<size_t> starts, sizes;
    starts.reserve(k);
    sizes.reserve(k);
    for (auto it = scored.begin(); it != mid; ++it) {
      starts.emplace_back(it->start);
      sizes.emplace_back(it->count);
    }
    auto next_nodes =
      CentroidsNode::Deserialize(in, level - 1, d, starts, sizes);
    Search<Metric>(query, in, nprobe, out_ids, out_centroids, next_nodes,
                   starts);
  }
};

class CentroidsBuilder;

class CentroidsTree {
 public:
  CentroidsTree(IVFHeader&& head, CentroidsNode&& root,
                size_t next_level_offset)
    : _head{std::move(head)},
      _root{std::move(root)},
      _next_level_offset{next_level_offset} {}

  CentroidsTree(const CentroidsTree&) = delete;
  CentroidsTree(CentroidsTree&&) = default;

  CentroidsTree& operator=(const CentroidsTree&) = delete;
  CentroidsTree& operator=(CentroidsTree&&) = default;

  static CentroidsTree Deserialize(IndexInput& in, uint64_t byte_size);

  void Search(std::span<const float> query, IndexInput& in, uint32_t nprobe,
              std::vector<uint32_t>& out_ids,
              std::vector<float>* out_centroids) const;

  size_t Dim() const noexcept { return _head.d; }
  VectorMetric Metric() const noexcept { return _head.metric; }
  bool Empty() const noexcept { return _head.d == 0; }

  void SetQuantStats(bstring stats) noexcept {
    _head.quant_stats = std::move(stats);
  }
  const bstring& QuantStats() const noexcept { return _head.quant_stats; }

 private:
  IVFHeader _head;
  CentroidsNode _root;
  size_t _next_level_offset;
};

struct CentroidsSpan {
  size_t offset = 0;
  size_t byte_size = 0;
};

class CentroidsBuilder {
 public:
  CentroidsBuilder() = default;

  static CentroidsBuilder Create(const ColumnReader& vector_column,
                                 ReadContext& ctx, size_t rows,
                                 VectorMetric metric, uint32_t d);

  void Finish();

  CentroidsSpan Serialize(IndexOutput& out) const;

  std::span<const float> LeafCentroids() const noexcept {
    return _nodes.back().centroids;
  }

  std::vector<uint32_t> AssignCentroids(std::span<const float> data,
                                        size_t d) const;

 private:
  std::vector<CentroidsNode> _nodes;
  VectorMetric _metric = VectorMetric::L2Sqr;
  uint32_t _d = 0;
  size_t _n_clusters;
};

}  // namespace irs
