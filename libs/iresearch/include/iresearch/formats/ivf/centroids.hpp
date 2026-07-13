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
  std::vector<size_t> offsets;
  size_t size;
  size_t level;
  size_t next_level_offset;

  static CentroidsNode Deserialize(IndexInput& in, size_t level, size_t d,
                                   size_t offset = 0, size_t size = 0);
  void Serialize(IndexOutput& out) const;

  template<VectorMetric Metric>
  void Search(std::span<const float> query, IndexInput& in, uint32_t nprobe,
              std::vector<uint32_t>& out_ids, std::vector<float>* out_centroids,
              size_t base = 0) const {
    const uint16_t d = query.size();
    const auto* q = reinterpret_cast<const byte_type*>(query.data());
    std::vector<std::pair<float, size_t>> scored;
    scored.reserve(size);
    for (size_t i = 0; i < size; ++i) {
      const byte_type* c =
        reinterpret_cast<const byte_type*>(centroids.data() + d * i);
      scored.emplace_back(ComputeDistance<Metric>(q, c, d), i);
    }
    const auto k = std::min<size_t>(nprobe, size);
    const auto mid = scored.begin() + k;
    constexpr bool kOrder = VectorMetricNearestIsLargest(Metric);
    std::partial_sort(scored.begin(), mid, scored.end(),
                      [&](const auto& l, const auto& r) noexcept {
                        if constexpr (kOrder) {
                          return l.first > r.first;
                        } else {
                          return l.first < r.first;
                        }
                      });
    if (level == 0) {
      out_ids.reserve(out_ids.size() + k);
      if (out_centroids) {
        out_centroids->reserve(out_centroids->size() + k * d);
      }
      for (auto it = scored.begin(); it != mid; ++it) {
        const auto i = it->second;
        out_ids.emplace_back(static_cast<uint32_t>(base + i));
        if (out_centroids) {
          out_centroids->append_range(std::span{centroids}.subspan(i * d, d));
        }
      }
    } else {
      for (auto it = scored.begin(); it != mid; ++it) {
        const auto i = it->second;
        in.Seek(next_level_offset);
        const size_t offset = offsets[i];
        const size_t size =
          i + 1 < offsets.size() ? offsets[i + 1] - offsets[i] : 0;
        auto child = CentroidsNode::Deserialize(in, level - 1, d, offset, size);
        child.Search<Metric>(query, in, nprobe, out_ids, out_centroids, offset);
      }
    }
  }
};

class CentroidsBuilder;

class CentroidsTree {
 public:
  CentroidsTree() = default;
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
