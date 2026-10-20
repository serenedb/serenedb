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
#include <span>
#include <vector>

#include "iresearch/index/column_info.hpp"
#include "iresearch/store/data_input.hpp"
#include "iresearch/types.hpp"
#include "iresearch/utils/resource_manager.hpp"
#include "iresearch/utils/string.hpp"

namespace irs {

class IndexOutput;
class ColumnReader;
class ReadContext;

struct IVFHeader {
  VectorMetric metric;
  uint32_t d;

  static IVFHeader Deserialize(IndexInput& in);
  void Serialize(IndexOutput& out) const;
};

struct CentroidsNodeView {
  std::span<const float> centroids;
  std::span<const size_t> child_offsets;
  size_t base;
  size_t size;
};

struct LayerBuffers {
  std::vector<std::vector<float>> centroids;
  std::vector<std::vector<size_t>> child_offsets;
};

struct CentroidsNode {
  std::vector<float> centroids;
  std::vector<size_t> child_offsets;
  size_t size;
  size_t level;
  size_t d;

  CentroidsNode(size_t level, size_t d) : level{level}, d{d} {}

  struct Candidate {
    float dist;
    size_t id;
    std::span<const float> centroid;
  };

  static std::vector<CentroidsNode> Deserialize(IndexInput& in, size_t level,
                                                size_t d,
                                                std::span<const size_t> starts,
                                                std::span<const size_t> sizes);
  void Serialize(IndexOutput& out) const;

  static std::vector<CentroidsNodeView> ReadLayer(
    IndexInput& in, size_t level, size_t d, std::span<const size_t> starts,
    std::span<const size_t> sizes, LayerBuffers& bufs, size_t& n_total);
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
              ManagedVector<float>* out_centroids, uint32_t beam = 0) const;

  size_t Dim() const noexcept { return _head.d; }
  VectorMetric Metric() const noexcept { return _head.metric; }
  bool Empty() const noexcept { return _head.d == 0; }
  size_t Levels() const noexcept { return _root.level + 1; }
  size_t RootSize() const noexcept { return _root.size; }

  void SetQuantStats(uint64_t offset, uint64_t byte_size,
                     bool list_batches) noexcept {
    _stats_offset = offset;
    _stats_byte_size = byte_size;
    _list_batches = list_batches;
  }
  bool HasQuantStats() const noexcept { return _stats_byte_size != 0; }
  uint64_t QuantStatsOffset() const noexcept { return _stats_offset; }
  bool ListBatches() const noexcept { return _list_batches; }

 private:
  IVFHeader _head;
  CentroidsNode _root;
  size_t _next_level_offset;
  uint64_t _stats_offset = 0;
  uint64_t _stats_byte_size = 0;
  bool _list_batches = false;
};

struct CentroidsSpan {
  size_t offset = 0;
  size_t byte_size = 0;
};

struct AssignedCentroids {
  std::vector<size_t> ids;
  std::vector<size_t> perm;
};

struct CentroidsBuildParams {
  size_t posting_size = 0;
  size_t max_fanout = 0;
  double sample_factor = 0;
  uint64_t min_train_sample = 0;
};

class CentroidsBuilder {
 public:
  struct Node {
    std::vector<float> centroids;
    std::vector<size_t> children;
    size_t leafs = 0;

    size_t Rows(size_t d) const noexcept { return centroids.size() / d; }
  };

  CentroidsBuilder() = default;

  static CentroidsBuilder Create(const ColumnReader& vector_column,
                                 ReadContext& ctx, size_t rows,
                                 VectorMetric metric, uint32_t d,
                                 const CentroidsBuildParams& params = {});

  static CentroidsBuilder CreateFromSample(
    std::vector<float> sample, uint32_t d, VectorMetric metric,
    const CentroidsBuildParams& params = {});

  CentroidsSpan Serialize(IndexOutput& out) const;

  AssignedCentroids AssignCentroids(
    std::span<float> data, size_t d,
    std::span<std::span<const float>> centroids_out = {}) const;

  size_t NumClusters() const noexcept { return _nodes.empty() ? 1 : _n_rows; }

 private:
  static CentroidsBuilder BuildFromSample(std::vector<float> sample, uint32_t d,
                                          VectorMetric metric, size_t leaf_size,
                                          size_t max_fanout);

  void BuildTree(std::vector<float> sample, size_t leaf_size,
                 size_t max_fanout);

  void AssignCentroidsImpl(
    size_t node_index, std::span<float> data, size_t d, std::span<size_t> ids,
    std::span<size_t> perm,
    std::span<std::span<const float>> centroids_out) const;

  std::vector<Node> _nodes;
  std::vector<size_t> _row_bases;
  VectorMetric _metric = VectorMetric::L2Sqr;
  uint32_t _d = 0;
  size_t _n_rows = 0;
};

}  // namespace irs
