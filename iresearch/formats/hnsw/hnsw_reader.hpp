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

#include <absl/base/call_once.h>

#include <cstdint>
#include <memory>
#include <vector>

#include "iresearch/formats/hnsw/hnsw_bases.hpp"
#include "iresearch/formats/hnsw/hnsw_graph.hpp"
#include "iresearch/formats/index/idx_reader.hpp"
#include "iresearch/formats/ivf/quantizer.hpp"
#include "iresearch/search/detail/ann_index.hpp"

namespace irs {

struct HnswHeader {
  uint32_t d = 0;
  VectorMetric metric = VectorMetric::L2Sqr;
  VectorQuantization quant = VectorQuantization::None;
  uint32_t ef_construction = 0;
  uint32_t record_size = 0;
  uint64_t rows = 0;
  bool rotated = false;
};

struct HnswData {
  HnswGraph graph;
  std::vector<float> vectors;
  std::shared_ptr<const std::vector<float>> rotation;
  std::shared_ptr<const HnswByteBasis> byte_rotation;
  std::vector<float> norms;
  std::vector<float> tails;
  bstring codes;
  std::vector<float> centroid;
  std::shared_ptr<const QuantizerStats> stats;
};

struct HnswSearchLimits {
  uint32_t ef = 0;
  score_t threshold = kHnswNoThreshold;
  size_t max_results = 0;
  bool inclusive = false;
};

class HnswIndex final : public AnnIndex {
 public:
  HnswIndex(HnswHeader header, HnswMeta meta, field_id field,
            HnswBases* bases) noexcept
    : _header{header}, _meta{meta}, _field{field}, _bases{bases} {}

  AnnKind Kind() const noexcept final { return AnnKind::Hnsw; }

  uint32_t Dim() const noexcept final { return _header.d; }

  bool Empty() const noexcept final { return _header.rows == 0; }

  bool HasQuantStats() const noexcept final {
    return _header.quant != VectorQuantization::None;
  }

  bool SupportsFilter() const noexcept final { return true; }

  bool SupportsRange() const noexcept final { return true; }

  QueryBuilder::ptr PrepareKnn(const SubReader& segment,
                               const PrepareContext& ctx,
                               const VectorFilterOptions& opts, uint32_t effort,
                               AnnQueryCache* cache) const final;

  QueryBuilder::ptr PrepareRange(const SubReader& segment,
                                 const PrepareContext& ctx,
                                 const VectorFilterOptions& opts, float radius,
                                 bool inclusive, uint32_t effort,
                                 AnnQueryCache* cache) const final;

  AnnExactCodes PrepareExact(const SubReader& segment,
                             std::span<const float> query,
                             VectorMetric metric) const final;

  static HnswHeader ReadHeader(IndexInput& in);

  std::shared_ptr<const HnswData> Load(const SubReader& segment) const;

  const HnswHeader& Header() const noexcept { return _header; }

 private:
  QueryBuilder::ptr PrepareSearch(const SubReader& segment,
                                  const PrepareContext& ctx,
                                  const VectorFilterOptions& opts,
                                  HnswSearchLimits limits,
                                  AnnQueryCache* cache) const;

  HnswHeader _header;
  HnswMeta _meta;
  field_id _field;
  HnswBases* _bases;
  mutable absl::once_flag _once;
  mutable std::shared_ptr<const HnswData> _data;
};

}  // namespace irs
