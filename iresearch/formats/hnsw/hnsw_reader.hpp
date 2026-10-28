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
#include "iresearch/utils/huge_page_allocator.hpp"

namespace irs {

struct HnswData {
  HnswGraph graph;
  HugePageVector<float> vectors;
  std::shared_ptr<const std::vector<float>> rotation;
  std::shared_ptr<const HnswByteBasis> byte_rotation;
  HugePageVector<float> norms;
  HugePageVector<float> tails;
  HugePageVector<byte_type> codes;
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
  HnswIndex(HnswMeta meta, field_id field, HnswBases* bases) noexcept
    : _meta{meta}, _field{field}, _bases{bases} {}

  AnnKind Kind() const noexcept final { return AnnKind::Hnsw; }

  uint32_t Dim() const noexcept final { return _meta.header.d; }

  bool Empty() const noexcept final { return _meta.header.rows == 0; }

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

  std::shared_ptr<const HnswData> Load(const SubReader& segment) const;

  const HnswHeader& Header() const noexcept { return _meta.header; }

 private:
  QueryBuilder::ptr PrepareSearch(const SubReader& segment,
                                  const PrepareContext& ctx,
                                  const VectorFilterOptions& opts,
                                  HnswSearchLimits limits,
                                  AnnQueryCache* cache) const;

  HnswMeta _meta;
  field_id _field;
  HnswBases* _bases;
  mutable absl::once_flag _once;
  mutable std::shared_ptr<const HnswData> _data;
};

}  // namespace irs
