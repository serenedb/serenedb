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
#include <memory>
#include <vector>

#include "iresearch/index/column_info.hpp"
#include "iresearch/search/filter.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs {

struct VectorFilterOptions {
  std::vector<float> query;
  field_id centroids_id = field_limits::invalid();
  field_id postings_id = field_limits::invalid();
  VectorMetric metric = VectorMetric::L2Sqr;
  VectorQuantization quant = VectorQuantization::None;
  uint32_t max_search_fanout = 16;
  uint32_t ef_search = 0;
  uint32_t min_ef = 0;
  std::shared_ptr<const Filter> inner;

  bool operator==(const VectorFilterOptions& rhs) const noexcept = default;
};

struct AnnIndex {
  virtual ~AnnIndex() = default;

  virtual AnnKind Kind() const noexcept = 0;

  virtual uint32_t Dim() const noexcept = 0;

  virtual bool Empty() const noexcept = 0;

  virtual bool HasQuantStats() const noexcept = 0;

  virtual bool SupportsFilter() const noexcept = 0;

  virtual bool SupportsRange() const noexcept = 0;

  virtual QueryBuilder::ptr PrepareKnn(const SubReader& segment,
                                       const PrepareContext& ctx,
                                       const VectorFilterOptions& opts,
                                       uint32_t effort) const = 0;

  virtual QueryBuilder::ptr PrepareRange(const SubReader& segment,
                                         const PrepareContext& ctx,
                                         const VectorFilterOptions& opts,
                                         float radius, bool inclusive,
                                         uint32_t effort) const = 0;
};

inline bool PrepareInnerFilter(const std::shared_ptr<const Filter>& inner,
                               const SubReader& segment,
                               const PrepareContext& ctx,
                               QueryBuilder::ptr& out) {
  if (!inner) {
    return true;
  }
  auto inner_ctx = ctx;
  inner_ctx.collector = nullptr;
  out = inner->PrepareSegment(segment, inner_ctx);
  return out != nullptr && !QueryBuilder::IsEmpty(*out);
}

}  // namespace irs
