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

#include <memory>
#include <vector>

#include "iresearch/index/column_info.hpp"
#include "iresearch/search/filter.hpp"

namespace irs {

class ByRadius;

// Range options. The filter's field id is the original ARRAY(FLOAT) vector
// column; `postings_id` is the companion IVF posting field. `radius` is
// expressed in the metric's own kernel space (squared L2 for VectorMetric::L2Sqr)
// -- the connector converts the user predicate at plan time. Every cluster is
// probed (no nprobe), and the iterator emits exactly the docs within radius.
struct ByRadiusOptions {
  using FilterType = ByRadius;

  std::vector<float> query;
  field_id centroids_id = field_limits::invalid();
  field_id postings_id = field_limits::invalid();
  VectorMetric metric = VectorMetric::L2Sqr;
  float radius = 0.f;
  bool inclusive = false;
  std::shared_ptr<const Filter> inner;

  bool operator==(const ByRadiusOptions& rhs) const noexcept {
    return query == rhs.query && centroids_id == rhs.centroids_id &&
           postings_id == rhs.postings_id && metric == rhs.metric &&
           radius == rhs.radius && inclusive == rhs.inclusive &&
           inner == rhs.inner;
  }
};

class ByRadius final : public FilterWithField<ByRadiusOptions> {
 public:
  Query::ptr prepare(const PrepareContext& ctx) const final;
};

}  // namespace irs
