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

class ByVectorSimilarity;

// kNN options. The filter's field id is the original ARRAY(FLOAT) vector column
// (used for exact rerank); `centroids_id` and `postings_id` are the companion
// IVF field ids allocated by the connector. `nprobe` cluster lists nearest to
// `query` are unioned; the outer top-K collector truncates the scored result.
// `inner` is an optional predicate (e.g. a text filter) intersected with the
// cluster-union candidates before rerank, for hybrid search.
struct ByVectorSimilarityOptions {
  using FilterType = ByVectorSimilarity;

  std::vector<float> query;
  field_id centroids_id = field_limits::invalid();
  field_id postings_id = field_limits::invalid();
  VectorMetric metric = VectorMetric::L2Sqr;
  uint32_t nprobe = 1;
  std::shared_ptr<const Filter> inner;

  bool operator==(const ByVectorSimilarityOptions& rhs) const noexcept {
    return query == rhs.query && centroids_id == rhs.centroids_id &&
           postings_id == rhs.postings_id && metric == rhs.metric &&
           nprobe == rhs.nprobe && inner == rhs.inner;
  }
};

class ByVectorSimilarity final
  : public FilterWithField<ByVectorSimilarityOptions> {
 public:
  QueryBuilder::ptr PrepareSegment(const SubReader& segment,
                                   const PrepareContext& ctx) const final;
};

}  // namespace irs
