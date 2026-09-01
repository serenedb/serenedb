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

#include "iresearch/search/filter.hpp"
#include "iresearch/search/vector_filter_util.hpp"

namespace irs {

class ByVectorSimilarity;

struct ByVectorSimilarityOptions : VectorFilterOptions {
  using FilterType = ByVectorSimilarity;

  uint32_t nprobe = 1;
  uint32_t max_search_fanout = 16;

  bool operator==(const ByVectorSimilarityOptions& rhs) const noexcept =
    default;
};

class ByVectorSimilarity final
  : public FilterWithField<ByVectorSimilarityOptions> {
 public:
  QueryBuilder::ptr PrepareSegment(const SubReader& segment,
                                   const PrepareContext& ctx) const final;

  PrepareCollector::ptr MakeCollectorImpl(const Scorer* scorer) const final;
};

}  // namespace irs
