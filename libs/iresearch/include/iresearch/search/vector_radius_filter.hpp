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

#include "iresearch/search/filter.hpp"
#include "iresearch/search/vector_filter_util.hpp"

namespace irs {

class ByRadius;

struct ByRadiusOptions : VectorFilterOptions {
  using FilterType = ByRadius;

  float radius = 0.f;
  bool inclusive = false;

  bool operator==(const ByRadiusOptions& rhs) const noexcept {
    return VectorFilterOptions::operator==(rhs) && radius == rhs.radius &&
           inclusive == rhs.inclusive;
  }
};

class ByRadius final : public FilterWithField<ByRadiusOptions> {
 public:
  QueryBuilder::ptr PrepareSegment(const SubReader& segment,
                                   const PrepareContext& ctx) const final;
};

}  // namespace irs
