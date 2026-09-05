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

#include <tuple>
#include <utility>

#include "iresearch/search/probe/concept.hpp"

namespace irs::probe {

template<Type Include, Type Exclude>
class SparseExclusionDocs {
 public:
  template<typename IncludeArgs, typename ExcludeArgs>
  SparseExclusionDocs(std::piecewise_construct_t, IncludeArgs&& include,
                      ExcludeArgs&& exclude)
    : _include{std::make_from_tuple<Include>(
        std::forward<IncludeArgs>(include))},
      _exclude{
        std::make_from_tuple<Exclude>(std::forward<ExcludeArgs>(exclude))} {}

  SparseExclusionDocs(SparseExclusionDocs&&) = delete;
  SparseExclusionDocs& operator=(SparseExclusionDocs&&) = delete;

  doc_id_t Probe(doc_id_t target) {
    if (const auto probe = _include.Probe(target); probe != target) {
      return probe;
    }
    if (_exclude.Probe(target) == target) {
      return target + 1;
    }
    return target;
  }

 private:
  Include _include;
  Exclude _exclude;
};

}  // namespace irs::probe
