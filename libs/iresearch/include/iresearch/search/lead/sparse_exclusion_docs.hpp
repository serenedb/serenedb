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

#include "iresearch/search/lead/concept.hpp"
#include "iresearch/search/probe/concept.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::lead {

template<Type Include, probe::Type Exclude>
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

  doc_id_t Value() const noexcept { return _doc; }

  doc_id_t Advance() { return Converge(_include.Advance()); }

  doc_id_t Seek(doc_id_t target) {
    if (target <= _doc) {
      return _doc;
    }
    return Converge(_include.Seek(target));
  }

 private:
  doc_id_t Converge(doc_id_t doc) {
    while (!doc_limits::eof(doc) && _exclude.Probe(doc) == doc) {
      doc = _include.Advance();
    }
    return _doc = doc;
  }

  Include _include;
  Exclude _exclude;
  doc_id_t _doc = doc_limits::invalid();
};

}  // namespace irs::lead
