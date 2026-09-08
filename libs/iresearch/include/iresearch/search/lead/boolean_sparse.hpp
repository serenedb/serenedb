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
#include <type_traits>
#include <utility>

#include "basics/empty.hpp"
#include "iresearch/search/lead/concept.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::lead {

template<Type Lead, typename Probes, typename Excludes>
class BooleanSparse {
 public:
  static constexpr bool kProbes = !std::is_same_v<Probes, utils::Empty>;
  static constexpr bool kExcludes = !std::is_same_v<Excludes, utils::Empty>;
  static_assert(kProbes || kExcludes);

  template<typename LeadArgs, typename ProbesArgs, typename ExcludesArgs>
  BooleanSparse(std::piecewise_construct_t, LeadArgs&& lead,
                ProbesArgs&& probes, ExcludesArgs&& excludes)
    : _lead{std::make_from_tuple<Lead>(std::forward<LeadArgs>(lead))},
      _probes{std::make_from_tuple<Probes>(std::forward<ProbesArgs>(probes))},
      _excludes{
        std::make_from_tuple<Excludes>(std::forward<ExcludesArgs>(excludes))} {}

  BooleanSparse(BooleanSparse&&) = delete;
  BooleanSparse& operator=(BooleanSparse&&) = delete;

  doc_id_t Advance() { return Converge(_lead.Advance()); }

  doc_id_t Seek(doc_id_t target) {
    if (target <= _doc) {
      return _doc;
    }
    return Converge(_lead.Seek(target));
  }

 private:
  doc_id_t Converge(doc_id_t doc) {
    while (!doc_limits::eof(doc)) {
      if constexpr (kProbes) {
        const auto probe = _probes.Probe(doc);
        if (probe != doc) {
          doc = _lead.Seek(probe);
          continue;
        }
      }
      if constexpr (kExcludes) {
        if (_excludes.Probe(doc) == doc) {
          doc = _lead.Advance();
          continue;
        }
      }
      break;
    }
    return _doc = doc;
  }

  Lead _lead;
  [[no_unique_address]] Probes _probes;
  [[no_unique_address]] Excludes _excludes;
  doc_id_t _doc = doc_limits::invalid();
};

}  // namespace irs::lead
