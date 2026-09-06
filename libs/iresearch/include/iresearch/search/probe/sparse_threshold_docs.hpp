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

#include <utility>
#include <vector>

#include "basics/assert.h"
#include "iresearch/search/common/fixed_array.hpp"
#include "iresearch/search/probe/concept.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::probe {

template<Type Leaf, size_t N = 0>
class SparseThresholdDocs {
 public:
  template<typename Init>
  SparseThresholdDocs(size_t size, Init&& init, uint32_t min_match)
    : _probes{size, std::forward<Init>(init)}, _min_match{min_match} {
    SDB_ASSERT(_min_match > 1);
    SDB_ASSERT(_probes.size() >= _min_match);
  }

  doc_id_t Probe(doc_id_t target) {
    uint32_t hits = 0;
    uint32_t left = static_cast<uint32_t>(_probes.size());
    for (auto& probe : _probes) {
      hits += static_cast<uint32_t>(probe.Probe(target) == target);
      if (hits == _min_match) {
        return target;
      }
      --left;
      if (hits + left < _min_match) {
        break;
      }
    }
    return target + 1;
  }

 private:
  search::RunOf<Leaf, N> _probes;
  uint32_t _min_match;
};

}  // namespace irs::probe
