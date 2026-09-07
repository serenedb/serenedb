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

#include <algorithm>
#include <utility>

#include "basics/shared.hpp"
#include "iresearch/search/common/fixed_array.hpp"
#include "iresearch/search/probe/concept.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::probe {

template<Type Leaf, size_t N = 0>
class SparseDisjunctionDocs {
 public:
  template<typename Init>
  SparseDisjunctionDocs(size_t size, Init&& init)
    : _leaves{size, std::forward<Init>(init)} {
    SDB_ASSERT(_leaves.size() > 1);
  }

  SparseDisjunctionDocs(SparseDisjunctionDocs&&) = delete;
  SparseDisjunctionDocs& operator=(SparseDisjunctionDocs&&) = delete;

  doc_id_t Probe(doc_id_t target) {
    auto next = doc_limits::eof();
    for (size_t i = 0, count = _leaves.size(); i != count; ++i) {
      const auto doc = _leaves[i].Probe(target);
      if (doc == target) {
        return target;
      }
      next = std::min(next, doc);
    }
    return next;
  }

 private:
  search::RunOf<Leaf, N> _leaves;
};

}  // namespace irs::probe
