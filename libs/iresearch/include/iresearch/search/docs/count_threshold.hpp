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

#include "iresearch/search/common/window.hpp"
#include "iresearch/search/docs/emit.hpp"
#include "iresearch/search/docs/root.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::docs {

template<typename Leaves, typename Table>
class CountThreshold : public Root {
 public:
  template<typename LeavesArgs>
  CountThreshold(Table table, std::piecewise_construct_t, LeavesArgs&& leaves,
                 uint32_t min_match)
    : _emit{table},
      _leaves{std::make_from_tuple<Leaves>(std::forward<LeavesArgs>(leaves))},
      _min_match{min_match} {
    SDB_ASSERT(_min_match > 1);
  }

  uint32_t Run(doc_id_t* IRS_RESTRICT out, uint32_t capacity) final {
    SDB_ASSERT(capacity >= doc_limits::kMinCapacity);
    uint32_t n = 0;

    for (;;) {
      if (!_emit.Drain(out, capacity, n)) {
        return n;
      }
      if (_leaves.Live() < _min_match || n == capacity) {
        return n;
      }
      if (!_emit.Skip(_min)) {
        return n;
      }
      SDB_ASSERT(_min <= doc_limits::eof() - search::kWindowDocs);
      const doc_id_t max = _min + search::kWindowDocs;

      const auto next = _leaves.Visit(
        max, [&](auto& leaf) { return leaf.Count(_min, max, _counts); });

      auto* const mask = _emit.Mask();
      for (uint32_t w = 0; w != search::kWindowWords; ++w) {
        auto* const counts = _counts + w * search::kWindowBits;
        uint64_t word = 0;
        for (uint32_t i = 0; i != search::kWindowBits; ++i) {
          word |= uint64_t{counts[i] >= _min_match} << i;
        }
        std::fill_n(counts, search::kWindowBits, uint32_t{0});
        mask[w] = word;
      }
      _emit.Opened(_min);
      _min = next;
    }
  }

 private:
  alignas(64) uint32_t _counts[search::kWindowDocs]{};
  Emit<Table> _emit;
  Leaves _leaves;
  doc_id_t _min = 0;
  uint32_t _min_match;
};

}  // namespace irs::docs
