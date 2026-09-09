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

#include "iresearch/search/common/window.hpp"
#include "iresearch/search/docs/emit.hpp"
#include "iresearch/search/docs/root.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::docs {

template<typename Leaves, typename Table>
class Complement : public Root {
 public:
  template<typename LeavesArgs>
  Complement(Table table, std::piecewise_construct_t, LeavesArgs&& leaves,
             doc_id_t count)
    : _emit{table},
      _leaves{std::make_from_tuple<Leaves>(std::forward<LeavesArgs>(leaves))},
      _end{doc_limits::min() + count} {}

  uint32_t Run(doc_id_t* IRS_RESTRICT out, uint32_t capacity) final {
    SDB_ASSERT(capacity >= doc_limits::kMinCapacity);
    uint32_t n = 0;

    for (;;) {
      if (!_emit.Drain(out, capacity, n)) {
        return n;
      }
      if (!_emit.Skip(_min)) {
        _min = _end;
      }
      if (_min >= _end || n == capacity) {
        return n;
      }
      const doc_id_t max = _min + search::kWindowDocs;
      if (!_leaves.Empty()) {
        _leaves.Visit(max, [&](auto& leaf) {
          return leaf.FillOr(_min, max, _excluded.data());
        });
      }
      auto* const mask = _emit.Mask();
      auto base = _min;
      for (size_t w = 0; w != search::kWindowWords;
           ++w, base += search::kWindowBits) {
        auto word = ~_excluded[w];
        _excluded[w] = 0;
        if (base + search::kWindowBits > _end) {
          const auto keep = _end > base ? _end - base : 0;
          word &= keep >= search::kWindowBits ? ~uint64_t{0}
                                              : (uint64_t{1} << keep) - 1;
        }
        mask[w] = word;
      }
      _emit.Opened(_min);
      _min = max;
    }
  }

 private:
  Emit<Table> _emit;
  search::Scratch _excluded{};
  Leaves _leaves;
  doc_id_t _min = doc_limits::min();
  doc_id_t _end;
};

}  // namespace irs::docs
