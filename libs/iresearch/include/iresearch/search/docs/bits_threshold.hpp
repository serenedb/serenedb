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
#include <vector>

#include "iresearch/search/common/window.hpp"
#include "iresearch/search/docs/emit.hpp"
#include "iresearch/search/docs/root.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::docs {

template<typename Leaves, typename Table>
class BitsThreshold : public Root {
 public:
  template<typename LeavesArgs>
  BitsThreshold(Table table, std::piecewise_construct_t, LeavesArgs&& leaves,
                uint32_t min_match)
    : _emit{table},
      _leaves{std::make_from_tuple<Leaves>(std::forward<LeavesArgs>(leaves))},
      _planes(size_t{min_match} * search::kWindowWords, 0),
      _min_match{min_match} {
    SDB_ASSERT(_min_match > 1);
  }

  uint32_t Run(doc_id_t* IRS_RESTRICT out, uint32_t capacity) final {
    SDB_ASSERT(capacity >= doc_limits::kMinCapacity);
    uint32_t n = 0;
    auto* const planes = _planes.data();
    const auto top = size_t{_min_match} - 1;

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

      std::fill(_planes.begin(), _planes.end(), uint64_t{0});
      bool first = true;
      const auto next = _leaves.Visit(max, [&](auto& leaf) {
        if (first) {
          first = false;
          return leaf.FillOr(_min, max, planes);
        }
        search::Clear(_window.data(), search::kWindowWords);
        const auto doc = leaf.FillOr(_min, max, _window.data());
        search::FoldCarry(planes, _window.data(), search::kWindowWords, top);
        return doc;
      });

      std::copy_n(planes + top * search::kWindowWords, search::kWindowWords,
                  _emit.Mask());
      _emit.Opened(_min);
      _min = next;
    }
  }

 private:
  Emit<Table> _emit;
  search::Scratch _window{};
  Leaves _leaves;
  std::vector<uint64_t> _planes;
  doc_id_t _min = 0;
  uint32_t _min_match;
};

}  // namespace irs::docs
