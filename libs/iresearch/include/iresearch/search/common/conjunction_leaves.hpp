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

#include <absl/algorithm/container.h>

#include <algorithm>
#include <array>
#include <numeric>
#include <span>
#include <type_traits>
#include <vector>

#include "basics/shared.hpp"
#include "iresearch/formats/posting_meta.hpp"
#include "iresearch/search/common/fixed_array.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::search {

template<typename Leaf, size_t N = 0>
class ConjunctionLeaves {
 public:
  struct Slot {
    Leaf leaf;
    uint32_t order = 0;
  };

  using Slots = RunOf<Slot, N>;

  explicit ConjunctionLeaves(size_t size) : _slots(size) {
    SDB_ASSERT(size > 1);
  }

  ConjunctionLeaves(ConjunctionLeaves&&) = delete;
  ConjunctionLeaves& operator=(ConjunctionLeaves&&) = delete;

  template<typename Meta, typename Prepare, typename Emit>
  void Open(std::span<Meta> metas, Prepare&& prepare, Emit&& emit) {
    const auto at = [metas](uint32_t i) -> const PostingMeta& {
      if constexpr (std::is_pointer_v<std::remove_const_t<Meta>>) {
        return *metas[i];
      } else {
        return metas[i];
      }
    };
    SDB_ASSERT(metas.size() == _slots.size());
    RunOf<uint32_t, N> order{metas.size(),
                             [](uint32_t& slot, size_t i) noexcept {
                               slot = static_cast<uint32_t>(i);
                             }};
    absl::c_sort(order, [&](uint32_t lhs, uint32_t rhs) {
      return at(lhs).docs_count < at(rhs).docs_count;
    });

    for (size_t i = 0; i != _slots.size(); ++i) {
      const auto j = order[i];
      _slots[i].order = j;
      prepare(_slots[i].leaf, at(j));
      emit(j, _slots[i]);
    }
  }

  size_t Size() const noexcept { return _slots.size(); }

  doc_id_t Seek(doc_id_t from) { return Agree(Lead().Seek(from)); }

  doc_id_t Next() { return Agree(Lead().Advance()); }

  IRS_FORCE_INLINE doc_id_t ProbeRest(doc_id_t target) {
    for (auto it = _slots.begin() + 1, end = _slots.end(); it != end; ++it) {
      if (const auto probe = it->leaf.Probe(target); probe != target) {
        return probe;
      }
    }
    return target;
  }

  IRS_FORCE_INLINE doc_id_t Probe(doc_id_t target) {
    if (const auto probe = Lead().Probe(target); probe != target) {
      return probe;
    }
    return ProbeRest(target);
  }

 private:
  Leaf& Lead() noexcept { return _slots.front().leaf; }

  doc_id_t Agree(doc_id_t doc) {
    while (!doc_limits::eof(doc)) {
      const auto probe = ProbeRest(doc);
      if (probe == doc) {
        return doc;
      }
      doc = Lead().Seek(probe);
    }
    return doc;
  }

  Slots _slots;
};

}  // namespace irs::search
