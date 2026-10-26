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

#include <utility>

#include "iresearch/utils/containers/fixed.hpp"
#include "iresearch/utils/shared.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::detail {

template<typename Leaf, size_t N = 0>
class ConjunctionLeaves {
 public:
  template<typename Estimate, typename Make, typename Emit>
  ConjunctionLeaves(size_t size, Estimate&& estimate, Make&& make, Emit&& emit)
    : ConjunctionLeaves{SortBy(Order{size,
                                     [](uint32_t& j, size_t i) noexcept {
                                       j = static_cast<uint32_t>(i);
                                     }},
                               estimate),
                        make, emit} {}

  ConjunctionLeaves(ConjunctionLeaves&&) = delete;
  ConjunctionLeaves& operator=(ConjunctionLeaves&&) = delete;

  size_t Size() const noexcept { return _leaves.size(); }

  Leaf* begin() noexcept { return _leaves.begin(); }

  Leaf* end() noexcept { return _leaves.end(); }

  doc_id_t Seek(doc_id_t from) { return Agree(Lead().Seek(from)); }

  doc_id_t Next() { return Agree(Lead().Next()); }

  IRS_FORCE_INLINE doc_id_t ProbeRest(doc_id_t target) {
    for (auto it = _leaves.begin() + 1, end = _leaves.end(); it != end; ++it) {
      if (const auto probe = it->Probe(target); probe != target) {
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

  Leaf& Lead() noexcept { return _leaves.front(); }

 private:
  using Order = containers::Fixed<uint32_t, N>;

  template<typename Estimate>
  static const Order& SortBy(Order&& order, Estimate& estimate) {
    absl::c_sort(order, [&](uint32_t lhs, uint32_t rhs) {
      return estimate(lhs) < estimate(rhs);
    });
    return order;
  }

  template<typename Make, typename Emit>
  ConjunctionLeaves(const Order& order, Make& make, Emit& emit)
    : _leaves{order.size(), std::piecewise_construct,
              [&](size_t i) { return make(order[i]); }} {
    SDB_ASSERT(!_leaves.empty());
    for (size_t i = 0; i != _leaves.size(); ++i) {
      emit(order[i], _leaves[i]);
    }
  }

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

  containers::Fixed<Leaf, N> _leaves;
};

}  // namespace irs::detail
