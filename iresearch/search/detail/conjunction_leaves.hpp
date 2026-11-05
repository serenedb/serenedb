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

#ifdef __AVX2__
#include <immintrin.h>
#endif

#include <absl/algorithm/container.h>

#include <bit>
#include <span>
#include <utility>

#include "iresearch/utils/containers/fixed.hpp"
#include "iresearch/utils/empty.hpp"
#include "iresearch/utils/shared.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::detail {

inline constexpr size_t kMergeBlock = 8;

inline IRS_FORCE_INLINE uint32_t CommonLanes(const doc_id_t* a,
                                             const doc_id_t* b) noexcept {
#ifdef __AVX2__
  const auto lanes = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(a));
  auto common =
    _mm256_cmpeq_epi32(lanes, _mm256_set1_epi32(static_cast<int32_t>(b[0])));
  for (size_t k = 1; k != kMergeBlock; ++k) {
    common = _mm256_or_si256(
      common,
      _mm256_cmpeq_epi32(lanes, _mm256_set1_epi32(static_cast<int32_t>(b[k]))));
  }
  return static_cast<uint32_t>(_mm256_movemask_ps(_mm256_castsi256_ps(common)));
#else
  uint32_t common = 0;
  for (size_t k = 0; k != kMergeBlock; ++k) {
    for (size_t l = 0; l != kMergeBlock; ++l) {
      common |= static_cast<uint32_t>(a[l] == b[k]) << l;
    }
  }
  return common;
#endif
}

inline doc_id_t FirstCommon(std::span<const doc_id_t> a,
                            std::span<const doc_id_t> b) noexcept {
  SDB_ASSERT(!b.empty());
  size_t i = 0;
  size_t j = 0;
  while (i + kMergeBlock <= a.size() && j + kMergeBlock <= b.size()) {
    if (const auto common = CommonLanes(a.data() + i, b.data() + j);
        common != 0) [[unlikely]] {
      return a[i + std::countr_zero(common)];
    }
    const auto a_last = a[i + kMergeBlock - 1];
    const auto b_last = b[j + kMergeBlock - 1];
    i += kMergeBlock * static_cast<size_t>(a_last < b_last);
    j += kMergeBlock * static_cast<size_t>(b_last < a_last);
  }
  return j < b.size() ? b[j] : b.back() + 1;
}

template<typename Leaf, size_t N = 0>
class ConjunctionLeaves {
  static constexpr bool kDeferrable =
    requires(Leaf& leaf) { leaf.ProbeDeferred(doc_id_t{}); };
  static constexpr bool kMergeable = requires(Leaf& leaf) { leaf.Onward(); };

 public:
  template<typename Estimate, typename Make, typename Emit>
  ConjunctionLeaves(size_t size, Estimate&& estimate, Make&& make, Emit&& emit)
    : ConjunctionLeaves{SortBy(Order{size,
                                     [](uint32_t& j, size_t i) noexcept {
                                       j = static_cast<uint32_t>(i);
                                     }},
                               estimate),
                        make, emit} {}

  template<typename Estimate, typename Make, typename Emit>
  ConjunctionLeaves(std::span<const uint32_t> indices, Estimate&& estimate,
                    Make&& make, Emit&& emit)
    : ConjunctionLeaves{
        SortBy(Order{indices.size(),
                     [&](uint32_t& j, size_t i) noexcept { j = indices[i]; }},
               estimate),
        make, emit} {}

  ConjunctionLeaves(ConjunctionLeaves&&) = delete;
  ConjunctionLeaves& operator=(ConjunctionLeaves&&) = delete;

  size_t Size() const noexcept { return _leaves.size(); }

  Leaf* begin() noexcept { return _leaves.begin(); }

  Leaf* end() noexcept { return _leaves.end(); }

  doc_id_t Seek(doc_id_t from) { return Agree(Lead().Seek(from)); }

  doc_id_t Next() { return Agree(Lead().Next()); }

  template<typename Consult>
  doc_id_t Next(doc_id_t end, Consult& consult) {
    return Agree(Lead().Next(), end, consult);
  }

  IRS_FORCE_INLINE doc_id_t ProbeRest(doc_id_t target) {
    if constexpr (kDeferrable && N != 0) {
      return [&]<size_t... I>(std::index_sequence<I...>) IRS_FORCE_INLINE {
        auto probe = target;
        static_cast<void>((
          ((probe = ProbeOf(_leaves[I + 1], target, I + 1)) == target) && ...));
        return probe;
      }(std::make_index_sequence<N - 1>{});
    } else {
      auto* const leaves = _leaves.begin();
      for (size_t i = 1, size = _leaves.size(); i != size; ++i) {
        if (const auto probe = ProbeOf(leaves[i], target, i); probe != target) {
          return probe;
        }
      }
      return target;
    }
  }

  void DeferFrom(size_t first)
    requires kDeferrable
  {
    _deferred = static_cast<uint32_t>(first);
  }

  Leaf* Deferred() noexcept {
    if constexpr (kDeferrable) {
      return _leaves.begin() + _deferred;
    } else {
      return _leaves.end();
    }
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

  IRS_FORCE_INLINE doc_id_t ProbeOf(Leaf& leaf, doc_id_t doc, size_t i) {
    if constexpr (kDeferrable) {
      if (i >= _deferred && leaf.Deferred()) [[unlikely]] {
        return leaf.ProbeDeferred(doc);
      }
    }
    return leaf.Probe(doc);
  }

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
    if constexpr (kDeferrable) {
      _deferred = static_cast<uint32_t>(_leaves.size());
    }
    if constexpr (kMergeable) {
      _merges =
        _leaves.size() > 1 &&
        _leaves[1].Estimate() < kMergeBlock * uint64_t{Lead().Estimate()};
    }
  }

  doc_id_t Agree(doc_id_t doc) {
    while (!doc_limits::eof(doc)) {
      const auto probe = ProbeRest(doc);
      if (probe == doc) {
        return doc;
      }
      doc = Pass(probe);
    }
    return doc;
  }

  IRS_FORCE_INLINE doc_id_t Pass(doc_id_t probe) {
    if constexpr (kMergeable) {
      if (_merges) {
        if (const auto& next = _leaves[1];
            next.Value() == probe && !doc_limits::eof(probe)) {
          return Lead().Probe(FirstCommon(Lead().Upcoming(), next.Onward()));
        }
      }
    }
    return Lead().Seek(probe);
  }

  template<typename Consult>
  doc_id_t Agree(doc_id_t doc, doc_id_t end, Consult& consult) {
    const auto size = _leaves.size();
    while (doc < end) {
      size_t i = 1;
      for (; i != size; ++i) {
        auto& leaf = _leaves[i];
        const bool consulted = !leaf.Covers(doc);
        if (consulted && !consult.Admits(doc, i)) {
          doc = Lead().Next();
          break;
        }
        const auto probe = ProbeOf(leaf, doc, i);
        if (consulted) {
          consult.Probed(i, probe == doc);
        }
        if (probe != doc) {
          doc = Pass(probe);
          break;
        }
      }
      if (i == size) {
        return doc;
      }
    }
    return doc;
  }

  containers::Fixed<Leaf, N> _leaves;
  [[no_unique_address]] utils::Need<kDeferrable, uint32_t> _deferred{};
  [[no_unique_address]] utils::Need<kMergeable, bool> _merges{};
};

}  // namespace irs::detail
