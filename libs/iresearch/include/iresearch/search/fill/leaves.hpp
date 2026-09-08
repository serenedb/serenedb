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
#include <bit>
#include <tuple>
#include <utility>
#include <vector>

#include "basics/bit_utils.hpp"
#include "iresearch/search/common/fixed_array.hpp"
#include "iresearch/search/common/window.hpp"
#include "iresearch/search/fill/concept.hpp"
#include "iresearch/search/probe/concept.hpp"
#include "iresearch/types.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::fill {

template<Producer Leaf, size_t N = 0>
class AndLeaves {
 public:
  template<typename Init>
  AndLeaves(size_t size, Init&& init)
    : _leaves{size, std::forward<Init>(init)} {
    SDB_ASSERT(!_leaves.empty());
  }

  doc_id_t Restrict(doc_id_t min, doc_id_t max, uint64_t* IRS_RESTRICT mask) {
    const auto words = search::WindowWords(min, max);
    const auto last = _leaves.size() - 1;
    doc_id_t next = 0;
    for (size_t i = 0;; ++i) {
      search::Clear(_own.data(), words);
      next = std::max(next, _leaves[i].FillOr(min, max, _own.data()));
      if (search::FoldAnd(mask, _own.data(), words) == 0 || i == last) {
        return next;
      }
    }
  }

 private:
  search::Scratch _own;
  search::RunOf<Leaf, N> _leaves;
};

template<probe::Type Probe>
class ProbedAndNot {
 public:
  template<typename Args>
  ProbedAndNot(std::piecewise_construct_t, Args&& probe)
    : _probe{std::make_from_tuple<Probe>(std::forward<Args>(probe))} {}

  ProbedAndNot(ProbedAndNot&&) = delete;
  ProbedAndNot& operator=(ProbedAndNot&&) = delete;

  void Remove(doc_id_t min, doc_id_t max, uint64_t* IRS_RESTRICT mask) {
    const auto words = search::WindowWords(min, max);
    auto base = min;
    for (size_t w = 0; w != words; ++w, base += search::kWindowBits) {
      auto word = mask[w];
      auto live = word;
      while (word != 0) {
        const auto bit = static_cast<uint32_t>(std::countr_zero(word));
        const auto doc = base + bit;
        if (_probe.Probe(doc) == doc) {
          live &= ~(uint64_t{1} << bit);
        }
        word = PopBit(word);
      }
      mask[w] = live;
    }
  }

  void Remove(doc_id_t min, doc_id_t max, uint64_t* IRS_RESTRICT mask,
              score_t* IRS_RESTRICT scores, score_t reset) {
    const auto words = search::WindowWords(min, max);
    auto base = min;
    for (size_t w = 0; w != words; ++w, base += search::kWindowBits) {
      auto word = mask[w];
      auto live = word;
      while (word != 0) {
        const auto bit = static_cast<uint32_t>(std::countr_zero(word));
        const auto doc = base + bit;
        if (_probe.Probe(doc) == doc) {
          live &= ~(uint64_t{1} << bit);
          scores[w * search::kWindowBits + bit] = reset;
        }
        word = PopBit(word);
      }
      mask[w] = live;
    }
  }

 private:
  Probe _probe;
};

}  // namespace irs::fill
