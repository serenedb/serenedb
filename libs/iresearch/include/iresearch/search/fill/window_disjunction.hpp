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

#include <bit>
#include <cstdint>
#include <utility>

#include "basics/bit_utils.hpp"
#include "basics/empty.hpp"
#include "basics/shared.hpp"
#include "iresearch/search/common/window.hpp"
#include "iresearch/search/scorer.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::fill {

template<typename Leaves>
class WindowDisjunctionDocs {
 public:
  template<typename LeavesArgs>
  WindowDisjunctionDocs(std::piecewise_construct_t, LeavesArgs&& leaves)
    : _leaves{std::make_from_tuple<Leaves>(std::forward<LeavesArgs>(leaves))} {}

  doc_id_t FillOr(doc_id_t min, doc_id_t max, uint64_t* IRS_RESTRICT mask) {
    if (_leaves.Empty()) {
      return doc_limits::eof();
    }
    return _leaves.Visit(
      max, [&](auto& leaf) { return leaf.FillOr(min, max, mask); });
  }

 private:
  Leaves _leaves;
};

template<typename Leaves, bool HasConst = false>
class WindowDisjunctionScored {
 public:
  template<typename LeavesArgs>
  WindowDisjunctionScored(std::piecewise_construct_t, LeavesArgs&& leaves,
                          ScoreMergeType merge, score_t absorbed = 0)
    : _leaves{std::make_from_tuple<Leaves>(std::forward<LeavesArgs>(leaves))},
      _absorbed{absorbed},
      _inner{merge} {}

  doc_id_t Fill(doc_id_t min, doc_id_t max, uint64_t* IRS_RESTRICT mask,
                score_t* IRS_RESTRICT scores) {
    if (_leaves.Empty()) {
      return doc_limits::eof();
    }
    if constexpr (!HasConst) {
      return _leaves.Visit(
        max, [&](auto& leaf) { return leaf.Fill(min, max, mask, scores); });
    } else {
      const auto next = _leaves.Visit(max, [&](auto& leaf) {
        return leaf.Fill(min, max, _own.data(), scores);
      });
      const auto words = search::WindowWords(min, max);
      irs::ResolveMergeType(_inner, [&]<ScoreMergeType Inner> {
        Absorb<Inner>(mask, scores, words);
      });
      return next;
    }
  }

 private:
  template<ScoreMergeType Inner>
  IRS_FORCE_INLINE void Absorb(uint64_t* IRS_RESTRICT mask,
                               score_t* IRS_RESTRICT scores,
                               size_t words) noexcept {
    for (size_t w = 0; w != words; ++w) {
      auto bits = std::exchange(_own[w], uint64_t{0});
      mask[w] |= bits;
      const size_t base = w * search::kWindowBits;
      while (bits != 0) {
        irs::Merge<Inner>(
          scores[base + static_cast<uint32_t>(std::countr_zero(bits))],
          _absorbed);
        bits = PopBit(bits);
      }
    }
  }

  [[no_unique_address]] utils::Need<HasConst, search::Scratch> _own{};
  Leaves _leaves;
  [[no_unique_address]] utils::Need<HasConst, score_t> _absorbed;
  [[no_unique_address]] utils::Need<HasConst, ScoreMergeType> _inner;
};

}  // namespace irs::fill
