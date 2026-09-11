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

#include <absl/base/optimization.h>

#include <algorithm>
#include <bit>
#include <cstdint>
#include <tuple>
#include <type_traits>
#include <utility>

#include "basics/bit_utils.hpp"
#include "basics/empty.hpp"
#include "iresearch/search/scorers/score_policy.hpp"
#include "iresearch/search/detail/window.hpp"
#include "iresearch/search/scorers/scorer.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::fill {

template<typename Lead, typename Others, typename Optional, typename Excludes,
         typename Score = utils::Empty>
class BooleanWindow {
 public:
  static constexpr bool kLead = !std::is_same_v<Lead, utils::Empty>;
  static constexpr bool kOthers = !std::is_same_v<Others, utils::Empty>;
  static constexpr bool kOptional = !std::is_same_v<Optional, utils::Empty>;
  static constexpr bool kExcludes = !std::is_same_v<Excludes, utils::Empty>;
  static constexpr bool kScored = std::is_same_v<Score, detail::Scored>;
  static constexpr bool kRetracts = kScored && detail::Retracts<Optional>();
  static_assert(kLead != kOptional);
  static_assert(kLead || !kOthers);
  static_assert(!kScored || (kOptional && !kExcludes));

  template<typename LeadArgs, typename OthersArgs, typename OptionalArgs,
           typename ExcludesArgs>
  BooleanWindow(std::piecewise_construct_t, LeadArgs&& lead,
                OthersArgs&& others, OptionalArgs&& optional,
                ExcludesArgs&& excludes, Score score = {})
    : _lead{std::make_from_tuple<Lead>(std::forward<LeadArgs>(lead))},
      _others{std::make_from_tuple<Others>(std::forward<OthersArgs>(others))},
      _optional{
        std::make_from_tuple<Optional>(std::forward<OptionalArgs>(optional))},
      _excludes{
        std::make_from_tuple<Excludes>(std::forward<ExcludesArgs>(excludes))},
      _score{score} {
    if constexpr (kRetracts) {
      std::fill_n(_scores, detail::kWindowDocs, _score.absorbed);
    }
  }

  BooleanWindow(BooleanWindow&&) = delete;
  BooleanWindow& operator=(BooleanWindow&&) = delete;

  doc_id_t FillOr(doc_id_t min, doc_id_t max, uint64_t* IRS_RESTRICT mask)
    requires(!kScored)
  {
    if constexpr (kOptional && !kExcludes) {
      return _optional.Fill(min, max, mask);
    } else {
      const auto words = detail::WindowWords(min, max);
      const auto next = Compute(min, max, words);
      for (size_t w = 0; w != words; ++w) {
        mask[w] |= _own[w];
      }
      return next;
    }
  }

  doc_id_t FillAnd(doc_id_t min, doc_id_t max, uint64_t* IRS_RESTRICT mask)
    requires(!kScored)
  {
    const auto words = detail::WindowWords(min, max);
    const auto next = Compute(min, max, words);
    detail::FoldAnd(mask, _own.data(), words);
    return next;
  }

  doc_id_t FillAndNot(doc_id_t min, doc_id_t max, uint64_t* IRS_RESTRICT mask)
    requires(!kScored)
  {
    const auto words = detail::WindowWords(min, max);
    const auto next = Compute(min, max, words);
    detail::FoldAndNot(mask, _own.data(), words);
    return next;
  }

  doc_id_t Fill(doc_id_t min, doc_id_t max, uint64_t* IRS_RESTRICT mask,
                score_t* IRS_RESTRICT scores)
    requires kScored
  {
    if constexpr (kRetracts) {
      const auto next = _optional.Fill(min, max, _own.data(), _scores);
      const auto words = detail::WindowWords(min, max);
      irs::ResolveMergeType(_score.inner, [&]<ScoreMergeType Inner> {
        Fold<Inner>(mask, scores, words);
      });
      return next;
    } else {
      if (_score.absorbed == 0) {
        return _optional.Fill(min, max, mask, scores);
      }
      const auto next = _optional.Fill(min, max, _own.data(), scores);
      const auto words = detail::WindowWords(min, max);
      irs::ResolveMergeType(_score.inner, [&]<ScoreMergeType Inner> {
        Absorb<Inner>(mask, scores, words);
      });
      return next;
    }
  }

 private:
  doc_id_t Compute(doc_id_t min, doc_id_t max, size_t words) {
    auto* const own = _own.data();
    detail::Clear(own, words);
    doc_id_t next;
    if constexpr (kLead) {
      next = _lead.FillOr(min, max, own);
      if constexpr (kOthers) {
        next = std::max(next, _others.Restrict(min, max, own));
      }
    } else {
      next = _optional.Fill(min, max, own);
    }
    if constexpr (kExcludes) {
      _excludes.Remove(min, max, own);
    }
    return next;
  }

  template<ScoreMergeType Inner>
  IRS_FORCE_INLINE void Absorb(uint64_t* IRS_RESTRICT mask,
                               score_t* IRS_RESTRICT scores,
                               size_t words) noexcept {
    for (size_t w = 0; w != words; ++w) {
      auto bits = std::exchange(_own[w], uint64_t{0});
      mask[w] |= bits;
      const size_t base = w * detail::kWindowBits;
      while (bits != 0) {
        irs::Merge<Inner>(
          scores[base + static_cast<uint32_t>(std::countr_zero(bits))],
          _score.absorbed);
        bits = PopBit(bits);
      }
    }
  }

  template<ScoreMergeType Inner>
  IRS_FORCE_INLINE void Fold(uint64_t* IRS_RESTRICT mask,
                             score_t* IRS_RESTRICT scores,
                             size_t words) noexcept
    requires kRetracts
  {
    for (size_t w = 0; w != words; ++w) {
      auto bits = std::exchange(_own[w], uint64_t{0});
      mask[w] |= bits;
      const size_t base = w * detail::kWindowBits;
      while (bits != 0) {
        const auto offset =
          base + static_cast<uint32_t>(std::countr_zero(bits));
        irs::Merge<Inner>(scores[offset], _scores[offset]);
        if constexpr (!detail::LazyReset<Optional>()) {
          _scores[offset] = _score.absorbed;
        }
        bits = PopBit(bits);
      }
    }
  }

  detail::Scratch _own{};
  [[no_unique_address]] ABSL_CACHELINE_ALIGNED
    utils::Need<kRetracts, score_t[detail::kWindowDocs]> _scores{};
  [[no_unique_address]] Lead _lead;
  [[no_unique_address]] Others _others;
  [[no_unique_address]] Optional _optional;
  [[no_unique_address]] Excludes _excludes;
  [[no_unique_address]] Score _score;
};

}  // namespace irs::fill
