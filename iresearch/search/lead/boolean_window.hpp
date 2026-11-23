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
#include <cstdint>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

#include "iresearch/search/detail/boolean_window_base.hpp"
#include "iresearch/search/detail/window.hpp"
#include "iresearch/search/scorers/make_window.hpp"
#include "iresearch/search/scorers/score_args.hpp"
#include "iresearch/search/scorers/score_function.hpp"
#include "iresearch/utils/bit_utils.hpp"
#include "iresearch/utils/empty.hpp"
#include "iresearch/utils/shared.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::lead {

template<typename Lead, typename Others, typename Optional, typename Excludes,
         typename Score = utils::Empty>
class BooleanWindow
  : public detail::BooleanWindowBase<
      BooleanWindow<Lead, Others, Optional, Excludes, Score>, Score,
      !std::is_same_v<Score, utils::Empty>,
      !std::is_same_v<Score, utils::Empty> && !detail::LazyReset<Optional>()>,
    detail::WindowClauses<Lead, Others, Optional, Excludes> {
  using Base = detail::BooleanWindowBase<
    BooleanWindow, Score, !std::is_same_v<Score, utils::Empty>,
    !std::is_same_v<Score, utils::Empty> && !detail::LazyReset<Optional>()>;
  using Clauses = detail::WindowClauses<Lead, Others, Optional, Excludes>;
  using Base::_doc;
  using Base::_filled;
  using Base::_gathered;
  using Base::_min;
  using Base::_window;
  using Clauses::_optional;
  using Clauses::FillClauses;

 public:
  using Clauses::kExcludes;
  using Clauses::kLead;
  using Clauses::kOptional;
  using Clauses::kOthers;
  static constexpr bool kScored = !std::is_same_v<Score, utils::Empty>;
  static_assert(kLead != kOptional);
  static_assert(!kScored || (kOptional && !kExcludes));

  template<typename LeadArgs, typename OthersArgs, typename OptionalArgs,
           typename ExcludesArgs>
  BooleanWindow(std::piecewise_construct_t, LeadArgs&& lead,
                OthersArgs&& others, OptionalArgs&& optional,
                ExcludesArgs&& excludes, Score score = {})
    : Base{score},
      Clauses{std::piecewise_construct, std::forward<LeadArgs>(lead),
              std::forward<OthersArgs>(others),
              std::forward<OptionalArgs>(optional),
              std::forward<ExcludesArgs>(excludes)} {}

  doc_id_t Next() { return Seek(_doc + 1); }

  doc_id_t Seek(doc_id_t target) {
    if (target <= _doc) {
      return _doc;
    }
    return _doc = this->From(target);
  }

  IRS_FORCE_INLINE void FetchScoreArgs(uint32_t slot)
    requires kScored
  {
    SDB_ASSERT(slot < kScoreBlock);
    _gathered[slot] = _filled ? _window[_doc - _min] : score_t{0};
  }

  doc_id_t FillWindow(doc_id_t min, doc_id_t max, uint64_t* words) {
    if constexpr (kScored) {
      return _optional.Fill(min, max, words, _window);
    } else {
      return FillClauses(min, max, words);
    }
  }
};

}  // namespace irs::lead
