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
#include <limits>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

#include "iresearch/search/detail/boolean_window_base.hpp"
#include "iresearch/search/detail/window.hpp"
#include "iresearch/search/scorers/make_window.hpp"
#include "iresearch/search/scorers/score_args.hpp"
#include "iresearch/search/scorers/score_function.hpp"
#include "iresearch/search/scorers/score_policy.hpp"
#include "iresearch/utils/bit_utils.hpp"
#include "iresearch/utils/empty.hpp"
#include "iresearch/utils/shared.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::probe {

template<typename Optional, typename Score = utils::Empty, bool Bounded = false>
class BooleanWindow
  : public detail::BooleanWindowBase<BooleanWindow<Optional, Score, Bounded>,
                                     Score,
                                     std::is_same_v<Score, detail::Scored>,
                                     std::is_same_v<Score, detail::Scored>> {
  using Base = detail::BooleanWindowBase<BooleanWindow, Score,
                                         std::is_same_v<Score, detail::Scored>,
                                         std::is_same_v<Score, detail::Scored>>;
  using Base::_doc;
  using Base::_filled;
  using Base::_gathered;
  using Base::_min;
  using Base::_spent;
  using Base::_window;

 public:
  static constexpr bool kScored = std::is_same_v<Score, detail::Scored>;
  static constexpr bool kBounded = Bounded;
  static_assert(!detail::Retracts<Optional>());
  static_assert(!kBounded || kScored);

  template<typename OptionalArgs>
  BooleanWindow(std::piecewise_construct_t, OptionalArgs&& optional,
                Score score = {})
    : Base{score},
      _optional{
        std::make_from_tuple<Optional>(std::forward<OptionalArgs>(optional))} {}

  template<typename OptionalArgs>
  BooleanWindow(std::piecewise_construct_t, OptionalArgs&& optional,
                Score score, score_t bound)
    requires kBounded
    : Base{score},
      _optional{
        std::make_from_tuple<Optional>(std::forward<OptionalArgs>(optional))},
      _bound{bound} {}

  BooleanWindow(BooleanWindow&&) = delete;
  BooleanWindow& operator=(BooleanWindow&&) = delete;

  doc_id_t Probe(doc_id_t target) {
    if (target <= _doc) {
      return _doc;
    }
    return _doc = this->From(target);
  }

  doc_id_t AdvanceBlock(doc_id_t target)
    requires kBounded
  {
    if (doc_limits::eof(target)) {
      return doc_limits::eof();
    }
    if (!_filled || target >= _min + detail::kWindowDocs) {
      if (_spent) {
        return doc_limits::eof();
      }
      this->Refill(target);
    }
    return _min + (detail::kWindowDocs - 1);
  }

  score_t MaxScore(doc_id_t last) const noexcept
    requires kBounded
  {
    if (_filled && last < _min + detail::kWindowDocs) {
      return _max;
    }
    return _spent ? score_t{0} : _bound;
  }

  IRS_FORCE_INLINE void FetchScoreArgs(uint32_t slot)
    requires kScored
  {
    SDB_ASSERT(slot < kScoreBlock);
    const auto offset = _doc - _min;
    _gathered[slot] =
      offset < detail::kWindowDocs ? _window[offset] : score_t{0};
  }

  doc_id_t FillWindow(doc_id_t min, doc_id_t max, uint64_t* words) {
    if constexpr (kScored) {
      const auto next = _optional.Fill(min, max, words, _window);
      if constexpr (kBounded) {
        score_t top = 0;
        for (uint32_t w = 0; w != detail::kWindowWords; ++w) {
          auto word = words[w];
          const auto base = w * detail::kWindowBits;
          while (word != 0) {
            top = std::max(top, _window[base + std::countr_zero(word)]);
            word = PopBit(word);
          }
        }
        _max = top;
      }
      return next;
    } else {
      return _optional.Fill(min, max, words);
    }
  }

 private:
  Optional _optional;
  [[no_unique_address]] utils::Need<kBounded, score_t> _bound =
    std::numeric_limits<score_t>::max();
  [[no_unique_address]] utils::Need<kBounded, score_t> _max = 0;
};

}  // namespace irs::probe
