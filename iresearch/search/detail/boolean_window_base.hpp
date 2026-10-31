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
#include <cstdint>
#include <type_traits>
#include <vector>

#include "iresearch/search/detail/window.hpp"
#include "iresearch/search/scorers/make_window.hpp"
#include "iresearch/search/scorers/score_function.hpp"
#include "iresearch/utils/empty.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::detail {

template<typename Lead, typename Others, typename Optional, typename Excludes>
IRS_FORCE_INLINE doc_id_t FillClauses(Lead& lead, Others& others,
                                      Optional& optional, Excludes& excludes,
                                      doc_id_t min, doc_id_t max,
                                      uint64_t* IRS_RESTRICT words) {
  doc_id_t next;
  if constexpr (!std::is_same_v<Lead, utils::Empty>) {
    next = lead.FillOr(min, max, words);
    if constexpr (!std::is_same_v<Others, utils::Empty>) {
      next = std::max(next, others.Restrict(min, max, words));
    }
  } else {
    next = optional.Fill(min, max, words);
  }
  if constexpr (!std::is_same_v<Excludes, utils::Empty>) {
    excludes.Remove(min, max, words);
  }
  return next;
}

template<typename Derived, typename Score, bool Scored, bool ZeroWindow>
class BooleanWindowBase {
 public:
  explicit BooleanWindowBase(Score score) noexcept : _score{score} {}

  ScoreFunction PrepareScore()
    requires Scored
  {
    return MakeWindowScore(_score.inner, _gathered, _score.absorbed);
  }

  void CollectScorers(std::vector<ScoreFunction>& out)
    requires Scored
  {
    AppendScorer(out, PrepareScore());
  }

 protected:
  doc_id_t From(doc_id_t target) {
    if (doc_limits::eof(target)) {
      return doc_limits::eof();
    }
    for (;;) {
      if (!_filled || target >= _min + kWindowDocs) {
        if (_spent) {
          return doc_limits::eof();
        }
        Refill(target);
      }
      if (const auto found = FindSet(_mask.data(), target - _min);
          found != kWindowDocs) {
        return _min + found;
      }
      if (_spent) {
        return doc_limits::eof();
      }
      if (!NextWindow(_min, _next, target)) {
        return doc_limits::eof();
      }
    }
  }

  void Refill(doc_id_t target) {
    SDB_ASSERT(!_filled || target >= _min);
    auto* const words = _mask.data();
    if constexpr (ZeroWindow) {
      ClearScored(words, _window);
    } else {
      Clear(words, kWindowWords);
    }
    _min = target;
    _filled = true;
    _next =
      static_cast<Derived&>(*this).FillWindow(_min, _min + kWindowDocs, words);
    _spent = doc_limits::eof(_next);
  }

  Scratch _mask{};
  [[no_unique_address]] ABSL_CACHELINE_ALIGNED
    utils::Need<Scored, score_t[kWindowDocs]> _window{};
  [[no_unique_address]] ABSL_CACHELINE_ALIGNED
    utils::Need<Scored, score_t[kScoreBlock]> _gathered{};
  doc_id_t _min = 0;
  doc_id_t _next = doc_limits::eof();
  doc_id_t _doc = doc_limits::invalid();
  bool _filled = false;
  bool _spent = false;
  [[no_unique_address]] Score _score;
};

}  // namespace irs::detail
