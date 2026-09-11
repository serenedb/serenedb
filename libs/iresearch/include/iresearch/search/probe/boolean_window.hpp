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

#include "basics/bit_utils.hpp"
#include "basics/empty.hpp"
#include "basics/shared.hpp"
#include "iresearch/search/scorers/make_window.hpp"
#include "iresearch/search/scorers/score_args.hpp"
#include "iresearch/search/scorers/score_policy.hpp"
#include "iresearch/search/detail/window.hpp"
#include "iresearch/search/scorers/score_function.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::probe {

template<typename Optional, typename Score = utils::Empty, bool Bounded = false>
class BooleanWindow {
 public:
  static constexpr bool kScored = std::is_same_v<Score, detail::Scored>;
  static constexpr bool kBounded = Bounded;
  static_assert(!detail::Retracts<Optional>());
  static_assert(!kBounded || kScored);

  template<typename OptionalArgs>
  BooleanWindow(std::piecewise_construct_t, OptionalArgs&& optional,
                Score score = {})
    : _optional{std::make_from_tuple<Optional>(
        std::forward<OptionalArgs>(optional))},
      _score{score} {}

  template<typename OptionalArgs>
  BooleanWindow(std::piecewise_construct_t, OptionalArgs&& optional,
                Score score, score_t bound)
    requires kBounded
    : _optional{std::make_from_tuple<Optional>(
        std::forward<OptionalArgs>(optional))},
      _score{score},
      _bound{bound} {}

  BooleanWindow(BooleanWindow&&) = delete;
  BooleanWindow& operator=(BooleanWindow&&) = delete;

  doc_id_t Probe(doc_id_t target) {
    if (target <= _doc) {
      return _doc;
    }
    return _doc = From(target);
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
      Refill(target);
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
    _gathered[slot] = _window[_doc - _min];
  }

  ScoreFunction PrepareScore()
    requires kScored
  {
    return detail::MakeWindowScore(_score.inner, _gathered, _score.absorbed);
  }

  void CollectScorers(std::vector<ScoreFunction>& out)
    requires kScored
  {
    detail::AppendScorer(out, PrepareScore());
  }

 private:

  doc_id_t From(doc_id_t target) {
    if (doc_limits::eof(target)) {
      return doc_limits::eof();
    }
    for (;;) {
      if (!_filled || target >= _min + detail::kWindowDocs) {
        if (_spent) {
          return doc_limits::eof();
        }
        Refill(target);
      }
      if (const auto found = Find(target - _min); found != detail::kWindowDocs) {
        return _min + found;
      }
      if (_spent) {
        return doc_limits::eof();
      }
      if (!detail::NextWindow(_min, _next, target)) {
        return doc_limits::eof();
      }
    }
  }

  void Refill(doc_id_t target) {
    SDB_ASSERT(!_filled || target >= _min);
    auto* const words = _mask.data();
    if constexpr (kScored) {
      for (uint32_t w = 0; w != detail::kWindowWords; ++w) {
        auto word = words[w];
        words[w] = 0;
        const auto base = w * detail::kWindowBits;
        while (word != 0) {
          _window[base + std::countr_zero(word)] = 0;
          word = PopBit(word);
        }
      }
    } else {
      detail::Clear(words, detail::kWindowWords);
    }
    _min = target;
    _filled = true;
    const auto max = _min + detail::kWindowDocs;
    doc_id_t next;
    if constexpr (kScored) {
      next = _optional.Fill(_min, max, words, _window);
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
    } else {
      next = _optional.Fill(_min, max, words);
    }
    _next = next;
    _spent = doc_limits::eof(next);
  }

  doc_id_t Find(doc_id_t offset) const noexcept {
    auto word = offset / detail::kWindowBits;
    auto bits = _mask[word] & (~uint64_t{0} << (offset % detail::kWindowBits));
    for (;;) {
      if (bits != 0) {
        return static_cast<doc_id_t>(word * detail::kWindowBits + std::countr_zero(bits));
      }
      if (++word == detail::kWindowWords) {
        return detail::kWindowDocs;
      }
      bits = _mask[word];
    }
  }

  detail::Scratch _mask{};
  [[no_unique_address]] ABSL_CACHELINE_ALIGNED
    utils::Need<kScored, score_t[detail::kWindowDocs]> _window{};
  [[no_unique_address]] ABSL_CACHELINE_ALIGNED
    utils::Need<kScored, score_t[kScoreBlock]> _gathered{};
  Optional _optional;
  doc_id_t _min = 0;
  doc_id_t _next = doc_limits::eof();
  doc_id_t _doc = doc_limits::invalid();
  bool _filled = false;
  bool _spent = false;
  [[no_unique_address]] Score _score;
  [[no_unique_address]] utils::Need<kBounded, score_t> _bound =
    std::numeric_limits<score_t>::max();
  [[no_unique_address]] utils::Need<kBounded, score_t> _max = 0;
};

}  // namespace irs::probe
