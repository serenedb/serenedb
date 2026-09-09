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

#include <bit>
#include <cstdint>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

#include "basics/bit_utils.hpp"
#include "basics/empty.hpp"
#include "basics/shared.hpp"
#include "iresearch/search/common/score/make_window.hpp"
#include "iresearch/search/common/score_args.hpp"
#include "iresearch/search/common/score_policy.hpp"
#include "iresearch/search/common/window.hpp"
#include "iresearch/search/score_function.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::probe {

template<typename Optional, typename Score = utils::Empty>
class BooleanWindow {
 public:
  static constexpr bool kScored = std::is_same_v<Score, search::Scored>;
  static_assert(!search::Retracts<Optional>());

  template<typename OptionalArgs>
  BooleanWindow(std::piecewise_construct_t, OptionalArgs&& optional,
                Score score = {})
    : _optional{std::make_from_tuple<Optional>(
        std::forward<OptionalArgs>(optional))},
      _score{score} {}

  BooleanWindow(BooleanWindow&&) = delete;
  BooleanWindow& operator=(BooleanWindow&&) = delete;

  doc_id_t Probe(doc_id_t target) {
    if (target <= _doc) {
      return _doc;
    }
    return _doc = From(target);
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
    return search::MakeWindowScore(_score.inner, _gathered, _score.absorbed);
  }

  void CollectScorers(std::vector<ScoreFunction>& out)
    requires kScored
  {
    search::AppendScorer(out, PrepareScore());
  }

 private:
  static constexpr auto kBits = search::kWindowBits;
  static constexpr auto kWindow = search::kWindowDocs;

  doc_id_t From(doc_id_t target) {
    if (doc_limits::eof(target)) {
      return doc_limits::eof();
    }
    for (;;) {
      if (!_filled || target >= _min + kWindow) {
        if (_spent) {
          return doc_limits::eof();
        }
        Refill(target);
      }
      if (const auto found = Find(target - _min); found != kWindow) {
        return _min + found;
      }
      if (_spent) {
        return doc_limits::eof();
      }
      if (!search::NextWindow(_min, _next, target)) {
        return doc_limits::eof();
      }
    }
  }

  void Refill(doc_id_t target) {
    SDB_ASSERT(!_filled || target >= _min);
    auto* const words = _mask.data();
    if constexpr (kScored) {
      for (uint32_t w = 0; w != search::kWindowWords; ++w) {
        auto word = words[w];
        words[w] = 0;
        const auto base = w * kBits;
        while (word != 0) {
          _window[base + std::countr_zero(word)] = 0;
          word = PopBit(word);
        }
      }
    } else {
      search::Clear(words, search::kWindowWords);
    }
    _min = target;
    _filled = true;
    const auto max = _min + kWindow;
    doc_id_t next;
    if constexpr (kScored) {
      next = _optional.Fill(_min, max, words, _window);
    } else {
      next = _optional.Fill(_min, max, words);
    }
    _next = next;
    _spent = doc_limits::eof(next);
  }

  doc_id_t Find(doc_id_t offset) const noexcept {
    auto word = offset / kBits;
    auto bits = _mask[word] & (~uint64_t{0} << (offset % kBits));
    for (;;) {
      if (bits != 0) {
        return static_cast<doc_id_t>(word * kBits + std::countr_zero(bits));
      }
      if (++word == search::kWindowWords) {
        return kWindow;
      }
      bits = _mask[word];
    }
  }

  search::Scratch _mask{};
  [[no_unique_address]] ABSL_CACHELINE_ALIGNED
    utils::Need<kScored, score_t[kWindow]> _window{};
  [[no_unique_address]] ABSL_CACHELINE_ALIGNED
    utils::Need<kScored, score_t[kScoreBlock]> _gathered{};
  Optional _optional;
  doc_id_t _min = 0;
  doc_id_t _next = doc_limits::eof();
  doc_id_t _doc = doc_limits::invalid();
  bool _filled = false;
  bool _spent = false;
  [[no_unique_address]] Score _score;
};

}  // namespace irs::probe
