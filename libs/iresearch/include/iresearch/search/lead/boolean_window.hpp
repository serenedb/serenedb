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

#include "basics/bit_utils.hpp"
#include "basics/empty.hpp"
#include "basics/shared.hpp"
#include "iresearch/search/scorers/make_window.hpp"
#include "iresearch/search/scorers/score_args.hpp"
#include "iresearch/search/detail/window.hpp"
#include "iresearch/search/scorers/score_function.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::lead {

template<typename Lead, typename Others, typename Optional, typename Excludes,
         typename Score = utils::Empty>
class BooleanWindow {
 public:
  static constexpr bool kLead = !std::is_same_v<Lead, utils::Empty>;
  static constexpr bool kOthers = !std::is_same_v<Others, utils::Empty>;
  static constexpr bool kOptional = !std::is_same_v<Optional, utils::Empty>;
  static constexpr bool kExcludes = !std::is_same_v<Excludes, utils::Empty>;
  static constexpr bool kScored = !std::is_same_v<Score, utils::Empty>;
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
      _score{score} {}

  BooleanWindow(BooleanWindow&&) = delete;
  BooleanWindow& operator=(BooleanWindow&&) = delete;

  doc_id_t Advance() { return Seek(_doc + 1); }

  doc_id_t Seek(doc_id_t target) {
    if (target <= _doc) {
      return _doc;
    }
    return _doc = From(target);
  }

  IRS_FORCE_INLINE void FetchScoreArgs(uint32_t slot)
    requires kScored
  {
    SDB_ASSERT(slot < kScoreBlock);
    _gathered[slot] = _filled ? _window[_doc - _min] : score_t{0};
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
    if constexpr (kScored && !detail::LazyReset<Optional>()) {
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
    if constexpr (kLead) {
      next = _lead.FillOr(_min, max, words);
      if constexpr (kOthers) {
        next = std::max(next, _others.Restrict(_min, max, words));
      }
    } else if constexpr (kScored) {
      next = _optional.Fill(_min, max, words, _window);
    } else {
      next = _optional.Fill(_min, max, words);
    }
    if constexpr (kExcludes) {
      _excludes.Remove(_min, max, words);
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
  [[no_unique_address]] Lead _lead;
  [[no_unique_address]] Others _others;
  [[no_unique_address]] Optional _optional;
  [[no_unique_address]] Excludes _excludes;
  doc_id_t _min = 0;
  doc_id_t _next = doc_limits::eof();
  doc_id_t _doc = doc_limits::invalid();
  bool _filled = false;
  bool _spent = false;
  [[no_unique_address]] Score _score;
};

}  // namespace irs::lead
