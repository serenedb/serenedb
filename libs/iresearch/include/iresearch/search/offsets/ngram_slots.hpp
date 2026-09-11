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
#include <array>
#include <span>
#include <type_traits>
#include <utility>
#include <vector>

#include "iresearch/search/common/fixed_array.hpp"
#include "iresearch/search/ngram_matcher.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::search {

template<typename Leaf, bool Scored = false, bool Offs = false, size_t N = 0>
class NGramSlots {
 public:
  static constexpr bool kOffsets = Offs;
  static_assert(!Offs || Leaf::kOffsets);

  template<typename Init>
  NGramSlots(size_t size, Init&& init, uint32_t min_match, size_t total_terms)
    : _leaves{size, std::forward<Init>(init)},
      _live{size,
            [this](Leaf*& slot, size_t i) noexcept { slot = &_leaves[i]; }},
      _live_count{size},
      _checker{size, total_terms, min_match},
      _min_match{min_match} {
    for (size_t i = 0; i != size; ++i) {
      _checker.Slot(i) = {_leaves[i].ValueRef(), _leaves[i].Positions()};
    }
    SDB_ASSERT(_min_match != 0);
    SDB_ASSERT(_leaves.size() > 1);
    SDB_ASSERT(_leaves.size() >= _min_match);
  }

  NGramSlots(NGramSlots&&) = delete;
  NGramSlots& operator=(NGramSlots&&) = delete;

 private:
  template<typename Advance>
  void Gather(Advance&& advance, doc_id_t from) {
    for (size_t i = 0; i != _live_count; ++i) {
      if (_live[i]->Value() < from) {
        advance(*_live[i]);
      }
    }
    std::sort(_live.begin(), _live.begin() + _live_count,
              [](const Leaf* lhs, const Leaf* rhs) noexcept {
                return lhs->Value() < rhs->Value();
              });
    while (_live_count != 0 &&
           doc_limits::eof(_live[_live_count - 1]->Value())) {
      --_live_count;
    }
  }

  uint32_t Agreeing(doc_id_t doc) const noexcept {
    uint32_t matches = 0;
    while (matches != _live_count && _live[matches]->Value() == doc) {
      ++matches;
    }
    return matches;
  }

 public:
  doc_id_t Seek(doc_id_t from) {
    for (;;) {
      Gather([from](Leaf& leaf) { return leaf.Seek(from); }, from);
      if (_live_count < _min_match) {
        return doc_limits::eof();
      }
      const auto candidate = _live[0]->Value();
      const auto matches = Agreeing(candidate);
      if (matches >= _min_match) {
        _matches = matches;
        return candidate;
      }
      from = _live[_min_match - 1]->Value();
    }
  }

  doc_id_t Next(doc_id_t doc) { return Seek(doc + 1); }

  doc_id_t Probe(doc_id_t target) {
    Gather([target](Leaf& leaf) { return leaf.Probe(target); }, target);
    if (_live_count < _min_match) {
      return doc_limits::eof();
    }
    const auto matches = Agreeing(target);
    if (matches >= _min_match) {
      _matches = matches;
      return target;
    }
    return _live[_min_match - 1]->Value();
  }

  bool Match(doc_id_t doc) { return _checker.Match(_matches, doc); }

  uint32_t Freq() const noexcept
    requires(Scored)
  {
    return _checker.GetFreq();
  }

  score_t Boost() const noexcept
    requires(Scored)
  {
    return _checker.GetBoost();
  }

  std::span<const OffsAttr> Offsets() const noexcept
    requires(Offs)
  {
    return _checker.Offsets();
  }

 private:
  using Base = std::conditional_t<Offs, ngram::NGramPosition, ngram::Dummy>;
  using Checker = ngram::SerialPositionsChecker<Base, Scored || Offs, N>;

  search::RunOf<Leaf, N> _leaves;
  search::RunOf<Leaf*, N> _live;
  size_t _live_count = 0;
  Checker _checker;
  uint32_t _min_match;
  uint32_t _matches = 0;
};

}  // namespace irs::search
