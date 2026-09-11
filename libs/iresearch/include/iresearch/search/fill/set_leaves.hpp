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
#include <cstddef>
#include <tuple>
#include <utility>

#include "iresearch/search/common/fixed_array.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::fill {

template<typename Leaf, size_t N = 0>
class SetLeaves {
 public:
  template<typename Init>
  SetLeaves(size_t size, Init&& init)
    : _leaves{size, std::forward<Init>(init)},
      _live{size,
            [this](Leaf*& slot, size_t i) noexcept { slot = &_leaves[i]; }},
      _next{size, [](doc_id_t& slot,
                     size_t) noexcept { slot = doc_limits::invalid(); }},
      _live_count{size} {}

  SetLeaves(SetLeaves&&) = delete;
  SetLeaves& operator=(SetLeaves&&) = delete;

  bool Empty() const noexcept { return _live_count == 0; }

  size_t Live() const noexcept { return _live_count; }

  template<typename Op>
  doc_id_t Visit(doc_id_t max, Op&& op) {
    doc_id_t next = doc_limits::eof();
    for (size_t i = 0; i < _live_count;) {
      if (_next[i] < max) {
        const auto doc = op(*_live[i]);
        if (doc_limits::eof(doc)) {
          --_live_count;
          _live[i] = _live[_live_count];
          _next[i] = _next[_live_count];
          continue;
        }
        _next[i] = doc;
      }
      next = std::min(next, _next[i]);
      ++i;
    }
    return next;
  }

 private:
  search::RunOf<Leaf, N> _leaves;
  search::RunOf<Leaf*, N> _live;
  search::RunOf<doc_id_t, N> _next;
  size_t _live_count = 0;
};

template<typename First, typename Second>
class MixedSetLeaves {
 public:
  template<typename FirstArgs, typename SecondArgs>
  MixedSetLeaves(std::piecewise_construct_t, FirstArgs&& first,
                 SecondArgs&& second)
    : _first{std::make_from_tuple<SetLeaves<First>>(
        std::forward<FirstArgs>(first))},
      _second{std::make_from_tuple<SetLeaves<Second>>(
        std::forward<SecondArgs>(second))} {}

  MixedSetLeaves(MixedSetLeaves&&) = delete;
  MixedSetLeaves& operator=(MixedSetLeaves&&) = delete;

  bool Empty() const noexcept { return _first.Empty() && _second.Empty(); }

  size_t Live() const noexcept { return _first.Live() + _second.Live(); }

  template<typename Op>
  doc_id_t Visit(doc_id_t max, Op&& op) {
    const auto first = _first.Visit(max, op);
    const auto second = _second.Visit(max, op);
    return std::min(first, second);
  }

 private:
  SetLeaves<First> _first;
  SetLeaves<Second> _second;
};

}  // namespace irs::fill
