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

#include "basics/assert.h"
#include "iresearch/search/common/window.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::lead {

template<typename Leaves>
class CountThresholdDocs {
 public:
  template<typename LeavesArgs>
  CountThresholdDocs(std::piecewise_construct_t, LeavesArgs&& leaves,
                     uint32_t min_match)
    : _leaves{std::make_from_tuple<Leaves>(std::forward<LeavesArgs>(leaves))},
      _min_match{min_match} {
    SDB_ASSERT(_min_match > 1);
  }

  doc_id_t Advance() { return Seek(_doc + 1); }

  doc_id_t Seek(doc_id_t target) {
    if (target <= _doc) {
      return _doc;
    }
    return _doc = From(target);
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
        if (_leaves.Live() < _min_match) {
          return doc_limits::eof();
        }
        Refill(target);
      }
      if (const auto found = Find(target - _min); found != kWindow) {
        return _min + found;
      }
      if (_leaves.Live() < _min_match) {
        return doc_limits::eof();
      }
      if (!search::NextWindow(_min, _next, target)) {
        return doc_limits::eof();
      }
    }
  }

  void Refill(doc_id_t target) {
    SDB_ASSERT(!_filled || target >= _min);
    _min = target - target % kWindow;
    _filled = true;
    const auto max = _min + kWindow;
    _next = _leaves.Visit(
      max, [&](auto& leaf) { return leaf.Count(_min, max, _counts); });
    for (uint32_t w = 0; w != search::kWindowWords; ++w) {
      auto* const counts = _counts + w * kBits;
      uint64_t word = 0;
      for (uint32_t i = 0; i != kBits; ++i) {
        word |= uint64_t{counts[i] >= _min_match} << i;
      }
      std::fill_n(counts, kBits, uint32_t{0});
      _mask[w] = word;
    }
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

  ABSL_CACHELINE_ALIGNED uint32_t _counts[kWindow]{};
  search::Scratch _mask{};
  Leaves _leaves;
  doc_id_t _min = 0;
  doc_id_t _next = doc_limits::eof();
  doc_id_t _doc = doc_limits::invalid();
  uint32_t _min_match;
  bool _filled = false;
};

}  // namespace irs::lead
