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
#include <utility>
#include <vector>

#include "basics/bit_utils.hpp"
#include "iresearch/search/common/score/make_window.hpp"
#include "iresearch/search/common/score_args.hpp"
#include "iresearch/search/common/window.hpp"
#include "iresearch/search/score_function.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::lead {

template<typename Leaves>
class BitsThresholdScored {
 public:
  static constexpr uint32_t kBatch = kScoreBlock;

  template<typename LeavesArgs>
  BitsThresholdScored(std::piecewise_construct_t, LeavesArgs&& leaves,
                      uint32_t min_match, ScoreMergeType inner,
                      score_t absorbed = 0)
    : _planes(size_t{min_match} * search::kWindowWords, 0),
      _leaves{std::make_from_tuple<Leaves>(std::forward<LeavesArgs>(leaves))},
      _absorbed{absorbed},
      _min_match{min_match},
      _inner{inner} {
    SDB_ASSERT(_min_match > 1);
  }

  BitsThresholdScored(BitsThresholdScored&&) = delete;
  BitsThresholdScored& operator=(BitsThresholdScored&&) = delete;

  doc_id_t Advance() { return Seek(_doc + 1); }

  doc_id_t Seek(doc_id_t target) {
    if (target <= _doc) {
      return _doc;
    }
    return _doc = From(target);
  }

  doc_id_t Probe(doc_id_t target) { return Seek(target); }

  IRS_FORCE_INLINE void FetchScoreArgs(uint32_t slot) noexcept {
    SDB_ASSERT(slot < kBatch);
    _gathered[slot] = _filled ? _window[_doc - _min] : score_t{0};
  }

  ScoreFunction PrepareScore() {
    return search::MakeWindowScore(_inner, _gathered, _absorbed);
  }

  void CollectScorers(std::vector<ScoreFunction>& out) {
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
    auto* const planes = _planes.data();
    if (_filled) {
      for (size_t w = 0; w != search::kWindowWords; ++w) {
        auto touched = planes[w];
        const size_t base = w * kBits;
        while (touched != 0) {
          _window[base + static_cast<uint32_t>(std::countr_zero(touched))] = 0;
          touched = PopBit(touched);
        }
      }
    }
    std::fill(_planes.begin(), _planes.end(), uint64_t{0});
    _min = target - target % kWindow;
    _filled = true;
    const size_t top = size_t{_min_match} - 1;
    _next = _leaves.Visit(_min + kWindow, [&](auto& leaf) {
      search::Clear(_scratch.data(), search::kWindowWords);
      const auto doc =
        leaf.Fill(_min, _min + kWindow, _scratch.data(), _window);
      search::FoldCarry(planes, _scratch.data(), search::kWindowWords, top);
      return doc;
    });
    std::copy_n(planes + top * search::kWindowWords, search::kWindowWords,
                _mask.data());
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

  search::Scratch _scratch{};
  search::Scratch _mask{};
  ABSL_CACHELINE_ALIGNED score_t _window[kWindow]{};
  ABSL_CACHELINE_ALIGNED score_t _gathered[kBatch]{};
  std::vector<uint64_t> _planes;
  Leaves _leaves;
  doc_id_t _min = 0;
  doc_id_t _next = doc_limits::eof();
  doc_id_t _doc = doc_limits::invalid();
  score_t _absorbed;
  uint32_t _min_match;
  ScoreMergeType _inner;
  bool _filled = false;
};

}  // namespace irs::lead
