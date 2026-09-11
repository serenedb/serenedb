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
#include <cstddef>
#include <utility>

#include "basics/shared.hpp"
#include "iresearch/search/detail/fixed_array.hpp"
#include "iresearch/search/detail/window.hpp"
#include "iresearch/search/top/posting_pruned_disj.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::top::detail {

template<typename Input>
class DisjunctionLead {
 public:
  using Leaf = irs::detail::PostingPrunedDisj<Input>;

  template<typename Init>
  DisjunctionLead(size_t size, Init&& init)
    : _leaves{size, std::forward<Init>(init)} {
    SDB_ASSERT(!_leaves.empty());
  }

  DisjunctionLead(DisjunctionLead&&) = delete;
  DisjunctionLead& operator=(DisjunctionLead&&) = delete;

  doc_id_t Value() const noexcept { return _doc; }

  doc_id_t Advance() {
    if (doc_limits::eof(_doc)) [[unlikely]] {
      return _doc;
    }
    return Seek(_doc + 1);
  }

  doc_id_t Seek(doc_id_t target) {
    auto next = doc_limits::eof();
    for (auto& leaf : _leaves) {
      auto doc = leaf.Value();
      if (doc < target) {
        doc = leaf.Seek(target);
      }
      next = std::min(next, doc);
    }
    return _doc = next;
  }

  doc_id_t BlockLast() {
    SDB_ASSERT(!doc_limits::eof(_doc));
    auto last = _doc + (irs::detail::kWindowDocs - 1);
    for (auto& leaf : _leaves) {
      const auto doc = leaf.Value();
      if (doc_limits::eof(doc)) {
        continue;
      }
      const auto end = leaf.SeekToBlock(std::max(doc, _doc));
      if (!doc_limits::eof(end)) {
        last = std::min(last, end);
      }
    }
    return last;
  }

  score_t MaxScore(doc_id_t last) noexcept {
    score_t bound = 0;
    for (auto& leaf : _leaves) {
      bound += leaf.MaxScore(last);
    }
    return bound;
  }

  template<typename Visitor>
  void ForEachScoredBlock(doc_id_t max, Visitor&& visit) {
    while (_doc < max) {
      const auto min = _doc;
      const auto end = max - min > irs::detail::kWindowDocs ? min + irs::detail::kWindowDocs : max;
      std::fill_n(_mask, irs::detail::kWindowWords, uint64_t{0});
      std::fill_n(_window, irs::detail::kWindowDocs, score_t{0});
      for (auto& leaf : _leaves) {
        if (leaf.Value() < min) {
          leaf.Seek(min);
        }
        if (leaf.Value() < end) {
          leaf.Fill(min, end, _mask, _window);
        }
      }
      Emit(min, visit);
      auto next = doc_limits::eof();
      for (const auto& leaf : _leaves) {
        next = std::min(next, leaf.Value());
      }
      _doc = next;
    }
  }

 private:
  template<typename Visitor>
  void Emit(doc_id_t min, Visitor&& visit) {
    uint32_t len = 0;
    for (size_t w = 0; w != irs::detail::kWindowWords; ++w) {
      auto word = _mask[w];
      while (word != 0) {
        const auto offset = static_cast<uint32_t>(w * irs::detail::kWindowBits) +
                            static_cast<uint32_t>(std::countr_zero(word));
        word &= word - 1;
        _docs[len] = min + offset;
        _scores[len] = _window[offset];
        if (++len == doc_limits::kBlockSize) {
          visit(_docs, len, _scores);
          len = 0;
        }
      }
    }
    if (len != 0) {
      visit(_docs, len, _scores);
    }
  }

  ABSL_CACHELINE_ALIGNED uint64_t _mask[irs::detail::kWindowWords]{};
  ABSL_CACHELINE_ALIGNED score_t _window[irs::detail::kWindowDocs]{};
  ABSL_CACHELINE_ALIGNED doc_id_t _docs[doc_limits::kBlockSize]{};
  ABSL_CACHELINE_ALIGNED score_t _scores[doc_limits::kBlockSize]{};
  irs::detail::FixedArray<Leaf> _leaves;
  doc_id_t _doc = doc_limits::invalid();
};

}  // namespace irs::top::detail
