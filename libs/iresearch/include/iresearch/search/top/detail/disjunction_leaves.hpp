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
#include <optional>
#include <utility>

#include "basics/bit_utils.hpp"
#include "basics/shared.hpp"
#include "iresearch/search/common/fixed_array.hpp"
#include "iresearch/search/common/score/make_probe.hpp"
#include "iresearch/search/common/window.hpp"
#include "iresearch/search/score_function.hpp"
#include "iresearch/search/top/posting_pruned_clause.hpp"
#include "iresearch/search/top/posting_pruned_disj.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::top::detail {

template<typename Input>
class DisjunctionClause {
 public:
  using Leaf = search::PostingPrunedClause<Input>;

  DisjunctionClause() = default;
  DisjunctionClause(DisjunctionClause&&) = delete;
  DisjunctionClause& operator=(DisjunctionClause&&) = delete;

  template<typename Init>
  void Prepare(size_t size, uint32_t min_match, Init&& init) {
    SDB_ASSERT(size != 0);
    SDB_ASSERT(min_match != 0 && min_match <= size);
    _leaves.emplace(size, std::forward<Init>(init));
    _held.emplace(size);
    _matched.emplace(size);
    _min_match = min_match;
  }

  doc_id_t Value() const noexcept {
    auto next = doc_limits::eof();
    for (const auto& leaf : *_leaves) {
      next = std::min(next, leaf.Value());
    }
    return next;
  }

  doc_id_t AdvanceBlock(doc_id_t target) {
    auto end = doc_limits::eof();
    for (auto& leaf : *_leaves) {
      const auto e = leaf.AdvanceBlock(std::max(leaf.Value(), target));
      if (!doc_limits::eof(e)) {
        end = std::min(end, e);
      }
    }
    return end;
  }

  score_t MaxScore(doc_id_t last) noexcept {
    score_t bound = 0;
    for (auto& leaf : *_leaves) {
      bound += leaf.MaxScore(last);
    }
    return bound;
  }

  doc_id_t Probe(doc_id_t target) {
    auto& leaves = *_leaves;
    const auto count = leaves.size();
    _hit = false;
    if (_min_match == 1) {
      auto next = doc_limits::eof();
      for (size_t i = 0; i != count; ++i) {
        const auto doc = leaves[i].Probe(target);
        if (doc == target) {
          _doc = target;
          _first = static_cast<uint32_t>(i);
          _hit = true;
          return target;
        }
        next = std::min(next, doc);
      }
      return next;
    }
    uint32_t hits = 0;
    auto left = static_cast<uint32_t>(count);
    for (size_t i = 0; i != count; ++i) {
      if (leaves[i].Probe(target) == target) {
        (*_matched)[hits++] = static_cast<uint32_t>(i);
        if (hits == _min_match) {
          _doc = target;
          _hit = true;
          return target;
        }
      }
      if (hits + --left < _min_match) {
        return target + 1;
      }
    }
    SDB_UNREACHABLE();
  }

  IRS_FORCE_INLINE void FetchScoreArgs(uint32_t slot) {
    SDB_ASSERT(slot < kScoreBlock);
    if (!_hit) {
      return;
    }
    auto& leaves = *_leaves;
    auto& held = *_held;
    size_t from = 0;
    if (_min_match == 1) {
      SetBit(held[_first], slot);
      leaves[_first].FetchScoreArgs(slot);
      from = _first + 1;
    } else {
      for (uint32_t k = 0; k != _min_match; ++k) {
        const auto i = (*_matched)[k];
        SetBit(held[i], slot);
        leaves[i].FetchScoreArgs(slot);
      }
      from = (*_matched)[_min_match - 1] + 1;
    }
    for (size_t i = from, count = leaves.size(); i != count; ++i) {
      if (leaves[i].Probe(_doc) != _doc) {
        continue;
      }
      SetBit(held[i], slot);
      leaves[i].FetchScoreArgs(slot);
    }
  }

  ScoreFunction PrepareScore() {
    return search::MakeProbeOf(ScoreMergeType::Sum, *_leaves, *_held);
  }

 private:
  std::optional<search::FixedArray<Leaf>> _leaves;
  std::optional<search::FixedArray<uint32_t>> _held;
  std::optional<search::FixedArray<uint32_t>> _matched;
  uint32_t _min_match = 1;
  uint32_t _first = 0;
  doc_id_t _doc = doc_limits::invalid();
  bool _hit = false;
};

template<typename Input>
class DisjunctionLead {
 public:
  using Leaf = search::PostingPrunedDisj<Input>;
  static constexpr doc_id_t kWindow = search::kWindowDocs;
  static constexpr size_t kNumWords = search::kWindowWords;

  DisjunctionLead() = default;
  DisjunctionLead(DisjunctionLead&&) = delete;
  DisjunctionLead& operator=(DisjunctionLead&&) = delete;

  template<typename Init>
  void Prepare(size_t size, Init&& init) {
    SDB_ASSERT(size != 0);
    _leaves.emplace(size, std::forward<Init>(init));
  }

  doc_id_t Value() const noexcept { return _doc; }

  doc_id_t Advance() {
    if (doc_limits::eof(_doc)) [[unlikely]] {
      return _doc;
    }
    return Seek(_doc + 1);
  }

  doc_id_t Seek(doc_id_t target) {
    auto next = doc_limits::eof();
    for (auto& leaf : *_leaves) {
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
    auto last = _doc + (kWindow - 1);
    for (auto& leaf : *_leaves) {
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
    for (auto& leaf : *_leaves) {
      bound += leaf.MaxScore(last);
    }
    return bound;
  }

  template<typename Visitor>
  void ForEachScoredBlock(doc_id_t max, Visitor&& visit) {
    while (_doc < max) {
      const auto min = _doc;
      const auto end = max - min > kWindow ? min + kWindow : max;
      std::fill_n(_mask, kNumWords, uint64_t{0});
      std::fill_n(_window, kWindow, score_t{0});
      for (auto& leaf : *_leaves) {
        if (leaf.Value() < min) {
          leaf.Seek(min);
        }
        if (leaf.Value() < end) {
          leaf.Fill(min, end, _mask, _window);
        }
      }
      Emit(min, visit);
      auto next = doc_limits::eof();
      for (const auto& leaf : *_leaves) {
        next = std::min(next, leaf.Value());
      }
      _doc = next;
    }
  }

 private:
  template<typename Visitor>
  void Emit(doc_id_t min, Visitor&& visit) {
    uint32_t len = 0;
    for (size_t w = 0; w != kNumWords; ++w) {
      auto word = _mask[w];
      while (word != 0) {
        const auto offset = static_cast<uint32_t>(w * search::kWindowBits) +
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

  ABSL_CACHELINE_ALIGNED uint64_t _mask[kNumWords]{};
  ABSL_CACHELINE_ALIGNED score_t _window[kWindow]{};
  ABSL_CACHELINE_ALIGNED doc_id_t _docs[doc_limits::kBlockSize]{};
  ABSL_CACHELINE_ALIGNED score_t _scores[doc_limits::kBlockSize]{};
  std::optional<search::FixedArray<Leaf>> _leaves;
  doc_id_t _doc = doc_limits::invalid();
};

}  // namespace irs::top::detail
