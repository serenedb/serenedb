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
#include <tuple>
#include <utility>
#include <vector>

#include "iresearch/search/detail/exclude_block.hpp"
#include "iresearch/search/detail/window.hpp"
#include "iresearch/search/fill/concept.hpp"
#include "iresearch/search/probe/concept.hpp"
#include "iresearch/types.hpp"
#include "iresearch/utils/bit_utils.hpp"
#include "iresearch/utils/containers/fixed.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::fill {

template<Producer Leaf, size_t N = 0>
class AndLeaves {
 public:
  template<typename Init>
  AndLeaves(size_t size, Init&& init)
    : _leaves{size, std::forward<Init>(init)} {
    SDB_ASSERT(!_leaves.empty());
  }

  doc_id_t Restrict(doc_id_t min, doc_id_t max, uint64_t* IRS_RESTRICT mask) {
    const auto words = detail::WindowWords(min, max);
    doc_id_t next = 0;
    auto card = detail::Cardinality(mask, words);
    for (size_t i = 0; i != _leaves.size(); ++i) {
      if (card == 0) {
        return next;
      }
      if (Probes(card, _leaves[i], min, max)) {
        auto n = detail::TakeDocs(min, mask, words, _docs.data());
        n = Keep(_docs.data(), n, _docs.data(), i);
        detail::PutDocs(min, _docs.data(), n, mask);
        return next;
      }
      detail::Clear(_own.data(), words);
      next = std::max(next, _leaves[i].FillOr(min, max, _own.data()));
      card = detail::FoldAnd(mask, _own.data(), words);
    }
    return next;
  }

  uint32_t Keep(const doc_id_t* in, uint32_t n, doc_id_t* out,
                size_t from = 0) {
    for (auto i = from; i != _leaves.size(); ++i) {
      n = _leaves[i].Keep(in, n, out);
      in = out;
      if (n == 0) {
        return 0;
      }
    }
    if (in != out) {
      std::copy_n(in, n, out);
    }
    return n;
  }

 private:
  IRS_FORCE_INLINE static bool Probes(uint32_t card, Leaf& leaf, doc_id_t min,
                                      doc_id_t max) {
    return card < detail::kSparseWindow &&
           card * detail::kProbeCost < leaf.FillCost(min, max);
  }

  detail::Scratch _own;
  SlackBuf<doc_id_t, detail::kSparseWindow, doc_limits::kDocsSlack> _docs;
  containers::Fixed<Leaf, N> _leaves;
};

template<probe::Type Leaf>
class ProbedAndNot {
 public:
  template<typename Args>
  ProbedAndNot(std::piecewise_construct_t, Args&& probe)
    : _probe{std::make_from_tuple<Leaf>(std::forward<Args>(probe))} {}

  ProbedAndNot(ProbedAndNot&&) = delete;
  ProbedAndNot& operator=(ProbedAndNot&&) = delete;

  IRS_FORCE_INLINE doc_id_t Probe(doc_id_t target) {
    return _probe.Probe(target);
  }

  IRS_FORCE_INLINE bool Test(doc_id_t doc)
    requires requires(Leaf& probe) { probe.Test(doc); }
  {
    return _probe.Test(doc);
  }

  void Remove(doc_id_t min, doc_id_t max, uint64_t* IRS_RESTRICT mask) {
    const auto words = detail::WindowWords(min, max);
    if constexpr (kWords) {
      for (size_t w = 0; w != words; ++w) {
        mask[w] &= ~Excluded(min, w);
      }
      return;
    }
    auto base = min;
    for (size_t w = 0; w != words; ++w, base += detail::kWindowBits) {
      auto word = mask[w];
      auto live = word;
      while (word != 0) {
        const auto bit = static_cast<uint32_t>(std::countr_zero(word));
        const auto doc = base + bit;
        if (detail::IsExcluded(_probe, doc)) {
          UnsetBit(live, bit);
        }
        word = PopBit(word);
      }
      mask[w] = live;
    }
  }

  void Remove(doc_id_t min, doc_id_t max, uint64_t* IRS_RESTRICT mask,
              score_t* IRS_RESTRICT scores, score_t reset) {
    const auto words = detail::WindowWords(min, max);
    auto base = min;
    for (size_t w = 0; w != words; ++w, base += detail::kWindowBits) {
      auto word = mask[w];
      auto live = word;
      while (word != 0) {
        const auto bit = static_cast<uint32_t>(std::countr_zero(word));
        const auto doc = base + bit;
        if (detail::IsExcluded(_probe, doc)) {
          UnsetBit(live, bit);
          scores[w * detail::kWindowBits + bit] = reset;
        }
        word = PopBit(word);
      }
      mask[w] = live;
    }
  }

 private:
  static constexpr bool kWords = requires(const Leaf& probe) {
    probe.Words();
    probe.WordCount();
    Leaf::kMin;
  };

  IRS_FORCE_INLINE uint64_t Excluded(doc_id_t min, size_t w) const noexcept {
    return detail::WordAt(_probe.Words(), _probe.WordCount(),
                          static_cast<int64_t>(min) -
                            static_cast<int64_t>(Leaf::kMin) +
                            static_cast<int64_t>(w * detail::kWindowBits));
  }

  Leaf _probe;
};

template<typename Leaves>
class FilledAndNot {
 public:
  template<typename LeavesArgs>
  FilledAndNot(std::piecewise_construct_t, LeavesArgs&& leaves)
    : _leaves{std::make_from_tuple<Leaves>(std::forward<LeavesArgs>(leaves))} {}

  FilledAndNot(FilledAndNot&&) = delete;
  FilledAndNot& operator=(FilledAndNot&&) = delete;

  void Remove(doc_id_t min, doc_id_t max, uint64_t* IRS_RESTRICT mask) {
    _leaves.Visit(max,
                  [&](auto& leaf) { return leaf.FillAndNot(min, max, mask); });
  }

  uint32_t Drop(const doc_id_t* in, uint32_t n, doc_id_t* out) {
    if (in != out) {
      std::copy_n(in, n, out);
    }
    _leaves.Each([&](auto& leaf) {
      if (n != 0) {
        n = leaf.Drop(out, n, out);
      }
    });
    return n;
  }

  void Remove(doc_id_t min, doc_id_t max, uint64_t* IRS_RESTRICT mask,
              score_t* IRS_RESTRICT scores, score_t reset) {
    const auto words = detail::WindowWords(min, max);
    uint64_t before[detail::kWindowWords];
    std::copy_n(mask, words, before);
    Remove(min, max, mask);
    for (size_t w = 0; w != words; ++w) {
      auto removed = before[w] & ~mask[w];
      while (removed != 0) {
        scores[w * detail::kWindowBits + std::countr_zero(removed)] = reset;
        removed = PopBit(removed);
      }
    }
  }

 private:
  Leaves _leaves;
};

}  // namespace irs::fill
