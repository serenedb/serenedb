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
#include <span>
#include <tuple>
#include <utility>
#include <vector>

#include "iresearch/utils/bit_utils.hpp"
#include "iresearch/utils/shared.hpp"
#include "iresearch/search/detail/plan.hpp"
#include "iresearch/search/detail/window.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::detail {

struct BooleanGroups {
  std::span<const PostingClause> must;
  std::span<const QueryBuilder::ptr> must_filters;
  std::span<const PostingClause> should;
  std::span<const QueryBuilder::ptr> should_filters;
  std::vector<FillNode::ptr>* should_fills = nullptr;
  std::span<const PostingClause> must_not;
  std::span<const QueryBuilder::ptr> must_not_filters;
};

inline constexpr int kDenseWord = 16;

inline IRS_FORCE_INLINE uint64_t
TallyAnswer(const uint32_t* IRS_RESTRICT counts, uint32_t min_match) noexcept {
  uint64_t answer = 0;
  for (uint32_t i = 0; i != kWindowBits; ++i) {
    answer |= uint64_t{counts[i] >= min_match} << i;
  }
  return answer;
}

inline void TallyMask(const uint64_t* touched, uint64_t* mask,
                      uint32_t* IRS_RESTRICT counts,
                      score_t* IRS_RESTRICT scores, uint32_t min_match,
                      size_t words) noexcept {
  for (size_t w = 0; w != words; ++w) {
    auto rest = touched[w];
    if (rest == 0) {
      continue;
    }
    const auto base = w * kWindowBits;
    auto* const word_counts = counts + base;
    auto* const slots = scores + base;
    uint64_t answer = rest;
    if (std::popcount(rest) >= kDenseWord) {
      answer = TallyAnswer(word_counts, min_match);
      std::fill_n(word_counts, kWindowBits, uint32_t{0});
      if ((mask[w] & answer) == 0) {
        std::fill_n(slots, kWindowBits, score_t{0});
      }
    } else {
      while (rest != 0) {
        const auto bit = static_cast<uint32_t>(std::countr_zero(rest));
        if (word_counts[bit] < min_match) {
          answer ^= uint64_t{1} << bit;
          slots[bit] = 0;
        }
        word_counts[bit] = 0;
        rest = PopBit(rest);
      }
    }
    mask[w] &= answer;
  }
}

inline IRS_FORCE_INLINE void ResetTouched(uint64_t touched,
                                          score_t* IRS_RESTRICT scores,
                                          score_t constant) noexcept {
  if (touched == 0) {
    return;
  }
  if (std::popcount(touched) >= kDenseWord) {
    std::fill_n(scores, kWindowBits, constant);
    return;
  }
  while (touched != 0) {
    scores[static_cast<uint32_t>(std::countr_zero(touched))] = constant;
    touched = PopBit(touched);
  }
}

inline IRS_FORCE_INLINE void ResetRetracted(uint64_t retract, uint64_t answer,
                                            score_t* IRS_RESTRICT scores,
                                            score_t constant) noexcept {
  if (retract == 0) {
    return;
  }
  if (std::popcount(retract) >= kDenseWord) {
    for (uint32_t i = 0; i != kWindowBits; ++i) {
      scores[i] = ((answer >> i) & 1) != 0 ? scores[i] : constant;
    }
    return;
  }
  while (retract != 0) {
    scores[static_cast<uint32_t>(std::countr_zero(retract))] = constant;
    retract = PopBit(retract);
  }
}

template<typename Leaves>
class OrGroup {
 public:
  static constexpr bool kRetracts = false;

  template<typename... Args>
  explicit OrGroup(Args&&... args) : _leaves{std::forward<Args>(args)...} {}

  OrGroup(OrGroup&&) = delete;
  OrGroup& operator=(OrGroup&&) = delete;

  bool Exhausted() const noexcept { return _leaves.Empty(); }

  doc_id_t Fill(doc_id_t min, doc_id_t max, uint64_t* IRS_RESTRICT words) {
    return _leaves.Visit(max, [&](auto& leaf) IRS_FORCE_INLINE {
      return leaf.FillOr(min, max, words);
    });
  }

  doc_id_t Fill(doc_id_t min, doc_id_t max, uint64_t* IRS_RESTRICT words,
                score_t* IRS_RESTRICT scores) {
    return _leaves.Visit(max, [&](auto& leaf) IRS_FORCE_INLINE {
      return leaf.Fill(min, max, words, scores);
    });
  }

 private:
  Leaves _leaves;
};

template<typename Leaves, bool Lazy = false>
class ThresholdGroup {
 public:
  static constexpr bool kRetracts = true;
  static constexpr bool kLazyReset = Lazy;

  template<typename LeavesArgs>
  ThresholdGroup(std::piecewise_construct_t, LeavesArgs&& leaves,
                 uint32_t min_match, score_t constant)
    : _leaves{std::make_from_tuple<Leaves>(std::forward<LeavesArgs>(leaves))},
      _planes(size_t{min_match} * kWindowWords, 0),
      _constant{constant},
      _min_match{min_match} {
    SDB_ASSERT(_min_match > 1);
  }

  ThresholdGroup(ThresholdGroup&&) = delete;
  ThresholdGroup& operator=(ThresholdGroup&&) = delete;

  bool Exhausted() const noexcept { return _leaves.Live() < _min_match; }

  doc_id_t Fill(doc_id_t min, doc_id_t max, uint64_t* IRS_RESTRICT words) {
    std::fill(_planes.begin(), _planes.end(), uint64_t{0});
    if (Exhausted()) {
      return doc_limits::eof();
    }
    const auto count = WindowWords(min, max);
    auto* const planes = _planes.data();
    const auto top = size_t{_min_match} - 1;
    bool first = true;
    const auto next = _leaves.Visit(max, [&](auto& leaf) IRS_FORCE_INLINE {
      if (first) {
        first = false;
        return leaf.FillOr(min, max, planes);
      }
      Clear(_scratch.data(), count);
      const auto doc = leaf.FillOr(min, max, _scratch.data());
      FoldCarry(planes, _scratch.data(), count, top);
      return doc;
    });
    const auto* const answers = planes + top * kWindowWords;
    for (size_t w = 0; w != count; ++w) {
      words[w] |= answers[w];
    }
    return Exhausted() ? doc_limits::eof() : next;
  }

  doc_id_t Fill(doc_id_t min, doc_id_t max, uint64_t* IRS_RESTRICT words,
                score_t* IRS_RESTRICT scores) {
    auto* const planes = _planes.data();
    const auto constant = _constant;
    if constexpr (Lazy) {
      for (size_t w = 0; w != kWindowWords; ++w) {
        ResetTouched(planes[w], scores + w * kWindowBits, constant);
      }
    }
    std::fill(_planes.begin(), _planes.end(), uint64_t{0});
    if (Exhausted()) {
      return doc_limits::eof();
    }
    const auto count = WindowWords(min, max);
    const auto top = size_t{_min_match} - 1;
    const auto next = _leaves.Visit(max, [&](auto& leaf) IRS_FORCE_INLINE {
      Clear(_scratch.data(), count);
      const auto doc = leaf.Fill(min, max, _scratch.data(), scores);
      FoldCarry(planes, _scratch.data(), count, top);
      return doc;
    });
    const auto* const answers = planes + top * kWindowWords;
    for (size_t w = 0; w != count; ++w) {
      const auto answer = answers[w];
      if constexpr (!Lazy) {
        ResetRetracted(planes[w] & ~answer, answer, scores + w * kWindowBits,
                       constant);
      }
      words[w] |= answer;
    }
    return Exhausted() ? doc_limits::eof() : next;
  }

 private:
  Scratch _scratch{};
  Leaves _leaves;
  std::vector<uint64_t> _planes;
  score_t _constant;
  uint32_t _min_match;
};

template<typename Leaves, bool Lazy = false>
class TallyGroup {
 public:
  static constexpr bool kRetracts = true;
  static constexpr bool kLazyReset = Lazy;
  static constexpr bool kTally = !Lazy;

  template<typename LeavesArgs>
  TallyGroup(std::piecewise_construct_t, LeavesArgs&& leaves,
             uint32_t min_match, score_t constant)
    : _leaves{std::make_from_tuple<Leaves>(std::forward<LeavesArgs>(leaves))},
      _constant{constant},
      _min_match{min_match} {
    SDB_ASSERT(_min_match > 1);
  }

  TallyGroup(TallyGroup&&) = delete;
  TallyGroup& operator=(TallyGroup&&) = delete;

  bool Exhausted() const noexcept { return _leaves.Live() < _min_match; }

  uint32_t* Counts() noexcept { return _counts; }

  uint32_t MinMatch() const noexcept { return _min_match; }

  doc_id_t FillTouched(doc_id_t min, doc_id_t max, uint64_t* IRS_RESTRICT words,
                       score_t* IRS_RESTRICT scores) {
    static_assert(!Lazy);
    if (Exhausted()) {
      return doc_limits::eof();
    }
    const auto next = _leaves.Visit(max, [&](auto& leaf) IRS_FORCE_INLINE {
      return leaf.Count(min, max, _counts, words, scores);
    });
    return Exhausted() ? doc_limits::eof() : next;
  }

  doc_id_t Fill(doc_id_t min, doc_id_t max, uint64_t* IRS_RESTRICT words) {
    if (Exhausted()) {
      return doc_limits::eof();
    }
    const auto count = WindowWords(min, max);
    const auto min_match = _min_match;
    const auto next = _leaves.Visit(max, [&](auto& leaf) IRS_FORCE_INLINE {
      return leaf.Count(min, max, _counts);
    });
    for (size_t w = 0; w != count; ++w) {
      auto* const counts = _counts + w * kWindowBits;
      uint64_t word = 0;
      for (uint32_t i = 0; i != kWindowBits; ++i) {
        word |= uint64_t{counts[i] >= min_match} << i;
      }
      std::fill_n(counts, kWindowBits, uint32_t{0});
      words[w] |= word;
    }
    return Exhausted() ? doc_limits::eof() : next;
  }

  doc_id_t Fill(doc_id_t min, doc_id_t max, uint64_t* IRS_RESTRICT words,
                score_t* IRS_RESTRICT scores) {
    const auto constant = _constant;
    const auto min_match = _min_match;
    if constexpr (Lazy) {
      for (size_t w = 0; w != kWindowWords; ++w) {
        ResetTouched(std::exchange(_touched[w], uint64_t{0}),
                     scores + w * kWindowBits, constant);
      }
    }
    if (Exhausted()) {
      return doc_limits::eof();
    }
    const auto count = WindowWords(min, max);
    const auto next = _leaves.Visit(max, [&](auto& leaf) IRS_FORCE_INLINE {
      return leaf.Count(min, max, _counts, _touched.data(), scores);
    });
    for (size_t w = 0; w != count; ++w) {
      auto touched = _touched[w];
      if constexpr (!Lazy) {
        _touched[w] = 0;
      }
      if (touched == 0) {
        continue;
      }
      const auto base = w * kWindowBits;
      auto* const counts = _counts + base;
      auto* const slots = scores + base;
      uint64_t answer = 0;
      if (std::popcount(touched) >= kDenseWord) {
        answer = TallyAnswer(counts, min_match);
        std::fill_n(counts, kWindowBits, uint32_t{0});
        if constexpr (!Lazy) {
          for (uint32_t i = 0; i != kWindowBits; ++i) {
            slots[i] = ((answer >> i) & 1) != 0 ? slots[i] : constant;
          }
        }
      } else {
        while (touched != 0) {
          const auto bit = static_cast<uint32_t>(std::countr_zero(touched));
          const bool keep = counts[bit] >= min_match;
          answer |= uint64_t{keep} << bit;
          counts[bit] = 0;
          if constexpr (!Lazy) {
            slots[bit] = keep ? slots[bit] : constant;
          }
          touched = PopBit(touched);
        }
      }
      words[w] |= answer;
    }
    return Exhausted() ? doc_limits::eof() : next;
  }

 private:
  ABSL_CACHELINE_ALIGNED uint32_t _counts[kWindowDocs]{};
  Scratch _touched{};
  Leaves _leaves;
  score_t _constant;
  uint32_t _min_match;
};

}  // namespace irs::detail
