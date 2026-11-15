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

#include <absl/algorithm/container.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <iresearch/search/detail/phrase_open_gap_matcher.hpp>
#include <iresearch/utils/type_limits.hpp>
#include <random>
#include <span>
#include <string>
#include <utility>
#include <vector>

#include "phrase_reference.hpp"

namespace {

namespace ord = irs::detail::open_gap;
namespace ref = tests::phrase_ref;
using value_t = irs::PosAttr::value_t;
using Slots = std::vector<std::vector<value_t>>;
using Spans = std::vector<std::pair<value_t, value_t>>;

constexpr value_t kOpen = irs::pos_limits::kOpenGap;

ord::Gap Exact(value_t d) { return {d, d}; }
ord::Gap Range(value_t lo, value_t hi) { return {lo, hi}; }
ord::Gap Open(value_t lo) { return {lo, kOpen}; }

std::string Describe(const Slots& slots, const std::vector<ord::Gap>& gaps) {
  std::string out;
  for (size_t i = 0; i < slots.size(); ++i) {
    if (i > 0) {
      out += gaps[i].hi == kOpen ? " [" + std::to_string(gaps[i].lo) + ",inf] "
                                 : " [" + std::to_string(gaps[i].lo) + "," +
                                     std::to_string(gaps[i].hi) + "] ";
    }
    out += "{";
    for (const auto p : slots[i]) {
      out += std::to_string(p) + ",";
    }
    out += "}";
  }
  return out;
}

ord::Result RunSpans(const Slots& slots, const std::vector<ord::Gap>& gaps,
                     ord::Scratch& scratch, Spans& spans) {
  spans.clear();
  if (slots.size() < 2) {
    return {};
  }
  ord::VectorSource source{slots, scratch.next};
  scratch.walk.resize(gaps.size() - 1);
  return ord::Match<false, false>(
    source, gaps, ord::MinExtent(gaps), false, scratch.walk,
    [&](value_t start, value_t end, uint32_t, uint32_t) {
      spans.emplace_back(start, end);
    });
}

void Check(const Slots& slots, const std::vector<ord::Gap>& gaps) {
  SCOPED_TRACE(Describe(slots, gaps));
  std::vector<ref::Gap> ref_gaps;
  for (const auto g : gaps) {
    ref_gaps.push_back({g.lo, g.hi});
  }
  const auto expected = ref::Enumerate(slots, ref_gaps);

  ord::Scratch scratch;
  Spans spans;
  const auto res = RunSpans(slots, gaps, scratch, spans);
  EXPECT_EQ(res.any, expected.matched);
  EXPECT_EQ(res.freq, expected.freq);
  EXPECT_NEAR(res.weight, expected.weight, 1e-9);
  Spans expected_spans;
  for (const auto s : expected.spans) {
    expected_spans.emplace_back(s.start, s.end);
  }
  EXPECT_EQ(spans, expected_spans);

  const auto early = ord::Run(slots, gaps, true, scratch);
  EXPECT_EQ(early.any, expected.matched);
  if (early.any) {
    EXPECT_EQ(early.freq, 1U);
  }
}

void ExpectSpans(const Slots& slots, const std::vector<ord::Gap>& gaps,
                 const Spans& want) {
  ord::Scratch scratch;
  Spans spans;
  const auto res = RunSpans(slots, gaps, scratch, spans);
  EXPECT_EQ(spans, want);
  EXPECT_EQ(res.freq, want.size());
  EXPECT_EQ(res.any, !want.empty());
  Check(slots, gaps);
}

struct At {
  value_t pos = 0;
  irs::score_t boost = irs::kNoBoost;
};

using BoostedSlots = std::vector<std::vector<At>>;

class BoostedSource {
 public:
  explicit BoostedSource(const BoostedSlots& slots)
    : _slots{slots}, _next(slots.size()), _boost(slots.size()) {}

  value_t Next(size_t i) {
    if (_next[i] == _slots[i].size()) {
      return irs::pos_limits::eof();
    }
    const auto& at = _slots[i][_next[i]++];
    _boost[i] = at.boost;
    return at.pos;
  }

  value_t Seek(size_t, value_t) {
    ADD_FAILURE();
    return irs::pos_limits::eof();
  }

  uint32_t StartOffset(size_t) const noexcept { return 0; }

  uint32_t EndOffset(size_t) const noexcept { return 0; }

  irs::score_t Boost(size_t i) const noexcept { return _boost[i]; }

 private:
  const BoostedSlots& _slots;
  std::vector<size_t> _next;
  std::vector<irs::score_t> _boost;
};

ord::Result MatchBoosted(const BoostedSlots& slots,
                         const std::vector<ord::Gap>& gaps) {
  value_t min_extent = 1;
  for (size_t i = 1; i != gaps.size(); ++i) {
    min_extent += gaps[i].lo;
  }
  BoostedSource source{slots};
  std::vector<ord::WalkSlot<true>> levels(gaps.size() - 1);
  return ord::Match<false, true>(source, gaps, min_extent, false, levels,
                                 [](auto, auto, auto, auto) {});
}

irs::score_t ReferenceBoost(const BoostedSlots& slots,
                            const std::vector<ord::Gap>& gaps) {
  Slots positions;
  std::vector<ref::Gap> ref_gaps;
  for (size_t i = 0; i != slots.size(); ++i) {
    auto& slot = positions.emplace_back();
    for (const auto at : slots[i]) {
      slot.push_back(at.pos);
    }
    ref_gaps.push_back({gaps[i].lo, gaps[i].hi});
  }
  irs::score_t best = 0.f;
  for (const auto span : ref::Enumerate(positions, ref_gaps).spans) {
    std::vector<At> chain(slots.size());
    auto walk = [&](auto& self, size_t i) -> void {
      if (i == slots.size()) {
        if (chain.back().pos == span.end) {
          irs::score_t weakest = irs::kNoBoost;
          for (const auto at : chain) {
            weakest = std::min(weakest, at.boost);
          }
          best = std::max(best, weakest);
        }
        return;
      }
      for (const auto at : slots[i]) {
        if (i == 0 ? at.pos != span.start
                   : !ord::Reaches(gaps[i], chain[i - 1].pos, at.pos) ||
                       int64_t{at.pos} - chain[i - 1].pos < gaps[i].lo) {
          continue;
        }
        chain[i] = at;
        self(self, i + 1);
      }
    };
    walk(walk, 0);
  }
  return best;
}

}  // namespace

TEST(PhraseOpenGapMatcher, TwoSlotsOpen) {
  ExpectSpans({{1, 5}, {3, 8}}, {{0, 0}, Open(1)}, {{1, 3}, {5, 8}});
}

TEST(PhraseOpenGapMatcher, NestedCandidatesKeepInnermost) {
  ExpectSpans({{1, 2}, {3}}, {{0, 0}, Open(1)}, {{2, 3}});
  ExpectSpans({{1}, {2, 3}}, {{0, 0}, Open(1)}, {{1, 2}});
}

TEST(PhraseOpenGapMatcher, RepeatedWordNeedsDistinctPositions) {
  const std::vector<value_t> a{2, 4, 9};
  ExpectSpans({a, a}, {{0, 0}, Open(1)}, {{2, 4}, {4, 9}});
  ExpectSpans({{3}, {3}}, {{0, 0}, Open(1)}, {});
}

TEST(PhraseOpenGapMatcher, ZeroLowerBoundAllowsSamePosition) {
  ExpectSpans({{3}, {3}}, {{0, 0}, Open(0)}, {{3, 3}});
  ExpectSpans({{3, 7}, {3, 7}}, {{0, 0}, Open(0)}, {{3, 3}, {7, 7}});
}

TEST(PhraseOpenGapMatcher, ExactThenOpen) {
  ExpectSpans({{1, 10}, {2, 11}, {5, 20}}, {{0, 0}, Exact(1), Open(1)},
              {{1, 5}, {10, 20}});
}

TEST(PhraseOpenGapMatcher, RangeThenOpen) {
  ExpectSpans({{1, 10}, {3, 14}, {6, 30}}, {{0, 0}, Range(1, 3), Open(1)},
              {{1, 6}});
}

TEST(PhraseOpenGapMatcher, OpenThenExact) {
  ExpectSpans({{1, 4}, {6, 9}, {7}}, {{0, 0}, Open(2), Exact(1)}, {{4, 7}});
}

TEST(PhraseOpenGapMatcher, LowerBoundOnOpenJunction) {
  ExpectSpans({{1, 4}, {3, 8}}, {{0, 0}, Open(3)}, {{4, 8}});
}

TEST(PhraseOpenGapMatcher, SlotsFarApart) {
  ExpectSpans({{1}, {40'000'000}}, {{0, 0}, Open(1)}, {{1, 40'000'000}});
  ExpectSpans({{16'777'216}, {16'777'217, 80'000'000}}, {{0, 0}, Open(1)},
              {{16'777'216, 16'777'217}});
  ExpectSpans({{1, 40'000'000}, {2, 4'000'000'000U}}, {{0, 0}, Open(1)},
              {{1, 2}, {40'000'000, 4'000'000'000U}});
}

TEST(PhraseOpenGapMatcher, EmptySlotNeverMatches) {
  ExpectSpans({{1, 2}, {}}, {{0, 0}, Open(1)}, {});
  ExpectSpans({{}, {1, 2}}, {{0, 0}, Open(1)}, {});
  ExpectSpans({{1}, {}, {5}}, {{0, 0}, Exact(1), Open(1)}, {});
}

TEST(PhraseOpenGapMatcher, EarlyExitReportsAny) {
  const Slots hit{{1, 5}, {3, 8}};
  const Slots miss{{9}, {3, 8}};
  const std::vector<ord::Gap> gaps{{0, 0}, Open(1)};
  ord::Scratch scratch;
  EXPECT_TRUE(ord::Run(hit, gaps, true, scratch).any);
  EXPECT_FALSE(ord::Run(miss, gaps, true, scratch).any);
}

TEST(PhraseOpenGapMatcher, WeightDecaysWithWidth) {
  const Slots slots{{1, 10}, {2, 30}};
  const std::vector<ord::Gap> gaps{{0, 0}, Open(1)};
  ord::Scratch scratch;
  const auto res = ord::Run(slots, gaps, false, scratch);
  EXPECT_EQ(res.freq, 2U);
  EXPECT_DOUBLE_EQ(res.weight, 1.0 + 1.0 / 20.0);
  Check(slots, gaps);
}

TEST(PhraseOpenGapMatcher, RandomAgainstReference) {
  std::mt19937 rng{20260};
  const auto uniform = [&](uint32_t lo, uint32_t hi) {
    return std::uniform_int_distribution<uint32_t>{lo, hi}(rng);
  };

  for (size_t iter = 0; iter < 4000; ++iter) {
    const size_t n = uniform(2, 5);
    Slots slots(n);
    for (auto& slot : slots) {
      const uint32_t density =
        uniform(0, 2) == 0 ? 0 : (uniform(0, 1) == 0 ? 80 : 20);
      for (value_t p = 1; p <= 30; ++p) {
        if (uniform(0, 99) < density) {
          slot.push_back(p);
        }
      }
    }

    std::vector<ord::Gap> gaps(n, ord::Gap{0, 0});
    for (size_t i = 1; i < n; ++i) {
      switch (uniform(0, 2)) {
        case 0: {
          gaps[i] = Exact(uniform(0, 3));
        } break;
        case 1: {
          const value_t lo = uniform(0, 2);
          gaps[i] = Range(lo, lo + uniform(0, 3));
        } break;
        default: {
          gaps[i] = Open(uniform(0, 3));
        } break;
      }
    }
    if (absl::c_none_of(std::span{gaps}.subspan(1),
                        [](ord::Gap g) { return g.hi == kOpen; })) {
      gaps[uniform(1, n - 1)] = Open(uniform(0, 3));
    }
    Check(slots, gaps);
    if (HasFatalFailure() || HasFailure()) {
      return;
    }
  }
}

TEST(PhraseOpenGapMatcher, LongPhraseKeepsTheStackFlat) {
  constexpr size_t kSlots = 200'000;
  const Slots slots(kSlots, std::vector<value_t>{1});
  std::vector<ord::Gap> gaps(kSlots, Open(0));
  gaps[0] = {0, 0};
  ord::Scratch scratch;
  const auto res = ord::Run(slots, gaps, false, scratch);
  EXPECT_EQ(res.freq, 1U);
  EXPECT_DOUBLE_EQ(res.weight, 1.0);
}

TEST(PhraseOpenGapMatcher, StrongerAlternativeWinsInsideTheSpan) {
  const std::vector<ord::Gap> gaps{{0, 0}, Open(1), Range(1, 2)};
  const BoostedSlots exact_first{{{1, 1.f}}, {{2, 1.f}, {3, 0.5f}}, {{4, 1.f}}};
  const BoostedSlots fuzzy_first{{{1, 1.f}}, {{2, 0.5f}, {3, 1.f}}, {{4, 1.f}}};
  for (const auto* slots : {&exact_first, &fuzzy_first}) {
    const auto res = MatchBoosted(*slots, gaps);
    EXPECT_EQ(res.freq, 1U);
    EXPECT_FLOAT_EQ(res.boost, 1.f);
  }
}

TEST(PhraseOpenGapMatcher, StrongerAlternativeMustReachTheNextWord) {
  const std::vector<ord::Gap> gaps{{0, 0}, Open(1), Range(1, 2)};
  const BoostedSlots slots{{{1, 1.f}}, {{2, 1.f}, {3, 0.5f}}, {{5, 1.f}}};
  const auto res = MatchBoosted(slots, gaps);
  EXPECT_EQ(res.freq, 1U);
  EXPECT_FLOAT_EQ(res.boost, 0.5f);
}

TEST(PhraseOpenGapMatcher, RandomBoostsAgainstReference) {
  std::mt19937 rng{20261};
  const auto uniform = [&](uint32_t lo, uint32_t hi) {
    return std::uniform_int_distribution<uint32_t>{lo, hi}(rng);
  };
  constexpr irs::score_t kBoosts[] = {0.25f, 0.5f, 0.75f, 1.f};

  for (size_t iter = 0; iter < 4000; ++iter) {
    const size_t n = uniform(2, 5);
    BoostedSlots slots(n);
    for (auto& slot : slots) {
      for (value_t p = 1; p <= 20; ++p) {
        if (uniform(0, 99) < 40) {
          slot.push_back({p, kBoosts[uniform(0, 3)]});
        }
      }
    }
    std::vector<ord::Gap> gaps(n, ord::Gap{0, 0});
    for (size_t i = 1; i < n; ++i) {
      const value_t lo = uniform(0, 2);
      gaps[i] = uniform(0, 1) == 0 ? Open(lo) : Range(lo, lo + uniform(0, 3));
    }
    gaps[uniform(1, n - 1)].hi = kOpen;
    const auto res = MatchBoosted(slots, gaps);
    ASSERT_FLOAT_EQ(res.boost, ReferenceBoost(slots, gaps)) << iter;
  }
}
