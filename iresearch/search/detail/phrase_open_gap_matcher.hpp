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

#include <absl/algorithm/container.h>

#include <algorithm>
#include <cstdint>
#include <limits>
#include <numeric>
#include <span>
#include <utility>
#include <vector>

#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/formats/posting/iterator_pos.hpp"
#include "iresearch/index/iterators.hpp"
#include "iresearch/search/detail/phrase_matcher.hpp"
#include "iresearch/search/scorers/score_function.hpp"
#include "iresearch/utils/containers/fixed.hpp"
#include "iresearch/utils/empty.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs {
namespace detail::open_gap {

struct Gap {
  PosAttr::value_t lo;
  PosAttr::value_t hi;
};

struct Result {
  uint64_t freq = 0;
  double weight = 0.0;
  score_t boost = 0.f;
  bool any = false;
};

struct Chain {
  PosAttr::value_t end = 0;
  PosAttr::value_t start = 0;
  uint32_t start_offset = 0;
  score_t boost = kNoBoost;
};

struct Boosted {
  PosAttr::value_t pos = 0;
  score_t boost = kNoBoost;
};

struct BoostWindow {
  std::vector<Boosted> entries;
  size_t head = 0;
  PosAttr::value_t start = 0;
};

template<bool HasBoost>
struct WalkSlot {
  PosAttr::value_t pos = pos_limits::eof();
  PosAttr::value_t bound = 0;
  bool started = false;
  bool has_chain = false;
  Chain chain;
  [[no_unique_address]] utils::Need<HasBoost, BoostWindow> window;
};

struct SpanOffsets {
  uint32_t start;
  uint32_t end;
};

inline PosAttr::value_t MinExtent(std::span<const Gap> gaps) noexcept {
  PosAttr::value_t extent = 1;
  for (const auto& gap : gaps.subspan(1)) {
    extent += gap.lo;
  }
  return extent;
}

inline bool Reaches(const Gap& gap, PosAttr::value_t from,
                    PosAttr::value_t to) noexcept {
  return pos_limits::IsOpenGap(gap.hi) ||
         static_cast<uint64_t>(from) + gap.hi >= to;
}

inline void Remember(BoostWindow& window, const Chain& chain) {
  if (window.head == window.entries.size() || window.start != chain.start) {
    window.entries.clear();
    window.head = 0;
    window.start = chain.start;
  }
  while (window.entries.size() != window.head &&
         window.entries.back().boost <= chain.boost) {
    window.entries.pop_back();
  }
  window.entries.emplace_back(chain.end, chain.boost);
}

inline score_t Strongest(BoostWindow& window, const Gap& gap,
                         PosAttr::value_t to) noexcept {
  while (!Reaches(gap, window.entries[window.head].pos, to)) {
    ++window.head;
    SDB_ASSERT(window.head < window.entries.size());
  }
  return window.entries[window.head].boost;
}

template<typename Source, bool Offs, bool HasBoost>
class Walker {
 public:
  Walker(Source& source, std::span<const Gap> gaps,
         std::vector<WalkSlot<HasBoost>>& slots)
    : _source{source}, _gaps{gaps}, _slots{slots} {
    SDB_ASSERT(_slots.size() == gaps.size() - 1);
    for (auto& slot : _slots) {
      slot.started = false;
      slot.has_chain = false;
      if constexpr (HasBoost) {
        slot.window.entries.clear();
        slot.window.head = 0;
      }
    }
  }

  IRS_FORCE_INLINE const Chain* LatestChain(size_t top, PosAttr::value_t to) {
    SDB_ASSERT(to >= _gaps[top + 1].lo);
    _slots[top].bound = to - _gaps[top + 1].lo;
    size_t i = top;
    while (true) {
      if (HasPositionInBound(i)) {
        i = StartOrDescend(i);
      } else if (i == top) {
        return ChainReaching(top, to);
      } else {
        ExtendChain(++i);
      }
    }
  }

 private:
  bool HasPositionInBound(size_t i) {
    auto& slot = _slots[i];
    if (!slot.started) {
      slot.started = true;
      slot.pos = _source.Next(i);
    }
    return !pos_limits::eof(slot.pos) && slot.pos <= slot.bound;
  }

  size_t StartOrDescend(size_t i) {
    auto& slot = _slots[i];
    if (i == 0) {
      Chain chain{.end = slot.pos, .start = slot.pos};
      if constexpr (Offs) {
        chain.start_offset = _source.StartOffset(0);
      }
      if constexpr (HasBoost) {
        chain.boost = _source.Boost(0);
      }
      SetChain(slot, chain);
      slot.pos = _source.Next(0);
      return 0;
    }
    if (slot.pos < _gaps[i].lo) {
      slot.pos = _source.Next(i);
      return i;
    }
    _slots[i - 1].bound = slot.pos - _gaps[i].lo;
    return i - 1;
  }

  void ExtendChain(size_t i) {
    auto& slot = _slots[i];
    if (const auto* prev = ChainReaching(i - 1, slot.pos)) {
      Chain chain{
        .end = slot.pos,
        .start = prev->start,
        .start_offset = prev->start_offset,
      };
      if constexpr (HasBoost) {
        chain.boost = std::min(prev->boost, _source.Boost(i));
      }
      SetChain(slot, chain);
    }
    slot.pos = _source.Next(i);
  }

  void SetChain(WalkSlot<HasBoost>& slot, const Chain& chain) {
    slot.chain = chain;
    slot.has_chain = true;
    if constexpr (HasBoost) {
      Remember(slot.window, chain);
    }
  }

  const Chain* ChainReaching(size_t i, PosAttr::value_t to) {
    auto& slot = _slots[i];
    const auto& gap = _gaps[i + 1];
    if (!slot.has_chain || !Reaches(gap, slot.chain.end, to)) {
      return nullptr;
    }
    if constexpr (HasBoost) {
      _strongest = slot.chain;
      _strongest.boost = Strongest(slot.window, gap, to);
      return &_strongest;
    } else {
      return &slot.chain;
    }
  }

  Source& _source;
  std::span<const Gap> _gaps;
  std::vector<WalkSlot<HasBoost>>& _slots;
  [[no_unique_address]] utils::Need<HasBoost, Chain> _strongest;
};

inline bool AllOpen(std::span<const Gap> gaps) noexcept {
  return absl::c_all_of(gaps.subspan(1), [](const Gap& gap) {
    return pos_limits::IsOpenGap(gap.hi);
  });
}

template<typename Source>
bool Exists(Source& source, std::span<const Gap> gaps) {
  auto pos = source.Next(0);
  for (size_t i = 1; i != gaps.size() && !pos_limits::eof(pos); ++i) {
    const auto target = uint64_t{pos} + gaps[i].lo;
    if (target >= pos_limits::eof()) {
      return false;
    }
    pos = source.Seek(i, static_cast<PosAttr::value_t>(target));
  }
  return !pos_limits::eof(pos);
}

template<bool Offs, bool HasBoost, typename Source, typename Collect>
IRS_FORCE_INLINE Result Match(Source& source, std::span<const Gap> gaps,
                              PosAttr::value_t min_extent, bool early_exit,
                              std::vector<WalkSlot<HasBoost>>& slots,
                              Collect&& collect) {
  Result res;
  const size_t n = gaps.size();
  SDB_ASSERT(n >= 2);
  if (early_exit && AllOpen(gaps)) {
    res.any = Exists(source, gaps);
    res.freq = res.any ? 1 : 0;
    return res;
  }
  const auto& last = gaps[n - 1];
  PosAttr::value_t last_start = 0;
  double weight = 0.0;
  const auto count = [&](const Chain& chain, PosAttr::value_t end) {
    if (res.any && chain.start <= last_start) {
      return false;
    }
    res.any = true;
    last_start = chain.start;
    const auto width = uint64_t{end} - chain.start + 1;
    SDB_ASSERT(width >= min_extent);
    ++res.freq;
    weight += 1.0 / static_cast<double>(1 + width - min_extent);
    if constexpr (HasBoost) {
      res.boost =
        std::max(res.boost, std::min(chain.boost, source.Boost(n - 1)));
    }
    if constexpr (Offs) {
      collect(chain.start, end, chain.start_offset, source.EndOffset(n - 1));
    } else {
      collect(chain.start, end, 0u, 0u);
    }
    return true;
  };

  Walker<Source, Offs, HasBoost> walker{source, gaps, slots};
  for (auto end = source.Next(n - 1); !pos_limits::eof(end);
       end = source.Next(n - 1)) {
    if (end < last.lo) {
      continue;
    }
    const auto* chain = walker.LatestChain(n - 2, end);
    if (chain && count(*chain, end) && early_exit) {
      break;
    }
  }
  res.weight = weight;
  return res;
}

struct Scratch {
  std::vector<size_t> next;
  std::vector<WalkSlot<false>> walk;
};

class VectorSource {
 public:
  VectorSource(const std::vector<std::vector<PosAttr::value_t>>& slots,
               std::vector<size_t>& next)
    : _slots{slots}, _next{next} {
    _next.assign(slots.size(), 0);
  }

  PosAttr::value_t Next(size_t i) {
    const auto& slot = _slots[i];
    return _next[i] < slot.size() ? slot[_next[i]++] : pos_limits::eof();
  }

  PosAttr::value_t Seek(size_t i, PosAttr::value_t target) {
    const auto& slot = _slots[i];
    auto& next = _next[i];
    while (next < slot.size() && slot[next] < target) {
      ++next;
    }
    return next < slot.size() ? slot[next++] : pos_limits::eof();
  }

  uint32_t StartOffset(size_t) const noexcept { return 0; }

  uint32_t EndOffset(size_t) const noexcept { return 0; }

  score_t Boost(size_t) const noexcept { return kNoBoost; }

 private:
  const std::vector<std::vector<PosAttr::value_t>>& _slots;
  std::vector<size_t>& _next;
};

inline Result Run(const std::vector<std::vector<PosAttr::value_t>>& slots,
                  std::span<const Gap> gaps, bool early_exit,
                  Scratch& scratch) {
  SDB_ASSERT(gaps.size() == slots.size());
  if (slots.size() < 2) {
    return {};
  }
  VectorSource source{slots, scratch.next};
  scratch.walk.resize(gaps.size() - 1);
  return Match<false, false>(
    source, gaps, MinExtent(gaps), early_exit, scratch.walk,
    [](PosAttr::value_t, PosAttr::value_t, uint32_t, uint32_t) {});
}

template<typename Positions, bool Offs, bool HasBoost>
class IterSource {
 public:
  using Traits = TermPositionTraits<typename Positions::value_type>;

  explicit IterSource(Positions& pos) : _pos{pos} {}

  PosAttr::value_t Next(size_t i) {
    auto& it = *_pos[i].first;
    return it.next() ? it.value() : pos_limits::eof();
  }

  PosAttr::value_t Seek(size_t i, PosAttr::value_t target) {
    return _pos[i].first->seek(target);
  }

  uint32_t StartOffset(size_t i) const noexcept
    requires(Offs)
  {
    return Traits::Offsets(_pos[i]).start;
  }

  uint32_t EndOffset(size_t i) const noexcept
    requires(Offs)
  {
    return Traits::Offsets(_pos[i]).end;
  }

  score_t Boost(size_t i) const noexcept {
    if constexpr (HasBoost) {
      return Traits::Boost(_pos[i]);
    } else {
      return kNoBoost;
    }
  }

 private:
  Positions& _pos;
};

}  // namespace detail::open_gap

template<typename TermPositionT, bool Offs, bool HasFreq, size_t N = 0,
         bool HasBoost = false>
class PhraseOpenGapMatcher {
 public:
  using TermPosition = TermPositionT;
  using Traits = TermPositionTraits<TermPosition>;
  using Positions = containers::Fixed<TermPosition, N>;

  static constexpr bool kHasScale = HasFreq;
  static constexpr bool kHasFreq = HasFreq;
  static constexpr bool kOffsets = Offs;

  static_assert(HasFreq || !HasBoost);

  explicit PhraseOpenGapMatcher(size_t size) : _pos{size}, _gaps{size} {}

  TermPosition& Position(size_t i) noexcept {
    SDB_ASSERT(i < _pos.size());
    return _pos[i];
  }

  void Finish() {
    SDB_ASSERT(_pos.size() >= 2);
    for (size_t i = 0; i != _pos.size(); ++i) {
      const auto& interval = _pos[i].second;
      _gaps[i] = {interval.offs_min, interval.offs_max};
    }
    _min_extent = detail::open_gap::MinExtent({_gaps.data(), _gaps.size()});
    _walk.resize(_pos.size() - 1);
  }

  template<bool Ordered = false>
  IRS_FORCE_INLINE bool Match() {
    _phrase_freq = 0;
    if constexpr (Offs) {
      _start_offset = 0;
      _end_offset = 0;
      _matches.clear();
      _match_idx = 0;
    }
    detail::open_gap::IterSource<Positions, Offs, HasBoost> source{_pos};
    const auto res = detail::open_gap::Match<Offs, HasBoost>(
      source, {_gaps.data(), _gaps.size()}, _min_extent, !HasFreq, _walk,
      [&]([[maybe_unused]] PosAttr::value_t start,
          [[maybe_unused]] PosAttr::value_t end,
          [[maybe_unused]] uint32_t start_offset,
          [[maybe_unused]] uint32_t end_offset) {
        if constexpr (Offs && HasFreq) {
          _matches.push_back({start_offset, end_offset});
        }
      });
    if (!res.any) {
      return false;
    }
    Take(res);
    return true;
  }

  uint32_t GetFreq() const noexcept { return _phrase_freq; }

  uint32_t DocFreqBound()
    requires(HasFreq)
  {
    uint32_t bound = std::numeric_limits<uint32_t>::max();
    for (const auto& slot : _pos) {
      bound = std::min(bound, slot.first->DocFreq());
    }
    return bound;
  }

  uint32_t FreqBoundOf(uint32_t freq) const noexcept
    requires(HasFreq)
  {
    return freq;
  }

  score_t GetScale() const noexcept
    requires(kHasScale)
  {
    if constexpr (HasBoost) {
      return _phrase_scale * _phrase_boost;
    } else {
      return _phrase_scale;
    }
  }

  std::pair<uint32_t, uint32_t> Offsets() const noexcept
    requires(Offs)
  {
    return {_start_offset, _end_offset};
  }

  bool NextAlignment()
    requires(Offs)
  {
    if constexpr (!HasFreq) {
      return false;
    } else {
      if (_match_idx >= _matches.size()) {
        return false;
      }
      _start_offset = _matches[_match_idx].start;
      _end_offset = _matches[_match_idx].end;
      ++_match_idx;
      return true;
    }
  }

 private:
  void Take(const detail::open_gap::Result& res) {
    if constexpr (HasFreq) {
      _phrase_freq = std::saturate_cast<uint32_t>(res.freq);
      _phrase_scale =
        static_cast<score_t>(res.weight / static_cast<double>(res.freq));
      if constexpr (HasBoost) {
        _phrase_boost = res.boost;
      }
      if constexpr (Offs) {
        SDB_ASSERT(static_cast<uint32_t>(_matches.size()) == _phrase_freq);
        _start_offset = _matches[0].start;
        _end_offset = _matches[0].end;
        _match_idx = 1;
      }
    } else {
      _phrase_freq = 1;
      _phrase_scale = kNoBoost;
    }
  }

  Positions _pos;
  containers::Fixed<detail::open_gap::Gap, N> _gaps;
  PosAttr::value_t _min_extent = 1;
  std::vector<detail::open_gap::WalkSlot<HasBoost>> _walk;
  std::vector<detail::open_gap::SpanOffsets> _matches;
  size_t _match_idx = 0;

  uint32_t _phrase_freq = 0;
  score_t _phrase_scale = kNoBoost;
  [[no_unique_address]] utils::Need<HasBoost, score_t> _phrase_boost{};
  uint32_t _start_offset{0};
  uint32_t _end_offset{0};
};

template<bool Offs, bool HasFreq, size_t N = 0>
using FixedPhraseOpenGapMatcher =
  PhraseOpenGapMatcher<FixedTermPosition<Offs>, Offs, HasFreq, N>;

}  // namespace irs
