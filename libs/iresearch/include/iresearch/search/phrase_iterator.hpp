////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2016 by EMC Corporation, All Rights Reserved
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
/// Copyright holder is EMC Corporation
///
/// @author Andrey Abramov
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <algorithm>
#include <array>
#include <limits>
#include <memory>

#include "basics/empty.hpp"
#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/formats/posting/common.hpp"
#include "iresearch/formats/posting/format_block_128.hpp"
#include "iresearch/formats/posting/iterator_pos.hpp"
#include "iresearch/index/field_meta.hpp"
#include "iresearch/search/column_collector.hpp"
#include "iresearch/search/common/fixed_array.hpp"
#include "iresearch/search/score_function.hpp"
#include "iresearch/search/scorer.hpp"

namespace irs {

struct TermInterval {
  PosAttr::value_t offs_max{};
  PosAttr::value_t offs_min{};
  PosAttr::value_t lead_offset{};
  uint32_t term_group{};
};

template<bool Offs>
using FixedTermTraits = IteratorTraitsImpl<FormatTraits128, true, true, Offs>;

template<bool Offs>
using FixedTermPositionImpl = PositionImpl<FixedTermTraits<Offs>>;

template<bool Offs>
using FixedTermPosition = std::pair<FixedTermPositionImpl<Offs>*, TermInterval>;

template<typename T>
struct TermPositionTraits {
  using PositionImpl = PosAttr;

  static PosAttr::value_t Position(T& pos) {
    auto res = pos_limits::eof();
    pos.first->visit(&res, [](void* ctx, auto& it) {
      SDB_ASSERT(ctx);
      auto& position = *reinterpret_cast<PosAttr::value_t*>(ctx);
      if (pos_limits::valid(it.position->value())) {
        position = std::min(position, it.position->value());
      }
      return true;
    });
    return res;
  }

  static const TermInterval& Interval(const T& pos) noexcept {
    return pos.second;
  }

  static void ResetPos(const T&) {}
};

template<bool Offs>
struct TermPositionTraits<FixedTermPosition<Offs>> {
  using T = FixedTermPosition<Offs>;
  using PositionImpl = FixedTermPositionImpl<Offs>;

  static PosAttr::value_t Position(T& pos) noexcept {
    return pos.first->value();
  }

  static const TermInterval& Interval(const T& pos) noexcept {
    return pos.second;
  }

  static void ResetPos(const T& pos) { pos.first->reset(); }

  static const OffsAttr& Offsets(const T& pos) noexcept {
    static_assert(Offs);
    const auto* offs = irs::get<OffsAttr>(*pos.first);
    SDB_ASSERT(offs);
    return *offs;
  }
};

// clang-format off
// clang-format on

template<typename Iterator>
class SinglePositionStrategy {
 public:
  using Value = typename std::iterator_traits<Iterator>::value_type;
  using Traits = TermPositionTraits<Value>;
  using PositionImpl = Traits::PositionImpl;

  SinglePositionStrategy(Iterator& it, PositionImpl& lead_position,
                         bool = false)
    : _lead_it{it}, _lead_pos{lead_position} {}

  void NotifyNextLead(const Iterator&) noexcept {
    SDB_ASSERT(pos_limits::valid(_lead_pos.value()));
    SDB_ASSERT(!pos_limits::eof(_lead_pos.value()));
    _base_position = _lead_pos.value();
  }

  PosAttr::value_t NextPosition(const Iterator& it) noexcept {
    return _base_position + Traits::Interval(*it).offs_min;
  }

  static bool Match(PosAttr::value_t seek, PosAttr::value_t sought,
                    const Iterator&) noexcept {
    return seek == sought;
  }

  bool AdvanceIterators(bool match, PosAttr::value_t sought, const Iterator&,
                        Iterator& it) {
    if (!match) {
      SDB_ASSERT(sought > Traits::Interval(*it).lead_offset);
      _lead_pos.seek(sought - Traits::Interval(*it).lead_offset);
    }
    _base_position = sought;
    ++it;
    return match;
  }

  consteval bool NextPermutation(Iterator&, const Iterator&) { return false; }

 private:
  Iterator& _lead_it;
  PositionImpl& _lead_pos;
  PosAttr::value_t _base_position{pos_limits::eof()};
};

template<typename Iterator>
class IntervalPositionStrategy {
 public:
  using Value = typename std::iterator_traits<Iterator>::value_type;
  using Traits = TermPositionTraits<Value>;
  using PositionImpl = Traits::PositionImpl;

  IntervalPositionStrategy(Iterator& lead, PositionImpl& lead_position,
                           bool reversed = false)
    : _lead_it{lead}, _lead_pos{lead_position}, _reversed{reversed} {}

  void NotifyNextLead(const Iterator& end) noexcept {
    SDB_ASSERT(pos_limits::valid(_lead_pos.value()));
    SDB_ASSERT(!pos_limits::eof(_lead_pos.value()));
    _base_position = _lead_pos.value();
    _interval_delta = 0;
    if (_skipped) {
      for (auto reset_it = _lead_it + 1; reset_it != end; ++reset_it) {
        Traits::ResetPos(*reset_it);
      }
      _skipped = false;
    }
    _need_reset = false;
  }

  PosAttr::value_t NextPosition(const Iterator& it) {
    return Window(it).low + _interval_delta;
  }

  bool Match(PosAttr::value_t, PosAttr::value_t sought,
             const Iterator& it) const noexcept {
    return sought <= Window(it).high;
  }

  bool AdvanceIterators(bool match, PosAttr::value_t sought,
                        const Iterator& end, Iterator& it) {
    const auto fail_it = it;
    _interval_delta = 0;
    if (match) {
      ++it;
      if (_need_reset && it != end) {
        SDB_ASSERT(_skipped);
        for (auto reset_it = it; reset_it != end; ++reset_it) {
          Traits::ResetPos(*reset_it);
        }
        _need_reset = false;
      }
      SDB_ASSERT(!_need_reset || it == end);
      _base_position = sought;
      return true;
    }

    if (_skipped ? StepBack(it) : SkipBack(sought, fail_it, it)) {
      return true;
    }

    const auto bound = _skipped ? 0 : Reach(sought, _lead_it, fail_it);
    _lead_pos.seek(std::max(bound, _lead_pos.value() + 1));
    return false;
  }

  bool NextPermutation(Iterator& it, const Iterator& end) {
    SDB_ASSERT(it != _lead_it);
    const auto at_end = it == end;
    if (!at_end && !_skipped) {
      SDB_ASSERT(pos_limits::eof(Traits::Position(*it)));
      return false;
    }
    if (StepBack(it)) {
      return true;
    }

    it = end;
    return !at_end;
  }

 private:
  struct Range {
    PosAttr::value_t low;
    PosAttr::value_t high;
  };

  void Rebase(const Iterator& it) {
    const auto prev_base_it = it - 1;
    _base_position = prev_base_it == _lead_it ? _lead_pos.value()
                                              : Traits::Position(*prev_base_it);
  }

  bool SkipBack(PosAttr::value_t sought, const Iterator& fail_it,
                Iterator& it) {
    SDB_ASSERT(!_skipped);
    while (it != _lead_it + 1) {
      --it;
      Rebase(it);
      const auto window = Window(it);
      const auto want = Reach(sought, it, fail_it);
      if (want <= window.low || want > window.high) {
        continue;
      }
      _interval_delta = want - window.low;
      _skipped = true;
      return true;
    }
    return false;
  }

  bool StepBack(Iterator& it) {
    while (it != _lead_it + 1) {
      --it;
      const auto current_position = Traits::Position(*it);
      Rebase(it);
      const auto window = Window(it);
      if (current_position < window.high) {
        _need_reset = true;
        _interval_delta = current_position - window.low + 1;
        _skipped = true;
        return true;
      }
    }
    return false;
  }

  const TermInterval& Gap(const Iterator& it) const noexcept {
    return Traits::Interval(_reversed ? *(it - 1) : *it);
  }

  Range Window(const Iterator& it) const noexcept {
    const auto& gap = Gap(it);
    if (_reversed) {
      return {_base_position > gap.offs_max ? _base_position - gap.offs_max
                                            : pos_limits::min(),
              _base_position > gap.offs_min ? _base_position - gap.offs_min
                                            : pos_limits::invalid()};
    }
    return {_base_position + gap.offs_min, _base_position + gap.offs_max};
  }

  PosAttr::value_t Reach(PosAttr::value_t sought, Iterator from,
                         Iterator to) const noexcept {
    if (_reversed) {
      PosAttr::value_t span = 0;
      for (auto it = from + 1; it <= to; ++it) {
        span += Gap(it).offs_min;
      }
      return sought + span;
    }
    SDB_ASSERT(sought + Traits::Interval(*from).lead_offset >=
               Traits::Interval(*to).lead_offset);
    return sought - Traits::Interval(*to).lead_offset +
           Traits::Interval(*from).lead_offset;
  }

  Iterator& _lead_it;
  PositionImpl& _lead_pos;
  PosAttr::value_t _base_position{pos_limits::eof()};
  PosAttr::value_t _interval_delta{0};
  bool _reversed{false};
  bool _skipped{false};
  bool _need_reset{false};
};

template<typename TermPositionT, bool Offs, bool HasFreq, bool HasIntervals,
         bool HasBoost = false, size_t N = 0>
class PhraseFrequency {
 public:
  using TermPosition = TermPositionT;
  using Traits = TermPositionTraits<TermPosition>;
  using Positions = search::RunOf<TermPosition, N>;
  using ExecutionStrategy =
    std::conditional_t<HasIntervals,
                       IntervalPositionStrategy<typename Positions::iterator>,
                       SinglePositionStrategy<typename Positions::iterator>>;

  static_assert(!HasBoost || HasFreq);

  static constexpr bool kHasBoost = HasBoost;
  static constexpr bool kHasFreq = HasFreq;
  static constexpr bool kOffsets = Offs;

  explicit PhraseFrequency(size_t size) : _pos{size} {}

  TermPosition& Position(size_t i) noexcept {
    SDB_ASSERT(i < _pos.size());
    return _pos[i];
  }

  void Finish() {
    SDB_ASSERT(!_pos.empty());
    SDB_ASSERT(_pos.front().second.offs_min == 0);
    SDB_ASSERT(_pos.front().second.offs_max == 0);
    if constexpr (HasIntervals) {
      uint64_t scale = 1;
      for (const auto& slot : _pos) {
        scale *= slot.second.offs_max - slot.second.offs_min + 1;
        if (scale >= kMaxFreq) {
          scale = kMaxFreq;
          break;
        }
      }
      _freq_scale = static_cast<uint32_t>(scale);
    }
  }

  template<bool Ordered = false>
  IRS_FORCE_INLINE bool Match() {
    _phrase_freq = NextPosition<Ordered>();
    return _phrase_freq != 0;
  }

  uint32_t GetFreq() const noexcept { return _phrase_freq; }

  score_t GetBoost() const noexcept
    requires(HasBoost)
  {
    if (_phrase_freq == 0) {
      return kNoBoost;
    }
    return _phrase_boost /
           static_cast<score_t>(_pos.size() * size_t{_phrase_freq});
  }

  uint32_t DocFreqBound() {
    OrderByDocFreq();
    const auto freq = _pos.front().first->DocFreq();
    if constexpr (HasIntervals) {
      uint64_t by_window = uint64_t{freq} * _freq_scale;
      uint64_t by_occurrence = 1;
      for (const auto& slot : _pos) {
        by_occurrence *= slot.first->DocFreq();
        if (by_occurrence >= by_window) {
          return static_cast<uint32_t>(std::min<uint64_t>(by_window, kMaxFreq));
        }
      }
      return static_cast<uint32_t>(std::min<uint64_t>(by_occurrence, kMaxFreq));
    }
    return freq;
  }

  std::pair<uint32_t, uint32_t> Offsets() const noexcept
    requires(Offs)
  {
    return {Traits::Offsets(_pos.front()).start,
            Traits::Offsets(_pos.back()).end};
  }

  bool NextAlignment()
    requires(Offs)
  {
    return NextPosition() != 0;
  }

 private:
  template<bool Ordered = false>
  IRS_FORCE_INLINE uint32_t NextPosition() {
    if constexpr (HasBoost) {
      _phrase_boost = 0.f;
    }
    if constexpr (HasIntervals || Offs) {
      return NextPositionGeneric<Ordered>();
    } else {
      return NextPositionOptimized<Ordered>();
    }
  }

  IRS_FORCE_INLINE void TakeBoost() noexcept {
    if constexpr (HasBoost) {
      for (const auto& slot : _pos) {
        _phrase_boost += Traits::Boost(slot);
      }
    }
  }

  template<bool Ordered = false>
  uint32_t NextPositionGeneric() {
    if constexpr (!Ordered) {
      OrderByDocFreq();
    }
    uint32_t phrase_freq = 0;
    auto& lead = *_pos.front().first;
    lead.next();
    auto lead_it = std::begin(_pos);
    ExecutionStrategy strategy{lead_it, lead, _reversed};
    SDB_ASSERT(_pos.size() > 1);

    for (auto end = std::end(_pos); !pos_limits::eof(lead.value());) {
      strategy.NotifyNextLead(end);
      bool match = true;
      for (auto it = lead_it + 1; it != end;) {
        auto& pos = *it->first;

        const auto term_position = strategy.NextPosition(it);
        if (!pos_limits::valid(term_position)) {
          return phrase_freq;
        }
        const auto sought = pos.seek(term_position);

        if (pos_limits::eof(sought)) {
          if constexpr (HasFreq) {
            if (!strategy.NextPermutation(it, end)) {
              return phrase_freq;
            }

            if (it == end) {
              lead.next();
              match = false;
            }
            continue;
          } else {
            return phrase_freq;
          }
        }
        match = strategy.AdvanceIterators(
          strategy.Match(term_position, sought, it), sought, end, it);

        if constexpr (HasFreq) {
          if (it == end && match) {
            if (!strategy.NextPermutation(it, end)) {
              break;
            }
            ++phrase_freq;
            TakeBoost();
          }
        }
        if (!match) {
          break;
        }
      }
      if (match) {
        if constexpr (HasFreq) {
          ++phrase_freq;
          TakeBoost();
          lead.next();
        } else {
          return 1;
        }
      }
    }

    return phrase_freq;
  }

 private:
  void OrderByDocFreq() {
    if constexpr (Offs) {
    } else if constexpr (HasIntervals) {
      if (_pos.back().first->DocFreq() < _pos.front().first->DocFreq()) {
        absl::c_reverse(_pos);
        _reversed = !_reversed;
      }
    } else {
      absl::c_sort(_pos, [](const auto& l, const auto& r) {
        return l.first->DocFreq() < r.first->DocFreq();
      });
    }
  }

  template<bool Ordered = false>
  uint32_t NextPositionOptimized() {
    if constexpr (!Ordered) {
      OrderByDocFreq();
    }
    auto begin = _pos.begin();
    auto end = _pos.end();

    const auto new_lead_offset = begin->second.lead_offset;
    auto& lead = *begin->first;
    ++begin;
    auto lead_pos = lead.seek(pos_limits::min() + new_lead_offset);

    uint32_t phrase_freq = 0;
    while (true) {
    restart:
      if (pos_limits::eof(lead_pos)) [[unlikely]] {
        return phrase_freq;
      }
      for (auto it = begin; it != end; ++it) {
        const auto target =
          (lead_pos - new_lead_offset) + it->second.lead_offset;
        const auto sought = it->first->seek(target);
        if (sought != target) {
          if (pos_limits::eof(sought)) [[unlikely]] {
            return phrase_freq;
          }
          lead_pos =
            lead.seek((sought - it->second.lead_offset) + new_lead_offset);
          goto restart;
        }
      }
      if constexpr (HasFreq) {
        ++phrase_freq;
        TakeBoost();
        lead.next();
        lead_pos = lead.value();
      } else {
        return 1;
      }
    }
  }

  Positions _pos;
  uint32_t _phrase_freq = 0;
  [[no_unique_address]] utils::Need<HasBoost, score_t> _phrase_boost{};
  uint32_t _freq_scale = 1;
  bool _reversed = false;
};

template<bool Offs, bool HasFreq, bool HasIntervals, size_t N = 0>
using FixedPhraseFrequency = PhraseFrequency<FixedTermPosition<Offs>, Offs,
                                             HasFreq, HasIntervals, false, N>;

}  // namespace irs
