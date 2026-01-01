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

#include "disjunction.hpp"
#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/index/index_reader.hpp"

namespace irs {

template<typename Frequency>
class PhrasePosition final : public PosAttr, public Frequency {
 public:
  explicit PhrasePosition(
    std::vector<typename Frequency::TermPosition>&& pos) noexcept
    : Frequency{std::move(pos)} {
    std::tie(_start, _end) = this->GetOffsets();
  }

  Attribute* GetMutable(TypeInfo::type_id type) noexcept final {
    return type == irs::Type<OffsAttr>::id() ? &_offset : nullptr;
  }

  bool next() final {
    if (!_left) {
      // At least 1 position is always approved by the phrase,
      // and calling next() on exhausted iterator is UB.
      _left = 1;
      _value = irs::pos_limits::invalid();
      return false;
    }
    ++_value;
    _offset.start = *_start;
    _offset.end = *_end;
    _left += this->NextPosition() - 1;
    return true;
  }

 private:
  OffsAttr _offset;
  const uint32_t* _start{};
  const uint32_t* _end{};
  uint32_t _left{1};
};

template<typename T>
struct HasPosition : std::false_type {};

template<typename T>
struct HasPosition<PhrasePosition<T>> : std::true_type {};

struct TermInterval {
  PosAttr::value_t offs_max{};
  PosAttr::value_t offs_min{};
  PosAttr::value_t lead_offset{};
};

// position attribute + desired offset in the phrase
using FixedTermPosition = std::pair<PosAttr::ref, TermInterval>;

template<typename Tp>
struct TermPositionTraits {
  static PosAttr::value_t Position(Tp& pos) {
    PosAttr::value_t res = pos_limits::eof();
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

  static const TermInterval& Interval(const Tp& pos) noexcept {
    return pos.second;
  }

  static void ResetPos(const Tp&) {
    // variadic resets anyway.
    // FIXME (Dronplane) maybe it would be possible to avoid constant
    // resetting e.g. reset only at the beginning of VisitLead but all.
    // Will need to have some kind of visit_all interface for disjunction.
  }
};

template<>
struct TermPositionTraits<FixedTermPosition> {
  static PosAttr::value_t Position(FixedTermPosition& pos) noexcept {
    return pos.first.get().value();
  }

  static const TermInterval& Interval(const FixedTermPosition& pos) noexcept {
    return pos.second;
  }

  static void ResetPos(const FixedTermPosition& pos) {
    pos.first.get().reset();
  }
};

// clang-format off
// Phrase execution strategies.
// Strategy in principle controls lead position movements and generates
// positions for followers. Also strategy controls building permutations if
// requested. But due to current design strategy does not know how to move
// followers. So this is done outside.
//
// Strategy methods&invariants
// - NotifyNextLead(const Iterator& end)
//  Should be called each time new valid lead position is taken and new loop
//  over followers is started. All previous state is discarded. Follower
//  positions might be resetted if permutations were built to avoid skipping
//  matches.
//
// - NextPosition(const Iterator& it)
//  Calculates next position to seek for current follower (it)
//
// - Match(PosAttr::value_t seek, PosAttr::value_t sought, const TermInterval& interval)
//   Determines if sought is a valid match given seek and interval. Separated
//   from AdvanceIterators as Match in variadic phrase is determined in separate
//   code.
//
// - AdvanceIterators(bool match, PosAttr::value_t sought, const Iterator& end, Iterator& it)
//   Sets "it" to next follower to move. Not necessary next one in phrase. Might
//   set it = end if nothing to move. Also might adjust lead position if
//   necessary to get next match. Returns true if it is still a match. Might
//   return true even if initially match flag is false (e.g. strategy decided
//   that we still can continue despite missed follower position)
//
// - NextPermutation(Iterator& it, const Iterator& end)
//   Starts next permutation of current match if possible. And adjusts "it" to
//   the next movable follower. Returns if new permutation could be started.
//   Might set it = end and return true if permutaion could not be started and
//   we must just continue searching for matches with next lead position(corner
//   case).
// clang-format on

template<typename Iterator>
class SinglePositionStrategy {
 public:
  using Traits =
    TermPositionTraits<typename std::iterator_traits<Iterator>::value_type>;

  SinglePositionStrategy(Iterator& it, PosAttr& lead_position)
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
                    const TermInterval&) noexcept {
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

  consteval bool NextPermutation(Iterator&, const Iterator&) {
    // just start next phrase search
    return false;
  }

 private:
  Iterator& _lead_it;
  PosAttr& _lead_pos;
  PosAttr::value_t _base_position{pos_limits::eof()};
};

template<typename Iterator>
class IntervalPositionStrategy {
 public:
  using Traits =
    TermPositionTraits<typename std::iterator_traits<Iterator>::value_type>;

  IntervalPositionStrategy(Iterator& lead, PosAttr& lead_position)
    : _lead_it{lead}, _lead_pos{lead_position} {}

  void NotifyNextLead(const Iterator& end) noexcept {
    SDB_ASSERT(pos_limits::valid(_lead_pos.value()));
    SDB_ASSERT(!pos_limits::eof(_lead_pos.value()));
    _base_position = _lead_pos.value();
    _interval_delta = 0;
    if (_permutations) {
      // it is a new lead during permutations. Reset all iterators except lead
      // as we might skipped something during building permutations
      // TODO(Dronplane) we can possibly postpone resetting all iterators until
      // they are needed. (_need_reset as a counter?)
      for (auto reset_it = _lead_it + 1; reset_it != end; ++reset_it) {
        Traits::ResetPos(*reset_it);
      }
      _permutations = false;
    }
    _need_reset = false;
  }

  PosAttr::value_t NextPosition(const Iterator& it) {
    return _base_position + Traits::Interval(*it).offs_min + _interval_delta;
  }

  bool Match(PosAttr::value_t seek, PosAttr::value_t sought,
             const TermInterval& interval) const noexcept {
    SDB_ASSERT(sought >= seek);
    SDB_ASSERT(_interval_delta <= (interval.offs_max - interval.offs_min));
    return sought - seek <=
           interval.offs_max - interval.offs_min - _interval_delta;
  }

  bool AdvanceIterators(bool match, PosAttr::value_t sought,
                        const Iterator& end, Iterator& it) {
    const auto& interval = Traits::Interval(*it);
    _interval_delta = 0;
    if (match) {
      ++it;
      if (_need_reset && it != end) {
        SDB_ASSERT(_permutations);
        // TODO(Dronplane) we can possibly postpone resetting all iterators
        // until they are needed. (_need_reset as a counter?)
        for (auto reset_it = it; reset_it != end; ++reset_it) {
          Traits::ResetPos(*reset_it);
        }
        _need_reset = false;
      }
      // reset may leak here only if we are at the end and there is no iterators
      // to reset anyway.
      SDB_ASSERT(!_need_reset || it == end);
      _base_position = sought;
      return true;
    }

    while (it != _lead_it + 1) {
      --it;
      // let`s adjust prev iterator pos  - so it will try to seek to
      // "correct" position for our current position
      auto prev_base_it = (it - 1);
      _base_position = prev_base_it == _lead_it
                         ? _lead_pos.value()
                         : Traits::Position(*prev_base_it);
      const auto& current_interval = Traits::Interval(*it);
      if (current_interval.offs_max != current_interval.offs_min) {
        SDB_ASSERT(sought - interval.lead_offset +
                     current_interval.lead_offset >
                   current_interval.offs_min + _base_position);
        _interval_delta = sought - interval.lead_offset +
                          current_interval.lead_offset -
                          current_interval.offs_min - _base_position;
        if (_interval_delta + current_interval.offs_min <=
            current_interval.offs_max) {
          // found potentially still valid interval  - try to re-start
          // from here. It is still a "match" as we've found valid interval.
          return true;
        }
      }
    }
    // Reached lead. Move it to closest reasonable position and try to re-start.
    SDB_ASSERT(sought >= interval.lead_offset);
    SDB_ASSERT(_lead_pos.value() < (sought - interval.lead_offset));
    _lead_pos.seek(sought - interval.lead_offset);
    return false;
  }

  bool NextPermutation(Iterator& it, const Iterator& end) {
    // try to achieve next premutation
    SDB_ASSERT(it != _lead_it);
    const auto at_end = it == end;
    if (!at_end && !_permutations) {
      // we are not building premutations. So this is bailout due to eof on some
      // iterator.
      SDB_ASSERT(pos_limits::eof(Traits::Position(*it)));
      return false;
    }
    --it;
    PosAttr::value_t current_position = pos_limits::eof();
    while (it != _lead_it) {
      auto prev_base_it = (it - 1);
      current_position = Traits::Position(*it);
      _base_position = prev_base_it == _lead_it
                         ? _lead_pos.value()
                         : Traits::Position(*prev_base_it);
      SDB_ASSERT(current_position >=
                 _base_position + Traits::Interval(*it).offs_min);
      if (current_position < _base_position + Traits::Interval(*it).offs_max) {
        _need_reset = true;
        // Force "it" to move at least one step forward.
        _interval_delta = current_position - _base_position -
                          Traits::Interval(*it).offs_min + 1;
        _permutations = true;
        return true;
      }
      --it;
    }

    it = end;
    // If we are at the end of the phrase - that means we can't find new
    // premutation and should return false. But if we are in the middle of
    // building premutation and have exhausted iterator and can't find previous
    // with valid interval we should return true here to allow overriding eof
    // state and just start with next lead position as there could be more
    // matches. A bit kludgy solution though.
    return !at_end;
  }

 private:
  Iterator& _lead_it;
  PosAttr& _lead_pos;
  PosAttr::value_t _base_position{pos_limits::eof()};
  PosAttr::value_t _interval_delta{0};
  bool _permutations{false};
  bool _need_reset{false};
};

template<bool OneShot, bool HasFreq, bool HasIntervals>
class FixedPhraseFrequency {
 public:
  using TermPosition = FixedTermPosition;
  using Positions = std::vector<TermPosition>;

  using ExecutionStrategy =
    std::conditional_t<HasIntervals,
                       IntervalPositionStrategy<typename Positions::iterator>,
                       SinglePositionStrategy<typename Positions::iterator>>;

  explicit FixedPhraseFrequency(std::vector<TermPosition>&& pos) noexcept
    : _pos{std::move(pos)} {
    SDB_ASSERT(!_pos.empty());  // must not be empty
    // lead offset is always 0
    SDB_ASSERT(_pos.front().second.offs_min == 0);
    SDB_ASSERT(_pos.front().second.offs_max == 0);
  }

  Attribute* GetMutable(TypeInfo::type_id id) noexcept {
    if constexpr (HasFreq) {
      if (id == irs::Type<FreqAttr>::id()) {
        return &_phrase_freq;
      }
    }

    return nullptr;
  }

  // returns frequency of the phrase
  uint32_t EvaluateFreq() { return _phrase_freq.value = NextPosition(); }

 private:
  friend class PhrasePosition<FixedPhraseFrequency>;

  std::pair<const uint32_t*, const uint32_t*> GetOffsets() const noexcept {
    auto start = irs::get<irs::OffsAttr>(_pos.front().first.get());
    SDB_ASSERT(start);
    auto end = irs::get<irs::OffsAttr>(_pos.back().first.get());
    SDB_ASSERT(end);
    return {&start->start, &end->end};
  }

  uint32_t NextPosition() {
    uint32_t phrase_freq = 0;
    PosAttr& lead = _pos.front().first;
    lead.next();
    auto lead_it = std::begin(_pos);
    ExecutionStrategy strategy{lead_it, lead};
    SDB_ASSERT(_pos.size() > 1);

    for (auto end = std::end(_pos); !pos_limits::eof(lead.value());) {
      strategy.NotifyNextLead(end);
      bool match = true;
      for (auto it = lead_it + 1; it != end;) {
        PosAttr& pos = it->first;

        const auto term_position = strategy.NextPosition(it);
        if (!pos_limits::valid(term_position)) {
          return phrase_freq;
        }
        const auto sought = pos.seek(term_position);

        if (pos_limits::eof(sought)) {
          // exhausted
          if constexpr (!OneShot) {
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
          strategy.Match(term_position, sought, it->second), sought, end, it);

        if constexpr (!OneShot) {
          if (it == end && match) {
            if (!strategy.NextPermutation(it, end)) {
              break;
            }
            ++phrase_freq;
          }
        }
        if (!match) {
          break;
        }
      }
      if (match) {
        ++phrase_freq;
        if constexpr (OneShot) {
          return phrase_freq;
        } else {
          lead.next();
        }
      }
    }

    return phrase_freq;
  }

  // list of desired positions along with corresponding attributes
  Positions _pos;
  // freqency of the phrase in a document
  FreqAttr _phrase_freq;
};

// Adapter to use DocIterator with positions for disjunction
struct VariadicPhraseAdapter : ScoreAdapter<> {
  VariadicPhraseAdapter() = default;
  VariadicPhraseAdapter(DocIterator::ptr&& it, score_t boost) noexcept
    : ScoreAdapter<>(std::move(it)),
      position(irs::GetMutable<irs::PosAttr>(this->it.get())),
      boost(boost) {}

  irs::PosAttr* position{};
  score_t boost{kNoBoost};
};

static_assert(std::is_nothrow_move_constructible_v<VariadicPhraseAdapter>);
static_assert(std::is_nothrow_move_assignable_v<VariadicPhraseAdapter>);

struct VariadicPhraseOffsetAdapter final : VariadicPhraseAdapter {
  VariadicPhraseOffsetAdapter() = default;
  VariadicPhraseOffsetAdapter(DocIterator::ptr&& it, score_t boost) noexcept
    : VariadicPhraseAdapter{std::move(it), boost},
      offset{this->position ? irs::get<irs::OffsAttr>(*this->position)
                            // FIXME(gnusi): use constant
                            : nullptr} {}

  const irs::OffsAttr* offset{};
};

static_assert(
  std::is_nothrow_move_constructible_v<VariadicPhraseOffsetAdapter>);
static_assert(std::is_nothrow_move_assignable_v<VariadicPhraseOffsetAdapter>);

template<typename Adapter>
using VariadicTermPosition =
  std::pair<CompoundDocIterator<Adapter>*,
            TermInterval>;  // desired offset in the phrase

// Helper for variadic phrase frequency evaluation for cases when
// only one term may be at a single position in a phrase (e.g. synonyms)
template<typename Adapter, bool VolatileBoost, bool OneShot, bool HasFreq,
         bool HasIntervals>
class VariadicPhraseFrequency {
 public:
  using TermPosition = VariadicTermPosition<Adapter>;
  using Positions = std::vector<TermPosition>;

  using ExecutionSrategy =
    std::conditional_t<HasIntervals,
                       IntervalPositionStrategy<typename Positions::iterator>,
                       SinglePositionStrategy<typename Positions::iterator>>;

  explicit VariadicPhraseFrequency(std::vector<TermPosition>&& pos) noexcept
    : _pos{std::move(pos)}, _phrase_size{_pos.size()} {
    SDB_ASSERT(!_pos.empty() && _phrase_size);  // must not be empty
    // lead offset is always 0
    SDB_ASSERT(_pos.front().second.offs_min == 0);
    SDB_ASSERT(_pos.front().second.offs_max == 0);
  }

  Attribute* GetMutable(TypeInfo::type_id id) noexcept {
    if constexpr (VolatileBoost) {
      if (id == irs::Type<FilterBoost>::id()) {
        return &_phrase_boost;
      }
    }

    if constexpr (HasFreq) {
      if (id == irs::Type<FreqAttr>::id()) {
        return &_phrase_freq;
      }
    }

    return nullptr;
  }

  // Evaluate and return frequency of the phrase
  uint32_t EvaluateFreq() {
    if constexpr (VolatileBoost) {
      _phrase_boost.value = {};
    }
    _phrase_freq.value = 0;
    _pos.front().first->visit(this, VisitLead);

    if constexpr (VolatileBoost) {
      if (_phrase_freq.value) {
        _phrase_boost.value /=
          static_cast<score_t>(_phrase_size * _phrase_freq.value);
      }
    }

    return _phrase_freq.value;
  }

 private:
  friend class PhrasePosition<VariadicPhraseFrequency>;

  struct SubMatchContext {
    ExecutionSrategy& strategy;
    PosAttr::value_t term_position{pos_limits::eof()};
    PosAttr::value_t min_sought{pos_limits::eof()};
    TermInterval* interval{nullptr};
    const uint32_t* end{};  // end match offset
    score_t boost{};
    bool match{false};
  };

  std::pair<const uint32_t*, const uint32_t*> GetOffsets() const noexcept {
    return {&_start, &_end};
  }

  uint32_t NextPosition() {
    // FIXME(gnusi): don't change iterator state
    _phrase_freq.value = 0;
    _pos.front().first->visit(this, VisitLead);
    return _phrase_freq.value;
  }

  static bool VisitFollower(void* ctx, Adapter& it_adapter) {
    SDB_ASSERT(ctx);
    auto& match = *reinterpret_cast<SubMatchContext*>(ctx);
    auto* p = it_adapter.position;
    p->reset();
    const auto sought = p->seek(match.term_position);
    if (pos_limits::eof(sought)) {
      return true;
    }
    if (sought < match.min_sought) {
      match.min_sought = sought;
    }
    SDB_ASSERT(match.interval);
    if (!match.strategy.Match(match.term_position, sought, *match.interval)) {
      return true;
    }

    if constexpr (VolatileBoost) {
      match.boost += it_adapter.boost;
    }

    if constexpr (std::is_same_v<Adapter, VariadicPhraseOffsetAdapter>) {
      if (it_adapter.offset) {  // FIXME(gnusi): remove condition
        match.end = &it_adapter.offset->end;
      }
    }

    match.match = true;
    return false;
  }

  static bool VisitLead(void* ctx, Adapter& lead_adapter) {
    SDB_ASSERT(ctx);
    auto& self = *reinterpret_cast<VariadicPhraseFrequency*>(ctx);
    const auto end = std::end(self._pos);
    auto* lead = lead_adapter.position;
    lead->next();
    auto lead_it = std::begin(self._pos);
    ExecutionSrategy strategy{lead_it, *lead};

    SubMatchContext match{.strategy = strategy};

    auto increase_freq = [&] {
      ++self._phrase_freq.value;
      if constexpr (std::is_same_v<Adapter, VariadicPhraseOffsetAdapter>) {
        SDB_ASSERT(lead_adapter.offset);
        self._start = lead_adapter.offset->start;
        SDB_ASSERT(match.end);
        self._end = *match.end;
      }
      if constexpr (VolatileBoost) {
        self._phrase_boost.value += match.boost;
      }
    };

    while (!pos_limits::eof(lead->value())) {
      strategy.NotifyNextLead(end);
      match.match = true;
      if constexpr (VolatileBoost) {
        match.boost = lead_adapter.boost;
      }

      for (auto it = lead_it + 1; it != end;) {
        match.interval = &it->second;
        match.term_position = strategy.NextPosition(it);

        if (!pos_limits::valid(match.term_position)) {
          return false;  // invalid for all
        }

        match.match = false;
        match.min_sought = pos_limits::eof();

        it->first->visit(&match, VisitFollower);

        if (!match.match) {
          if (pos_limits::eof(match.min_sought)) {
            if constexpr (!OneShot) {
              if (!strategy.NextPermutation(it, end)) {
                return true;
              }
              if (it == end) {
                lead->next();
              }
              continue;
            } else {
              return true;
            }
          }
        }
        match.match =
          strategy.AdvanceIterators(match.match, match.min_sought, end, it);
        if constexpr (!OneShot) {
          if (it == end && match.match) {
            if (!strategy.NextPermutation(it, end)) {
              break;
            }
            increase_freq();
          }
        }
        if (!match.match) {
          break;
        }
      }
      if (match.match) {
        increase_freq();
        if constexpr (OneShot) {
          return false;
        } else {
          lead->next();
        }
      }
    }

    return true;
  }

  // list of desired positions along with corresponding attributes
  Positions _pos;
  // size of the phrase (speedup phrase boost evaluation)
  const size_t _phrase_size;
  FreqAttr _phrase_freq;      // freqency of the phrase in a document
  FilterBoost _phrase_boost;  // boost of the phrase in a document

  // FIXME(gnusi): refactor
  uint32_t _start{};
  uint32_t _end{};
};

// Not used currenly. We don't have synonyms ATM. Should be updated to use
// strategies when it would be possible to use and test this code.
// Helper for variadic phrase frequency evaluation for cases when
// different terms may be at the same position in a phrase (e.g.
// synonyms)
template<typename Adapter, bool VolatileBoost, bool OneShot, bool HasFreq>
class VariadicPhraseFrequencyOverlapped {
 public:
  using TermPosition = VariadicTermPosition<Adapter>;

  explicit VariadicPhraseFrequencyOverlapped(
    std::vector<TermPosition>&& pos) noexcept
    : _pos(std::move(pos)), _phrase_size(_pos.size()) {
    SDB_ASSERT(!_pos.empty() && _phrase_size);  // must not be empty
    SDB_ASSERT(0 == _pos.front().second);       // lead offset is always 0
  }

  Attribute* GetMutable(TypeInfo::type_id id) noexcept {
    if constexpr (VolatileBoost) {
      if (id == irs::Type<FilterBoost>::id()) {
        return &_phrase_boost;
      }
    }

    if constexpr (HasFreq) {
      if (id == irs::Type<FreqAttr>::id()) {
        return &_phrase_freq;
      }
    }

    return nullptr;
  }

  // returns frequency of the phrase
  uint32_t EvaluateFreq() {
    if constexpr (VolatileBoost) {
      _lead_freq = 0;
      _lead_boost = {};
      _phrase_boost.value = {};
    }

    _phrase_freq.value = 0;
    _pos.front().first->visit(this, VisitLead);

    if constexpr (VolatileBoost) {
      if (_lead_freq) {
        _phrase_boost.value =
          (_phrase_boost.value + (_lead_boost / _lead_freq)) / _phrase_size;
      }
    }

    return _phrase_freq.value;
  }

 private:
  struct SubMatchContext {
    PosAttr::value_t term_position{pos_limits::eof()};
    PosAttr::value_t min_sought{pos_limits::eof()};
    score_t boost{};
    uint32_t freq{};
  };

  static bool VisitFollower(void* ctx, Adapter& it_adapter) {
    SDB_ASSERT(ctx);
    auto& match = *reinterpret_cast<SubMatchContext*>(ctx);
    auto* p = it_adapter.position;
    p->reset();
    const auto sought = p->seek(match.term_position);
    if (pos_limits::eof(sought)) {
      return true;
    } else if (sought != match.term_position) {
      if (sought < match.min_sought) {
        match.min_sought = sought;
      }
      return true;
    }

    ++match.freq;
    if constexpr (VolatileBoost) {
      match.boost += it_adapter.boost;
    }

    return true;  // continue iteration in overlapped case
  }

  static bool VisitLead(void* ctx, Adapter& lead_adapter) {
    SDB_ASSERT(ctx);
    auto& self = *reinterpret_cast<VariadicPhraseFrequencyOverlapped*>(ctx);
    const auto end = std::end(self.pos_);
    auto* lead = lead_adapter.position;
    lead->next();

    SubMatchContext match;     // sub-match
    uint32_t phrase_freq = 0;  // phrase frequency for current lead_iterator
    // accumulated match frequency for current lead_iterator
    uint32_t match_freq;
    score_t phrase_boost = {};  // phrase boost for current lead_iterator
    score_t match_boost;  // accumulated match boost for current lead_iterator
    for (PosAttr::value_t base_position;
         !pos_limits::eof(base_position = lead->value());) {
      match_freq = 1;
      if constexpr (VolatileBoost) {
        match_boost = 0.f;
      }

      for (auto it = std::begin(self.pos_) + 1; it != end; ++it) {
        match.term_position = base_position + it->second;
        if (!pos_limits::valid(match.term_position)) {
          return false;  // invalid for all
        }

        match.freq = 0;
        if constexpr (VolatileBoost) {
          match.boost = 0.f;
        }
        match.min_sought = pos_limits::eof();

        it->first->visit(&match, VisitFollower);

        if (!match.freq) {
          match_freq = 0;

          if (!pos_limits::eof(match.min_sought)) {
            lead->seek(match.min_sought - it->second);
            break;
          }

          if constexpr (VolatileBoost) {
            if (phrase_freq) {
              ++self.lead_freq_;
              self.lead_boost_ += lead_adapter.boost;
              self.phrase_boost_.value += phrase_boost / phrase_freq;
            }
          }

          return true;  // eof for all
        }

        match_freq *= match.freq;
        if constexpr (VolatileBoost) {
          match_boost += match.boost / match.freq;
        }
      }

      if (match_freq) {
        self.phrase_freq_.value += match_freq;
        if constexpr (OneShot) {
          return false;
        }
        ++phrase_freq;
        if constexpr (VolatileBoost) {
          phrase_boost += match_boost;
        }
        lead->next();
      }
    }

    if constexpr (VolatileBoost) {
      if (phrase_freq) {
        ++self.lead_freq_;
        self.lead_boost_ += lead_adapter.boost;
        self.phrase_boost_.value += phrase_boost / phrase_freq;
      }
    }

    return true;
  }
  // list of desired positions along with corresponding attributes
  std::vector<TermPosition> _pos;
  // size of the phrase (speedup phrase boost evaluation)
  const size_t _phrase_size;
  FreqAttr _phrase_freq;      // freqency of the phrase in a document
  FilterBoost _phrase_boost;  // boost of the phrase in a document
  score_t _lead_boost{0.f};   // boost from all matched lead iterators
  uint32_t _lead_freq{0};     // number of matched lead iterators
};

// implementation is optimized for frequency based similarity measures
// for generic implementation see a03025accd8b84a5f8ecaaba7412fc92a1636be3
template<typename Conjunction, typename Frequency>
class PhraseIterator : public DocIterator {
 public:
  using TermPosition = typename Frequency::TermPosition;

  PhraseIterator(ScoreAdapters&& itrs, std::vector<TermPosition>&& pos)
    : _approx{NoopAggregator{},
              [](auto&& itrs) {
                absl::c_sort(itrs,
                             [](const auto& lhs, const auto& rhs) noexcept {
                               return CostAttr::extract(lhs, CostAttr::kMax) <
                                      CostAttr::extract(rhs, CostAttr::kMax);
                             });
                return std::move(itrs);
              }(std::move(itrs))},
      _freq{std::move(pos)} {
    std::get<AttributePtr<DocAttr>>(_attrs) =
      irs::GetMutable<DocAttr>(&_approx);

    // FIXME find a better estimation
    std::get<AttributePtr<irs::CostAttr>>(_attrs) =
      irs::GetMutable<irs::CostAttr>(&_approx);
  }

  PhraseIterator(ScoreAdapters&& itrs,
                 std::vector<typename Frequency::TermPosition>&& pos,
                 const SubReader& segment, const TermReader& field,
                 const byte_type* stats, const Scorers& ord, score_t boost)
    : PhraseIterator{std::move(itrs), std::move(pos)} {
    if (!ord.empty()) {
      auto& score = std::get<irs::ScoreAttr>(_attrs);
      CompileScore(score, ord.buckets(), segment, field, stats, *this, boost);
    }
  }

  Attribute* GetMutable(TypeInfo::type_id type) noexcept final {
    if (type == irs::Type<irs::PosAttr>::id()) {
      if constexpr (HasPosition<Frequency>::value) {
        return &_freq;
      } else {
        return nullptr;
      }
    }

    auto* attr = _freq.GetMutable(type);
    return attr ? attr : irs::GetMutable(_attrs, type);
  }

  doc_id_t value() const final {
    return std::get<AttributePtr<DocAttr>>(_attrs).ptr->value;
  }

  doc_id_t advance() final {
    while (true) {
      const auto doc = _approx.advance();
      if (doc_limits::eof(doc) || _freq.EvaluateFreq()) {
        return doc;
      }
    }
  }

  doc_id_t seek(doc_id_t target) final {
    if (const auto doc = value(); target <= doc) [[unlikely]] {
      return doc;
    }
    const auto doc = _approx.seek(target);
    if (doc_limits::eof(doc) || _freq.EvaluateFreq()) {
      return doc;
    }
    return advance();
  }

  uint32_t count() final { return Count(*this); }

 private:
  using Attributes =
    std::tuple<AttributePtr<DocAttr>, AttributePtr<CostAttr>, ScoreAttr>;

  // first approximation (conjunction over all words in a phrase)
  Conjunction _approx;
  Frequency _freq;
  Attributes _attrs;
};

}  // namespace irs
