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

#include <memory>

#include "basics/empty.hpp"
#include "disjunction.hpp"
#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/formats/posting/common.hpp"
#include "iresearch/formats/posting/format_block_128.hpp"
#include "iresearch/formats/posting/iterator_pos.hpp"
#include "iresearch/index/field_meta.hpp"
#include "iresearch/search/column_collector.hpp"
#include "iresearch/search/score_function.hpp"
#include "iresearch/search/scorer.hpp"

namespace irs {

template<typename Frequency>
class PhrasePosition final : public PosAttr, public Frequency {
 public:
  explicit PhrasePosition(
    std::vector<typename Frequency::TermPosition>&& pos) noexcept
    : Frequency{std::move(pos)} {
    std::tie(_start, _end) = this->GetOffsets();
  }

  explicit PhrasePosition(std::vector<typename Frequency::TermPosition>&& pos,
                          PosAttr::value_t max_slop) noexcept
    : Frequency{std::move(pos), max_slop} {
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

  static void ResetPos(const T&) {
    // variadic resets anyway.
    // FIXME (Dronplane) maybe it would be possible to avoid constant
    // resetting e.g. reset only at the beginning of VisitLead but all.
    // Will need to have some kind of visit_all interface for disjunction.
  }
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
  using Value = typename std::iterator_traits<Iterator>::value_type;
  using Traits = TermPositionTraits<Value>;
  using PositionImpl = Traits::PositionImpl;

  SinglePositionStrategy(Iterator& it, PositionImpl& lead_position)
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
  PositionImpl& _lead_pos;
  PosAttr::value_t _base_position{pos_limits::eof()};
};

template<typename Iterator>
class IntervalPositionStrategy {
 public:
  using Value = typename std::iterator_traits<Iterator>::value_type;
  using Traits = TermPositionTraits<Value>;
  using PositionImpl = Traits::PositionImpl;

  IntervalPositionStrategy(Iterator& lead, PositionImpl& lead_position)
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
  PositionImpl& _lead_pos;
  PosAttr::value_t _base_position{pos_limits::eof()};
  PosAttr::value_t _interval_delta{0};
  bool _permutations{false};
  bool _need_reset{false};
};

template<bool Offs, bool HasFreq, bool HasIntervals>
class FixedPhraseFrequency {
 public:
  using TermPosition = FixedTermPosition<Offs>;
  using Positions = std::vector<TermPosition>;
  using ExecutionStrategy =
    std::conditional_t<HasIntervals,
                       IntervalPositionStrategy<typename Positions::iterator>,
                       SinglePositionStrategy<typename Positions::iterator>>;

  static constexpr bool kHasBoost = false;
  static constexpr bool kHasFreq = HasFreq;

  explicit FixedPhraseFrequency(std::vector<TermPosition>&& pos) noexcept
    : _pos{std::move(pos)} {
    SDB_ASSERT(!_pos.empty());  // must not be empty
    // lead offset is always 0
    SDB_ASSERT(_pos.front().second.offs_min == 0);
    SDB_ASSERT(_pos.front().second.offs_max == 0);
  }

  IRS_FORCE_INLINE bool Match() {
    _phrase_freq = NextPosition();
    return _phrase_freq != 0;
  }

  uint32_t GetFreq() const noexcept { return _phrase_freq; }

 private:
  friend class PhrasePosition<FixedPhraseFrequency>;

  std::pair<const uint32_t*, const uint32_t*> GetOffsets() const noexcept {
    auto start = irs::get<OffsAttr>(*_pos.front().first);
    SDB_ASSERT(start);
    auto end = irs::get<OffsAttr>(*_pos.back().first);
    SDB_ASSERT(end);
    return {&start->start, &end->end};
  }

  IRS_FORCE_INLINE uint32_t NextPosition() {
    if constexpr (HasIntervals || Offs) {
      return NextPositionGeneric();
    } else {
      return NextPositionOptimized();
    }
  }

  uint32_t NextPositionGeneric() {
    uint32_t phrase_freq = 0;
    auto& lead = *_pos.front().first;
    lead.next();
    auto lead_it = std::begin(_pos);
    ExecutionStrategy strategy{lead_it, lead};
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
          // exhausted
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
          strategy.Match(term_position, sought, it->second), sought, end, it);

        if constexpr (HasFreq) {
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
        if constexpr (HasFreq) {
          ++phrase_freq;
          lead.next();
        } else {
          return 1;
        }
      }
    }

    return phrase_freq;
  }

  uint32_t NextPositionOptimized() {
    auto begin = _pos.begin();
    auto end = _pos.end();
    std::sort(begin, end, [](const auto& l, const auto& r) {
      return l.first->DocFreq() < r.first->DocFreq();
    });

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
        lead.next();
        lead_pos = lead.value();
      } else {
        return 1;
      }
    }
  }

  // list of desired positions along with corresponding attributes
  Positions _pos;
  // freqency of the phrase in a document
  uint32_t _phrase_freq = 0;
};

// Positional distance for sloppy phrase matching.
// Forward gap (curr > prev+1): costs (curr - prev - 1).
// Reversal (curr < prev): costs (prev - curr + 1).
// Adjacent or same position: costs 0.
inline PosAttr::value_t ComputeSlopDistance(const PosAttr::value_t* positions,
                                            size_t count) noexcept {
  SDB_ASSERT(count >= 2);
  PosAttr::value_t distance = 0;
  for (size_t i = 1; i < count; ++i) {
    auto prev = positions[i - 1];
    auto curr = positions[i];
    if (curr > prev + 1) {
      distance += curr - prev - 1;
    } else if (curr < prev) {
      distance += prev - curr + 1;
    }
  }
  return distance;
}

// Sloppy phrase frequency for fixed phrases (all parts are exact terms).
// Uses min-window algorithm: holds one current position per term,
// computes distance, advances the term with smallest position.
// Supports term reordering. Rejects duplicate positions.
// Scoring: boost = 1/(1+best_distance).
template<bool Offs>
class SlopPhraseFrequency {
 public:
  using TermPosition = FixedTermPosition<Offs>;
  using Positions = std::vector<TermPosition>;

  static constexpr bool kHasBoost = true;
  static constexpr bool kHasFreq = true;

  SlopPhraseFrequency(std::vector<TermPosition>&& pos,
                      PosAttr::value_t max_slop) noexcept
    : _pos{std::move(pos)}, _max_slop{max_slop} {
    SDB_ASSERT(_pos.size() >= 2);
    SDB_ASSERT(_max_slop > 0);
  }

  IRS_FORCE_INLINE bool Match() {
    _phrase_freq = 0;
    _best_distance = _max_slop + 1;
    MatchImpl();
    return _phrase_freq != 0;
  }

  uint32_t GetFreq() const noexcept { return _phrase_freq; }

  score_t GetBoost() const noexcept {
    if (_best_distance == 0) {
      return kNoBoost;
    }
    return 1.f / (1.f + static_cast<score_t>(_best_distance));
  }

 private:
  friend class PhrasePosition<SlopPhraseFrequency>;

  std::pair<const uint32_t*, const uint32_t*> GetOffsets() const noexcept {
    return {&_start_offset, &_end_offset};
  }

  uint32_t NextPosition() { return 0; }

  void MatchImpl() {
    const auto count = _pos.size();

    for (size_t i = 0; i < count; ++i) {
      if (!_pos[i].first->next()) {
        return;
      }
    }

    while (true) {
      size_t min_idx = 0;
      auto min_pos = _pos[0].first->value();

      PosAttr::value_t distance = 0;
      for (size_t i = 1; i < count; ++i) {
        auto prev = _pos[i - 1].first->value();
        auto curr = _pos[i].first->value();

        if (curr > prev + 1) {
          distance += curr - prev - 1;
        } else if (curr < prev) {
          distance += prev - curr + 1;
        }

        if (curr < min_pos) {
          min_pos = curr;
          min_idx = i;
        }
      }

      if (distance <= _max_slop) {
        // verify all positions are distinct - each query slot
        // must match a different token occurrence
        bool all_unique = true;
        for (size_t i = 0; i < count && all_unique; ++i) {
          for (size_t j = i + 1; j < count; ++j) {
            if (_pos[i].first->value() == _pos[j].first->value()) {
              all_unique = false;
              break;
            }
          }
        }
        if (all_unique) {
          ++_phrase_freq;
          if (distance < _best_distance) {
            _best_distance = distance;
            if constexpr (Offs) {
              // find leftmost/rightmost positions for byte offsets
              size_t left_idx = 0;
              size_t right_idx = 0;
              auto left_pos = _pos[0].first->value();
              auto right_pos = left_pos;
              for (size_t k = 1; k < count; ++k) {
                auto p = _pos[k].first->value();
                if (p < left_pos) {
                  left_pos = p;
                  left_idx = k;
                }
                if (p > right_pos) {
                  right_pos = p;
                  right_idx = k;
                }
              }
              auto* start_attr = irs::get<OffsAttr>(*_pos[left_idx].first);
              auto* end_attr = irs::get<OffsAttr>(*_pos[right_idx].first);
              if (start_attr && end_attr) {
                _start_offset = start_attr->start;
                _end_offset = end_attr->end;
              }
            }
          }
        }
      }

      if (!_pos[min_idx].first->next()) {
        return;
      }
    }
  }

  Positions _pos;
  PosAttr::value_t _max_slop;
  uint32_t _phrase_freq = 0;
  PosAttr::value_t _best_distance = 0;
  uint32_t _start_offset{0};
  uint32_t _end_offset{0};
};

// Adapter to use DocIterator with positions for disjunction
struct VariadicPhraseAdapter : ScoreAdapter {
  VariadicPhraseAdapter() = default;

  explicit VariadicPhraseAdapter(DocIterator::ptr it, score_t boost) noexcept
    : ScoreAdapter{std::move(it)}, boost{boost} {
    position = irs::GetMutable<PosAttr>(this);
  }

  PosAttr* position{};
  score_t boost{kNoBoost};
};

struct VariadicPhraseOffsetAdapter : VariadicPhraseAdapter {
  VariadicPhraseOffsetAdapter() = default;

  explicit VariadicPhraseOffsetAdapter(DocIterator::ptr it,
                                       score_t boost) noexcept
    : VariadicPhraseAdapter{std::move(it), boost} {
    offset = position ? irs::get<OffsAttr>(*position)
                      // TODO(gnusi) use constant
                      : nullptr;
  }

  const OffsAttr* offset{};
};

template<typename Adapter>
using VariadicTermPosition =
  std::pair<CompoundDocIterator<Adapter>*, TermInterval>;
// desired offset in the phrase

// Sloppy phrase frequency for variadic phrases (parts may be
// wildcard, prefix, levenshtein, range, etc.).
// Materializes positions from all sub-iterators via visit(),
// then runs the same min-window algorithm on vectors.
template<typename Adapter>
class SlopVariadicPhraseFrequency {
 public:
  using TermPosition = VariadicTermPosition<Adapter>;
  using Positions = std::vector<TermPosition>;

  static constexpr bool kHasBoost = true;
  static constexpr bool kHasFreq = true;

  SlopVariadicPhraseFrequency(std::vector<TermPosition>&& pos,
                              PosAttr::value_t max_slop) noexcept
    : _pos{std::move(pos)}, _max_slop{max_slop} {
    SDB_ASSERT(_pos.size() >= 2);
    SDB_ASSERT(_max_slop > 0);
  }

  IRS_FORCE_INLINE bool Match() {
    _phrase_freq = 0;
    _best_distance = _max_slop + 1;
    MatchImpl();
    return _phrase_freq != 0;
  }

  uint32_t GetFreq() const noexcept { return _phrase_freq; }

  score_t GetBoost() const noexcept {
    if (_best_distance == 0) {
      return kNoBoost;
    }
    return 1.f / (1.f + static_cast<score_t>(_best_distance));
  }

 private:
  friend class PhrasePosition<SlopVariadicPhraseFrequency>;

  static constexpr bool kHasOffsets =
    std::is_same_v<Adapter, VariadicPhraseOffsetAdapter>;

  // Position with optional byte offsets for highlighting.
  struct PosEntry {
    PosAttr::value_t pos;
    uint32_t start_offs{0};
    uint32_t end_offs{0};

    bool operator<(const PosEntry& rhs) const noexcept { return pos < rhs.pos; }
    bool operator==(const PosEntry& rhs) const noexcept {
      return pos == rhs.pos;
    }
  };

  std::pair<const uint32_t*, const uint32_t*> GetOffsets() const noexcept {
    return {&_start_offset, &_end_offset};
  }

  uint32_t NextPosition() { return 0; }

  // Collects positions (and offsets when available) from all
  // sub-iterators in a disjunction into a flat vector.
  static bool CollectPositions(void* ctx, Adapter& adapter) {
    SDB_ASSERT(ctx);
    auto& out = *reinterpret_cast<std::vector<PosEntry>*>(ctx);
    auto* p = adapter.position;
    if (!p) {
      return true;
    }
    const OffsAttr* offs = nullptr;
    if constexpr (kHasOffsets) {
      offs = adapter.offset;
    }
    p->reset();
    while (p->next()) {
      auto val = p->value();
      if (pos_limits::eof(val)) {
        break;
      }
      PosEntry entry{.pos = val};
      if constexpr (kHasOffsets) {
        if (offs) {
          entry.start_offs = offs->start;
          entry.end_offs = offs->end;
        }
      }
      out.push_back(entry);
    }
    return true;
  }

  void MatchImpl() {
    const auto count = _pos.size();

    std::vector<std::vector<PosEntry>> slot_positions(count);
    for (size_t i = 0; i < count; ++i) {
      _pos[i].first->visit(&slot_positions[i], CollectPositions);
      absl::c_sort(slot_positions[i]);
      auto last =
        std::unique(slot_positions[i].begin(), slot_positions[i].end());
      slot_positions[i].erase(last, slot_positions[i].end());
      if (slot_positions[i].empty()) {
        return;
      }
    }

    std::vector<size_t> idx(count, 0);
    while (true) {
      size_t min_slot = 0;
      auto min_pos = slot_positions[0][idx[0]].pos;

      PosAttr::value_t distance = 0;
      for (size_t i = 1; i < count; ++i) {
        auto prev = slot_positions[i - 1][idx[i - 1]].pos;
        auto curr = slot_positions[i][idx[i]].pos;

        if (curr > prev + 1) {
          distance += curr - prev - 1;
        } else if (curr < prev) {
          distance += prev - curr + 1;
        }

        if (curr < min_pos) {
          min_pos = curr;
          min_slot = i;
        }
      }

      if (distance <= _max_slop) {
        bool all_unique = true;
        for (size_t i = 0; i < count && all_unique; ++i) {
          for (size_t j = i + 1; j < count; ++j) {
            if (slot_positions[i][idx[i]].pos ==
                slot_positions[j][idx[j]].pos) {
              all_unique = false;
              break;
            }
          }
        }
        if (all_unique) {
          ++_phrase_freq;
          if (distance < _best_distance) {
            _best_distance = distance;
            if constexpr (kHasOffsets) {
              size_t left_slot = 0;
              size_t right_slot = 0;
              auto left_pos = slot_positions[0][idx[0]].pos;
              auto right_pos = left_pos;
              for (size_t k = 1; k < count; ++k) {
                auto p = slot_positions[k][idx[k]].pos;
                if (p < left_pos) {
                  left_pos = p;
                  left_slot = k;
                }
                if (p > right_pos) {
                  right_pos = p;
                  right_slot = k;
                }
              }
              _start_offset =
                slot_positions[left_slot][idx[left_slot]].start_offs;
              _end_offset =
                slot_positions[right_slot][idx[right_slot]].end_offs;
            }
          }
        }
      }

      ++idx[min_slot];
      if (idx[min_slot] >= slot_positions[min_slot].size()) {
        return;
      }
    }
  }

  Positions _pos;
  PosAttr::value_t _max_slop;
  uint32_t _phrase_freq = 0;
  PosAttr::value_t _best_distance = 0;
  uint32_t _start_offset{0};
  uint32_t _end_offset{0};
};

// Helper for variadic phrase frequency evaluation for cases when
// only one term may be at a single position in a phrase (e.g. synonyms)
template<typename Adapter, bool HasBoost, bool HasFreq, bool HasIntervals>
class VariadicPhraseFrequency {
 public:
  using TermPosition = VariadicTermPosition<Adapter>;
  using Positions = std::vector<TermPosition>;
  using ExecutionSrategy =
    std::conditional_t<HasIntervals,
                       IntervalPositionStrategy<typename Positions::iterator>,
                       SinglePositionStrategy<typename Positions::iterator>>;

  static constexpr bool kHasBoost = HasBoost;
  static constexpr bool kHasFreq = HasFreq;

  explicit VariadicPhraseFrequency(std::vector<TermPosition>&& pos) noexcept
    : _pos{std::move(pos)}, _phrase_size{_pos.size()} {
    SDB_ASSERT(_phrase_size != 0);
    // lead offset is always 0
    SDB_ASSERT(_pos.front().second.offs_min == 0);
    SDB_ASSERT(_pos.front().second.offs_max == 0);
  }

  // Evaluate and return frequency of the phrase
  bool Match() {
    if constexpr (HasBoost) {
      _phrase_boost = 0;  // TODO(mbkkt) 0 vs 1?
    }
    _phrase_freq = 0;
    _pos.front().first->visit(this, VisitLead);

    if constexpr (HasBoost) {
      if (_phrase_freq != 0) {
        _phrase_boost /= static_cast<score_t>(_phrase_size * _phrase_freq);
      }
    }

    return _phrase_freq != 0;
  }

  score_t GetBoost() const noexcept { return _phrase_boost; }
  uint32_t GetFreq() const noexcept { return _phrase_freq; }

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
    _phrase_freq = 0;
    _pos.front().first->visit(this, VisitLead);
    return _phrase_freq;
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

    if constexpr (HasBoost) {
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
      ++self._phrase_freq;
      if constexpr (std::is_same_v<Adapter, VariadicPhraseOffsetAdapter>) {
        SDB_ASSERT(lead_adapter.offset);
        self._start = lead_adapter.offset->start;
        SDB_ASSERT(match.end);
        self._end = *match.end;
      }
      if constexpr (HasBoost) {
        self._phrase_boost += match.boost;
      }
    };

    while (!pos_limits::eof(lead->value())) {
      strategy.NotifyNextLead(end);
      match.match = true;
      if constexpr (HasBoost) {
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
            if constexpr (HasFreq) {
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
        if constexpr (HasFreq) {
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
        if constexpr (HasFreq) {
          lead->next();
        } else {
          return false;
        }
      }
    }

    return true;
  }

  // list of desired positions along with corresponding attributes
  Positions _pos;
  // size of the phrase (speedup phrase boost evaluation)
  const size_t _phrase_size;
  uint32_t _phrase_freq = 0;         // freqency of the phrase in a document
  score_t _phrase_boost = kNoBoost;  // boost of the phrase in a document

  // FIXME(gnusi): refactor
  uint32_t _start{};
  uint32_t _end{};
};

// Not used currenly. We don't have synonyms ATM. Should be updated to use
// strategies when it would be possible to use and test this code.
// Helper for variadic phrase frequency evaluation for cases when
// different terms may be at the same position in a phrase (e.g.
// synonyms)
template<typename Adapter, bool HasBoost, bool HasFreq>
class VariadicPhraseFrequencyOverlapped {
 public:
  using TermPosition = VariadicTermPosition<Adapter>;
  using Positions = std::vector<TermPosition>;

  static constexpr bool kHasBoost = HasBoost;
  static constexpr bool kHasFreq = HasFreq;

  explicit VariadicPhraseFrequencyOverlapped(
    std::vector<TermPosition>&& pos) noexcept
    : _pos(std::move(pos)), _phrase_size(_pos.size()) {
    SDB_ASSERT(!_pos.empty() && _phrase_size);  // must not be empty
    SDB_ASSERT(0 == _pos.front().second);       // lead offset is always 0
  }

  bool Match() {
    if constexpr (HasBoost) {
      _lead_freq = 0;
      _lead_boost = 0;    // TODO(mbkkt) 0 vs 1?
      _phrase_boost = 0;  // TODO(mbkkt) 0 vs 1?
    }

    _phrase_freq = 0;
    _pos.front().first->visit(this, VisitLead);

    if constexpr (HasBoost) {
      if (_lead_freq) {
        _phrase_boost =
          (_phrase_boost + (_lead_boost / _lead_freq)) / _phrase_size;
      }
    }

    return _phrase_freq != 0;
  }

  score_t GetBoost() const noexcept { return _phrase_boost; }
  uint32_t GetFreq() const noexcept { return _phrase_freq; }

 private:
  struct SubMatchContext {
    PosAttr::value_t term_position = pos_limits::eof();
    PosAttr::value_t min_sought = pos_limits::eof();
    score_t boost = 0;  // TODO(mbkkt) 0 vs 1?
    uint32_t freq = 0;
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
    if constexpr (HasBoost) {
      match.boost += it_adapter.boost;
    }

    return true;  // continue iteration in overlapped case
  }

  static bool VisitLead(void* ctx, Adapter& lead_adapter) {
    SDB_ASSERT(ctx);
    auto& self = *reinterpret_cast<VariadicPhraseFrequencyOverlapped*>(ctx);
    const auto end = std::end(self._pos);
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
      if constexpr (HasBoost) {
        match_boost = 0;  // TODO(mbkkt) 0 vs 1?
      }

      for (auto it = std::begin(self._pos) + 1; it != end; ++it) {
        match.term_position = base_position + it->second;
        if (!pos_limits::valid(match.term_position)) {
          return false;  // invalid for all
        }

        match.freq = 0;
        if constexpr (HasBoost) {
          match.boost = 0;  // TODO(mbkkt) 0 vs 1?
        }
        match.min_sought = pos_limits::eof();

        it->first->visit(&match, VisitFollower);

        if (!match.freq) {
          match_freq = 0;

          if (!pos_limits::eof(match.min_sought)) {
            lead->seek(match.min_sought - it->second);
            break;
          }

          if constexpr (HasBoost) {
            if (phrase_freq) {
              ++self._lead_freq;
              self._lead_boost += lead_adapter.boost;
              self._phrase_boost += phrase_boost / phrase_freq;
            }
          }

          return true;  // eof for all
        }

        match_freq *= match.freq;
        if constexpr (HasBoost) {
          match_boost += match.boost / match.freq;
        }
      }

      if (match_freq) {
        self._phrase_freq += match_freq;
        if constexpr (HasFreq) {
          ++phrase_freq;
          if constexpr (HasBoost) {
            phrase_boost += match_boost;
          }
          lead->next();
        } else {
          return false;
        }
      }
    }

    if constexpr (HasBoost) {
      if (phrase_freq != 0) {
        ++self._lead_freq;
        self._lead_boost += lead_adapter.boost;
        self._phrase_boost += phrase_boost / phrase_freq;
      }
    }

    return true;
  }
  // list of desired positions along with corresponding attributes
  std::vector<TermPosition> _pos;
  // size of the phrase (speedup phrase boost evaluation)
  const size_t _phrase_size;
  uint32_t _phrase_freq = 0;  // freqency of the phrase in a document
  // TODO(mbkkt) 0 vs 1?
  score_t _phrase_boost = kNoBoost;  // boost of the phrase in a document
  // TODO(mbkkt) 0 vs 1?
  score_t _lead_boost = 0;  // boost from all matched lead iterators
  uint32_t _lead_freq = 0;  // number of matched lead iterators
};

// implementation is optimized for frequency based similarity measures
// for generic implementation see a03025accd8b84a5f8ecaaba7412fc92a1636be3
template<typename Conjunction, typename Frequency>
class PhraseIterator : public DocIterator {
 public:
  using TermPosition = typename Frequency::TermPosition;

  template<typename Adapters>
  PhraseIterator(doc_id_t docs_count, Adapters&& itrs,
                 std::vector<TermPosition>&& pos)
    : _approx{ScoreMergeType::Noop, docs_count,
              [](auto itrs) {
                absl::c_sort(itrs,
                             [](const auto& lhs, const auto& rhs) noexcept {
                               return CostAttr::extract(lhs, CostAttr::kMax) <
                                      CostAttr::extract(rhs, CostAttr::kMax);
                             });
                return std::move(itrs);
              }(std::forward<Adapters>(itrs))},
      _freq{std::move(pos)} {
    // FIXME find a better estimation
    _cost = irs::GetMutable<CostAttr>(&_approx);

    if constexpr (Frequency::kHasBoost) {
      _collected_boosts.value = std::allocator<score_t>{}.allocate(kScoreBlock);
    }
    if constexpr (Frequency::kHasFreq) {
      _collected_freqs.value = std::allocator<uint32_t>{}.allocate(kScoreBlock);
    }
  }

  ~PhraseIterator() {
    if constexpr (Frequency::kHasBoost) {
      std::allocator<score_t>{}.deallocate(_collected_boosts.value,
                                           kScoreBlock);
    }
    if constexpr (Frequency::kHasFreq) {
      std::allocator<uint32_t>{}.deallocate(_collected_freqs.value,
                                            kScoreBlock);
    }
  }

  template<typename Adapters>
  PhraseIterator(doc_id_t docs_count, Adapters&& itrs,
                 std::vector<TermPosition>&& pos, const FieldProperties& field,
                 const byte_type* stats, score_t boost)
    : PhraseIterator{docs_count, std::forward<Adapters>(itrs), std::move(pos)} {
    _stats = stats;
    _boost = boost;
    _field = field;
  }

  template<typename Adapters>
  PhraseIterator(doc_id_t docs_count, Adapters&& itrs,
                 std::vector<TermPosition>&& pos, PosAttr::value_t max_slop)
    : _approx{ScoreMergeType::Noop, docs_count,
              [](auto itrs) {
                absl::c_sort(itrs,
                             [](const auto& lhs, const auto& rhs) noexcept {
                               return CostAttr::extract(lhs, CostAttr::kMax) <
                                      CostAttr::extract(rhs, CostAttr::kMax);
                             });
                return std::move(itrs);
              }(std::forward<Adapters>(itrs))},
      _freq{std::move(pos), max_slop} {
    _cost = irs::GetMutable<CostAttr>(&_approx);
    if constexpr (Frequency::kHasBoost) {
      _collected_boosts.value = std::allocator<score_t>{}.allocate(kScoreBlock);
    }
    if constexpr (Frequency::kHasFreq) {
      _collected_freqs.value = std::allocator<uint32_t>{}.allocate(kScoreBlock);
    }
  }

  template<typename Adapters>
  PhraseIterator(doc_id_t docs_count, Adapters&& itrs,
                 std::vector<TermPosition>&& pos, PosAttr::value_t max_slop,
                 const FieldProperties& field, const byte_type* stats,
                 score_t boost)
    : PhraseIterator{docs_count, std::forward<Adapters>(itrs), std::move(pos),
                     max_slop} {
    _stats = stats;
    _boost = boost;
    _field = field;
  }

  ScoreFunction PrepareScore(const PrepareScoreContext& ctx) final {
    SDB_ASSERT(ctx.scorer);
    return ctx.scorer->PrepareScorer({
      .segment = *ctx.segment,
      .field = _field,
      .doc_attrs = *this,
      .fetcher = ctx.fetcher,
      .stats = _stats,
      .boost = _boost,
    });
  }

  Attribute* GetMutable(TypeInfo::type_id type) noexcept final {
    if (type == irs::Type<CostAttr>::id()) {
      return _cost;
    }
    if constexpr (Frequency::kHasBoost) {
      if (type == irs::Type<BoostBlockAttr>::id()) {
        return &_collected_boosts;
      }
    }
    if constexpr (Frequency::kHasFreq) {
      if (type == irs::Type<FreqBlockAttr>::id()) {
        return &_collected_freqs;
      }
    }
    if constexpr (HasPosition<Frequency>::value) {
      if (type == irs::Type<PosAttr>::id()) {
        return &_freq;
      }
      return _freq.GetMutable(type);
    } else {
      return nullptr;
    }
  }

  doc_id_t advance() final {
    while (true) {
      const auto doc = _approx.advance();
      if (doc_limits::eof(doc) || _freq.Match()) {
        return _doc = doc;
      }
    }
  }

  doc_id_t seek(doc_id_t target) final {
    if (const auto doc = value(); target <= doc) [[unlikely]] {
      return doc;
    }
    const auto doc = _approx.seek(target);
    if (doc_limits::eof(doc) || _freq.Match()) {
      return _doc = doc;
    }
    return advance();
  }

  doc_id_t LazySeek(doc_id_t target) final {
    // TODO(mbkkt) should be SDB_ASSERT(target > value())
    // but depends on underlying iterator implementation
    SDB_ASSERT(target >= value());
    const auto doc = _approx.LazySeek(target);
    if (target != doc) {
      return doc;
    }
    if (doc_limits::eof(doc) || _freq.Match()) {
      return _doc = doc;
    }
    return doc + 1;
  }

  uint32_t count() final { return CountImpl(*this); }

  void Collect(const ScoreFunction& scorer, ColumnArgsFetcher& fetcher,
               ScoreCollector& collector) final {
    CollectImpl(*this, scorer, fetcher, collector);
  }

  std::pair<doc_id_t, bool> FillBlock(doc_id_t min, doc_id_t max,
                                      uint64_t* mask,
                                      FillBlockScoreContext score,
                                      FillBlockMatchContext match) final {
    return FillBlockImpl(*this, min, max, mask, score, match);
  }

  void FetchScoreArgs(uint16_t index) final {
    if constexpr (Frequency::kHasBoost) {
      SDB_ASSERT(_collected_boosts.value);
      _collected_boosts.value[index] = _freq.GetBoost();
    }
    if constexpr (Frequency::kHasFreq) {
      SDB_ASSERT(_collected_freqs.value);
      _collected_freqs.value[index] = _freq.GetFreq();
    }
  }

 private:
  const byte_type* _stats = nullptr;
  score_t _boost = kNoBoost;
  FieldProperties _field;

  // first approximation (conjunction over all words in a phrase)
  Conjunction _approx;
  Frequency _freq;
  CostAttr* _cost = nullptr;
  [[no_unique_address]] utils::Need<Frequency::kHasBoost, BoostBlockAttr>
    _collected_boosts;
  [[no_unique_address]] utils::Need<Frequency::kHasFreq, FreqBlockAttr>
    _collected_freqs;
};

}  // namespace irs
