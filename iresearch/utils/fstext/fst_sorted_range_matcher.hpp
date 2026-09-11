////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2021 ArangoDB GmbH, Cologne, Germany
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
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
///
/// @author Andrey Abramov
////////////////////////////////////////////////////////////////////////////////

#pragma once

//clang-format off
#include "iresearch/utils/automaton.hpp"
// clang-format on
#include "fst/matcher.h"

namespace fst {

// A matcher that expects sorted labels on the side to be matched.
// If match_type == MATCH_INPUT, epsilons match the implicit self-loop
// Arc(kNoLabel, 0, Weight::One(), current_state) as well as any
// actual epsilon transitions. If match_type == MATCH_OUTPUT, then
// Arc(0, kNoLabel, Weight::One(), current_state) is instead matched.
template<class F, fst::MatchType MatchType = MATCH_INPUT>
class SortedRangeExplicitMatcher final : public MatcherBase<typename F::Arc> {
 public:
  using FST = F;
  using Arc = typename FST::Arc;
  using Label = typename Arc::Label;
  using StateId = typename Arc::StateId;
  using Weight = typename Arc::Weight;

  using MatcherBase<Arc>::Flags;
  using MatcherBase<Arc>::Properties;

  // Labels >= binary_label will be searched for by binary search;
  // o.w. linear search is used.
  // This doesn't copy the FST.
  SortedRangeExplicitMatcher(const FST* fst, Label binary_label = 1)
    : _fst(*fst), _binary_label(binary_label), _error(false) {}

  // This makes a copy of the FST.
  SortedRangeExplicitMatcher(const SortedRangeExplicitMatcher<FST>& matcher,
                             bool safe = false)
    : _owned_fst(matcher._fst.Copy(safe)),
      _fst(*_owned_fst),
      _binary_label(matcher._binary_label),
      _error(matcher._error) {}

  ~SortedRangeExplicitMatcher() final { Destroy(_aiter, &_aiter_pool); }

  SortedRangeExplicitMatcher<FST>* Copy(bool safe = false) const final {
    return new SortedRangeExplicitMatcher<FST>(*this, safe);
  }

  fst::MatchType Type(bool test) const final {
    if constexpr (MatchType == MATCH_NONE) {
      return MatchType;
    }
    const auto true_prop =
      MatchType == MATCH_INPUT ? kILabelSorted : kOLabelSorted;
    const auto false_prop =
      MatchType == MATCH_INPUT ? kNotILabelSorted : kNotOLabelSorted;
    const auto props = _fst.Properties(true_prop | false_prop, test);
    if (props & true_prop) {
      return MatchType;
    } else if (props & false_prop) {
      return MATCH_NONE;
    } else {
      return MATCH_UNKNOWN;
    }
  }

  void SetState(StateId s) final {
    if (_state == s) {
      return;
    }
    _state = s;
    if constexpr (MatchType == MATCH_NONE) {
      FSTERROR() << "SortedMatcher: Bad match type";
      _error = true;
    }
    Destroy(_aiter, &_aiter_pool);
    _aiter = new (&_aiter_pool) ArcIterator<FST>(_fst, s);
    _aiter->SetFlags(kArcNoCache, kArcNoCache);
    _narcs = internal::NumArcs(_fst, s);
  }

  bool Find(Label match_label) final {
    _exact_match = true;
    if (_error) {
      _match_label = kNoLabel;
      return false;
    }
    _match_label = match_label == kNoLabel ? 0 : match_label;
    if (Search()) {
      return true;
    } else {
      return false;
    }
  }

  // Positions matcher to the first position where inserting match_label would
  // maintain the sort order.
  void LowerBound(Label label) {
    _exact_match = false;
    if (_error) {
      _match_label = kNoLabel;
      return;
    }
    _match_label = label;
    Search();
  }

  // After Find(), returns false if no more exact matches.
  // After LowerBound(), returns false if no more arcs.
  bool Done() const final {
    if (_aiter->Done()) {
      return true;
    }
    if (!_exact_match) {
      return false;
    }
    _aiter->SetFlags(
      MatchType == MATCH_INPUT ? kArcILabelValue : kArcOLabelValue,
      kArcValueFlags);
    return GetLabel() != _match_label;
  }

  const Arc& Value() const final {
    _aiter->SetFlags(kArcValueFlags, kArcValueFlags);
    return _aiter->Value();
  }

  void Next() final { _aiter->Next(); }

  Weight Final(StateId s) const final { return MatcherBase<Arc>::Final(s); }

  ssize_t Priority(StateId s) final { return MatcherBase<Arc>::Priority(s); }

  const FST& GetFst() const final { return _fst; }

  uint64_t Properties(uint64_t inprops) const final {
    return inprops | (_error ? kError : 0);
  }

  size_t Position() const { return _aiter ? _aiter->Position() : 0; }

 private:
  constexpr Label GetLabel() const noexcept {
    const auto& arc = _aiter->Value();

    if constexpr (MatchType == MATCH_INPUT) {
      return arc.ilabel;
    }

    return arc.olabel;
  }

  bool BinarySearch();
  bool LinearSearch();
  bool Search();

  std::unique_ptr<const FST> _owned_fst;  // FST ptr if owned.
  const FST& _fst;                        // FST for matching.
  StateId _state{kNoStateId};             // Matcher state.
  ArcIterator<FST>* _aiter{};             // Iterator for current state.
  Label _binary_label;                    // Least label for binary search.
  Label _match_label{kNoLabel};           // Current label to be matched.
  size_t _narcs{0};                       // Current state arc count.
  MemoryPool<ArcIterator<FST>> _aiter_pool{1};  // Pool of arc iterators.
  bool _exact_match;                            // Exact match or lower bound?
  bool _error;                                  // Error encountered?
};

// Returns true iff match to match_label_. The arc iterator is positioned at the
// lower bound, that is, the first element greater than or equal to
// match_label_, or the end if all elements are less than match_label_.
// If multiple elements are equal to the `match_label_`, returns the rightmost
// one.
template<class FST, fst::MatchType MatchType>
inline bool SortedRangeExplicitMatcher<FST, MatchType>::BinarySearch() {
  size_t size = _narcs;
  if (size == 0) {
    return false;
  }
  size_t high = size - 1;
  while (size > 1) {
    const size_t half = size / 2;
    const size_t mid = high - half;
    _aiter->Seek(mid);
    const fsa::RangeLabel range{GetLabel()};
    if (range.max >= _match_label) {
      high = mid;
    }
    size -= half;
  }
  _aiter->Seek(high);
  const fsa::RangeLabel range{GetLabel()};
  if (range.min <= _match_label && range.max >= _match_label) {
    return true;
  }
  if (range.max < _match_label) {
    _aiter->Next();
  }
  return false;
}

// Returns true iff match to match_label_, positioning arc iterator at lower
// bound.
template<class FST, fst::MatchType MatchType>
inline bool SortedRangeExplicitMatcher<FST, MatchType>::LinearSearch() {
  for (_aiter->Reset(); !_aiter->Done(); _aiter->Next()) {
    const fsa::RangeLabel range{GetLabel()};
    if (range.min <= _match_label && range.max >= _match_label) {
      return true;
    }
    if (range.min > _match_label) {
      break;
    }
  }
  return false;
}

// Returns true iff match to match_label_, positioning arc iterator at lower
// bound.
template<class FST, fst::MatchType MatchType>
inline bool SortedRangeExplicitMatcher<FST, MatchType>::Search() {
  _aiter->SetFlags(MatchType == MATCH_INPUT ? kArcILabelValue : kArcOLabelValue,
                   kArcValueFlags);
  if (_match_label >= _binary_label) {
    return BinarySearch();
  } else {
    return LinearSearch();
  }
}

}  // namespace fst
