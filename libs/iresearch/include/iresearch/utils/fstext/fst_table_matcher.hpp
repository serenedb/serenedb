////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2019 ArangoDB GmbH, Cologne, Germany
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

#include <algorithm>

// clang-format off
#include "iresearch/utils/automaton.hpp"
// clang-format on
#include "basics/bit_utils.hpp"
#include "basics/math_utils.hpp"
#include "basics/misc.hpp"
#include "basics/std.hpp"
#include "fst/matcher.h"

namespace fst {

template<typename F, bool MatchInput, bool ByteLabel>
std::vector<typename F::Arc::Label> getStartLabels(const F& fst) {
  using Label = typename F::Arc::Label;

  if constexpr (ByteLabel) {
    size_t bits[256 / irs::BitsRequired<size_t>()]{};

    for (StateIterator<F> siter(fst); !siter.Done(); siter.Next()) {
      const auto state = siter.Value();
      for (ArcIterator<F> aiter(fst, state); !aiter.Done(); aiter.Next()) {
        const auto& arc = aiter.Value();
        fsa::RangeLabel range{MatchInput ? arc.ilabel : arc.olabel};
        SDB_ASSERT(range.min <= std::numeric_limits<uint8_t>::max());
        SDB_ASSERT(range.max <= std::numeric_limits<uint8_t>::max());
        range.max +=
          decltype(range.max)(range.max < std::numeric_limits<uint8_t>::max());

        irs::SetBit(bits[range.min / irs::BitsRequired<size_t>()],
                    range.min % irs::BitsRequired<size_t>());
        irs::SetBit(bits[range.max / irs::BitsRequired<size_t>()],
                    range.max % irs::BitsRequired<size_t>());
      }
    }

    const size_t size = std::popcount(bits[0]) + std::popcount(bits[1]) +
                        std::popcount(bits[2]) + std::popcount(bits[3]);

    std::vector<Label> labels(1 + size);
    auto begin = labels.begin();
    *begin = 0;
    ++begin;

    Label offset = 0;

    absl::c_for_each(bits, [&offset, &begin](size_t word) {
      for (size_t j = 0; j < irs::BitsRequired<size_t>(); ++j) {
        if (irs::CheckBit(word, j)) {
          *begin = offset + static_cast<Label>(j);
          ++begin;
        }
      }
      offset += irs::BitsRequired<size_t>();
    });

    return labels;
  } else {
    std::set<Label> labels{0};

    for (StateIterator<F> siter(fst); !siter.Done(); siter.Next()) {
      const auto state = siter.Value();
      for (ArcIterator<F> aiter(fst, state); !aiter.Done(); aiter.Next()) {
        const auto& arc = aiter.Value();
        fsa::RangeLabel range{MatchInput ? arc.ilabel : arc.olabel};
        SDB_ASSERT(range.min <= std::numeric_limits<uint16_t>::max());
        SDB_ASSERT(range.max <= std::numeric_limits<uint16_t>::max());
        range.max +=
          decltype(range.max)(range.max < std::numeric_limits<uint16_t>::max());

        labels.emplace(range.min);
        labels.emplace(range.max);
      }
    }
    return {labels.begin(), labels.end()};
  }
}

template<typename F,              // automaton
         size_t CacheSize = 256,  // size of a table for cached label offsets
         bool MatchInput = true,  // label to match
         bool ByteLabel =
           false  // byte automaton is defined over alphabet {0..256, Rho}
         >
class TableMatcher final : public MatcherBase<typename F::Arc> {
 public:
  using FST = F;
  using Arc = typename FST::Arc;
  using Label = typename Arc::Label;
  using StateId = typename Arc::StateId;
  using Weight = typename Arc::Weight;

  using MatcherBase<Arc>::Flags;
  using MatcherBase<Arc>::Properties;

  static constexpr fst::MatchType kMatchType =
    MatchInput ? fst::MATCH_INPUT : fst::MATCH_OUTPUT;

  // expected FST properties
  static constexpr auto kFstProperties =
    (kMatchType == MATCH_INPUT ? kILabelSorted : kOLabelSorted) |
    (kMatchType == MATCH_INPUT ? kIDeterministic : kODeterministic) | kAcceptor;

  explicit TableMatcher(const FST& fst, bool test_props)
    : _start_labels(fst::getStartLabels<F, MatchInput, ByteLabel>(fst)),
      _num_labels(_start_labels.size()),
      _transitions(fst.NumStates() * _num_labels, kNoStateId),
      _arc(kNoLabel, kNoLabel, Weight::NoWeight(), kNoStateId),
      _fst(&fst),
      _error(test_props &&
             (fst.Properties(kFstProperties, true) != kFstProperties)) {
    SDB_ASSERT(!_start_labels.empty());

    if (_error) {
      return;
    }

    // initialize transition table
    ArcIteratorData<Arc> data;
    for (StateIterator<FST> siter(fst); !siter.Done(); siter.Next()) {
      const auto state = siter.Value();

      fst.InitArcIterator(state, &data);

      if (!data.narcs) {
        if (!fst.Final(state)) {
          _sink = state;
        }
        continue;
      }

      // fill existing transitions
      auto arc = data.arcs;
      auto arc_end = data.arcs + data.narcs;
      auto label = _start_labels.begin();
      auto* state_transitions = _transitions.data() + state * _num_labels;

      for (; arc != arc_end && label != _start_labels.end(); ++arc) {
        const fsa::RangeLabel range{get_label(*arc)};
        SDB_ASSERT(range.min <= range.max);

        label = std::find(label, _start_labels.end(), range.min);
        SDB_ASSERT(label != _start_labels.end());

        auto* label_transitions =
          state_transitions + std::distance(_start_labels.begin(), label);
        for (; label != _start_labels.end() && range.max >= *label; ++label) {
          *label_transitions++ = arc->nextstate;
        }
      }
    }

    // initialize lookup table for first CacheSize labels,
    // code below is the optimized version of:
    // for (size_t i = 0; i < CacheSize; ++i) {
    //   cached_label_offsets_[i] = find_label_offset(i);
    // }
    auto begin = _start_labels.begin() + 1;
    auto end = _start_labels.end();
    size_t i = 0;
    size_t offset = 0;
    for (; i < std::size(_cached_label_offsets); ++i) {
      if (begin != end && size_t(*begin) == i) {
        ++offset;
        ++begin;
      }
      _cached_label_offsets[i] = offset;
    }
    std::fill(_cached_label_offsets + i, std::end(_cached_label_offsets),
              offset);
    _transitions_begin = _transitions.data();
  }

  TableMatcher* Copy(bool) const final { return new TableMatcher(*this); }

  MatchType Type(bool test) const final {
    if constexpr (kMatchType == MATCH_NONE) {
      return kMatchType;
    }

    constexpr const auto kTrueProp =
      (kMatchType == MATCH_INPUT) ? kILabelSorted : kOLabelSorted;

    constexpr const auto kFalseProp =
      (kMatchType == MATCH_INPUT) ? kNotILabelSorted : kNotOLabelSorted;

    const auto props = _fst->Properties(kTrueProp | kFalseProp, test);

    if (props & kTrueProp) {
      return kMatchType;
    } else if (props & kFalseProp) {
      return MATCH_NONE;
    } else {
      return MATCH_UNKNOWN;
    }
  }

  StateId Peek(StateId s, Label label) noexcept {
    SDB_ASSERT(!_error);

    size_t label_offset;
    if constexpr (ByteLabel &&
                  CacheSize > std::numeric_limits<uint8_t>::max()) {
      label_offset = _cached_label_offsets[size_t(label)];
    } else {
      label_offset = (size_t(label) < std::size(_cached_label_offsets)
                        ? _cached_label_offsets[size_t(label)]
                        : find_label_offset(label));
    }

    SDB_ASSERT(label_offset < _num_labels);
    return (_transitions_begin + s * _num_labels)[label_offset];
  }

  void SetState(StateId s) noexcept final {
    SDB_ASSERT(!_error);
    SDB_ASSERT(s * _num_labels < _transitions.size());
    _state_begin = _transitions_begin + s * _num_labels;
    _state = _state_begin;
    _state_end = _state_begin + _num_labels;
  }

  bool Find(Label label) noexcept final {
    SDB_ASSERT(!_error);

    size_t label_offset;
    if constexpr (ByteLabel &&
                  CacheSize > std::numeric_limits<uint8_t>::max()) {
      label_offset = _cached_label_offsets[size_t(label)];
    } else {
      label_offset = (size_t(label) < std::size(_cached_label_offsets)
                        ? _cached_label_offsets[size_t(label)]
                        : find_label_offset(label));
    }

    _state = _state_begin + label_offset;
    SDB_ASSERT(_state < _state_end);

    _arc.nextstate = *_state;
    return _arc.nextstate != kNoStateId;
  }

  bool Done() const noexcept final {
    SDB_ASSERT(!_error);
    return _state == _state_end;
  }

  const Arc& Value() const noexcept final {
    SDB_ASSERT(!_error);
    return _arc;
  }

  void Next() noexcept final {
    SDB_ASSERT(!_error);

    if (Done()) {
      return;
    }

    ++_state;

    for (; !Done(); ++_state) {
      if (*_state != kNoLabel) {
        SDB_ASSERT(_state > _state_begin && _state < _state_end);
        const auto label =
          _start_labels[size_t(std::distance(_state_begin, _state))];
        if constexpr (kMatchType == MATCH_INPUT) {
          _arc.ilabel = label;
        } else {
          _arc.olabel = label;
        }
        _arc.nextstate = *_state;
        return;
      }
    }
  }

  Weight Final(StateId s) const final { return MatcherBase<Arc>::Final(s); }

  ssize_t Priority(StateId s) final { return MatcherBase<Arc>::Priority(s); }

  const FST& GetFst() const noexcept final { return *_fst; }

  uint64_t Properties(uint64_t inprops) const noexcept final {
    return inprops | (_error ? kError : 0);
  }

  StateId sink() const noexcept { return _sink; }

 private:
  template<typename Arc>
  static typename Arc::Label get_label(Arc& arc) {
    if constexpr (kMatchType == MATCH_INPUT) {
      return arc.ilabel;
    }

    return arc.olabel;
  }

  size_t find_label_offset(Label label) const noexcept {
    const auto it = std::lower_bound(
      _start_labels.rbegin(), _start_labels.rend(), label, std::greater<>());

    SDB_ASSERT(it != _start_labels.rend());  // we cover the whole range
    SDB_ASSERT(_start_labels.rbegin() <= it);
    return size_t(std::distance(_start_labels.begin(), it.base())) - 1;
  }

  size_t _cached_label_offsets[CacheSize]{};
  std::vector<Label> _start_labels;
  size_t _num_labels;
  std::vector<StateId> _transitions;
  Arc _arc;
  StateId _sink{fst::kNoStateId};  // sink state
  const FST* _fst;                 // FST for matching
  const StateId* _transitions_begin;
  const StateId* _state_begin{};  // Matcher state begin
  const StateId* _state_end{};    // Matcher state end
  const StateId* _state{};        // Matcher current state
  bool _error;                    // Matcher validity
};

}  // namespace fst
