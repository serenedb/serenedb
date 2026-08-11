////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2026 SereneDB
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
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <algorithm>
#include <array>
#include <cstdint>
#include <limits>
#include <span>
#include <utility>
#include <vector>

#include "iresearch/utils/levenshtein_utils.hpp"
#include "iresearch/utils/string.hpp"
#include "iresearch/utils/utf8_utils.hpp"

namespace irs {

// Levenshtein acceptor that steps the parametric tables directly, with no DFA
// materialized.
//
// A state is a parametric state plus the offset into the target's code points,
// plus whatever of a multi-byte code point has already been consumed, so the
// language stays code-point-based exactly as the compiled automaton's is while
// the term iterator driving it walks bytes.
//
// The description is referenced, not copied: every producer takes it from a
// `pdp_f` provider whose descriptions outlive the query (`DefaultPDP` returns
// statics).
class LevenshteinAcceptor {
 public:
  // The edit distance of the accepted key, which similarity scoring ranks by,
  // so a walk publishes it.
  using PayloadType = byte_type;
  static constexpr bool kHasPayload = true;

  static constexpr uint32_t kDeadState = 0;

  struct State {
    uint32_t pstate;  // parametric state, `kDeadState` is the sink
    uint32_t offset;  // code points of the target already consumed
    uint32_t acc;     // bits of a partially decoded code point
    // 0 -- ready for a lead byte, which is the whole hot path; 1..3 --
    // continuation bytes still expected; `kPrefixPhase + n` -- n bytes of the
    // literal prefix matched. One field so a step tests one register.
    uint32_t phase;
  };

  LevenshteinAcceptor(const ParametricDescription& description,
                      bytes_view prefix, bytes_view target)
    : _description{&description},
      _transitions{description.transitions()},
      _prefix{prefix},
      _chi_size{static_cast<uint32_t>(description.chi_size())},
      _mask{description.chi_max() - 1},
      _no_distance{static_cast<byte_type>(description.max_distance() + 1)} {
    // A code point takes at least one byte, so this is an upper bound and the
    // decode never grows the vector.
    _target.reserve(target.size());
    utf8_utils::ToUTF32<false>(target, std::back_inserter(_target));
    // One characteristic bit vector per distinct code point of the target, so
    // a step is two word loads and a shift rather than a scan of the window.
    // The trailing word is what lets an unaligned read take `word + 1`
    // unconditionally.
    _words = _target.size() / kWordBits + 2;
    _ascii.fill(kNoSlot);
    _chi.reserve(_target.size() * _words);
    _chars.reserve(_target.size());
    const auto new_slot = [this] {
      const auto slot = static_cast<int32_t>(_chi.size() / _words);
      _chi.resize(_chi.size() + _words, 0);
      return slot;
    };
    for (size_t i = 0; i != _target.size(); ++i) {
      const auto c = _target[i];
      int32_t slot = kNoSlot;
      if (c < kAsciiMax) {
        slot = _ascii[c];
        if (slot == kNoSlot) {
          slot = new_slot();
          _ascii[c] = slot;
        }
      } else {
        // Ordered as it is built, so a repeat costs a binary search over the
        // few wide code points a target has rather than a scan of them all.
        const auto it =
          std::ranges::lower_bound(_wide, c, {}, &WideSlot::first);
        if (it != _wide.end() && it->first == c) {
          slot = it->second;
        } else {
          slot = new_slot();
          _wide.insert(it, WideSlot{c, slot});
        }
      }
      _chars.push_back({LeadByte(c), slot});
      _chi[static_cast<size_t>(slot) * _words + i / kWordBits] |=
        uint64_t{1} << (i % kWordBits);
    }
    // The transition every symbol outside the target takes, per parametric
    // state. Small -- one entry per state -- and it turns the common step into
    // a single indexed load.
    _zero_next.reserve(description.size());
    for (size_t s = 0; s != description.size(); ++s) {
      _zero_next.emplace_back(description.transition(s, 0));
    }
  }

  State Start() const noexcept {
    return {1, 0, 0, _prefix.empty() ? 0U : kPrefixPhase};
  }

  // The literal prefix every match starts with; empty when unbounded. What
  // lets a dictionary scan restrict itself to `[prefix, UpperBoundOf(prefix))`
  // instead of walking the whole field.
  bytes_view LowerBound() const noexcept { return _prefix; }

  static bool Alive(const State& state) noexcept {
    return state.pstate != kDeadState;
  }

  State Step(const State& state, byte_type label) const noexcept {
    if (state.phase != 0 || label >= 0x80) [[unlikely]] {
      return StepSlow(state, label);
    }
    return StepChar(state, label);
  }

  // A fuzzy state carries how much of the target it has consumed, so there is
  // no test for "this byte leaves the state where it is" short of taking the
  // step: a run of bytes costs exactly what stepping it costs. A caller that
  // would spend a run test to avoid a walk has nothing to gain here.
  static constexpr bool kCheapRuns = false;

  // The edit distance the state reports, or a value above `max_distance` when
  // it does not accept. A key ending inside a multi-byte code point accepts
  // with the distance of the boundary before it (the trailing bytes are a
  // symbol that does not exist); an unfinished literal prefix never accepts.
  PayloadType Distance(const State& state) const noexcept {
    if (state.phase >= kPrefixPhase || state.offset > _target.size()) {
      return _no_distance;
    }
    return _description->distance(state.pstate, _target.size() - state.offset);
  }

  bool Accept(const State& state, PayloadType& payload) const noexcept {
    const auto distance = Distance(state);
    if (distance >= _no_distance) {
      return false;
    }
    payload = distance;
    return true;
  }

  // Whole-term acceptance, for callers that test one key at a time instead of
  // walking a dictionary.
  bool Matches(bytes_view term) const noexcept {
    auto state = Start();
    for (const auto label : term) {
      state = Step(state, label);
      if (!Alive(state)) {
        return false;
      }
    }
    PayloadType payload{};
    return Accept(state, payload);
  }

  // Smallest and largest label that leaves `state` alive; the range is empty
  // (`lo > hi`, which is also what the `false` return says) when none does.
  // Evaluated once per dictionary block, which is what makes the window scan
  // below worth taking over stepping the block's every entry.
  bool LiveRange(const State& state, uint32_t& lo,
                 uint32_t& hi) const noexcept {
    if (state.phase >= kPrefixPhase) {
      lo = hi = _prefix[state.phase - kPrefixPhase];
      return true;
    }
    if (state.phase != 0) {
      lo = 0x80;
      hi = 0xBF;
      return true;
    }
    // A code point matching nothing in the window has an all-zero
    // characteristic vector; when that keeps the state alive so does every
    // byte, and there is no bound to give.
    if (_zero_next[state.pstate].first != kDeadState) {
      lo = 0;
      hi = std::numeric_limits<byte_type>::max();
      return true;
    }
    // Otherwise only the window's own code points survive -- at most
    // `chi_size` of them -- so their leading bytes are the whole live
    // alphabet, which is the bound a generic arc list cannot give.
    uint32_t best_lo = std::numeric_limits<uint32_t>::max();
    uint32_t best_hi = 0;
    const size_t end = Window(state.offset);
    for (size_t i = state.offset; i != end; ++i) {
      const auto& target = _chars[i];
      if (Transition(state.pstate, ChiAt(target.slot, state.offset)).first ==
          kDeadState) {
        continue;
      }
      best_lo = std::min<uint32_t>(best_lo, target.lead);
      best_hi = std::max<uint32_t>(best_hi, target.lead);
    }
    lo = best_lo;
    hi = best_hi;
    return best_lo <= best_hi;
  }

 private:
  static constexpr uint32_t kAsciiMax = 128;
  static constexpr uint32_t kPrefixPhase = 4;
  static constexpr int32_t kNoSlot = -1;
  static constexpr size_t kWordBits = 64;

  static constexpr State Dead() noexcept { return {kDeadState, 0, 0, 0}; }

  // The literal prefix and the middle of a multi-byte code point, off the hot
  // path so the ASCII step stays small enough to inline.
  State StepSlow(State state, byte_type label) const noexcept {
    if (state.phase >= kPrefixPhase) {
      const size_t pos = state.phase - kPrefixPhase;
      if (label != _prefix[pos]) {
        return Dead();
      }
      state.phase = pos + 1 == _prefix.size()
                      ? 0
                      : static_cast<uint32_t>(kPrefixPhase + pos + 1);
      return state;
    }
    if (state.phase != 0) {
      if ((label & 0xC0) != 0x80) {
        return Dead();
      }
      state.acc = (state.acc << 6) | (label & 0x3FU);
      if (--state.phase != 0) {
        return state;
      }
      return StepChar(state, state.acc);
    }
    if (label < 0xC2 || label > 0xF4) {
      return Dead();
    }
    if (label < 0xE0) {
      state.acc = label & 0x1FU;
      state.phase = 1;
    } else if (label < 0xF0) {
      state.acc = label & 0x0FU;
      state.phase = 2;
    } else {
      state.acc = label & 0x07U;
      state.phase = 3;
    }
    return state;
  }

  size_t Window(uint32_t offset) const noexcept {
    return std::min<size_t>(size_t{offset} + _chi_size, _target.size());
  }

  int32_t Slot(uint32_t c) const noexcept {
    if (c < kAsciiMax) {
      return _ascii[c];
    }
    const auto it = std::ranges::lower_bound(_wide, c, {}, &WideSlot::first);
    return it != _wide.end() && it->first == c ? it->second : kNoSlot;
  }

  // `chi_max` is `1 << chi_size`, so the row offset is a shift here where
  // `ParametricDescription::transition` has to multiply by a runtime value.
  const ParametricDescription::transition_t& Transition(
    uint32_t pstate, uint64_t chi) const noexcept {
    SDB_ASSERT(chi <= _mask);
    return _transitions[(size_t{pstate} << _chi_size) | chi];
  }

  // The first byte of `c`'s UTF-8 form, without encoding the rest of it.
  static constexpr byte_type LeadByte(uint32_t c) noexcept {
    if (c < 0x80) {
      return static_cast<byte_type>(c);
    }
    if (c < 0x800) {
      return static_cast<byte_type>(((c >> 6) & 0x1FU) | 0xC0U);
    }
    if (c < 0x10000) {
      return static_cast<byte_type>(((c >> 12) & 0x0FU) | 0xE0U);
    }
    return static_cast<byte_type>(((c >> 18) & 0x07U) | 0xF0U);
  }

  uint64_t ChiAt(int32_t slot, uint32_t offset) const noexcept {
    const uint64_t* bits = _chi.data() + static_cast<size_t>(slot) * _words;
    const size_t word = offset / kWordBits;
    const size_t align = offset % kWordBits;
    if (align == 0) {
      return bits[word] & _mask;
    }
    return ((bits[word] >> align) | (bits[word + 1] << (kWordBits - align))) &
           _mask;
  }

  State StepChar(State state, uint32_t c) const noexcept {
    const auto& transition = Compute(state.pstate, state.offset, c);
    if (transition.first == kDeadState) {
      return Dead();
    }
    state.pstate = transition.first;
    state.offset += transition.second;
    state.acc = 0;
    return state;
  }

  // A symbol the target does not contain has an all-zero characteristic vector
  // whatever the offset, so its transition depends on the parametric state
  // alone -- which is most symbols, and why it is worth its own table.
  const ParametricDescription::transition_t& Compute(
    uint32_t pstate, uint32_t offset, uint32_t c) const noexcept {
    const auto slot = Slot(c);
    return slot == kNoSlot ? _zero_next[pstate]
                           : Transition(pstate, ChiAt(slot, offset));
  }

  using WideSlot = std::pair<uint32_t, int32_t>;

  // Per code point of the target: the first byte of its UTF-8 form, which is
  // what bounds the live alphabet, and the characteristic-vector slot it
  // shares with its repeats.
  struct TargetChar {
    byte_type lead;
    int32_t slot;
  };

  const ParametricDescription* _description;
  std::span<const ParametricDescription::transition_t> _transitions;
  bstring _prefix;
  std::vector<uint32_t> _target;
  std::vector<TargetChar> _chars;
  std::vector<uint64_t> _chi;
  std::vector<ParametricDescription::transition_t> _zero_next;
  std::vector<WideSlot> _wide;
  std::array<int32_t, kAsciiMax> _ascii{};
  size_t _words{0};
  uint32_t _chi_size;
  uint64_t _mask;
  byte_type _no_distance;
};

}  // namespace irs
