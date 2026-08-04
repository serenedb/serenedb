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
  using PayloadType = byte_type;

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
      _prefix{prefix},
      _chi_size{static_cast<uint32_t>(description.chi_size())},
      _mask{description.chi_max() - 1},
      _no_distance{static_cast<byte_type>(description.max_distance() + 1)} {
    utf8_utils::ToUTF32<false>(target, std::back_inserter(_target));
    _lead.reserve(_target.size());
    for (const auto c : _target) {
      byte_type utf8[utf8_utils::kMaxCharSize];
      utf8_utils::FromChar32(c, utf8);
      _lead.emplace_back(utf8[0]);
    }
    // One characteristic bit vector per distinct code point of the target, so
    // a step is two word loads and a shift rather than a scan of the window.
    // The trailing word is what lets an unaligned read take `word + 1`
    // unconditionally.
    _words = _target.size() / kWordBits + 2;
    _ascii.fill(kNoSlot);
    for (size_t i = 0; i != _target.size(); ++i) {
      const auto c = _target[i];
      auto slot = Slot(c);
      if (slot == kNoSlot) {
        slot = static_cast<int32_t>(_chi.size() / _words);
        _chi.resize(_chi.size() + _words, 0);
        if (c < kAsciiMax) {
          _ascii[c] = slot;
        } else {
          _wide.emplace_back(c, slot);
          std::ranges::sort(_wide);
        }
      }
      _chi[static_cast<size_t>(slot) * _words + i / kWordBits] |=
        uint64_t{1} << (i % kWordBits);
    }
  }

  State Start() const noexcept {
    return {1, 0, 0, _prefix.empty() ? 0U : kPrefixPhase};
  }

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

  // Smallest label above `label` that leaves `state` alive.
  bool NextLabel(const State& state, uint32_t label,
                 uint32_t& out) const noexcept {
    if (label >= std::numeric_limits<byte_type>::max()) {
      return false;
    }
    if (state.phase >= kPrefixPhase) {
      const uint32_t only = _prefix[state.phase - kPrefixPhase];
      if (only <= label) {
        return false;
      }
      out = only;
      return true;
    }
    if (state.phase != 0) {
      if (label >= 0xBF) {
        return false;
      }
      out = std::max<uint32_t>(0x80, label + 1);
      return true;
    }
    // A code point matching nothing in the window has an all-zero
    // characteristic vector; when that keeps the state alive so does every
    // byte, and the tightest bound available is the next one.
    if (_description->transition(state.pstate, 0).first != kDeadState) {
      out = label + 1;
      return true;
    }
    // Otherwise only the window's own code points survive -- at most
    // `chi_size` of them -- so their leading bytes are the whole live
    // alphabet, which is the bound a generic arc list cannot give.
    uint32_t best = std::numeric_limits<uint32_t>::max();
    const size_t end = Window(state.offset);
    for (size_t i = state.offset; i != end; ++i) {
      const uint32_t lead = _lead[i];
      if (lead <= label || lead >= best) {
        continue;
      }
      if (_description->transition(state.pstate, Chi(state.offset, _target[i]))
            .first != kDeadState) {
        best = lead;
      }
    }
    if (best == std::numeric_limits<uint32_t>::max()) {
      return false;
    }
    out = best;
    return true;
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

  uint64_t Chi(uint32_t offset, uint32_t c) const noexcept {
    const auto slot = Slot(c);
    if (slot == kNoSlot) {
      return 0;
    }
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
    const auto& transition =
      _description->transition(state.pstate, Chi(state.offset, c));
    if (transition.first == kDeadState) {
      return Dead();
    }
    state.pstate = transition.first;
    state.offset += transition.second;
    state.acc = 0;
    return state;
  }

  using WideSlot = std::pair<uint32_t, int32_t>;

  const ParametricDescription* _description;
  bstring _prefix;
  std::vector<uint32_t> _target;
  std::vector<byte_type> _lead;
  std::vector<uint64_t> _chi;
  std::vector<WideSlot> _wide;
  std::array<int32_t, kAsciiMax> _ascii{};
  size_t _words{0};
  uint32_t _chi_size;
  uint64_t _mask;
  byte_type _no_distance;
};

}  // namespace irs
