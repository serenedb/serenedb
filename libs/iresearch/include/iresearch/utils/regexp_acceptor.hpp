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

#include <array>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <vector>

#include "basics/containers/bitset.hpp"
#include "iresearch/utils/regexp_utils.hpp"
#include "iresearch/utils/string.hpp"

namespace irs {

// Regexp acceptor over a dense byte-class transition table, compiled once at
// construction.
//
// The pattern is parsed by RE2 into a `Regexp` tree, that tree is rewritten so
// the whole-term semantics hold (anchors and `\b` are empty, `\B` matches
// nothing, `.` narrows to the strict UTF-8 model), and it is compiled to a
// `Prog` -- no pattern string is ever reassembled, so nothing depends on RE2
// being able to print what it parsed. RE2's own determinizer then floods that
// program into a table of `bytemap` classes, and RE2 is not consulted again:
// nothing here holds a `Prog`, a DFA or a lock, so an acceptor is immutable,
// copyable and safe to drive from several walks at once.
//
// Byte classes and premultiplied row offsets are the two representation
// choices that make a step two loads with a single dependent one, neither of
// them a call. Table sizes measured over the term patterns this is built for
// run 25-450 bytes, so the whole automaton sits in L1 beside the dictionary
// block it is deciding.
class RegexpAcceptor {
 public:
  // Payload the leapfrog reports; a regexp match carries no distance.
  using PayloadType = byte_type;

  // A row offset into the transition table, already multiplied by the row
  // stride so a step needs no multiply. Row 0 is the absorbing dead row, which
  // is what makes `Alive` a comparison against zero and lets a step be taken
  // without first testing the state it comes from.
  using State = uint32_t;

  // Marks the wildcard dialect: `%` and `_` over the byte model iresearch's
  // UTF-8 rho arcs describe, with every literal byte taken as itself, so a
  // pattern over arbitrary (non-UTF-8) bytes is expressible where a regexp
  // source string would not parse.
  struct WildcardTag {};

  RegexpAcceptor(bytes_view pattern, RegexpSyntax syntax = RegexpSyntax::Perl,
                 int64_t max_mem = kDefaultMaxMem);
  RegexpAcceptor(WildcardTag, bytes_view pattern,
                 int64_t max_mem = kDefaultMaxMem);

  // False when the pattern did not parse, did not compile, or accepts nothing
  // at all. Such an acceptor accepts nothing, so a caller may either report it
  // or intersect with it.
  bool ok() const noexcept { return _start != kDead; }

  State Start() const noexcept { return _start; }

  static bool Alive(State state) noexcept { return state != kDead; }

  State Step(State from, byte_type label) const noexcept {
    return _next[from + _bytemap[label]];
  }

  bool Accept(State state, PayloadType& payload) const noexcept {
    if (_accept[state >> _stride_bits] == 0) {
      return false;
    }
    payload = 0;
    return true;
  }

  // Testing whether a run of bytes moves this automaton costs a bit test per
  // byte, a fraction of what stepping the same bytes costs, which is what
  // makes deciding a whole dictionary block in one pass worth more than
  // walking its keys.
  static constexpr bool kCheapRuns = true;

  // Runs `from` over `[p, p + n)`, testing rather than stepping while the
  // automaton stays where it is: a byte the state self-loops on costs one bit
  // test against a per-state mask instead of the dependent load a step takes
  // through the transition table. Returns the offset of the first byte that
  // moves the automaton, or `n` when the whole run self-loops; `out` is the
  // state after that byte, and `from` when the run was consumed whole.
  size_t StepRun(State from, const byte_type* p, size_t n,
                 State& out) const noexcept {
    const auto* loop =
      _loop.data() + (size_t{from} >> _stride_bits) * kLiveWords;
    // A whole chunk is tested without a branch, so the case this exists for --
    // a run that does not move the automaton at all -- costs one load and two
    // ALU ops per byte and mispredicts nothing. Only a chunk that came back
    // dirty is walked byte by byte, and only to say where it went wrong.
    size_t i = 0;
    for (; i + kRunChunk <= n; i += kRunChunk) {
      bitset::word_t moved = 0;
      for (size_t j = 0; j != kRunChunk; ++j) {
        const size_t label = p[i + j];
        moved |= ~(loop[bitset::word(label)] >> bitset::bit(label));
      }
      if ((moved & 1) != 0) {
        break;
      }
    }
    for (; i != n; ++i) {
      const size_t label = p[i];
      if (((loop[bitset::word(label)] >> bitset::bit(label)) & 1) == 0) {
        out = Step(from, p[i]);
        return i;
      }
    }
    out = from;
    return n;
  }

  // Smallest label above `label` that leaves `state` alive.
  bool NextLabel(State state, uint32_t label, uint32_t& out) const noexcept {
    if (label >= kMaxLabel) {
      return false;
    }
    const auto* live =
      _live.data() + (size_t{state} >> _stride_bits) * kLiveWords;
    const size_t from = label + 1;
    size_t word = bitset::word(from);
    auto bits = live[word] & (~bitset::word_t{0} << bitset::bit(from));
    while (bits == 0) {
      if (++word == kLiveWords) {
        return false;
      }
      bits = live[word];
    }
    out = static_cast<uint32_t>(bitset::bit_offset(word) +
                                static_cast<size_t>(std::countr_zero(bits)));
    return true;
  }

  // Whole-term acceptance, for callers that test one key at a time instead of
  // walking a dictionary.
  bool Matches(bytes_view term) const noexcept {
    auto state = _start;
    for (const auto label : term) {
      state = Step(state, label);
      if (!Alive(state)) {
        return false;
      }
    }
    PayloadType payload{};
    return Accept(state, payload);
  }

  // Memory RE2 may spend on the program plus the determinization that builds
  // the table. Deliberately generous: a pattern that exhausts the budget
  // degrades into rejecting keys, which would change the result set, so the
  // budget is sized to be unreachable for the patterns a term regexp has
  // rather than to bound anything.
  static constexpr int64_t kDefaultMaxMem = 64 << 20;

#ifdef SDB_DEV
  static size_t Builds() noexcept;
#endif

 private:
  static constexpr State kDead = 0;
  static constexpr uint32_t kMaxLabel = 255;
  static constexpr size_t kLiveWords = bitset::bits_to_words(kMaxLabel + 1);
  static constexpr size_t kRunChunk = 8;

  void Build(bytes_view pattern, RegexpSyntax syntax, bool wildcard,
             int64_t max_mem);

  // `[stride]` transitions per state, row 0 dead and absorbing.
  std::vector<State> _next;
  // 256 bits of "this label leaves the state alive", per state, which answers
  // `NextLabel` with one scan instead of one probe per label.
  std::vector<bitset::word_t> _live;
  // 256 bits of "this label leaves the state where it is", per state, which is
  // what lets a run of bytes be tested instead of stepped.
  std::vector<bitset::word_t> _loop;
  std::vector<uint8_t> _accept;
  // Held by value rather than borrowed from the `Prog`: the program is dropped
  // once the table is built, and this is the load a step takes first.
  std::array<uint8_t, 256> _bytemap{};
  State _start{kDead};
  uint32_t _stride_bits{0};
};

}  // namespace irs
