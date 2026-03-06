////////////////////////////////////////////////////////////////////////////////
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

#include "regexp_utils_re2.hpp"

#include "automaton_utils.hpp"
#include "basics/utf8_utils.hpp"
#include "fst/closure.h"
#include "fst/concat.h"
#include "fst/union.h"
#include "fstext/determinize-star.h"
#include "re2/regexp.h"
#include "re2/walker-inl.h"

namespace irs {
namespace {

automaton MakeEpsilon() {
  automaton a;
  a.AddStates(1);
  a.SetStart(0);
  a.SetFinal(0);
  return a;
}

// Build automaton for a single Unicode codepoint (Rune → UTF-8 → MakeChar)
automaton MakeCharFromRune(int rune) {
  byte_type buf[utf8_utils::kMaxCharSize];
  uint32_t len = utf8_utils::FromChar32(static_cast<uint32_t>(rune), buf);
  return MakeChar(bytes_view{buf, len});
}

// ---------------------------------------------------------------------------
// UTF-8 byte-range automaton builder
//
// Given a range [lo, hi] of Unicode codepoints, builds an automaton that
// accepts exactly the UTF-8 byte sequences encoding those codepoints.
//
// Algorithm:
// 1. Split [lo, hi] at UTF-8 byte-length boundaries (0x80, 0x800, 0x10000)
//    so each sub-range has uniform byte length.
// 2. For each sub-range, recursively build a byte-level automaton:
//    - If first bytes of lo and hi are equal, emit that byte and recurse.
//    - Otherwise, split into 3 parts: lo's first byte (partial tail),
//      middle first bytes (full continuation range), hi's first byte
//      (partial tail).
// ---------------------------------------------------------------------------

// Add arcs from `from` to `to` in automaton `a` that accept all UTF-8
// byte sequences between lo_bytes and hi_bytes (inclusive).
// Both lo_bytes and hi_bytes must have the same length `len`.
void AddUtf8ByteRange(automaton& a, automaton::StateId from,
                      automaton::StateId to, const byte_type* lo,
                      const byte_type* hi, int len) {
  if (len == 1) {
    a.EmplaceArc(from, RangeLabel::From(lo[0], hi[0]), to);
    return;
  }

  if (lo[0] == hi[0]) {
    // Same first byte — emit it and recurse on the tail
    auto mid = a.AddState();
    a.EmplaceArc(from, RangeLabel::From(lo[0], lo[0]), mid);
    AddUtf8ByteRange(a, mid, to, lo + 1, hi + 1, len - 1);
    return;
  }

  // lo[0] < hi[0]: split into up to 3 parts

  // Part 1: first byte = lo[0], tail from lo[1:] to [0xBF, 0xBF, ...]
  {
    byte_type max_tail[4];
    std::fill_n(max_tail, len - 1, byte_type{0xBF});
    auto mid = a.AddState();
    a.EmplaceArc(from, RangeLabel::From(lo[0], lo[0]), mid);
    AddUtf8ByteRange(a, mid, to, lo + 1, max_tail, len - 1);
  }

  // Part 2: first byte in [lo[0]+1 .. hi[0]-1], full continuation range
  if (lo[0] + 1 <= hi[0] - 1) {
    byte_type min_tail[4], max_tail[4];
    std::fill_n(min_tail, len - 1, byte_type{0x80});
    std::fill_n(max_tail, len - 1, byte_type{0xBF});
    auto mid = a.AddState();
    a.EmplaceArc(from, RangeLabel::From(lo[0] + 1, hi[0] - 1), mid);
    AddUtf8ByteRange(a, mid, to, min_tail, max_tail, len - 1);
  }

  // Part 3: first byte = hi[0], tail from [0x80, 0x80, ...] to hi[1:]
  {
    byte_type min_tail[4];
    std::fill_n(min_tail, len - 1, byte_type{0x80});
    auto mid = a.AddState();
    a.EmplaceArc(from, RangeLabel::From(hi[0], hi[0]), mid);
    AddUtf8ByteRange(a, mid, to, min_tail, hi + 1, len - 1);
  }
}

// Build an automaton accepting UTF-8 encodings of codepoints in [lo, hi].
// Splits at byte-length boundaries and delegates to AddUtf8ByteRange.
automaton BuildUtf8RangeAutomaton(uint32_t lo, uint32_t hi) {
  if (lo > hi)
    return {};

  // UTF-8 byte-length boundaries
  constexpr uint32_t kBoundaries[] = {0x80, 0x800, 0x10000, 0x110000};

  std::vector<automaton> parts;
  uint32_t cur = lo;

  for (uint32_t bound : kBoundaries) {
    if (cur >= bound)
      continue;
    if (cur > hi)
      break;
    uint32_t end = std::min(hi, bound - 1);

    // [cur, end] — all codepoints have the same UTF-8 byte length
    byte_type lo_buf[utf8_utils::kMaxCharSize];
    byte_type hi_buf[utf8_utils::kMaxCharSize];
    uint32_t byte_len = utf8_utils::FromChar32(cur, lo_buf);
    utf8_utils::FromChar32(end, hi_buf);

    automaton a;
    auto start = a.AddState();
    auto final_st = a.AddState();
    a.SetStart(start);
    a.SetFinal(final_st);
    AddUtf8ByteRange(a, start, final_st, lo_buf, hi_buf,
                     static_cast<int>(byte_len));
    parts.push_back(std::move(a));

    cur = bound;
  }

  if (parts.empty())
    return {};
  automaton result = std::move(parts[0]);
  for (size_t i = 1; i < parts.size(); ++i) {
    fst::Union(&result, parts[i]);
  }
  return result;
}

// Build automaton for a character class from RE2's CharClass ranges
automaton BuildCharClassFromRe2(re2::CharClass* cc) {
  if (cc == nullptr || cc->empty()) {
    return {};  // no-match
  }

  // If it's a full class (matches everything), return MakeAny
  if (cc->full()) {
    return MakeAny();
  }

  // Check if all ranges are single-byte (ASCII) for efficient RangeLabel path
  bool all_ascii = true;
  for (auto it = cc->begin(); it != cc->end(); ++it) {
    if (it->hi > 0x7F) {
      all_ascii = false;
      break;
    }
  }

  if (all_ascii) {
    // Fast path: use RangeLabel arcs directly
    automaton a;
    a.AddStates(2);
    a.SetStart(0);
    a.SetFinal(1);
    for (auto it = cc->begin(); it != cc->end(); ++it) {
      a.EmplaceArc(0, RangeLabel::From(it->lo, it->hi), 1);
    }
    return a;
  }

  // General case: build proper UTF-8 byte-level automata for each range
  std::vector<automaton> parts;
  for (auto it = cc->begin(); it != cc->end(); ++it) {
    if (it->lo <= 0x7F && it->hi <= 0x7F) {
      // ASCII range — single arc with RangeLabel
      automaton a;
      a.AddStates(2);
      a.SetStart(0);
      a.SetFinal(1);
      a.EmplaceArc(0, RangeLabel::From(it->lo, it->hi), 1);
      parts.push_back(std::move(a));
    } else if (it->hi - it->lo < 256) {
      // Small multi-byte range — expand individual codepoints (simple path)
      for (int cp = it->lo; cp <= it->hi; ++cp) {
        parts.push_back(MakeCharFromRune(cp));
      }
    } else {
      // Large multi-byte range — build proper UTF-8 byte-range automaton
      parts.push_back(BuildUtf8RangeAutomaton(static_cast<uint32_t>(it->lo),
                                              static_cast<uint32_t>(it->hi)));
    }
  }

  if (parts.empty()) {
    return {};
  }

  automaton result = std::move(parts[0]);
  for (size_t i = 1; i < parts.size(); ++i) {
    fst::Union(&result, parts[i]);
  }
  return result;
}

// Walker that converts RE2's Regexp AST into an FST automaton.
// After RE2 Parse() + Simplify(), the tree contains only simple ops:
//   Literal, LiteralString, Concat, Alternate, Star, Plus, Quest,
//   AnyChar, AnyByte, CharClass, Capture, EmptyMatch, NoMatch,
//   BeginLine, EndLine, BeginText, EndText, etc.
//
// kRegexpRepeat ({n,m}) is eliminated by Simplify() and expanded
// into Concat/Quest combinations, so we don't need to handle it.
class Re2ToAutomatonWalker : public re2::Regexp::Walker<automaton> {
 public:
  Re2ToAutomatonWalker() : _error(false) {}

  bool has_error() const { return _error; }

  automaton PostVisit(re2::Regexp* re, automaton parent_arg, automaton pre_arg,
                      automaton* child_args, int nchild_args) override {
    switch (re->op()) {
      case re2::kRegexpLiteral:
        return MakeCharFromRune(re->rune());

      case re2::kRegexpLiteralString: {
        // Build concatenation of all runes in the string
        if (re->nrunes() == 0)
          return MakeEpsilon();

        // Build in reverse for efficient prepend-concat
        automaton result = MakeCharFromRune(re->runes()[re->nrunes() - 1]);
        for (int i = re->nrunes() - 2; i >= 0; --i) {
          automaton ch = MakeCharFromRune(re->runes()[i]);
          fst::Concat(ch, &result);
        }
        return result;
      }

      case re2::kRegexpConcat: {
        if (nchild_args == 0)
          return MakeEpsilon();
        if (nchild_args == 1)
          return std::move(child_args[0]);

        // Reverse-order prepend for efficiency (same as original parser)
        automaton result;
        result.SetStart(result.AddState());
        result.SetFinal(0, true);

        size_t total_states = result.NumStates();
        for (int i = 0; i < nchild_args; ++i) {
          total_states += child_args[i].NumStates();
        }
        result.ReserveStates(total_states);

        for (int i = nchild_args - 1; i >= 0; --i) {
          fst::Concat(child_args[i], &result);
        }
        return result;
      }

      case re2::kRegexpAlternate: {
        if (nchild_args == 0)
          return MakeEpsilon();
        if (nchild_args == 1)
          return std::move(child_args[0]);

        automaton result = std::move(child_args[0]);
        for (int i = 1; i < nchild_args; ++i) {
          fst::Union(&result, child_args[i]);
        }
        return result;
      }

      case re2::kRegexpStar:
        if (nchild_args != 1) {
          _error = true;
          return {};
        }
        fst::Closure(&child_args[0], fst::ClosureType::CLOSURE_STAR);
        return std::move(child_args[0]);

      case re2::kRegexpPlus:
        if (nchild_args != 1) {
          _error = true;
          return {};
        }
        fst::Closure(&child_args[0], fst::ClosureType::CLOSURE_PLUS);
        return std::move(child_args[0]);

      case re2::kRegexpQuest:
        if (nchild_args != 1) {
          _error = true;
          return {};
        }
        fst::Union(&child_args[0], MakeEpsilon());
        return std::move(child_args[0]);

      case re2::kRegexpAnyChar:
        return MakeAny();

      case re2::kRegexpAnyByte:
        // Single byte: range 0x00..0xFF
        return MakeAny();

      case re2::kRegexpCharClass:
        return BuildCharClassFromRe2(re->cc());

      case re2::kRegexpCapture:
        // Capture groups are transparent — just return the child
        if (nchild_args != 1) {
          _error = true;
          return {};
        }
        return std::move(child_args[0]);

      case re2::kRegexpEmptyMatch:
        return MakeEpsilon();

      case re2::kRegexpNoMatch:
        // Automaton that accepts nothing (no final states)
        return {};

      // Anchors — in SereneDB we match full terms (not substrings),
      // so anchors are effectively no-ops (epsilon)
      case re2::kRegexpBeginLine:
      case re2::kRegexpEndLine:
      case re2::kRegexpBeginText:
      case re2::kRegexpEndText:
      case re2::kRegexpWordBoundary:
      case re2::kRegexpNoWordBoundary:
        return MakeEpsilon();

      case re2::kRegexpHaveMatch:
        // Used by RE2::Set internally, should not appear after Parse().
        // Treat as epsilon for safety.
        return MakeEpsilon();

      case re2::kRegexpRepeat:
        // Should not appear after Simplify(), but handle defensively
        _error = true;
        return {};

      default:
        _error = true;
        return {};
    }
  }

  automaton ShortVisit(re2::Regexp* re, automaton parent_arg) override {
    return MakeEpsilon();
  }

  automaton Copy(automaton arg) override {
    // Deep copy of the automaton — needed when the AST has shared subtrees
    automaton copy;
    fst::Union(&copy, arg);  // crude but correct copy
    return copy;
  }

 private:
  bool _error;
};

}  // namespace

automaton FromRegexpRe2(bytes_view pattern) {
  if (pattern.empty()) {
    return MakeEpsilon();
  }

  // Convert bytes_view to string_view for RE2
  absl::string_view sv(reinterpret_cast<const char*>(pattern.data()),
                       pattern.size());

  // Step 1: RE2 parses the pattern string into an AST (Regexp* tree)
  re2::RegexpStatus status;
  re2::Regexp* re = re2::Regexp::Parse(sv, re2::Regexp::PerlX, &status);
  if (re == nullptr) {
    return {};  // parse error
  }

  // Step 2: RE2 simplifies/optimizes the AST
  re2::Regexp* sre = re->Simplify();
  re->Decref();
  if (sre == nullptr) {
    return {};  // simplification error
  }

  // Step 3: Walk the simplified AST, building FST automaton at each node
  Re2ToAutomatonWalker walker;
  automaton nfa = walker.Walk(sre, MakeEpsilon());
  sre->Decref();

  if (walker.has_error()) {
    return {};
  }

  // Step 4: Determinize NFA → DFA (same as original FromRegexp)
  nfa.SetProperties(fst::kILabelSorted, fst::kILabelSorted);

  automaton dfa;
  if (fst::DeterminizeStar(nfa, &dfa)) {
    return {};  // determinization failed
  }

  return dfa;
}

}  // namespace irs
