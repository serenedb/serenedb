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

#include "regexp_utils.hpp"

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

// RAII wrapper for re2::Regexp* (ref-counted via Decref)
struct RegexpDeleter {
  void operator()(re2::Regexp* re) const {
    if (re) {
      re->Decref();
    }
  }
};

using RegexpPtr = std::unique_ptr<re2::Regexp, RegexpDeleter>;

// Pattern analysis helpers

// Checks if pattern contains any unescaped metacharacters or RE2 escape
// sequences that change matching semantics (e.g. \d, \w, \b, \p).
bool HasMetacharacters(bytes_view pattern) noexcept {
  bool escaped = false;
  for (byte_type c : pattern) {
    if (escaped) {
      escaped = false;
      // After \, only actual regexp metacharacters are "simple escapes"
      // (e.g. \. \* \( \{ — just remove special meaning).
      // Everything else (\d, \w, \s, \b, \p, \Q, etc.) is an RE2
      // feature that changes matching semantics.
      if (!IsSimpleEscape(c)) {
        return true;
      }
      continue;
    }
    if (c == RegexpMeta::kEscape) {
      escaped = true;
      continue;
    }
    if (IsRegexpMeta(c)) {
      return true;
    }
  }
  return false;
}

bool HasEscapes(bytes_view pattern) noexcept {
  for (byte_type c : pattern) {
    if (c == RegexpMeta::kEscape) {
      return true;
    }
  }
  return false;
}

// Checks if pattern is a literal prefix followed by .* at the very end,
// with no RE2 special escape sequences in the prefix part.
bool IsLiteralPrefixDotStar(bytes_view pattern) noexcept {
  if (pattern.size() < 2) {
    return false;
  }

  bool escaped = false;
  for (size_t i = 0; i < pattern.size(); ++i) {
    if (escaped) {
      escaped = false;
      if (!IsSimpleEscape(pattern[i])) {
        return false;
      }
      continue;
    }
    if (pattern[i] == RegexpMeta::kEscape) {
      escaped = true;
      continue;
    }
    // First unescaped metacharacter must be '.' followed by '*' at the very end
    if (IsRegexpMeta(pattern[i])) {
      return pattern[i] == RegexpMeta::kDot && i + 1 == pattern.size() - 1 &&
             pattern[i + 1] == RegexpMeta::kStar;
    }
  }

  return false;
}

// Automaton building primitives

automaton MakeEpsilon() {
  automaton a;
  a.AddStates(1);
  a.SetStart(0);
  a.SetFinal(0);
  return a;
}

automaton MakeCharFromRune(int rune) {
  byte_type buf[utf8_utils::kMaxCharSize];
  auto len = utf8_utils::FromChar32(static_cast<uint32_t>(rune), buf);
  return MakeChar(bytes_view{buf, len});
}


void AddUtf8ByteRange(automaton& a, automaton::StateId from,
                      automaton::StateId to, const byte_type* lo,
                      const byte_type* hi, int len) {
  if (len == 1) {
    a.EmplaceArc(from, RangeLabel::From(lo[0], hi[0]), to);
    return;
  }

  if (lo[0] == hi[0]) {
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

automaton BuildUtf8RangeAutomaton(uint32_t lo, uint32_t hi) {
  if (lo > hi) {
    return {};
  }

  constexpr uint32_t kBoundaries[] = {0x80, 0x800, 0x10000, 0x110000};

  std::vector<automaton> parts;
  auto cur = lo;

  for (auto bound : kBoundaries) {
    if (cur >= bound) {
      continue;
    }
    if (cur > hi) {
      break;
    }
    auto end = std::min(hi, bound - 1);

    byte_type lo_buf[utf8_utils::kMaxCharSize];
    byte_type hi_buf[utf8_utils::kMaxCharSize];
    auto byte_len = utf8_utils::FromChar32(cur, lo_buf);
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

  if (parts.empty()) {
    return {};
  }
  auto result = std::move(parts[0]);
  for (size_t i = 1; i < parts.size(); ++i) {
    fst::Union(&result, parts[i]);
  }
  return result;
}

automaton BuildCharClassFromRe2(re2::CharClass* cc) {
  if (cc == nullptr || cc->empty()) {
    return {};
  }

  if (cc->full()) {
    return MakeAny();
  }

  bool all_ascii = true;
  for (auto it = cc->begin(); it != cc->end(); ++it) {
    if (it->hi > 0x7F) {
      all_ascii = false;
      break;
    }
  }

  if (all_ascii) {
    automaton a;
    a.AddStates(2);
    a.SetStart(0);
    a.SetFinal(1);
    for (auto it = cc->begin(); it != cc->end(); ++it) {
      a.EmplaceArc(0, RangeLabel::From(it->lo, it->hi), 1);
    }
    return a;
  }

  std::vector<automaton> parts;
  for (auto it = cc->begin(); it != cc->end(); ++it) {
    if (it->lo <= 0x7F && it->hi <= 0x7F) {
      automaton a;
      a.AddStates(2);
      a.SetStart(0);
      a.SetFinal(1);
      a.EmplaceArc(0, RangeLabel::From(it->lo, it->hi), 1);
      parts.push_back(std::move(a));
    } else {
      parts.push_back(BuildUtf8RangeAutomaton(static_cast<uint32_t>(it->lo),
                                              static_cast<uint32_t>(it->hi)));
    }
  }

  if (parts.empty()) {
    return {};
  }

  auto result = std::move(parts[0]);
  for (size_t i = 1; i < parts.size(); ++i) {
    fst::Union(&result, parts[i]);
  }
  return result;
}

// RE2 AST → FST automaton walker
//
// After RE2 Parse() + Simplify(), the tree contains only simple ops.
// kRegexpRepeat ({n,m}) is eliminated by Simplify() and expanded
// into Concat/Quest combinations, so we don't need to handle it.
//
// Error handling strategy:
//   RE2 AST invariants (e.g. Star has 1 child): SDB_ASSERT — these
//   indicate a bug in RE2 or in our code, not a user error.
//   kRegexpRepeat after Simplify, unknown RegexpOp: SDB_ASSERT — should
//   never happen; indicates version mismatch or corruption.
//   kRegexpNoMatch: returns empty automaton (no final states) — legitimate
//   AST node, not an error.
class Re2ToAutomatonWalker : public re2::Regexp::Walker<automaton> {
 public:
  Re2ToAutomatonWalker() : _error{false} {}

  bool has_error() const { return _error; }

  automaton PostVisit(re2::Regexp* re, automaton parent_arg, automaton pre_arg,
                      automaton* child_args, int nchild_args) override {
    switch (re->op()) {
      case re2::kRegexpLiteral:
        return MakeCharFromRune(re->rune());

      case re2::kRegexpLiteralString: {
        if (re->nrunes() == 0) {
          return MakeEpsilon();
        }
        automaton result = MakeCharFromRune(re->runes()[re->nrunes() - 1]);
        for (int i = re->nrunes() - 2; i >= 0; --i) {
          auto ch = MakeCharFromRune(re->runes()[i]);
          fst::Concat(ch, &result);
        }
        return result;
      }

      case re2::kRegexpConcat: {
        if (nchild_args == 0) {
          return MakeEpsilon();
        }
        if (nchild_args == 1) {
          return std::move(child_args[0]);
        }
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
        if (nchild_args == 0) {
          return MakeEpsilon();
        }
        if (nchild_args == 1) {
          return std::move(child_args[0]);
        }
        auto result = std::move(child_args[0]);
        for (int i = 1; i < nchild_args; ++i) {
          fst::Union(&result, child_args[i]);
        }
        return result;
      }

      case re2::kRegexpStar:
        SDB_ASSERT(nchild_args == 1);
        fst::Closure(&child_args[0], fst::ClosureType::CLOSURE_STAR);
        return std::move(child_args[0]);

      case re2::kRegexpPlus:
        SDB_ASSERT(nchild_args == 1);
        fst::Closure(&child_args[0], fst::ClosureType::CLOSURE_PLUS);
        return std::move(child_args[0]);

      case re2::kRegexpQuest:
        SDB_ASSERT(nchild_args == 1);
        fst::Union(&child_args[0], MakeEpsilon());
        return std::move(child_args[0]);

      case re2::kRegexpAnyChar:
        return MakeAny();

      case re2::kRegexpAnyByte: {
        // \C in RE2 — matches a single raw byte (0x00–0xFF).
        // This can split multi-byte UTF-8 sequences, but we handle it
        // faithfully at the byte level.
        automaton a;
        a.AddStates(2);
        a.SetStart(0);
        a.SetFinal(1);
        a.EmplaceArc(0, RangeLabel::From(0x00, 0xFF), 1);
        return a;
      }

      case re2::kRegexpCharClass:
        return BuildCharClassFromRe2(re->cc());

      case re2::kRegexpCapture:
        SDB_ASSERT(nchild_args == 1);
        return std::move(child_args[0]);

      case re2::kRegexpEmptyMatch:
        return MakeEpsilon();

      case re2::kRegexpNoMatch:
        return {};

      // Anchors: the automaton matches terms whole (from start to end),
      // so anchors add no additional constraint — epsilon is correct.
      // (?m) multiline mode produces BeginLine/EndLine instead of
      // BeginText/EndText, but for whole-term matching the semantics
      // are identical: the term has no internal newlines to anchor on.
      case re2::kRegexpBeginLine:
      case re2::kRegexpEndLine:
      case re2::kRegexpBeginText:
      case re2::kRegexpEndText:
        return MakeEpsilon();

      // \b — word boundary. In whole-term matching the start and end
      // of the term are always word boundaries, so this is a no-op.
      case re2::kRegexpWordBoundary:
        return MakeEpsilon();

      // \B — not-a-word-boundary. In whole-term matching the start and
      // end of the term are always word boundaries, so \B at those
      // positions can never be satisfied → matches nothing.
      case re2::kRegexpNoWordBoundary:
        return {};

      case re2::kRegexpHaveMatch:
        return MakeEpsilon();

      case re2::kRegexpRepeat:
        // Simplify() must eliminate all Repeat nodes
        SDB_ASSERT(false);
        _error = true;
        return {};

      default:
        SDB_ASSERT(false);
        _error = true;
        return {};
    }
  }

  automaton ShortVisit(re2::Regexp* re, automaton parent_arg) override {
    return MakeEpsilon();
  }

  automaton Copy(automaton arg) override {
    // arg is already copy-constructed (passed by value).
    // VectorFst has a proper copy constructor.
    return arg;
  }

 private:
  bool _error;
};

}  // namespace

// Pattern analysis utilities

bytes_view UnescapeRegexp(bytes_view in, bstring& out) {
  out.clear();
  out.reserve(in.size());

  bool escaped = false;
  for (byte_type c : in) {
    if (escaped) {
      // Should only be called for patterns classified as
      // LiteralEscaped/PrefixEscaped — only simple escapes allowed.
      SDB_ASSERT(IsSimpleEscape(c));
      out.push_back(c);
      escaped = false;
    } else if (c == RegexpMeta::kEscape) {
      escaped = true;
    } else {
      out.push_back(c);
    }
  }
  if (escaped) {
    out.push_back(RegexpMeta::kEscape);
  }

  return bytes_view{out.data(), out.size()};
}

RegexpType ComputeRegexpType(bytes_view pattern) noexcept {
  if (pattern.empty()) {
    return RegexpType::Literal;
  }

  if (!HasMetacharacters(pattern)) {
    return HasEscapes(pattern) ? RegexpType::LiteralEscaped
                               : RegexpType::Literal;
  }

  if (IsLiteralPrefixDotStar(pattern)) {
    bytes_view prefix{pattern.data(), pattern.size() - 2};
    return HasEscapes(prefix) ? RegexpType::PrefixEscaped : RegexpType::Prefix;
  }

  return RegexpType::Complex;
}

bytes_view ExtractRegexpPrefix(bytes_view pattern) noexcept {
  SDB_ASSERT(pattern.size() >= 2);
  return bytes_view{pattern.data(), pattern.size() - 2};
}

// RE2-based regexp → automaton

automaton FromRegexpRe2(bytes_view pattern) {
  // ComputeRegexpType routes empty patterns to ByTerm (Literal path),
  // so FromRegexpRe2 should never be called with an empty pattern.
  SDB_ASSERT(!pattern.empty());
  if (pattern.empty()) {
    return MakeEpsilon();
  }

  absl::string_view sv(reinterpret_cast<const char*>(pattern.data()),
                       pattern.size());

  // Step 1: Parse pattern → AST.
  // TODO: make dialect configurable (LikePerl / POSIX / custom flags)
  re2::RegexpStatus status;
  RegexpPtr re{re2::Regexp::Parse(sv, re2::Regexp::LikePerl, &status)};
  if (!re) {
    return {};
  }

  // Step 2: Simplify/optimize AST.
  // Rewrites {n,m} into Star/Plus/Quest, coalesces runs, normalizes classes.
  RegexpPtr sre{re->Simplify()};
  SDB_ASSERT(sre != nullptr);
  if (!sre) {
    return {};
  }

  // Step 3: Walk the simplified AST → build FST automaton.
  Re2ToAutomatonWalker walker;
  auto nfa = walker.Walk(sre.get(), MakeEpsilon());

  SDB_ASSERT(!walker.stopped_early());
  SDB_ASSERT(!walker.has_error());

  if (walker.has_error() || walker.stopped_early()) {
    return {};
  }

  // Step 4: Determinize NFA → DFA.
  nfa.SetProperties(fst::kILabelSorted, fst::kILabelSorted);

  automaton dfa;
  if (fst::DeterminizeStar(nfa, &dfa)) {
    return {};
  }

  return dfa;
}

}  // namespace irs
