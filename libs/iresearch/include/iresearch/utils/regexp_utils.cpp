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

#include "regexp_utils.hpp"

#include "automaton_utils.hpp"
#include "fst/closure.h"
#include "fst/concat.h"
#include "fst/union.h"
#include "fstext/determinize-star.h"
#include "iresearch/utils/utf8_utils.hpp"
#include "re2/regexp.h"
#include "re2/unicode_casefold.h"
#include "re2/walker-inl.h"

namespace irs {
namespace {

// RAII wrapper for re2::Regexp* (ref-counted via Decref)
struct RegexpDeleter {
  void operator()(re2::Regexp* re) const noexcept {
    if (re) {
      re->Decref();
    }
  }
};

using RegexpPtr = std::unique_ptr<re2::Regexp, RegexpDeleter>;

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

// Build automaton accepting all case-folded variants of `rune`.
// RE2 uses Unicode case folding cycles: for 'a' the cycle is {a, A},
// for 'k' it's {k, K, U+212A (Kelvin)}, for 's' it's {s, S, U+017F}, etc.
// We walk the cycle using RE2's public LookupCaseFold + ApplyFold API
// (see re2/unicode_casefold.h) to cover every variant exactly as RE2 does.
//
// The resulting automaton must be ilabel-sorted - the rest of the pipeline
// (DeterminizeStar, Match via SortedRangeExplicitMatcher) relies on this
// invariant.  CycleFoldRune returns variants in fold-cycle order, not in
// codepoint order, so for (?i:a) it yields {a, A} where 'a' (0x61) comes
// before 'A' (0x41) - producing unsorted arcs.  We fix this by collecting
// the variants first and sorting by codepoint before emitting arcs.
// UTF-8 is lexicographically consistent with codepoint ordering, so sorting
// by codepoint also sorts by first-byte ilabel.
automaton MakeCaseFoldedChar(int rune) {
  auto cycle_fold = [](re2::Rune r) -> re2::Rune {
    const re2::CaseFold* f =
      re2::LookupCaseFold(re2::unicode_casefold, re2::num_unicode_casefold, r);
    if (f == nullptr || r < f->lo) {
      return r;
    }
    return re2::ApplyFold(f, r);
  };

  std::vector<re2::Rune> variants;
  re2::Rune r = rune;
  do {
    variants.push_back(r);
    r = cycle_fold(r);
  } while (r != rune);

  absl::c_sort(variants);

  automaton a;
  a.AddStates(2);
  a.SetStart(0);
  a.SetFinal(1);

  for (re2::Rune v : variants) {
    byte_type buf[utf8_utils::kMaxCharSize];
    auto len = utf8_utils::FromChar32(static_cast<uint32_t>(v), buf);
    Utf8EmplaceArc(a, 0, bytes_view{buf, len}, 1);
  }

  return a;
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
    [[maybe_unused]] auto hi_byte_len = utf8_utils::FromChar32(end, hi_buf);
    // kBoundaries split guarantees both endpoints use the same byte length
    SDB_ASSERT(byte_len == hi_byte_len);

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
  size_t total_states = result.NumStates();
  for (size_t i = 1; i < parts.size(); ++i) {
    total_states += parts[i].NumStates();
  }
  result.ReserveStates(total_states);

  for (size_t i = 1; i < parts.size(); ++i) {
    fst::Union(&result, parts[i]);
  }

  return result;
}

automaton BuildCharClass(re2::CharClass* cc) {
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
  size_t total_states = result.NumStates();
  for (size_t i = 1; i < parts.size(); ++i) {
    total_states += parts[i].NumStates();
  }
  result.ReserveStates(total_states);

  for (size_t i = 1; i < parts.size(); ++i) {
    fst::Union(&result, parts[i]);
  }
  return result;
}

// RE2 AST -> FST automaton walker
//
// After RE2 Parse() + Simplify(), the tree contains only simple ops.
// kRegexpRepeat ({n,m}) is eliminated by Simplify() and expanded
// into Concat/Quest combinations, so we don't need to handle it.
//
// Error handling strategy:
//   RE2 AST invariants (e.g. Star has 1 child): SDB_ASSERT - these
//   indicate a bug in RE2 or in our code, not a user error.
//   kRegexpRepeat after Simplify, unknown RegexpOp: SDB_ASSERT - should
//   never happen; indicates version mismatch or corruption.
//   kRegexpNoMatch: returns empty automaton (no final states) - legitimate
//   AST node, not an error.
class RegexpToAutomatonWalker : public re2::Regexp::Walker<automaton> {
 public:
  // NOLINTNEXTLINE(clang-analyzer-optin.cplusplus.UninitializedObject)
  RegexpToAutomatonWalker() : _error{false} {}

  bool HasError() const { return _error; }

  automaton PostVisit(re2::Regexp* re, automaton parent_arg, automaton pre_arg,
                      automaton* child_args, int nchild_args) override {
    switch (re->op()) {
      case re2::kRegexpLiteral: {
        // RE2 encodes case-insensitive single chars (e.g. from (?i:a))
        // as kRegexpLiteral with FoldCase flag set - we must accept all
        // case-folded variants of the rune.
        if (re->parse_flags() & re2::Regexp::FoldCase) {
          return MakeCaseFoldedChar(re->rune());
        }
        return MakeCharFromRune(re->rune());
      }

      case re2::kRegexpLiteralString: {
        if (re->nrunes() == 0) {
          return MakeEpsilon();
        }
        const bool fold_case = (re->parse_flags() & re2::Regexp::FoldCase) != 0;
        auto make_char = [fold_case](int rune) {
          return fold_case ? MakeCaseFoldedChar(rune) : MakeCharFromRune(rune);
        };
        automaton result = make_char(re->runes()[re->nrunes() - 1]);
        for (int i = re->nrunes() - 2; i >= 0; --i) {
          auto ch = make_char(re->runes()[i]);
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

        // Build concatenation by prepending children in reverse order.
        // fst::Concat(A, &B) prepends A before B (modifying B in place),
        // so to build child[0]Xchild[1]X...Xchild[N-1] we start from
        // child[N-1] and prepend child[N-2], child[N-3], ..., child[0].
        // This matches the wildcard approach and avoids the overhead of
        // the appending alternative (which would need to rewire growing
        // final states on each step).
        automaton result = std::move(child_args[nchild_args - 1]);
        size_t total_states = result.NumStates();
        for (int i = 0; i < nchild_args - 1; ++i) {
          total_states += child_args[i].NumStates();
        }
        result.ReserveStates(total_states);

        for (int i = nchild_args - 2; i >= 0; --i) {
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
        size_t total_states = result.NumStates();
        for (int i = 1; i < nchild_args; ++i) {
          total_states += child_args[i].NumStates();
        }
        result.ReserveStates(total_states);

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
        // \C in RE2 - matches a single raw byte (0x00-0xFF).
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
        return BuildCharClass(re->cc());

      case re2::kRegexpCapture:
        SDB_ASSERT(nchild_args == 1);
        return std::move(child_args[0]);

      case re2::kRegexpEmptyMatch:
        return MakeEpsilon();

      case re2::kRegexpNoMatch:
        return {};

      // Anchors: the automaton matches terms whole (from start to end),
      // so anchors add no additional constraint - epsilon is correct.
      // (?m) multiline mode produces BeginLine/EndLine instead of
      // BeginText/EndText, but for whole-term matching the semantics
      // are identical: the term has no internal newlines to anchor on.
      case re2::kRegexpBeginLine:
      case re2::kRegexpEndLine:
      case re2::kRegexpBeginText:
      case re2::kRegexpEndText:
        return MakeEpsilon();

      // \b - word boundary. In whole-term matching the start and end
      // of the term are always word boundaries, so this is a no-op.
      case re2::kRegexpWordBoundary:
        return MakeEpsilon();

      // \B - not-a-word-boundary. Zero-width assertions cannot be
      // faithfully modeled in our DFA without doubling the state space
      // to track "previous character was word/non-word".  We conservatively
      // return an empty automaton - any pattern containing \B matches
      // nothing.  This is acceptable because:
      //   1) Terms are typically tokenizer output (whole words), making
      //      intra-term \B largely meaningless.
      //   2) \B in user-facing regexp filters is extremely rare.
      //   3) Underapproximation (no false positives) is safer for a
      //      search engine than overapproximation.
      // If full \b/\B support is ever needed, the approach is to split
      // each automaton state into (state, prev_was_word_char) pairs.
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
      // LiteralEscaped/PrefixEscaped - only simple escapes allowed.
      SDB_ASSERT(IsSimpleEscape(c));
      out.push_back(c);
      escaped = false;
    } else if (c == AsByte(RegexpMeta::kEscape)) {
      escaped = true;
    } else {
      out.push_back(c);
    }
  }
  if (escaped) {
    out.push_back(AsByte(RegexpMeta::kEscape));
  }

  return bytes_view{out.data(), out.size()};
}

RegexpType ComputeRegexpType(bytes_view pattern) noexcept {
  if (pattern.empty()) {
    return RegexpType::Literal;
  }

  bool has_escapes = false;
  bool escaped = false;
  for (size_t i = 0; i < pattern.size(); ++i) {
    if (escaped) {
      escaped = false;
      if (!IsSimpleEscape(pattern[i])) {
        return RegexpType::Complex;
      }
      has_escapes = true;
      continue;
    }
    if (pattern[i] == AsByte(RegexpMeta::kEscape)) {
      escaped = true;
      continue;
    }
    if (!IsRegexpMeta(pattern[i])) {
      continue;
    }
    // First unescaped metacharacter: only .* at end is Prefix
    if (pattern[i] == AsByte(RegexpMeta::kDot) && i + 1 == pattern.size() - 1 &&
        pattern[i + 1] == AsByte(RegexpMeta::kStar)) {
      return has_escapes ? RegexpType::PrefixEscaped : RegexpType::Prefix;
    }
    return RegexpType::Complex;
  }

  if (escaped) {
    has_escapes = true;
  }

  return has_escapes ? RegexpType::LiteralEscaped : RegexpType::Literal;
}

bytes_view ExtractRegexpPrefix(bytes_view pattern) noexcept {
  SDB_ASSERT(pattern.size() >= 2);
  return bytes_view{pattern.data(), pattern.size() - 2};
}

// RE2-based regexp -> automaton

automaton FromRegexp(bytes_view pattern, int64_t max_dfa_states,
                     RegexpSyntax syntax) {
  // ComputeRegexpType routes empty patterns to ByTerm (Literal path),
  // so FromRegexp should never be called with an empty pattern.
  SDB_ASSERT(!pattern.empty());
  if (pattern.empty()) {
    return MakeEpsilon();
  }

  absl::string_view sv(reinterpret_cast<const char*>(pattern.data()),
                       pattern.size());

  // Parse pattern -> AST.
  //
  // Perl mode: LikePerl is the most permissive built-in mode:
  //   LikePerl = ClassNL | OneLine | PerlB | PerlX | UnicodeGroups
  //
  // Why LikePerl and not individual flags:
  //   - PerlX: enables (?:...), (?i:...), (?P<n>...) - essential for
  //     advanced users.  Without it these are parse errors.
  //   - UnicodeGroups: enables \p{Cyrillic}, \P{Latin} etc. - essential
  //     for i18n.  Without it these are parse errors.
  //   - PerlB: enables \b/\B.  \b is correct for whole-term matching
  //     (boundaries are always word boundaries).  \B is a documented
  //     underapproximation (matches nothing).  Without PerlB both are
  //     parse errors, which is worse.
  //   - OneLine: ^ and $ match only start/end of text.  Redundant for
  //     whole-term matching (always effectively on), but harmless.
  //   - ClassNL: [^a] can match \n.  Irrelevant - indexed terms
  //     typically don't contain newlines.
  //
  // POSIX ERE mode: ClassNL | OneLine only.  Deliberately omits every
  // Perl extension flag so that \d, \w, \b, (?:...), (?i:...), \Q...\E,
  // \p{...}, (?P<>), \C all produce parse errors (empty automaton via
  // the !re branch below).  Users get strict POSIX ERE semantics:
  // literals, . * + ? | () [] {n,m}, anchors ^ $, POSIX char classes
  // like [[:alpha:]].  ClassNL and OneLine are kept because they don't
  // enable Perl features - they only pin down whether ^/$ and [^...]
  // treat \n specially, and the POSIX-consistent choice is to treat \n
  // as just another byte.
  //
  // Flags we intentionally do NOT set in either mode:
  //   - FoldCase: case folding is handled by the analyzer at index time,
  //     not by the regexp engine.  Users can still use (?i:...) inline
  //     in Perl mode.
  //   - DotNL: let . match \n - irrelevant, terms don't contain \n.
  //   - NonGreedy: greedy vs non-greedy doesn't affect DFA acceptance
  //     (only matters for capturing, which we don't do).
  //   - NeverNL: never match \n - irrelevant for same reason as DotNL.
  //   - NeverCapture: minor parser optimization, walker already ignores
  //     captures.
  //   - Literal: redundant - ComputeRegexpType already fast-paths literals.
  //   - Latin1: exotic, UTF-8 is the standard.
  const auto flags = (syntax == RegexpSyntax::Perl)
                       ? re2::Regexp::LikePerl

                       : (re2::Regexp::ClassNL | re2::Regexp::OneLine);

  re2::RegexpStatus status;
  RegexpPtr re{re2::Regexp::Parse(sv, flags, &status)};
  if (!re) {
    SDB_ERROR(IRESEARCH, "RE2 regexp parse error: ", status.Text());
    return {};
  }

  // Simplify/optimize AST.
  // Rewrites {n,m} into Star/Plus/Quest, coalesces runs, normalizes classes.
  RegexpPtr sre{re->Simplify()};
  SDB_ASSERT(sre != nullptr);
  if (!sre) {
    return {};
  }

  // Walk the simplified AST -> build FST automaton.
  RegexpToAutomatonWalker walker;
  auto nfa = walker.Walk(sre.get(), MakeEpsilon());

  SDB_ASSERT(!walker.stopped_early());
  SDB_ASSERT(!walker.HasError());

  if (walker.HasError() || walker.stopped_early()) {
    return {};
  }

  // Determinize NFA -> DFA.
  SDB_ASSERT(nfa.Properties(fst::kILabelSorted, true) & fst::kILabelSorted);
  nfa.SetProperties(fst::kILabelSorted, fst::kILabelSorted);

  automaton dfa;
  if (fst::DeterminizeStar(nfa, &dfa)) {
    SDB_ERROR(IRESEARCH, "RE2 regexp determinization failed");
    return {};
  }

  // Guard against exponential state blowup from pathological
  // patterns (e.g. [ab]{20} can produce 2^20 DFA states).
  if (max_dfa_states > 0 && dfa.NumStates() > max_dfa_states) {
    SDB_ERROR(IRESEARCH, "RE2 regexp DFA too large: ", dfa.NumStates(),
              " states (limit: ", max_dfa_states, ")");
    return {};
  }

  return dfa;
}

}  // namespace irs
