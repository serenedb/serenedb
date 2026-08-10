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

#include "iresearch/utils/regexp_acceptor.hpp"

#include <algorithm>
#include <atomic>
#include <limits>
#include <memory>
#include <string>
#include <vector>

#include "basics/assert.h"
#include "basics/log.h"
#include "iresearch/utils/utf8_utils.hpp"
#include "iresearch/utils/wildcard_utils.hpp"
#include "re2/prog.h"
#include "re2/regexp.h"
#include "re2/walker-inl.h"

namespace irs {
namespace {

using ParseFlags = re2::Regexp::ParseFlags;

// RE2's compiler expands every rune range into strict UTF-8 byte sequences
// except the one range `[0x80, Runemax]`, which `Add_80_10ffff` deliberately
// widens to `[C2-DF][80-BF] | [E0-EF][80-BF]{2} | [F0-F4][80-BF]{3}` -- so an
// overlong `E0 80 80` and an above-U+10FFFF `F4 90 80 80` match. iresearch's
// model widens `.` in exactly that way only when the class is *full* (see
// `AnyCodePoint`); a partial class expands strictly, range by range.
// Splitting the range below Runemax into two class nodes is what keeps RE2 off
// the wide path, and it is a narrowing: RE2 then rejects the ill-formed
// sequences it accepts today.
constexpr re2::Rune kWideRangeLo = 0x80;
constexpr re2::Rune kBmpMax = 0xFFFF;

re2::Regexp* SplitAtBmp(re2::CharClassBuilder& low, ParseFlags flags) {
  re2::CharClassBuilder high;
  high.AddRange(kBmpMax + 1, re2::Runemax);
  re2::Regexp* subs[]{re2::Regexp::NewCharClass(low.GetCharClass(), flags),
                      re2::Regexp::NewCharClass(high.GetCharClass(), flags)};
  return re2::Regexp::AlternateNoFactor(subs, 2, flags);
}

re2::Regexp* NarrowCharClass(re2::Regexp* re, ParseFlags flags) {
  auto* cc = re->cc();
  if (!cc || cc->empty() || cc->full()) {
    return re->Incref();
  }
  const auto* last = cc->end() - 1;
  if (last->hi != re2::Runemax || last->lo > kWideRangeLo) {
    return re->Incref();
  }
  re2::CharClassBuilder low;
  for (auto* it = cc->begin(); it != cc->end(); ++it) {
    low.AddRange(it->lo, std::min(it->hi, kBmpMax));
  }
  return SplitAtBmp(low, flags);
}

// `(?s).` sets `DotNL`, which parses to `kRegexpAnyChar` rather than to the
// `[^\n]` class `.` gives -- and RE2 compiles that straight onto the wide
// path. It is the same range and it takes the same split.
re2::Regexp* NarrowAnyChar(ParseFlags flags) {
  re2::CharClassBuilder low;
  low.AddRange(0, kBmpMax);
  return SplitAtBmp(low, flags);
}

// Rebuilds the parsed tree with the whole-term reading of the empty-width
// operators, which is not the one RE2's own compiler gives them: a term is
// matched from its first byte to its last, so an anchor constrains nothing
// wherever it sits, and RE2 would make `a$b` unmatchable instead. Character
// classes are narrowed to the strict
// UTF-8 model (see `NarrowCharClass`); everything else is left to RE2's
// compiler, which is the whole point of going through a `Regexp` tree rather
// than an automaton of our own.
class RegexpRewriter : public re2::Regexp::Walker<re2::Regexp*> {
 public:
  bool HasError() const noexcept { return _error; }

  re2::Regexp* PostVisit(re2::Regexp* re, re2::Regexp* /*parent_arg*/,
                         re2::Regexp* /*pre_arg*/, re2::Regexp** child_args,
                         int nchild_args) override {
    const auto flags = static_cast<ParseFlags>(re->parse_flags());
    switch (re->op()) {
      case re2::kRegexpBeginLine:
      case re2::kRegexpEndLine:
      case re2::kRegexpBeginText:
      case re2::kRegexpEndText:
      // The first and last byte of a term are always word boundaries, and a
      // term has no interior the tokenizer would put a boundary in.
      case re2::kRegexpWordBoundary:
      case re2::kRegexpHaveMatch:
        Release(child_args, nchild_args);
        return Empty(flags);

      // `\B` cannot be modelled without splitting every state by whether the
      // previous byte was a word character. Underapproximating is the safer
      // direction for a search engine.
      case re2::kRegexpNoWordBoundary:
        Release(child_args, nchild_args);
        return re2::Regexp::Alternate(nullptr, 0, flags);

      case re2::kRegexpStar:
        SDB_ASSERT(nchild_args == 1);
        return re2::Regexp::Star(child_args[0], flags);
      case re2::kRegexpPlus:
        SDB_ASSERT(nchild_args == 1);
        return re2::Regexp::Plus(child_args[0], flags);
      case re2::kRegexpQuest:
        SDB_ASSERT(nchild_args == 1);
        return re2::Regexp::Quest(child_args[0], flags);
      case re2::kRegexpRepeat:
        // `Simplify()` rewrites every `{n,m}` into the three above.
        SDB_ASSERT(false);
        _error = true;
        Release(child_args, nchild_args);
        return Empty(flags);

      case re2::kRegexpConcat:
        return re2::Regexp::Concat(child_args, nchild_args, flags);
      case re2::kRegexpAlternate:
        return re2::Regexp::Alternate(child_args, nchild_args, flags);

      case re2::kRegexpCharClass:
        SDB_ASSERT(nchild_args == 0);
        return NarrowCharClass(re, flags);
      case re2::kRegexpAnyChar:
        SDB_ASSERT(nchild_args == 0);
        return NarrowAnyChar(flags);

      // Acceptance does not depend on where the groups are.
      case re2::kRegexpCapture:
        SDB_ASSERT(nchild_args == 1);
        return child_args[0];

      default:
        SDB_ASSERT(nchild_args == 0);
        Release(child_args, nchild_args);
        return re->Incref();
    }
  }

  re2::Regexp* ShortVisit(re2::Regexp* re,
                          re2::Regexp* /*parent_arg*/) override {
    _error = true;
    return Empty(static_cast<ParseFlags>(re->parse_flags()));
  }

  // The walk hands the same result to two positions when a node repeats, and
  // these results are owning references.
  re2::Regexp* Copy(re2::Regexp* arg) override { return arg->Incref(); }

 private:
  static re2::Regexp* Empty(ParseFlags flags) {
    return re2::Regexp::Concat(nullptr, 0, flags);
  }

  static void Release(re2::Regexp** args, int count) {
    for (int i = 0; i != count; ++i) {
      args[i]->Decref();
    }
  }

  bool _error{false};
};

// The wildcard dialect is compiled in RE2's Latin-1 encoding, where runes *are*
// bytes: a literal byte of the pattern is that byte, and `_` / `%` spell out
// iresearch's UTF-8 model byte by byte -- the loose one, overlongs and
// surrogates included. A wildcard pattern
// is arbitrary bytes, which is why it cannot go through a regexp source string
// at all.
constexpr auto kWildcardFlags =
  static_cast<ParseFlags>(static_cast<int>(re2::Regexp::Latin1) |
                          static_cast<int>(re2::Regexp::OneLine) |
                          static_cast<int>(re2::Regexp::ClassNL));

re2::Regexp* ByteClass(uint32_t lo, uint32_t hi) {
  re2::CharClassBuilder cc;
  cc.AddRange(static_cast<re2::Rune>(lo), static_cast<re2::Rune>(hi));
  return re2::Regexp::NewCharClass(cc.GetCharClass(), kWildcardFlags);
}

re2::Regexp* AnyCodePoint() {
  const auto sequence = [](uint32_t lo, uint32_t hi, int continuations) {
    re2::Regexp* subs[utf8_utils::kMaxCharSize];
    subs[0] = ByteClass(lo, hi);
    for (int i = 0; i != continuations; ++i) {
      subs[i + 1] = ByteClass(0x80, 0xBF);
    }
    return re2::Regexp::Concat(subs, continuations + 1, kWildcardFlags);
  };
  re2::Regexp* alts[]{ByteClass(0x00, 0x7F), sequence(0xC2, 0xDF, 1),
                      sequence(0xE0, 0xEF, 2), sequence(0xF0, 0xF4, 3)};
  return re2::Regexp::AlternateNoFactor(alts, 4, kWildcardFlags);
}

re2::Regexp* WildcardTree(bytes_view pattern) {
  std::vector<re2::Regexp*> parts;
  parts.reserve(pattern.size());
  bool escaped = false;
  for (const auto c : pattern) {
    if (escaped) {
      parts.emplace_back(ByteClass(c, c));
      escaped = false;
      continue;
    }
    switch (c) {
      case WildcardMatch::kAnyStr:
        parts.emplace_back(re2::Regexp::Star(AnyCodePoint(), kWildcardFlags));
        break;
      case WildcardMatch::kAnyChr:
        parts.emplace_back(AnyCodePoint());
        break;
      case WildcardMatch::kEscape:
        escaped = true;
        break;
      default:
        parts.emplace_back(ByteClass(c, c));
        break;
    }
  }
  return re2::Regexp::Concat(parts.data(), static_cast<int>(parts.size()),
                             kWildcardFlags);
}

re2::Regexp* RegexpTree(bytes_view pattern, RegexpSyntax syntax) {
  const absl::string_view sv{reinterpret_cast<const char*>(pattern.data()),
                             pattern.size()};
  // The two dialects a term regexp is offered: Perl and POSIX ERE.
  const auto flags = syntax == RegexpSyntax::Perl
                       ? re2::Regexp::LikePerl
                       : (re2::Regexp::ClassNL | re2::Regexp::OneLine);

  re2::RegexpStatus status;
  re2::Regexp* parsed = re2::Regexp::Parse(sv, flags, &status);
  if (!parsed) {
    SDB_ERROR(IRESEARCH, "RE2 regexp parse error: ", status.Text());
    return nullptr;
  }
  re2::Regexp* simple = parsed->Simplify();
  parsed->Decref();
  if (!simple) {
    return nullptr;
  }

  RegexpRewriter rewriter;
  re2::Regexp* re = rewriter.Walk(simple, nullptr);
  simple->Decref();
  if (re && (rewriter.HasError() || rewriter.stopped_early())) {
    SDB_ERROR(IRESEARCH, "RE2 regexp too deep to rewrite");
    re->Decref();
    return nullptr;
  }
  return re;
}

#ifdef SDB_DEV
// Determinizations since process start, so a dev-build test can pin how often
// one happens. Debug-only: nothing in the system reads it.
std::atomic_size_t kBuilds{0};
#endif

}  // namespace

#ifdef SDB_DEV
size_t RegexpAcceptor::Builds() noexcept {
  return kBuilds.load(std::memory_order_relaxed);
}
#endif

RegexpAcceptor::RegexpAcceptor(bytes_view pattern, RegexpSyntax syntax,
                               int64_t max_mem) {
  Build(pattern, syntax, false, max_mem);
}

RegexpAcceptor::RegexpAcceptor(WildcardTag, bytes_view pattern,
                               int64_t max_mem) {
  Build(pattern, RegexpSyntax::Perl, true, max_mem);
}

void RegexpAcceptor::Build(bytes_view pattern, RegexpSyntax syntax,
                           bool wildcard, int64_t max_mem) {
#ifdef SDB_DEV
  kBuilds.fetch_add(1, std::memory_order_relaxed);
#endif
  // Row 0 is the dead row, and it exists whatever happens below: a pattern
  // that never compiles still has to answer `Step` and `Accept` for a caller
  // that chose to intersect with it rather than report it.
  _next.assign(1, kDead);
  _range.assign(1, kNoLabels);
  _loop.assign(kMaskWords, 0);
  _accept.assign(1, 0);
  _lower.clear();

  re2::Regexp* re =
    wildcard ? WildcardTree(pattern) : RegexpTree(pattern, syntax);
  if (!re) {
    return;
  }
  std::unique_ptr<re2::Prog> prog{re->CompileToProg(max_mem)};
  re->Decref();
  if (!prog) {
    SDB_ERROR(IRESEARCH, "RE2 regexp did not compile within ", max_mem,
              " bytes");
    return;
  }
  // The match must cover the whole term: with this set, a match instruction
  // only fires on the imaginary end-of-text byte, so the automaton is
  // end-anchored natively rather than by checking where a search stopped.
  prog->set_anchor_end(true);
  prog->set_dfa_mem(max_mem);
  // `BuildEntireDFA` floods from the unanchored entry point; pointing that at
  // the anchored one is what makes the flooded automaton the one a whole-term
  // walk steps, and it is why no RE2 patch is needed to get the table out.
  prog->set_start_unanchored(prog->start());

  const auto classes = static_cast<uint32_t>(prog->bytemap_range());
  SDB_ASSERT(classes != 0);
  const uint32_t row = classes + 1;

  // One row per state as RE2 hands it over, `classes` transitions followed by
  // the end-of-text column, `-1` for dead.
  std::vector<int> rows;
  std::vector<uint8_t> match;
  bool over_budget = false;
  prog->BuildEntireDFA(re2::Prog::kLongestMatch,
                       [&](const int* next, bool is_match) {
                         if (!next) {
                           over_budget = true;
                           return;
                         }
                         rows.insert(rows.end(), next, next + row);
                         match.emplace_back(is_match);
                       });
  const auto states = static_cast<uint32_t>(match.size());
  _stride_bits = static_cast<uint32_t>(std::bit_width(classes - 1));
  const uint32_t stride = uint32_t{1} << _stride_bits;
  SDB_ASSERT(stride >= classes);
  if (over_budget ||
      (size_t{states} + 1) * stride > std::numeric_limits<State>::max()) {
    SDB_ERROR(IRESEARCH, "RE2 regexp did not determinize within ", max_mem,
              " bytes");
    _stride_bits = 0;
    return;
  }
  if (states == 0) {
    // The language is empty. That is "accepts nothing", which `ok()` reports
    // and a caller may intersect with.
    _stride_bits = 0;
    return;
  }
  _next.assign(size_t{states + 1} << _stride_bits, kDead);
  _range.assign(states + 1, kNoLabels);
  _loop.assign(size_t{states + 1} * kMaskWords, 0);
  _accept.assign(states + 1, 0);
  std::copy(prog->bytemap(), prog->bytemap() + _bytemap.size(),
            _bytemap.begin());
  _start = stride;

  for (uint32_t s = 0; s != states; ++s) {
    const int* next = rows.data() + size_t{s} * row;
    const State base = (s + 1) * stride;
    for (uint32_t c = 0; c != classes; ++c) {
      _next[base + c] = next[c] < 0 ? kDead : (next[c] + 1) * stride;
    }
    // The end-of-text column is the whole of acceptance: a state accepts when
    // stepping the imaginary end byte lands on a state that recorded a match.
    const int eot = next[classes];
    _accept[s + 1] = eot >= 0 && match[eot] != 0;
    auto& range = _range[s + 1];
    auto* loop = _loop.data() + size_t{s + 1} * kMaskWords;
    for (size_t label = 0; label != _bytemap.size(); ++label) {
      const State target = _next[base + _bytemap[label]];
      if (target == kDead) {
        continue;
      }
      if (range.lo > range.hi) {
        range.lo = static_cast<uint8_t>(label);
      }
      range.hi = static_cast<uint8_t>(label);
      if (target == base) {
        loop[bitset::word(label)] |= bitset::word_t{1} << bitset::bit(label);
      }
    }
  }

  // The dictionary range every match lies in, read off the table just built: a
  // walk seeks to the low end instead of starting at the trie root, which is
  // what gives an alternation of literal prefixes the plan a single literal
  // prefix gets. Taking the smallest live label at every step spells out the
  // smallest key the language has, and stopping early only loosens the bound
  // -- a key that diverges from it does so upwards. A pattern that takes the
  // empty string reports no bound at all, which is "walk everything".
  for (State state = _start; _lower.size() != kMaxBoundLength;) {
    const auto index = state >> _stride_bits;
    if (_accept[index] != 0) {
      break;
    }
    const auto range = _range[index];
    if (range.lo > range.hi) {
      break;
    }
    _lower.push_back(range.lo);
    state = Step(state, range.lo);
  }
}

}  // namespace irs
