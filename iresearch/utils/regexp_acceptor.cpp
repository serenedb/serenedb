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
#include <memory>
#include <new>
#include <span>
#include <string>
#include <utility>
#include <vector>

#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/log.hpp"
#include "iresearch/utils/utf8_utils.hpp"
#include "iresearch/utils/wildcard_utils.hpp"
#include "re2/prog.h"
#include "re2/regexp.h"
#include "re2/walker-inl.h"

namespace irs {
namespace {

using ParseFlags = re2::Regexp::ParseFlags;

constexpr uint32_t kLastWord = 1U << 8;
constexpr uint32_t kStartContext = re2::kEmptyBeginText | re2::kEmptyBeginLine;

uint32_t BeforeByte(uint32_t context, uint8_t label) noexcept {
  uint32_t satisfied = context & kStartContext;
  if (label == '\n') {
    satisfied |= re2::kEmptyEndLine;
  }
  const bool word = re2::Prog::IsWordChar(label);
  satisfied |= word == ((context & kLastWord) != 0) ? re2::kEmptyNonWordBoundary
                                                    : re2::kEmptyWordBoundary;
  return satisfied;
}

uint32_t AfterByte(uint8_t label) noexcept {
  return (label == '\n' ? uint32_t{re2::kEmptyBeginLine} : 0U) |
         (re2::Prog::IsWordChar(label) ? kLastWord : 0U);
}

uint32_t AtEnd(uint32_t context) noexcept {
  return (context & kStartContext) | re2::kEmptyEndText | re2::kEmptyEndLine |
         ((context & kLastWord) != 0 ? re2::kEmptyWordBoundary
                                     : re2::kEmptyNonWordBoundary);
}

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

bool SameRegexp(re2::Regexp* a, re2::Regexp* b) {
  if (a == b) {
    return true;
  }
  if (a->op() != b->op() || a->parse_flags() != b->parse_flags() ||
      a->nsub() != b->nsub()) {
    return false;
  }
  switch (a->op()) {
    case re2::kRegexpLiteral:
      return a->rune() == b->rune();
    case re2::kRegexpLiteralString:
      return std::equal(a->runes(), a->runes() + a->nrunes(), b->runes(),
                        b->runes() + b->nrunes());
    case re2::kRegexpCharClass:
      return std::equal(a->cc()->begin(), a->cc()->end(), b->cc()->begin(),
                        b->cc()->end(),
                        [](const re2::RuneRange& x, const re2::RuneRange& y) {
                          return x.lo == y.lo && x.hi == y.hi;
                        });
    case re2::kRegexpRepeat:
      if (a->min() != b->min() || a->max() != b->max()) {
        return false;
      }
      break;
    case re2::kRegexpCapture:
      if (a->cap() != b->cap()) {
        return false;
      }
      break;
    case re2::kRegexpHaveMatch:
      return a->match_id() == b->match_id();
    default:
      break;
  }
  for (int i = 0; i != a->nsub(); ++i) {
    if (!SameRegexp(a->sub()[i], b->sub()[i])) {
      return false;
    }
  }
  return true;
}

void AppendPieces(re2::Regexp* re, std::vector<re2::Regexp*>& pieces) {
  if (re->op() == re2::kRegexpConcat) {
    for (int i = 0; i != re->nsub(); ++i) {
      AppendPieces(re->sub()[i], pieces);
    }
  } else if (re->op() != re2::kRegexpEmptyMatch) {
    pieces.push_back(re);
  }
}

bool ExactRunes(re2::Regexp* re, std::vector<re2::Rune>& runes) {
  switch (re->op()) {
    case re2::kRegexpLiteral:
      if ((re->parse_flags() & re2::Regexp::FoldCase) != 0) {
        return false;
      }
      runes.push_back(re->rune());
      return true;
    case re2::kRegexpLiteralString:
      if ((re->parse_flags() & re2::Regexp::FoldCase) != 0) {
        return false;
      }
      runes.insert(runes.end(), re->runes(), re->runes() + re->nrunes());
      return true;
    case re2::kRegexpCharClass:
      if (re->cc()->size() != 1 ||
          re->cc()->begin()->lo != re->cc()->begin()->hi) {
        return false;
      }
      runes.push_back(re->cc()->begin()->lo);
      return true;
    default:
      return false;
  }
}

bool EmptyWidth(re2::RegexpOp op) noexcept {
  switch (op) {
    case re2::kRegexpBeginLine:
    case re2::kRegexpEndLine:
    case re2::kRegexpBeginText:
    case re2::kRegexpEndText:
    case re2::kRegexpWordBoundary:
    case re2::kRegexpNoWordBoundary:
      return true;
    default:
      return false;
  }
}

void AppendRune(bstring& out, re2::Rune rune, bool latin1) {
  if (latin1) {
    out.push_back(static_cast<byte_type>(rune));
    return;
  }
  char utf8[re2::UTFmax];
  const int n = re2::runetochar(utf8, &rune);
  out.append(reinterpret_cast<const byte_type*>(utf8), static_cast<size_t>(n));
}

bool AppendExact(std::span<re2::Regexp* const> pieces, bstring& out) {
  std::vector<re2::Rune> runes;
  for (auto* piece : pieces) {
    runes.clear();
    if (EmptyWidth(piece->op())) {
      continue;
    }
    if (!ExactRunes(piece, runes)) {
      return false;
    }
    const bool latin1 = (piece->parse_flags() & re2::Regexp::Latin1) != 0;
    for (const auto rune : runes) {
      AppendRune(out, rune, latin1);
    }
  }
  return true;
}

bool Unbounded(std::span<re2::Regexp* const> pieces) {
  return std::any_of(pieces.begin(), pieces.end(), [](re2::Regexp* piece) {
    return piece->op() == re2::kRegexpStar || piece->op() == re2::kRegexpPlus;
  });
}

constexpr size_t kMaxSuffixes = 64;

std::vector<bstring> SuffixesOf(re2::Regexp* re) {
  std::vector<bstring> out;
  if (re->op() == re2::kRegexpAlternate) {
    for (int i = 0; i != re->nsub(); ++i) {
      auto sub = SuffixesOf(re->sub()[i]);
      if (sub.empty()) {
        return {};
      }
      out.insert(out.end(), std::make_move_iterator(sub.begin()),
                 std::make_move_iterator(sub.end()));
    }
    return out;
  }
  std::vector<re2::Regexp*> pieces;
  AppendPieces(re, pieces);
  std::vector<re2::Rune> runes;
  auto first = pieces.end();
  while (first != pieces.begin()) {
    auto* piece = *(first - 1);
    if (!EmptyWidth(piece->op()) && !ExactRunes(piece, runes)) {
      break;
    }
    --first;
  }
  bstring tail;
  AppendExact({first, pieces.end()}, tail);
  std::vector<bstring> heads;
  auto bound = first;
  if (first != pieces.begin() &&
      (*(first - 1))->op() == re2::kRegexpAlternate) {
    auto* alternate = *(first - 1);
    for (int i = 0; i != alternate->nsub(); ++i) {
      std::vector<re2::Regexp*> branch;
      AppendPieces(alternate->sub()[i], branch);
      bstring head;
      if (!AppendExact(branch, head)) {
        heads.clear();
        break;
      }
      heads.push_back(std::move(head));
    }
    if (!heads.empty()) {
      --bound;
    }
  }
  if (!Unbounded({pieces.begin(), bound})) {
    return {};
  }
  if (heads.empty()) {
    if (!tail.empty()) {
      out.push_back(std::move(tail));
    }
    return out;
  }
  for (auto& head : heads) {
    head += tail;
    if (head.empty()) {
      return {};
    }
    out.push_back(std::move(head));
  }
  return out;
}

void NormalizeSuffixes(std::vector<bstring>& suffixes) {
  std::sort(suffixes.begin(), suffixes.end(),
            [](const bstring& a, const bstring& b) {
              return a.size() != b.size() ? a.size() < b.size() : a < b;
            });
  std::vector<bstring> kept;
  for (auto& suffix : suffixes) {
    if (std::none_of(kept.begin(), kept.end(), [&](const bstring& shorter) {
          return bytes_view{suffix}.ends_with(shorter);
        })) {
      kept.push_back(std::move(suffix));
    }
  }
  if (kept.size() > kMaxSuffixes) {
    kept.clear();
  }
  std::stable_sort(
    kept.begin(), kept.end(),
    [](const bstring& a, const bstring& b) { return a.back() < b.back(); });
  suffixes = std::move(kept);
}

void NormalizeExempt(std::vector<RegexpAcceptor::ExemptKey>& keys) {
  std::sort(keys.begin(), keys.end(), [](const auto& a, const auto& b) {
    return a.key != b.key ? a.key < b.key : a.prefix > b.prefix;
  });
  std::vector<RegexpAcceptor::ExemptKey> kept;
  for (auto& key : keys) {
    if (kept.empty() ||
        !(kept.back().prefix ? bytes_view{key.key}.starts_with(kept.back().key)
                             : key.key == kept.back().key)) {
      kept.push_back(std::move(key));
    }
  }
  keys = std::move(kept);
}

bstring InfixOf(re2::Regexp* re) {
  std::vector<re2::Regexp*> pieces;
  AppendPieces(re, pieces);
  std::vector<re2::Rune> runes;
  bstring best;
  bool unbounded = false;
  for (auto it = pieces.begin(); it != pieces.end();) {
    runes.clear();
    if (!EmptyWidth((*it)->op()) && !ExactRunes(*it, runes)) {
      unbounded = unbounded || (*it)->op() == re2::kRegexpStar ||
                  (*it)->op() == re2::kRegexpPlus;
      ++it;
      continue;
    }
    bstring run;
    for (; it != pieces.end(); ++it) {
      runes.clear();
      if (EmptyWidth((*it)->op())) {
        continue;
      }
      if (!ExactRunes(*it, runes)) {
        break;
      }
      const bool latin1 = ((*it)->parse_flags() & re2::Regexp::Latin1) != 0;
      for (const auto rune : runes) {
        AppendRune(run, rune, latin1);
      }
    }
    if (unbounded && run.size() > best.size()) {
      best = std::move(run);
    }
  }
  return best;
}

constexpr size_t kMaxLiterals = 1024;
constexpr size_t kMaxLiteralDepth = 64;

bool FiniteLanguage(re2::Regexp* re, size_t depth, std::vector<bstring>& out) {
  if (depth == kMaxLiteralDepth) {
    return false;
  }
  const bool latin1 = (re->parse_flags() & re2::Regexp::Latin1) != 0;
  switch (re->op()) {
    case re2::kRegexpNoMatch:
      out.clear();
      return true;
    case re2::kRegexpEmptyMatch:
      out.assign(1, bstring{});
      return true;
    case re2::kRegexpLiteral:
    case re2::kRegexpLiteralString: {
      std::vector<re2::Rune> runes;
      if (!ExactRunes(re, runes)) {
        return false;
      }
      bstring literal;
      for (const auto rune : runes) {
        AppendRune(literal, rune, latin1);
      }
      out.assign(1, std::move(literal));
      return true;
    }
    case re2::kRegexpCharClass: {
      size_t size = 0;
      for (const auto& range : *re->cc()) {
        size += static_cast<size_t>(range.hi - range.lo) + 1;
        if (size > kMaxLiterals) {
          return false;
        }
      }
      out.clear();
      for (const auto& range : *re->cc()) {
        for (auto rune = range.lo; rune <= range.hi; ++rune) {
          AppendRune(out.emplace_back(), rune, latin1);
        }
      }
      return true;
    }
    case re2::kRegexpCapture:
      return FiniteLanguage(re->sub()[0], depth + 1, out);
    case re2::kRegexpQuest:
      if (!FiniteLanguage(re->sub()[0], depth + 1, out) ||
          out.size() == kMaxLiterals) {
        return false;
      }
      out.emplace_back();
      return true;
    case re2::kRegexpConcat: {
      out.assign(1, bstring{});
      std::vector<bstring> part;
      std::vector<bstring> next;
      for (int i = 0; i != re->nsub(); ++i) {
        if (!FiniteLanguage(re->sub()[i], depth + 1, part) ||
            (!part.empty() && out.size() > kMaxLiterals / part.size())) {
          return false;
        }
        next.clear();
        for (const auto& head : out) {
          for (const auto& tail : part) {
            auto& joined = next.emplace_back(head);
            joined += tail;
          }
        }
        out.swap(next);
      }
      return true;
    }
    case re2::kRegexpAlternate: {
      out.clear();
      std::vector<bstring> part;
      for (int i = 0; i != re->nsub(); ++i) {
        if (!FiniteLanguage(re->sub()[i], depth + 1, part) ||
            out.size() + part.size() > kMaxLiterals) {
          return false;
        }
        out.insert(out.end(), std::make_move_iterator(part.begin()),
                   std::make_move_iterator(part.end()));
      }
      return true;
    }
    default:
      return false;
  }
}

using Alt = std::vector<re2::Regexp*>;
using Pieces = std::span<re2::Regexp* const>;

re2::Regexp* ConcatOf(Pieces pieces, ParseFlags flags) {
  std::vector<re2::Regexp*> subs;
  subs.reserve(pieces.size());
  for (auto* piece : pieces) {
    subs.push_back(piece->Incref());
  }
  return re2::Regexp::Concat(subs.data(), static_cast<int>(subs.size()), flags);
}

bool LiteralLed(const Alt& alt) noexcept {
  return !alt.empty() && (alt.front()->op() == re2::kRegexpLiteral ||
                          alt.front()->op() == re2::kRegexpLiteralString);
}

int RuneCount(re2::Regexp* literal) {
  return literal->op() == re2::kRegexpLiteral ? 1 : literal->nrunes();
}

re2::Rune RuneAt(re2::Regexp* literal, int i) {
  return literal->op() == re2::kRegexpLiteral ? literal->rune()
                                              : literal->runes()[i];
}

re2::Regexp* RuneRange(re2::Regexp* literal, int from, int to) {
  const auto flags = literal->parse_flags();
  if (to - from == 1) {
    return re2::Regexp::NewLiteral(RuneAt(literal, from), flags);
  }
  std::vector<re2::Rune> runes;
  runes.reserve(static_cast<size_t>(to - from));
  for (int i = from; i != to; ++i) {
    runes.push_back(RuneAt(literal, i));
  }
  return re2::Regexp::LiteralString(runes.data(), to - from, flags);
}

bool LiteralLess(re2::Regexp* a, re2::Regexp* b) {
  if (a->parse_flags() != b->parse_flags()) {
    return a->parse_flags() < b->parse_flags();
  }
  const int na = RuneCount(a);
  const int nb = RuneCount(b);
  for (int i = 0; i != std::min(na, nb); ++i) {
    if (RuneAt(a, i) != RuneAt(b, i)) {
      return RuneAt(a, i) < RuneAt(b, i);
    }
  }
  return na < nb;
}

class Factoring {
 public:
  Factoring() = default;
  Factoring(const Factoring&) = delete;
  Factoring& operator=(const Factoring&) = delete;
  ~Factoring() {
    for (auto* re : _fresh) {
      re->Decref();
    }
  }

  re2::Regexp* Factor(std::vector<Alt> alts, ParseFlags flags, size_t depth) {
    std::vector<re2::Regexp*> out;
    out.reserve(alts.size());
    bool empty = false;
    const auto emit = [&](re2::Regexp* re) {
      if (re->op() == re2::kRegexpEmptyMatch) {
        if (empty) {
          re->Decref();
          return;
        }
        empty = true;
      }
      out.push_back(re);
    };
    if (depth == kMaxDepth) {
      for (const auto& alt : alts) {
        emit(ConcatOf(alt, flags));
      }
      return re2::Regexp::AlternateNoFactor(
        out.data(), static_cast<int>(out.size()), flags);
    }

    const auto front = [](const Alt& a, size_t n) { return a[n]; };
    const auto back = [](const Alt& a, size_t n) {
      return a[a.size() - 1 - n];
    };
    const auto run = [&](size_t i, size_t end, auto&& piece) {
      size_t j = i + 1;
      if (!alts[i].empty()) {
        while (j != end && !alts[j].empty() &&
               SameRegexp(piece(alts[i], 0), piece(alts[j], 0))) {
          ++j;
        }
      }
      size_t shared = 1;
      if (j - i > 1) {
        for (bool same = true; same; shared += same) {
          for (size_t k = i; k != j && same; ++k) {
            same = shared < alts[k].size() &&
                   SameRegexp(piece(alts[i], shared), piece(alts[k], shared));
          }
        }
      }
      return std::pair{j, shared};
    };

    if (alts.size() > 1) {
      if (const auto [end, shared] = run(0, alts.size(), front);
          end == alts.size()) {
        std::vector<Alt> rest;
        rest.reserve(alts.size());
        for (const auto& alt : alts) {
          rest.emplace_back(alt.begin() + shared, alt.end());
        }
        re2::Regexp* parts[]{ConcatOf(Pieces{alts[0]}.first(shared), flags),
                             Factor(std::move(rest), flags, depth + 1)};
        return re2::Regexp::Concat(parts, 2, flags);
      }
      if (const auto [end, shared] = run(0, alts.size(), back);
          end == alts.size()) {
        std::vector<Alt> rest;
        rest.reserve(alts.size());
        for (const auto& alt : alts) {
          rest.emplace_back(alt.begin(), alt.end() - shared);
        }
        re2::Regexp* parts[]{Factor(std::move(rest), flags, depth + 1),
                             ConcatOf(Pieces{alts[0]}.last(shared), flags)};
        return re2::Regexp::Concat(parts, 2, flags);
      }
    }

    std::stable_sort(alts.begin(), alts.end(), [](const Alt& a, const Alt& b) {
      if (LiteralLed(a) != LiteralLed(b)) {
        return LiteralLed(a);
      }
      return LiteralLed(a) && LiteralLess(a.front(), b.front());
    });

    size_t pending = 0;
    const auto flush = [&](size_t end) {
      for (size_t i = pending; i != end;) {
        const auto [j, shared] = run(i, end, back);
        if (j - i == 1) {
          emit(ConcatOf(alts[i], flags));
        } else {
          std::vector<Alt> rest;
          rest.reserve(j - i);
          for (size_t k = i; k != j; ++k) {
            rest.emplace_back(alts[k].begin(), alts[k].end() - shared);
          }
          re2::Regexp* parts[]{Factor(std::move(rest), flags, depth + 1),
                               ConcatOf(Pieces{alts[i]}.last(shared), flags)};
          emit(re2::Regexp::Concat(parts, 2, flags));
        }
        i = j;
      }
    };
    for (size_t i = 0; i != alts.size();) {
      size_t j = i + 1;
      if (LiteralLed(alts[i])) {
        auto* first = alts[i].front();
        while (j != alts.size() && LiteralLed(alts[j]) &&
               alts[j].front()->parse_flags() == first->parse_flags() &&
               RuneAt(alts[j].front(), 0) == RuneAt(first, 0)) {
          ++j;
        }
      }
      if (j - i > 1) {
        auto* first = alts[i].front();
        int common = RuneCount(first);
        for (size_t k = i + 1; k != j; ++k) {
          auto* other = alts[k].front();
          int same = 1;
          while (same < common && same < RuneCount(other) &&
                 RuneAt(other, same) == RuneAt(first, same)) {
            ++same;
          }
          common = same;
        }
        flush(i);
        std::vector<Alt> rest;
        rest.reserve(j - i);
        for (size_t k = i; k != j; ++k) {
          auto* literal = alts[k].front();
          auto& tail = rest.emplace_back();
          tail.reserve(alts[k].size());
          if (common != RuneCount(literal)) {
            tail.push_back(_fresh.emplace_back(
              RuneRange(literal, common, RuneCount(literal))));
          }
          tail.insert(tail.end(), alts[k].begin() + 1, alts[k].end());
        }
        re2::Regexp* parts[]{RuneRange(first, 0, common),
                             Factor(std::move(rest), flags, depth + 1)};
        emit(re2::Regexp::Concat(parts, 2, flags));
        i = j;
        pending = j;
        continue;
      }
      const auto [end, shared] = run(i, alts.size(), front);
      if (end - i == 1) {
        ++i;
        continue;
      }
      flush(i);
      std::vector<Alt> rest;
      rest.reserve(end - i);
      for (size_t k = i; k != end; ++k) {
        rest.emplace_back(alts[k].begin() + shared, alts[k].end());
      }
      re2::Regexp* parts[]{ConcatOf(Pieces{alts[i]}.first(shared), flags),
                           Factor(std::move(rest), flags, depth + 1)};
      emit(re2::Regexp::Concat(parts, 2, flags));
      i = end;
      pending = end;
    }
    flush(alts.size());
    return re2::Regexp::AlternateNoFactor(out.data(),
                                          static_cast<int>(out.size()), flags);
  }

 private:
  static constexpr size_t kMaxDepth = 32;

  std::vector<re2::Regexp*> _fresh;
};

re2::Regexp* FactorAlternation(re2::Regexp** subs, int nsubs,
                               ParseFlags flags) {
  std::vector<Alt> alts(static_cast<size_t>(nsubs));
  for (int i = 0; i != nsubs; ++i) {
    AppendPieces(subs[i], alts[static_cast<size_t>(i)]);
  }
  re2::Regexp* re;
  {
    Factoring factoring;
    re = factoring.Factor(std::move(alts), flags, 0);
  }
  for (int i = 0; i != nsubs; ++i) {
    subs[i]->Decref();
  }
  return re;
}

// Character classes are narrowed to the strict UTF-8 model (see
// `NarrowCharClass`); everything else is left to RE2's compiler, which is the
// whole point of going through a `Regexp` tree rather than an automaton of our
// own.
class RegexpRewriter : public re2::Regexp::Walker<re2::Regexp*> {
 public:
  bool HasError() const noexcept { return _error; }

  re2::Regexp* PostVisit(re2::Regexp* re, re2::Regexp* /*parent_arg*/,
                         re2::Regexp* /*pre_arg*/, re2::Regexp** child_args,
                         int nchild_args) override {
    const auto flags = static_cast<ParseFlags>(re->parse_flags());
    switch (re->op()) {
      case re2::kRegexpHaveMatch:
        Release(child_args, nchild_args);
        return Empty(flags);

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
        return FactorAlternation(child_args, nchild_args, flags);

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

re2::Regexp* BytesTree(bytes_view bytes, bool any_tail) {
  std::vector<re2::Rune> runes(bytes.begin(), bytes.end());
  re2::Regexp* parts[2];
  int count = 0;
  if (!runes.empty()) {
    parts[count++] = re2::Regexp::LiteralString(
      runes.data(), static_cast<int>(runes.size()), kWildcardFlags);
  }
  if (any_tail) {
    parts[count++] = re2::Regexp::Star(
      ByteClass(0x00, RegexpAcceptor::kMaxLabel), kWildcardFlags);
  }
  return re2::Regexp::Concat(parts, count, kWildcardFlags);
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

void RegexpTreeDeleter::operator()(re2::Regexp* re) const noexcept {
  re->Decref();
}

RegexpTreePtr ParseRegexpTree(bytes_view pattern, RegexpSyntax syntax) {
  return RegexpTreePtr{RegexpTree(pattern, syntax)};
}

#ifdef SDB_DEV
size_t RegexpAcceptor::Builds() noexcept {
  return kBuilds.load(std::memory_order_relaxed);
}

size_t RegexpAcceptor::Rows() const {
  std::lock_guard lock{_mutex};
  return _rows.size();
}
#endif

RegexpAcceptor::RegexpAcceptor(bytes_view pattern, RegexpSyntax syntax,
                               int64_t max_mem, size_t max_dfa_mem)
  : _max_dfa_mem{max_dfa_mem} {
  Compile(pattern, syntax, false, max_mem);
}

RegexpAcceptor::RegexpAcceptor(WildcardTag, bytes_view pattern, int64_t max_mem,
                               size_t max_dfa_mem)
  : _max_dfa_mem{max_dfa_mem} {
  Compile(pattern, RegexpSyntax::Perl, true, max_mem);
}

RegexpAcceptor::RegexpAcceptor(std::span<const Part> parts, int64_t max_mem,
                               size_t max_dfa_mem)
  : _max_dfa_mem{max_dfa_mem} {
  CompileParts(parts, max_mem);
}

RegexpAcceptor::~RegexpAcceptor() = default;

void RegexpAcceptor::Compile(bytes_view pattern, RegexpSyntax syntax,
                             bool wildcard, int64_t max_mem) {
#ifdef SDB_DEV
  kBuilds.fetch_add(1, std::memory_order_relaxed);
#endif
  re2::Regexp* re =
    wildcard ? WildcardTree(pattern) : RegexpTree(pattern, syntax);
  if (re) {
    _suffixes = SuffixesOf(re);
    NormalizeSuffixes(_suffixes);
    if (_suffixes.empty()) {
      _infix = InfixOf(re);
    }
    if (!wildcard && FiniteLanguage(re, 0, _literals)) {
      std::sort(_literals.begin(), _literals.end());
      _literals.erase(std::unique(_literals.begin(), _literals.end()),
                      _literals.end());
      _finite = true;
    } else {
      _literals.clear();
    }
    _prog.reset(re->CompileToProg(max_mem));
    re->Decref();
    if (!_prog) {
      SDB_ERROR(IRESEARCH, "RE2 regexp did not compile within ", max_mem,
                " bytes");
    }
  }
  Setup();
}

void RegexpAcceptor::CompileParts(std::span<const Part> parts,
                                  int64_t max_mem) {
#ifdef SDB_DEV
  kBuilds.fetch_add(1, std::memory_order_relaxed);
#endif
  std::vector<re2::Regexp*> text;
  std::vector<re2::Regexp*> bytes;
  bool ok = true;
  bool constrained = true;
  for (const auto& part : parts) {
    re2::Regexp* re = nullptr;
    switch (part.kind) {
      case PartKind::Term:
        re = BytesTree(part.pattern, false);
        break;
      case PartKind::Prefix:
        re = BytesTree(part.pattern, true);
        break;
      case PartKind::Wildcard:
        re = WildcardTree(part.pattern);
        break;
      case PartKind::Perl:
        re = RegexpTree(part.pattern, RegexpSyntax::Perl);
        break;
      case PartKind::PosixEre:
        re = RegexpTree(part.pattern, RegexpSyntax::PosixEre);
        break;
    }
    if (!re) {
      ok = false;
      break;
    }
    if (part.kind == PartKind::Term || part.kind == PartKind::Prefix) {
      _exempt.push_back({bstring{part.pattern}, part.kind == PartKind::Prefix});
    } else if (auto suffixes = SuffixesOf(re); suffixes.empty()) {
      constrained = false;
    } else {
      _suffixes.insert(_suffixes.end(),
                       std::make_move_iterator(suffixes.begin()),
                       std::make_move_iterator(suffixes.end()));
    }
    (part.kind == PartKind::Perl || part.kind == PartKind::PosixEre ? text
                                                                    : bytes)
      .push_back(re);
  }
  NormalizeSuffixes(_suffixes);
  if (!ok || !constrained || _suffixes.empty()) {
    _suffixes.clear();
    _exempt.clear();
  }
  NormalizeExempt(_exempt);
  if (!ok) {
    for (auto* re : text) {
      re->Decref();
    }
    for (auto* re : bytes) {
      re->Decref();
    }
    text.clear();
    bytes.clear();
  }

  const auto group = [](std::vector<re2::Regexp*>& subs) -> re2::Regexp* {
    if (subs.size() < 2) {
      return subs.empty() ? nullptr : subs.front();
    }
    const auto flags = static_cast<ParseFlags>(subs.front()->parse_flags());
    return FactorAlternation(subs.data(), static_cast<int>(subs.size()), flags);
  };
  re2::Regexp* trees[]{group(text), group(bytes)};
  auto* only = trees[0] == nullptr   ? trees[1]
               : trees[1] == nullptr ? trees[0]
                                     : nullptr;
  if (only && _suffixes.empty()) {
    _infix = InfixOf(only);
  }
  bool finite = trees[0] != nullptr || trees[1] != nullptr;
  for (auto* tree : trees) {
    std::vector<bstring> words;
    if (tree && finite) {
      finite = FiniteLanguage(tree, 0, words);
      _literals.insert(_literals.end(), std::make_move_iterator(words.begin()),
                       std::make_move_iterator(words.end()));
    }
  }
  if (finite && _literals.size() <= kMaxLiterals) {
    std::sort(_literals.begin(), _literals.end());
    _literals.erase(std::unique(_literals.begin(), _literals.end()),
                    _literals.end());
    _finite = true;
  } else {
    _literals.clear();
  }

  std::unique_ptr<re2::Prog> progs[2];
  bool compiled = true;
  for (size_t i = 0; i != 2; ++i) {
    if (trees[i]) {
      progs[i].reset(trees[i]->CompileToProg(max_mem));
      trees[i]->Decref();
      compiled = compiled && progs[i] != nullptr;
    }
  }
  if (!compiled) {
    SDB_ERROR(IRESEARCH, "RE2 regexp did not compile within ", max_mem,
              " bytes");
  } else if (progs[0] && progs[1]) {
    _prog = std::move(progs[0]);
    _bytes_prog = std::move(progs[1]);
  } else {
    _prog = std::move(progs[0] ? progs[0] : progs[1]);
  }
  Setup();
}

void RegexpAcceptor::Setup() {
  _split = _prog ? _prog->size() : 0;
  {
    std::lock_guard lock{_mutex};
    if (_prog && _bytes_prog) {
      _prog->set_anchor_end(true);
      _bytes_prog->set_anchor_end(true);
      const auto* text = _prog->bytemap();
      const auto* bytes = _bytes_prog->bytemap();
      _classes = 0;
      for (uint32_t label = 0; label <= kMaxLabel; ++label) {
        uint32_t c = 0;
        for (; c != _classes; ++c) {
          const auto seen = _representative[c];
          if (text[seen] == text[label] && bytes[seen] == bytes[label]) {
            break;
          }
        }
        if (c == _classes) {
          _representative[_classes++] = static_cast<uint8_t>(label);
        }
        _bytemap[label] = static_cast<uint8_t>(c);
      }
    } else if (_prog) {
      _prog->set_anchor_end(true);
      _classes = static_cast<uint32_t>(_prog->bytemap_range());
      SDB_ASSERT(_classes != 0);
      std::copy(_prog->bytemap(), _prog->bytemap() + _bytemap.size(),
                _bytemap.begin());
      for (int label = kMaxLabel; label >= 0; --label) {
        _representative[_bytemap[label]] = static_cast<uint8_t>(label);
      }
    } else {
      _classes = 1;
    }

    _dead = AllocateLocked(0);
    _dead->dead = true;
    _unknown = AllocateLocked(0);
    _unknown->unknown = true;
    _unknown->lo = 0;
    _unknown->hi = kMaxLabel;
    _unknown->loop.fill(~bitset::word_t{0});
    for (uint32_t c = 0; c != _classes; ++c) {
      _dead->Next()[c].store(_dead, std::memory_order_relaxed);
      _unknown->Next()[c].store(_unknown, std::memory_order_relaxed);
    }
    _dead->built.store(true, std::memory_order_relaxed);
    _unknown->built.store(true, std::memory_order_relaxed);

    if (!_prog) {
      _start = _dead;
      return;
    }

    _index.assign(static_cast<size_t>(Size()), 0);
    _resolved_index.assign(static_cast<size_t>(Size()), 0);
    _queue.clear();
    AddStarts(_queue, _index, _stack);
    _start = InternLocked(_queue, kStartContext);

    std::vector<Row*> pending;
    if (!_start->dead && !_start->unknown) {
      pending.push_back(const_cast<Row*>(_start));
    }
    for (size_t built = 0; !pending.empty() && built != kFloodRows;) {
      Row* row = pending.back();
      pending.pop_back();
      if (row->built.load(std::memory_order_relaxed)) {
        continue;
      }
      BuildLocked(row);
      ++built;
      for (uint32_t c = 0; c != _classes; ++c) {
        auto* target =
          const_cast<Row*>(row->Next()[c].load(std::memory_order_relaxed));
        if (!target->built.load(std::memory_order_relaxed)) {
          pending.push_back(target);
        }
      }
    }
  }

  for (State state = _start; _lower.size() != kMaxBoundLength;) {
    if (state->accept || state->unknown) {
      break;
    }
    uint32_t lo;
    uint32_t hi;
    if (!LiveRange(state, lo, hi)) {
      break;
    }
    _lower.push_back(static_cast<byte_type>(lo));
    state = Step(state, static_cast<byte_type>(lo));
  }
}

RegexpAcceptor::State RegexpAcceptor::StepSlow(State from, uint8_t c) const {
  Build(const_cast<Row*>(from));
  return from->Next()[c].load(std::memory_order_acquire);
}

void RegexpAcceptor::Build(Row* row) const {
  std::lock_guard lock{_mutex};
  BuildLocked(row);
}

void RegexpAcceptor::BuildLocked(Row* row) const {
  if (row->built.load(std::memory_order_relaxed)) {
    return;
  }
  SDB_ASSERT(_prog);
  auto* next = row->Next();
  for (uint32_t c = 0; c != _classes; ++c) {
    const uint8_t label = _representative[c];
    const int* set = row->set;
    size_t size = row->set_size;
    if (row->assertions) {
      Resolve(row->set, row->set_size, BeforeByte(row->context, label),
              _resolved, _resolved_index, _stack);
      set = _resolved.data();
      size = _resolved.size();
    }
    _queue.clear();
    for (size_t i = 0; i != size; ++i) {
      auto* ip = Inst(set[i]);
      if (ip->opcode() == re2::kInstByteRange && ip->Matches(label)) {
        AddToQueue(_queue, _index, _stack, Out(set[i], ip), 0);
      }
    }
    next[c].store(InternLocked(_queue, AfterByte(label)),
                  std::memory_order_release);
  }
  uint8_t lo = 1;
  uint8_t hi = 0;
  for (size_t label = 0; label <= kMaxLabel; ++label) {
    const auto* target = next[_bytemap[label]].load(std::memory_order_relaxed);
    if (target->dead) {
      continue;
    }
    if (lo > hi) {
      lo = static_cast<uint8_t>(label);
    }
    hi = static_cast<uint8_t>(label);
    if (target == row) {
      row->loop[bitset::word(label)] |= bitset::word_t{1} << bitset::bit(label);
    }
  }
  row->lo = lo;
  row->hi = hi;
  row->built.store(true, std::memory_order_release);
}

RegexpAcceptor::State RegexpAcceptor::InternLocked(
  const std::vector<int>& queue, uint32_t context) const {
  _canon.clear();
  bool match = false;
  uint32_t pending = 0;
  for (const int id : queue) {
    auto* ip = Inst(id);
    switch (ip->opcode()) {
      case re2::kInstByteRange:
        _canon.push_back(id);
        break;
      case re2::kInstMatch:
        _canon.push_back(id);
        match = true;
        break;
      case re2::kInstEmptyWidth:
        _canon.push_back(id);
        pending |= ip->empty();
        break;
      default:
        break;
    }
  }
  if (_canon.empty()) {
    return _dead;
  }
  std::sort(_canon.begin(), _canon.end());
  const size_t size = _canon.size();
  if (pending == 0) {
    context = 0;
  }
  _canon.push_back(static_cast<int>(context));
  const std::string_view key{reinterpret_cast<const char*>(_canon.data()),
                             _canon.size() * sizeof(int)};
  if (const auto it = _rows.find(key); it != _rows.end()) {
    return it->second;
  }
  if (_dfa_mem + RowBytes(size) > _max_dfa_mem) {
    return _unknown;
  }
  bool accept = match;
  if (pending != 0) {
    Resolve(_canon.data(), static_cast<uint32_t>(size), AtEnd(context), _queue,
            _index, _stack);
    accept = std::any_of(_queue.begin(), _queue.end(), [&](int id) {
      return Inst(id)->opcode() == re2::kInstMatch;
    });
  }
  Row* row = AllocateLocked(size);
  auto* set = const_cast<int*>(row->set);
  std::copy(_canon.begin(), _canon.end(), set);
  row->set_size = static_cast<uint32_t>(size);
  row->context = context;
  row->assertions = pending != 0;
  row->accept = accept;
  _rows.emplace(std::string_view{reinterpret_cast<const char*>(set),
                                 _canon.size() * sizeof(int)},
                row);
  return row;
}

void RegexpAcceptor::Resolve(const int* set, uint32_t size, uint32_t satisfied,
                             std::vector<int>& queue,
                             std::vector<uint32_t>& index,
                             std::vector<int>& stack) const {
  queue.clear();
  for (uint32_t i = 0; i != size; ++i) {
    AddToQueue(queue, index, stack, set[i], satisfied);
  }
}

size_t RegexpAcceptor::RowBytes(size_t set_size) const noexcept {
  const size_t bytes = sizeof(Row) +
                       size_t{_classes} * sizeof(std::atomic<const Row*>) +
                       (set_size + 1) * sizeof(int);
  return (bytes + alignof(Row) - 1) & ~(alignof(Row) - 1);
}

RegexpAcceptor::Row* RegexpAcceptor::AllocateLocked(size_t set_size) const {
  const size_t bytes = RowBytes(set_size);
  if (_chunk_used + bytes > _chunk_size) {
    _chunk_size = std::max(kChunkBytes, bytes);
    _chunks.emplace_back(std::make_unique<std::byte[]>(_chunk_size));
    _chunk_used = 0;
    _arena_bytes += _chunk_size;
  }
  auto* memory = _chunks.back().get() + _chunk_used;
  _chunk_used += bytes;
  _dfa_mem += bytes;
  auto* row = new (memory) Row{};
  auto* next = row->Next();
  for (uint32_t c = 0; c != _classes; ++c) {
    new (next + c) std::atomic<const Row*>{nullptr};
  }
  row->set = reinterpret_cast<const int*>(next + _classes);
  return row;
}

void RegexpAcceptor::AddToQueue(std::vector<int>& queue,
                                std::vector<uint32_t>& index,
                                std::vector<int>& stack, int id,
                                uint32_t satisfied) const {
  stack.clear();
  stack.push_back(id);
  while (!stack.empty()) {
    int current = stack.back();
    stack.pop_back();
    while (!Fail(current)) {
      const auto slot = index[current];
      if (slot < queue.size() && queue[slot] == current) {
        break;
      }
      index[current] = static_cast<uint32_t>(queue.size());
      queue.push_back(current);
      auto* ip = Inst(current);
      switch (ip->opcode()) {
        case re2::kInstCapture:
        case re2::kInstNop:
          if (!ip->last()) {
            stack.push_back(current + 1);
          }
          current = Out(current, ip);
          break;
        case re2::kInstAltMatch:
          current = current + 1;
          break;
        case re2::kInstEmptyWidth:
          if (!ip->last()) {
            stack.push_back(current + 1);
          }
          current = (ip->empty() & ~satisfied) != 0 ? 0 : Out(current, ip);
          break;
        default:
          current = ip->last() ? 0 : current + 1;
          break;
      }
    }
  }
}

size_t RegexpAcceptor::MemoryUsage() const {
  std::lock_guard lock{_mutex};
  size_t bytes =
    sizeof(*this) + _lower.capacity() + _arena_bytes +
    _rows.capacity() * (sizeof(std::string_view) + sizeof(Row*)) +
    _index.capacity() * sizeof(uint32_t) +
    (_queue.capacity() + _stack.capacity() + _canon.capacity()) * sizeof(int);
  bytes += static_cast<size_t>(Size()) * sizeof(re2::Prog::Inst);
  return bytes;
}

void RegexpAcceptor::AddStarts(std::vector<int>& queue,
                               std::vector<uint32_t>& index,
                               std::vector<int>& stack) const {
  AddToQueue(queue, index, stack, _prog->start(), 0);
  if (_bytes_prog) {
    AddToQueue(queue, index, stack, _split + _bytes_prog->start(), 0);
  }
}

bool RegexpAcceptor::Matches(bytes_view term) const {
  if (_start->unknown) {
    return Simulate(nullptr, term);
  }
  State state = _start;
  for (size_t i = 0; i != term.size(); ++i) {
    const State next = Step(state, term[i]);
    if (next->unknown) [[unlikely]] {
      return Simulate(state, term.substr(i));
    }
    if (next->dead) {
      return false;
    }
    state = next;
  }
  return state->accept;
}

bool RegexpAcceptor::Simulate(State from, bytes_view rest) const {
  struct Scratch {
    std::vector<int> current;
    std::vector<int> next;
    std::vector<int> resolved;
    std::vector<int> stack;
    std::vector<uint32_t> current_index;
    std::vector<uint32_t> next_index;
    std::vector<uint32_t> resolved_index;
  };
  thread_local Scratch scratch;
  const auto size = static_cast<size_t>(Size());
  if (scratch.current_index.size() < size) {
    scratch.current_index.resize(size);
    scratch.next_index.resize(size);
    scratch.resolved_index.resize(size);
  }
  auto& current = scratch.current;
  auto& next = scratch.next;
  auto& resolved = scratch.resolved;
  auto& current_index = scratch.current_index;
  auto& next_index = scratch.next_index;
  current.clear();
  uint32_t context = kStartContext;
  if (from == nullptr) {
    AddStarts(current, current_index, scratch.stack);
  } else {
    current.assign(from->set, from->set + from->set_size);
    context = from->context;
  }
  for (const auto label : rest) {
    Resolve(current.data(), static_cast<uint32_t>(current.size()),
            BeforeByte(context, label), resolved, scratch.resolved_index,
            scratch.stack);
    next.clear();
    for (const int id : resolved) {
      auto* ip = Inst(id);
      if (ip->opcode() == re2::kInstByteRange && ip->Matches(label)) {
        AddToQueue(next, next_index, scratch.stack, Out(id, ip), 0);
      }
    }
    if (next.empty()) {
      return false;
    }
    std::swap(current, next);
    std::swap(current_index, next_index);
    context = AfterByte(label);
  }
  Resolve(current.data(), static_cast<uint32_t>(current.size()), AtEnd(context),
          resolved, scratch.resolved_index, scratch.stack);
  return std::any_of(resolved.begin(), resolved.end(), [&](int id) {
    return Inst(id)->opcode() == re2::kInstMatch;
  });
}

}  // namespace irs
