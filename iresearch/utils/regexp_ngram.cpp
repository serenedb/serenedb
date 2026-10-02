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

#include "iresearch/utils/regexp_ngram.hpp"

#include <absl/algorithm/container.h>
#include <absl/strings/ascii.h>
#include <absl/strings/str_cat.h>

#include <algorithm>
#include <limits>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <utility>
#include <vector>

#include "iresearch/utils/assert.hpp"
#include "re2/regexp.h"

namespace irs {
namespace {

using Kind = GramQuery::Kind;
using Runes = std::u32string;
using RuneSet = std::vector<Runes>;

constexpr size_t kMaxDepth = 256;
constexpr size_t kMaxVisits = 100000;

struct TreeDeleter {
  void operator()(re2::Regexp* re) const noexcept { re->Decref(); }
};

using Tree = std::unique_ptr<re2::Regexp, TreeDeleter>;

Tree ParseTree(bytes_view pattern, RegexpSyntax syntax) {
  const absl::string_view sv{reinterpret_cast<const char*>(pattern.data()),
                             pattern.size()};
  const auto flags =
    static_cast<re2::Regexp::ParseFlags>(RegexpOptions(syntax).ParseFlags());
  re2::RegexpStatus status;
  const Tree parsed{re2::Regexp::Parse(sv, flags, &status)};
  if (!parsed) {
    return {};
  }
  return Tree{parsed->Simplify()};
}

// What is known about the strings a subexpression matches: either the exact
// set, or sets their prefixes and suffixes are drawn from, plus a condition
// every match satisfies.
struct Info {
  bool can_empty{false};
  bool has_exact{false};
  RuneSet exact;
  RuneSet prefix;
  RuneSet suffix;
  GramQuery match;
};

Info AnyMatch() {
  return {.can_empty = true, .prefix = {Runes{}}, .suffix = {Runes{}}};
}

Info AnyChar() { return {.prefix = {Runes{}}, .suffix = {Runes{}}}; }

Info NoMatch() { return {.match = {.kind = Kind::None}}; }

Info EmptyString() {
  return {.can_empty = true, .has_exact = true, .exact = {Runes{}}};
}

GramQuery Combine(Kind op, GramQuery a, GramQuery b) {
  SDB_ASSERT(op == Kind::And || op == Kind::Or);
  const auto absorbing = op == Kind::And ? Kind::None : Kind::All;
  const auto neutral = op == Kind::And ? Kind::All : Kind::None;
  if (a.kind == absorbing || b.kind == absorbing) {
    return {.kind = absorbing};
  }
  if (a.kind == neutral) {
    return b;
  }
  if (b.kind == neutral) {
    return a;
  }
  GramQuery out{.kind = op};
  const auto add = [&](GramQuery&& q) {
    if (absl::c_find(out.children, q) == out.children.end()) {
      out.children.push_back(std::move(q));
    }
  };
  for (auto* q : {&a, &b}) {
    if (q->kind != op) {
      add(std::move(*q));
      continue;
    }
    for (auto& child : q->children) {
      add(std::move(child));
    }
  }
  if (out.children.size() == 1) {
    return std::move(out.children.front());
  }
  return out;
}

size_t MinLen(const RuneSet& set) noexcept {
  size_t len = set.empty() ? 0 : set.front().size();
  for (const auto& s : set) {
    len = std::min(len, s.size());
  }
  return len;
}

size_t MaxLen(const RuneSet& set) noexcept {
  size_t len = 0;
  for (const auto& s : set) {
    len = std::max(len, s.size());
  }
  return len;
}

void Clean(RuneSet& set, bool suffix) {
  if (suffix) {
    absl::c_sort(set, [](const Runes& a, const Runes& b) {
      return std::lexicographical_compare(a.rbegin(), a.rend(), b.rbegin(),
                                          b.rend());
    });
  } else {
    absl::c_sort(set);
  }
  set.erase(std::unique(set.begin(), set.end()), set.end());
}

RuneSet Cross(const RuneSet& x, const RuneSet& y, bool suffix) {
  RuneSet out;
  out.reserve(x.size() * y.size());
  for (const auto& a : x) {
    for (const auto& b : y) {
      out.push_back(a + b);
    }
  }
  Clean(out, suffix);
  return out;
}

RuneSet Union(RuneSet x, const RuneSet& y, bool suffix) {
  x.insert(x.end(), y.begin(), y.end());
  Clean(x, suffix);
  return x;
}

bool IsSurrogate(re2::Rune rune) noexcept {
  return rune >= 0xD800 && rune <= 0xDFFF;
}

// RE2 keeps `FoldCase` on a literal only for ASCII letters and for runes with
// no other case; past ASCII we cannot tell those apart, so assume a fold.
bool MayFold(re2::Rune rune) noexcept {
  return rune >= 0x80 || absl::ascii_isalpha(static_cast<unsigned char>(rune));
}

bstring Utf8(const Runes& runes) {
  bstring out;
  for (const auto r : runes) {
    char utf8[re2::UTFmax];
    const auto rune = static_cast<re2::Rune>(r);
    const int n = re2::runetochar(utf8, &rune);
    out.append(reinterpret_cast<const byte_type*>(utf8),
               static_cast<size_t>(n));
  }
  return out;
}

size_t RuneLength(bytes_view utf8) noexcept {
  return static_cast<size_t>(
    absl::c_count_if(utf8, [](byte_type b) { return (b & 0xC0) != 0x80; }));
}

size_t Shortest(const GramQuery& query) noexcept {
  if (query.kind == Kind::Literal) {
    return RuneLength(query.literal);
  }
  if (query.children.empty()) {
    return 0;
  }
  auto len = std::numeric_limits<size_t>::max();
  for (const auto& child : query.children) {
    len = std::min(len, Shortest(child));
  }
  return len;
}

// Only an AND may lose children: that weakens the condition. An OR that does
// not fit becomes ALL.
GramQuery Prune(GramQuery query, size_t budget) {
  if (LeafCount(query) <= budget) {
    return query;
  }
  if (query.kind != Kind::And) {
    return {};
  }
  auto& children = query.children;
  std::vector<size_t> shortest;
  shortest.reserve(children.size());
  for (const auto& child : children) {
    shortest.push_back(Shortest(child));
  }
  std::vector<size_t> order(children.size());
  absl::c_iota(order, size_t{0});
  absl::c_stable_sort(
    order, [&](size_t a, size_t b) { return shortest[a] > shortest[b]; });
  for (const auto i : order) {
    children[i] = Prune(std::move(children[i]), budget);
    budget -= LeafCount(children[i]);
  }
  GramQuery out;
  for (auto& child : children) {
    out = Combine(Kind::And, std::move(out), std::move(child));
  }
  return out;
}

void Append(std::string& out, const GramQuery& query) {
  switch (query.kind) {
    case Kind::All:
      out += "ALL";
      return;
    case Kind::None:
      out += "NONE";
      return;
    case Kind::Literal:
      out += '"';
      for (const auto c : query.literal) {
        if (c == '"' || c == '\\') {
          out += '\\';
          out += static_cast<char>(c);
        } else if (absl::ascii_isprint(c)) {
          out += static_cast<char>(c);
        } else {
          absl::StrAppend(&out, "\\x", absl::Hex(uint32_t{c}, absl::kZeroPad2));
        }
      }
      out += '"';
      return;
    case Kind::And:
    case Kind::Or:
      out += query.kind == Kind::And ? "And(" : "Or(";
      for (size_t i = 0; i != query.children.size(); ++i) {
        if (i != 0) {
          out += ", ";
        }
        Append(out, query.children[i]);
      }
      out += ')';
      return;
  }
}

// Cox's analysis ("Regular Expression Matching with a Trigram Index",
// google/codesearch index/regexp.go) over code points rather than bytes, with
// whole strings as leaves and both ends of the term spelled by the boundary.
class Extractor {
 public:
  Extractor(size_t gram_size, byte_type boundary, const GramQueryLimits& limits)
    : _n{gram_size}, _boundary{boundary}, _limits{limits} {}

  GramQuery Extract(re2::Regexp* root) {
    std::vector<Info> pieces;
    pieces.push_back(Edge());
    Pieces(root, 0, pieces);
    pieces.push_back(Edge());
    auto info = ConcatAll(pieces);
    if (_poisoned) {
      return {};
    }
    AddExact(info);
    return Prune(std::move(info.match), _limits.max_leaves);
  }

 private:
  Info Edge() const {
    return {.has_exact = true, .exact = {Runes(1, _boundary)}};
  }

  Info Exact(RuneSet set) {
    Info info{.has_exact = true, .exact = std::move(set)};
    Simplify(info);
    return info;
  }

  Info Whole(re2::Regexp* re, size_t depth) {
    std::vector<Info> pieces;
    Pieces(re, depth, pieces);
    return ConcatAll(pieces);
  }

  // Nested concatenations and literal runs are flattened into one list of
  // pieces, so that `ConcatAll` sees every exact neighbour.
  void Pieces(re2::Regexp* re, size_t depth, std::vector<Info>& pieces) {
    if (depth == kMaxDepth || ++_visits > kMaxVisits ||
        (re->parse_flags() & re2::Regexp::Latin1) != 0) {
      _poisoned = true;
    }
    if (_poisoned) {
      pieces.push_back(AnyMatch());
      return;
    }
    const bool fold = (re->parse_flags() & re2::Regexp::FoldCase) != 0;
    switch (re->op()) {
      case re2::kRegexpConcat:
        for (int i = 0; i != re->nsub(); ++i) {
          Pieces(re->sub()[i], depth + 1, pieces);
        }
        return;
      case re2::kRegexpLiteral: {
        const auto rune = re->rune();
        AppendLiteral({&rune, 1}, fold, pieces);
        return;
      }
      case re2::kRegexpLiteralString:
        AppendLiteral({re->runes(), static_cast<size_t>(re->nrunes())}, fold,
                      pieces);
        return;
      default:
        pieces.push_back(Analyze(re, depth));
        return;
    }
  }

  Info Analyze(re2::Regexp* re, size_t depth) {
    switch (re->op()) {
      case re2::kRegexpNoMatch:
        return NoMatch();
      // Assertions narrow the match without consuming a rune.
      case re2::kRegexpEmptyMatch:
      case re2::kRegexpBeginLine:
      case re2::kRegexpEndLine:
      case re2::kRegexpBeginText:
      case re2::kRegexpEndText:
      case re2::kRegexpWordBoundary:
      case re2::kRegexpNoWordBoundary:
        return EmptyString();
      case re2::kRegexpCharClass:
        return Class(*re->cc());
      case re2::kRegexpAnyChar:
        return AnyChar();
      // `\C` can stop inside a code point and shift the gram boundaries of
      // everything after it, so no gram of the pattern is required.
      case re2::kRegexpAnyByte:
        _poisoned = true;
        return AnyChar();
      case re2::kRegexpAlternate: {
        auto info = Whole(re->sub()[0], depth + 1);
        for (int i = 1; i != re->nsub(); ++i) {
          info = Alternate(std::move(info), Whole(re->sub()[i], depth + 1));
        }
        return info;
      }
      case re2::kRegexpQuest:
        return Alternate(Whole(re->sub()[0], depth + 1), EmptyString());
      case re2::kRegexpPlus: {
        auto info = Whole(re->sub()[0], depth + 1);
        if (info.has_exact) {
          info.prefix = info.exact;
          info.suffix = std::move(info.exact);
          info.exact.clear();
          info.has_exact = false;
        }
        Simplify(info);
        return info;
      }
      case re2::kRegexpCapture:
        return Whole(re->sub()[0], depth + 1);
      default:
        for (int i = 0; i != re->nsub(); ++i) {
          Whole(re->sub()[i], depth + 1);
        }
        return AnyMatch();
    }
  }

  void AppendLiteral(std::span<const re2::Rune> runes, bool fold,
                     std::vector<Info>& pieces) {
    Runes run;
    const auto flush = [&] {
      if (!run.empty()) {
        pieces.push_back(Exact({std::move(run)}));
        run.clear();
      }
    };
    for (const auto rune : runes) {
      _poisoned = _poisoned || IsSurrogate(rune);
      if (fold && MayFold(rune)) {
        flush();
        pieces.push_back(AnyChar());
      } else {
        run.push_back(static_cast<char32_t>(rune));
      }
    }
    flush();
  }

  Info Class(re2::CharClass& cc) {
    size_t size = 0;
    for (const auto& range : cc) {
      size += static_cast<size_t>(range.hi - range.lo) + 1;
    }
    if (size == 0) {
      return NoMatch();
    }
    if (size > _limits.max_class) {
      return AnyChar();
    }
    RuneSet set;
    set.reserve(size);
    for (const auto& range : cc) {
      for (auto rune = range.lo; rune <= range.hi; ++rune) {
        _poisoned = _poisoned || IsSurrogate(rune);
        set.emplace_back(1, static_cast<char32_t>(rune));
      }
    }
    return Exact(std::move(set));
  }

  // Exact neighbours are joined before the left fold: folding `.*` + `abc` +
  // edge would cut the suffix set to `bc` before the edge arrives.
  Info ConcatAll(std::vector<Info>& pieces) {
    std::vector<Info> groups;
    groups.reserve(pieces.size());
    for (auto& piece : pieces) {
      if (!groups.empty() && Joins(groups.back(), piece)) {
        groups.back() = Concat(std::move(groups.back()), std::move(piece));
      } else {
        groups.push_back(std::move(piece));
      }
    }
    if (groups.empty()) {
      return EmptyString();
    }
    auto info = std::move(groups.front());
    for (size_t i = 1; i != groups.size(); ++i) {
      info = Concat(std::move(info), std::move(groups[i]));
    }
    return info;
  }

  bool Joins(const Info& x, const Info& y) const noexcept {
    return x.has_exact && y.has_exact &&
           x.exact.size() * y.exact.size() <= _limits.max_exact &&
           MaxLen(x.exact) + MaxLen(y.exact) <= _limits.max_exact_runes;
  }

  Info Concat(Info x, Info y) {
    Info xy{
      .can_empty = x.can_empty && y.can_empty,
      .match = Combine(Kind::And, std::move(x.match), std::move(y.match)),
    };
    if (x.has_exact && y.has_exact) {
      xy.has_exact = true;
      xy.exact = Cross(x.exact, y.exact, false);
      Simplify(xy);
      return xy;
    }
    if (x.has_exact) {
      xy.prefix = Cross(x.exact, y.prefix, false);
    } else if (x.can_empty) {
      xy.prefix =
        Union(std::move(x.prefix), y.has_exact ? y.exact : y.prefix, false);
    } else {
      xy.prefix = std::move(x.prefix);
    }
    if (y.has_exact) {
      xy.suffix = Cross(x.suffix, y.exact, true);
    } else if (y.can_empty) {
      xy.suffix =
        Union(std::move(y.suffix), x.has_exact ? x.exact : x.suffix, true);
    } else {
      xy.suffix = std::move(y.suffix);
    }
    // A match crosses the seam: one of these strings is in it.
    if (!x.has_exact && !y.has_exact && x.suffix.size() <= _limits.max_set &&
        y.prefix.size() <= _limits.max_set &&
        MinLen(x.suffix) + MinLen(y.prefix) >= _n) {
      AndLiterals(xy.match, Cross(x.suffix, y.prefix, false));
    }
    Simplify(xy);
    return xy;
  }

  Info Alternate(Info x, Info y) {
    Info xy{.can_empty = x.can_empty || y.can_empty};
    if (x.has_exact && y.has_exact) {
      xy.has_exact = true;
      xy.exact = Union(std::move(x.exact), y.exact, false);
    } else if (x.has_exact) {
      xy.prefix = Union(x.exact, y.prefix, false);
      xy.suffix = Union(x.exact, y.suffix, true);
      AddExact(x);
    } else if (y.has_exact) {
      xy.prefix = Union(std::move(x.prefix), y.exact, false);
      xy.suffix = Union(std::move(x.suffix), y.exact, true);
      AddExact(y);
    } else {
      xy.prefix = Union(std::move(x.prefix), y.prefix, false);
      xy.suffix = Union(std::move(x.suffix), y.suffix, true);
    }
    xy.match = Combine(Kind::Or, std::move(x.match), std::move(y.match));
    Simplify(xy);
    return xy;
  }

  // Unlike Cox, an exact set is not given up for being long enough to hold a
  // gram: `abcdef.*` keeps the whole prefix rather than its first gram.
  void Simplify(Info& info) {
    if (info.has_exact) {
      Clean(info.exact, false);
      if (info.exact.size() <= _limits.max_exact &&
          MaxLen(info.exact) <= _limits.max_exact_runes) {
        return;
      }
      AddExact(info);
      for (const auto& s : info.exact) {
        const auto len = std::min(s.size(), _n - 1);
        info.prefix.push_back(s.substr(0, len));
        info.suffix.push_back(s.substr(s.size() - len));
      }
      info.exact.clear();
      info.has_exact = false;
    }
    SimplifySet(info, false);
    SimplifySet(info, true);
  }

  void SimplifySet(Info& info, bool suffix) {
    auto& set = suffix ? info.suffix : info.prefix;
    Clean(set, suffix);
    AndLiterals(info.match, set);
    for (size_t len = _n - 1;; --len) {
      for (auto& s : set) {
        if (s.size() > len) {
          s = suffix ? s.substr(s.size() - len) : s.substr(0, len);
        }
      }
      Clean(set, suffix);
      if (set.size() <= _limits.max_set || len == 0) {
        break;
      }
    }
    RuneSet kept;
    for (auto& s : set) {
      if (kept.empty() ||
          !(suffix ? s.ends_with(kept.back()) : s.starts_with(kept.back()))) {
        kept.push_back(std::move(s));
      }
    }
    set = std::move(kept);
  }

  void AndLiterals(GramQuery& match, const RuneSet& set) const {
    if (set.empty() || MinLen(set) < _n) {
      return;
    }
    GramQuery any{.kind = Kind::None};
    for (const auto& s : set) {
      any = Combine(Kind::Or, std::move(any),
                    GramQuery{.kind = Kind::Literal, .literal = Utf8(s)});
    }
    match = Combine(Kind::And, std::move(match), std::move(any));
  }

  void AddExact(Info& info) const {
    if (info.has_exact) {
      AndLiterals(info.match, info.exact);
    }
  }

  size_t _n;
  char32_t _boundary;
  GramQueryLimits _limits;
  size_t _visits{0};
  bool _poisoned{false};
};

bool AnyButNewline(re2::Regexp* re) {
  return re->op() == re2::kRegexpCharClass &&
         re->cc()->size() == re2::Runemax && !re->cc()->Contains('\n');
}

class LikeShape {
 public:
  LikeShape(size_t gram_size, byte_type boundary)
    : _n{gram_size}, _boundary{boundary} {}

  std::optional<GramPlan> Extract(re2::Regexp* root,
                                  const GramQueryLimits& limits) {
    if (!Collect(root, 0)) {
      return std::nullopt;
    }
    std::vector<bstring> parts;
    bstring best;
    const auto flush = [&](bytes_view piece) {
      if (RuneLength(piece) >= _n) {
        parts.emplace_back(piece);
      } else if (best.size() <= piece.size()) {
        best = piece;
      }
    };
    bool exact = true;
    bstring piece(1, _boundary);
    for (size_t i = 0; i != _tokens.size(); ++i) {
      const auto& token = _tokens[i];
      if (token.unit == Unit::Literal) {
        piece += token.bytes;
        continue;
      }
      exact = exact && token.unit == Unit::Any && token.dot_nl &&
              (i == 0 || i + 1 == _tokens.size());
      flush(piece);
      piece.clear();
    }
    if (!piece.empty()) {
      piece += _boundary;
      flush(piece);
    }
    GramPlan plan{.exact = exact};
    if (!parts.empty()) {
      for (auto& part : parts) {
        plan.query =
          Combine(Kind::And, std::move(plan.query),
                  GramQuery{.kind = Kind::Literal, .literal = std::move(part)});
      }
      plan.query = Prune(std::move(plan.query), limits.max_leaves);
    } else if (exact || best != bytes_view{&_boundary, 1}) {
      SDB_ASSERT(!best.empty());
      plan.query = {.kind = Kind::Literal, .literal = std::move(best)};
    }
    return plan;
  }

 private:
  enum class Unit : uint8_t {
    Literal,
    One,
    Any,
  };

  struct Token {
    Unit unit;
    bool dot_nl{false};
    bstring bytes;
  };

  bool Collect(re2::Regexp* re, size_t depth) {
    if (depth == kMaxDepth || (re->parse_flags() & re2::Regexp::Latin1) != 0) {
      return false;
    }
    switch (re->op()) {
      case re2::kRegexpConcat:
        for (int i = 0; i != re->nsub(); ++i) {
          if (!Collect(re->sub()[i], depth + 1)) {
            return false;
          }
        }
        return true;
      case re2::kRegexpCapture:
        return Collect(re->sub()[0], depth + 1);
      case re2::kRegexpEmptyMatch:
        return true;
      case re2::kRegexpBeginText:
        return _tokens.empty();
      case re2::kRegexpEndText:
        _end = true;
        return true;
      case re2::kRegexpLiteral: {
        const auto rune = re->rune();
        return Literal(re, {&rune, 1});
      }
      case re2::kRegexpLiteralString:
        return Literal(re, {re->runes(), static_cast<size_t>(re->nrunes())});
      case re2::kRegexpAnyChar:
        return Wildcard(Unit::One, true);
      case re2::kRegexpCharClass:
        return AnyButNewline(re) && Wildcard(Unit::One, false);
      case re2::kRegexpStar:
      case re2::kRegexpPlus: {
        auto* sub = re->sub()[0];
        const bool dot_nl = sub->op() == re2::kRegexpAnyChar;
        if (!dot_nl && !AnyButNewline(sub)) {
          return false;
        }
        if (re->op() == re2::kRegexpPlus && !Wildcard(Unit::One, dot_nl)) {
          return false;
        }
        return Wildcard(Unit::Any, dot_nl);
      }
      default:
        return false;
    }
  }

  bool Literal(re2::Regexp* re, std::span<const re2::Rune> runes) {
    if (_end || (re->parse_flags() & re2::Regexp::FoldCase) != 0) {
      return false;
    }
    if (_tokens.empty() || _tokens.back().unit != Unit::Literal) {
      _tokens.push_back({.unit = Unit::Literal});
    }
    auto& bytes = _tokens.back().bytes;
    for (auto rune : runes) {
      if (IsSurrogate(rune)) {
        return false;
      }
      char utf8[re2::UTFmax];
      const int n = re2::runetochar(utf8, &rune);
      bytes.append(reinterpret_cast<const byte_type*>(utf8),
                   static_cast<size_t>(n));
    }
    return true;
  }

  bool Wildcard(Unit unit, bool dot_nl) {
    if (_end) {
      return false;
    }
    _tokens.push_back({.unit = unit, .dot_nl = dot_nl});
    return true;
  }

  size_t _n;
  byte_type _boundary;
  std::vector<Token> _tokens;
  bool _end{false};
};

}  // namespace

re2::RE2::Options RegexpOptions(RegexpSyntax syntax) {
  re2::RE2::Options options;
  options.set_log_errors(false);
  options.set_max_mem(int64_t{256} << 20);
  if (syntax == RegexpSyntax::PosixEre) {
    options.set_posix_syntax(true);
    options.set_one_line(true);
  }
  return options;
}

GramPlan ExtractGramQuery(bytes_view pattern, RegexpSyntax syntax,
                          size_t gram_size, byte_type boundary,
                          const GramQueryLimits& limits) {
  SDB_ASSERT(gram_size != 0);
  SDB_ASSERT(boundary < 0x80);
  const auto tree = ParseTree(pattern, syntax);
  if (!tree) {
    return {.query = {.kind = Kind::None}};
  }
  if (auto plan = LikeShape{gram_size, boundary}.Extract(tree.get(), limits)) {
    return *std::move(plan);
  }
  return {.query = Extractor{gram_size, boundary, limits}.Extract(tree.get())};
}

size_t LeafCount(const GramQuery& query) noexcept {
  if (query.kind == Kind::Literal) {
    return 1;
  }
  size_t count = 0;
  for (const auto& child : query.children) {
    count += LeafCount(child);
  }
  return count;
}

std::string ToString(const GramQuery& query) {
  std::string out;
  Append(out, query);
  return out;
}

}  // namespace irs
