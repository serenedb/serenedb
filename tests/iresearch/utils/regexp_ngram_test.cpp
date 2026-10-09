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

#include <absl/algorithm/container.h>
#include <absl/strings/str_cat.h>

#include <cstddef>
#include <iresearch/utils/regexp_acceptor.hpp>
#include <iresearch/utils/regexp_ngram.hpp>
#include <random>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "tests_shared.hpp"

namespace {

constexpr irs::byte_type kBoundary = 0x1F;

irs::bytes_view Bytes(std::string_view s) {
  return irs::ViewCast<irs::byte_type>(s);
}

std::string Extract(std::string_view pattern, size_t n = 3,
                    irs::RegexpSyntax syntax = irs::RegexpSyntax::Perl,
                    const irs::GramQueryLimits& limits = {}) {
  return irs::ToString(
    irs::ExtractGramQuery(Bytes(pattern), syntax, n, kBoundary, limits).query);
}

bool Exact(std::string_view pattern, size_t n = 3) {
  return irs::ExtractGramQuery(Bytes(pattern), irs::RegexpSyntax::Perl, n,
                               kBoundary)
    .exact;
}

struct Case {
  std::string_view pattern;
  std::string_view expected;
  size_t n{3};
};

void Check(std::span<const Case> cases,
           irs::RegexpSyntax syntax = irs::RegexpSyntax::Perl) {
  for (const auto& c : cases) {
    EXPECT_EQ(c.expected, Extract(c.pattern, c.n, syntax))
      << "pattern: " << c.pattern << ", n: " << c.n;
  }
}

bool Eval(const irs::GramQuery& query, irs::bytes_view wrapped) {
  using Kind = irs::GramQuery::Kind;
  switch (query.kind) {
    case Kind::All:
      return true;
    case Kind::None:
      return false;
    case Kind::Literal:
      return wrapped.find(query.literal) != irs::bytes_view::npos;
    case Kind::And:
      for (const auto& child : query.children) {
        if (!Eval(child, wrapped)) {
          return false;
        }
      }
      return true;
    case Kind::Or:
      for (const auto& child : query.children) {
        if (Eval(child, wrapped)) {
          return true;
        }
      }
      return false;
  }
  return false;
}

}  // namespace

TEST(RegexpNGramTest, shapes) {
  static constexpr Case kCases[]{
    {"abc", R"("\x1fabc\x1f")"},
    {"^abc$", R"("\x1fabc\x1f")"},
    {"abc.*", R"("\x1fabc")"},
    {".*abc", R"("abc\x1f")"},
    {".*abc.*", R"("abc")"},
    {"ab.*cd", R"(And("\x1fab", "cd\x1f"))"},
    {"foo.*bar.*baz", R"(And("\x1ffoo", "bar", "baz\x1f"))"},
    {".*(abc|xyz).*", R"(Or("abc", "xyz"))"},
    {".*(abc|x).*", "ALL"},
    {".*(abc|xy)z.*", R"(Or("abcz", "xyz"))"},
    {"[ab]cd", R"(Or("\x1facd\x1f", "\x1fbcd\x1f"))"},
    {R"([a-z]+@example\.com)", R"("@example.com\x1f")"},
    {"abc+", R"("\x1fabc")"},
    {"(ab)+(cd)+", R"(And("\x1fab", "abcd", "cd\x1f"))"},
    {"a.c", R"("c\x1f")"},
    {"a.c", R"(And("\x1fa", "c\x1f"))", 2},
    {"(?s)a.c", R"("c\x1f")"},
    {"gr[ae]y", R"(Or("\x1fgray\x1f", "\x1fgrey\x1f"))"},
    {"(abc)?", "ALL"},
    {"(abc)?", R"(Or("\x1f\x1f", "\x1fabc\x1f"))", 2},
    {"", R"("\x1f\x1f")"},
    {"a*", "ALL"},
    {".*", "ALL"},
    {"(?s).*", R"("\x1f")"},
    {"alpha", R"("\x1falpha\x1f")"},
    {"ab", R"("\x1fab\x1f")"},
    {"ab.*", R"("\x1fab")"},
    {"ab.*", R"("\x1fab")", 2},
    {"a.*", R"("\x1fa")"},
    {".*a", R"("a\x1f")"},
    {".*a.*", R"("a")"},
    {"x.*x", R"("x\x1f")"},
    {"..*", "ALL"},
    {"(?s)..*", "ALL"},
    {".*_.", R"("_")"},
    {"(?m)^a", R"("\x1fa\x1f")"},
    {R"(\babc\b)", R"("\x1fabc\x1f")"},
    {R"(a\Bbc)", R"("\x1fabc\x1f")"},
    {"a{2,3}", R"(Or("\x1faa\x1f", "\x1faaa\x1f"))"},
    {"(ab){2}", R"("\x1fabab\x1f")"},
    {"x(a[bc]d)+y",
     R"(And(Or("abd", "acd"), Or("\x1fxab", "\x1fxac"), Or("bdy\x1f", "cdy\x1f")))"},
    {R"(a\x1fb)", R"("\x1fa\x1fb\x1f")"},
    {R"(a\x41b)", R"("\x1faAb\x1f")"},
    {R"(a\pLbc)", R"("bc\x1f")"},
    {"abc", R"("\x1fabc\x1f")", 4},
    {".*abc.*", R"("abc")", 4},
    {".*abcd.*", R"("abcd")", 4},
  };
  Check(kCases);
}

TEST(RegexpNGramTest, exact_shapes) {
  for (const std::string_view pattern :
       {"abc", "^abc$", "(?s)abc.*", "(?s).*abc", "(?s).*abc.*", "(?s)a.*",
        "(?s).*a", "(?s).*", "", R"([\s\S]*abc)", "(ab){2}"}) {
    EXPECT_TRUE(Exact(pattern)) << pattern;
  }
  for (const std::string_view pattern :
       {"abc.*", ".*abc", "a.c", "(?s)a.c", "(?s)ab.*cd", "(?s).+abc",
        "(?i)abc", "[ab]cd", "(abc)?", "a+", R"(\babc)"}) {
    EXPECT_FALSE(Exact(pattern)) << pattern;
  }
}

TEST(RegexpNGramTest, posix) {
  static constexpr Case kCases[]{
    {"gr[ae]y", R"(Or("\x1fgray\x1f", "\x1fgrey\x1f"))"},
    {"foo.*bar", R"(And("\x1ffoo", "bar\x1f"))"},
    {R"(\d)", "NONE"},
  };
  Check(kCases, irs::RegexpSyntax::PosixEre);
}

TEST(RegexpNGramTest, code_points) {
  // "sobak" and "zhuk" in Cyrillic, two bytes per letter.
  EXPECT_EQ(R"("\xd1\x81\xd0\xbe\xd0\xb1\xd0\xb0\xd0\xba")",
            Extract(".*\xD1\x81\xD0\xBE\xD0\xB1\xD0\xB0\xD0\xBA.*"));
  EXPECT_EQ(R"("\xd1\x81\xd0\xbe")", Extract(".*\xD1\x81\xD0\xBE.*"));
  EXPECT_EQ(R"("\xd1\x81\xd0\xbe")", Extract(".*\xD1\x81\xD0\xBE.*", 2));

  const auto zhuk = irs::ExtractGramQuery(Bytes("(?i)\xD0\xB6\xD1\x83\xD0\xBA"),
                                          irs::RegexpSyntax::Perl, 3, kBoundary)
                      .query;
  ASSERT_EQ(irs::GramQuery::Kind::Or, zhuk.kind);
  EXPECT_EQ(8U, zhuk.children.size());
  const irs::GramQuery lower{
    .kind = irs::GramQuery::Kind::Literal,
    .literal = irs::bstring{Bytes("\x1F\xD0\xB6\xD1\x83\xD0\xBA\x1F")},
  };
  EXPECT_NE(zhuk.children.end(), absl::c_find(zhuk.children, lower));
}

TEST(RegexpNGramTest, case_folding) {
  static constexpr Case kCases[]{
    {"(?i)abc",
     R"(Or("\x1fABC\x1f", "\x1fABc\x1f", "\x1fAbC\x1f", "\x1fAbc\x1f", )"
     R"("\x1faBC\x1f", "\x1faBc\x1f", "\x1fabC\x1f", "\x1fabc\x1f"))"},
    {"(?i)abc123",
     R"(Or("\x1fABC123\x1f", "\x1fABc123\x1f", "\x1fAbC123\x1f", )"
     R"("\x1fAbc123\x1f", "\x1faBC123\x1f", "\x1faBc123\x1f", )"
     R"("\x1fabC123\x1f", "\x1fabc123\x1f"))"},
    {"123(?i:abc)",
     R"(Or("\x1f123ABC\x1f", "\x1f123ABc\x1f", "\x1f123AbC\x1f", )"
     R"("\x1f123Abc\x1f", "\x1f123aBC\x1f", "\x1f123aBc\x1f", )"
     R"("\x1f123abC\x1f", "\x1f123abc\x1f"))"},
    {"(?i).*abc.*",
     R"(Or("ABC", "aBC", "AbC", "abC", "ABc", "aBc", "Abc", "abc"))"},
    {"(?i).*abc.*",
     R"(And(Or("AB", "aB", "Ab", "ab"), Or("BC", "bC", "Bc", "bc")))", 2},
    {"(?i)ab.*", R"(Or("\x1fAB", "\x1fAb", "\x1faB", "\x1fab"))"},
    {"[Aa]bc", R"(Or("\x1fAbc\x1f", "\x1fabc\x1f"))"},
    {R"((?i)\x{4E2D}12)", R"("12\x1f")"},
  };
  Check(kCases);
  EXPECT_EQ(Extract("(?i)abc"), Extract("(?i)ABC"));
}

// RE2 parses `(?i)k` and `(?i)s` into classes of three runes: the third case is
// the Kelvin sign U+212A and the long s U+017F.
TEST(RegexpNGramTest, case_folding_third_case) {
  static constexpr Case kCases[]{
    {"(?i)k", R"(Or("\x1fK\x1f", "\x1fk\x1f", "\x1f\xe2\x84\xaa\x1f"))"},
    {"(?i)s", R"(Or("\x1fS\x1f", "\x1fs\x1f", "\x1f\xc5\xbf\x1f"))"},
    {"(?i).*k12.*", R"(Or("K12", "k12", "\xe2\x84\xaa12"))"},
    {"(?i).*s12.*", R"(Or("S12", "s12", "\xc5\xbf12"))"},
    {"(?i).*ok1.*",
     R"(Or("OK1", "oK1", "Ok1", "ok1", "O\xe2\x84\xaa1", "o\xe2\x84\xaa1"))"},
    {"[Kk]1", R"(Or("\x1fK1\x1f", "\x1fk1\x1f"))"},
  };
  Check(kCases);
  EXPECT_EQ(Extract("(?i)k"), Extract("(?i)K"));
  EXPECT_EQ(Extract("(?i)k"), Extract(R"((?i)\x{212A})"));
}

TEST(RegexpNGramTest, case_folding_budget) {
  const auto query =
    irs::ExtractGramQuery(Bytes("(?i).*outofmemoryerror.*"),
                          irs::RegexpSyntax::Perl, 3, kBoundary)
      .query;
  ASSERT_EQ(irs::GramQuery::Kind::And, query.kind);
  EXPECT_LE(irs::LeafCount(query), irs::GramQueryLimits{}.max_leaves);
  EXPECT_TRUE(Eval(query, Bytes("\x1Fjava.lang.OutOfMemoryError\x1F")));
  EXPECT_TRUE(Eval(query, Bytes("\x1FOUTOFMEMORYERROR\x1F")));
  EXPECT_FALSE(Eval(query, Bytes("\x1Fout of memory error\x1F")));

  std::mt19937 rng{20261003};
  std::string letters;
  for (int i = 0; i != 1000; ++i) {
    letters += static_cast<char>('a' + rng() % 26);
  }
  const auto pattern = absl::StrCat("(?i).*", letters, ".*");
  const auto long_query =
    irs::ExtractGramQuery(Bytes(pattern), irs::RegexpSyntax::Perl, 3, kBoundary)
      .query;
  ASSERT_EQ(irs::GramQuery::Kind::And, long_query.kind);
  EXPECT_LE(irs::LeafCount(long_query), irs::GramQueryLimits{}.max_leaves);
  EXPECT_TRUE(Eval(long_query, Bytes(absl::StrCat("\x1F", letters, "\x1F"))));
}

TEST(RegexpNGramTest, nothing_to_require) {
  EXPECT_EQ("ALL", Extract(R"(\Cabc)"));
  EXPECT_EQ("ALL", Extract(R"(.*\C.*)"));
  EXPECT_EQ("ALL", Extract(R"(abc(\C)*)"));
  EXPECT_EQ("ALL", Extract(R"(\x{D800}abc)"));
}

TEST(RegexpNGramTest, nothing_matches) {
  EXPECT_EQ("NONE", Extract("("));
  EXPECT_EQ("NONE", Extract(R"(foo\)"));
  EXPECT_EQ("NONE", Extract(R"([^\x00-\x{10FFFF}])"));
  EXPECT_EQ("NONE", Extract(R"(abc[^\x00-\x{10FFFF}])"));
}

TEST(RegexpNGramTest, limits) {
  EXPECT_EQ(R"(Or("acd\x1f", "bcd\x1f"))",
            Extract("[ab]cd", 3, irs::RegexpSyntax::Perl, {.max_exact = 1}));

  EXPECT_EQ(R"(And(Or("\x1fab", "\x1fcd"), Or("abx\x1f", "cdx\x1f")))",
            Extract("(ab|cd)+x"));
  EXPECT_EQ("ALL",
            Extract("(ab|cd)+x", 3, irs::RegexpSyntax::Perl, {.max_set = 1}));

  EXPECT_EQ(R"(Or("\x1fax\x1f", "\x1fbx\x1f", "\x1fcx\x1f"))",
            Extract("[a-c]x"));
  EXPECT_EQ("ALL",
            Extract("[a-c]x", 3, irs::RegexpSyntax::Perl, {.max_class = 2}));

  EXPECT_EQ(R"(And("abcdefgh", "\x1fab", "gh\x1f"))",
            Extract(R"(\babcdefgh)", 3, irs::RegexpSyntax::Perl,
                    {.max_exact_runes = 4}));

  EXPECT_EQ(R"(Or("abc", "def", "ghi"))", Extract(".*(abc|def|ghi).*"));
  EXPECT_EQ("ALL", Extract(".*(abc|def|ghi).*", 3, irs::RegexpSyntax::Perl,
                           {.max_leaves = 2}));
  EXPECT_EQ(R"(And("\x1fabc", "def", "ghi\x1f"))", Extract("abc.*def.*ghi"));
  EXPECT_EQ(
    R"(And("\x1fabc", "ghi\x1f"))",
    Extract("abc.*def.*ghi", 3, irs::RegexpSyntax::Perl, {.max_leaves = 2}));

  std::string words = ".*(";
  for (int i = 0; i != 100; ++i) {
    absl::StrAppend(&words, i == 0 ? "" : "|", "w", i, "x", i * 7, "y");
  }
  words += ").*";
  const auto query =
    irs::ExtractGramQuery(Bytes(words), irs::RegexpSyntax::Perl, 3, kBoundary)
      .query;
  EXPECT_LE(irs::LeafCount(query), irs::GramQueryLimits{}.max_leaves);
}

// Every term the acceptor matches satisfies the extracted query.
TEST(RegexpNGramTest, matched_terms_satisfy_query) {
  static constexpr std::string_view kAlphabet[]{
    "a", "b", "c", "A", "\x1F", "\xD0\xB6",
  };
  std::vector<std::string> terms{""};
  for (size_t from = 0, len = 0; len != 4; ++len) {
    const auto to = terms.size();
    for (auto i = from; i != to; ++i) {
      for (const auto letter : kAlphabet) {
        terms.push_back(absl::StrCat(terms[i], letter));
      }
    }
    from = to;
  }

  std::vector<std::string> patterns{
    "abc",
    "^abc$",
    "abc.*",
    ".*abc",
    ".*abc.*",
    "ab.*cd",
    "a.c",
    "(abc)?",
    "a*",
    ".*(abc|ba).*",
    "[ab]cA",
    "(ab)+",
    "(?i)abc",
    "(?i)ab1",
    "a\\x1fb",
    "\\babc",
    "a\\Bb",
    "(?s).a.",
    "a{2,3}b",
    "(a|b)(c|A)*",
    "\xD0\xB6+a",
    ".*\xD0\xB6"
    "a.*",
    "[^a]bc",
    "(?m)^a$",
  };
  static constexpr std::string_view kAtoms[]{
    "a",   "b", "c", "A",      "\xD0\xB6", ".",      "[ab]",    "[^a]",
    "\\b", "^", "$", "(?i:a)", "\\x1f",    "(a|bc)", "(ab|c)?",
  };
  static constexpr std::string_view kSuffixes[]{
    "", "", "", "*", "+", "?", "{1,2}",
  };
  std::mt19937 rng{20261001};
  for (int i = 0; i != 200; ++i) {
    std::string pattern;
    const auto atoms = 1 + rng() % 5;
    for (size_t j = 0; j != atoms; ++j) {
      absl::StrAppend(&pattern, kAtoms[rng() % std::size(kAtoms)],
                      kSuffixes[rng() % std::size(kSuffixes)]);
    }
    patterns.push_back(std::move(pattern));
  }

  for (const auto& pattern : patterns) {
    const irs::RegexpAcceptor acceptor{Bytes(pattern)};
    for (const auto n : {size_t{2}, size_t{3}}) {
      const auto query =
        irs::ExtractGramQuery(Bytes(pattern), irs::RegexpSyntax::Perl, n,
                              kBoundary)
          .query;
      for (const auto& term : terms) {
        if (!acceptor.Matches(Bytes(term))) {
          continue;
        }
        const auto wrapped = absl::StrCat("\x1F", term, "\x1F");
        EXPECT_TRUE(Eval(query, Bytes(wrapped)))
          << "pattern: " << pattern << ", n: " << n << ", term: " << term
          << ", query: " << irs::ToString(query);
      }
    }
  }
}

// Every term RE2 matches satisfies the extracted query, over the runes RE2
// folds together.
TEST(RegexpNGramTest, folded_terms_satisfy_query) {
  static constexpr std::string_view kAlphabet[]{
    "a", "A", "k", "K", "\xE2\x84\xAA", "s", "S", "\xC5\xBF", "1",
  };
  std::vector<std::string> terms{""};
  for (size_t from = 0, len = 0; len != 3; ++len) {
    const auto to = terms.size();
    for (auto i = from; i != to; ++i) {
      for (const auto letter : kAlphabet) {
        terms.push_back(absl::StrCat(terms[i], letter));
      }
    }
    from = to;
  }

  std::vector<std::string> patterns{
    "(?i)k",
    "(?i)s",
    "(?i)ks",
    "(?i)ask",
    "(?i).*k.*",
    "(?i)a.*s",
    "[Kk]a",
    "[Ss]+",
    "(?i)AK1",
    "(?i)(a|k)+1",
    R"((?i)\x{212A}s)",
    R"(\x{17F}k)",
  };
  static constexpr std::string_view kAtoms[]{
    "a",    "A",      "k",      "S",         "1",
    ".",    "(?i:a)", "(?i:k)", "(?i:s)",    "(?i:ak)",
    "[Kk]", "[Ss]",   "(a|K)",  "\\x{212A}", "\\x{17F}",
  };
  static constexpr std::string_view kSuffixes[]{
    "", "", "", "*", "+", "?",
  };
  std::mt19937 rng{20261003};
  for (int i = 0; i != 300; ++i) {
    std::string pattern;
    const auto atoms = 1 + rng() % 4;
    for (size_t j = 0; j != atoms; ++j) {
      absl::StrAppend(&pattern, kAtoms[rng() % std::size(kAtoms)],
                      kSuffixes[rng() % std::size(kSuffixes)]);
    }
    patterns.push_back(std::move(pattern));
  }

  for (const auto& pattern : patterns) {
    const re2::RE2 re{pattern, irs::RegexpOptions(irs::RegexpSyntax::Perl)};
    ASSERT_TRUE(re.ok()) << pattern;
    for (const auto n : {size_t{2}, size_t{3}}) {
      const auto query =
        irs::ExtractGramQuery(Bytes(pattern), irs::RegexpSyntax::Perl, n,
                              kBoundary)
          .query;
      for (const auto& term : terms) {
        if (!re2::RE2::FullMatch(term, re)) {
          continue;
        }
        const auto wrapped = absl::StrCat("\x1F", term, "\x1F");
        EXPECT_TRUE(Eval(query, Bytes(wrapped)))
          << "pattern: " << pattern << ", n: " << n << ", term: " << term
          << ", query: " << irs::ToString(query);
      }
    }
  }
}

TEST(RegexpNGramTest, exact_plans_decide_terms) {
  static constexpr std::string_view kAlphabet[]{"a", "b", "\n", "\xD0\xB6"};
  std::vector<std::string> terms{""};
  for (size_t from = 0, len = 0; len != 4; ++len) {
    const auto to = terms.size();
    for (auto i = from; i != to; ++i) {
      for (const auto letter : kAlphabet) {
        terms.push_back(absl::StrCat(terms[i], letter));
      }
    }
    from = to;
  }

  std::vector<std::string> patterns{
    "", "a", "ab", "(?s).*", "(?s)a.*", "(?s).*a", "(?s).*ab.*", "^ab$",
  };
  static constexpr std::string_view kAtoms[]{
    "a", "b", "\xD0\xB6", "ab", "(?s:.*)", ".*", "(?s:.)", ".", "\\n",
  };
  std::mt19937 rng{20261002};
  for (int i = 0; i != 400; ++i) {
    std::string pattern;
    const auto atoms = rng() % 5;
    for (size_t j = 0; j != atoms; ++j) {
      absl::StrAppend(&pattern, kAtoms[rng() % std::size(kAtoms)]);
    }
    patterns.push_back(std::move(pattern));
  }

  size_t exact = 0;
  for (const auto& pattern : patterns) {
    const irs::RegexpAcceptor acceptor{Bytes(pattern)};
    for (const auto n : {size_t{2}, size_t{3}}) {
      const auto plan = irs::ExtractGramQuery(
        Bytes(pattern), irs::RegexpSyntax::Perl, n, kBoundary);
      if (!plan.exact) {
        continue;
      }
      ++exact;
      for (const auto& term : terms) {
        const auto wrapped = absl::StrCat("\x1F", term, "\x1F");
        EXPECT_EQ(acceptor.Matches(Bytes(term)),
                  Eval(plan.query, Bytes(wrapped)))
          << "pattern: " << pattern << ", n: " << n << ", term: " << term
          << ", query: " << irs::ToString(plan.query);
      }
    }
  }
  EXPECT_LT(100U, exact);
}
