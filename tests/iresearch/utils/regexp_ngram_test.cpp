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

constexpr irs::byte_type kBoundary = 0xFF;

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
    {"abc", R"("\xffabc\xff")"},
    {"^abc$", R"("\xffabc\xff")"},
    {"abc.*", R"("\xffabc")"},
    {".*abc", R"("abc\xff")"},
    {".*abc.*", R"("abc")"},
    {"ab.*cd", R"(And("\xffab", "cd\xff"))"},
    {"foo.*bar.*baz", R"(And("\xfffoo", "bar", "baz\xff"))"},
    {".*(abc|xyz).*", R"(Or("abc", "xyz"))"},
    {".*(abc|x).*", "ALL"},
    {".*(abc|xy)z.*", R"(Or("abcz", "xyz"))"},
    {"[ab]cd", R"(Or("\xffacd\xff", "\xffbcd\xff"))"},
    {R"([a-z]+@example\.com)", R"("@example.com\xff")"},
    {"abc+", R"("\xffabc")"},
    {"(ab)+(cd)+", R"(And("\xffab", "abcd", "cd\xff"))"},
    {"a.c", R"("c\xff")"},
    {"a.c", R"(And("\xffa", "c\xff"))", 2},
    {"(?s)a.c", R"("c\xff")"},
    {"gr[ae]y", R"(Or("\xffgray\xff", "\xffgrey\xff"))"},
    {"(abc)?", "ALL"},
    {"(abc)?", R"(Or("\xffabc\xff", "\xff\xff"))", 2},
    {"", R"("\xff\xff")"},
    {"a*", "ALL"},
    {".*", "ALL"},
    {"(?s).*", R"("\xff")"},
    {"alpha", R"("\xffalpha\xff")"},
    {"ab", R"("\xffab\xff")"},
    {"ab.*", R"("\xffab")"},
    {"ab.*", R"("\xffab")", 2},
    {"a.*", R"("\xffa")"},
    {".*a", R"("a\xff")"},
    {".*a.*", R"("a")"},
    {"x.*x", R"("x\xff")"},
    {"..*", "ALL"},
    {"(?s)..*", "ALL"},
    {".*_.", R"("_")"},
    {"(?m)^a", R"("\xffa\xff")"},
    {R"(\babc\b)", R"("\xffabc\xff")"},
    {R"(a\Bbc)", R"("\xffabc\xff")"},
    {"a{2,3}", R"(Or("\xffaaa\xff", "\xffaa\xff"))"},
    {"(ab){2}", R"("\xffabab\xff")"},
    {"x(a[bc]d)+y",
     R"(And(Or("abd", "acd"), Or("\xffxab", "\xffxac"), Or("bdy\xff", "cdy\xff")))"},
    {R"(a\x41b)", R"("\xffaAb\xff")"},
    {R"(a\x1fb)", R"("\xffa\x1fb\xff")"},
    {R"(a\x{ff}b)", R"("\xffa\xc3\xbfb\xff")"},
    {R"(a\pLbc)", R"("bc\xff")"},
    {"abc", R"("\xffabc\xff")", 4},
    {".*abc.*", R"("abc")", 4},
    {".*abcd.*", R"("abcd")", 4},
  };
  Check(kCases);
}

TEST(RegexpNGramTest, exact_shapes) {
  for (const std::string_view pattern :
       {"abc", "^abc$", "(?s)abc.*", "(?s).*abc", "(?s).*abc.*", "(?s)a.*",
        "(?s).*a", "(?s).*", "", R"([\s\S]*abc)", "(ab){2}", R"(a\x1fb)",
        R"((?s)\x{ff}.*)"}) {
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
    {"gr[ae]y", R"(Or("\xffgray\xff", "\xffgrey\xff"))"},
    {"foo.*bar", R"(And("\xfffoo", "bar\xff"))"},
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
    .literal = irs::bstring{Bytes("\xFF\xD0\xB6\xD1\x83\xD0\xBA\xFF")},
  };
  EXPECT_NE(zhuk.children.end(), absl::c_find(zhuk.children, lower));
}

TEST(RegexpNGramTest, case_folding) {
  static constexpr Case kCases[]{
    {"(?i)abc",
     R"(Or("\xffABC\xff", "\xffABc\xff", "\xffAbC\xff", "\xffAbc\xff", )"
     R"("\xffaBC\xff", "\xffaBc\xff", "\xffabC\xff", "\xffabc\xff"))"},
    {"(?i)abc123",
     R"(Or("\xffABC123\xff", "\xffABc123\xff", "\xffAbC123\xff", )"
     R"("\xffAbc123\xff", "\xffaBC123\xff", "\xffaBc123\xff", )"
     R"("\xffabC123\xff", "\xffabc123\xff"))"},
    {"123(?i:abc)",
     R"(Or("\xff123ABC\xff", "\xff123ABc\xff", "\xff123AbC\xff", )"
     R"("\xff123Abc\xff", "\xff123aBC\xff", "\xff123aBc\xff", )"
     R"("\xff123abC\xff", "\xff123abc\xff"))"},
    {"(?i).*abc.*",
     R"(Or("ABC", "aBC", "AbC", "abC", "ABc", "aBc", "Abc", "abc"))"},
    {"(?i).*abc.*",
     R"(And(Or("AB", "aB", "Ab", "ab"), Or("BC", "bC", "Bc", "bc")))", 2},
    {"(?i)ab.*", R"(Or("\xffAB", "\xffAb", "\xffaB", "\xffab"))"},
    {"[Aa]bc", R"(Or("\xffAbc\xff", "\xffabc\xff"))"},
    {R"((?i)\x{4E2D}12)", R"("12\xff")"},
  };
  Check(kCases);
  EXPECT_EQ(Extract("(?i)abc"), Extract("(?i)ABC"));
}

// RE2 parses `(?i)k` and `(?i)s` into classes of three runes: the third case is
// the Kelvin sign U+212A and the long s U+017F.
TEST(RegexpNGramTest, case_folding_third_case) {
  static constexpr Case kCases[]{
    {"(?i)k", R"(Or("\xffK\xff", "\xffk\xff", "\xff\xe2\x84\xaa\xff"))"},
    {"(?i)s", R"(Or("\xffS\xff", "\xffs\xff", "\xff\xc5\xbf\xff"))"},
    {"(?i).*k12.*", R"(Or("K12", "k12", "\xe2\x84\xaa12"))"},
    {"(?i).*s12.*", R"(Or("S12", "s12", "\xc5\xbf12"))"},
    {"(?i).*ok1.*",
     R"(Or("OK1", "oK1", "Ok1", "ok1", "O\xe2\x84\xaa1", "o\xe2\x84\xaa1"))"},
    {"[Kk]1", R"(Or("\xffK1\xff", "\xffk1\xff"))"},
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
  EXPECT_TRUE(Eval(query, Bytes("\xFFjava.lang.OutOfMemoryError\xFF")));
  EXPECT_TRUE(Eval(query, Bytes("\xFFOUTOFMEMORYERROR\xFF")));
  EXPECT_FALSE(Eval(query, Bytes("\xFFout of memory error\xFF")));

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
  EXPECT_TRUE(Eval(long_query, Bytes(absl::StrCat("\xFF", letters, "\xFF"))));
}

TEST(RegexpNGramTest, nothing_to_require) {
  EXPECT_EQ("ALL", Extract(R"(\Cabc)"));
  EXPECT_EQ("ALL", Extract(R"(.*\C.*)"));
  EXPECT_EQ("ALL", Extract(R"(abc(\C)*)"));
  EXPECT_EQ("ALL", Extract(R"(\x{D800}abc)"));
  EXPECT_EQ("ALL", Extract(R"([\x{D7FF}-\x{D800}]abc)"));
}

TEST(RegexpNGramTest, nothing_matches) {
  EXPECT_EQ("NONE", Extract("("));
  EXPECT_EQ("NONE", Extract(R"(foo\)"));
  EXPECT_EQ("NONE", Extract(R"([^\x00-\x{10FFFF}])"));
  EXPECT_EQ("NONE", Extract(R"(abc[^\x00-\x{10FFFF}])"));
}

TEST(RegexpNGramTest, limits) {
  EXPECT_EQ(R"(Or("acd\xff", "bcd\xff"))",
            Extract("[ab]cd", 3, irs::RegexpSyntax::Perl, {.max_exact = 1}));

  EXPECT_EQ(R"(And(Or("\xffab", "\xffcd"), Or("abx\xff", "cdx\xff")))",
            Extract("(ab|cd)+x"));
  EXPECT_EQ("ALL",
            Extract("(ab|cd)+x", 3, irs::RegexpSyntax::Perl, {.max_set = 1}));

  EXPECT_EQ(R"(Or("\xffax\xff", "\xffbx\xff", "\xffcx\xff"))",
            Extract("[a-c]x"));
  EXPECT_EQ("ALL",
            Extract("[a-c]x", 3, irs::RegexpSyntax::Perl, {.max_class = 2}));

  EXPECT_EQ(R"(And("abcdefgh", "\xffab", "gh\xff"))",
            Extract(R"(\babcdefgh)", 3, irs::RegexpSyntax::Perl,
                    {.max_exact_runes = 4}));

  EXPECT_EQ(R"(Or("abc", "def", "ghi"))", Extract(".*(abc|def|ghi).*"));
  EXPECT_EQ("ALL", Extract(".*(abc|def|ghi).*", 3, irs::RegexpSyntax::Perl,
                           {.max_leaves = 2}));
  EXPECT_EQ(R"(And("\xffabc", "def", "ghi\xff"))", Extract("abc.*def.*ghi"));
  EXPECT_EQ(
    R"(And("\xffabc", "ghi\xff"))",
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
    "a", "b", "c", "A", "\x1F", "\xC3\xBF", "\xD0\xB6",
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
    "a\\x{ff}b",
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
    "\\b", "^", "$", "(?i:a)", "\\x1f",    "(a|bc)", "(ab|c)?", "\\x{ff}",
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
        const auto wrapped = absl::StrCat("\xFF", term, "\xFF");
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
        const auto wrapped = absl::StrCat("\xFF", term, "\xFF");
        EXPECT_TRUE(Eval(query, Bytes(wrapped)))
          << "pattern: " << pattern << ", n: " << n << ", term: " << term
          << ", query: " << irs::ToString(query);
      }
    }
  }
}

TEST(RegexpNGramTest, exact_plans_decide_terms) {
  static constexpr std::string_view kAlphabet[]{"a",    "b",        "\n",
                                                "\x1F", "\xC3\xBF", "\xD0\xB6"};
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
    "a",      "b", "\xD0\xB6", "ab",    "(?s:.*)", ".*",
    "(?s:.)", ".", "\\n",      "\\x1f", "\\x{ff}",
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
        const auto wrapped = absl::StrCat("\xFF", term, "\xFF");
        EXPECT_EQ(acceptor.Matches(Bytes(term)),
                  Eval(plan.query, Bytes(wrapped)))
          << "pattern: " << pattern << ", n: " << n << ", term: " << term
          << ", query: " << irs::ToString(plan.query);
      }
    }
  }
  EXPECT_LT(100U, exact);
}
