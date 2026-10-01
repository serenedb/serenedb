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
    irs::ExtractGramQuery(Bytes(pattern), syntax, n, kBoundary, limits));
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
    {"a.c", "ALL"},
    {"a.c", R"(And("\x1fa", "c\x1f"))", 2},
    {"(?s)a.c", "ALL"},
    {"gr[ae]y", R"(Or("\x1fgray\x1f", "\x1fgrey\x1f"))"},
    {"(abc)?", "ALL"},
    {"(abc)?", R"(Or("\x1f\x1f", "\x1fabc\x1f"))", 2},
    {"", "ALL"},
    {"a*", "ALL"},
    {".*", "ALL"},
    {"alpha", R"("\x1falpha\x1f")"},
    {"ab", R"("\x1fab\x1f")"},
    {"ab.*", R"("\x1fab")"},
    {"ab.*", R"("\x1fab")", 2},
    {".*a", "ALL"},
    {"x.*x", "ALL"},
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
    {".*abc.*", "ALL", 4},
    {".*abcd.*", R"("abcd")", 4},
  };
  Check(kCases);
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
  EXPECT_EQ("ALL", Extract(".*\xD1\x81\xD0\xBE.*"));
  EXPECT_EQ(R"("\xd1\x81\xd0\xbe")", Extract(".*\xD1\x81\xD0\xBE.*", 2));

  const auto zhuk =
    irs::ExtractGramQuery(Bytes("(?i)\xD0\xB6\xD1\x83\xD0\xBA"),
                          irs::RegexpSyntax::Perl, 3, kBoundary);
  ASSERT_EQ(irs::GramQuery::Kind::Or, zhuk.kind);
  EXPECT_EQ(8U, zhuk.children.size());
  const irs::GramQuery lower{
    .kind = irs::GramQuery::Kind::Literal,
    .literal = irs::bstring{Bytes("\x1F\xD0\xB6\xD1\x83\xD0\xBA\x1F")},
  };
  EXPECT_NE(zhuk.children.end(), absl::c_find(zhuk.children, lower));
}

TEST(RegexpNGramTest, case_folding_drops_letters) {
  EXPECT_EQ("ALL", Extract("(?i)abc"));
  EXPECT_EQ(R"("123\x1f")", Extract("(?i)abc123"));
  EXPECT_EQ(R"("\x1f123")", Extract("123(?i:abc)"));
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

  EXPECT_EQ(
    R"(And("abcdefgh", "\x1fab", "gh\x1f"))",
    Extract("abcdefgh", 3, irs::RegexpSyntax::Perl, {.max_exact_runes = 4}));

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
    irs::ExtractGramQuery(Bytes(words), irs::RegexpSyntax::Perl, 3, kBoundary);
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
      const auto query = irs::ExtractGramQuery(
        Bytes(pattern), irs::RegexpSyntax::Perl, n, kBoundary);
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
