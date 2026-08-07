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

// `regexp_utils` is the classifier that decides which filter a pattern becomes:
// a term, a prefix walk, or the full acceptor. Getting it wrong does not fail a
// query, it silently loses the fast path -- so every class it can answer is
// pinned here.
//
// Alongside the classifier sits the acceptance suite: which terms a pattern
// selects, asserted one pattern at a time through `RegexpAcceptor::Matches`.
// What is pinned here is not RE2's matching but the reading we impose on top
// of it -- anchors and `\b` erased, `\B` under-approximated to nothing, every
// partial character class narrowed to the strict UTF-8 model, and the wildcard
// dialect taken byte for byte -- plus the two dialects, the memory budget and
// the primitives (`StepRun`, `LiveRange`) the dictionary walk drives. That the
// walk yields exactly what a full scan plus a per-term test yields is asserted
// separately, in `index/index_acceptor_walk_tests.cpp`.

#include <cstdint>
#include <iresearch/utils/regexp_acceptor.hpp>
#include <iresearch/utils/regexp_utils.hpp>
#include <string>
#include <string_view>

#include "tests_shared.hpp"

class RegexpUtilsTest : public TestBase {
 protected:
  static irs::bytes_view ToBytesView(std::string_view sv) {
    return irs::ViewCast<irs::byte_type>(sv);
  }

  static bool Accepts(const irs::RegexpAcceptor& a, std::string_view str) {
    return a.Matches(ToBytesView(str));
  }

  static irs::RegexpAcceptor FromPerl(std::string_view pattern) {
    return irs::RegexpAcceptor{ToBytesView(pattern)};
  }

  static irs::RegexpAcceptor FromPosix(std::string_view pattern) {
    return irs::RegexpAcceptor{ToBytesView(pattern),
                               irs::RegexpSyntax::PosixEre};
  }
};

// ComputeRegexpType - pattern classification

TEST_F(RegexpUtilsTest, regexp_type_empty) {
  ASSERT_EQ(irs::RegexpType::Literal, irs::ComputeRegexpType(ToBytesView("")));
}

TEST_F(RegexpUtilsTest, regexp_type_literal) {
  ASSERT_EQ(irs::RegexpType::Literal,
            irs::ComputeRegexpType(ToBytesView("foo")));
  ASSERT_EQ(irs::RegexpType::Literal,
            irs::ComputeRegexpType(ToBytesView("hello world")));
  ASSERT_EQ(irs::RegexpType::Literal,
            irs::ComputeRegexpType(ToBytesView("123abc")));
  ASSERT_EQ(irs::RegexpType::LiteralEscaped,
            irs::ComputeRegexpType(ToBytesView("foo\\.bar")));
  ASSERT_EQ(irs::RegexpType::LiteralEscaped,
            irs::ComputeRegexpType(ToBytesView("a\\*b")));
  ASSERT_EQ(irs::RegexpType::LiteralEscaped,
            irs::ComputeRegexpType(ToBytesView("a\\+b\\?c")));
}

TEST_F(RegexpUtilsTest, regexp_type_prefix) {
  ASSERT_EQ(irs::RegexpType::Prefix,
            irs::ComputeRegexpType(ToBytesView("foo.*")));
  ASSERT_EQ(irs::RegexpType::Prefix,
            irs::ComputeRegexpType(ToBytesView("abc.*")));
  ASSERT_EQ(irs::RegexpType::Prefix,
            irs::ComputeRegexpType(ToBytesView("x.*")));
  ASSERT_EQ(irs::RegexpType::Prefix,
            irs::ComputeRegexpType(ToBytesView("hello world.*")));
}

TEST_F(RegexpUtilsTest, regexp_type_prefix_escaped) {
  ASSERT_EQ(irs::RegexpType::PrefixEscaped,
            irs::ComputeRegexpType(ToBytesView("foo\\.bar.*")));
  ASSERT_EQ(irs::RegexpType::PrefixEscaped,
            irs::ComputeRegexpType(ToBytesView("a\\*b.*")));
}

TEST_F(RegexpUtilsTest, regexp_type_complex) {
  ASSERT_EQ(irs::RegexpType::Complex,
            irs::ComputeRegexpType(ToBytesView("fo+")));
  ASSERT_EQ(irs::RegexpType::Complex,
            irs::ComputeRegexpType(ToBytesView("a|b")));
  ASSERT_EQ(irs::RegexpType::Complex,
            irs::ComputeRegexpType(ToBytesView(".*foo")));
  ASSERT_EQ(irs::RegexpType::Complex,
            irs::ComputeRegexpType(ToBytesView("a.*b")));
  ASSERT_EQ(irs::RegexpType::Complex,
            irs::ComputeRegexpType(ToBytesView("[abc]")));
  ASSERT_EQ(irs::RegexpType::Complex,
            irs::ComputeRegexpType(ToBytesView("a?b")));
  ASSERT_EQ(irs::RegexpType::Complex,
            irs::ComputeRegexpType(ToBytesView("(ab)+")));
  // .* is classified as Prefix with empty prefix
  ASSERT_EQ(irs::RegexpType::Prefix, irs::ComputeRegexpType(ToBytesView(".*")));
  ASSERT_EQ(irs::RegexpType::Complex,
            irs::ComputeRegexpType(ToBytesView("fo+.*")));
  ASSERT_EQ(irs::RegexpType::Complex,
            irs::ComputeRegexpType(ToBytesView("a|b.*")));
}

// ExtractRegexpPrefix

TEST_F(RegexpUtilsTest, extract_prefix) {
  EXPECT_EQ(
    "foo", irs::ViewCast<char>(irs::ExtractRegexpPrefix(ToBytesView("foo.*"))));
  EXPECT_EQ(
    "abc", irs::ViewCast<char>(irs::ExtractRegexpPrefix(ToBytesView("abc.*"))));
  EXPECT_EQ("x",
            irs::ViewCast<char>(irs::ExtractRegexpPrefix(ToBytesView("x.*"))));
  EXPECT_EQ("",
            irs::ViewCast<char>(irs::ExtractRegexpPrefix(ToBytesView(".*"))));
  EXPECT_EQ("hello world", irs::ViewCast<char>(irs::ExtractRegexpPrefix(
                             ToBytesView("hello world.*"))));
}

// UnescapeRegexp

TEST_F(RegexpUtilsTest, unescape_empty) {
  irs::bstring out;
  auto result = irs::UnescapeRegexp(ToBytesView(""), out);
  EXPECT_TRUE(result.empty());
  EXPECT_TRUE(out.empty());
}

TEST_F(RegexpUtilsTest, unescape_no_escapes) {
  irs::bstring out;
  auto result = irs::UnescapeRegexp(ToBytesView("hello"), out);
  EXPECT_EQ("hello", irs::ViewCast<char>(result));
}

TEST_F(RegexpUtilsTest, unescape_single_metacharacters) {
  irs::bstring out;
  EXPECT_EQ(".",
            irs::ViewCast<char>(irs::UnescapeRegexp(ToBytesView("\\."), out)));
  EXPECT_EQ("*",
            irs::ViewCast<char>(irs::UnescapeRegexp(ToBytesView("\\*"), out)));
  EXPECT_EQ("+",
            irs::ViewCast<char>(irs::UnescapeRegexp(ToBytesView("\\+"), out)));
  EXPECT_EQ("?",
            irs::ViewCast<char>(irs::UnescapeRegexp(ToBytesView("\\?"), out)));
  EXPECT_EQ("|",
            irs::ViewCast<char>(irs::UnescapeRegexp(ToBytesView("\\|"), out)));
  EXPECT_EQ("(",
            irs::ViewCast<char>(irs::UnescapeRegexp(ToBytesView("\\("), out)));
  EXPECT_EQ(")",
            irs::ViewCast<char>(irs::UnescapeRegexp(ToBytesView("\\)"), out)));
  EXPECT_EQ("[",
            irs::ViewCast<char>(irs::UnescapeRegexp(ToBytesView("\\["), out)));
  EXPECT_EQ("]",
            irs::ViewCast<char>(irs::UnescapeRegexp(ToBytesView("\\]"), out)));
  EXPECT_EQ("^",
            irs::ViewCast<char>(irs::UnescapeRegexp(ToBytesView("\\^"), out)));
  EXPECT_EQ("$",
            irs::ViewCast<char>(irs::UnescapeRegexp(ToBytesView("\\$"), out)));
  EXPECT_EQ("\\",
            irs::ViewCast<char>(irs::UnescapeRegexp(ToBytesView("\\\\"), out)));
  EXPECT_EQ("{",
            irs::ViewCast<char>(irs::UnescapeRegexp(ToBytesView("\\{"), out)));
  EXPECT_EQ("}",
            irs::ViewCast<char>(irs::UnescapeRegexp(ToBytesView("\\}"), out)));
}

TEST_F(RegexpUtilsTest, unescape_all_metacharacters_concatenated) {
  irs::bstring out;
  auto result = irs::UnescapeRegexp(
    ToBytesView("\\.\\*\\+\\?\\|\\(\\)\\[\\]\\^\\$\\\\\\{\\}"), out);
  EXPECT_EQ(".*+?|()[]^$\\{}", irs::ViewCast<char>(result));
}

TEST_F(RegexpUtilsTest, unescape_mixed_literals_and_escapes) {
  irs::bstring out;
  auto result = irs::UnescapeRegexp(ToBytesView("foo\\.bar\\*baz"), out);
  EXPECT_EQ("foo.bar*baz", irs::ViewCast<char>(result));
}

TEST_F(RegexpUtilsTest, unescape_trailing_backslash_kept) {
  // Documented behavior: lone trailing backslash is appended to output.
  // Not a valid regexp on its own, but UnescapeRegexp must be robust.
  irs::bstring out;
  auto result = irs::UnescapeRegexp(ToBytesView("foo\\"), out);
  EXPECT_EQ("foo\\", irs::ViewCast<char>(result));
}

TEST_F(RegexpUtilsTest, unescape_clears_output_buffer) {
  // The function must clear() out before writing - callers may reuse
  // a buffer across multiple patterns.
  irs::bstring out;
  out.assign(ToBytesView("stale-data"));
  auto result = irs::UnescapeRegexp(ToBytesView("abc"), out);
  EXPECT_EQ("abc", irs::ViewCast<char>(result));
  EXPECT_EQ(3u, out.size());
}

// ComputeRegexpType - backslash corner cases
//
// The classifier has subtle interactions with escape sequences.
// These tests anchor the current behavior so changes are intentional.

TEST_F(RegexpUtilsTest, regexp_type_single_escape_only) {
  // "\." on wire - one literal dot -> LiteralEscaped
  EXPECT_EQ(irs::RegexpType::LiteralEscaped,
            irs::ComputeRegexpType(ToBytesView("\\.")));
}

TEST_F(RegexpUtilsTest, regexp_type_literal_backslash) {
  // "\\" on wire (2 bytes: \, \) = one literal backslash -> LiteralEscaped
  EXPECT_EQ(irs::RegexpType::LiteralEscaped,
            irs::ComputeRegexpType(ToBytesView("\\\\")));
}

TEST_F(RegexpUtilsTest, regexp_type_two_literal_backslashes) {
  // "\\\\" on wire (4 bytes) = two literal backslashes -> LiteralEscaped
  EXPECT_EQ(irs::RegexpType::LiteralEscaped,
            irs::ComputeRegexpType(ToBytesView("\\\\\\\\")));
}

TEST_F(RegexpUtilsTest, regexp_type_trailing_backslash) {
  // "foo\" on wire - trailing lone backslash. HasMetacharacters sees
  // the \, sets escaped=true, loop ends - no unescaped metacharacter
  // was returned as true. HasEscapes returns true -> LiteralEscaped.
  EXPECT_EQ(irs::RegexpType::LiteralEscaped,
            irs::ComputeRegexpType(ToBytesView("foo\\")));
}

TEST_F(RegexpUtilsTest, regexp_type_escaped_dot_then_star) {
  // "\.*" on wire = literal dot, then unescaped *. The * is a
  // metacharacter but not part of a .* tail (no preceding unescaped
  // dot) -> Complex.
  EXPECT_EQ(irs::RegexpType::Complex,
            irs::ComputeRegexpType(ToBytesView("\\.*")));
}

TEST_F(RegexpUtilsTest, regexp_type_escaped_backslash_plus_dotstar) {
  // "\\.*" on wire (4 bytes: \, \, ., *) = one literal backslash,
  // then .* -> PrefixEscaped with prefix "\\" (one backslash literal).
  EXPECT_EQ(irs::RegexpType::PrefixEscaped,
            irs::ComputeRegexpType(ToBytesView("\\\\.*")));
}

TEST_F(RegexpUtilsTest, regexp_type_escaped_prefix_with_dotstar) {
  // "\.foo.*" on wire = \., then foo, then .* -> PrefixEscaped
  EXPECT_EQ(irs::RegexpType::PrefixEscaped,
            irs::ComputeRegexpType(ToBytesView("\\.foo.*")));
}

TEST_F(RegexpUtilsTest, regexp_type_perl_escape_sequences) {
  // \d, \w, \s, \b, \p{...} - all change matching semantics and
  // must route to the full acceptor path (Complex).
  EXPECT_EQ(irs::RegexpType::Complex,
            irs::ComputeRegexpType(ToBytesView("\\d")));
  EXPECT_EQ(irs::RegexpType::Complex,
            irs::ComputeRegexpType(ToBytesView("\\w+")));
  EXPECT_EQ(irs::RegexpType::Complex,
            irs::ComputeRegexpType(ToBytesView("\\s")));
  EXPECT_EQ(irs::RegexpType::Complex,
            irs::ComputeRegexpType(ToBytesView("\\bfoo")));
  EXPECT_EQ(irs::RegexpType::Complex,
            irs::ComputeRegexpType(ToBytesView("\\p{Cyrillic}")));
}

TEST_F(RegexpUtilsTest, regexp_type_cstyle_escape) {
  // These change matching semantics, so they route to the full acceptor.
  EXPECT_EQ(irs::RegexpType::Complex,
            irs::ComputeRegexpType(ToBytesView("\\n")));
  EXPECT_EQ(irs::RegexpType::Complex,
            irs::ComputeRegexpType(ToBytesView("\\t")));
  EXPECT_EQ(irs::RegexpType::Complex,
            irs::ComputeRegexpType(ToBytesView("foo\\n")));
}

// Patterns that look prefix-shaped but are not

TEST_F(RegexpUtilsTest, regexp_type_prefix_two_dotstars) {
  // "a.*.*" - has unescaped . in the middle, not just the final .*
  // -> Complex (not Prefix)
  EXPECT_EQ(irs::RegexpType::Complex,
            irs::ComputeRegexpType(ToBytesView("a.*.*")));
}

TEST_F(RegexpUtilsTest, regexp_type_prefix_star_before_dotstar) {
  // "abc*.*" - has unescaped * before .*, so the "prefix" would contain
  // metacharacters -> Complex.
  EXPECT_EQ(irs::RegexpType::Complex,
            irs::ComputeRegexpType(ToBytesView("abc*.*")));
}

TEST_F(RegexpUtilsTest, regexp_type_prefix_inner_dot) {
  // "a.b.*" - unescaped dot in prefix position -> Complex
  EXPECT_EQ(irs::RegexpType::Complex,
            irs::ComputeRegexpType(ToBytesView("a.b.*")));
}

TEST_F(RegexpUtilsTest, regexp_type_prefix_only_dot_no_star) {
  // "foo." - literal prefix with trailing unescaped dot, no .* tail
  // -> Complex (not Prefix, because there's no trailing *)
  EXPECT_EQ(irs::RegexpType::Complex,
            irs::ComputeRegexpType(ToBytesView("foo.")));
}

TEST_F(RegexpUtilsTest, regexp_type_prefix_only_star_no_dot) {
  // "foo*" - ends in *, but * preceded by 'o' (literal), not '.'
  // -> Complex
  EXPECT_EQ(irs::RegexpType::Complex,
            irs::ComputeRegexpType(ToBytesView("foo*")));
}

TEST_F(RegexpUtilsTest, regexp_type_prefix_empty_prefix_dotstar) {
  EXPECT_EQ(irs::RegexpType::Prefix, irs::ComputeRegexpType(ToBytesView(".*")));
}

TEST_F(RegexpUtilsTest, regexp_type_prefix_escaped_dotstar_in_middle) {
  // "a\.*" - escaped dot, then *. The * is applied to the literal dot,
  // so the pattern is "a, then zero-or-more dots" -> Complex.
  EXPECT_EQ(irs::RegexpType::Complex,
            irs::ComputeRegexpType(ToBytesView("a\\.*")));
}

// UTF-8: a multi-byte literal prefix must still be recognised as one, or
// prefix searches silently lose the fast path.

TEST_F(RegexpUtilsTest, regexp_type_utf8_cyrillic_prefix) {
  EXPECT_EQ(irs::RegexpType::Prefix,
            irs::ComputeRegexpType(ToBytesView("привет.*")));
}

TEST_F(RegexpUtilsTest, regexp_type_utf8_chinese_prefix) {
  EXPECT_EQ(irs::RegexpType::Prefix,
            irs::ComputeRegexpType(ToBytesView("中文.*")));
}

TEST_F(RegexpUtilsTest, regexp_type_utf8_emoji_prefix) {
  EXPECT_EQ(irs::RegexpType::Prefix,
            irs::ComputeRegexpType(ToBytesView("😀.*")));
}

TEST_F(RegexpUtilsTest, regexp_type_utf8_mixed_prefix) {
  EXPECT_EQ(irs::RegexpType::Prefix,
            irs::ComputeRegexpType(ToBytesView("aб中😀.*")));
}

TEST_F(RegexpUtilsTest, regexp_type_utf8_literal) {
  // UTF-8 string with no metacharacters at all -> Literal
  EXPECT_EQ(irs::RegexpType::Literal,
            irs::ComputeRegexpType(ToBytesView("привет")));
  EXPECT_EQ(irs::RegexpType::Literal,
            irs::ComputeRegexpType(ToBytesView("中文")));
  EXPECT_EQ(irs::RegexpType::Literal,
            irs::ComputeRegexpType(ToBytesView("😀")));
}

TEST_F(RegexpUtilsTest, extract_prefix_utf8) {
  EXPECT_EQ("привет", irs::ViewCast<char>(
                        irs::ExtractRegexpPrefix(ToBytesView("привет.*"))));
  EXPECT_EQ("中文", irs::ViewCast<char>(
                      irs::ExtractRegexpPrefix(ToBytesView("中文.*"))));
}

// The classifier and the acceptor have to agree about which dialect a pattern
// is in: a Complex pattern is the one handed to the acceptor, so it must
// compile, and the two syntaxes must not be silently interchangeable.

TEST_F(RegexpUtilsTest, complex_patterns_compile) {
  constexpr std::string_view kComplex[]{
    "fo+", "a|b", ".*foo", "a.*b", "[abc]", "a?b", "(ab)+", "\\d", "\\w+",
  };
  for (const auto pattern : kComplex) {
    SCOPED_TRACE(testing::Message("Pattern: '") << pattern << "'");
    ASSERT_EQ(irs::RegexpType::Complex,
              irs::ComputeRegexpType(ToBytesView(pattern)));
    const irs::RegexpAcceptor acceptor{ToBytesView(pattern)};
    EXPECT_TRUE(acceptor.ok());
  }
}

TEST_F(RegexpUtilsTest, unparsable_pattern_is_not_ok) {
  // An acceptor that could not compile accepts nothing, and says so rather
  // than accepting everything.
  const irs::RegexpAcceptor acceptor{ToBytesView("(")};
  EXPECT_FALSE(acceptor.ok());
  EXPECT_FALSE(Accepts(acceptor, "("));
  EXPECT_FALSE(Accepts(acceptor, ""));
}

TEST_F(RegexpUtilsTest, perl_and_posix_syntaxes_differ) {
  // A Perl class is a class under Perl syntax and is not part of POSIX ERE at
  // all, so the same pattern compiles in one dialect and is rejected by the
  // other -- which is why the syntax travels with the pattern rather than being
  // a global default.
  const irs::RegexpAcceptor perl{ToBytesView("\\d"), irs::RegexpSyntax::Perl};
  ASSERT_TRUE(perl.ok());
  EXPECT_TRUE(Accepts(perl, "7"));
  EXPECT_FALSE(Accepts(perl, "d"));

  const irs::RegexpAcceptor posix{ToBytesView("\\d"),
                                  irs::RegexpSyntax::PosixEre};
  EXPECT_FALSE(posix.ok());

  // What both dialects share still means the same thing in both.
  for (const auto syntax :
       {irs::RegexpSyntax::Perl, irs::RegexpSyntax::PosixEre}) {
    const irs::RegexpAcceptor a{ToBytesView("ab+c"), syntax};
    ASSERT_TRUE(a.ok());
    EXPECT_TRUE(Accepts(a, "abc"));
    EXPECT_TRUE(Accepts(a, "abbc"));
    EXPECT_FALSE(Accepts(a, "ac"));
  }
}

// The wildcard dialect takes every literal byte as itself, so it expresses
// patterns over bytes a regexp source string could not carry.

TEST_F(RegexpUtilsTest, wildcard_dialect) {
  const irs::RegexpAcceptor prefix{irs::RegexpAcceptor::WildcardTag{},
                                   ToBytesView("foo%")};
  ASSERT_TRUE(prefix.ok());
  EXPECT_TRUE(Accepts(prefix, "foo"));
  EXPECT_TRUE(Accepts(prefix, "foobar"));
  EXPECT_FALSE(Accepts(prefix, "fo"));

  const irs::RegexpAcceptor single{irs::RegexpAcceptor::WildcardTag{},
                                   ToBytesView("f_o")};
  ASSERT_TRUE(single.ok());
  EXPECT_TRUE(Accepts(single, "foo"));
  EXPECT_TRUE(Accepts(single, "fxo"));
  EXPECT_FALSE(Accepts(single, "fo"));
  EXPECT_FALSE(Accepts(single, "fxxo"));

  // A regexp metacharacter is a literal byte here.
  const irs::RegexpAcceptor literal_dot{irs::RegexpAcceptor::WildcardTag{},
                                        ToBytesView("a.c")};
  ASSERT_TRUE(literal_dot.ok());
  EXPECT_TRUE(Accepts(literal_dot, "a.c"));
  EXPECT_FALSE(Accepts(literal_dot, "abc"));
}

// UTF-8 acceptance across every byte tier.
//
// The tree the acceptor is built from narrows every partial character class to
// the strict UTF-8 model, range by range, and the compiler expands each range
// into byte sequences. A mistake there is invisible to a pattern that only ever
// sees ASCII, so each tier -- and each boundary between tiers -- is pinned.

TEST_F(RegexpUtilsTest, match_utf8_literal) {
  auto a = FromPerl("привет");
  ASSERT_TRUE(a.ok());
  EXPECT_TRUE(Accepts(a, "привет"));
  EXPECT_FALSE(Accepts(a, "приветы"));
  EXPECT_FALSE(Accepts(a, "привет!"));
}

TEST_F(RegexpUtilsTest, match_utf8_prefix) {
  auto a = FromPerl("при.*");
  ASSERT_TRUE(a.ok());
  EXPECT_TRUE(Accepts(a, "привет"));
  EXPECT_TRUE(Accepts(a, "приветствую"));
  EXPECT_TRUE(Accepts(a, "при"));
  EXPECT_FALSE(Accepts(a, "пока"));
}

TEST_F(RegexpUtilsTest, match_utf8_dot) {
  auto a = FromPerl("пр.вет");
  ASSERT_TRUE(a.ok());
  EXPECT_TRUE(Accepts(a, "привет"));
  EXPECT_TRUE(Accepts(a, "прXвет"));
}

TEST_F(RegexpUtilsTest, match_utf8_alternation) {
  auto a = FromPerl("да|нет");
  ASSERT_TRUE(a.ok());
  EXPECT_TRUE(Accepts(a, "да"));
  EXPECT_TRUE(Accepts(a, "нет"));
  EXPECT_FALSE(Accepts(a, "может"));
}

TEST_F(RegexpUtilsTest, match_utf8_quantifiers) {
  {
    auto a = FromPerl("а+");
    ASSERT_TRUE(a.ok());
    EXPECT_TRUE(Accepts(a, "а"));
    EXPECT_TRUE(Accepts(a, "ааа"));
    EXPECT_FALSE(Accepts(a, ""));
  }
  {
    auto a = FromPerl("ха?");
    ASSERT_TRUE(a.ok());
    EXPECT_TRUE(Accepts(a, "х"));
    EXPECT_TRUE(Accepts(a, "ха"));
    EXPECT_FALSE(Accepts(a, "хаа"));
  }
}

TEST_F(RegexpUtilsTest, match_utf8_range) {
  auto a = FromPerl("[а-г]+");
  ASSERT_TRUE(a.ok());
  EXPECT_TRUE(Accepts(a, "абвг"));
  EXPECT_TRUE(Accepts(a, "ааа"));
  EXPECT_FALSE(Accepts(a, "дежз"));
  EXPECT_FALSE(Accepts(a, "abc"));
}

TEST_F(RegexpUtilsTest, match_utf8_mixed) {
  auto a = FromPerl("hello.*мир");
  ASSERT_TRUE(a.ok());
  EXPECT_TRUE(Accepts(a, "helloмир"));
  EXPECT_TRUE(Accepts(a, "hello мир"));
  EXPECT_TRUE(Accepts(a, "hello, мир"));
  EXPECT_FALSE(Accepts(a, "hello"));
}

TEST_F(RegexpUtilsTest, match_utf8_3byte_literal) {
  // Chinese characters: 中 = E4 B8 AD, 文 = E6 96 87, 字 = E5 AD 97
  auto a = FromPerl("中文");
  ASSERT_TRUE(a.ok());
  EXPECT_TRUE(Accepts(a, "中文"));
  EXPECT_FALSE(Accepts(a, "中"));
  EXPECT_FALSE(Accepts(a, "文"));
  EXPECT_FALSE(Accepts(a, "中文字"));
  EXPECT_FALSE(Accepts(a, ""));
}

TEST_F(RegexpUtilsTest, match_utf8_3byte_dot) {
  auto a = FromPerl("中.字");
  ASSERT_TRUE(a.ok());
  EXPECT_TRUE(Accepts(a, "中文字"));
  EXPECT_TRUE(Accepts(a, "中X字"));
  EXPECT_TRUE(Accepts(a, "中国字"));  // 国 is also 3-byte
  EXPECT_FALSE(Accepts(a, "中字"));
  EXPECT_FALSE(Accepts(a, "中文文字"));
}

TEST_F(RegexpUtilsTest, match_utf8_3byte_quantifiers) {
  {
    auto a = FromPerl("中*");
    ASSERT_TRUE(a.ok());
    EXPECT_TRUE(Accepts(a, ""));
    EXPECT_TRUE(Accepts(a, "中"));
    EXPECT_TRUE(Accepts(a, "中中中"));
    EXPECT_FALSE(Accepts(a, "中文"));
  }
  {
    auto a = FromPerl("文+");
    ASSERT_TRUE(a.ok());
    EXPECT_FALSE(Accepts(a, ""));
    EXPECT_TRUE(Accepts(a, "文"));
    EXPECT_TRUE(Accepts(a, "文文文"));
  }
  {
    auto a = FromPerl("中文?字");
    ASSERT_TRUE(a.ok());
    EXPECT_TRUE(Accepts(a, "中字"));
    EXPECT_TRUE(Accepts(a, "中文字"));
    EXPECT_FALSE(Accepts(a, "中文文字"));
  }
}

TEST_F(RegexpUtilsTest, match_utf8_3byte_prefix_suffix) {
  {
    auto a = FromPerl("中.*");
    ASSERT_TRUE(a.ok());
    EXPECT_TRUE(Accepts(a, "中"));
    EXPECT_TRUE(Accepts(a, "中文"));
    EXPECT_TRUE(Accepts(a, "中文字"));
    EXPECT_TRUE(Accepts(a, "中abc"));
    EXPECT_FALSE(Accepts(a, "文中"));
  }
  {
    auto a = FromPerl(".*字");
    ASSERT_TRUE(a.ok());
    EXPECT_TRUE(Accepts(a, "字"));
    EXPECT_TRUE(Accepts(a, "文字"));
    EXPECT_TRUE(Accepts(a, "中文字"));
    EXPECT_TRUE(Accepts(a, "abc字"));
    EXPECT_FALSE(Accepts(a, "字中"));
  }
  {
    auto a = FromPerl(".*文.*");
    ASSERT_TRUE(a.ok());
    EXPECT_TRUE(Accepts(a, "文"));
    EXPECT_TRUE(Accepts(a, "中文"));
    EXPECT_TRUE(Accepts(a, "文字"));
    EXPECT_TRUE(Accepts(a, "中文字"));
    EXPECT_FALSE(Accepts(a, "中字"));
  }
}

TEST_F(RegexpUtilsTest, match_utf8_3byte_alternation) {
  auto a = FromPerl("中国|日本|韓国");
  ASSERT_TRUE(a.ok());
  EXPECT_TRUE(Accepts(a, "中国"));
  EXPECT_TRUE(Accepts(a, "日本"));
  EXPECT_TRUE(Accepts(a, "韓国"));
  EXPECT_FALSE(Accepts(a, "中"));
  EXPECT_FALSE(Accepts(a, "国"));
  EXPECT_FALSE(Accepts(a, "中日韓"));
}

TEST_F(RegexpUtilsTest, match_utf8_3byte_char_class) {
  auto a = FromPerl("[中文字]+");
  ASSERT_TRUE(a.ok());
  EXPECT_TRUE(Accepts(a, "中"));
  EXPECT_TRUE(Accepts(a, "文"));
  EXPECT_TRUE(Accepts(a, "字"));
  EXPECT_TRUE(Accepts(a, "中文字"));
  EXPECT_TRUE(Accepts(a, "字文中文字"));
  EXPECT_FALSE(Accepts(a, ""));
  EXPECT_FALSE(Accepts(a, "中abc"));
}

TEST_F(RegexpUtilsTest, match_utf8_3byte_range) {
  // Range of CJK characters: 一 (U+4E00) to 三 (U+4E09)
  auto a = FromPerl("[一-三]+");
  ASSERT_TRUE(a.ok());
  EXPECT_TRUE(Accepts(a, "一"));
  EXPECT_TRUE(Accepts(a, "三"));
  EXPECT_TRUE(Accepts(a, "一一一"));
}

TEST_F(RegexpUtilsTest, match_utf8_4byte_literal) {
  // Emoji: 😀 = F0 9F 98 80 (U+1F600)
  auto a = FromPerl("😀");
  ASSERT_TRUE(a.ok());
  EXPECT_TRUE(Accepts(a, "😀"));
  EXPECT_FALSE(Accepts(a, "😀😀"));
  EXPECT_FALSE(Accepts(a, ""));
  EXPECT_FALSE(Accepts(a, "X"));
}

TEST_F(RegexpUtilsTest, match_utf8_4byte_multiple) {
  auto a = FromPerl("😀🎉");
  ASSERT_TRUE(a.ok());
  EXPECT_TRUE(Accepts(a, "😀🎉"));
  EXPECT_FALSE(Accepts(a, "😀"));
  EXPECT_FALSE(Accepts(a, "🎉"));
  EXPECT_FALSE(Accepts(a, "🎉😀"));
}

TEST_F(RegexpUtilsTest, match_utf8_4byte_dot) {
  auto a = FromPerl("a.b");
  ASSERT_TRUE(a.ok());
  EXPECT_TRUE(Accepts(a, "a😀b"));
  EXPECT_TRUE(Accepts(a, "a🎉b"));
  EXPECT_TRUE(Accepts(a, "aXb"));
  EXPECT_FALSE(Accepts(a, "ab"));
  EXPECT_FALSE(Accepts(a, "a😀😀b"));
}

TEST_F(RegexpUtilsTest, match_utf8_4byte_quantifiers) {
  {
    auto a = FromPerl("😀*");
    ASSERT_TRUE(a.ok());
    EXPECT_TRUE(Accepts(a, ""));
    EXPECT_TRUE(Accepts(a, "😀"));
    EXPECT_TRUE(Accepts(a, "😀😀😀"));
    EXPECT_FALSE(Accepts(a, "😀🎉"));
  }
  {
    auto a = FromPerl("🎉+");
    ASSERT_TRUE(a.ok());
    EXPECT_FALSE(Accepts(a, ""));
    EXPECT_TRUE(Accepts(a, "🎉"));
    EXPECT_TRUE(Accepts(a, "🎉🎉🎉"));
  }
  {
    auto a = FromPerl("a😀?b");
    ASSERT_TRUE(a.ok());
    EXPECT_TRUE(Accepts(a, "ab"));
    EXPECT_TRUE(Accepts(a, "a😀b"));
    EXPECT_FALSE(Accepts(a, "a😀😀b"));
  }
}

TEST_F(RegexpUtilsTest, match_utf8_4byte_prefix_suffix) {
  {
    auto a = FromPerl("😀.*");
    ASSERT_TRUE(a.ok());
    EXPECT_TRUE(Accepts(a, "😀"));
    EXPECT_TRUE(Accepts(a, "😀hello"));
    EXPECT_TRUE(Accepts(a, "😀🎉🚀"));
    EXPECT_FALSE(Accepts(a, "hello😀"));
  }
  {
    auto a = FromPerl(".*🎉");
    ASSERT_TRUE(a.ok());
    EXPECT_TRUE(Accepts(a, "🎉"));
    EXPECT_TRUE(Accepts(a, "hello🎉"));
    EXPECT_TRUE(Accepts(a, "😀🎉"));
    EXPECT_FALSE(Accepts(a, "🎉hello"));
  }
}

TEST_F(RegexpUtilsTest, match_utf8_4byte_alternation) {
  auto a = FromPerl("😀|🎉|🚀");
  ASSERT_TRUE(a.ok());
  EXPECT_TRUE(Accepts(a, "😀"));
  EXPECT_TRUE(Accepts(a, "🎉"));
  EXPECT_TRUE(Accepts(a, "🚀"));
  EXPECT_FALSE(Accepts(a, "X"));
  EXPECT_FALSE(Accepts(a, "😀🎉"));
}

TEST_F(RegexpUtilsTest, match_utf8_4byte_char_class) {
  auto a = FromPerl("[😀🎉🚀]+");
  ASSERT_TRUE(a.ok());
  EXPECT_TRUE(Accepts(a, "😀"));
  EXPECT_TRUE(Accepts(a, "🎉"));
  EXPECT_TRUE(Accepts(a, "🚀"));
  EXPECT_TRUE(Accepts(a, "😀🎉🚀"));
  EXPECT_TRUE(Accepts(a, "🎉🎉🎉"));
  EXPECT_FALSE(Accepts(a, ""));
  EXPECT_FALSE(Accepts(a, "😀X"));
}

TEST_F(RegexpUtilsTest, match_utf8_4byte_rare_cjk) {
  // Rare CJK: 𠀀 = F0 A0 80 80 (U+20000), 𠀁 = F0 A0 80 81 (U+20001)
  auto a = FromPerl("𠀀𠀁");
  ASSERT_TRUE(a.ok());
  EXPECT_TRUE(Accepts(a, "𠀀𠀁"));
  EXPECT_FALSE(Accepts(a, "𠀀"));
  EXPECT_FALSE(Accepts(a, "𠀁"));
}

TEST_F(RegexpUtilsTest, match_utf8_mixed_all_lengths) {
  // a (1 byte) + б (2 bytes) + 中 (3 bytes) + 😀 (4 bytes)
  auto a = FromPerl("aб中😀");
  ASSERT_TRUE(a.ok());
  EXPECT_TRUE(Accepts(a, "aб中😀"));
  EXPECT_FALSE(Accepts(a, "aб中"));
  EXPECT_FALSE(Accepts(a, "б中😀"));
}

TEST_F(RegexpUtilsTest, match_utf8_mixed_dot_any_length) {
  // Dot matches any single code point regardless of byte length.
  auto a = FromPerl("....");
  ASSERT_TRUE(a.ok());
  EXPECT_TRUE(Accepts(a, "abcd"));
  EXPECT_TRUE(Accepts(a, "абвг"));
  EXPECT_TRUE(Accepts(a, "中文日本"));
  EXPECT_TRUE(Accepts(a, "😀🎉🚀🌟"));
  EXPECT_TRUE(Accepts(a, "aб中😀"));
  EXPECT_FALSE(Accepts(a, "abc"));
  EXPECT_FALSE(Accepts(a, "abcde"));
}

TEST_F(RegexpUtilsTest, match_utf8_mixed_quantifiers) {
  auto a = FromPerl("при.*中.*😀");
  ASSERT_TRUE(a.ok());
  EXPECT_TRUE(Accepts(a, "при中😀"));
  EXPECT_TRUE(Accepts(a, "приXXX中YYY😀"));
  EXPECT_TRUE(Accepts(a, "при中文字😀"));
  EXPECT_FALSE(Accepts(a, "при中"));
  EXPECT_FALSE(Accepts(a, "中😀"));
}

TEST_F(RegexpUtilsTest, match_utf8_mixed_char_class) {
  auto a = FromPerl("[aбя中😀]+");
  ASSERT_TRUE(a.ok());
  EXPECT_TRUE(Accepts(a, "a"));
  EXPECT_TRUE(Accepts(a, "б"));
  EXPECT_TRUE(Accepts(a, "中"));
  EXPECT_TRUE(Accepts(a, "😀"));
  EXPECT_TRUE(Accepts(a, "aб中😀"));
  EXPECT_TRUE(Accepts(a, "😀中бa"));
  EXPECT_FALSE(Accepts(a, ""));
  EXPECT_FALSE(Accepts(a, "x"));
}

TEST_F(RegexpUtilsTest, match_utf8_mixed_foo_star_bar_with_emoji) {
  auto a = FromPerl("foo.*bar");
  ASSERT_TRUE(a.ok());
  EXPECT_TRUE(Accepts(a, "foo😀bar"));
  EXPECT_TRUE(Accepts(a, "foo🎉🚀🌟bar"));
  EXPECT_TRUE(Accepts(a, "foo中文bar"));
  EXPECT_TRUE(Accepts(a, "fooприветbar"));
  EXPECT_TRUE(Accepts(a, "foo😀中приbar"));
}

// The strict UTF-8 model is what makes a partial class reject an ill-formed
// byte sequence rather than step through it: `.` is a class, so a lone
// continuation byte is not a character it accepts.

TEST_F(RegexpUtilsTest, match_dot_rejects_lone_continuation_byte) {
  auto a = FromPerl(".");
  ASSERT_TRUE(a.ok());
  EXPECT_TRUE(Accepts(a, "a"));
  EXPECT_TRUE(Accepts(a, "\xD0\xB0"));       // а
  EXPECT_FALSE(Accepts(a, "\x80"));          // lone continuation
  EXPECT_FALSE(Accepts(a, "\xBF"));          // lone continuation
  EXPECT_FALSE(Accepts(a, "\xD0"));          // truncated 2-byte lead
  EXPECT_FALSE(Accepts(a, "\xE2\x9E"));      // truncated 3-byte lead
  EXPECT_FALSE(Accepts(a, "\xE2\x9E\x61"));  // 3-byte lead, bad continuation
}

// UTF-8 range boundary-crossing
//
// A rune range is split at the byte-length boundaries (0x80, 0x800, 0x10000)
// and each part expanded on its own; the splitting is what a class spanning
// two tiers depends on.

TEST_F(RegexpUtilsTest, utf8_range_crossing_1_to_2_byte) {
  // [U+0070 .. U+00A0] crosses 0x80 boundary: {1-byte part, 2-byte part}
  auto a = FromPerl("[\\x{70}-\\x{A0}]");
  ASSERT_TRUE(a.ok());
  EXPECT_TRUE(Accepts(a, "p"));          // U+0070 (1-byte)
  EXPECT_TRUE(Accepts(a, "\x7F"));       // U+007F (1-byte, boundary)
  EXPECT_TRUE(Accepts(a, "\xC2\x80"));   // U+0080 (2-byte, boundary)
  EXPECT_TRUE(Accepts(a, "\xC2\xA0"));   // U+00A0 (2-byte)
  EXPECT_FALSE(Accepts(a, "o"));         // U+006F
  EXPECT_FALSE(Accepts(a, "\xC2\xA1"));  // U+00A1
  EXPECT_FALSE(Accepts(a, ""));
}

TEST_F(RegexpUtilsTest, utf8_range_crossing_2_to_3_byte) {
  // [U+07F0 .. U+0810] crosses 0x800 boundary
  auto a = FromPerl("[\\x{7F0}-\\x{810}]");
  ASSERT_TRUE(a.ok());
  EXPECT_TRUE(Accepts(a, "\xDF\xB0"));       // U+07F0 (2-byte)
  EXPECT_TRUE(Accepts(a, "\xDF\xBF"));       // U+07FF (2-byte, boundary)
  EXPECT_TRUE(Accepts(a, "\xE0\xA0\x80"));   // U+0800 (3-byte, boundary)
  EXPECT_TRUE(Accepts(a, "\xE0\xA0\x90"));   // U+0810 (3-byte)
  EXPECT_FALSE(Accepts(a, "\xDF\xAF"));      // U+07EF
  EXPECT_FALSE(Accepts(a, "\xE0\xA0\x91"));  // U+0811
}

TEST_F(RegexpUtilsTest, utf8_range_crossing_3_to_4_byte) {
  // [U+FFFE .. U+10001] crosses 0x10000 boundary
  auto a = FromPerl("[\\x{FFFE}-\\x{10001}]");
  ASSERT_TRUE(a.ok());
  EXPECT_TRUE(Accepts(a, "\xEF\xBF\xBE"));      // U+FFFE (3-byte)
  EXPECT_TRUE(Accepts(a, "\xEF\xBF\xBF"));      // U+FFFF (3-byte, boundary)
  EXPECT_TRUE(Accepts(a, "\xF0\x90\x80\x80"));  // U+10000 (4-byte, boundary)
  EXPECT_TRUE(Accepts(a, "\xF0\x90\x80\x81"));  // U+10001 (4-byte)
  EXPECT_FALSE(Accepts(a, "\xEF\xBF\xBD"));     // U+FFFD
}

TEST_F(RegexpUtilsTest, utf8_range_full_unicode) {
  // [U+0000 .. U+10FFFF] - full Unicode range; all four byte-length parts
  auto a = FromPerl("[\\x{00}-\\x{10FFFF}]");
  ASSERT_TRUE(a.ok());
  EXPECT_TRUE(Accepts(a, "a"));                 // 1-byte
  EXPECT_TRUE(Accepts(a, "\xC3\xA9"));          // é (2-byte)
  EXPECT_TRUE(Accepts(a, "\xE4\xB8\xAD"));      // 中 (3-byte)
  EXPECT_TRUE(Accepts(a, "\xF0\x9F\x98\x80"));  // 😀 (4-byte)
  EXPECT_FALSE(Accepts(a, ""));                 // requires one codepoint
  EXPECT_FALSE(Accepts(a, "ab"));               // only one codepoint
}

TEST_F(RegexpUtilsTest, utf8_range_mixed_ascii_and_unicode) {
  // Char class with both an ASCII range and a Unicode range.
  auto a = FromPerl("[a-z\\x{400}-\\x{4FF}]+");
  ASSERT_TRUE(a.ok());
  EXPECT_TRUE(Accepts(a, "abc"));
  EXPECT_TRUE(Accepts(a, "абв"));     // Cyrillic
  EXPECT_TRUE(Accepts(a, "abcабв"));  // Mixed
  EXPECT_FALSE(Accepts(a, "ABC"));
  EXPECT_FALSE(Accepts(a, ""));
  EXPECT_FALSE(Accepts(a, "a1b"));
}

// Multi-tier character classes: one class combining 1/2/3/4-byte code points,
// where every tier becomes a separate byte-sequence alternative.

TEST_F(RegexpUtilsTest, char_class_all_four_byte_tiers) {
  // a (1-byte) + б (2-byte) + 中 (3-byte) + 😀 (4-byte)
  auto a = FromPerl("[a\xD0\xB1\xE4\xB8\xAD\xF0\x9F\x98\x80]+");
  ASSERT_TRUE(a.ok());
  EXPECT_TRUE(Accepts(a, "a"));
  EXPECT_TRUE(Accepts(a, "\xD0\xB1"));          // б
  EXPECT_TRUE(Accepts(a, "\xE4\xB8\xAD"));      // 中
  EXPECT_TRUE(Accepts(a, "\xF0\x9F\x98\x80"));  // 😀
  EXPECT_TRUE(Accepts(a, "a\xD0\xB1\xE4\xB8\xAD\xF0\x9F\x98\x80"));
  EXPECT_FALSE(Accepts(a, "b"));
  EXPECT_FALSE(Accepts(a, ""));
}

TEST_F(RegexpUtilsTest, char_class_ranges_across_tiers) {
  // Three ranges, one in each byte-length tier:
  //   a-z            (ASCII, 1-byte)
  //   а-я            (Cyrillic lower, 2-byte: U+0430 - U+044F)
  //   U+4E00-U+4FFF  (portion of CJK block, 3-byte)
  auto a = FromPerl("[a-z\xD0\xB0-\xD1\x8F\xE4\xB8\x80-\xE4\xBF\xBF]+");
  ASSERT_TRUE(a.ok());
  EXPECT_TRUE(Accepts(a, "abc"));                       // ASCII
  EXPECT_TRUE(Accepts(a, "\xD0\xBF\xD1\x80\xD0\xB8"));  // при
  EXPECT_TRUE(Accepts(a, "\xE4\xB8\xAD"));              // 中 (U+4E2D)
  EXPECT_TRUE(Accepts(a, "a\xD0\xBF\xE4\xB8\xAD"));     // a + п + 中
  EXPECT_FALSE(Accepts(a, "ABC"));  // uppercase not in range
  // 中文 -- 文 (U+6587) is outside the CJK sub-range
  EXPECT_FALSE(Accepts(a, "\xE4\xB8\xAD\xE6\x96\x87"));
  EXPECT_FALSE(Accepts(a, ""));
}

TEST_F(RegexpUtilsTest, negated_char_class_all_four_tiers) {
  // [^aб中😀]+ - exclude one from each tier
  auto a = FromPerl("[^a\xD0\xB1\xE4\xB8\xAD\xF0\x9F\x98\x80]+");
  ASSERT_TRUE(a.ok());
  EXPECT_TRUE(Accepts(a, "b"));
  EXPECT_TRUE(Accepts(a, "\xD0\xB2"));      // в (different Cyrillic)
  EXPECT_TRUE(Accepts(a, "\xE6\x96\x87"));  // 文 (different CJK)
  EXPECT_FALSE(Accepts(a, "a"));
  EXPECT_FALSE(Accepts(a, "\xD0\xB1"));  // б
}

// C-style escapes
//
// \n, \t, \r, \f, \v are not regexp metacharacters - they are parsed as
// literal control characters, and the classifier routes them to the acceptor
// because they change what the pattern means.

TEST_F(RegexpUtilsTest, match_escape_newline) {
  auto a = FromPerl("a\\nb");
  ASSERT_TRUE(a.ok());
  EXPECT_TRUE(Accepts(a, "a\nb"));
  EXPECT_FALSE(Accepts(a, "ab"));
  EXPECT_FALSE(Accepts(a, "a b"));
}

TEST_F(RegexpUtilsTest, match_escape_tab) {
  auto a = FromPerl("a\\tb");
  ASSERT_TRUE(a.ok());
  EXPECT_TRUE(Accepts(a, "a\tb"));
  EXPECT_FALSE(Accepts(a, "ab"));
  EXPECT_FALSE(Accepts(a, "a b"));
}

TEST_F(RegexpUtilsTest, match_escape_cr_ff_vt) {
  {
    auto a = FromPerl("\\r");
    ASSERT_TRUE(a.ok());
    EXPECT_TRUE(Accepts(a, "\r"));
    EXPECT_FALSE(Accepts(a, "\n"));
  }
  {
    auto a = FromPerl("\\f");
    ASSERT_TRUE(a.ok());
    EXPECT_TRUE(Accepts(a, "\f"));
  }
  {
    auto a = FromPerl("\\v");
    ASSERT_TRUE(a.ok());
    EXPECT_TRUE(Accepts(a, "\v"));
  }
}

// Hex codepoint escapes: \x41 (2-digit) and \x{...} (braced), each producing
// one specific literal rune whatever its byte length.

TEST_F(RegexpUtilsTest, match_escape_hex_two_digit) {
  // \x41 = 'A'
  auto a = FromPerl("\\x41");
  ASSERT_TRUE(a.ok());
  EXPECT_TRUE(Accepts(a, "A"));
  EXPECT_FALSE(Accepts(a, "a"));
  EXPECT_FALSE(Accepts(a, ""));
}

TEST_F(RegexpUtilsTest, match_escape_hex_braced_ascii) {
  // \x{20} = space
  auto a = FromPerl("a\\x{20}b");
  ASSERT_TRUE(a.ok());
  EXPECT_TRUE(Accepts(a, "a b"));
  EXPECT_FALSE(Accepts(a, "ab"));
}

TEST_F(RegexpUtilsTest, match_escape_hex_braced_unicode) {
  // \x{4E2D} = 中 (U+4E2D, 3-byte UTF-8)
  auto a = FromPerl("\\x{4E2D}");
  ASSERT_TRUE(a.ok());
  EXPECT_TRUE(Accepts(a, "\xE4\xB8\xAD"));   // 中
  EXPECT_FALSE(Accepts(a, "\xE6\x96\x87"));  // 文 (different char)
}

TEST_F(RegexpUtilsTest, match_escape_hex_braced_emoji) {
  // \x{1F600} = 😀 (U+1F600, 4-byte UTF-8)
  auto a = FromPerl("\\x{1F600}");
  ASSERT_TRUE(a.ok());
  EXPECT_TRUE(Accepts(a, "\xF0\x9F\x98\x80"));   // 😀
  EXPECT_FALSE(Accepts(a, "\xF0\x9F\x8E\x89"));  // 🎉
}

// Perl-class escapes inside a character class: they union with the rest of the
// class rather than standing alone, which is a different tree shape.

TEST_F(RegexpUtilsTest, match_escape_digit_in_class) {
  auto a = FromPerl("[\\d]+");
  ASSERT_TRUE(a.ok());
  EXPECT_TRUE(Accepts(a, "0"));
  EXPECT_TRUE(Accepts(a, "12345"));
  EXPECT_FALSE(Accepts(a, "abc"));
  EXPECT_FALSE(Accepts(a, ""));
}

TEST_F(RegexpUtilsTest, match_escape_multiple_perl_in_class) {
  // [\d\s] = digits or whitespace
  auto a = FromPerl("[\\d\\s]+");
  ASSERT_TRUE(a.ok());
  EXPECT_TRUE(Accepts(a, "123"));
  EXPECT_TRUE(Accepts(a, "   "));
  EXPECT_TRUE(Accepts(a, "1 2 3"));
  EXPECT_FALSE(Accepts(a, "abc"));
  EXPECT_FALSE(Accepts(a, "a 1"));
}

TEST_F(RegexpUtilsTest, match_escape_negated_perl_in_class) {
  // [^\d] = non-digit
  auto a = FromPerl("[^\\d]+");
  ASSERT_TRUE(a.ok());
  EXPECT_TRUE(Accepts(a, "abc"));
  EXPECT_TRUE(Accepts(a, "   "));
  EXPECT_TRUE(Accepts(a, "!@#"));
  EXPECT_FALSE(Accepts(a, "123"));
  EXPECT_FALSE(Accepts(a, "abc1"));
}

TEST_F(RegexpUtilsTest, match_escape_perl_with_literal_in_class) {
  // [\d_] = digit or underscore
  auto a = FromPerl("[\\d_]+");
  ASSERT_TRUE(a.ok());
  EXPECT_TRUE(Accepts(a, "123"));
  EXPECT_TRUE(Accepts(a, "___"));
  EXPECT_TRUE(Accepts(a, "1_2_3"));
  EXPECT_FALSE(Accepts(a, "abc"));
}

TEST_F(RegexpUtilsTest, match_escape_unicode_property_in_class) {
  // [\p{Cyrillic}_] = Cyrillic letter or underscore
  auto a = FromPerl("[\\p{Cyrillic}_]+");
  ASSERT_TRUE(a.ok());
  EXPECT_TRUE(Accepts(a, "абв"));
  EXPECT_TRUE(Accepts(a, "__"));
  EXPECT_TRUE(Accepts(a, "а_б"));
  EXPECT_FALSE(Accepts(a, "abc"));
}

// Negated Perl classes
//
// The positive \d / \w forms are covered end-to-end elsewhere; only these
// prove the negated forms select the right terms rather than merely compiling.

TEST_F(RegexpUtilsTest, match_perl_class_non_digit) {
  auto a = FromPerl("\\D+");
  ASSERT_TRUE(a.ok());
  EXPECT_TRUE(Accepts(a, "abc"));
  EXPECT_TRUE(Accepts(a, "hello world"));
  EXPECT_TRUE(Accepts(a, "!@#"));
  EXPECT_FALSE(Accepts(a, "123"));
  EXPECT_FALSE(Accepts(a, "abc123"));  // mixed - one digit breaks
  EXPECT_FALSE(Accepts(a, ""));
}

TEST_F(RegexpUtilsTest, match_perl_class_non_word) {
  auto a = FromPerl("\\W+");
  ASSERT_TRUE(a.ok());
  EXPECT_TRUE(Accepts(a, "!@#"));
  EXPECT_TRUE(Accepts(a, "  "));
  EXPECT_TRUE(Accepts(a, "---"));
  EXPECT_FALSE(Accepts(a, "abc"));
  EXPECT_FALSE(Accepts(a, "123"));
  EXPECT_FALSE(Accepts(a, "_"));  // _ is a word char
  EXPECT_FALSE(Accepts(a, ""));
}

TEST_F(RegexpUtilsTest, match_perl_class_non_whitespace) {
  auto a = FromPerl("\\S+");
  ASSERT_TRUE(a.ok());
  EXPECT_TRUE(Accepts(a, "abc"));
  EXPECT_TRUE(Accepts(a, "123"));
  EXPECT_TRUE(Accepts(a, "!@#"));
  EXPECT_FALSE(Accepts(a, "   "));
  EXPECT_FALSE(Accepts(a, "abc def"));  // space breaks
  EXPECT_FALSE(Accepts(a, ""));
}

TEST_F(RegexpUtilsTest, match_perl_class_combined) {
  // \d+\D+\d+ - digits, then non-digits, then digits
  auto a = FromPerl("\\d+\\D+\\d+");
  ASSERT_TRUE(a.ok());
  EXPECT_TRUE(Accepts(a, "12abc34"));
  EXPECT_TRUE(Accepts(a, "1x2"));
  EXPECT_FALSE(Accepts(a, "abc"));
  EXPECT_FALSE(Accepts(a, "123"));
  EXPECT_FALSE(Accepts(a, "12"));
}

// Full-class detection: [\d\D], [\w\W], [\s\S] collapse to "any character",
// which is the one class that is deliberately *not* narrowed to strict UTF-8.

TEST_F(RegexpUtilsTest, full_class_digit_nondigit) {
  auto a = FromPerl("[\\d\\D]+");
  ASSERT_TRUE(a.ok());
  EXPECT_TRUE(Accepts(a, "abc"));
  EXPECT_TRUE(Accepts(a, "123"));
  EXPECT_TRUE(Accepts(a, "!@# $%^"));
  EXPECT_TRUE(Accepts(a, "abc123!@#"));
  EXPECT_FALSE(Accepts(a, ""));
}

TEST_F(RegexpUtilsTest, full_class_word_nonword) {
  auto a = FromPerl("[\\w\\W]+");
  ASSERT_TRUE(a.ok());
  EXPECT_TRUE(Accepts(a, "abc"));
  EXPECT_TRUE(Accepts(a, "!@#"));
  EXPECT_TRUE(Accepts(a, "abc !@#"));
  EXPECT_FALSE(Accepts(a, ""));
}

TEST_F(RegexpUtilsTest, full_class_space_nonspace) {
  auto a = FromPerl("[\\s\\S]+");
  ASSERT_TRUE(a.ok());
  EXPECT_TRUE(Accepts(a, "abc"));
  EXPECT_TRUE(Accepts(a, "   "));
  EXPECT_TRUE(Accepts(a, "abc   def"));
  EXPECT_FALSE(Accepts(a, ""));
}

// Case-folding acceptance
//
// `(?i:)` expands each letter into its whole fold cycle, and a cycle can leave
// the letter's byte tier -- k folds with U+212A, s with U+017F. These were
// written as regression cover for a real arc-ordering bug: nothing but an
// accept/reject assertion catches a fold that emits the right runes in the
// wrong shape.

TEST_F(RegexpUtilsTest, fold_case_single_ascii_lower) {
  auto a = FromPerl("(?i:a)");
  ASSERT_TRUE(a.ok());
  EXPECT_TRUE(Accepts(a, "a"));
  EXPECT_TRUE(Accepts(a, "A"));
  EXPECT_FALSE(Accepts(a, ""));
  EXPECT_FALSE(Accepts(a, "b"));
  EXPECT_FALSE(Accepts(a, "aa"));
}

TEST_F(RegexpUtilsTest, fold_case_single_ascii_upper) {
  // Same fold cycle {a, A}, entered from 'A' instead.
  auto a = FromPerl("(?i:A)");
  ASSERT_TRUE(a.ok());
  EXPECT_TRUE(Accepts(a, "a"));
  EXPECT_TRUE(Accepts(a, "A"));
}

TEST_F(RegexpUtilsTest, fold_case_mixed_input) {
  auto a = FromPerl("(?i:aBc)");
  ASSERT_TRUE(a.ok());
  EXPECT_TRUE(Accepts(a, "abc"));
  EXPECT_TRUE(Accepts(a, "ABC"));
  EXPECT_TRUE(Accepts(a, "aBc"));
  EXPECT_TRUE(Accepts(a, "AbC"));
  EXPECT_FALSE(Accepts(a, "abcd"));
}

TEST_F(RegexpUtilsTest, fold_case_unicode_k_kelvin) {
  // k's fold cycle includes U+212A (Kelvin sign, 3-byte UTF-8: E2 84 AA).
  auto a = FromPerl("(?i:k)");
  ASSERT_TRUE(a.ok());
  EXPECT_TRUE(Accepts(a, "k"));
  EXPECT_TRUE(Accepts(a, "K"));
  EXPECT_TRUE(Accepts(a, "\xE2\x84\xAA"));  // U+212A
  EXPECT_FALSE(Accepts(a, ""));
  EXPECT_FALSE(Accepts(a, "l"));
}

TEST_F(RegexpUtilsTest, fold_case_unicode_s_long) {
  // s's fold cycle includes U+017F (long s, 2-byte UTF-8: C5 BF).
  auto a = FromPerl("(?i:s)");
  ASSERT_TRUE(a.ok());
  EXPECT_TRUE(Accepts(a, "s"));
  EXPECT_TRUE(Accepts(a, "S"));
  EXPECT_TRUE(Accepts(a, "\xC5\xBF"));  // U+017F
  EXPECT_FALSE(Accepts(a, "t"));
}

TEST_F(RegexpUtilsTest, fold_case_multiple_concat) {
  // Two separate fold groups concatenated - a different tree from (?i:abcd).
  auto a = FromPerl("(?i:ab)(?i:cd)");
  ASSERT_TRUE(a.ok());
  EXPECT_TRUE(Accepts(a, "abcd"));
  EXPECT_TRUE(Accepts(a, "ABCD"));
  EXPECT_TRUE(Accepts(a, "AbCd"));
  EXPECT_FALSE(Accepts(a, "abc"));
  EXPECT_FALSE(Accepts(a, ""));
}

TEST_F(RegexpUtilsTest, fold_case_non_alpha_pass_through) {
  // Digits and symbols have no fold cycle.
  auto a = FromPerl("(?i:a1b)");
  ASSERT_TRUE(a.ok());
  EXPECT_TRUE(Accepts(a, "a1b"));
  EXPECT_TRUE(Accepts(a, "A1B"));
  EXPECT_FALSE(Accepts(a, "a2b"));
  EXPECT_FALSE(Accepts(a, "aXb"));
}

TEST_F(RegexpUtilsTest, fold_case_with_quantifier) {
  auto a = FromPerl("(?i:abc)+");
  ASSERT_TRUE(a.ok());
  EXPECT_TRUE(Accepts(a, "abc"));
  EXPECT_TRUE(Accepts(a, "ABC"));
  EXPECT_TRUE(Accepts(a, "abcABC"));
  EXPECT_TRUE(Accepts(a, "ABCabcAbC"));
  EXPECT_FALSE(Accepts(a, ""));
  EXPECT_FALSE(Accepts(a, "ab"));
}

TEST_F(RegexpUtilsTest, fold_case_with_star_quantifier) {
  auto a = FromPerl("(?i:abc)*");
  ASSERT_TRUE(a.ok());
  EXPECT_TRUE(Accepts(a, ""));
  EXPECT_TRUE(Accepts(a, "abc"));
  EXPECT_TRUE(Accepts(a, "ABC"));
  EXPECT_TRUE(Accepts(a, "abcABCaBc"));
  EXPECT_FALSE(Accepts(a, "abd"));
}

TEST_F(RegexpUtilsTest, fold_case_with_alternation) {
  auto a = FromPerl("(?i:foo|bar)");
  ASSERT_TRUE(a.ok());
  EXPECT_TRUE(Accepts(a, "foo"));
  EXPECT_TRUE(Accepts(a, "FOO"));
  EXPECT_TRUE(Accepts(a, "Foo"));
  EXPECT_TRUE(Accepts(a, "bar"));
  EXPECT_TRUE(Accepts(a, "BAR"));
  EXPECT_FALSE(Accepts(a, "baz"));
}

TEST_F(RegexpUtilsTest, fold_case_combined_with_dot_star) {
  auto a = FromPerl("(?i:foo).*(?i:bar)");
  ASSERT_TRUE(a.ok());
  EXPECT_TRUE(Accepts(a, "foobar"));
  EXPECT_TRUE(Accepts(a, "FOObar"));
  EXPECT_TRUE(Accepts(a, "fooBAR"));
  EXPECT_TRUE(Accepts(a, "FooXyZBar"));
  EXPECT_FALSE(Accepts(a, "foo"));
  EXPECT_FALSE(Accepts(a, "bar"));
}

TEST_F(RegexpUtilsTest, fold_case_with_perl_class) {
  // Folding \d is a no-op - digits have no case.
  auto a = FromPerl("(?i:\\d+)");
  ASSERT_TRUE(a.ok());
  EXPECT_TRUE(Accepts(a, "123"));
  EXPECT_FALSE(Accepts(a, "abc"));
}

TEST_F(RegexpUtilsTest, fold_case_cyrillic) {
  // Cyrillic а (U+0430) folds with А (U+0410)
  auto a = FromPerl("(?i:\xD0\xB0)");
  ASSERT_TRUE(a.ok());
  EXPECT_TRUE(Accepts(a, "\xD0\xB0"));   // а
  EXPECT_TRUE(Accepts(a, "\xD0\x90"));   // А
  EXPECT_FALSE(Accepts(a, "\xD0\xB1"));  // б
}

TEST_F(RegexpUtilsTest, fold_case_cyrillic_string) {
  // (?i:мир) - each letter folds independently
  auto a = FromPerl("(?i:\xD0\xBC\xD0\xB8\xD1\x80)");
  ASSERT_TRUE(a.ok());
  EXPECT_TRUE(Accepts(a, "\xD0\xBC\xD0\xB8\xD1\x80"));           // мир
  EXPECT_TRUE(Accepts(a, "\xD0\x9C\xD0\x98\xD0\xA0"));           // МИР
  EXPECT_TRUE(Accepts(a, "\xD0\x9C\xD0\xB8\xD1\x80"));           // Мир
  EXPECT_FALSE(Accepts(a, "\xD0\xBC\xD0\xB8\xD1\x80\xD0\xB0"));  // мира
}

TEST_F(RegexpUtilsTest, fold_case_latin_extended) {
  // Latin ñ (U+00F1) folds with Ñ (U+00D1)
  auto a = FromPerl("(?i:\xC3\xB1)");
  ASSERT_TRUE(a.ok());
  EXPECT_TRUE(Accepts(a, "\xC3\xB1"));  // ñ
  EXPECT_TRUE(Accepts(a, "\xC3\x91"));  // Ñ
  EXPECT_FALSE(Accepts(a, "n"));
}

TEST_F(RegexpUtilsTest, fold_case_mixed_ascii_and_cyrillic) {
  auto a = FromPerl("(?i:aаb)");
  ASSERT_TRUE(a.ok());
  EXPECT_TRUE(Accepts(a, "aаb"));
  EXPECT_TRUE(Accepts(a, "AАB"));
  EXPECT_TRUE(Accepts(a, "aАB"));
  EXPECT_FALSE(Accepts(a, "aбb"));
}

// Inline flag negation and DotNL: a flag applies to the subtree it is written
// in, so turning one off in the middle has to be honoured there and nowhere
// else.

TEST_F(RegexpUtilsTest, flag_negate_case_insensitive) {
  // (?i:a(?-i:b)c) - 'a' and 'c' fold, 'b' does not.
  auto a = FromPerl("(?i:a(?-i:b)c)");
  ASSERT_TRUE(a.ok());
  EXPECT_TRUE(Accepts(a, "abc"));
  EXPECT_TRUE(Accepts(a, "Abc"));
  EXPECT_TRUE(Accepts(a, "abC"));
  EXPECT_TRUE(Accepts(a, "AbC"));
  EXPECT_FALSE(Accepts(a, "aBc"));
  EXPECT_FALSE(Accepts(a, "ABc"));
  EXPECT_FALSE(Accepts(a, "ABC"));
  EXPECT_FALSE(Accepts(a, "xbc"));
}

TEST_F(RegexpUtilsTest, flag_negate_case_insensitive_standalone) {
  auto a = FromPerl("(?-i:abc)");
  ASSERT_TRUE(a.ok());
  EXPECT_TRUE(Accepts(a, "abc"));
  EXPECT_FALSE(Accepts(a, "ABC"));
  EXPECT_FALSE(Accepts(a, "Abc"));
}

TEST_F(RegexpUtilsTest, flag_dot_nl_enabled) {
  // (?s:.) - dot matches newline when DotNL is on
  auto a = FromPerl("(?s:.)");
  ASSERT_TRUE(a.ok());
  EXPECT_TRUE(Accepts(a, "a"));
  EXPECT_TRUE(Accepts(a, "\n"));
  EXPECT_TRUE(Accepts(a, " "));
  EXPECT_FALSE(Accepts(a, ""));
  EXPECT_FALSE(Accepts(a, "ab"));
}

TEST_F(RegexpUtilsTest, flag_dot_nl_disabled_by_default) {
  auto a = FromPerl(".");
  ASSERT_TRUE(a.ok());
  EXPECT_TRUE(Accepts(a, "a"));
  EXPECT_FALSE(Accepts(a, "\n"));  // the default is DotNL off
}

// Unicode properties and negated classes over multi-byte content.

TEST_F(RegexpUtilsTest, perl_unicode_property) {
  auto a = FromPerl("\\p{Cyrillic}+");
  ASSERT_TRUE(a.ok());
  EXPECT_TRUE(Accepts(a, "привет"));
  EXPECT_TRUE(Accepts(a, "абв"));
  EXPECT_FALSE(Accepts(a, "abc"));
  EXPECT_FALSE(Accepts(a, ""));
}

TEST_F(RegexpUtilsTest, negated_unicode_property) {
  auto a = FromPerl("\\P{Cyrillic}+");
  ASSERT_TRUE(a.ok());
  EXPECT_TRUE(Accepts(a, "hello"));
  EXPECT_TRUE(Accepts(a, "123"));
  EXPECT_TRUE(Accepts(a, "中文"));
  EXPECT_TRUE(Accepts(a, "\xC3\xA9"));        // é (Latin, not Cyrillic)
  EXPECT_FALSE(Accepts(a, "\xD0\xB0"));       // а (Cyrillic)
  EXPECT_FALSE(Accepts(a, "hello\xD0\xB0"));  // one Cyrillic char breaks it
}

TEST_F(RegexpUtilsTest, negated_unicode_range) {
  // Anything outside the Cyrillic block.
  auto a = FromPerl("[^\\x{400}-\\x{4FF}]+");
  ASSERT_TRUE(a.ok());
  EXPECT_TRUE(Accepts(a, "hello"));
  EXPECT_TRUE(Accepts(a, "\xE4\xB8\xAD"));      // 中 (3-byte)
  EXPECT_TRUE(Accepts(a, "\xF0\x9F\x98\x80"));  // 😀 (4-byte)
  EXPECT_FALSE(Accepts(a, "\xD0\xB0"));         // а (inside Cyrillic)
}

TEST_F(RegexpUtilsTest, negated_char_class_mixed_ascii_unicode) {
  // [^a-zа-я]+ = not ASCII lowercase and not Cyrillic lowercase
  auto a = FromPerl("[^a-z\xD0\xB0-\xD1\x8F]+");
  ASSERT_TRUE(a.ok());
  EXPECT_TRUE(Accepts(a, "ABC"));
  EXPECT_TRUE(Accepts(a, "123"));
  EXPECT_TRUE(Accepts(a, "\xD0\x90"));   // А (Cyrillic upper) ok
  EXPECT_FALSE(Accepts(a, "abc"));       // lowercase ASCII excluded
  EXPECT_FALSE(Accepts(a, "\xD0\xB0"));  // а (Cyrillic lower) excluded
}

TEST_F(RegexpUtilsTest, negated_single_char) {
  auto a = FromPerl("[^a]");
  ASSERT_TRUE(a.ok());
  EXPECT_TRUE(Accepts(a, "b"));
  EXPECT_TRUE(Accepts(a, "1"));
  EXPECT_TRUE(Accepts(a, " "));
  EXPECT_FALSE(Accepts(a, "a"));
  EXPECT_FALSE(Accepts(a, ""));
  EXPECT_FALSE(Accepts(a, "bb"));
}

// Perl extensions whose whole-term reading the tree rewrite decides.

TEST_F(RegexpUtilsTest, perl_word_boundary_negative_unsupported) {
  // \B cannot be modelled without splitting every state by whether the
  // previous byte was a word character, so it is under-approximated to the
  // empty language: a pattern containing it selects nothing at all.
  auto a = FromPerl("foo\\Bbar");
  EXPECT_FALSE(Accepts(a, "foobar"));
  EXPECT_FALSE(Accepts(a, "foo"));
  EXPECT_FALSE(Accepts(a, "bar"));
  EXPECT_FALSE(Accepts(a, ""));
  EXPECT_FALSE(Accepts(a, "fooXbar"));
}

TEST_F(RegexpUtilsTest, perl_non_capturing_group) {
  auto a = FromPerl("(?:ab)+c");
  ASSERT_TRUE(a.ok());
  EXPECT_TRUE(Accepts(a, "abc"));
  EXPECT_TRUE(Accepts(a, "ababc"));
  EXPECT_FALSE(Accepts(a, "c"));
  EXPECT_FALSE(Accepts(a, "ab"));
}

TEST_F(RegexpUtilsTest, perl_any_byte) {
  // \C matches a single raw byte
  auto a = FromPerl("a\\Cb");
  ASSERT_TRUE(a.ok());
  EXPECT_TRUE(Accepts(a, "aXb"));
  EXPECT_TRUE(Accepts(a, "a1b"));
}

TEST_F(RegexpUtilsTest, perl_counted_quantifiers) {
  {
    auto a = FromPerl("a{3}");
    ASSERT_TRUE(a.ok());
    EXPECT_TRUE(Accepts(a, "aaa"));
    EXPECT_FALSE(Accepts(a, "aa"));
    EXPECT_FALSE(Accepts(a, "aaaa"));
  }
  {
    auto a = FromPerl("a{2,4}");
    ASSERT_TRUE(a.ok());
    EXPECT_FALSE(Accepts(a, "a"));
    EXPECT_TRUE(Accepts(a, "aa"));
    EXPECT_TRUE(Accepts(a, "aaa"));
    EXPECT_TRUE(Accepts(a, "aaaa"));
    EXPECT_FALSE(Accepts(a, "aaaaa"));
  }
  {
    auto a = FromPerl("a{2,}");
    ASSERT_TRUE(a.ok());
    EXPECT_FALSE(Accepts(a, "a"));
    EXPECT_TRUE(Accepts(a, "aa"));
    EXPECT_TRUE(Accepts(a, "aaaaaaaaa"));
  }
}

// POSIX bracket classes, which both dialects share.

TEST_F(RegexpUtilsTest, posix_class_alpha) {
  auto a = FromPerl("[[:alpha:]]+");
  ASSERT_TRUE(a.ok());
  EXPECT_TRUE(Accepts(a, "abc"));
  EXPECT_TRUE(Accepts(a, "ABC"));
  EXPECT_TRUE(Accepts(a, "aBcDeF"));
  EXPECT_FALSE(Accepts(a, "123"));
  EXPECT_FALSE(Accepts(a, "abc123"));
  EXPECT_FALSE(Accepts(a, ""));
}

TEST_F(RegexpUtilsTest, posix_class_digit) {
  auto a = FromPerl("[[:digit:]]+");
  ASSERT_TRUE(a.ok());
  EXPECT_TRUE(Accepts(a, "0"));
  EXPECT_TRUE(Accepts(a, "12345"));
  EXPECT_FALSE(Accepts(a, "abc"));
  EXPECT_FALSE(Accepts(a, "1a2"));
}

TEST_F(RegexpUtilsTest, posix_class_alnum) {
  auto a = FromPerl("[[:alnum:]]+");
  ASSERT_TRUE(a.ok());
  EXPECT_TRUE(Accepts(a, "abc"));
  EXPECT_TRUE(Accepts(a, "123"));
  EXPECT_TRUE(Accepts(a, "abc123"));
  EXPECT_TRUE(Accepts(a, "ABC"));
  EXPECT_FALSE(Accepts(a, "abc_"));  // underscore is not alnum
  EXPECT_FALSE(Accepts(a, " "));
  EXPECT_FALSE(Accepts(a, ""));
}

TEST_F(RegexpUtilsTest, posix_class_space) {
  auto a = FromPerl("[[:space:]]+");
  ASSERT_TRUE(a.ok());
  EXPECT_TRUE(Accepts(a, " "));
  EXPECT_TRUE(Accepts(a, "   "));
  EXPECT_TRUE(Accepts(a, "\t"));
  EXPECT_FALSE(Accepts(a, "abc"));
  EXPECT_FALSE(Accepts(a, ""));
}

TEST_F(RegexpUtilsTest, posix_class_upper_lower) {
  {
    auto a = FromPerl("[[:upper:]]+");
    ASSERT_TRUE(a.ok());
    EXPECT_TRUE(Accepts(a, "ABC"));
    EXPECT_FALSE(Accepts(a, "abc"));
    EXPECT_FALSE(Accepts(a, "Abc"));
  }
  {
    auto a = FromPerl("[[:lower:]]+");
    ASSERT_TRUE(a.ok());
    EXPECT_TRUE(Accepts(a, "abc"));
    EXPECT_FALSE(Accepts(a, "ABC"));
    EXPECT_FALSE(Accepts(a, "aBc"));
  }
}

TEST_F(RegexpUtilsTest, posix_class_negated) {
  auto a = FromPerl("[[:^alpha:]]+");
  ASSERT_TRUE(a.ok());
  EXPECT_TRUE(Accepts(a, "123"));
  EXPECT_TRUE(Accepts(a, "   "));
  EXPECT_TRUE(Accepts(a, "!@#"));
  EXPECT_FALSE(Accepts(a, "abc"));
  EXPECT_FALSE(Accepts(a, "a1"));
}

TEST_F(RegexpUtilsTest, posix_class_combined_with_range) {
  auto a = FromPerl("[[:digit:]_]+");
  ASSERT_TRUE(a.ok());
  EXPECT_TRUE(Accepts(a, "123"));
  EXPECT_TRUE(Accepts(a, "_"));
  EXPECT_TRUE(Accepts(a, "1_2_3"));
  EXPECT_FALSE(Accepts(a, "abc"));
  EXPECT_FALSE(Accepts(a, ""));
}

// POSIX ERE dialect
//
// The POSIX syntax parses without the Perl classes, the Perl boundaries, the
// Perl extensions and the Unicode groups. Core ERE features work; every Perl
// extension is a parse error, i.e. an acceptor of nothing.

TEST_F(RegexpUtilsTest, posix_ere_literal) {
  auto a = FromPosix("foo");
  ASSERT_TRUE(a.ok());
  EXPECT_TRUE(Accepts(a, "foo"));
  EXPECT_FALSE(Accepts(a, "bar"));
  EXPECT_FALSE(Accepts(a, ""));
}

TEST_F(RegexpUtilsTest, posix_ere_basic_metacharacters) {
  {
    auto a = FromPosix("a.b");
    ASSERT_TRUE(a.ok());
    EXPECT_TRUE(Accepts(a, "axb"));
    EXPECT_FALSE(Accepts(a, "ab"));
  }
  {
    auto a = FromPosix("ab*c");
    ASSERT_TRUE(a.ok());
    EXPECT_TRUE(Accepts(a, "ac"));
    EXPECT_TRUE(Accepts(a, "abbbc"));
  }
  {
    auto a = FromPosix("ab+c");
    ASSERT_TRUE(a.ok());
    EXPECT_FALSE(Accepts(a, "ac"));
    EXPECT_TRUE(Accepts(a, "abc"));
    EXPECT_TRUE(Accepts(a, "abbbc"));
  }
  {
    auto a = FromPosix("ab?c");
    ASSERT_TRUE(a.ok());
    EXPECT_TRUE(Accepts(a, "ac"));
    EXPECT_TRUE(Accepts(a, "abc"));
    EXPECT_FALSE(Accepts(a, "abbc"));
  }
}

TEST_F(RegexpUtilsTest, posix_ere_char_class_range) {
  auto a = FromPosix("[a-z]+");
  ASSERT_TRUE(a.ok());
  EXPECT_TRUE(Accepts(a, "abc"));
  EXPECT_TRUE(Accepts(a, "z"));
  EXPECT_FALSE(Accepts(a, "ABC"));
  EXPECT_FALSE(Accepts(a, ""));
}

TEST_F(RegexpUtilsTest, posix_ere_char_class_negated) {
  auto a = FromPosix("[^0-9]+");
  ASSERT_TRUE(a.ok());
  EXPECT_TRUE(Accepts(a, "abc"));
  EXPECT_FALSE(Accepts(a, "123"));
  EXPECT_FALSE(Accepts(a, "a1b"));
}

TEST_F(RegexpUtilsTest, posix_ere_bracket_class_alpha_digit) {
  // The canonical POSIX spelling of what Perl writes \w / \d for.
  {
    auto a = FromPosix("[[:alpha:]]+");
    ASSERT_TRUE(a.ok());
    EXPECT_TRUE(Accepts(a, "abc"));
    EXPECT_TRUE(Accepts(a, "ABC"));
    EXPECT_FALSE(Accepts(a, "123"));
    EXPECT_FALSE(Accepts(a, "abc1"));
  }
  {
    auto a = FromPosix("[[:digit:]]+");
    ASSERT_TRUE(a.ok());
    EXPECT_TRUE(Accepts(a, "123"));
    EXPECT_FALSE(Accepts(a, "abc"));
  }
}

TEST_F(RegexpUtilsTest, posix_ere_alternation_grouping) {
  auto a = FromPosix("(cat|dog)+");
  ASSERT_TRUE(a.ok());
  EXPECT_TRUE(Accepts(a, "cat"));
  EXPECT_TRUE(Accepts(a, "dog"));
  EXPECT_TRUE(Accepts(a, "catdog"));
  EXPECT_TRUE(Accepts(a, "catcatdog"));
  EXPECT_FALSE(Accepts(a, ""));
  EXPECT_FALSE(Accepts(a, "bird"));
}

TEST_F(RegexpUtilsTest, posix_ere_counted_quantifier) {
  {
    auto a = FromPosix("a{3}");
    ASSERT_TRUE(a.ok());
    EXPECT_TRUE(Accepts(a, "aaa"));
    EXPECT_FALSE(Accepts(a, "aa"));
    EXPECT_FALSE(Accepts(a, "aaaa"));
  }
  {
    auto a = FromPosix("a{2,4}");
    ASSERT_TRUE(a.ok());
    EXPECT_TRUE(Accepts(a, "aa"));
    EXPECT_TRUE(Accepts(a, "aaaa"));
    EXPECT_FALSE(Accepts(a, "a"));
    EXPECT_FALSE(Accepts(a, "aaaaa"));
  }
  {
    auto a = FromPosix("a{2,}");
    ASSERT_TRUE(a.ok());
    EXPECT_FALSE(Accepts(a, "a"));
    EXPECT_TRUE(Accepts(a, "aa"));
    EXPECT_TRUE(Accepts(a, "aaaaaaaaa"));
  }
}

TEST_F(RegexpUtilsTest, posix_ere_anchors) {
  // Anchors are no-ops for whole-term matching in both dialects.
  auto a = FromPosix("^foo$");
  ASSERT_TRUE(a.ok());
  EXPECT_TRUE(Accepts(a, "foo"));
  EXPECT_FALSE(Accepts(a, "foobar"));
  EXPECT_FALSE(Accepts(a, "barfoo"));
}

TEST_F(RegexpUtilsTest, posix_ere_dot_star_prefix) {
  auto a = FromPosix("foo.*");
  ASSERT_TRUE(a.ok());
  EXPECT_TRUE(Accepts(a, "foo"));
  EXPECT_TRUE(Accepts(a, "foobar"));
  EXPECT_FALSE(Accepts(a, "fo"));
  EXPECT_FALSE(Accepts(a, "barfoo"));
}

TEST_F(RegexpUtilsTest, posix_ere_utf8_literal) {
  // Only \p{...} group names are gated on the Unicode-groups flag; literal
  // UTF-8 characters pass through whatever the dialect.
  auto a = FromPosix("\xD0\xBF\xD1\x80\xD0\xB8\xD0\xB2\xD0\xB5\xD1\x82");
  ASSERT_TRUE(a.ok());
  EXPECT_TRUE(Accepts(a, "\xD0\xBF\xD1\x80\xD0\xB8\xD0\xB2\xD0\xB5\xD1\x82"));
  EXPECT_FALSE(Accepts(a, ""));
  EXPECT_FALSE(Accepts(a, "hello"));
}

TEST_F(RegexpUtilsTest, posix_ere_stacked_quantifier_allowed) {
  // Stacked quantifiers are rejected only when the Perl extensions are on; in
  // POSIX they parse and squash to a single star. The inverse is anchored by
  // invalid_double_quantifier.
  auto a = FromPosix("a**");
  ASSERT_TRUE(a.ok());
  EXPECT_TRUE(Accepts(a, ""));
  EXPECT_TRUE(Accepts(a, "a"));
  EXPECT_TRUE(Accepts(a, "aaa"));
  EXPECT_FALSE(Accepts(a, "b"));
}

TEST_F(RegexpUtilsTest, posix_ere_perl_equivalence_core_features) {
  // A pattern valid in both dialects has to select the same terms in both:
  // testing each dialect alone would not catch a change that shifted one of
  // them.
  constexpr std::string_view kPatterns[] = {
    "abc",      "a.b",          "ab*c",         "ab+c",         "ab?c",
    "a{2,4}",   "(a|b)+",       "[a-z]+",       "[^0-9]+",      "^foo$",
    "foo.*bar", "[[:alpha:]]+", "[[:digit:]]+", "cat|dog|bird",
  };
  constexpr std::string_view kInputs[] = {
    "",    "a",           "abc", "aaa", "foo", "foobar",
    "123", "hello world", "cat", "dog", "ac",
  };

  for (auto pat : kPatterns) {
    SCOPED_TRACE(testing::Message() << "pattern: " << pat);
    const auto perl = FromPerl(pat);
    const auto posix = FromPosix(pat);
    ASSERT_TRUE(perl.ok());
    ASSERT_TRUE(posix.ok());
    for (auto in : kInputs) {
      EXPECT_EQ(Accepts(perl, in), Accepts(posix, in))
        << "pattern: " << pat << " input: " << in;
    }
  }
}

// POSIX rejects every Perl extension - the acceptor is not ok and selects
// nothing, rather than silently falling back to a different reading.

TEST_F(RegexpUtilsTest, posix_ere_rejects_perl_digit_class) {
  EXPECT_FALSE(FromPosix("\\d+").ok());
}

TEST_F(RegexpUtilsTest, posix_ere_rejects_perl_word_class) {
  EXPECT_FALSE(FromPosix("\\w+").ok());
}

TEST_F(RegexpUtilsTest, posix_ere_rejects_word_boundary) {
  EXPECT_FALSE(FromPosix("\\bfoo").ok());
  EXPECT_FALSE(FromPosix("foo\\B").ok());
}

TEST_F(RegexpUtilsTest, posix_ere_rejects_non_capturing_group) {
  EXPECT_FALSE(FromPosix("(?:ab)+").ok());
}

TEST_F(RegexpUtilsTest, posix_ere_rejects_inline_flag) {
  EXPECT_FALSE(FromPosix("(?i:abc)").ok());
}

TEST_F(RegexpUtilsTest, posix_ere_rejects_literal_quoting) {
  EXPECT_FALSE(FromPosix("\\Q.*\\E").ok());
}

TEST_F(RegexpUtilsTest, posix_ere_rejects_unicode_property) {
  EXPECT_FALSE(FromPosix("\\p{Cyrillic}+").ok());
}

TEST_F(RegexpUtilsTest, posix_ere_rejects_named_capture) {
  EXPECT_FALSE(FromPosix("(?P<w>foo)").ok());
}

TEST_F(RegexpUtilsTest, posix_ere_rejects_any_byte) {
  EXPECT_FALSE(FromPosix("a\\Cb").ok());
}

TEST_F(RegexpUtilsTest, posix_ere_default_is_perl) {
  // Without an explicit syntax the default is Perl, so everything POSIX
  // rejects above has to work.
  {
    auto a = FromPerl("\\d+");
    ASSERT_TRUE(a.ok());
    EXPECT_TRUE(Accepts(a, "123"));
  }
  {
    auto a = FromPerl("(?i:abc)");
    ASSERT_TRUE(a.ok());
    EXPECT_TRUE(Accepts(a, "ABC"));
  }
  {
    auto a = FromPerl("(?:ab)+c");
    ASSERT_TRUE(a.ok());
    EXPECT_TRUE(Accepts(a, "ababc"));
  }
}

// Malformed patterns: each is rejected outright rather than silently becoming
// an accept-all, which would turn a typo into a full-dictionary scan.

TEST_F(RegexpUtilsTest, invalid_unclosed_paren) {
  EXPECT_FALSE(FromPerl("(abc").ok());
}

TEST_F(RegexpUtilsTest, invalid_unexpected_rparen) {
  EXPECT_FALSE(FromPerl("abc)").ok());
}

TEST_F(RegexpUtilsTest, invalid_unclosed_bracket) {
  EXPECT_FALSE(FromPerl("[abc").ok());
}

TEST_F(RegexpUtilsTest, invalid_empty_bracket) {
  EXPECT_FALSE(FromPerl("[]").ok());
}

TEST_F(RegexpUtilsTest, invalid_quantifier_at_start) {
  EXPECT_FALSE(FromPerl("*abc").ok());
  EXPECT_FALSE(FromPerl("+abc").ok());
  EXPECT_FALSE(FromPerl("?abc").ok());
}

TEST_F(RegexpUtilsTest, invalid_trailing_backslash) {
  EXPECT_FALSE(FromPerl("abc\\").ok());
}

TEST_F(RegexpUtilsTest, invalid_range_order) {
  EXPECT_FALSE(FromPerl("[z-a]").ok());
}

TEST_F(RegexpUtilsTest, invalid_double_quantifier) {
  EXPECT_FALSE(FromPerl("a**").ok());
  EXPECT_FALSE(FromPerl("a++").ok());
  EXPECT_FALSE(FromPerl("a?*").ok());
  EXPECT_FALSE(FromPerl("a*+").ok());
}

TEST_F(RegexpUtilsTest, invalid_quantifier_after_pipe) {
  EXPECT_FALSE(FromPerl("a|*").ok());
  EXPECT_FALSE(FromPerl("a|+").ok());
}

TEST_F(RegexpUtilsTest, invalid_quantifier_after_open_paren) {
  EXPECT_FALSE(FromPerl("(*a)").ok());
  EXPECT_FALSE(FromPerl("(+a)").ok());
}

// The memory budget
//
// The budget is the only guard left on how big a pattern may get, and blowing
// it is a *silent* failure: the acceptor degrades into rejecting every key, so
// the query returns an empty result set instead of an error. That makes the
// default's headroom and the exhausted acceptor's behaviour both worth
// pinning.

namespace {

// `[ab]*a[ab]{15}` accepts exactly the {a,b} strings whose 16th character from
// the end is an 'a', and determinizes to tens of thousands of states -- the
// shape the old fixed state cap rejected outright.
std::string BlowupMatch() { return "a" + std::string(15, 'b'); }

}  // namespace

TEST_F(RegexpUtilsTest, max_mem_default_allows_blowup_pattern) {
  auto a = FromPerl("[ab]*a[ab]{15}");
  ASSERT_TRUE(a.ok());
  EXPECT_TRUE(Accepts(a, BlowupMatch()));
  EXPECT_TRUE(Accepts(a, std::string(16, 'a')));
  EXPECT_TRUE(Accepts(a, "b" + BlowupMatch()));
  EXPECT_FALSE(Accepts(a, std::string(16, 'b')));
  EXPECT_FALSE(Accepts(a, std::string(15, 'a')));
}

TEST_F(RegexpUtilsTest, max_mem_too_small_rejects_everything) {
  // A budget that cannot hold the compiled program: the acceptor reports that
  // it is not ok, and accepts nothing at all rather than everything.
  const irs::RegexpAcceptor a{ToBytesView("[abc]{5}"), irs::RegexpSyntax::Perl,
                              /*max_mem=*/1};
  EXPECT_FALSE(a.ok());
  EXPECT_FALSE(Accepts(a, "abcab"));
  EXPECT_FALSE(Accepts(a, ""));
}

TEST_F(RegexpUtilsTest, max_mem_exhausted_by_determinization) {
  // Big enough to be a plausible budget, far too small for this pattern's
  // transition table. Whichever of the two budget checks fires, the result is
  // the same: nothing is selected.
  const irs::RegexpAcceptor a{ToBytesView("[ab]*a[ab]{15}"),
                              irs::RegexpSyntax::Perl, /*max_mem=*/4 << 10};
  EXPECT_FALSE(a.ok());
  EXPECT_FALSE(Accepts(a, BlowupMatch()));
  EXPECT_FALSE(Accepts(a, ""));

  // The same pattern under the default budget does select it, so the rejection
  // above is the budget and not a parse error.
  const auto unlimited = FromPerl("[ab]*a[ab]{15}");
  ASSERT_TRUE(unlimited.ok());
  EXPECT_TRUE(Accepts(unlimited, BlowupMatch()));
}

TEST_F(RegexpUtilsTest, max_mem_generous_budget_builds_normal_pattern) {
  const irs::RegexpAcceptor a{ToBytesView("(a|b)(c|d)(e|f)"),
                              irs::RegexpSyntax::Perl,
                              irs::RegexpAcceptor::kDefaultMaxMem};
  ASSERT_TRUE(a.ok());
  EXPECT_TRUE(Accepts(a, "ace"));
  EXPECT_TRUE(Accepts(a, "bdf"));
  EXPECT_FALSE(Accepts(a, "acg"));
  EXPECT_FALSE(Accepts(a, "ac"));
}

// Large counted quantifiers: the parser expands {n,m} into concatenations, and
// a big count is where that expansion is most likely to go wrong.

TEST_F(RegexpUtilsTest, simplify_large_exact_count) {
  auto a = FromPerl("a{100}");
  ASSERT_TRUE(a.ok());
  EXPECT_TRUE(Accepts(a, std::string(100, 'a')));
  EXPECT_FALSE(Accepts(a, std::string(99, 'a')));
  EXPECT_FALSE(Accepts(a, std::string(101, 'a')));
}

TEST_F(RegexpUtilsTest, simplify_large_bounded_range) {
  auto a = FromPerl("a{50,100}");
  ASSERT_TRUE(a.ok());
  EXPECT_TRUE(Accepts(a, std::string(50, 'a')));
  EXPECT_TRUE(Accepts(a, std::string(75, 'a')));
  EXPECT_TRUE(Accepts(a, std::string(100, 'a')));
  EXPECT_FALSE(Accepts(a, std::string(49, 'a')));
  EXPECT_FALSE(Accepts(a, std::string(101, 'a')));
}

TEST_F(RegexpUtilsTest, simplify_large_open_range) {
  auto a = FromPerl("a{50,}");
  ASSERT_TRUE(a.ok());
  EXPECT_TRUE(Accepts(a, std::string(50, 'a')));
  EXPECT_TRUE(Accepts(a, std::string(100, 'a')));
  EXPECT_FALSE(Accepts(a, std::string(49, 'a')));
}

TEST_F(RegexpUtilsTest, simplify_nested_counted_group) {
  auto a = FromPerl("(ab){3,5}");
  ASSERT_TRUE(a.ok());
  EXPECT_TRUE(Accepts(a, "ababab"));           // 3
  EXPECT_TRUE(Accepts(a, "abababab"));         // 4
  EXPECT_TRUE(Accepts(a, "ababababab"));       // 5
  EXPECT_FALSE(Accepts(a, "abab"));            // 2
  EXPECT_FALSE(Accepts(a, "ababababababab"));  // 7
}

// Assorted real-world shapes.

TEST_F(RegexpUtilsTest, edge_anchors_ignored) {
  {
    auto a = FromPerl("^foo$");
    ASSERT_TRUE(a.ok());
    EXPECT_TRUE(Accepts(a, "foo"));
    EXPECT_FALSE(Accepts(a, "foobar"));
  }
  {
    auto a = FromPerl("^foo");
    ASSERT_TRUE(a.ok());
    EXPECT_TRUE(Accepts(a, "foo"));
  }
}

TEST_F(RegexpUtilsTest, edge_nested_groups_complex) {
  auto a = FromPerl("((a|b)*c)+");
  ASSERT_TRUE(a.ok());
  EXPECT_TRUE(Accepts(a, "c"));
  EXPECT_TRUE(Accepts(a, "ac"));
  EXPECT_TRUE(Accepts(a, "bc"));
  EXPECT_TRUE(Accepts(a, "abababc"));
  EXPECT_TRUE(Accepts(a, "cc"));
  EXPECT_TRUE(Accepts(a, "acbc"));
  EXPECT_FALSE(Accepts(a, ""));
  EXPECT_FALSE(Accepts(a, "ab"));
}

TEST_F(RegexpUtilsTest, edge_email_like_pattern) {
  auto a = FromPerl("[a-z]+@[a-z]+\\.[a-z]+");
  ASSERT_TRUE(a.ok());
  EXPECT_TRUE(Accepts(a, "user@mail.com"));
  EXPECT_TRUE(Accepts(a, "test@example.org"));
  EXPECT_FALSE(Accepts(a, "user@mail"));
  EXPECT_FALSE(Accepts(a, "@mail.com"));
  EXPECT_FALSE(Accepts(a, "user@.com"));
}

TEST_F(RegexpUtilsTest, edge_long_alternation) {
  auto a = FromPerl("a|b|c|d|e|f|g|h|i|j");
  ASSERT_TRUE(a.ok());
  EXPECT_TRUE(Accepts(a, "a"));
  EXPECT_TRUE(Accepts(a, "j"));
  EXPECT_FALSE(Accepts(a, "k"));
  EXPECT_FALSE(Accepts(a, "ab"));
}

TEST_F(RegexpUtilsTest, edge_deeply_nested) {
  auto a = FromPerl("((((a))))");
  ASSERT_TRUE(a.ok());
  EXPECT_TRUE(Accepts(a, "a"));
  EXPECT_FALSE(Accepts(a, ""));
  EXPECT_FALSE(Accepts(a, "aa"));
}

TEST_F(RegexpUtilsTest, edge_mixed_quantifiers) {
  auto a = FromPerl("a+b*c?d");
  ASSERT_TRUE(a.ok());
  EXPECT_TRUE(Accepts(a, "ad"));
  EXPECT_TRUE(Accepts(a, "abd"));
  EXPECT_TRUE(Accepts(a, "acd"));
  EXPECT_TRUE(Accepts(a, "abcd"));
  EXPECT_TRUE(Accepts(a, "aaabbbcd"));
  EXPECT_FALSE(Accepts(a, "d"));
  EXPECT_FALSE(Accepts(a, "abccd"));
}

// StepRun and LiveRange
//
// These are what the dictionary walk drives instead of testing keys one at a
// time: `StepRun` decides a whole block prefix in one pass and `LiveRange`
// tells the walk which of a block's entries can reach the automaton at all.
// Nothing else reaches the chunk/tail split.

TEST_F(RegexpUtilsTest, step_run_consumes_a_self_looping_run) {
  // The start state of `[ab]*c` self-loops on 'a' and 'b'.
  const auto a = FromPerl("[ab]*c");
  ASSERT_TRUE(a.ok());
  const std::string run(20, 'a');
  const auto* p = ToBytesView(run).data();

  irs::RegexpAcceptor::State out{};
  EXPECT_EQ(run.size(), a.StepRun(a.Start(), p, run.size(), out));
  EXPECT_EQ(a.Start(), out);
}

TEST_F(RegexpUtilsTest, step_run_stops_at_the_first_moving_byte) {
  const auto a = FromPerl("[ab]*c");
  ASSERT_TRUE(a.ok());

  // Inside the byte-at-a-time tail (the chunk is 8 bytes).
  {
    const std::string run = "aaaaaaaaac";
    const auto* p = ToBytesView(run).data();
    irs::RegexpAcceptor::State out{};
    EXPECT_EQ(9, a.StepRun(a.Start(), p, run.size(), out));
    EXPECT_EQ(a.Step(a.Start(), 'c'), out);
    EXPECT_TRUE(irs::RegexpAcceptor::Alive(out));
  }
  // Inside a chunk: the fold marks it dirty and the tail re-walks it.
  {
    const std::string run = "aaaaaaaacaaaaaaa";
    const auto* p = ToBytesView(run).data();
    irs::RegexpAcceptor::State out{};
    EXPECT_EQ(8, a.StepRun(a.Start(), p, run.size(), out));
    EXPECT_EQ(a.Step(a.Start(), 'c'), out);
  }
  // A byte that kills the automaton is a move like any other.
  {
    const std::string run = "aaaaaaaax";
    const auto* p = ToBytesView(run).data();
    irs::RegexpAcceptor::State out{};
    EXPECT_EQ(8, a.StepRun(a.Start(), p, run.size(), out));
    EXPECT_FALSE(irs::RegexpAcceptor::Alive(out));
  }
  // An empty run never moves.
  {
    irs::RegexpAcceptor::State out{};
    EXPECT_EQ(0, a.StepRun(a.Start(), nullptr, 0, out));
    EXPECT_EQ(a.Start(), out);
  }
}

TEST_F(RegexpUtilsTest, live_range_hulls_the_live_labels) {
  const auto a = FromPerl("[ab]*c");
  ASSERT_TRUE(a.ok());

  uint32_t lo = 0;
  uint32_t hi = 0;
  ASSERT_TRUE(a.LiveRange(a.Start(), lo, hi));
  EXPECT_EQ('a', lo);
  EXPECT_EQ('c', hi);

  // 0x01 and 'z' are 121 labels apart, so the hull spans most of the alphabet
  // even though only two labels are in it.
  const auto sparse = FromPerl("[\\x01z]");
  ASSERT_TRUE(sparse.ok());
  ASSERT_TRUE(sparse.LiveRange(sparse.Start(), lo, hi));
  EXPECT_EQ(0x01, lo);
  EXPECT_EQ('z', hi);

  // Nothing leaves the state after the whole pattern is matched.
  EXPECT_FALSE(a.LiveRange(a.Step(a.Start(), 'c'), lo, hi));
}
