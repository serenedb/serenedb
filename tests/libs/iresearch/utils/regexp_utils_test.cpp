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

#include <iresearch/utils/automaton_utils.hpp>
#include <iresearch/utils/regexp_utils.hpp>

#include "tests_shared.hpp"

class RegexpUtilsTest : public TestBase {
 protected:
  static void AssertProperties(const irs::automaton& a) {
    constexpr auto kExpectedProperties =
      fst::kILabelSorted | fst::kOLabelSorted | fst::kIDeterministic |
      fst::kAcceptor | fst::kUnweighted;
    EXPECT_EQ(kExpectedProperties, a.Properties(kExpectedProperties, true));
  }

  static bool Accepts(const irs::automaton& a, std::string_view str) {
    return irs::Accept<irs::byte_type>(a, irs::ViewCast<irs::byte_type>(str));
  }

  static irs::bytes_view ToBytesView(std::string_view sv) {
    return irs::ViewCast<irs::byte_type>(sv);
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

// Basic patterns - literals and empty

TEST_F(RegexpUtilsTest, match_empty) {
  // Empty pattern is routed to ByTerm by ComputeRegexpType,
  // never reaches FromRegexp.  Verify classification only.
  ASSERT_EQ(irs::RegexpType::Literal, irs::ComputeRegexpType(ToBytesView("")));
}

TEST_F(RegexpUtilsTest, match_literal) {
  auto a = irs::FromRegexp("foo");
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, "foo"));
  EXPECT_FALSE(Accepts(a, "fo"));
  EXPECT_FALSE(Accepts(a, "fooo"));
  EXPECT_FALSE(Accepts(a, "bar"));
  EXPECT_FALSE(Accepts(a, ""));
  EXPECT_FALSE(Accepts(a, "Foo"));  // case sensitive
}

TEST_F(RegexpUtilsTest, match_single_char) {
  auto a = irs::FromRegexp("a");
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, "a"));
  EXPECT_FALSE(Accepts(a, ""));
  EXPECT_FALSE(Accepts(a, "aa"));
  EXPECT_FALSE(Accepts(a, "b"));
}

// Dot (any single character)

TEST_F(RegexpUtilsTest, match_dot_middle) {
  auto a = irs::FromRegexp("a.c");
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, "abc"));
  EXPECT_TRUE(Accepts(a, "aXc"));
  EXPECT_TRUE(Accepts(a, "a1c"));
  EXPECT_TRUE(Accepts(a, "a c"));    // space
  EXPECT_FALSE(Accepts(a, "ac"));    // no char between
  EXPECT_FALSE(Accepts(a, "abbc"));  // two chars
}

TEST_F(RegexpUtilsTest, match_dot_multiple) {
  auto a = irs::FromRegexp("...");
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, "abc"));
  EXPECT_TRUE(Accepts(a, "123"));
  EXPECT_TRUE(Accepts(a, "   "));
  EXPECT_FALSE(Accepts(a, "ab"));
  EXPECT_FALSE(Accepts(a, "abcd"));
  EXPECT_FALSE(Accepts(a, ""));
}

TEST_F(RegexpUtilsTest, match_dot_single) {
  auto a = irs::FromRegexp(".");
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, "a"));
  EXPECT_TRUE(Accepts(a, "X"));
  EXPECT_TRUE(Accepts(a, "1"));
  EXPECT_FALSE(Accepts(a, ""));
  EXPECT_FALSE(Accepts(a, "ab"));
}

// Star (zero or more)

TEST_F(RegexpUtilsTest, match_star_middle) {
  auto a = irs::FromRegexp("ab*c");
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, "ac"));
  EXPECT_TRUE(Accepts(a, "abc"));
  EXPECT_TRUE(Accepts(a, "abbc"));
  EXPECT_TRUE(Accepts(a, "abbbbbc"));
  EXPECT_FALSE(Accepts(a, "aXc"));
  EXPECT_FALSE(Accepts(a, "ab"));
}

TEST_F(RegexpUtilsTest, match_star_alone) {
  auto a = irs::FromRegexp("a*");
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, ""));
  EXPECT_TRUE(Accepts(a, "a"));
  EXPECT_TRUE(Accepts(a, "aaaa"));
  EXPECT_FALSE(Accepts(a, "b"));
  EXPECT_FALSE(Accepts(a, "ab"));
}

TEST_F(RegexpUtilsTest, match_star_at_end) {
  auto a = irs::FromRegexp("foo*");
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, "fo"));
  EXPECT_TRUE(Accepts(a, "foo"));
  EXPECT_TRUE(Accepts(a, "fooo"));
  EXPECT_FALSE(Accepts(a, "f"));
  EXPECT_FALSE(Accepts(a, "foobar"));
}

// Plus (one or more)

TEST_F(RegexpUtilsTest, match_plus_middle) {
  auto a = irs::FromRegexp("ab+c");
  AssertProperties(a);
  EXPECT_FALSE(Accepts(a, "ac"));
  EXPECT_TRUE(Accepts(a, "abc"));
  EXPECT_TRUE(Accepts(a, "abbc"));
  EXPECT_TRUE(Accepts(a, "abbbbbc"));
}

TEST_F(RegexpUtilsTest, match_plus_alone) {
  auto a = irs::FromRegexp("a+");
  AssertProperties(a);
  EXPECT_FALSE(Accepts(a, ""));
  EXPECT_TRUE(Accepts(a, "a"));
  EXPECT_TRUE(Accepts(a, "aaaa"));
  EXPECT_FALSE(Accepts(a, "b"));
}

TEST_F(RegexpUtilsTest, match_plus_at_end) {
  auto a = irs::FromRegexp("foo+");
  AssertProperties(a);
  EXPECT_FALSE(Accepts(a, "fo"));
  EXPECT_TRUE(Accepts(a, "foo"));
  EXPECT_TRUE(Accepts(a, "fooo"));
  EXPECT_FALSE(Accepts(a, "f"));
}

TEST_F(RegexpUtilsTest, match_dot_plus_alone) {
  auto a = irs::FromRegexp(".+");
  AssertProperties(a);
  EXPECT_FALSE(Accepts(a, ""));
  EXPECT_TRUE(Accepts(a, "a"));
  EXPECT_TRUE(Accepts(a, "abc"));
  EXPECT_TRUE(Accepts(a, "anything"));
}

// Question mark (zero or one)

TEST_F(RegexpUtilsTest, match_question_middle) {
  auto a = irs::FromRegexp("ab?c");
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, "ac"));
  EXPECT_TRUE(Accepts(a, "abc"));
  EXPECT_FALSE(Accepts(a, "abbc"));
}

TEST_F(RegexpUtilsTest, match_question_realistic) {
  auto a = irs::FromRegexp("colou?r");
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, "color"));
  EXPECT_TRUE(Accepts(a, "colour"));
  EXPECT_FALSE(Accepts(a, "colouur"));
}

TEST_F(RegexpUtilsTest, match_question_at_end) {
  auto a = irs::FromRegexp("foo?");
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, "fo"));
  EXPECT_TRUE(Accepts(a, "foo"));
  EXPECT_FALSE(Accepts(a, "fooo"));
  EXPECT_FALSE(Accepts(a, "f"));
}

TEST_F(RegexpUtilsTest, match_question_at_start) {
  auto a = irs::FromRegexp("a?bc");
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, "bc"));
  EXPECT_TRUE(Accepts(a, "abc"));
  EXPECT_FALSE(Accepts(a, "aabc"));
}

TEST_F(RegexpUtilsTest, match_question_multiple) {
  auto a = irs::FromRegexp("a?b?c?");
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, ""));
  EXPECT_TRUE(Accepts(a, "a"));
  EXPECT_TRUE(Accepts(a, "b"));
  EXPECT_TRUE(Accepts(a, "c"));
  EXPECT_TRUE(Accepts(a, "ab"));
  EXPECT_TRUE(Accepts(a, "bc"));
  EXPECT_TRUE(Accepts(a, "ac"));
  EXPECT_TRUE(Accepts(a, "abc"));
  EXPECT_FALSE(Accepts(a, "abcd"));
  EXPECT_FALSE(Accepts(a, "aabbcc"));
}

TEST_F(RegexpUtilsTest, match_question_with_groups) {
  auto a = irs::FromRegexp("(foo)?bar");
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, "bar"));
  EXPECT_TRUE(Accepts(a, "foobar"));
  EXPECT_FALSE(Accepts(a, "foofoobar"));
  EXPECT_FALSE(Accepts(a, "fobar"));
}

// Alternation (pipe)

TEST_F(RegexpUtilsTest, match_alternation_simple) {
  auto a = irs::FromRegexp("a|b");
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, "a"));
  EXPECT_TRUE(Accepts(a, "b"));
  EXPECT_FALSE(Accepts(a, "c"));
  EXPECT_FALSE(Accepts(a, "ab"));
  EXPECT_FALSE(Accepts(a, ""));
}

TEST_F(RegexpUtilsTest, match_alternation_words) {
  auto a = irs::FromRegexp("cat|dog|bird");
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, "cat"));
  EXPECT_TRUE(Accepts(a, "dog"));
  EXPECT_TRUE(Accepts(a, "bird"));
  EXPECT_FALSE(Accepts(a, "fish"));
  EXPECT_FALSE(Accepts(a, "catdog"));
}

TEST_F(RegexpUtilsTest, match_alternation_overlapping) {
  auto a = irs::FromRegexp("foo|foobar");
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, "foo"));
  EXPECT_TRUE(Accepts(a, "foobar"));
  EXPECT_FALSE(Accepts(a, "foob"));
  EXPECT_FALSE(Accepts(a, "fo"));
}

TEST_F(RegexpUtilsTest, match_alternation_with_empty_right) {
  // a| = a or empty string
  auto a = irs::FromRegexp("a|");
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, "a"));
  EXPECT_TRUE(Accepts(a, ""));
  EXPECT_FALSE(Accepts(a, "b"));
}

TEST_F(RegexpUtilsTest, match_alternation_with_empty_left) {
  // |a = empty string or a
  auto a = irs::FromRegexp("|a");
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, ""));
  EXPECT_TRUE(Accepts(a, "a"));
  EXPECT_FALSE(Accepts(a, "b"));
}

TEST_F(RegexpUtilsTest, match_alternation_both_empty) {
  auto a = irs::FromRegexp("|");
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, ""));
  EXPECT_FALSE(Accepts(a, "a"));
}

TEST_F(RegexpUtilsTest, match_alternation_multiple_empty) {
  auto a = irs::FromRegexp("||a||b||");
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, ""));
  EXPECT_TRUE(Accepts(a, "a"));
  EXPECT_TRUE(Accepts(a, "b"));
  EXPECT_FALSE(Accepts(a, "c"));
}

TEST_F(RegexpUtilsTest, match_alternation_with_quantifiers) {
  {
    auto a = irs::FromRegexp("a+|b*");
    AssertProperties(a);
    EXPECT_TRUE(Accepts(a, "a"));
    EXPECT_TRUE(Accepts(a, "aaa"));
    EXPECT_TRUE(Accepts(a, ""));
    EXPECT_TRUE(Accepts(a, "b"));
    EXPECT_TRUE(Accepts(a, "bbb"));
    EXPECT_FALSE(Accepts(a, "ab"));
  }
  {
    auto a = irs::FromRegexp("(a|b)+");
    AssertProperties(a);
    EXPECT_TRUE(Accepts(a, "a"));
    EXPECT_TRUE(Accepts(a, "b"));
    EXPECT_TRUE(Accepts(a, "ababab"));
    EXPECT_TRUE(Accepts(a, "aaabbb"));
    EXPECT_FALSE(Accepts(a, ""));
    EXPECT_FALSE(Accepts(a, "c"));
  }
}

// Grouping

TEST_F(RegexpUtilsTest, match_grouping_repeat) {
  auto a = irs::FromRegexp("(ab)+");
  AssertProperties(a);
  EXPECT_FALSE(Accepts(a, ""));
  EXPECT_TRUE(Accepts(a, "ab"));
  EXPECT_TRUE(Accepts(a, "abab"));
  EXPECT_TRUE(Accepts(a, "ababab"));
  EXPECT_FALSE(Accepts(a, "a"));
  EXPECT_FALSE(Accepts(a, "aba"));
}

TEST_F(RegexpUtilsTest, match_grouping_alternation) {
  auto a = irs::FromRegexp("(a|b)*");
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, ""));
  EXPECT_TRUE(Accepts(a, "a"));
  EXPECT_TRUE(Accepts(a, "b"));
  EXPECT_TRUE(Accepts(a, "ababab"));
  EXPECT_TRUE(Accepts(a, "aaabbb"));
  EXPECT_FALSE(Accepts(a, "c"));
}

TEST_F(RegexpUtilsTest, match_grouping_nested) {
  auto a = irs::FromRegexp("((ab)+c)+");
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, "abc"));
  EXPECT_TRUE(Accepts(a, "ababc"));
  EXPECT_TRUE(Accepts(a, "abcabc"));
  EXPECT_TRUE(Accepts(a, "abcababc"));
  EXPECT_FALSE(Accepts(a, "ab"));
  EXPECT_FALSE(Accepts(a, ""));
}

TEST_F(RegexpUtilsTest, match_grouping_combined_quantifiers) {
  {
    // (a+)? = zero or one occurrence of one-or-more a's
    auto a = irs::FromRegexp("(a+)?");
    AssertProperties(a);
    EXPECT_TRUE(Accepts(a, ""));
    EXPECT_TRUE(Accepts(a, "a"));
    EXPECT_TRUE(Accepts(a, "aaa"));
  }
  {
    // (a?)+ = one or more occurrences of zero-or-one a
    auto a = irs::FromRegexp("(a?)+");
    AssertProperties(a);
    EXPECT_TRUE(Accepts(a, ""));
    EXPECT_TRUE(Accepts(a, "a"));
    EXPECT_TRUE(Accepts(a, "aaa"));
  }
  {
    // (a*)+ = same as a*
    auto a = irs::FromRegexp("(a*)+");
    AssertProperties(a);
    EXPECT_TRUE(Accepts(a, ""));
    EXPECT_TRUE(Accepts(a, "a"));
    EXPECT_TRUE(Accepts(a, "aaaa"));
  }
}

// Character classes

TEST_F(RegexpUtilsTest, match_char_class_simple) {
  auto a = irs::FromRegexp("[abc]");
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, "a"));
  EXPECT_TRUE(Accepts(a, "b"));
  EXPECT_TRUE(Accepts(a, "c"));
  EXPECT_FALSE(Accepts(a, "d"));
  EXPECT_FALSE(Accepts(a, "ab"));
  EXPECT_FALSE(Accepts(a, ""));
}

TEST_F(RegexpUtilsTest, match_char_class_with_quantifier) {
  auto a = irs::FromRegexp("[abc]+");
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, "a"));
  EXPECT_TRUE(Accepts(a, "abc"));
  EXPECT_TRUE(Accepts(a, "cba"));
  EXPECT_TRUE(Accepts(a, "aaabbbccc"));
  EXPECT_FALSE(Accepts(a, ""));
  EXPECT_FALSE(Accepts(a, "abcd"));
}

TEST_F(RegexpUtilsTest, match_char_class_range_lowercase) {
  auto a = irs::FromRegexp("[a-c]");
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, "a"));
  EXPECT_TRUE(Accepts(a, "b"));
  EXPECT_TRUE(Accepts(a, "c"));
  EXPECT_FALSE(Accepts(a, "d"));
  EXPECT_FALSE(Accepts(a, "A"));
}

TEST_F(RegexpUtilsTest, match_char_class_range_digits) {
  auto a = irs::FromRegexp("[0-9]+");
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, "0"));
  EXPECT_TRUE(Accepts(a, "123"));
  EXPECT_TRUE(Accepts(a, "9876543210"));
  EXPECT_FALSE(Accepts(a, ""));
  EXPECT_FALSE(Accepts(a, "12a34"));
}

TEST_F(RegexpUtilsTest, match_char_class_range_mixed) {
  auto a = irs::FromRegexp("[a-zA-Z]+");
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, "abc"));
  EXPECT_TRUE(Accepts(a, "ABC"));
  EXPECT_TRUE(Accepts(a, "AbCdEf"));
  EXPECT_FALSE(Accepts(a, "abc123"));
}

TEST_F(RegexpUtilsTest, match_char_class_escape) {
  auto a = irs::FromRegexp("[\\-\\]]");
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, "-"));
  EXPECT_TRUE(Accepts(a, "]"));
  EXPECT_FALSE(Accepts(a, "a"));
  EXPECT_FALSE(Accepts(a, "["));
}

TEST_F(RegexpUtilsTest, match_char_class_dash_at_end) {
  auto a = irs::FromRegexp("[abc-]");
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, "a"));
  EXPECT_TRUE(Accepts(a, "b"));
  EXPECT_TRUE(Accepts(a, "c"));
  EXPECT_TRUE(Accepts(a, "-"));
  EXPECT_FALSE(Accepts(a, "d"));
}

TEST_F(RegexpUtilsTest, match_char_class_dash_at_start) {
  auto a = irs::FromRegexp("[-abc]");
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, "-"));
  EXPECT_TRUE(Accepts(a, "a"));
}

// Escape sequences

TEST_F(RegexpUtilsTest, match_escape_dot) {
  auto a = irs::FromRegexp("a\\.b");
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, "a.b"));
  EXPECT_FALSE(Accepts(a, "aXb"));
  EXPECT_FALSE(Accepts(a, "ab"));
}

TEST_F(RegexpUtilsTest, match_escape_star) {
  auto a = irs::FromRegexp("a\\*b");
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, "a*b"));
  EXPECT_FALSE(Accepts(a, "ab"));
  EXPECT_FALSE(Accepts(a, "aaaaab"));
}

TEST_F(RegexpUtilsTest, match_escape_parens) {
  auto a = irs::FromRegexp("\\(test\\)");
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, "(test)"));
  EXPECT_FALSE(Accepts(a, "test"));
}

TEST_F(RegexpUtilsTest, match_escape_backslash) {
  auto a = irs::FromRegexp("a\\\\b");
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, "a\\b"));
  EXPECT_FALSE(Accepts(a, "ab"));
}

TEST_F(RegexpUtilsTest, match_escape_question) {
  auto a = irs::FromRegexp("a\\?b");
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, "a?b"));
  EXPECT_FALSE(Accepts(a, "ab"));
  EXPECT_FALSE(Accepts(a, "aab"));
}

TEST_F(RegexpUtilsTest, match_escape_plus) {
  auto a = irs::FromRegexp("a\\+b");
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, "a+b"));
  EXPECT_FALSE(Accepts(a, "ab"));
  EXPECT_FALSE(Accepts(a, "aab"));
}

TEST_F(RegexpUtilsTest, match_escape_pipe) {
  auto a = irs::FromRegexp("a\\|b");
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, "a|b"));
  EXPECT_FALSE(Accepts(a, "a"));
  EXPECT_FALSE(Accepts(a, "b"));
}

TEST_F(RegexpUtilsTest, match_escape_brackets) {
  auto a = irs::FromRegexp("\\[test\\]");
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, "[test]"));
  EXPECT_FALSE(Accepts(a, "test"));
}

// .* patterns (primary use case for search)

TEST_F(RegexpUtilsTest, match_dot_star_alone) {
  auto a = irs::FromRegexp(".*");
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, ""));
  EXPECT_TRUE(Accepts(a, "a"));
  EXPECT_TRUE(Accepts(a, "anything goes here 123 !@#"));
}

TEST_F(RegexpUtilsTest, match_dot_star_prefix) {
  auto a = irs::FromRegexp("foo.*");
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, "foo"));
  EXPECT_TRUE(Accepts(a, "foobar"));
  EXPECT_TRUE(Accepts(a, "foo123"));
  EXPECT_FALSE(Accepts(a, "fo"));
  EXPECT_FALSE(Accepts(a, "barfoo"));
}

TEST_F(RegexpUtilsTest, match_dot_star_suffix) {
  auto a = irs::FromRegexp(".*foo");
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, "foo"));
  EXPECT_TRUE(Accepts(a, "barfoo"));
  EXPECT_TRUE(Accepts(a, "123foo"));
  EXPECT_FALSE(Accepts(a, "foobar"));
}

TEST_F(RegexpUtilsTest, match_dot_star_infix) {
  auto a = irs::FromRegexp(".*foo.*");
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, "foo"));
  EXPECT_TRUE(Accepts(a, "foobar"));
  EXPECT_TRUE(Accepts(a, "barfoo"));
  EXPECT_TRUE(Accepts(a, "barfoobar"));
  EXPECT_FALSE(Accepts(a, "bar"));
}

TEST_F(RegexpUtilsTest, match_dot_star_multiple) {
  auto a = irs::FromRegexp(".*a.*b.*");
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, "ab"));
  EXPECT_TRUE(Accepts(a, "xaybz"));
  EXPECT_TRUE(Accepts(a, "XXXaYYYbZZZ"));
  EXPECT_FALSE(Accepts(a, "ba"));
  EXPECT_FALSE(Accepts(a, "a"));
  EXPECT_FALSE(Accepts(a, "b"));
}

TEST_F(RegexpUtilsTest, match_dot_star_vs_dot_plus) {
  {
    auto a = irs::FromRegexp("a.*b");
    AssertProperties(a);
    EXPECT_TRUE(Accepts(a, "ab"));  // .* matches empty
    EXPECT_TRUE(Accepts(a, "aXb"));
    EXPECT_TRUE(Accepts(a, "aXXXb"));
  }
  {
    auto a = irs::FromRegexp("a.+b");
    AssertProperties(a);
    EXPECT_FALSE(Accepts(a, "ab"));  // .+ requires at least one char
    EXPECT_TRUE(Accepts(a, "aXb"));
    EXPECT_TRUE(Accepts(a, "aXXXb"));
  }
}

// foo.*bar - key pattern (DFA handles without backtracking)

TEST_F(RegexpUtilsTest, match_foo_dot_star_bar_basic) {
  auto a = irs::FromRegexp("foo.*bar");
  AssertProperties(a);

  EXPECT_TRUE(Accepts(a, "foobar"));
  EXPECT_TRUE(Accepts(a, "foo123bar"));
  EXPECT_TRUE(Accepts(a, "fooXXXbar"));

  EXPECT_FALSE(Accepts(a, "foo"));
  EXPECT_FALSE(Accepts(a, "bar"));
  EXPECT_FALSE(Accepts(a, "fooba"));
  EXPECT_FALSE(Accepts(a, "oobar"));
}

TEST_F(RegexpUtilsTest, match_foo_dot_star_bar_tricky) {
  auto a = irs::FromRegexp("foo.*bar");
  AssertProperties(a);

  EXPECT_TRUE(Accepts(a, "foobarbar"));
  EXPECT_TRUE(Accepts(a, "foobarbazbar"));
  EXPECT_TRUE(Accepts(a, "foobaXbar"));
  EXPECT_TRUE(Accepts(a, "foobasbar"));
  EXPECT_TRUE(Accepts(a, "foobaxbar"));
  EXPECT_TRUE(Accepts(a, "foobabar"));
  EXPECT_TRUE(Accepts(a, "foobababababar"));
}

TEST_F(RegexpUtilsTest, match_foo_dot_star_bar_no_match) {
  auto a = irs::FromRegexp("foo.*bar");
  AssertProperties(a);

  EXPECT_FALSE(Accepts(a, "foobasba"));
  EXPECT_FALSE(Accepts(a, "foobarbaz"));
  EXPECT_FALSE(Accepts(a, "fooXbar "));
}

// foo*bar vs foo.*bar (common confusion)

TEST_F(RegexpUtilsTest, match_foo_star_bar_vs_foo_dot_star_bar) {
  // foo*bar = fo + (o*) + bar
  {
    auto a = irs::FromRegexp("foo*bar");
    AssertProperties(a);
    EXPECT_TRUE(Accepts(a, "fobar"));
    EXPECT_TRUE(Accepts(a, "foobar"));
    EXPECT_TRUE(Accepts(a, "fooobar"));
    EXPECT_TRUE(Accepts(a, "foooobar"));

    EXPECT_FALSE(Accepts(a, "fooXbar"));
    EXPECT_FALSE(Accepts(a, "foobasbar"));
  }

  // foo.*bar = foo + (any chars) + bar
  {
    auto a = irs::FromRegexp("foo.*bar");
    AssertProperties(a);
    EXPECT_FALSE(Accepts(a, "fobar"));  // missing 'o'
    EXPECT_TRUE(Accepts(a, "foobar"));
    EXPECT_TRUE(Accepts(a, "fooXbar"));
    EXPECT_TRUE(Accepts(a, "foobasbar"));
  }
}

TEST_F(RegexpUtilsTest, match_utf8_literal) {
  auto a = irs::FromRegexp("привет");
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, "привет"));
  EXPECT_FALSE(Accepts(a, "приветы"));
  EXPECT_FALSE(Accepts(a, "привет!"));
}

TEST_F(RegexpUtilsTest, match_utf8_prefix) {
  auto a = irs::FromRegexp("при.*");
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, "привет"));
  EXPECT_TRUE(Accepts(a, "приветствую"));
  EXPECT_TRUE(Accepts(a, "при"));
  EXPECT_FALSE(Accepts(a, "пока"));
}

TEST_F(RegexpUtilsTest, match_utf8_dot) {
  auto a = irs::FromRegexp("пр.вет");
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, "привет"));
  EXPECT_TRUE(Accepts(a, "прXвет"));
}

TEST_F(RegexpUtilsTest, match_utf8_alternation) {
  auto a = irs::FromRegexp("да|нет");
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, "да"));
  EXPECT_TRUE(Accepts(a, "нет"));
  EXPECT_FALSE(Accepts(a, "может"));
}

TEST_F(RegexpUtilsTest, match_utf8_quantifiers) {
  {
    auto a = irs::FromRegexp("а+");
    AssertProperties(a);
    EXPECT_TRUE(Accepts(a, "а"));
    EXPECT_TRUE(Accepts(a, "ааа"));
    EXPECT_FALSE(Accepts(a, ""));
  }
  {
    auto a = irs::FromRegexp("ха?");
    AssertProperties(a);
    EXPECT_TRUE(Accepts(a, "х"));
    EXPECT_TRUE(Accepts(a, "ха"));
    EXPECT_FALSE(Accepts(a, "хаа"));
  }
}

TEST_F(RegexpUtilsTest, match_utf8_range) {
  auto a = irs::FromRegexp("[а-г]+");
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, "абвг"));
  EXPECT_TRUE(Accepts(a, "ааа"));
  EXPECT_FALSE(Accepts(a, "дежз"));
  EXPECT_FALSE(Accepts(a, "abc"));
}

TEST_F(RegexpUtilsTest, match_utf8_mixed) {
  auto a = irs::FromRegexp("hello.*мир");
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, "helloмир"));
  EXPECT_TRUE(Accepts(a, "hello мир"));
  EXPECT_TRUE(Accepts(a, "hello, мир"));
  EXPECT_FALSE(Accepts(a, "hello"));
}

// UTF-8 3-byte characters (Chinese, Japanese, Korean - U+0800 to U+FFFF)

TEST_F(RegexpUtilsTest, match_utf8_3byte_literal) {
  // Chinese characters: 中 = E4 B8 AD, 文 = E6 96 87, 字 = E5 AD 97
  auto a = irs::FromRegexp("中文");
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, "中文"));
  EXPECT_FALSE(Accepts(a, "中"));
  EXPECT_FALSE(Accepts(a, "文"));
  EXPECT_FALSE(Accepts(a, "中文字"));
  EXPECT_FALSE(Accepts(a, ""));
}

TEST_F(RegexpUtilsTest, match_utf8_3byte_dot) {
  auto a = irs::FromRegexp("中.字");
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, "中文字"));
  EXPECT_TRUE(Accepts(a, "中X字"));
  EXPECT_TRUE(Accepts(a, "中国字"));  // 国 is also 3-byte
  EXPECT_FALSE(Accepts(a, "中字"));
  EXPECT_FALSE(Accepts(a, "中文文字"));
}

TEST_F(RegexpUtilsTest, match_utf8_3byte_quantifiers) {
  // Star quantifier with 3-byte char
  {
    auto a = irs::FromRegexp("中*");
    AssertProperties(a);
    EXPECT_TRUE(Accepts(a, ""));
    EXPECT_TRUE(Accepts(a, "中"));
    EXPECT_TRUE(Accepts(a, "中中中"));
    EXPECT_FALSE(Accepts(a, "中文"));
  }
  // Plus quantifier
  {
    auto a = irs::FromRegexp("文+");
    AssertProperties(a);
    EXPECT_FALSE(Accepts(a, ""));
    EXPECT_TRUE(Accepts(a, "文"));
    EXPECT_TRUE(Accepts(a, "文文文"));
  }
  // Optional quantifier
  {
    auto a = irs::FromRegexp("中文?字");
    AssertProperties(a);
    EXPECT_TRUE(Accepts(a, "中字"));
    EXPECT_TRUE(Accepts(a, "中文字"));
    EXPECT_FALSE(Accepts(a, "中文文字"));
  }
}

TEST_F(RegexpUtilsTest, match_utf8_3byte_prefix_suffix) {
  // Prefix: 中.*
  {
    auto a = irs::FromRegexp("中.*");
    AssertProperties(a);
    EXPECT_TRUE(Accepts(a, "中"));
    EXPECT_TRUE(Accepts(a, "中文"));
    EXPECT_TRUE(Accepts(a, "中文字"));
    EXPECT_TRUE(Accepts(a, "中abc"));
    EXPECT_FALSE(Accepts(a, "文中"));
  }
  // Suffix: .*字
  {
    auto a = irs::FromRegexp(".*字");
    AssertProperties(a);
    EXPECT_TRUE(Accepts(a, "字"));
    EXPECT_TRUE(Accepts(a, "文字"));
    EXPECT_TRUE(Accepts(a, "中文字"));
    EXPECT_TRUE(Accepts(a, "abc字"));
    EXPECT_FALSE(Accepts(a, "字中"));
  }
  // Infix: .*文.*
  {
    auto a = irs::FromRegexp(".*文.*");
    AssertProperties(a);
    EXPECT_TRUE(Accepts(a, "文"));
    EXPECT_TRUE(Accepts(a, "中文"));
    EXPECT_TRUE(Accepts(a, "文字"));
    EXPECT_TRUE(Accepts(a, "中文字"));
    EXPECT_FALSE(Accepts(a, "中字"));
  }
}

TEST_F(RegexpUtilsTest, match_utf8_3byte_alternation) {
  auto a = irs::FromRegexp("中国|日本|韓国");
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, "中国"));
  EXPECT_TRUE(Accepts(a, "日本"));
  EXPECT_TRUE(Accepts(a, "韓国"));
  EXPECT_FALSE(Accepts(a, "中"));
  EXPECT_FALSE(Accepts(a, "国"));
  EXPECT_FALSE(Accepts(a, "中日韓"));
}

TEST_F(RegexpUtilsTest, match_utf8_3byte_char_class) {
  // Character class with 3-byte chars
  auto a = irs::FromRegexp("[中文字]+");
  AssertProperties(a);
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
  // 一 = E4 B8 80, 二 = E4 BA 8C, 三 = E4 B8 89
  // needs to check
  auto a = irs::FromRegexp("[一-三]+");
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, "一"));
  EXPECT_TRUE(Accepts(a, "三"));
  EXPECT_TRUE(Accepts(a, "一一一"));
}

// UTF-8 4-byte characters (Emojis, rare CJK - U+10000 to U+10FFFF)

TEST_F(RegexpUtilsTest, match_utf8_4byte_literal) {
  // Emoji: 😀 = F0 9F 98 80 (U+1F600)
  auto a = irs::FromRegexp("😀");
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, "😀"));
  EXPECT_FALSE(Accepts(a, "😀😀"));
  EXPECT_FALSE(Accepts(a, ""));
  EXPECT_FALSE(Accepts(a, "X"));
}

TEST_F(RegexpUtilsTest, match_utf8_4byte_multiple) {
  // Multiple emojis: 🎉 = F0 9F 8E 89, 🚀 = F0 9F 9A 80
  auto a = irs::FromRegexp("😀🎉");
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, "😀🎉"));
  EXPECT_FALSE(Accepts(a, "😀"));
  EXPECT_FALSE(Accepts(a, "🎉"));
  EXPECT_FALSE(Accepts(a, "🎉😀"));
}

TEST_F(RegexpUtilsTest, match_utf8_4byte_dot) {
  auto a = irs::FromRegexp("a.b");
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, "a😀b"));
  EXPECT_TRUE(Accepts(a, "a🎉b"));
  EXPECT_TRUE(Accepts(a, "aXb"));
  EXPECT_FALSE(Accepts(a, "ab"));
  EXPECT_FALSE(Accepts(a, "a😀😀b"));
}

TEST_F(RegexpUtilsTest, match_utf8_4byte_quantifiers) {
  // Star quantifier with emoji
  {
    auto a = irs::FromRegexp("😀*");
    AssertProperties(a);
    EXPECT_TRUE(Accepts(a, ""));
    EXPECT_TRUE(Accepts(a, "😀"));
    EXPECT_TRUE(Accepts(a, "😀😀😀"));
    EXPECT_FALSE(Accepts(a, "😀🎉"));
  }
  // Plus quantifier
  {
    auto a = irs::FromRegexp("🎉+");
    AssertProperties(a);
    EXPECT_FALSE(Accepts(a, ""));
    EXPECT_TRUE(Accepts(a, "🎉"));
    EXPECT_TRUE(Accepts(a, "🎉🎉🎉"));
  }
  // Optional quantifier
  {
    auto a = irs::FromRegexp("a😀?b");
    AssertProperties(a);
    EXPECT_TRUE(Accepts(a, "ab"));
    EXPECT_TRUE(Accepts(a, "a😀b"));
    EXPECT_FALSE(Accepts(a, "a😀😀b"));
  }
}

TEST_F(RegexpUtilsTest, match_utf8_4byte_prefix_suffix) {
  // Prefix with emoji
  {
    auto a = irs::FromRegexp("😀.*");
    AssertProperties(a);
    EXPECT_TRUE(Accepts(a, "😀"));
    EXPECT_TRUE(Accepts(a, "😀hello"));
    EXPECT_TRUE(Accepts(a, "😀🎉🚀"));
    EXPECT_FALSE(Accepts(a, "hello😀"));
  }
  // Suffix with emoji
  {
    auto a = irs::FromRegexp(".*🎉");
    AssertProperties(a);
    EXPECT_TRUE(Accepts(a, "🎉"));
    EXPECT_TRUE(Accepts(a, "hello🎉"));
    EXPECT_TRUE(Accepts(a, "😀🎉"));
    EXPECT_FALSE(Accepts(a, "🎉hello"));
  }
}

TEST_F(RegexpUtilsTest, match_utf8_4byte_alternation) {
  auto a = irs::FromRegexp("😀|🎉|🚀");
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, "😀"));
  EXPECT_TRUE(Accepts(a, "🎉"));
  EXPECT_TRUE(Accepts(a, "🚀"));
  EXPECT_FALSE(Accepts(a, "X"));
  EXPECT_FALSE(Accepts(a, "😀🎉"));
}

TEST_F(RegexpUtilsTest, match_utf8_4byte_char_class) {
  // Character class with emojis
  auto a = irs::FromRegexp("[😀🎉🚀]+");
  AssertProperties(a);
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
  auto a = irs::FromRegexp("𠀀𠀁");
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, "𠀀𠀁"));
  EXPECT_FALSE(Accepts(a, "𠀀"));
  EXPECT_FALSE(Accepts(a, "𠀁"));
}

// Mixed UTF-8 byte lengths (1, 2, 3, 4 bytes together)

TEST_F(RegexpUtilsTest, match_utf8_mixed_all_lengths) {
  // a (1 byte) + б (2 bytes) + 中 (3 bytes) + 😀 (4 bytes)
  auto a = irs::FromRegexp("aб中😀");
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, "aб中😀"));
  EXPECT_FALSE(Accepts(a, "aб中"));
  EXPECT_FALSE(Accepts(a, "б中😀"));
}

TEST_F(RegexpUtilsTest, match_utf8_mixed_dot_any_length) {
  // Dot should match any single char regardless of byte length
  auto a = irs::FromRegexp("....");  // four dots = four chars
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, "abcd"));
  EXPECT_TRUE(Accepts(a, "абвг"));
  EXPECT_TRUE(Accepts(a, "中文日本"));
  EXPECT_TRUE(Accepts(a, "😀🎉🚀🌟"));
  EXPECT_TRUE(Accepts(a, "aб中😀"));
  EXPECT_FALSE(Accepts(a, "abc"));
  EXPECT_FALSE(Accepts(a, "abcde"));
}

TEST_F(RegexpUtilsTest, match_utf8_mixed_quantifiers) {
  // Pattern: (any 2-byte)+ followed by (any 3-byte)* followed by emoji
  auto a = irs::FromRegexp("при.*中.*😀");
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, "при中😀"));
  EXPECT_TRUE(Accepts(a, "приXXX中YYY😀"));
  EXPECT_TRUE(Accepts(a, "при中文字😀"));
  EXPECT_FALSE(Accepts(a, "при中"));
  EXPECT_FALSE(Accepts(a, "中😀"));
}

TEST_F(RegexpUtilsTest, match_utf8_mixed_char_class) {
  // Char class with mixed byte lengths
  auto a = irs::FromRegexp("[aбя中😀]+");
  AssertProperties(a);
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
  // foo.*bar with emoji in the middle
  auto a = irs::FromRegexp("foo.*bar");
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, "foo😀bar"));
  EXPECT_TRUE(Accepts(a, "foo🎉🚀🌟bar"));
  EXPECT_TRUE(Accepts(a, "foo中文bar"));
  EXPECT_TRUE(Accepts(a, "fooприветbar"));
  EXPECT_TRUE(Accepts(a, "foo😀中приbar"));
}

// Parse errors

TEST_F(RegexpUtilsTest, invalid_unclosed_paren) {
  auto a = irs::FromRegexp("(abc");
  EXPECT_EQ(0, a.NumStates());
}

TEST_F(RegexpUtilsTest, invalid_unexpected_rparen) {
  auto a = irs::FromRegexp("abc)");
  EXPECT_EQ(0, a.NumStates());
}

TEST_F(RegexpUtilsTest, invalid_unclosed_bracket) {
  auto a = irs::FromRegexp("[abc");
  EXPECT_EQ(0, a.NumStates());
}

TEST_F(RegexpUtilsTest, invalid_empty_bracket) {
  auto a = irs::FromRegexp("[]");
  EXPECT_EQ(0, a.NumStates());
}

TEST_F(RegexpUtilsTest, invalid_quantifier_at_start) {
  EXPECT_EQ(0, irs::FromRegexp("*abc").NumStates());
  EXPECT_EQ(0, irs::FromRegexp("+abc").NumStates());
  EXPECT_EQ(0, irs::FromRegexp("?abc").NumStates());
}

TEST_F(RegexpUtilsTest, invalid_trailing_backslash) {
  auto a = irs::FromRegexp("abc\\");
  EXPECT_EQ(0, a.NumStates());
}

TEST_F(RegexpUtilsTest, invalid_range_order) {
  auto a = irs::FromRegexp("[z-a]");
  EXPECT_EQ(0, a.NumStates());
}

TEST_F(RegexpUtilsTest, invalid_double_quantifier) {
  EXPECT_EQ(0, irs::FromRegexp("a**").NumStates());
  EXPECT_EQ(0, irs::FromRegexp("a++").NumStates());
  EXPECT_EQ(0, irs::FromRegexp("a?*").NumStates());
  EXPECT_EQ(0, irs::FromRegexp("a*+").NumStates());
}

TEST_F(RegexpUtilsTest, invalid_quantifier_after_pipe) {
  EXPECT_EQ(0, irs::FromRegexp("a|*").NumStates());
  EXPECT_EQ(0, irs::FromRegexp("a|+").NumStates());
}

TEST_F(RegexpUtilsTest, invalid_quantifier_after_open_paren) {
  EXPECT_EQ(0, irs::FromRegexp("(*a)").NumStates());
  EXPECT_EQ(0, irs::FromRegexp("(+a)").NumStates());
}

// Edge cases

TEST_F(RegexpUtilsTest, edge_anchors_ignored) {
  {
    auto a = irs::FromRegexp("^foo$");
    AssertProperties(a);
    EXPECT_TRUE(Accepts(a, "foo"));
    EXPECT_FALSE(Accepts(a, "foobar"));
  }
  {
    auto a = irs::FromRegexp("^foo");
    AssertProperties(a);
    EXPECT_TRUE(Accepts(a, "foo"));
  }
}

TEST_F(RegexpUtilsTest, edge_nested_groups_complex) {
  auto a = irs::FromRegexp("((a|b)*c)+");
  AssertProperties(a);
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
  auto a = irs::FromRegexp("[a-z]+@[a-z]+\\.[a-z]+");
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, "user@mail.com"));
  EXPECT_TRUE(Accepts(a, "test@example.org"));
  EXPECT_FALSE(Accepts(a, "user@mail"));
  EXPECT_FALSE(Accepts(a, "@mail.com"));
  EXPECT_FALSE(Accepts(a, "user@.com"));
}

TEST_F(RegexpUtilsTest, edge_long_alternation) {
  auto a = irs::FromRegexp("a|b|c|d|e|f|g|h|i|j");
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, "a"));
  EXPECT_TRUE(Accepts(a, "j"));
  EXPECT_FALSE(Accepts(a, "k"));
  EXPECT_FALSE(Accepts(a, "ab"));
}

TEST_F(RegexpUtilsTest, edge_deeply_nested) {
  auto a = irs::FromRegexp("((((a))))");
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, "a"));
  EXPECT_FALSE(Accepts(a, ""));
  EXPECT_FALSE(Accepts(a, "aa"));
}

TEST_F(RegexpUtilsTest, edge_mixed_quantifiers) {
  auto a = irs::FromRegexp("a+b*c?d");
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, "ad"));        // a+ b* c? d
  EXPECT_TRUE(Accepts(a, "abd"));       // a+ b* c? d
  EXPECT_TRUE(Accepts(a, "acd"));       // a+ b* c? d
  EXPECT_TRUE(Accepts(a, "abcd"));      // all present
  EXPECT_TRUE(Accepts(a, "aaabbbcd"));  // multiple a, b
  EXPECT_FALSE(Accepts(a, "d"));        // missing a+
  EXPECT_FALSE(Accepts(a, "abccd"));    // two c
}

// RE2-specific features

TEST_F(RegexpUtilsTest, re2_counted_quantifiers) {
  {
    auto a = irs::FromRegexp("a{3}");
    AssertProperties(a);
    EXPECT_TRUE(Accepts(a, "aaa"));
    EXPECT_FALSE(Accepts(a, "aa"));
    EXPECT_FALSE(Accepts(a, "aaaa"));
  }
  {
    auto a = irs::FromRegexp("a{2,4}");
    AssertProperties(a);
    EXPECT_FALSE(Accepts(a, "a"));
    EXPECT_TRUE(Accepts(a, "aa"));
    EXPECT_TRUE(Accepts(a, "aaa"));
    EXPECT_TRUE(Accepts(a, "aaaa"));
    EXPECT_FALSE(Accepts(a, "aaaaa"));
  }
  {
    auto a = irs::FromRegexp("a{2,}");
    AssertProperties(a);
    EXPECT_FALSE(Accepts(a, "a"));
    EXPECT_TRUE(Accepts(a, "aa"));
    EXPECT_TRUE(Accepts(a, "aaaaaaaaa"));
  }
}

TEST_F(RegexpUtilsTest, re2_perl_classes) {
  {
    auto a = irs::FromRegexp("\\d+");
    AssertProperties(a);
    EXPECT_TRUE(Accepts(a, "123"));
    EXPECT_TRUE(Accepts(a, "0"));
    EXPECT_FALSE(Accepts(a, "abc"));
    EXPECT_FALSE(Accepts(a, ""));
  }
  {
    auto a = irs::FromRegexp("\\w+");
    AssertProperties(a);
    EXPECT_TRUE(Accepts(a, "abc"));
    EXPECT_TRUE(Accepts(a, "abc123"));
    EXPECT_TRUE(Accepts(a, "_foo"));
    EXPECT_FALSE(Accepts(a, ""));
    EXPECT_FALSE(Accepts(a, "a b"));
  }
}

TEST_F(RegexpUtilsTest, re2_non_capturing_group) {
  auto a = irs::FromRegexp("(?:ab)+c");
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, "abc"));
  EXPECT_TRUE(Accepts(a, "ababc"));
  EXPECT_FALSE(Accepts(a, "c"));
  EXPECT_FALSE(Accepts(a, "ab"));
}

TEST_F(RegexpUtilsTest, re2_case_insensitive_inline) {
  auto a = irs::FromRegexp("(?i:abc)");
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, "abc"));
  EXPECT_TRUE(Accepts(a, "ABC"));
  EXPECT_TRUE(Accepts(a, "AbC"));
  EXPECT_FALSE(Accepts(a, "abcd"));
}

TEST_F(RegexpUtilsTest, re2_literal_quoting) {
  auto a = irs::FromRegexp("\\Q.*+?\\E");
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, ".*+?"));
  EXPECT_FALSE(Accepts(a, "anything"));
  EXPECT_FALSE(Accepts(a, ""));
}

TEST_F(RegexpUtilsTest, re2_unicode_property) {
  auto a = irs::FromRegexp("\\p{Cyrillic}+");
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, "привет"));
  EXPECT_TRUE(Accepts(a, "абв"));
  EXPECT_FALSE(Accepts(a, "abc"));
  EXPECT_FALSE(Accepts(a, ""));
}

TEST_F(RegexpUtilsTest, re2_word_boundary) {
  // \b at term boundaries is epsilon (no-op for whole-term matching)
  auto a = irs::FromRegexp("\\bfoo\\b");
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, "foo"));
  EXPECT_FALSE(Accepts(a, "foobar"));
}

TEST_F(RegexpUtilsTest, re2_no_word_boundary_matches_nothing) {
  // \B is a documented limitation - any pattern containing it should
  // match nothing.  The empty automaton has no final states, so
  // concatenating it into a larger pattern leaves the result unable
  // to accept any input, even though NumStates() may be non-zero.
  auto a = irs::FromRegexp("foo\\Bbar");
  EXPECT_FALSE(Accepts(a, "foobar"));
  EXPECT_FALSE(Accepts(a, "foo"));
  EXPECT_FALSE(Accepts(a, "bar"));
  EXPECT_FALSE(Accepts(a, ""));
  EXPECT_FALSE(Accepts(a, "fooXbar"));
}

TEST_F(RegexpUtilsTest, re2_named_capture) {
  // Named captures are treated as regular groups (captures ignored)
  auto a = irs::FromRegexp("(?P<word>foo)bar");
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, "foobar"));
  EXPECT_FALSE(Accepts(a, "foo"));
  EXPECT_FALSE(Accepts(a, "bar"));
}

TEST_F(RegexpUtilsTest, re2_any_byte) {
  // \C matches a single raw byte
  auto a = irs::FromRegexp("a\\Cb");
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, "aXb"));
  EXPECT_TRUE(Accepts(a, "a1b"));
}

TEST_F(RegexpUtilsTest, re2_dfa_size_limit) {
  // Pattern that may produce large DFA - should either succeed
  // or return empty automaton (hit limit), but not OOM
  auto a = irs::FromRegexp("[ab]{20}");
  // Either valid DFA or empty (rejected by limit) - both acceptable
  if (a.NumStates() > 0) {
    AssertProperties(a);
    EXPECT_TRUE(Accepts(a, "aaaaaaaaaaaaaaaaaaaa"));
    EXPECT_TRUE(Accepts(a, "abababababababababab"));
    EXPECT_FALSE(Accepts(a, "aaaaaaaaaaaaaaaaaaaaa"));  // 21 chars
  }
  // If NumStates == 0, it was rejected by DFA limit - also fine
}

TEST_F(RegexpUtilsTest, re2_dfa_size_limit_custom) {
  // Very low limit - should reject even simple patterns
  auto a = irs::FromRegexp("[abc]{5}", /*max_dfa_states=*/5);
  // With only 5 states allowed, this should be rejected
  EXPECT_EQ(0, a.NumStates());
}

TEST_F(RegexpUtilsTest, re2_dfa_size_limit_zero_unlimited) {
  // 0 means no limit
  auto a = irs::FromRegexp("(a|b)(c|d)(e|f)", /*max_dfa_states=*/0);
  ASSERT_GT(a.NumStates(), 0);
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, "ace"));
  EXPECT_TRUE(Accepts(a, "bdf"));
}

// Case-folding regression coverage
//
// The bug we just fixed: CycleFoldRune emitted arcs in fold-cycle order
// (a then A, i.e. 0x61 then 0x41), leaving the NFA not ilabel-sorted.
// Downstream matchers rely on sortedness for binary search -> Accepts("ABC")
// returned false.  These tests cover every fold-cycle shape that's likely
// to regress.

TEST_F(RegexpUtilsTest, fold_case_single_ascii_lower) {
  auto a = irs::FromRegexp("(?i:a)");
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, "a"));
  EXPECT_TRUE(Accepts(a, "A"));
  EXPECT_FALSE(Accepts(a, ""));
  EXPECT_FALSE(Accepts(a, "b"));
  EXPECT_FALSE(Accepts(a, "aa"));
}

TEST_F(RegexpUtilsTest, fold_case_single_ascii_upper) {
  // Pattern is uppercase - fold cycle is the same {a, A}, but emits
  // starting from 'A' (0x41).  Must still produce sorted arcs.
  auto a = irs::FromRegexp("(?i:A)");
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, "a"));
  EXPECT_TRUE(Accepts(a, "A"));
}

TEST_F(RegexpUtilsTest, fold_case_mixed_input) {
  auto a = irs::FromRegexp("(?i:aBc)");
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, "abc"));
  EXPECT_TRUE(Accepts(a, "ABC"));
  EXPECT_TRUE(Accepts(a, "aBc"));
  EXPECT_TRUE(Accepts(a, "AbC"));
  EXPECT_FALSE(Accepts(a, "abcd"));
}

TEST_F(RegexpUtilsTest, fold_case_unicode_k_kelvin) {
  // k's fold cycle includes U+212A (Kelvin sign, 3-byte UTF-8: E2 84 AA).
  // Exercises the multi-byte branch of the sort + Utf8EmplaceArc path.
  auto a = irs::FromRegexp("(?i:k)");
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, "k"));
  EXPECT_TRUE(Accepts(a, "K"));
  EXPECT_TRUE(Accepts(a, "\xE2\x84\xAA"));  // U+212A
  EXPECT_FALSE(Accepts(a, ""));
  EXPECT_FALSE(Accepts(a, "l"));
}

TEST_F(RegexpUtilsTest, fold_case_unicode_s_long) {
  // s's fold cycle includes U+017F (long s, 2-byte UTF-8: C5 BF).
  auto a = irs::FromRegexp("(?i:s)");
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, "s"));
  EXPECT_TRUE(Accepts(a, "S"));
  EXPECT_TRUE(Accepts(a, "\xC5\xBF"));  // U+017F
  EXPECT_FALSE(Accepts(a, "t"));
}

TEST_F(RegexpUtilsTest, fold_case_multiple_concat) {
  // Two separate FoldCase nodes concatenated - different AST shape from
  // the single (?i:abcd) case.
  auto a = irs::FromRegexp("(?i:ab)(?i:cd)");
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, "abcd"));
  EXPECT_TRUE(Accepts(a, "ABCD"));
  EXPECT_TRUE(Accepts(a, "AbCd"));
  EXPECT_FALSE(Accepts(a, "abc"));
  EXPECT_FALSE(Accepts(a, ""));
}

TEST_F(RegexpUtilsTest, fold_case_with_quantifier) {
  auto a = irs::FromRegexp("(?i:abc)+");
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, "abc"));
  EXPECT_TRUE(Accepts(a, "ABC"));
  EXPECT_TRUE(Accepts(a, "abcABC"));
  EXPECT_TRUE(Accepts(a, "ABCabcAbC"));
  EXPECT_FALSE(Accepts(a, ""));
  EXPECT_FALSE(Accepts(a, "ab"));
}

TEST_F(RegexpUtilsTest, fold_case_with_alternation) {
  auto a = irs::FromRegexp("(?i:foo|bar)");
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, "foo"));
  EXPECT_TRUE(Accepts(a, "FOO"));
  EXPECT_TRUE(Accepts(a, "Foo"));
  EXPECT_TRUE(Accepts(a, "bar"));
  EXPECT_TRUE(Accepts(a, "BAR"));
  EXPECT_FALSE(Accepts(a, "baz"));
}

TEST_F(RegexpUtilsTest, fold_case_non_alpha_pass_through) {
  // Digits and symbols have no fold cycle - must still work correctly
  // alongside folded letters.
  auto a = irs::FromRegexp("(?i:a1b)");
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, "a1b"));
  EXPECT_TRUE(Accepts(a, "A1B"));
  EXPECT_FALSE(Accepts(a, "a2b"));
  EXPECT_FALSE(Accepts(a, "aXb"));
}

// UnescapeRegexp direct tests
//
// The module function had no direct coverage - only indirect through
// ByTerm/ByPrefix paths.

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
  // "foo\" on wire - trailing lone backslash.  HasMetacharacters sees
  // the \, sets escaped=true, loop ends - no unescaped metacharacter
  // was returned as true.  HasEscapes returns true -> LiteralEscaped.
  EXPECT_EQ(irs::RegexpType::LiteralEscaped,
            irs::ComputeRegexpType(ToBytesView("foo\\")));
}

TEST_F(RegexpUtilsTest, regexp_type_escaped_dot_then_star) {
  // "\.*" on wire = literal dot, then unescaped *.  The * is a
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
  // \d, \w, \s, \b, \p{...}, \Q...\E - all change matching semantics and
  // must route to the full automaton path (Complex).
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

// UTF-8 range boundary-crossing
//
// BuildUtf8RangeAutomaton splits at byte-length boundaries (0x80,
// 0x800, 0x10000) and unions the parts.  The splitting logic is
// non-trivial - thin coverage up to now.

TEST_F(RegexpUtilsTest, utf8_range_crossing_1_to_2_byte) {
  // [U+0070 .. U+00A0] crosses 0x80 boundary: {1-byte part, 2-byte part}
  auto a = irs::FromRegexp("[\\x{70}-\\x{A0}]");
  AssertProperties(a);
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
  auto a = irs::FromRegexp("[\\x{7F0}-\\x{810}]");
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, "\xDF\xB0"));       // U+07F0 (2-byte)
  EXPECT_TRUE(Accepts(a, "\xDF\xBF"));       // U+07FF (2-byte, boundary)
  EXPECT_TRUE(Accepts(a, "\xE0\xA0\x80"));   // U+0800 (3-byte, boundary)
  EXPECT_TRUE(Accepts(a, "\xE0\xA0\x90"));   // U+0810 (3-byte)
  EXPECT_FALSE(Accepts(a, "\xDF\xAF"));      // U+07EF
  EXPECT_FALSE(Accepts(a, "\xE0\xA0\x91"));  // U+0811
}

TEST_F(RegexpUtilsTest, utf8_range_crossing_3_to_4_byte) {
  // [U+FFFE .. U+10001] crosses 0x10000 boundary
  auto a = irs::FromRegexp("[\\x{FFFE}-\\x{10001}]");
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, "\xEF\xBF\xBE"));      // U+FFFE (3-byte)
  EXPECT_TRUE(Accepts(a, "\xEF\xBF\xBF"));      // U+FFFF (3-byte, boundary)
  EXPECT_TRUE(Accepts(a, "\xF0\x90\x80\x80"));  // U+10000 (4-byte, boundary)
  EXPECT_TRUE(Accepts(a, "\xF0\x90\x80\x81"));  // U+10001 (4-byte)
  EXPECT_FALSE(Accepts(a, "\xEF\xBF\xBD"));     // U+FFFD
}

TEST_F(RegexpUtilsTest, utf8_range_full_unicode) {
  // [U+0000 .. U+10FFFF] - full Unicode range; all four byte-length parts
  auto a = irs::FromRegexp("[\\x{00}-\\x{10FFFF}]");
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, "a"));                 // 1-byte
  EXPECT_TRUE(Accepts(a, "\xC3\xA9"));          // é (2-byte)
  EXPECT_TRUE(Accepts(a, "\xE4\xB8\xAD"));      // 中 (3-byte)
  EXPECT_TRUE(Accepts(a, "\xF0\x9F\x98\x80"));  // 😀 (4-byte)
  EXPECT_FALSE(Accepts(a, ""));                 // requires one codepoint
  EXPECT_FALSE(Accepts(a, "ab"));               // only one codepoint
}

TEST_F(RegexpUtilsTest, utf8_range_mixed_ascii_and_unicode) {
  // Char class with both an ASCII range and a Unicode range -
  // BuildUtf8RangeAutomaton invoked twice and results unioned via Union().
  auto a = irs::FromRegexp("[a-z\\x{400}-\\x{4FF}]+");
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, "abc"));
  EXPECT_TRUE(Accepts(a, "абв"));     // Cyrillic
  EXPECT_TRUE(Accepts(a, "abcабв"));  // Mixed
  EXPECT_FALSE(Accepts(a, "ABC"));
  EXPECT_FALSE(Accepts(a, ""));
  EXPECT_FALSE(Accepts(a, "a1b"));
}

// Property-invariant broad coverage
//
// Fast canary: every pattern FromRegexp produces must satisfy the
// ilabel-sorted / deterministic / acceptor / unweighted invariants.
// Would have caught the case-folding sort bug at property level rather
// than at accept/reject level.

TEST_F(RegexpUtilsTest, invariant_properties_broad_coverage) {
  constexpr std::string_view kPatterns[] = {
    // Literals
    "a",
    "foo",
    "hello world",
    "abc123",
    // With escapes
    "\\.",
    "\\*",
    "a\\.b",
    "\\Q.+?\\E",
    // Basic metacharacters
    ".",
    "..",
    "a.b",
    "a.*b",
    "a.+b",
    // Quantifiers
    "a*",
    "a+",
    "a?",
    "a{3}",
    "a{2,5}",
    "a{3,}",
    // Alternation
    "a|b",
    "cat|dog|bird",
    "foo|foobar",
    "a|",
    "|a",
    // Groups
    "(ab)+",
    "(a|b)*",
    "(?:ab)+",
    "(?P<n>ab)",
    "((a|b)*c)+",
    // Char classes
    "[abc]",
    "[a-z]+",
    "[^0-9]+",
    "[0-9a-zA-Z]",
    // Perl classes
    "\\d+",
    "\\w+",
    "\\s+",
    "\\D",
    "\\W",
    "\\S",
    // Anchors (should be no-ops)
    "^foo",
    "foo$",
    "^foo$",
    "\\bfoo\\b",
    // UTF-8
    "привет",
    "中文",
    "😀",
    "[а-я]+",
    "[中文字]+",
    // Case fold - the bug we just fixed
    "(?i:a)",
    "(?i:A)",
    "(?i:k)",
    "(?i:s)",
    "(?i:abc)",
    "(?i:ab)(?i:cd)",
    "(?i:foo|bar)",
    // Combined
    "foo.*bar",
    ".*foo.*",
    "(a|b)c*d?",
    "\\p{Cyrillic}+",
    // Unicode ranges spanning byte-length boundaries
    "[\\x{70}-\\x{A0}]",
    "[\\x{7F0}-\\x{810}]",
    "[\\x{FFFE}-\\x{10001}]",
    // Any-byte
    "a\\Cb",
  };

  for (auto pat : kPatterns) {
    SCOPED_TRACE(testing::Message() << "pattern: " << pat);
    auto a = irs::FromRegexp(pat);
    if (a.NumStates() == 0) {
      // parse error or DFA-limit hit - acceptable, not a property violation
      continue;
    }
    AssertProperties(a);
  }
}

// Negated Perl classes - verify matching behavior, not just parse success
//
// The existing re2_perl_classes test only calls .prepare() on \D, \W etc.
// That proves they parse, but doesn't catch a regression that made them
// match the wrong thing.  These verify accept/reject semantics.

TEST_F(RegexpUtilsTest, perl_class_non_digit) {
  auto a = irs::FromRegexp("\\D+");
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, "abc"));
  EXPECT_TRUE(Accepts(a, "hello world"));
  EXPECT_TRUE(Accepts(a, "!@#"));
  EXPECT_FALSE(Accepts(a, "123"));
  EXPECT_FALSE(Accepts(a, "abc123"));  // mixed - one digit breaks
  EXPECT_FALSE(Accepts(a, ""));
}

TEST_F(RegexpUtilsTest, perl_class_non_word) {
  auto a = irs::FromRegexp("\\W+");
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, "!@#"));
  EXPECT_TRUE(Accepts(a, "  "));
  EXPECT_TRUE(Accepts(a, "---"));
  EXPECT_FALSE(Accepts(a, "abc"));
  EXPECT_FALSE(Accepts(a, "123"));
  EXPECT_FALSE(Accepts(a, "_"));  // _ is word char
  EXPECT_FALSE(Accepts(a, ""));
}

TEST_F(RegexpUtilsTest, perl_class_non_whitespace) {
  auto a = irs::FromRegexp("\\S+");
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, "abc"));
  EXPECT_TRUE(Accepts(a, "123"));
  EXPECT_TRUE(Accepts(a, "!@#"));
  EXPECT_FALSE(Accepts(a, "   "));
  EXPECT_FALSE(Accepts(a, "abc def"));  // space breaks
  EXPECT_FALSE(Accepts(a, ""));
}

TEST_F(RegexpUtilsTest, perl_class_combined) {
  // \d+\D+\d+ - digits, then non-digits, then digits
  auto a = irs::FromRegexp("\\d+\\D+\\d+");
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, "12abc34"));
  EXPECT_TRUE(Accepts(a, "1x2"));
  EXPECT_FALSE(Accepts(a, "abc"));
  EXPECT_FALSE(Accepts(a, "123"));
  EXPECT_FALSE(Accepts(a, "12"));
}

// Non-ASCII case folding
//
// Our fix uses RE2's LookupCaseFold + ApplyFold, which covers the full
// Unicode fold map.  ASCII tests prove the mechanism works; these verify
// it holds for multi-byte alphabets.

TEST_F(RegexpUtilsTest, fold_case_cyrillic) {
  // Cyrillic a (U+0430) folds with U+0410 (uppercase A)
  auto a = irs::FromRegexp("(?i:\xD0\xB0)");  // (?i:а)
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, "\xD0\xB0"));   // а
  EXPECT_TRUE(Accepts(a, "\xD0\x90"));   // А
  EXPECT_FALSE(Accepts(a, "\xD0\xB1"));  // б
}

TEST_F(RegexpUtilsTest, fold_case_cyrillic_string) {
  // (?i:мир) - each letter folds independently
  auto a = irs::FromRegexp("(?i:\xD0\xBC\xD0\xB8\xD1\x80)");  // (?i:мир)
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, "\xD0\xBC\xD0\xB8\xD1\x80"));           // мир
  EXPECT_TRUE(Accepts(a, "\xD0\x9C\xD0\x98\xD0\xA0"));           // МИР
  EXPECT_TRUE(Accepts(a, "\xD0\x9C\xD0\xB8\xD1\x80"));           // Мир
  EXPECT_FALSE(Accepts(a, "\xD0\xBC\xD0\xB8\xD1\x80\xD0\xB0"));  // мира
}

TEST_F(RegexpUtilsTest, fold_case_latin_extended) {
  // Latin ñ (U+00F1) folds with Ñ (U+00D1)
  auto a = irs::FromRegexp("(?i:\xC3\xB1)");  // (?i:ñ)
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, "\xC3\xB1"));  // ñ
  EXPECT_TRUE(Accepts(a, "\xC3\x91"));  // Ñ
  EXPECT_FALSE(Accepts(a, "n"));
}

TEST_F(RegexpUtilsTest, fold_case_mixed_ascii_and_cyrillic) {
  // Pattern mixes ASCII and Cyrillic, both should fold independently
  auto a = irs::FromRegexp("(?i:aаb)");
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, "aаb"));
  EXPECT_TRUE(Accepts(a, "AАB"));
  EXPECT_TRUE(Accepts(a, "aАB"));
  EXPECT_FALSE(Accepts(a, "aбb"));
}

TEST_F(RegexpUtilsTest, fold_case_combined_with_quantifier) {
  auto a = irs::FromRegexp("(?i:abc)*");
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, ""));
  EXPECT_TRUE(Accepts(a, "abc"));
  EXPECT_TRUE(Accepts(a, "ABC"));
  EXPECT_TRUE(Accepts(a, "abcABCaBc"));
  EXPECT_FALSE(Accepts(a, "abd"));
}

TEST_F(RegexpUtilsTest, fold_case_combined_with_dot_star) {
  auto a = irs::FromRegexp("(?i:foo).*(?i:bar)");
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, "foobar"));
  EXPECT_TRUE(Accepts(a, "FOObar"));
  EXPECT_TRUE(Accepts(a, "fooBAR"));
  EXPECT_TRUE(Accepts(a, "FooXyZBar"));
  EXPECT_FALSE(Accepts(a, "foo"));
  EXPECT_FALSE(Accepts(a, "bar"));
}

TEST_F(RegexpUtilsTest, fold_case_with_perl_class) {
  // FoldCase flag applied to \d should be a no-op (digits have no case)
  auto a = irs::FromRegexp("(?i:\\d+)");
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, "123"));
  EXPECT_FALSE(Accepts(a, "abc"));
}

// Negated Unicode character classes
//
// BuildCharClass's complement path on multi-byte content -
// thin coverage currently.

TEST_F(RegexpUtilsTest, negated_unicode_property) {
  // \P{Cyrillic} = anything that isn't Cyrillic
  auto a = irs::FromRegexp("\\P{Cyrillic}+");
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, "hello"));
  EXPECT_TRUE(Accepts(a, "123"));
  EXPECT_TRUE(Accepts(a, "中文"));
  EXPECT_TRUE(Accepts(a, "\xC3\xA9"));        // é (Latin, not Cyrillic)
  EXPECT_FALSE(Accepts(a, "\xD0\xB0"));       // а (Cyrillic)
  EXPECT_FALSE(Accepts(a, "hello\xD0\xB0"));  // one Cyrillic char breaks
}

TEST_F(RegexpUtilsTest, negated_unicode_range) {
  // [^U+0400 .. U+04FF] = anything outside Cyrillic block
  auto a = irs::FromRegexp("[^\\x{400}-\\x{4FF}]+");
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, "hello"));
  EXPECT_TRUE(Accepts(a, "\xE4\xB8\xAD"));      // 中 (3-byte, outside Cyrillic)
  EXPECT_TRUE(Accepts(a, "\xF0\x9F\x98\x80"));  // 😀 (4-byte)
  EXPECT_FALSE(Accepts(a, "\xD0\xB0"));         // а (inside Cyrillic)
}

TEST_F(RegexpUtilsTest, negated_char_class_mixed_ascii_unicode) {
  // [^a-zа-я]+ = not ASCII lowercase and not Cyrillic lowercase
  auto a = irs::FromRegexp("[^a-z\xD0\xB0-\xD1\x8F]+");
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, "ABC"));  // uppercase ASCII ok
  EXPECT_TRUE(Accepts(a, "123"));
  EXPECT_TRUE(Accepts(a, "\xD0\x90"));   // А (Cyrillic upper) ok
  EXPECT_FALSE(Accepts(a, "abc"));       // lowercase ASCII excluded
  EXPECT_FALSE(Accepts(a, "\xD0\xB0"));  // а (Cyrillic lower) excluded
}

TEST_F(RegexpUtilsTest, negated_single_char) {
  auto a = irs::FromRegexp("[^a]");
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, "b"));
  EXPECT_TRUE(Accepts(a, "1"));
  EXPECT_TRUE(Accepts(a, " "));
  EXPECT_FALSE(Accepts(a, "a"));
  EXPECT_FALSE(Accepts(a, ""));
  EXPECT_FALSE(Accepts(a, "bb"));
}

// IsLiteralPrefixDotStar edge cases
//
// Patterns that superficially look like prefix + .* but should NOT be
// classified as Prefix - the classifier's definition is "literal
// prefix, then .* at the very end, nothing else".

TEST_F(RegexpUtilsTest, classify_prefix_two_dotstars) {
  // "a.*.*" - has unescaped . in the middle, not just the final .*
  // -> Complex (not Prefix)
  EXPECT_EQ(irs::RegexpType::Complex,
            irs::ComputeRegexpType(ToBytesView("a.*.*")));
}

TEST_F(RegexpUtilsTest, classify_prefix_star_before_dotstar) {
  // "abc*.*" - has unescaped * before .*, so the "prefix" would contain
  // metacharacters. -> Complex.
  EXPECT_EQ(irs::RegexpType::Complex,
            irs::ComputeRegexpType(ToBytesView("abc*.*")));
}

TEST_F(RegexpUtilsTest, classify_prefix_inner_dot) {
  // "a.b.*" - unescaped dot in prefix position -> Complex
  EXPECT_EQ(irs::RegexpType::Complex,
            irs::ComputeRegexpType(ToBytesView("a.b.*")));
}

TEST_F(RegexpUtilsTest, classify_prefix_only_dot_no_star) {
  // "foo." - literal prefix with trailing unescaped dot, no .* tail
  // -> Complex (not Prefix, because there's no trailing *)
  EXPECT_EQ(irs::RegexpType::Complex,
            irs::ComputeRegexpType(ToBytesView("foo.")));
}

TEST_F(RegexpUtilsTest, classify_prefix_only_star_no_dot) {
  // "foo*" - ends in *, but * preceded by 'o' (literal), not '.'
  // -> Complex
  EXPECT_EQ(irs::RegexpType::Complex,
            irs::ComputeRegexpType(ToBytesView("foo*")));
}

TEST_F(RegexpUtilsTest, classify_prefix_empty_prefix_dotstar) {
  // ".*" - empty prefix + .* -> Prefix (already covered elsewhere,
  // anchoring again in this context)
  EXPECT_EQ(irs::RegexpType::Prefix, irs::ComputeRegexpType(ToBytesView(".*")));
}

TEST_F(RegexpUtilsTest, classify_prefix_escaped_dotstar_in_middle) {
  // "a\.*" - escaped dot, then *.  The * is applied to the literal dot,
  // so the pattern is "a, then zero-or-more dots".  -> Complex.
  EXPECT_EQ(irs::RegexpType::Complex,
            irs::ComputeRegexpType(ToBytesView("a\\.*")));
}

// Multi-tier character classes
//
// Char class combining 1/2/3/4-byte UTF-8 codepoints in one class -
// stresses the Union-based path in BuildCharClass where each
// tier becomes a separate UTF-8 range automaton before being unioned.

TEST_F(RegexpUtilsTest, char_class_all_four_byte_tiers) {
  // a (1-byte) + б (2-byte) + 中 (3-byte) + 😀 (4-byte)
  auto a = irs::FromRegexp("[a\xD0\xB1\xE4\xB8\xAD\xF0\x9F\x98\x80]+");
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, "a"));
  EXPECT_TRUE(Accepts(a, "\xD0\xB1"));          // б
  EXPECT_TRUE(Accepts(a, "\xE4\xB8\xAD"));      // 中
  EXPECT_TRUE(Accepts(a, "\xF0\x9F\x98\x80"));  // 😀
  EXPECT_TRUE(
    Accepts(a, "a\xD0\xB1\xE4\xB8\xAD\xF0\x9F\x98\x80"));  // all together
  EXPECT_FALSE(Accepts(a, "b"));
  EXPECT_FALSE(Accepts(a, ""));
}

TEST_F(RegexpUtilsTest, char_class_ranges_across_tiers) {
  // Three ranges, one in each byte-length tier:
  //   a-z        (ASCII, 1-byte)
  //   а-я        (Cyrillic lower, 2-byte: U+0430 - U+044F)
  //   U+4E00-U+4FFF  (portion of CJK block, 3-byte)
  auto a = irs::FromRegexp("[a-z\xD0\xB0-\xD1\x8F\xE4\xB8\x80-\xE4\xBF\xBF]+");
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, "abc"));  // ASCII
  EXPECT_TRUE(
    Accepts(a, "\xD0\xBF\xD1\x80\xD0\xB8"));  // при (Cyrillic, in range)
  EXPECT_TRUE(Accepts(a, "\xE4\xB8\xAD"));    // 中 (U+4E2D, in CJK range)
  EXPECT_TRUE(
    Accepts(a, "a\xD0\xBF\xE4\xB8\xAD"));  // mixed: a + п + 中 all in range
  EXPECT_FALSE(Accepts(a, "ABC"));         // uppercase not in range
  EXPECT_FALSE(Accepts(
    a, "\xE4\xB8\xAD\xE6\x96\x87"));  // 中文 - 文 (U+6587) outside range
  EXPECT_FALSE(Accepts(a, ""));
}

TEST_F(RegexpUtilsTest, negated_char_class_all_four_tiers) {
  // [^aб中😀]+ - exclude one from each tier
  auto a = irs::FromRegexp("[^a\xD0\xB1\xE4\xB8\xAD\xF0\x9F\x98\x80]+");
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, "b"));
  EXPECT_TRUE(Accepts(a, "\xD0\xB2"));      // в (different Cyrillic)
  EXPECT_TRUE(Accepts(a, "\xE6\x96\x87"));  // 文 (different CJK)
  EXPECT_FALSE(Accepts(a, "a"));
  EXPECT_FALSE(Accepts(a, "\xD0\xB1"));  // б
}

// Simplify() stress - large counted quantifiers
//
// RE2's Simplify() expands {n,m} into Concat/Quest sequences.  Large
// counts stress both the AST expansion and downstream DeterminizeStar.
// These shouldn't crash or time out.

TEST_F(RegexpUtilsTest, simplify_large_exact_count) {
  auto a = irs::FromRegexp("a{100}");
  // Either succeeds or hits DFA limit - both acceptable
  if (a.NumStates() > 0) {
    AssertProperties(a);
    EXPECT_TRUE(Accepts(a, std::string(100, 'a')));
    EXPECT_FALSE(Accepts(a, std::string(99, 'a')));
    EXPECT_FALSE(Accepts(a, std::string(101, 'a')));
  }
}

TEST_F(RegexpUtilsTest, simplify_large_bounded_range) {
  auto a = irs::FromRegexp("a{50,100}");
  if (a.NumStates() > 0) {
    AssertProperties(a);
    EXPECT_TRUE(Accepts(a, std::string(50, 'a')));
    EXPECT_TRUE(Accepts(a, std::string(75, 'a')));
    EXPECT_TRUE(Accepts(a, std::string(100, 'a')));
    EXPECT_FALSE(Accepts(a, std::string(49, 'a')));
    EXPECT_FALSE(Accepts(a, std::string(101, 'a')));
  }
}

TEST_F(RegexpUtilsTest, simplify_large_open_range) {
  // a{50,} - 50 or more a's.  This produces a Plus after the first 50.
  auto a = irs::FromRegexp("a{50,}");
  if (a.NumStates() > 0) {
    AssertProperties(a);
    EXPECT_TRUE(Accepts(a, std::string(50, 'a')));
    EXPECT_TRUE(Accepts(a, std::string(100, 'a')));
    EXPECT_FALSE(Accepts(a, std::string(49, 'a')));
  }
}

TEST_F(RegexpUtilsTest, simplify_nested_counted_group) {
  // Walker's Copy() path - after Simplify, repeated subtrees may share
  // nodes (DAG), so Copy gets called.
  auto a = irs::FromRegexp("(ab){3,5}");
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, "ababab"));           // 3
  EXPECT_TRUE(Accepts(a, "abababab"));         // 4
  EXPECT_TRUE(Accepts(a, "ababababab"));       // 5
  EXPECT_FALSE(Accepts(a, "abab"));            // 2
  EXPECT_FALSE(Accepts(a, "ababababababab"));  // 7
}

TEST_F(RegexpUtilsTest, posix_class_alpha) {
  auto a = irs::FromRegexp("[[:alpha:]]+");
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, "abc"));
  EXPECT_TRUE(Accepts(a, "ABC"));
  EXPECT_TRUE(Accepts(a, "aBcDeF"));
  EXPECT_FALSE(Accepts(a, "123"));
  EXPECT_FALSE(Accepts(a, "abc123"));
  EXPECT_FALSE(Accepts(a, ""));
}

TEST_F(RegexpUtilsTest, posix_class_digit) {
  auto a = irs::FromRegexp("[[:digit:]]+");
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, "0"));
  EXPECT_TRUE(Accepts(a, "12345"));
  EXPECT_FALSE(Accepts(a, "abc"));
  EXPECT_FALSE(Accepts(a, "1a2"));
}

TEST_F(RegexpUtilsTest, posix_class_alnum) {
  auto a = irs::FromRegexp("[[:alnum:]]+");
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, "abc"));
  EXPECT_TRUE(Accepts(a, "123"));
  EXPECT_TRUE(Accepts(a, "abc123"));
  EXPECT_TRUE(Accepts(a, "ABC"));
  EXPECT_FALSE(Accepts(a, "abc_"));  // underscore not alnum
  EXPECT_FALSE(Accepts(a, " "));
  EXPECT_FALSE(Accepts(a, ""));
}

TEST_F(RegexpUtilsTest, posix_class_space) {
  auto a = irs::FromRegexp("[[:space:]]+");
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, " "));
  EXPECT_TRUE(Accepts(a, "   "));
  EXPECT_TRUE(Accepts(a, "\t"));
  EXPECT_FALSE(Accepts(a, "abc"));
  EXPECT_FALSE(Accepts(a, ""));
}

TEST_F(RegexpUtilsTest, posix_class_upper_lower) {
  {
    auto a = irs::FromRegexp("[[:upper:]]+");
    AssertProperties(a);
    EXPECT_TRUE(Accepts(a, "ABC"));
    EXPECT_FALSE(Accepts(a, "abc"));
    EXPECT_FALSE(Accepts(a, "Abc"));
  }
  {
    auto a = irs::FromRegexp("[[:lower:]]+");
    AssertProperties(a);
    EXPECT_TRUE(Accepts(a, "abc"));
    EXPECT_FALSE(Accepts(a, "ABC"));
    EXPECT_FALSE(Accepts(a, "aBc"));
  }
}

TEST_F(RegexpUtilsTest, posix_class_negated) {
  // [[:^alpha:]] = anything that's NOT an alpha char
  auto a = irs::FromRegexp("[[:^alpha:]]+");
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, "123"));
  EXPECT_TRUE(Accepts(a, "   "));
  EXPECT_TRUE(Accepts(a, "!@#"));
  EXPECT_FALSE(Accepts(a, "abc"));
  EXPECT_FALSE(Accepts(a, "a1"));
}

TEST_F(RegexpUtilsTest, posix_class_combined_with_range) {
  // POSIX class inside a regular char class, combined with other ranges
  auto a = irs::FromRegexp("[[:digit:]_]+");
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, "123"));
  EXPECT_TRUE(Accepts(a, "_"));
  EXPECT_TRUE(Accepts(a, "1_2_3"));
  EXPECT_FALSE(Accepts(a, "abc"));
  EXPECT_FALSE(Accepts(a, ""));
}

// C-style escapes in patterns
//
// \n, \t, \r, \f, \v are not regexp metacharacters - RE2 parses them as
// literal control characters.  ComputeRegexpType routes them to Complex
// (because \n is not a "simple escape" in our sense).

TEST_F(RegexpUtilsTest, escape_newline) {
  auto a = irs::FromRegexp("a\\nb");
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, "a\nb"));
  EXPECT_FALSE(Accepts(a, "ab"));
  EXPECT_FALSE(Accepts(a, "a b"));
}

TEST_F(RegexpUtilsTest, escape_tab) {
  auto a = irs::FromRegexp("a\\tb");
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, "a\tb"));
  EXPECT_FALSE(Accepts(a, "ab"));
  EXPECT_FALSE(Accepts(a, "a b"));
}

TEST_F(RegexpUtilsTest, escape_cr_ff_vt) {
  {
    auto a = irs::FromRegexp("\\r");
    AssertProperties(a);
    EXPECT_TRUE(Accepts(a, "\r"));
    EXPECT_FALSE(Accepts(a, "\n"));
  }
  {
    auto a = irs::FromRegexp("\\f");
    AssertProperties(a);
    EXPECT_TRUE(Accepts(a, "\f"));
  }
  {
    auto a = irs::FromRegexp("\\v");
    AssertProperties(a);
    EXPECT_TRUE(Accepts(a, "\v"));
  }
}

TEST_F(RegexpUtilsTest, escape_cstyle_classified_as_complex) {
  // Double-check that our classifier routes these through the full
  // automaton path (they change matching semantics).
  EXPECT_EQ(irs::RegexpType::Complex,
            irs::ComputeRegexpType(ToBytesView("\\n")));
  EXPECT_EQ(irs::RegexpType::Complex,
            irs::ComputeRegexpType(ToBytesView("\\t")));
  EXPECT_EQ(irs::RegexpType::Complex,
            irs::ComputeRegexpType(ToBytesView("foo\\n")));
}

// Hex and octal codepoint escapes
//
// \x20 (2-digit hex), \x{1F600} (braced hex), \042 (octal) - all produce
// a specific literal rune.

TEST_F(RegexpUtilsTest, hex_escape_two_digit) {
  // \x41 = 'A'
  auto a = irs::FromRegexp("\\x41");
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, "A"));
  EXPECT_FALSE(Accepts(a, "a"));
  EXPECT_FALSE(Accepts(a, ""));
}

TEST_F(RegexpUtilsTest, hex_escape_braced_ascii) {
  // \x{20} = space
  auto a = irs::FromRegexp("a\\x{20}b");
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, "a b"));
  EXPECT_FALSE(Accepts(a, "ab"));
}

TEST_F(RegexpUtilsTest, hex_escape_braced_unicode) {
  // \x{4E2D} = 中 (U+4E2D, 3-byte UTF-8)
  auto a = irs::FromRegexp("\\x{4E2D}");
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, "\xE4\xB8\xAD"));   // 中
  EXPECT_FALSE(Accepts(a, "\xE6\x96\x87"));  // 文 (different char)
}

TEST_F(RegexpUtilsTest, hex_escape_braced_emoji) {
  // \x{1F600} = 😀 (U+1F600, 4-byte UTF-8)
  auto a = irs::FromRegexp("\\x{1F600}");
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, "\xF0\x9F\x98\x80"));   // 😀
  EXPECT_FALSE(Accepts(a, "\xF0\x9F\x8E\x89"));  // 🎉
}

// Full-class detection (cc->full() branch in BuildCharClass)
//
// Patterns like [\d\D], [\w\W], [\s\S] are equivalent to "any character"
// and RE2 collapses them into a full CharClass.  This hits the
// `if (cc->full()) return MakeAny();` branch that's otherwise untested.

TEST_F(RegexpUtilsTest, full_class_digit_nondigit) {
  // [\d\D] = digits + non-digits = everything
  auto a = irs::FromRegexp("[\\d\\D]+");
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, "abc"));
  EXPECT_TRUE(Accepts(a, "123"));
  EXPECT_TRUE(Accepts(a, "!@# $%^"));
  EXPECT_TRUE(Accepts(a, "abc123!@#"));
  EXPECT_FALSE(Accepts(a, ""));
}

TEST_F(RegexpUtilsTest, full_class_word_nonword) {
  // [\w\W] = word chars + non-word chars = everything
  auto a = irs::FromRegexp("[\\w\\W]+");
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, "abc"));
  EXPECT_TRUE(Accepts(a, "!@#"));
  EXPECT_TRUE(Accepts(a, "abc !@#"));
  EXPECT_FALSE(Accepts(a, ""));
}

TEST_F(RegexpUtilsTest, full_class_space_nonspace) {
  // [\s\S] = whitespace + non-whitespace = everything
  auto a = irs::FromRegexp("[\\s\\S]+");
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, "abc"));
  EXPECT_TRUE(Accepts(a, "   "));
  EXPECT_TRUE(Accepts(a, "abc   def"));
  EXPECT_FALSE(Accepts(a, ""));
}

// Flag negation and DotNL
//
// (?-i:...) turns off FoldCase locally.  (?s:...) turns on DotNL so that
// . matches newlines.  These verify flag propagation through AST nodes,
// not just top-level application.

TEST_F(RegexpUtilsTest, flag_negate_case_insensitive) {
  // (?i:a(?-i:b)c) - outer is FoldCase, inner 'b' has it turned off.
  // So: 'a' matches {a,A}, 'b' matches only literal 'b', 'c' matches {c,C}.
  auto a = irs::FromRegexp("(?i:a(?-i:b)c)");
  AssertProperties(a);
  // Valid: 'a'-or-'A' + literal 'b' + 'c'-or-'C'
  EXPECT_TRUE(Accepts(a, "abc"));
  EXPECT_TRUE(Accepts(a, "Abc"));
  EXPECT_TRUE(Accepts(a, "abC"));
  EXPECT_TRUE(Accepts(a, "AbC"));
  // Invalid: 'B' in middle - FoldCase is off there
  EXPECT_FALSE(Accepts(a, "aBc"));
  EXPECT_FALSE(Accepts(a, "ABc"));
  EXPECT_FALSE(Accepts(a, "ABC"));
  // Invalid: wrong first/last char
  EXPECT_FALSE(Accepts(a, "xbc"));
}

TEST_F(RegexpUtilsTest, flag_negate_case_insensitive_standalone) {
  // (?-i:abc) alone - FoldCase is off, same as plain abc
  auto a = irs::FromRegexp("(?-i:abc)");
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, "abc"));
  EXPECT_FALSE(Accepts(a, "ABC"));
  EXPECT_FALSE(Accepts(a, "Abc"));
}

TEST_F(RegexpUtilsTest, flag_dot_nl_enabled) {
  // (?s:.) - dot matches newline when DotNL is on
  auto a = irs::FromRegexp("(?s:.)");
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, "a"));
  EXPECT_TRUE(Accepts(a, "\n"));
  EXPECT_TRUE(Accepts(a, " "));
  EXPECT_FALSE(Accepts(a, ""));
  EXPECT_FALSE(Accepts(a, "ab"));
}

TEST_F(RegexpUtilsTest, flag_dot_nl_disabled_by_default) {
  // Plain . (LikePerl default) - does NOT match newline
  auto a = irs::FromRegexp(".");
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, "a"));
  EXPECT_FALSE(Accepts(a, "\n"));  // key assertion - default is DotNL off
}

// UTF-8 literal prefix classification
//
// Practical case: pattern like "привет.*" must classify as Prefix so the
// filter routes to ByPrefix fast-path.  If it regresses to Complex, UTF-8
// prefix searches silently lose the fast path.

TEST_F(RegexpUtilsTest, classify_utf8_cyrillic_prefix) {
  EXPECT_EQ(irs::RegexpType::Prefix,
            irs::ComputeRegexpType(ToBytesView("привет.*")));
}

TEST_F(RegexpUtilsTest, classify_utf8_chinese_prefix) {
  EXPECT_EQ(irs::RegexpType::Prefix,
            irs::ComputeRegexpType(ToBytesView("中文.*")));
}

TEST_F(RegexpUtilsTest, classify_utf8_emoji_prefix) {
  EXPECT_EQ(irs::RegexpType::Prefix,
            irs::ComputeRegexpType(ToBytesView("😀.*")));
}

TEST_F(RegexpUtilsTest, classify_utf8_mixed_prefix) {
  EXPECT_EQ(irs::RegexpType::Prefix,
            irs::ComputeRegexpType(ToBytesView("aб中😀.*")));
}

TEST_F(RegexpUtilsTest, classify_utf8_literal) {
  // UTF-8 string with no metacharacters at all -> Literal
  EXPECT_EQ(irs::RegexpType::Literal,
            irs::ComputeRegexpType(ToBytesView("привет")));
  EXPECT_EQ(irs::RegexpType::Literal,
            irs::ComputeRegexpType(ToBytesView("中文")));
  EXPECT_EQ(irs::RegexpType::Literal,
            irs::ComputeRegexpType(ToBytesView("😀")));
}

TEST_F(RegexpUtilsTest, extract_utf8_prefix) {
  // Verify ExtractRegexpPrefix also works for UTF-8 prefixes
  EXPECT_EQ("привет", irs::ViewCast<char>(
                        irs::ExtractRegexpPrefix(ToBytesView("привет.*"))));
  EXPECT_EQ("中文", irs::ViewCast<char>(
                      irs::ExtractRegexpPrefix(ToBytesView("中文.*"))));
}

// Escapes inside character classes
//
// Perl-class escapes like \d, \w, \s work both as standalone and inside
// [...].  Inside a class, they combine with other elements via union,
// exercising a different Simplify path.

TEST_F(RegexpUtilsTest, escape_digit_in_class) {
  auto a = irs::FromRegexp("[\\d]+");
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, "0"));
  EXPECT_TRUE(Accepts(a, "12345"));
  EXPECT_FALSE(Accepts(a, "abc"));
  EXPECT_FALSE(Accepts(a, ""));
}

TEST_F(RegexpUtilsTest, escape_multiple_perl_in_class) {
  // [\d\s] = digits or whitespace
  auto a = irs::FromRegexp("[\\d\\s]+");
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, "123"));
  EXPECT_TRUE(Accepts(a, "   "));
  EXPECT_TRUE(Accepts(a, "1 2 3"));
  EXPECT_FALSE(Accepts(a, "abc"));
  EXPECT_FALSE(Accepts(a, "a 1"));
}

TEST_F(RegexpUtilsTest, escape_negated_perl_in_class) {
  // [^\d] = non-digit
  auto a = irs::FromRegexp("[^\\d]+");
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, "abc"));
  EXPECT_TRUE(Accepts(a, "   "));
  EXPECT_TRUE(Accepts(a, "!@#"));
  EXPECT_FALSE(Accepts(a, "123"));
  EXPECT_FALSE(Accepts(a, "abc1"));
}

TEST_F(RegexpUtilsTest, escape_perl_with_literal_in_class) {
  // [\d_] = digit or underscore
  auto a = irs::FromRegexp("[\\d_]+");
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, "123"));
  EXPECT_TRUE(Accepts(a, "___"));
  EXPECT_TRUE(Accepts(a, "1_2_3"));
  EXPECT_FALSE(Accepts(a, "abc"));
}

TEST_F(RegexpUtilsTest, escape_unicode_property_in_class) {
  // [\p{Cyrillic}_] = Cyrillic letter or underscore
  auto a = irs::FromRegexp("[\\p{Cyrillic}_]+");
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, "абв"));
  EXPECT_TRUE(Accepts(a, "__"));
  EXPECT_TRUE(Accepts(a, "а_б"));
  EXPECT_FALSE(Accepts(a, "abc"));
}
