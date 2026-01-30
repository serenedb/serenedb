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
  ASSERT_EQ(irs::RegexpType::Literal,
            irs::ComputeRegexpType(ToBytesView("foo\\.bar")));
  ASSERT_EQ(irs::RegexpType::Literal,
            irs::ComputeRegexpType(ToBytesView("a\\*b")));
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
  ASSERT_EQ(irs::RegexpType::Complex,
            irs::ComputeRegexpType(ToBytesView(".*")));
  ASSERT_EQ(irs::RegexpType::Complex,
            irs::ComputeRegexpType(ToBytesView("fo+.*")));
  ASSERT_EQ(irs::RegexpType::Complex,
            irs::ComputeRegexpType(ToBytesView("a|b.*")));
}

TEST_F(RegexpUtilsTest, extract_prefix) {
  EXPECT_EQ(
    "foo", irs::ViewCast<char>(irs::ExtractRegexpPrefix(ToBytesView("foo.*"))));
  EXPECT_EQ(
    "abc", irs::ViewCast<char>(irs::ExtractRegexpPrefix(ToBytesView("abc.*"))));
  EXPECT_EQ("x",
            irs::ViewCast<char>(irs::ExtractRegexpPrefix(ToBytesView("x.*"))));
  EXPECT_EQ("",
            irs::ViewCast<char>(irs::ExtractRegexpPrefix(ToBytesView(".*"))));
}

TEST_F(RegexpUtilsTest, match_empty) {
  auto a = irs::FromRegexp("");
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, ""));
  EXPECT_FALSE(Accepts(a, "a"));
}

TEST_F(RegexpUtilsTest, match_literal) {
  auto a = irs::FromRegexp("foo");
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, "foo"));
  EXPECT_FALSE(Accepts(a, "fo"));
  EXPECT_FALSE(Accepts(a, "fooo"));
  EXPECT_FALSE(Accepts(a, "bar"));
  EXPECT_FALSE(Accepts(a, ""));
}

TEST_F(RegexpUtilsTest, match_single_char) {
  auto a = irs::FromRegexp("a");
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, "a"));
  EXPECT_FALSE(Accepts(a, ""));
  EXPECT_FALSE(Accepts(a, "aa"));
  EXPECT_FALSE(Accepts(a, "b"));
}

TEST_F(RegexpUtilsTest, match_dot) {
  {
    auto a = irs::FromRegexp("a.c");
    AssertProperties(a);
    EXPECT_TRUE(Accepts(a, "abc"));
    EXPECT_TRUE(Accepts(a, "aXc"));
    EXPECT_TRUE(Accepts(a, "a1c"));
    EXPECT_FALSE(Accepts(a, "ac"));
    EXPECT_FALSE(Accepts(a, "abbc"));
  }
  {
    auto a = irs::FromRegexp("...");
    AssertProperties(a);
    EXPECT_TRUE(Accepts(a, "abc"));
    EXPECT_TRUE(Accepts(a, "123"));
    EXPECT_FALSE(Accepts(a, "ab"));
    EXPECT_FALSE(Accepts(a, "abcd"));
  }
  {
    auto a = irs::FromRegexp(".");
    AssertProperties(a);
    EXPECT_TRUE(Accepts(a, "a"));
    EXPECT_TRUE(Accepts(a, "X"));
    EXPECT_FALSE(Accepts(a, ""));
    EXPECT_FALSE(Accepts(a, "ab"));
  }
}

TEST_F(RegexpUtilsTest, match_star) {
  {
    auto a = irs::FromRegexp("ab*c");
    AssertProperties(a);
    EXPECT_TRUE(Accepts(a, "ac"));
    EXPECT_TRUE(Accepts(a, "abc"));
    EXPECT_TRUE(Accepts(a, "abbc"));
    EXPECT_TRUE(Accepts(a, "abbbbbc"));
    EXPECT_FALSE(Accepts(a, "aXc"));
  }
  {
    auto a = irs::FromRegexp("a*");
    AssertProperties(a);
    EXPECT_TRUE(Accepts(a, ""));
    EXPECT_TRUE(Accepts(a, "a"));
    EXPECT_TRUE(Accepts(a, "aaaa"));
    EXPECT_FALSE(Accepts(a, "b"));
  }
}

TEST_F(RegexpUtilsTest, match_plus) {
  {
    auto a = irs::FromRegexp("ab+c");
    AssertProperties(a);
    EXPECT_FALSE(Accepts(a, "ac"));
    EXPECT_TRUE(Accepts(a, "abc"));
    EXPECT_TRUE(Accepts(a, "abbc"));
    EXPECT_TRUE(Accepts(a, "abbbbbc"));
  }
  {
    auto a = irs::FromRegexp("a+");
    AssertProperties(a);
    EXPECT_FALSE(Accepts(a, ""));
    EXPECT_TRUE(Accepts(a, "a"));
    EXPECT_TRUE(Accepts(a, "aaaa"));
    EXPECT_FALSE(Accepts(a, "b"));
  }
}

TEST_F(RegexpUtilsTest, match_question) {
  {
    auto a = irs::FromRegexp("ab?c");
    AssertProperties(a);
    EXPECT_TRUE(Accepts(a, "ac"));
    EXPECT_TRUE(Accepts(a, "abc"));
    EXPECT_FALSE(Accepts(a, "abbc"));
  }
  {
    auto a = irs::FromRegexp("colou?r");
    AssertProperties(a);
    EXPECT_TRUE(Accepts(a, "color"));
    EXPECT_TRUE(Accepts(a, "colour"));
    EXPECT_FALSE(Accepts(a, "colouur"));
  }
}

TEST_F(RegexpUtilsTest, match_alternation) {
  {
    auto a = irs::FromRegexp("a|b");
    AssertProperties(a);
    EXPECT_TRUE(Accepts(a, "a"));
    EXPECT_TRUE(Accepts(a, "b"));
    EXPECT_FALSE(Accepts(a, "c"));
    EXPECT_FALSE(Accepts(a, "ab"));
  }
  {
    auto a = irs::FromRegexp("cat|dog|bird");
    AssertProperties(a);
    EXPECT_TRUE(Accepts(a, "cat"));
    EXPECT_TRUE(Accepts(a, "dog"));
    EXPECT_TRUE(Accepts(a, "bird"));
    EXPECT_FALSE(Accepts(a, "fish"));
    EXPECT_FALSE(Accepts(a, "catdog"));
  }
  {
    auto a = irs::FromRegexp("foo|foobar");
    AssertProperties(a);
    EXPECT_TRUE(Accepts(a, "foo"));
    EXPECT_TRUE(Accepts(a, "foobar"));
    EXPECT_FALSE(Accepts(a, "foob"));
  }
  {
    auto a = irs::FromRegexp("a|");
    AssertProperties(a);
    EXPECT_TRUE(Accepts(a, "a"));
    EXPECT_TRUE(Accepts(a, ""));
  }
}

TEST_F(RegexpUtilsTest, match_grouping) {
  {
    auto a = irs::FromRegexp("(ab)+");
    AssertProperties(a);
    EXPECT_FALSE(Accepts(a, ""));
    EXPECT_TRUE(Accepts(a, "ab"));
    EXPECT_TRUE(Accepts(a, "abab"));
    EXPECT_TRUE(Accepts(a, "ababab"));
    EXPECT_FALSE(Accepts(a, "a"));
    EXPECT_FALSE(Accepts(a, "aba"));
  }
  {
    auto a = irs::FromRegexp("(a|b)*");
    AssertProperties(a);
    EXPECT_TRUE(Accepts(a, ""));
    EXPECT_TRUE(Accepts(a, "a"));
    EXPECT_TRUE(Accepts(a, "b"));
    EXPECT_TRUE(Accepts(a, "ababab"));
    EXPECT_TRUE(Accepts(a, "aaabbb"));
    EXPECT_FALSE(Accepts(a, "c"));
  }
  {
    auto a = irs::FromRegexp("(foo)?bar");
    AssertProperties(a);
    EXPECT_TRUE(Accepts(a, "bar"));
    EXPECT_TRUE(Accepts(a, "foobar"));
    EXPECT_FALSE(Accepts(a, "foofoobar"));
  }
  {
    auto a = irs::FromRegexp("((ab)+c)+");
    AssertProperties(a);
    EXPECT_TRUE(Accepts(a, "abc"));
    EXPECT_TRUE(Accepts(a, "ababc"));
    EXPECT_TRUE(Accepts(a, "abcabc"));
    EXPECT_FALSE(Accepts(a, "ab"));
  }
}

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

TEST_F(RegexpUtilsTest, match_char_class_range) {
  {
    auto a = irs::FromRegexp("[a-c]");
    AssertProperties(a);
    EXPECT_TRUE(Accepts(a, "a"));
    EXPECT_TRUE(Accepts(a, "b"));
    EXPECT_TRUE(Accepts(a, "c"));
    EXPECT_FALSE(Accepts(a, "d"));
  }
  {
    auto a = irs::FromRegexp("[0-9]+");
    AssertProperties(a);
    EXPECT_TRUE(Accepts(a, "0"));
    EXPECT_TRUE(Accepts(a, "123"));
    EXPECT_TRUE(Accepts(a, "9876543210"));
    EXPECT_FALSE(Accepts(a, ""));
    EXPECT_FALSE(Accepts(a, "12a34"));
  }
  {
    auto a = irs::FromRegexp("[a-zA-Z]+");
    AssertProperties(a);
    EXPECT_TRUE(Accepts(a, "abc"));
    EXPECT_TRUE(Accepts(a, "ABC"));
    EXPECT_TRUE(Accepts(a, "AbCdEf"));
    EXPECT_FALSE(Accepts(a, "abc123"));
  }
}

TEST_F(RegexpUtilsTest, match_char_class_escape) {
  auto a = irs::FromRegexp("[\\-\\]]");
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, "-"));
  EXPECT_TRUE(Accepts(a, "]"));
  EXPECT_FALSE(Accepts(a, "a"));
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
    EXPECT_TRUE(Accepts(a, "ab"));  
    EXPECT_TRUE(Accepts(a, "aXb"));
    EXPECT_TRUE(Accepts(a, "aXXXb"));
  }
  {
    auto a = irs::FromRegexp("a.+b");
    AssertProperties(a);
    EXPECT_FALSE(Accepts(a, "ab"));  
    EXPECT_TRUE(Accepts(a, "aXb"));
    EXPECT_TRUE(Accepts(a, "aXXXb"));
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

TEST_F(RegexpUtilsTest, edge_single_dot) {
  auto a = irs::FromRegexp(".");
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, "a"));
  EXPECT_FALSE(Accepts(a, ""));
  EXPECT_FALSE(Accepts(a, "ab"));
}

TEST_F(RegexpUtilsTest, edge_empty_alternation_branch) {
  auto a = irs::FromRegexp("|a");
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, ""));
  EXPECT_TRUE(Accepts(a, "a"));
  EXPECT_FALSE(Accepts(a, "b"));
}

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
