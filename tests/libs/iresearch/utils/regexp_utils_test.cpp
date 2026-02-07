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
  ASSERT_EQ(irs::RegexpType::Literal,
            irs::ComputeRegexpType(ToBytesView("foo\\.bar")));
  ASSERT_EQ(irs::RegexpType::Literal,
            irs::ComputeRegexpType(ToBytesView("a\\*b")));
  ASSERT_EQ(irs::RegexpType::Literal,
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
  EXPECT_EQ("hello world",
            irs::ViewCast<char>(irs::ExtractRegexpPrefix(ToBytesView("hello world.*"))));
}

// Basic patterns - literals and empty

TEST_F(RegexpUtilsTest, match_empty) {
  auto a = irs::FromRegexp("");
  AssertProperties(a);
  EXPECT_TRUE(Accepts(a, ""));
  EXPECT_FALSE(Accepts(a, "a"));
  EXPECT_FALSE(Accepts(a, " "));
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
  EXPECT_TRUE(Accepts(a, "a c"));  // space
  EXPECT_FALSE(Accepts(a, "ac"));   // no char between
  EXPECT_FALSE(Accepts(a, "abbc")); // two chars
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
  EXPECT_FALSE(Accepts(a, "abbc")); h
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
  EXPECT_TRUE(Accepts(a, "ad"));       // a+ b* c? d
  EXPECT_TRUE(Accepts(a, "abd"));      // a+ b* c? d
  EXPECT_TRUE(Accepts(a, "acd"));      // a+ b* c? d
  EXPECT_TRUE(Accepts(a, "abcd"));     // all present
  EXPECT_TRUE(Accepts(a, "aaabbbcd")); // multiple a, b
  EXPECT_FALSE(Accepts(a, "d"));       // missing a+
  EXPECT_FALSE(Accepts(a, "abccd"));   // two c
}
