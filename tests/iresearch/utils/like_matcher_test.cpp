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

#include <gtest/gtest.h>
#include <re2/re2.h>

#include <algorithm>
#include <fstream>
#include <iresearch/utils/like_matcher.hpp>
#include <random>
#include <string>
#include <string_view>
#include <vector>

#include "tests_shared.hpp"

namespace {

irs::bytes_view B(std::string_view s) {
  return irs::ViewCast<irs::byte_type>(s);
}

std::string Printable(std::string_view s) {
  static constexpr std::string_view kHex = "0123456789ABCDEF";
  std::string out;
  for (const auto c : s) {
    const auto b = static_cast<unsigned char>(c);
    if (b >= 0x20 && b < 0x7F && b != '\\') {
      out += c;
    } else {
      out += "\\x";
      out += kHex[b >> 4];
      out += kHex[b & 0xF];
    }
  }
  return out;
}

class Re2Like {
 public:
  explicit Re2Like(std::string_view pattern) : _re{Regex(pattern), Options()} {}

  bool ok() const { return _re.ok(); }

  bool Match(std::string_view text) const {
    return RE2::PartialMatch(text, _re);
  }

 private:
  static std::string Regex(std::string_view pattern) {
    static constexpr std::string_view kMeta = "\\[](){}.*+?|^$";
    std::string regex = "\\A";
    bool escaped = false;
    for (const auto c : pattern) {
      if (escaped) {
        escaped = false;
      } else if (c == '\\') {
        escaped = true;
        continue;
      } else if (c == '%') {
        regex += ".*";
        continue;
      } else if (c == '_') {
        regex += '.';
        continue;
      }
      if (kMeta.find(c) != std::string_view::npos) {
        regex += '\\';
      }
      regex += c;
    }
    regex += "\\z";
    return regex;
  }

  static RE2::Options Options() {
    RE2::Options options;
    options.set_dot_nl(true);
    options.set_log_errors(false);
    return options;
  }

  RE2 _re;
};

constexpr std::string_view kUnits[] = {
  "a",
  "b",
  "c",
  "%",
  "_",
  "\\",
  ".",
  "\n",
  std::string_view{"\0", 1},
  "\xC3\xA9",
  "\xE2\x82\xAC",
  "\xF0\x9F\x98\x80",
  "\xE0\x80\x80",
  "\xED\xA0\x80",
  "\xF4\x90\x80\x80",
};

constexpr std::string_view kJunk[] = {
  "\x80",
  "\xBF",
  "\xC0\x80",
  "\xC1\xBF",
  "\xFF",
  "\xF5\x80\x80\x80",
  "\xC3",
  "\xE2\x82",
  "\xF0\x9F\x98",
  "\xC3"
  "a",
};

constexpr std::string_view kPatternTokens[] = {
  "a",
  "b",
  "c",
  "%",
  "_",
  "\\",
  ".",
  "*",
  "(",
  "[",
  "^",
  "$",
  "{",
  "|",
  "\n",
  "\xC3\xA9",
  "\xE2\x82\xAC",
  "\xF0\x9F\x98\x80",
  "\xED\xA0\x80",
};

constexpr std::string_view kBadPatternTokens[] = {
  "\xFF", "\x80", "\xC3", "\xC0\x80", "\xE0\x80\x80", "\xF4\x90\x80\x80",
};

template<size_t N>
std::string_view Pick(std::mt19937& rng, const std::string_view (&from)[N]) {
  return from[std::uniform_int_distribution<size_t>{0, N - 1}(rng)];
}

bool Coin(std::mt19937& rng, int one_in) {
  return std::uniform_int_distribution<int>{0, one_in - 1}(rng) == 0;
}

std::string RandomUnits(std::mt19937& rng, size_t max) {
  std::string text;
  for (auto n = std::uniform_int_distribution<size_t>{0, max}(rng); n != 0;
       --n) {
    text += Coin(rng, 12) ? Pick(rng, kJunk) : Pick(rng, kUnits);
  }
  return text;
}

std::string Instantiate(std::mt19937& rng, std::string_view pattern) {
  std::string text;
  bool escaped = false;
  for (const auto c : pattern) {
    if (escaped) {
      escaped = false;
    } else if (c == '\\') {
      escaped = true;
      continue;
    } else if (c == '%') {
      text += RandomUnits(rng, 3);
      continue;
    } else if (c == '_') {
      text += Pick(rng, kUnits);
      continue;
    }
    text += c;
  }
  if (Coin(rng, 3) && !text.empty()) {
    const auto at = std::uniform_int_distribution<size_t>{0, text.size()}(rng);
    switch (std::uniform_int_distribution<int>{0, 2}(rng)) {
      case 0:
        text.insert(at, Coin(rng, 2) ? Pick(rng, kJunk) : Pick(rng, kUnits));
        break;
      case 1:
        text.erase(std::min(at, text.size() - 1), 1);
        break;
      default:
        text[std::min(at, text.size() - 1)] =
          static_cast<char>(std::uniform_int_distribution<int>{0, 255}(rng));
        break;
    }
  }
  return text;
}

void ExpectSameAsRe2(std::mt19937& rng, std::string_view pattern,
                     size_t texts) {
  const Re2Like re2{pattern};
  const irs::LikeMatcher matcher{B(pattern)};
  ASSERT_EQ(re2.ok(), matcher.ok()) << Printable(pattern);
  if (!matcher.ok()) {
    return;
  }
  for (size_t i = 0; i != texts; ++i) {
    const auto text =
      Coin(rng, 2) ? Instantiate(rng, pattern) : RandomUnits(rng, 10);
    ASSERT_EQ(re2.Match(text), matcher.Match(B(text)))
      << Printable(pattern) << " ~ " << Printable(text);
  }
}

}  // namespace

TEST(LikeMatcherTest, shapes) {
  struct Case {
    std::string_view pattern;
    std::string_view text;
    bool match;
  };
  static constexpr Case kCases[] = {
    {"", "", true},
    {"", "a", false},
    {"%", "", true},
    {"%", "abc", true},
    {"%%", "abc", true},
    {"abc", "abc", true},
    {"abc", "abcd", false},
    {"abc", "ab", false},
    {"abc%", "abcd", true},
    {"abc%", "xabc", false},
    {"%bcd", "abcd", true},
    {"%bcd", "abcdx", false},
    {"%bc%", "abcd", true},
    {"%bc%", "acbd", false},
    {"a%d", "abcd", true},
    {"a%d", "ad", true},
    {"a%c%d", "abcd", true},
    {"a%d%d", "abcd", false},
    {"ab%ba", "aba", false},
    {"ab%ba", "abba", true},
    {"%a%a%", "banana", true},
    {"%an%an%", "banana", true},
    {"%an%an%an%", "banana", false},
    {"_bcd", "abcd", true},
    {"a__d", "abcd", true},
    {"a_d", "abcd", false},
    {"%_%", "", false},
    {"%_%", "a", true},
    {"%__", "a", false},
    {"__%", "ab", true},
    {"%b_d%", "abcd", true},
    {"%b_d%", "abd", false},
    {"%_b_%", "abc", true},
    {"%_b_%", "bc", false},
    {"_", "\xC3\xA9", true},
    {"__", "\xC3\xA9", false},
    {"%\xC3\xA9%", "caf\xC3\xA9x", true},
    {"caf_", "caf\xC3\xA9", true},
    {"%_\xE2\x82\xAC", "a\xE2\x82\xAC", true},
    {"%_\xE2\x82\xAC", "\xE2\x82\xAC", false},
    {"a\\%b", "a%b", true},
    {"a\\%b", "axb", false},
    {"a\\_b", "a_b", true},
    {"a\\_b", "axb", false},
    {"a\\", "a", true},
    {"\\\\", "\\", true},
    {"\\a\\b", "ab", true},
  };
  for (const auto& [pattern, text, match] : kCases) {
    const irs::LikeMatcher matcher{B(pattern)};
    ASSERT_TRUE(matcher.ok()) << Printable(pattern);
    EXPECT_EQ(match, matcher.Match(B(text)))
      << Printable(pattern) << " ~ " << Printable(text);
  }
}

TEST(LikeMatcherTest, wildcards_consume_what_re2_dot_consumes) {
  const irs::LikeMatcher one{B("_")};
  const irs::LikeMatcher any{B("%")};
  const irs::LikeMatcher around{B("a%b")};
  for (const std::string_view unit :
       {"\xE0\x80\x80", "\xED\xA0\x80", "\xF0\x80\x80\x80", "\xF4\x90\x80\x80",
        "\xF4\xBF\xBF\xBF"}) {
    EXPECT_TRUE(one.Match(B(unit))) << Printable(unit);
    EXPECT_TRUE(any.Match(B(unit))) << Printable(unit);
    EXPECT_TRUE(around.Match(B("a" + std::string{unit} + "b")))
      << Printable(unit);
  }
  for (const auto junk : kJunk) {
    EXPECT_FALSE(one.Match(B(junk))) << Printable(junk);
    EXPECT_FALSE(any.Match(B(junk))) << Printable(junk);
    EXPECT_FALSE(around.Match(B("a" + std::string{junk} + "b")))
      << Printable(junk);
  }
}

TEST(LikeMatcherTest, rejects_patterns_re2_rejects) {
  for (const auto bad : kBadPatternTokens) {
    EXPECT_FALSE(irs::LikeMatcher{B(bad)}.ok()) << Printable(bad);
    EXPECT_FALSE(irs::LikeMatcher{B("%" + std::string{bad} + "_")}.ok())
      << Printable(bad);
  }
  EXPECT_FALSE(irs::LikeMatcher{B("\xC3%\xA9")}.ok());
  EXPECT_FALSE(irs::LikeMatcher{B("\xC3_\xA9")}.ok());
  EXPECT_TRUE(irs::LikeMatcher{B("\xC3\\\xA9")}.ok());
  EXPECT_TRUE(irs::LikeMatcher{B("\xED\xA0\x80")}.ok());
  EXPECT_TRUE(irs::LikeMatcher{B(std::string_view{"a\0b", 3})}.ok());
}

TEST(LikeMatcherTest, equality_is_by_structure) {
  EXPECT_EQ(irs::LikeMatcher{B("a\\b%")}, irs::LikeMatcher{B("ab%")});
  EXPECT_EQ(irs::LikeMatcher{B("a%%b")}, irs::LikeMatcher{B("a%b")});
  EXPECT_NE(irs::LikeMatcher{B("a%b")}, irs::LikeMatcher{B("a_b")});
  EXPECT_NE(irs::LikeMatcher{B("a%b")}, irs::LikeMatcher{B("ab")});
  EXPECT_NE(irs::LikeMatcher{B("a_b%")}, irs::LikeMatcher{B("a_%b")});
}

TEST(LikeMatcherTest, corpus_matches_re2) {
  std::ifstream corpus{TestEnv::resource("patterns/like.tsv")};
  ASSERT_TRUE(corpus.is_open());
  std::mt19937 rng{1139};
  size_t patterns = 0;
  for (std::string line; std::getline(corpus, line);) {
    std::string_view pattern = line;
    for (int column = 0; column != 3; ++column) {
      const auto tab = pattern.find('\t');
      ASSERT_NE(std::string_view::npos, tab) << line;
      pattern.remove_prefix(tab + 1);
    }
    ASSERT_NO_FATAL_FAILURE(ExpectSameAsRe2(rng, pattern, 200));
    ++patterns;
  }
  EXPECT_LT(300, patterns);
}

TEST(LikeMatcherTest, random_patterns_match_re2) {
  std::mt19937 rng{20260929};
  for (size_t i = 0; i != 4000; ++i) {
    std::string pattern;
    for (auto n = std::uniform_int_distribution<size_t>{0, 8}(rng); n != 0;
         --n) {
      pattern += Coin(rng, 40) ? Pick(rng, kBadPatternTokens)
                               : Pick(rng, kPatternTokens);
    }
    ASSERT_NO_FATAL_FAILURE(ExpectSameAsRe2(rng, pattern, 60));
  }
}
