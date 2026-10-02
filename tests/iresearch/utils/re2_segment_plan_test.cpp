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
#include <random>
#include <string>
#include <string_view>
#include <vector>

#include "tests_shared.hpp"

namespace {

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

std::string LikeRegex(std::string_view pattern) {
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

RE2::Options Options(bool dot_nl, bool latin1 = false) {
  RE2::Options options;
  options.set_dot_nl(dot_nl);
  options.set_log_errors(false);
  if (latin1) {
    options.set_encoding(RE2::Options::EncodingLatin1);
  }
  return options;
}

class PlanAndEngines {
 public:
  PlanAndEngines(const std::string& regex, const RE2::Options& options)
    : _plan{regex, options}, _engines{"(" + regex + ")", options} {}

  bool ok() const { return _plan.ok() && _engines.ok(); }

  void Expect(std::string_view text) const {
    ASSERT_EQ(RE2::PartialMatch(text, _engines), RE2::PartialMatch(text, _plan))
      << Printable(_plan.pattern()) << " ~ " << Printable(text);
    ASSERT_EQ(RE2::FullMatch(text, _engines), RE2::FullMatch(text, _plan))
      << Printable(_plan.pattern()) << " ~ " << Printable(text);
    std::string_view engines_match;
    std::string_view plan_match;
    const bool engines =
      _engines.Match(text, 0, text.size(), RE2::ANCHOR_BOTH, &engines_match, 1);
    const bool plan =
      _plan.Match(text, 0, text.size(), RE2::ANCHOR_BOTH, &plan_match, 1);
    ASSERT_EQ(engines, plan)
      << Printable(_plan.pattern()) << " ~ " << Printable(text);
    if (plan) {
      ASSERT_EQ(engines_match.data(), plan_match.data());
      ASSERT_EQ(engines_match.size(), plan_match.size());
    }
  }

  void ExpectWindows(std::string_view text) const {
    for (size_t begin = 0; begin <= text.size(); ++begin) {
      for (size_t end = begin; end <= text.size(); ++end) {
        for (const auto anchor :
             {RE2::UNANCHORED, RE2::ANCHOR_START, RE2::ANCHOR_BOTH}) {
          for (const int n : {0, 1}) {
            std::string_view engines_match;
            std::string_view plan_match;
            const bool engines =
              _engines.Match(text, begin, end, anchor, &engines_match, n);
            const bool plan =
              _plan.Match(text, begin, end, anchor, &plan_match, n);
            ASSERT_EQ(engines, plan)
              << Printable(_plan.pattern()) << " ~ " << Printable(text) << " ["
              << begin << ", " << end << ") anchor " << anchor << " n " << n;
            if (plan && n == 1) {
              ASSERT_EQ(engines_match.data(), plan_match.data());
              ASSERT_EQ(engines_match.size(), plan_match.size());
            }
          }
        }
      }
    }
  }

  const RE2& plan() const { return _plan; }

 private:
  RE2 _plan;
  RE2 _engines;
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
      text += RandomUnits(rng, Coin(rng, 4) ? 40 : 3);
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

void ExpectLikeSameAsEngines(std::mt19937& rng, std::string_view pattern,
                             size_t texts) {
  for (const bool dot_nl : {true, false}) {
    const PlanAndEngines re{LikeRegex(pattern), Options(dot_nl)};
    if (!re.ok()) {
      continue;
    }
    for (size_t i = 0; i != texts; ++i) {
      const auto text =
        Coin(rng, 2) ? Instantiate(rng, pattern) : RandomUnits(rng, 10);
      ASSERT_NO_FATAL_FAILURE(re.Expect(text));
    }
  }
}

}  // namespace

TEST(Re2SegmentPlanTest, like_shapes) {
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
    const PlanAndEngines re{LikeRegex(pattern), Options(true)};
    ASSERT_TRUE(re.ok()) << Printable(pattern);
    EXPECT_EQ(match, RE2::PartialMatch(text, re.plan()))
      << Printable(pattern) << " ~ " << Printable(text);
    ASSERT_NO_FATAL_FAILURE(re.Expect(text));
    ASSERT_NO_FATAL_FAILURE(re.ExpectWindows(text));
    ASSERT_NO_FATAL_FAILURE(re.ExpectWindows("x" + std::string{text}));
  }
}

TEST(Re2SegmentPlanTest, dots_consume_what_the_engines_consume) {
  for (const bool dot_nl : {true, false}) {
    const PlanAndEngines one{"\\A.\\z", Options(dot_nl)};
    const PlanAndEngines any{"\\A.*\\z", Options(dot_nl)};
    const PlanAndEngines around{"\\Aa.*b\\z", Options(dot_nl)};
    for (const std::string_view unit :
         {"\xE0\x80\x80", "\xED\xA0\x80", "\xF0\x80\x80\x80",
          "\xF4\x90\x80\x80", "\xF4\xBF\xBF\xBF", "\n"}) {
      ASSERT_NO_FATAL_FAILURE(one.Expect(unit));
      ASSERT_NO_FATAL_FAILURE(any.Expect(unit));
      ASSERT_NO_FATAL_FAILURE(around.Expect("a" + std::string{unit} + "b"));
    }
    for (const auto junk : kJunk) {
      ASSERT_NO_FATAL_FAILURE(one.Expect(junk));
      ASSERT_NO_FATAL_FAILURE(any.Expect(junk));
      ASSERT_NO_FATAL_FAILURE(around.Expect("a" + std::string{junk} + "b"));
    }
  }
}

TEST(Re2SegmentPlanTest, unanchored_contains_under_full_match) {
  std::mt19937 rng{1139};
  for (const std::string_view regex :
       {".*\xD0\xBE\xD1\x81\xD1\x82\xD1\x8C.*", ".*ab.*", "a.*b", ".*a.b.*",
        "..*\xC3\xA9", ".*", "."}) {
    for (const bool dot_nl : {true, false}) {
      const PlanAndEngines re{std::string{regex}, Options(dot_nl)};
      ASSERT_TRUE(re.ok()) << Printable(regex);
      for (size_t i = 0; i != 400; ++i) {
        auto text = RandomUnits(rng, Coin(rng, 3) ? 80 : 8);
        if (Coin(rng, 2)) {
          const auto at =
            std::uniform_int_distribution<size_t>{0, text.size()}(rng);
          text.insert(at,
                      Coin(rng, 2) ? "ab" : "\xD0\xBE\xD1\x81\xD1\x82\xD1\x8C");
        }
        ASSERT_NO_FATAL_FAILURE(re.Expect(text));
        if (text.size() <= 24) {
          ASSERT_NO_FATAL_FAILURE(re.ExpectWindows(text));
        }
      }
    }
  }
}

TEST(Re2SegmentPlanTest, latin1_units_are_bytes) {
  std::mt19937 rng{20260930};
  for (const std::string_view regex :
       {"\\Aa.*b\\z", "\\A.\\xE9.*\\z", "\\A.*\\xFF\\z", "\\A...\\z"}) {
    for (const bool dot_nl : {true, false}) {
      const PlanAndEngines re{std::string{regex}, Options(dot_nl, true)};
      ASSERT_TRUE(re.ok()) << Printable(regex);
      for (size_t i = 0; i != 400; ++i) {
        std::string text;
        for (auto n = std::uniform_int_distribution<size_t>{0, 6}(rng); n != 0;
             --n) {
          text += static_cast<char>(
            Coin(rng, 2) ? "ab\n\xE9\xFF"[rng() % 5]
                         : std::uniform_int_distribution<int>{0, 255}(rng));
        }
        ASSERT_NO_FATAL_FAILURE(re.Expect(text));
      }
    }
  }
}

TEST(Re2SegmentPlanTest, windows_and_anchors) {
  const RE2 re{"a.*b", Options(true)};
  const std::string_view text = "xa-by";
  std::string_view match;
  EXPECT_TRUE(re.Match(text, 1, 4, RE2::ANCHOR_BOTH, &match, 1));
  EXPECT_EQ("a-b", match);
  EXPECT_FALSE(re.Match(text, 0, 4, RE2::ANCHOR_BOTH, &match, 1));
  EXPECT_TRUE(re.Match(text, 0, 5, RE2::UNANCHORED, &match, 1));
  EXPECT_EQ("a-b", match);
  const RE2 anchored{"\\Aa.*b\\z", Options(true)};
  EXPECT_FALSE(anchored.Match(text, 1, 4, RE2::UNANCHORED, nullptr, 0));
  EXPECT_TRUE(anchored.Match("a-b", 0, 3, RE2::UNANCHORED, nullptr, 0));
  EXPECT_FALSE(anchored.Match("xa-b", 1, 4, RE2::ANCHOR_BOTH, nullptr, 0));
  EXPECT_FALSE(anchored.Match("xa-b", 1, 4, RE2::ANCHOR_START, &match, 1));
  const RE2 literal{"\\Aabc", Options(true)};
  EXPECT_FALSE(literal.Match("xabc", 1, 4, RE2::ANCHOR_BOTH, nullptr, 0));
  EXPECT_TRUE(literal.Match("abcx", 0, 3, RE2::ANCHOR_BOTH, &match, 1));
  EXPECT_EQ("abc", match);
}

TEST(Re2SegmentPlanTest, like_windows_match_engines) {
  std::mt19937 rng{20260930};
  std::vector<std::string> patterns;
  std::ifstream corpus{TestEnv::resource("patterns/like.tsv")};
  ASSERT_TRUE(corpus.is_open());
  for (std::string line; std::getline(corpus, line);) {
    std::string_view pattern = line;
    for (int column = 0; column != 3; ++column) {
      pattern.remove_prefix(pattern.find('\t') + 1);
    }
    patterns.emplace_back(pattern);
  }
  for (size_t i = 0; i != 200; ++i) {
    std::string pattern;
    for (auto n = std::uniform_int_distribution<size_t>{0, 6}(rng); n != 0;
         --n) {
      pattern += Pick(rng, kPatternTokens);
    }
    patterns.push_back(std::move(pattern));
  }
  for (const auto& pattern : patterns) {
    const auto anchored = LikeRegex(pattern);
    const auto core = anchored.substr(2, anchored.size() - 4);
    for (const auto& regex : {anchored, core}) {
      for (const bool dot_nl : {true, false}) {
        const PlanAndEngines re{regex, Options(dot_nl)};
        if (!re.ok()) {
          continue;
        }
        for (size_t i = 0; i != 4; ++i) {
          auto text =
            Coin(rng, 2) ? Instantiate(rng, pattern) : RandomUnits(rng, 6);
          if (Coin(rng, 2)) {
            text.insert(0, Pick(rng, kUnits));
          }
          if (Coin(rng, 2)) {
            text += Pick(rng, kUnits);
          }
          text.resize(std::min<size_t>(text.size(), 24));
          ASSERT_NO_FATAL_FAILURE(re.ExpectWindows(text));
        }
      }
    }
  }
}

TEST(Re2SegmentPlanTest, like_corpus_matches_engines) {
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
    ASSERT_NO_FATAL_FAILURE(ExpectLikeSameAsEngines(rng, pattern, 200));
    ++patterns;
  }
  EXPECT_LT(300, patterns);
}

TEST(Re2SegmentPlanTest, random_like_patterns_match_engines) {
  std::mt19937 rng{20260929};
  for (size_t i = 0; i != 4000; ++i) {
    std::string pattern;
    for (auto n = std::uniform_int_distribution<size_t>{0, 8}(rng); n != 0;
         --n) {
      pattern += Coin(rng, 40) ? Pick(rng, kBadPatternTokens)
                               : Pick(rng, kPatternTokens);
    }
    ASSERT_NO_FATAL_FAILURE(ExpectLikeSameAsEngines(rng, pattern, 60));
  }
}
