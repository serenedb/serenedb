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
#include <re2/set.h>

#include <fstream>
#include <random>
#include <string>
#include <string_view>
#include <vector>

#include "tests_shared.hpp"

namespace {

constexpr std::string_view kTextTokens[] = {
  "a",
  "b",
  "c",
  "ab",
  "abc",
  "x",
  "y",
  "z",
  "q",
  "Q",
  "-",
  ".",
  "0",
  "7",
  " ",
  "\n",
  "\xC3\xA9",
  "\xC3\x89",
  "\xE2\x82\xAC",
  "\xFF",
};

constexpr std::string_view kPatternTokens[] = {
  "ab",
  "c",
  "-",
  "\\.",
  ".",
  ".*",
  "[a-c]+",
  "\\d+",
  "x*",
  "(y|z)",
  "(ab)",
  "(?:ab)+",
  "a{2}",
  "(?i:q)",
  "(?i)",
  "\\b",
  "\xC3\xA9",
  "[\xC3\xA9-\xC3\xAA]",
  "\\pL",
  "\\s",
  "(c|-)",
  "b?",
  "(?i:\xC3\xA9)",
  "\\C",
};

template<size_t N>
std::string_view Pick(std::mt19937& rng, const std::string_view (&from)[N]) {
  return from[std::uniform_int_distribution<size_t>{0, N - 1}(rng)];
}

std::string RandomText(std::mt19937& rng, std::string_view pattern) {
  std::string text;
  for (auto n = std::uniform_int_distribution<size_t>{0, 12}(rng); n != 0;
       --n) {
    if (!pattern.empty() &&
        std::uniform_int_distribution<int>{0, 3}(rng) == 0) {
      const auto at =
        std::uniform_int_distribution<size_t>{0, pattern.size() - 1}(rng);
      const auto size = std::uniform_int_distribution<size_t>{1, 4}(rng);
      text += pattern.substr(at, size);
    } else {
      text += Pick(rng, kTextTokens);
    }
  }
  return text;
}

void ExpectSameAsSet(std::mt19937& rng, const std::string& pattern,
                     const RE2::Options& options, size_t texts) {
  const RE2 re{pattern, options};
  if (!re.ok()) {
    return;
  }
  RE2::Set set{options, RE2::UNANCHORED};
  ASSERT_EQ(0, set.Add(pattern, nullptr)) << pattern;
  ASSERT_TRUE(set.Compile()) << pattern;
  for (size_t i = 0; i != texts; ++i) {
    const auto text = RandomText(rng, pattern);
    RE2::Set::ErrorInfo error{RE2::Set::kNoError};
    const bool expected = set.Match(text, nullptr, &error);
    if (!expected && error.kind == RE2::Set::kOutOfMemory) {
      continue;
    }
    ASSERT_EQ(RE2::Set::kNoError, error.kind) << pattern;
    ASSERT_EQ(expected, RE2::PartialMatch(text, re))
      << pattern << " ~ " << text;
    std::string_view whole;
    ASSERT_EQ(expected,
              re.Match(text, 0, text.size(), RE2::UNANCHORED, &whole, 1))
      << pattern << " ~ " << text;
  }
}

RE2::Options Quiet() {
  RE2::Options options;
  options.set_log_errors(false);
  return options;
}

}  // namespace

TEST(Re2RequiredLiteralTest, absent_literal_is_no_match) {
  const RE2 re{"[a-z]+-[0-9]+"};
  EXPECT_FALSE(RE2::PartialMatch("abc 123", re));
  EXPECT_TRUE(RE2::PartialMatch("x abc-123 y", re));
  const RE2 folded{"(?i)[a-z]+-Q"};
  EXPECT_TRUE(RE2::PartialMatch("abc-q", folded));
  const RE2 latin1{"[a-z]+\xE9", RE2::Latin1};
  EXPECT_TRUE(RE2::PartialMatch("caf\xE9", latin1));
  EXPECT_FALSE(RE2::PartialMatch("cafe", latin1));
  const RE2 unicode{"\\w+\xC3\xA9"};
  EXPECT_TRUE(RE2::PartialMatch("caf\xC3\xA9", unicode));
  EXPECT_FALSE(RE2::PartialMatch("caf\xC3\x89", unicode));
}

TEST(Re2RequiredLiteralTest, window_is_respected) {
  const RE2 re{"[a-z]+-[0-9]+"};
  const std::string_view text = "ab-1 cd";
  std::string_view match;
  EXPECT_TRUE(re.Match(text, 0, 4, RE2::UNANCHORED, &match, 1));
  EXPECT_EQ("ab-1", match);
  EXPECT_FALSE(re.Match(text, 3, text.size(), RE2::UNANCHORED, &match, 1));
  EXPECT_FALSE(re.Match(text, 0, 2, RE2::UNANCHORED, nullptr, 0));
}

TEST(Re2RequiredLiteralTest, corpus_matches_set) {
  std::ifstream corpus{TestEnv::resource("patterns/regexp.tsv")};
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
    ASSERT_NO_FATAL_FAILURE(
      ExpectSameAsSet(rng, std::string{pattern}, Quiet(), 300));
    ++patterns;
  }
  EXPECT_LT(50, patterns);
}

TEST(Re2RequiredLiteralTest, random_patterns_match_set) {
  std::mt19937 rng{20260929};
  auto latin1 = Quiet();
  latin1.set_encoding(RE2::Options::EncodingLatin1);
  auto folded = Quiet();
  folded.set_case_sensitive(false);
  for (size_t i = 0; i != 3000; ++i) {
    std::string pattern;
    for (auto n = std::uniform_int_distribution<size_t>{1, 6}(rng); n != 0;
         --n) {
      pattern += Pick(rng, kPatternTokens);
    }
    ASSERT_NO_FATAL_FAILURE(ExpectSameAsSet(rng, pattern, Quiet(), 40));
    ASSERT_NO_FATAL_FAILURE(ExpectSameAsSet(rng, pattern, latin1, 20));
    ASSERT_NO_FATAL_FAILURE(ExpectSameAsSet(rng, pattern, folded, 20));
  }
}
