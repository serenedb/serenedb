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

#include <random>
#include <string>
#include <string_view>
#include <vector>

#include "tests_shared.hpp"

namespace {

RE2::Options Options(bool thread_safe, int64_t max_mem) {
  RE2::Options options;
  options.set_log_errors(false);
  options.set_thread_safe(thread_safe);
  options.set_max_mem(max_mem);
  return options;
}

constexpr std::string_view kRegexes[] = {
  "ab",
  "a+b",
  "(a|b)*abb",
  "[a-c]+x",
  "^ab",
  "ab$",
  "\\bab\\b",
  "(ab|ba)+",
  "a.b",
  "(?s)a.*b",
  "(a)(b)?c",
  "[^a]+",
  "(a|aa)*b",
  "\\w+\\s",
  "(?i)AB",
  "x*",
  "(a|b|c|d|e|f|g|h)*[a-h]{8}",
  "\xD0\xBE\xD1\x81\xD1\x82\xD1\x8C",
};

std::string RandomText(std::mt19937& rng) {
  static constexpr std::string_view kPieces[] = {
    "a", "b", "c", "x", " ", "\n", "ab", "ba", "\xD0\xBE", "\xD1\x81",
  };
  std::string text;
  for (auto n = rng() % 40; n != 0; --n) {
    text += kPieces[rng() % std::size(kPieces)];
  }
  return text;
}

void ExpectSame(const RE2& shared, const RE2& owned, std::string_view text) {
  ASSERT_EQ(RE2::PartialMatch(text, shared), RE2::PartialMatch(text, owned))
    << shared.pattern() << " ~ " << text;
  ASSERT_EQ(RE2::FullMatch(text, shared), RE2::FullMatch(text, owned))
    << shared.pattern() << " ~ " << text;
  const int n = 1 + shared.NumberOfCapturingGroups();
  for (const auto anchor :
       {RE2::UNANCHORED, RE2::ANCHOR_START, RE2::ANCHOR_BOTH}) {
    for (size_t begin = 0; begin <= text.size(); begin += 3) {
      std::vector<std::string_view> a(n);
      std::vector<std::string_view> b(n);
      const bool x =
        shared.Match(text, begin, text.size(), anchor, a.data(), n);
      const bool y = owned.Match(text, begin, text.size(), anchor, b.data(), n);
      ASSERT_EQ(x, y) << shared.pattern() << " ~ " << text << " @" << begin;
      if (x) {
        for (int i = 0; i != n; ++i) {
          ASSERT_EQ(a[i].data(), b[i].data()) << shared.pattern();
          ASSERT_EQ(a[i].size(), b[i].size()) << shared.pattern();
        }
      }
    }
  }
}

}  // namespace

TEST(Re2OwnedDfaTest, owned_matches_like_shared) {
  std::mt19937 rng{20260930};
  for (const int64_t max_mem :
       {int64_t{8} << 20, int64_t{1} << 17, int64_t{1} << 14}) {
    for (const auto regex : kRegexes) {
      const RE2 shared{regex, Options(true, max_mem)};
      const RE2 owned{regex, Options(false, max_mem)};
      ASSERT_EQ(shared.ok(), owned.ok()) << regex;
      if (!shared.ok()) {
        continue;
      }
      for (size_t i = 0; i != 200; ++i) {
        ASSERT_NO_FATAL_FAILURE(ExpectSame(shared, owned, RandomText(rng)));
      }
    }
  }
}

const RE2* g_counted = nullptr;
size_t g_resets = 0;

void CountReset(const re2::hooks::DFAStateCacheReset&) {
  if (re2::hooks::context == g_counted) {
    ++g_resets;
  }
}

TEST(Re2OwnedDfaTest, cache_resets_keep_results) {
  auto* previous = re2::hooks::GetDFAStateCacheResetHook();
  re2::hooks::SetDFAStateCacheResetHook(CountReset);
  std::mt19937 rng{1139};
  const std::string regex = "(a|b)*a(a|b){12}c";
  for (const int64_t max_mem : {int64_t{1} << 16, int64_t{1} << 17}) {
    const RE2 shared{regex, Options(true, max_mem)};
    const RE2 owned{regex, Options(false, max_mem)};
    ASSERT_TRUE(shared.ok());
    ASSERT_TRUE(owned.ok());
    g_counted = &owned;
    g_resets = 0;
    for (size_t i = 0; i != 20; ++i) {
      std::string text;
      for (size_t j = 0; j != 4000; ++j) {
        text += "ab"[rng() % 2];
      }
      text[text.size() - 13] = "ab"[rng() % 2];
      text += 'c';
      ASSERT_EQ(RE2::PartialMatch(text, shared),
                RE2::PartialMatch(text, owned));
      std::string_view a;
      std::string_view b;
      ASSERT_EQ(shared.Match(text, 0, text.size(), RE2::UNANCHORED, &a, 1),
                owned.Match(text, 0, text.size(), RE2::UNANCHORED, &b, 1));
      ASSERT_EQ(a.data(), b.data());
      ASSERT_EQ(a.size(), b.size());
    }
    EXPECT_LT(0, g_resets) << max_mem;
  }
  g_counted = nullptr;
  re2::hooks::SetDFAStateCacheResetHook(previous);
}

TEST(Re2OwnedDfaTest, possible_match_range_is_the_same) {
  for (const auto regex : kRegexes) {
    const RE2 shared{regex, Options(true, int64_t{8} << 20)};
    const RE2 owned{regex, Options(false, int64_t{8} << 20)};
    std::string shared_min;
    std::string shared_max;
    std::string owned_min;
    std::string owned_max;
    ASSERT_EQ(shared.PossibleMatchRange(&shared_min, &shared_max, 10),
              owned.PossibleMatchRange(&owned_min, &owned_max, 10))
      << regex;
    EXPECT_EQ(shared_min, owned_min) << regex;
    EXPECT_EQ(shared_max, owned_max) << regex;
  }
}
