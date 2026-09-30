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
#include <re2/literal_finder.h>
#include <re2/re2.h>

#include <random>
#include <string>
#include <string_view>

#include "tests_shared.hpp"

namespace {

constexpr std::string_view kPieces[] = {
  "a",
  "b",
  "e",
  "q",
  " ",
  "ab",
  "ba",
  "\xD0\xBE",
  "\xD1\x81",
  "\xD1\x82",
  "\xD1\x8C",
  "\xD0\xB5",
  "\xD0",
  "\xBE",
  "\x81",
  "\xFF",
  std::string_view{"\0", 1},
};

template<typename Rng>
std::string RandomBytes(Rng& rng, size_t max_pieces) {
  std::string out;
  for (auto n = std::uniform_int_distribution<size_t>{0, max_pieces}(rng);
       n != 0; --n) {
    out += kPieces[std::uniform_int_distribution<size_t>{
      0, std::size(kPieces) - 1}(rng)];
  }
  return out;
}

template<typename Rng>
std::string Plant(Rng& rng, std::string text, std::string_view needle) {
  for (auto n = std::uniform_int_distribution<int>{0, 3}(rng); n != 0; --n) {
    const auto at = std::uniform_int_distribution<size_t>{0, text.size()}(rng);
    text.insert(at, needle);
  }
  return text;
}

TEST(Re2LiteralFinderTest, finds_what_find_finds) {
  std::mt19937 rng{20260930};
  for (size_t round = 0; round != 20000; ++round) {
    auto needle = RandomBytes(rng, 12);
    if (needle.empty()) {
      needle = "ab";
    }
    const auto text = Plant(rng, RandomBytes(rng, 120), needle);
    const std::string_view view{text};
    const re2::LiteralFinder finder{needle};
    const char* end = text.data() + text.size();
    for (size_t from = 0; from <= text.size(); ++from) {
      const auto expected = view.find(needle, from);
      const char* found = finder.Find(needle, text.data() + from, end);
      ASSERT_EQ(
        expected == std::string_view::npos ? std::string_view::npos : expected,
        found == nullptr ? std::string_view::npos
                         : static_cast<size_t>(found - text.data()))
        << "round " << round << " from " << from << " needle size "
        << needle.size() << " text size " << text.size();
    }
  }
}

TEST(Re2LiteralFinderTest, equal_is_memcmp) {
  std::mt19937 rng{20260930};
  for (size_t n = 0; n != 40; ++n) {
    for (size_t round = 0; round != 300; ++round) {
      std::string a;
      for (size_t i = 0; i != n; ++i) {
        a += std::string_view{"ab\xD0\x00", 4}[rng() % 4];
      }
      auto b = a;
      for (auto flips = rng() % 3; flips != 0 && n != 0; --flips) {
        b[rng() % n] ^= static_cast<char>(1 + rng() % 255);
      }
      ASSERT_EQ(memcmp(a.data(), b.data(), n) == 0,
                re2::LiteralFinder::Equal(a.data(), b.data(), n))
        << "size " << n << " round " << round;
    }
  }
}

TEST(Re2LiteralFinderTest, empty_and_single_byte_needles) {
  const std::string text(100, 'x');
  const char* end = text.data() + text.size();
  const auto find = [&](std::string_view needle, const char* from) {
    return re2::LiteralFinder{needle}.Find(needle, from, end);
  };
  EXPECT_EQ(text.data(), find("", text.data()));
  EXPECT_EQ(nullptr, find("y", text.data()));
  EXPECT_EQ(text.data() + 7, find("x", text.data() + 7));
  EXPECT_EQ(nullptr, find("xx", end - 1));
  EXPECT_EQ(end - 2, find("xx", end - 2));
}

void ExpectLeftmostMatch(const re2::RE2& re, std::string_view text) {
  std::string_view expected;
  bool any = false;
  for (size_t start = 0; start <= text.size() && !any; ++start) {
    any =
      re.Match(text, start, text.size(), re2::RE2::ANCHOR_START, &expected, 1);
  }
  std::string_view actual;
  const bool found =
    re.Match(text, 0, text.size(), re2::RE2::UNANCHORED, &actual, 1);
  ASSERT_EQ(any, found) << re.pattern();
  if (any) {
    EXPECT_EQ(expected.data(), actual.data()) << re.pattern();
    EXPECT_EQ(expected.size(), actual.size()) << re.pattern();
  }
}

TEST(Re2LiteralFinderTest, prefix_accel_and_required_literal_find_the_match) {
  constexpr std::string_view kPrefixes[] = {
    "ab", "ba",        "\xD0\xBE\xD1\x81", "\xD0\xBF\xD0\xB5\xD1\x80\xD0\xB5",
    "q",  "e\xD1\x8C",
  };
  constexpr std::string_view kSuffixes[] = {
    "", "[a-z]*", "(?:x|y)", ".?b", "\\w+", "[^ ]{0,3}e",
  };
  constexpr std::string_view kRequired[] = {
    "[a-z]+ab",
    "\\w+\xD1\x8C",
    "[a-e]+ba[0-9]*",
    ".{2}\xD0\xBE\xD1\x81",
  };
  std::mt19937 rng{1139};
  const auto check = [&](const std::string& pattern, std::string_view plant) {
    const re2::RE2 re{pattern};
    ASSERT_TRUE(re.ok()) << pattern;
    for (size_t i = 0; i != 200; ++i) {
      std::string text;
      for (auto n = std::uniform_int_distribution<size_t>{0, 60}(rng); n != 0;
           --n) {
        text +=
          std::uniform_int_distribution<int>{0, 3}(rng) == 0
            ? std::string{" "}
            : std::string{
                kPieces[std::uniform_int_distribution<size_t>{0, 11}(rng)]};
      }
      text = Plant(rng, text, plant);
      ASSERT_NO_FATAL_FAILURE(ExpectLeftmostMatch(re, text));
    }
  };
  for (const auto prefix : kPrefixes) {
    for (const auto suffix : kSuffixes) {
      std::string pattern{prefix};
      pattern += suffix;
      ASSERT_NO_FATAL_FAILURE(check(pattern, prefix));
    }
  }
  for (const auto pattern : kRequired) {
    ASSERT_NO_FATAL_FAILURE(check(std::string{pattern}, "ab"));
    ASSERT_NO_FATAL_FAILURE(check(std::string{pattern}, "\xD0\xBE\xD1\x81"));
  }
}

}  // namespace
