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
#include <re2/byte_set_finder.h>
#include <re2/literal_finder.h>
#include <re2/multi_literal_finder.h>
#include <re2/re2.h>

#include <algorithm>
#include <random>
#include <string>
#include <string_view>
#include <vector>

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

TEST(Re2LiteralFinderTest, byte_set_find_is_scan) {
  std::mt19937 rng{20260930};
  for (size_t round = 0; round != 3000; ++round) {
    uint64_t bits[4] = {};
    std::string members;
    for (auto n = 1 + rng() % 12; n != 0; --n) {
      const auto b = static_cast<uint8_t>(rng() % 256);
      bits[b >> 6] |= uint64_t{1} << (b & 63);
      members += static_cast<char>(b);
    }
    re2::ByteSetFinder finder;
    if (!finder.Build(bits)) {
      continue;
    }
    std::string text;
    for (auto n = rng() % 100; n != 0; --n) {
      text += rng() % 16 == 0 ? members[rng() % members.size()]
                              : static_cast<char>(rng() % 256);
    }
    const char* end = text.data() + text.size();
    for (size_t from = 0; from <= text.size(); ++from) {
      const auto expected = text.find_first_of(members, from);
      const char* found = finder.Find(text.data() + from, end);
      ASSERT_EQ(expected, found == nullptr
                            ? std::string::npos
                            : static_cast<size_t>(found - text.data()))
        << "round " << round << " from " << from << " text size "
        << text.size();
    }
  }
}

TEST(Re2LiteralFinderTest, multi_literal_find_is_leftmost_find) {
  std::mt19937 rng{20260930};
  for (size_t round = 0; round != 3000; ++round) {
    std::vector<std::string> literals;
    for (auto n = 1 + rng() % (round % 3 == 0 ? 70 : 8); n != 0; --n) {
      auto literal = RandomBytes(rng, 6);
      if (literal.empty()) {
        literal = "ab";
      }
      literals.push_back(std::move(literal));
    }
    re2::MultiLiteralFinder finder;
    if (!finder.Build(literals.size(), [&](size_t i) -> std::string_view {
          return literals[i];
        })) {
      ASSERT_GT(literals.size(), re2::MultiLiteralFinder::kMaxLiterals);
      continue;
    }
    auto text = RandomBytes(rng, 100);
    for (auto n = rng() % 4; n != 0; --n) {
      text = Plant(rng, text, literals[rng() % literals.size()]);
    }
    const std::string_view view{text};
    const char* end = text.data() + text.size();
    for (size_t from = 0; from <= text.size(); ++from) {
      auto expected = std::string_view::npos;
      for (const auto& literal : literals) {
        expected = std::min(expected, view.find(literal, from));
      }
      const char* found = finder.Find(text.data() + from, end);
      ASSERT_EQ(expected, found == nullptr
                            ? std::string_view::npos
                            : static_cast<size_t>(found - text.data()))
        << "round " << round << " from " << from << " literals "
        << literals.size() << " text size " << text.size();
    }
  }
}

TEST(Re2LiteralFinderTest, multi_literal_candidates_split_like_a_scan) {
  std::mt19937 rng{1286};
  for (size_t round = 0; round != 3000; ++round) {
    std::vector<std::string> literals;
    for (auto n = 1 + rng() % 12; n != 0; --n) {
      auto literal = RandomBytes(rng, 4);
      if (literal.empty()) {
        literal = "ba";
      }
      literals.push_back(std::move(literal));
    }
    re2::MultiLiteralFinder finder;
    ASSERT_TRUE(
      finder.Build(literals.size(),
                   [&](size_t i) -> std::string_view { return literals[i]; }));
    auto text = RandomBytes(rng, 120);
    for (auto n = rng() % 6; n != 0; --n) {
      text = Plant(rng, text, literals[rng() % literals.size()]);
    }
    const std::string_view view{text};
    const auto match_at = [&](size_t at) -> size_t {
      for (const auto& literal : literals) {
        if (view.substr(at).starts_with(literal)) {
          return literal.size();
        }
      }
      return 0;
    };
    std::vector<std::pair<size_t, size_t>> expected;
    for (size_t at = 0; at < text.size();) {
      const auto size = match_at(at);
      if (size == 0) {
        ++at;
        continue;
      }
      expected.emplace_back(at, size);
      at += size;
    }
    std::vector<std::pair<size_t, size_t>> actual;
    finder.ForEachCandidate(
      text.data(), text.data() + text.size(), [&](const char* candidate) {
        const auto at = static_cast<size_t>(candidate - text.data());
        const auto size = match_at(at);
        if (size == 0) {
          return candidate + 1;
        }
        actual.emplace_back(at, size);
        return candidate + size;
      });
    ASSERT_EQ(expected, actual)
      << "round " << round << " literals " << literals.size() << " text size "
      << text.size();
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

TEST(Re2LiteralFinderTest, multi_literal_accel_finds_the_match) {
  constexpr std::string_view kPatterns[] = {
    "\xD0\xBF\xD0\xB5\xD1\x80\xD0\xB5|\xD0\xBE\xD1\x81|ab",
    "(\xD0\xBE\xD1\x81|ba)[a-z]*",
    "ab|ba|qe|e\xD1\x8C",
    "a(b|q)e|b(a|e)",
    "(?:ab|ba)+q",
    "ab|a",
    "ab\\b|ba",
    "(ab|ba)|(qe|eq)b",
    "\xD0\xBE\xD1\x81(\xD1\x82\xD1\x8C|\xD0\xB5)|ab(a|b)(q|e)",
    "ab|ba|(?i)qe",
    "(?i)\xD0\xBE\xD1\x81\xD1\x82\xD1\x8C",
    "(?i)\xD0\xB5\xD0\xBE|ab",
    "[ab]e|q[ab]",
    "[ab][\xD0\xBE\xD1\x81]q|(?i)\xD1\x81\xD1\x82",
    "(?i)ab\xD1\x8C",
    "(?i)[a-z]+\xD0\xBE\xD1\x81",
    "(?i)\\w+\xD1\x81\xD1\x82\xD1\x8C",
    "[a-e]+(?i)ab\xD1\x8C",
    "(?i)[a-z ]+qe[ab]",
  };
  constexpr std::string_view kCasedPieces[] = {
    "a",        "b",        "e",        "q",        " ",        "A",
    "B",        "\xD0\xBE", "\xD0\x9E", "\xD1\x81", "\xD0\xA1", "\xD1\x82",
    "\xD0\xA2", "\xD1\x8C", "\xD0\xAC", "\xD0\xB5", "\xD0\x95",
  };
  std::mt19937 rng{1134};
  for (const auto pattern : kPatterns) {
    const re2::RE2 re{pattern};
    ASSERT_TRUE(re.ok()) << pattern;
    for (size_t i = 0; i != 300; ++i) {
      std::string text;
      for (auto n = std::uniform_int_distribution<size_t>{0, 70}(rng); n != 0;
           --n) {
        text += kCasedPieces[std::uniform_int_distribution<size_t>{
          0, std::size(kCasedPieces) - 1}(rng)];
      }
      ASSERT_NO_FATAL_FAILURE(ExpectLeftmostMatch(re, text));
    }
  }
}

TEST(Re2LiteralFinderTest, first_byte_accel_finds_the_match) {
  constexpr std::string_view kClassPieces[] = {
    "a", "b", " ", "\xD0\xBE", "\xD1\x81", "1", "2024", "X",
    "B", "#", "@", "\n",       "Q",        "z", "7x",   "C",
  };
  constexpr std::string_view kPatterns[] = {
    "[0-9]{4}",   "[0-9]+x",      "[A-Z][a-z]+",  "[#@][a-z]+",
    "(?i)[q-t]z", "[0-9]|[A-C]b", "\\b[0-9]+\\b", "(?m)^[0-9]",
    "[0-9]*",     "(?:[0-9]|X)Q", "[A-Z]{2,}",    "[#@]\\w*",
  };
  std::mt19937 rng{20260930};
  for (const auto pattern : kPatterns) {
    const re2::RE2 re{pattern};
    ASSERT_TRUE(re.ok()) << pattern;
    for (size_t i = 0; i != 300; ++i) {
      std::string text;
      for (auto n = std::uniform_int_distribution<size_t>{0, 80}(rng); n != 0;
           --n) {
        text += kClassPieces[std::uniform_int_distribution<size_t>{
          0, std::size(kClassPieces) - 1}(rng)];
      }
      ASSERT_NO_FATAL_FAILURE(ExpectLeftmostMatch(re, text));
    }
  }
}

}  // namespace
