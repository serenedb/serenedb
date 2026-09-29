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

#include <absl/strings/ascii.h>

#include <algorithm>
#include <cstring>
#include <iresearch/analysis/split_by_non_alpha_tokenizer.hpp>
#include <iresearch/analysis/text/case/case.hpp>
#include <iresearch/analysis/text/words/masks.hpp>
#include <iresearch/analysis/text/words/split_by_non_alpha.hpp>
#include <iresearch/analysis/token_batch.hpp>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "tests_shared.hpp"
#include "token_sink_utils.hpp"

namespace {

using irs::analysis::SplitByNonAlphaTokenizer;
using Chars = SplitByNonAlphaTokenizer::Options::Chars;
using Runs = std::vector<std::pair<size_t, size_t>>;

Runs ReferenceRuns(std::string_view v) {
  Runs out;
  size_t i = 0;
  while (i < v.size()) {
    while (i < v.size() &&
           !absl::ascii_isalnum(static_cast<unsigned char>(v[i]))) {
      ++i;
    }
    const size_t begin = i;
    while (i < v.size() &&
           absl::ascii_isalnum(static_cast<unsigned char>(v[i]))) {
      ++i;
    }
    if (i != begin) {
      out.emplace_back(begin, i);
    }
  }
  return out;
}

Runs SplitRuns(std::string_view v) {
  Runs out;
  irs::analysis::words::SplitByNonAlpha(
    tests::ToStringT(v),
    [&](size_t begin, size_t end) { out.emplace_back(begin, end); });
  return out;
}

bool KeptByte(char c) {
  const auto b = static_cast<unsigned char>(c);
  return b >= 0x80 || absl::ascii_isalnum(b);
}

Runs ReferenceRunsKeepNonAscii(std::string_view v) {
  Runs out;
  size_t i = 0;
  while (i < v.size()) {
    while (i < v.size() && !KeptByte(v[i])) {
      ++i;
    }
    const size_t begin = i;
    while (i < v.size() && KeptByte(v[i])) {
      ++i;
    }
    if (i != begin) {
      out.emplace_back(begin, i);
    }
  }
  return out;
}

Runs SplitRunsKeepNonAscii(std::string_view v) {
  Runs out;
  irs::analysis::words::SplitByNonAlpha<true>(
    tests::ToStringT(v),
    [&](size_t begin, size_t end) { out.emplace_back(begin, end); });
  return out;
}

template<bool Wide>
Runs NonSpaceRuns(std::string_view v) {
  Runs out;
  auto on_block = [](size_t, irs::analysis::classify::Block) {};
  auto on_run = [&](size_t begin, size_t end) { out.emplace_back(begin, end); };
  const auto* data = reinterpret_cast<const irs::byte_type*>(v.data());
  if constexpr (Wide) {
#if defined(__x86_64__)
    irs::analysis::words::ForEachNonSpaceRunWide<false>(data, v.size(),
                                                        on_block, on_run);
#endif
  } else {
    irs::analysis::words::ForEachNonSpaceRun<false>(data, v.size(), on_block,
                                                    on_run);
  }
  return out;
}

template<bool Wide, bool Letters>
Runs AlnumRuns(std::string_view v) {
  Runs out;
  auto on_run = [&](size_t begin, size_t end) { out.emplace_back(begin, end); };
  const auto* data = reinterpret_cast<const irs::byte_type*>(v.data());
  if constexpr (Wide) {
#if defined(__x86_64__)
    irs::analysis::words::ForEachAlnumRunWide<Letters>(data, v.size(), on_run);
#endif
  } else {
    irs::analysis::words::ForEachAlnumRun<Letters>(data, v.size(), on_run);
  }
  return out;
}

bool HasWideKernels() {
#if defined(__x86_64__)
  return irs::analysis::classify::HasAvx512Bw();
#else
  return false;
#endif
}

struct Tok {
  std::string term;
  uint32_t offs_start;
  uint32_t offs_end;
};

std::vector<Tok> Pull(irs::analysis::Tokenizer& a, std::string_view data) {
  std::vector<Tok> out;
  auto tokens = tests::Analyze(a, data);
  EXPECT_TRUE(tokens.has_value());
  if (!tokens) {
    return out;
  }
  for (auto& t : *tokens) {
    out.push_back({std::move(t.term), t.offs_start, t.offs_end});
  }
  return out;
}

using Terms = std::vector<std::string>;

Terms SplitTerms(SplitByNonAlphaTokenizer::Options opts, std::string_view v) {
  auto a = SplitByNonAlphaTokenizer::Make(opts);
  Terms out;
  for (auto& t : Pull(*a, v)) {
    out.push_back(std::move(t.term));
  }
  return out;
}

std::vector<SplitByNonAlphaTokenizer::Options> AllConfigs() {
  std::vector<SplitByNonAlphaTokenizer::Options> out;
  for (const auto chars : {Chars::Ascii, Chars::AsciiBytes, Chars::Alnum,
                           Chars::Letters, Chars::Whitespace}) {
    for (const auto case_convert :
         {irs::Case::None, irs::Case::Lower, irs::Case::Upper}) {
      out.push_back({.case_convert = case_convert, .chars = chars});
    }
  }
  return out;
}

std::string CaseConverted(std::string_view v, irs::Case c) {
  if (c == irs::Case::None) {
    return std::string{v};
  }
  std::string out(irs::analysis::casing::CaseConvertUtf8Bound(v.size()), '\0');
  auto* p = reinterpret_cast<irs::byte_type*>(out.data());
  out.resize(c == irs::Case::Lower
               ? irs::analysis::casing::CaseConvertUtf8<true>(v, p)
               : irs::analysis::casing::CaseConvertUtf8<false>(v, p));
  return out;
}

}  // namespace

TEST(classify_block_test, ExhaustiveAgainstScalar) {
  irs::byte_type block[irs::analysis::classify::kClassifyBlock];
  const irs::byte_type targets[] = {' ', ',', 0x00, 0xFF};
  for (int c = 0; c < 256; ++c) {
    for (size_t at = 0; at < irs::analysis::classify::kClassifyBlock; ++at) {
      for (size_t i = 0; i < irs::analysis::classify::kClassifyBlock; ++i) {
        block[i] = static_cast<irs::byte_type>(i * 7 + 13);
      }
      block[at] = static_cast<irs::byte_type>(c);

      irs::analysis::words::WordMasks expect{0, 0, 0};
      uint32_t expect_eq = 0;
      uint32_t expect_any = 0;
      for (size_t i = 0; i < irs::analysis::classify::kClassifyBlock; ++i) {
        const auto x = block[i];
        const bool d = x >= '0' && x <= '9';
        const auto f = static_cast<irs::byte_type>(x | 0x20);
        const bool a = f >= 'a' && f <= 'z';
        expect.digit |= static_cast<uint32_t>(d) << i;
        expect.alpha |= static_cast<uint32_t>(a) << i;
        expect.word |= static_cast<uint32_t>(d || a || x == '_') << i;
        expect_eq |= static_cast<uint32_t>(x == ' ') << i;
        for (const auto t : targets) {
          expect_any |= static_cast<uint32_t>(x == t) << i;
        }
      }

      const auto m = irs::analysis::words::ClassifyWordBlock(block);
      ASSERT_EQ(expect.word, m.word) << "c=" << c << " at=" << at;
      ASSERT_EQ(expect.alpha, m.alpha) << "c=" << c << " at=" << at;
      ASSERT_EQ(expect.digit, m.digit) << "c=" << c << " at=" << at;
      ASSERT_EQ(expect_eq,
                irs::analysis::classify::ClassifyEqBlock(block, ' '));
      ASSERT_EQ(expect_any,
                irs::analysis::classify::ClassifyAnyEqBlock(block, targets));
    }
  }
}

TEST(split_by_non_alpha_tokenizer_test, consts) {
  static_assert("split_by_non_alpha" ==
                irs::Type<SplitByNonAlphaTokenizer>::name());
}

TEST(split_by_non_alpha_tokenizer_test, basic_pull) {
  auto a = SplitByNonAlphaTokenizer::Make({});
  auto tokens = Pull(*a, "Hello, World! 123abc");
  ASSERT_EQ(3u, tokens.size());
  EXPECT_EQ("Hello", tokens[0].term);
  EXPECT_EQ(0u, tokens[0].offs_start);
  EXPECT_EQ(5u, tokens[0].offs_end);
  EXPECT_EQ("World", tokens[1].term);
  EXPECT_EQ("123abc", tokens[2].term);
  EXPECT_EQ(14u, tokens[2].offs_start);
  EXPECT_EQ(20u, tokens[2].offs_end);
}

TEST(split_by_non_alpha_tokenizer_test, case_convert_pull) {
  auto lower =
    SplitByNonAlphaTokenizer::Make({.case_convert = irs::Case::Lower});
  auto tokens = Pull(*lower, "Hello WORLD");
  ASSERT_EQ(2u, tokens.size());
  EXPECT_EQ("hello", tokens[0].term);
  EXPECT_EQ("world", tokens[1].term);

  auto upper =
    SplitByNonAlphaTokenizer::Make({.case_convert = irs::Case::Upper});
  tokens = Pull(*upper, "Hello world");
  ASSERT_EQ(2u, tokens.size());
  EXPECT_EQ("HELLO", tokens[0].term);
  EXPECT_EQ("WORLD", tokens[1].term);
}

TEST(split_by_non_alpha_tokenizer_test, case_convert_folds_ascii_only) {
  auto lower =
    SplitByNonAlphaTokenizer::Make({.case_convert = irs::Case::Lower});
  const auto tokens = Pull(*lower, "Straße ÜBER Ab1 LongerThanTwelveXYZ");
  ASSERT_EQ(5u, tokens.size());
  EXPECT_EQ("stra", tokens[0].term);
  EXPECT_EQ(0u, tokens[0].offs_start);
  EXPECT_EQ(4u, tokens[0].offs_end);
  EXPECT_EQ("e", tokens[1].term);
  EXPECT_EQ(6u, tokens[1].offs_start);
  EXPECT_EQ(7u, tokens[1].offs_end);
  EXPECT_EQ("ber", tokens[2].term);
  EXPECT_EQ(10u, tokens[2].offs_start);
  EXPECT_EQ(13u, tokens[2].offs_end);
  EXPECT_EQ("ab1", tokens[3].term);
  EXPECT_EQ("longerthantwelvexyz", tokens[4].term);
  EXPECT_EQ(18u, tokens[4].offs_start);
  EXPECT_EQ(37u, tokens[4].offs_end);

  auto upper =
    SplitByNonAlphaTokenizer::Make({.case_convert = irs::Case::Upper});
  const auto up = Pull(*upper, "Straße über");
  ASSERT_EQ(3u, up.size());
  EXPECT_EQ("STRA", up[0].term);
  EXPECT_EQ("E", up[1].term);
  EXPECT_EQ("BER", up[2].term);
  EXPECT_EQ(10u, up[2].offs_start);
  EXPECT_EQ(13u, up[2].offs_end);
}

TEST(split_by_non_alpha_tokenizer_test, case_convert_long_short_mix) {
  auto lower =
    SplitByNonAlphaTokenizer::Make({.case_convert = irs::Case::Lower});
  const std::string long_a(40, 'A');
  const std::string long_q(70, 'Q');
  const std::string long_z(300, 'Z');
  for (const std::string& v :
       {long_a + " Xy " + long_q + " Ab" + long_z + "1 Cd",
        std::string(61, 'M') + " Nn " + std::string(13, 'P') + " r " +
          std::string(12, 'S') + std::string(52, 'T') + " Uv",
        std::string(127, 'K') + " L " + std::string(128, 'W') + " x",
        "aB " + std::string(255, 'C') + "d " + std::string(20, 'E') + " Fg",
        std::string(13, 'H') + " " + std::string(13, 'I') + " " +
          std::string(13, 'J')}) {
    SCOPED_TRACE(testing::Message() << "size=" << v.size());
    const auto runs = ReferenceRuns(v);
    const auto lo = Pull(*lower, v);
    ASSERT_EQ(runs.size(), lo.size());
    for (size_t i = 0; i < runs.size(); ++i) {
      SCOPED_TRACE(testing::Message() << "token=" << i);
      std::string expect(v, runs[i].first, runs[i].second - runs[i].first);
      for (auto& c : expect) {
        c =
          static_cast<char>(absl::ascii_tolower(static_cast<unsigned char>(c)));
      }
      ASSERT_EQ(expect, lo[i].term);
      ASSERT_EQ(runs[i].first, lo[i].offs_start);
      ASSERT_EQ(runs[i].second, lo[i].offs_end);
    }
  }
}

TEST(split_by_non_alpha_tokenizer_test, case_convert_oracle_all_sizes) {
  constexpr std::string_view kAlnum = "abcxyzABCXYZ059";
  constexpr std::string_view kSeps = " ,.-_\t\n\x80\xC3\xA9\xFF";
  auto lower =
    SplitByNonAlphaTokenizer::Make({.case_convert = irs::Case::Lower});
  auto upper =
    SplitByNonAlphaTokenizer::Make({.case_convert = irs::Case::Upper});
  uint64_t seed = 0xf01d;
  const auto next = [&] {
    seed = seed * 6364136223846793005ULL + 1442695040888963407ULL;
    return static_cast<size_t>(seed >> 33);
  };
  for (size_t size = 0; size <= 600; size += size < 80 ? 1 : 7) {
    for (size_t iter = 0; iter < 12; ++iter) {
      const size_t sep_percent = (iter * 9) % 60;
      std::string v(size, '\0');
      for (auto& c : v) {
        c = next() % 100 < sep_percent ? kSeps[next() % kSeps.size()]
                                       : kAlnum[next() % kAlnum.size()];
      }
      SCOPED_TRACE(testing::Message() << "size=" << size << " iter=" << iter
                                      << " value=\"" << v << "\"");
      const auto runs = ReferenceRuns(v);
      const auto lo = Pull(*lower, v);
      const auto up = Pull(*upper, v);
      ASSERT_EQ(runs.size(), lo.size());
      ASSERT_EQ(runs.size(), up.size());
      for (size_t i = 0; i < runs.size(); ++i) {
        SCOPED_TRACE(testing::Message() << "token=" << i);
        std::string expect_lo(v, runs[i].first, runs[i].second - runs[i].first);
        std::string expect_up = expect_lo;
        for (auto& c : expect_lo) {
          c = static_cast<char>(
            absl::ascii_tolower(static_cast<unsigned char>(c)));
        }
        for (auto& c : expect_up) {
          c = static_cast<char>(
            absl::ascii_toupper(static_cast<unsigned char>(c)));
        }
        ASSERT_EQ(expect_lo, lo[i].term);
        ASSERT_EQ(expect_up, up[i].term);
        ASSERT_EQ(runs[i].first, lo[i].offs_start);
        ASSERT_EQ(runs[i].second, lo[i].offs_end);
        ASSERT_EQ(runs[i].first, up[i].offs_start);
        ASSERT_EQ(runs[i].second, up[i].offs_end);
      }
    }
  }
}

TEST(split_by_non_alpha_tokenizer_test, ascii_bytes_pull) {
  auto a = SplitByNonAlphaTokenizer::Make({.chars = Chars::AsciiBytes});
  const auto tokens = Pull(*a,
                           "Grüße, Welt! 北京123 foo\xE2\x80\x94"
                           "bar");
  ASSERT_EQ(4u, tokens.size());
  EXPECT_EQ("Grüße", tokens[0].term);
  EXPECT_EQ(0u, tokens[0].offs_start);
  EXPECT_EQ(7u, tokens[0].offs_end);
  EXPECT_EQ("Welt", tokens[1].term);
  EXPECT_EQ(9u, tokens[1].offs_start);
  EXPECT_EQ(13u, tokens[1].offs_end);
  EXPECT_EQ("北京123", tokens[2].term);
  EXPECT_EQ(15u, tokens[2].offs_start);
  EXPECT_EQ(24u, tokens[2].offs_end);
  EXPECT_EQ(
    "foo\xE2\x80\x94"
    "bar",
    tokens[3].term);
  EXPECT_EQ(25u, tokens[3].offs_start);
  EXPECT_EQ(34u, tokens[3].offs_end);

  auto lower = SplitByNonAlphaTokenizer::Make(
    {.case_convert = irs::Case::Lower, .chars = Chars::AsciiBytes});
  const auto lo = Pull(*lower, "ÜBER Grüße");
  ASSERT_EQ(2u, lo.size());
  EXPECT_EQ("über", lo[0].term);
  EXPECT_EQ("grüße", lo[1].term);
}

TEST(split_by_non_alpha_tokenizer_test, ascii_bytes_oracle_all_sizes) {
  constexpr std::string_view kKept = "abcxyzABCXYZ059\x80\xC3\xA9\xFF";
  constexpr std::string_view kSeps = " ,.-_\t\n";
  uint64_t seed = 0xc0ffee;
  const auto next = [&] {
    seed = seed * 6364136223846793005ULL + 1442695040888963407ULL;
    return static_cast<size_t>(seed >> 33);
  };
  for (const auto case_convert :
       {irs::Case::None, irs::Case::Lower, irs::Case::Upper}) {
    SCOPED_TRACE(static_cast<int>(case_convert));
    auto a = SplitByNonAlphaTokenizer::Make(
      {.case_convert = case_convert, .chars = Chars::AsciiBytes});
    for (size_t size = 0; size <= 300; size += size < 80 ? 1 : 11) {
      for (size_t iter = 0; iter < 8; ++iter) {
        const size_t sep_percent = (iter * 13) % 70;
        std::string v(size, '\0');
        for (auto& c : v) {
          c = next() % 100 < sep_percent ? kSeps[next() % kSeps.size()]
                                         : kKept[next() % kKept.size()];
        }
        SCOPED_TRACE(testing::Message() << "size=" << size << " iter=" << iter);
        const auto runs = ReferenceRunsKeepNonAscii(v);
        ASSERT_EQ(runs, SplitRunsKeepNonAscii(v));
        const auto tokens = Pull(*a, v);
        ASSERT_EQ(runs.size(), tokens.size());
        for (size_t i = 0; i < runs.size(); ++i) {
          SCOPED_TRACE(testing::Message() << "token=" << i);
          std::string expect(v, runs[i].first, runs[i].second - runs[i].first);
          const bool ascii = std::all_of(
            expect.begin(), expect.end(),
            [](char c) { return static_cast<unsigned char>(c) < 0x80; });
          if (case_convert != irs::Case::None && !ascii) {
            std::string converted(
              irs::analysis::casing::CaseConvertUtf8Bound(expect.size()), '\0');
            auto* out = reinterpret_cast<irs::byte_type*>(converted.data());
            converted.resize(
              case_convert == irs::Case::Lower
                ? irs::analysis::casing::CaseConvertUtf8<true>(expect, out)
                : irs::analysis::casing::CaseConvertUtf8<false>(expect, out));
            expect = std::move(converted);
          }
          for (auto& c : expect) {
            const auto b = static_cast<unsigned char>(c);
            if (!ascii) {
              break;
            }
            if (case_convert == irs::Case::Lower) {
              c = static_cast<char>(absl::ascii_tolower(b));
            } else if (case_convert == irs::Case::Upper) {
              c = static_cast<char>(absl::ascii_toupper(b));
            }
          }
          ASSERT_EQ(expect, tokens[i].term);
          ASSERT_EQ(runs[i].first, tokens[i].offs_start);
          ASSERT_EQ(runs[i].second, tokens[i].offs_end);
        }
      }
    }
  }
}

TEST(split_by_non_alpha_tokenizer_test, chars_goldens) {
  constexpr std::string_view kText =
    "Grüße, Welt! 北京123 foo\xE2\x80\x94"
    "bar abc_42";
  EXPECT_EQ((Terms{"Gr", "e", "Welt", "123", "foo", "bar", "abc", "42"}),
            SplitTerms({.chars = Chars::Ascii}, kText));
  EXPECT_EQ((Terms{"Grüße", "Welt", "北京123",
                   "foo\xE2\x80\x94"
                   "bar",
                   "abc", "42"}),
            SplitTerms({.chars = Chars::AsciiBytes}, kText));
  EXPECT_EQ((Terms{"Grüße", "Welt", "北京123", "foo", "bar", "abc", "42"}),
            SplitTerms({.chars = Chars::Alnum}, kText));
  EXPECT_EQ((Terms{"Grüße", "Welt", "北京", "foo", "bar", "abc"}),
            SplitTerms({.chars = Chars::Letters}, kText));
}

TEST(split_by_non_alpha_tokenizer_test, chars_case_conversion) {
  EXPECT_EQ(
    (Terms{"über", "straße", "abc"}),
    SplitTerms({.case_convert = irs::Case::Lower, .chars = Chars::AsciiBytes},
               "ÜBER Straße ABC"));
  EXPECT_EQ(
    (Terms{"über", "straße", "abc"}),
    SplitTerms({.case_convert = irs::Case::Lower, .chars = Chars::Alnum},
               "ÜBER Straße ABC"));
  EXPECT_EQ(
    (Terms{"ÜBER", "STRASSE"}),
    SplitTerms({.case_convert = irs::Case::Upper, .chars = Chars::Letters},
               "über 42 STRASSE"));
}

TEST(split_by_non_alpha_tokenizer_test, marks_stay_in_words) {
  EXPECT_EQ((Terms{"नमस्ते", "दुनिया"}),
            SplitTerms({.chars = Chars::Letters}, "नमस्ते, दुनिया"));
  EXPECT_EQ((Terms{"cafe\xCC\x81"}),
            SplitTerms({.chars = Chars::Alnum}, "cafe\xCC\x81!"));
  EXPECT_EQ((Terms{"x", "y"}),
            SplitTerms({.chars = Chars::Alnum}, "x\xF0\x9F\x98\x80y"));
}

TEST(split_by_non_alpha_tokenizer_test, letter_crosses_block_boundary) {
  auto a = SplitByNonAlphaTokenizer::Make({.chars = Chars::Alnum});
  for (size_t prefix = 25; prefix <= 40; ++prefix) {
    SCOPED_TRACE(prefix);
    std::string value(prefix, 'a');
    value += "\xC3\xA9\xE5\x8C\x97 x";
    const auto tokens = Pull(*a, value);
    ASSERT_EQ(2u, tokens.size());
    EXPECT_EQ(prefix + 5, tokens[0].term.size());
    EXPECT_EQ(0u, tokens[0].offs_start);
    EXPECT_EQ(prefix + 5, tokens[0].offs_end);
    EXPECT_EQ("x", tokens[1].term);
  }
}

TEST(split_by_non_alpha_tokenizer_test, fast_word_ranges_are_words) {
  constexpr std::pair<uint32_t, uint32_t> kRanges[] = {
    {0x00C0, 0x00D6}, {0x00D8, 0x00F6}, {0x00F8, 0x027F}, {0x0400, 0x047F},
    {0x04C0, 0x04FF}, {0x4000, 0x4DBF}, {0x4E00, 0x9FFF}, {0xAC00, 0xD77F},
  };
  for (const auto chars : {Chars::Alnum, Chars::Letters}) {
    SCOPED_TRACE(static_cast<int>(chars));
    auto a = SplitByNonAlphaTokenizer::Make({.chars = chars});
    for (const auto [lo, hi] : kRanges) {
      for (uint32_t cp = lo; cp <= hi; ++cp) {
        std::string v(2 + irs::utf8_utils::kMaxCharSize, 'a');
        v.resize(1 + irs::utf8_utils::FromChar32(
                       cp, reinterpret_cast<irs::byte_type*>(v.data() + 1)));
        v += 'b';
        const auto tokens = Pull(*a, v);
        ASSERT_EQ(1u, tokens.size()) << std::hex << cp;
        ASSERT_EQ(v, tokens[0].term) << std::hex << cp;
      }
    }
  }
}

TEST(split_by_non_alpha_tokenizer_test, unicode_oracle_all_sizes) {
  struct Piece {
    std::string_view bytes;
    bool alnum;
    bool letter;
  };
  constexpr Piece kPieces[] = {
    {"a", true, true},
    {"Z", true, true},
    {"5", true, false},
    {"_", false, false},
    {" ", false, false},
    {",", false, false},
    {"\xC3\xA9", true, true},
    {"\xCC\x81", true, true},
    {"\xE5\x8C\x97", true, true},
    {"\xD9\xA3", true, false},
    {"\xC2\xBD", true, false},
    {"\xC2\xA0", false, false},
    {"\xE2\x80\x94", false, false},
    {"\xE3\x80\x80", false, false},
    {"\xF0\x9F\x98\x80", false, false},
    {"\xF0\x9D\x90\x80", true, true},
    {"\xC3\x97", false, false},
    {"\xC3\xB7", false, false},
    {"\xC4\x80", true, true},
    {"\xD0\x96", true, true},
    {"\xD2\x82", false, false},
    {"\xD3\xBF", true, true},
    {"\xE4\xB7\x80", false, false},
    {"\xE4\xB8\x80", true, true},
    {"\xEA\xB0\x80", true, true},
    {"\xED\x9E\xA3", true, true},
    {"\xCE\xB1", true, true},
    {"\xE3\x81\x82", true, true},
  };
  uint64_t seed = 0xa1a1;
  const auto next = [&] {
    seed = seed * 6364136223846793005ULL + 1442695040888963407ULL;
    return static_cast<size_t>(seed >> 33);
  };
  for (const auto chars : {Chars::Alnum, Chars::Letters}) {
    for (const auto case_convert :
         {irs::Case::None, irs::Case::Lower, irs::Case::Upper}) {
      SCOPED_TRACE(testing::Message()
                   << "chars=" << static_cast<int>(chars)
                   << " case=" << static_cast<int>(case_convert));
      auto a = SplitByNonAlphaTokenizer::Make(
        {.case_convert = case_convert, .chars = chars});
      for (size_t pieces = 0; pieces <= 120; ++pieces) {
        for (size_t iter = 0; iter < 6; ++iter) {
          const size_t sep_percent = (iter * 17) % 80;
          std::string v;
          Runs runs;
          bool open = false;
          for (size_t i = 0; i < pieces; ++i) {
            const bool word = next() % 100 >= sep_percent;
            Piece piece;
            do {
              piece = kPieces[next() % std::size(kPieces)];
            } while ((chars == Chars::Letters ? piece.letter : piece.alnum) !=
                     word);
            if (word && !open) {
              runs.emplace_back(v.size(), 0);
            }
            open = word;
            v += piece.bytes;
            if (word) {
              runs.back().second = v.size();
            }
          }
          SCOPED_TRACE(testing::Message()
                       << "pieces=" << pieces << " iter=" << iter);
          const bool letters = chars == Chars::Letters;
          ASSERT_EQ(runs, (letters ? AlnumRuns<false, true>(v)
                                   : AlnumRuns<false, false>(v)));
          if (HasWideKernels()) {
            ASSERT_EQ(runs, (letters ? AlnumRuns<true, true>(v)
                                     : AlnumRuns<true, false>(v)));
          }
          const auto tokens = Pull(*a, v);
          ASSERT_EQ(runs.size(), tokens.size());
          for (size_t i = 0; i < runs.size(); ++i) {
            SCOPED_TRACE(testing::Message() << "token=" << i);
            const std::string_view run{v.data() + runs[i].first,
                                       runs[i].second - runs[i].first};
            ASSERT_EQ(CaseConverted(run, case_convert), tokens[i].term);
            ASSERT_EQ(runs[i].first, tokens[i].offs_start);
            ASSERT_EQ(runs[i].second, tokens[i].offs_end);
          }
        }
      }
    }
  }
}

TEST(split_by_non_alpha_tokenizer_test, whitespace_goldens) {
  EXPECT_EQ(
    (Terms{"Hello,", "world!",
           "foo\xE2\x80\x94"
           "bar",
           "a\xC2\xA9", "x\xE2\x80\x8By", "end"}),
    SplitTerms({.chars = Chars::Whitespace},
               "Hello,  world!\tfoo\xE2\x80\x94"
               "bar\xC2\xA0"
               "a\xC2\xA9\xE3\x80\x80x\xE2\x80\x8By\xE2\x80\x83\r\nend"));
  EXPECT_EQ(
    (Terms{"über", "straße"}),
    SplitTerms({.case_convert = irs::Case::Lower, .chars = Chars::Whitespace},
               "ÜBER\xE2\x80\xA8Straße"));
  EXPECT_EQ((Terms{}), SplitTerms({.chars = Chars::Whitespace},
                                  " \t\xC2\x85\xE1\x9A\x80\xE2\x81\x9F"));
}

TEST(split_by_non_alpha_tokenizer_test, whitespace_crosses_block_boundary) {
  auto a = SplitByNonAlphaTokenizer::Make({.chars = Chars::Whitespace});
  for (size_t prefix = 26; prefix <= 34; ++prefix) {
    SCOPED_TRACE(prefix);
    std::string value(prefix, 'a');
    value += "\xE3\x80\x80";
    value += "b\xC2\xA0";
    value += "c" + std::string(40, 'd');
    const auto tokens = Pull(*a, value);
    ASSERT_EQ(3u, tokens.size());
    EXPECT_EQ(std::string(prefix, 'a'), tokens[0].term);
    EXPECT_EQ("b", tokens[1].term);
    EXPECT_EQ(prefix + 3, tokens[1].offs_start);
    EXPECT_EQ("c" + std::string(40, 'd'), tokens[2].term);
    EXPECT_EQ(prefix + 6, tokens[2].offs_start);
  }
}

TEST(split_by_non_alpha_tokenizer_test, whitespace_oracle_all_sizes) {
  struct Piece {
    std::string_view bytes;
    bool space;
  };
  constexpr Piece kPieces[] = {
    {"a", false},
    {"Z", false},
    {"5", false},
    {"_", false},
    {",", false},
    {"\x01", false},
    {"\xC3\xA9", false},
    {"\xC2\xA9", false},
    {"\xE2\x80\x94", false},
    {"\xE2\x80\x8B", false},
    {"\xE3\x81\x82", false},
    {"\xE1\x9A\x81", false},
    {"\xF0\x9F\x98\x80", false},
    {" ", true},
    {"\t", true},
    {"\n", true},
    {"\r", true},
    {"\x0B", true},
    {"\x0C", true},
    {"\xC2\x85", true},
    {"\xC2\xA0", true},
    {"\xE1\x9A\x80", true},
    {"\xE2\x80\x80", true},
    {"\xE2\x80\x8A", true},
    {"\xE2\x80\xA8", true},
    {"\xE2\x80\xA9", true},
    {"\xE2\x80\xAF", true},
    {"\xE2\x81\x9F", true},
    {"\xE3\x80\x80", true},
  };
  uint64_t seed = 0x5bace;
  const auto next = [&] {
    seed = seed * 6364136223846793005ULL + 1442695040888963407ULL;
    return static_cast<size_t>(seed >> 33);
  };
  for (const auto case_convert :
       {irs::Case::None, irs::Case::Lower, irs::Case::Upper}) {
    SCOPED_TRACE(static_cast<int>(case_convert));
    auto a = SplitByNonAlphaTokenizer::Make(
      {.case_convert = case_convert, .chars = Chars::Whitespace});
    for (size_t pieces = 0; pieces <= 120; ++pieces) {
      for (size_t iter = 0; iter < 6; ++iter) {
        const size_t sep_percent = (iter * 17) % 80;
        std::string v;
        Runs runs;
        bool open = false;
        for (size_t i = 0; i < pieces; ++i) {
          const bool word = next() % 100 >= sep_percent;
          Piece piece;
          do {
            piece = kPieces[next() % std::size(kPieces)];
          } while (piece.space == word);
          if (word && !open) {
            runs.emplace_back(v.size(), 0);
          }
          open = word;
          v += piece.bytes;
          if (word) {
            runs.back().second = v.size();
          }
        }
        SCOPED_TRACE(testing::Message()
                     << "pieces=" << pieces << " iter=" << iter);
        ASSERT_EQ(runs, NonSpaceRuns<false>(v));
        if (HasWideKernels()) {
          ASSERT_EQ(runs, NonSpaceRuns<true>(v));
        }
        const auto tokens = Pull(*a, v);
        ASSERT_EQ(runs.size(), tokens.size());
        for (size_t i = 0; i < runs.size(); ++i) {
          SCOPED_TRACE(testing::Message() << "token=" << i);
          const std::string_view run{v.data() + runs[i].first,
                                     runs[i].second - runs[i].first};
          ASSERT_EQ(CaseConverted(run, case_convert), tokens[i].term);
          ASSERT_EQ(runs[i].first, tokens[i].offs_start);
          ASSERT_EQ(runs[i].second, tokens[i].offs_end);
        }
      }
    }
  }
}

TEST(split_by_non_alpha_tokenizer_test, native_fill_matches_pull) {
  const std::vector<std::string> values = {
    "Hello, World! 123abc",
    "",
    "   ...   ",
    "one",
    "a-b-c-d-e",
    "Trailing punctuation here!!!",
    "UPPER lower MiXeD 42",
    "TwelveLtrsAB ThirteenLtrsX SuPeRcAlIfRaGiLiStIcExPiAlIdOcIoUs",
    "ElevenLtrsA@TwelveLtrsAB",
    "Grüße ÜBER 北京123 foo\xE2\x80\x94"
    "bar",
    std::string(40, 'x') + "\xC3\xA9" + std::string(30, 'Y')};

  for (const auto& opts : AllConfigs()) {
    SCOPED_TRACE(testing::Message()
                 << "chars=" << static_cast<int>(opts.chars)
                 << " case=" << static_cast<int>(opts.case_convert));
    auto pull_a = SplitByNonAlphaTokenizer::Make(opts);
    auto fill_a = SplitByNonAlphaTokenizer::Make(opts);

    for (const auto& v : values) {
      SCOPED_TRACE(v);
      const auto pulled = Pull(*pull_a, v);

      auto batch = std::make_unique<irs::TokenBatch>();
      std::vector<irs::DocRun> runs;
      std::vector<Tok> filled;
      const auto collect = [&](irs::TokenBatch& batch,
                               std::span<const irs::DocRun>) {
        EXPECT_FALSE(fill_a->Traits().explicit_pos);
        for (uint32_t i = 0; i < batch.count; ++i) {
          const auto& t = batch.terms[i];
          filled.push_back({std::string{t.GetData(), t.GetSize()},
                            batch.offs_start[i], batch.offs_end[i]});
        }
      };
      tests::FnTokenSink sink{irs::TokenLayout::TermsPosOffs, collect};
      ASSERT_TRUE(fill_a->Fill(v, sink.writer, {sink.layout}));
      sink.writer.Finish();

      ASSERT_EQ(pulled.size(), filled.size());
      for (size_t i = 0; i < pulled.size(); ++i) {
        SCOPED_TRACE(i);
        ASSERT_EQ(pulled[i].term, filled[i].term);
        ASSERT_EQ(pulled[i].offs_start, filled[i].offs_start);
        ASSERT_EQ(pulled[i].offs_end, filled[i].offs_end);
      }
    }
  }
}

TEST(split_by_non_alpha_tokenizer_test, column_fill_matches_pull) {
  const std::vector<std::string> raw = {
    "Hello World",        "",    "a1 b2 c3",
    "no-delimiters-here", "END", "ABCDEFGHIJKL ABCDEFGHIJKLM",
    "Grüße ÜBER 北京123"};
  std::vector<duckdb::string_t> values;
  for (size_t i = 0; i < raw.size(); ++i) {
    values.emplace_back(raw[i].data(), static_cast<uint32_t>(raw[i].size()));
  }

  for (const auto& opts : AllConfigs()) {
    SCOPED_TRACE(testing::Message()
                 << "chars=" << static_cast<int>(opts.chars)
                 << " case=" << static_cast<int>(opts.case_convert));
    auto pull_a = SplitByNonAlphaTokenizer::Make(opts);
    auto fill_a = SplitByNonAlphaTokenizer::Make(opts);

    size_t flushes = 0;
    const auto check = [&](irs::TokenBatch& batch, irs::DocRuns runs) {
      ++flushes;
      ASSERT_EQ(raw.size(), runs.size());

      uint32_t token_idx = 0;
      for (size_t v = 0; v < raw.size(); ++v) {
        SCOPED_TRACE(raw[v]);
        ASSERT_EQ(100 + v, runs[v].doc);
        const auto pulled = Pull(*pull_a, raw[v]);
        ASSERT_EQ(pulled.size(), runs[v].ntokens);
        for (const auto& expected : pulled) {
          const auto& t = batch.terms[token_idx];
          ASSERT_EQ(expected.term, (std::string{t.GetData(), t.GetSize()}));
          ASSERT_EQ(expected.offs_start, batch.offs_start[token_idx]);
          ASSERT_EQ(expected.offs_end, batch.offs_end[token_idx]);
          ++token_idx;
        }
      }
      ASSERT_EQ(batch.count, token_idx);
    };
    tests::FnTokenSink sink{irs::TokenLayout::TermsPosOffs, check};
    tests::FillColumn(*fill_a, values, 100, sink.writer, sink.layout);
    sink.writer.Finish();
    ASSERT_EQ(1, flushes);
  }
}

TEST(classify_block_test, NibbleSetAgainstScalar) {
  constexpr size_t kBlock = irs::analysis::classify::kClassifyBlock;
  uint64_t seed = 0xb17e5;
  const auto next = [&] {
    seed = seed * 6364136223846793005ULL + 1442695040888963407ULL;
    return static_cast<size_t>(seed >> 33);
  };
  irs::byte_type block[kBlock];
  for (size_t variant = 0; variant < 200; ++variant) {
    irs::analysis::classify::ByteSet bytes;
    irs::analysis::classify::NibbleSet nibbles;
    const size_t count = 1 + next() % 40;
    for (size_t i = 0; i < count; ++i) {
      const auto b = static_cast<irs::byte_type>(
        variant % 2 == 0 ? next() % 128 : next() % 256);
      bytes.Add(b);
      nibbles.Add(b);
    }
    if (!nibbles.Blockable()) {
      continue;
    }
    for (int c = 0; c < 256; ++c) {
      for (size_t i = 0; i < kBlock; ++i) {
        block[i] = static_cast<irs::byte_type>(next() % 256);
      }
      block[c % kBlock] = static_cast<irs::byte_type>(c);
      uint32_t expect = 0;
      for (size_t i = 0; i < kBlock; ++i) {
        expect |= static_cast<uint32_t>(bytes.Contains(block[i])) << i;
      }
      ASSERT_EQ(expect,
                irs::analysis::classify::ClassifyNibbleBlock(block, nibbles))
        << "variant=" << variant << " c=" << c;
    }
  }
}

TEST(classify_block_test, LoadPaddedExact) {
  constexpr size_t kBlock = irs::analysis::classify::kClassifyBlock;
  irs::byte_type data[kBlock + 8];
  for (size_t i = 0; i < sizeof data; ++i) {
    data[i] = static_cast<irs::byte_type>(0x81 + i * 5);
  }
  for (size_t size = 0; size < kBlock; ++size) {
    const auto block = irs::analysis::classify::LoadPadded(data + 3, size);
    irs::byte_type got[kBlock];
    std::memcpy(got, &block, sizeof got);
    for (size_t i = 0; i < kBlock; ++i) {
      ASSERT_EQ(i < size ? data[3 + i] : 0, got[i])
        << "size=" << size << " i=" << i;
    }
  }
}

TEST(split_by_non_alpha_test, runs_block_boundaries) {
  const auto alnum = [](size_t n) { return std::string(n, 'a'); };
  const auto sep = [](size_t n) { return std::string(n, ' '); };
  for (const std::string& v :
       {std::string{},
        alnum(1),
        alnum(16),
        alnum(31),
        alnum(32),
        alnum(33),
        alnum(40),
        alnum(64),
        alnum(65),
        sep(31),
        sep(32),
        sep(40),
        alnum(31) + sep(1),
        alnum(31) + sep(1) + alnum(1),
        alnum(30) + sep(2) + alnum(30),
        sep(31) + alnum(2),
        sep(32) + alnum(1),
        alnum(32) + sep(1) + alnum(7),
        alnum(63) + sep(1),
        alnum(63) + sep(1) + alnum(1),
        sep(1) + alnum(31) + sep(1) + alnum(31) + sep(1),
        alnum(5) + sep(27) + alnum(5) + sep(27) + alnum(5)}) {
    SCOPED_TRACE(testing::Message() << "size=" << v.size());
    ASSERT_EQ(ReferenceRuns(v), SplitRuns(v));
  }
}

TEST(split_by_non_alpha_test, runs_oracle_all_sizes) {
  constexpr std::string_view kAlnum = "abcxyzABCXYZ059";
  constexpr std::string_view kSeps = " ,.-_\t\n\x80\xC3\xA9\xFF";
  uint64_t seed = 0x5eed;
  const auto next = [&] {
    seed = seed * 6364136223846793005ULL + 1442695040888963407ULL;
    return static_cast<size_t>(seed >> 33);
  };
  for (size_t size = 0; size <= 100; ++size) {
    for (size_t iter = 0; iter < 40; ++iter) {
      const size_t sep_percent = (iter * 7) % 100;
      std::string v(size, '\0');
      for (auto& c : v) {
        c = next() % 100 < sep_percent ? kSeps[next() % kSeps.size()]
                                       : kAlnum[next() % kAlnum.size()];
      }
      SCOPED_TRACE(testing::Message() << "size=" << size << " iter=" << iter
                                      << " value=\"" << v << "\"");
      ASSERT_EQ(ReferenceRuns(v), SplitRuns(v));
    }
  }
}
