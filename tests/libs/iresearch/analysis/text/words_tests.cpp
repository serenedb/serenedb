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
///
/// Test vectors adopted from turbopuffer/alyze (MIT, src/uax29/word/mod.rs)
/// and StringZilla (Apache-2.0, test/utf8_wordbreaks.cpp).
////////////////////////////////////////////////////////////////////////////////

#include <gtest/gtest.h>

#include <algorithm>
#include <fstream>
#include <random>
#include <sstream>
#include <string>
#include <vector>

#include "break_test_utils.hpp"
#include "iresearch/analysis/text/words/unicode.hpp"
#include "iresearch/utils/utf8_case_tables.hpp"
#include "iresearch/utils/utf8_utils.hpp"
#include "tests_config.hpp"

namespace {

using irs::analysis::words::ScanUnicode;
using irs::analysis::words::UnicodeSegment;

using Span = std::pair<uint32_t, uint32_t>;

std::vector<UnicodeSegment> Scan(std::string_view text) {
  std::vector<UnicodeSegment> out;
  ScanUnicode(duckdb::string_t{text.data(), static_cast<uint32_t>(text.size())},
              [&](const UnicodeSegment& seg) { out.push_back(seg); });
  return out;
}

std::vector<Span> Segments(std::string_view text) {
  std::vector<Span> out;
  for (const auto& seg : Scan(text)) {
    out.emplace_back(seg.begin, seg.end);
  }
  return out;
}

std::vector<std::string> Terms(std::string_view text) {
  std::vector<std::string> out;
  for (const auto& seg : Scan(text)) {
    out.emplace_back(text.substr(seg.begin, seg.end - seg.begin));
  }
  return out;
}

TEST(words_unicode_test, word_break_test_conformance) {
  const auto cases = tests::LoadBreakTestCases(IRS_TEST_RESOURCE_DIR
                                               "/unicode/WordBreakTest.txt");
  ASSERT_EQ(1944u, cases.size());
  size_t failures = 0;
  for (const auto& c : cases) {
    std::vector<Span> expected;
    for (size_t k = 0; k + 1 < c.boundaries.size(); ++k) {
      expected.emplace_back(c.boundaries[k], c.boundaries[k + 1]);
    }
    const auto actual = Segments(c.bytes);
    if (actual != expected) {
      ++failures;
      EXPECT_EQ(expected, actual) << "line: " << c.line;
    }
  }
  EXPECT_EQ(0u, failures);
}

TEST(words_unicode_test, ascii_basics) {
  EXPECT_TRUE(Segments("").empty());
  EXPECT_EQ((std::vector<std::string>{"a"}), Terms("a"));
  EXPECT_EQ((std::vector<std::string>{"."}), Terms("."));
  EXPECT_EQ((std::vector<std::string>{"hello"}), Terms("hello"));
  EXPECT_EQ((std::vector<std::string>{"abc123"}), Terms("abc123"));
  EXPECT_EQ((std::vector<std::string>{"won't"}), Terms("won't"));
  EXPECT_EQ((std::vector<std::string>{"example.com"}), Terms("example.com"));
  EXPECT_EQ((std::vector<std::string>{"e.g", ".", " ", "hello"}),
            Terms("e.g. hello"));
  EXPECT_EQ((std::vector<std::string>{"a_1"}), Terms("a_1"));
  EXPECT_EQ((std::vector<std::string>{"_a"}), Terms("_a"));
  EXPECT_EQ((std::vector<std::string>{"3.14"}), Terms("3.14"));
  EXPECT_EQ((std::vector<std::string>{"1,234"}), Terms("1,234"));
  EXPECT_EQ((std::vector<std::string>{"a", ":", "5"}), Terms("a:5"));
  EXPECT_EQ((std::vector<std::string>{"a", ":", ":", "b"}), Terms("a::b"));
  EXPECT_EQ((std::vector<std::string>{"can", "'"}), Terms("can'"));
  EXPECT_EQ((std::vector<std::string>{"can", "'", " ", "hi"}),
            Terms("can' hi"));
  EXPECT_EQ((std::vector<std::string>{"a", "   ", "c"}), Terms("a   c"));
}

TEST(words_unicode_test, crlf_wb3) {
  EXPECT_EQ((std::vector<std::string>{"\r\n"}), Terms("\r\n"));
  EXPECT_EQ((std::vector<std::string>{"\r\n", "\r\n"}), Terms("\r\n\r\n"));
  EXPECT_EQ((std::vector<std::string>{"\r"}), Terms("\r"));
  EXPECT_EQ((std::vector<std::string>{"\n", "\n"}), Terms("\n\n"));
  EXPECT_EQ((std::vector<std::string>{"\n", "\r"}), Terms("\n\r"));
  EXPECT_EQ((std::vector<Span>{{0, 5}, {5, 7}, {7, 12}}),
            Segments("line1\r\nline2"));
}

TEST(words_unicode_test, mixed_straddle) {
  EXPECT_EQ((std::vector<std::string>{"caf\xC3\xA9", " ", "society"}),
            Terms("caf\xC3\xA9 society"));
  EXPECT_EQ((std::vector<Span>{{0, 5}, {5, 6}, {6, 13}}),
            Segments("caf\xC3\xA9 society"));
  EXPECT_EQ((std::vector<std::string>{"\xC3\xA9طab"}), Terms("\xC3\xA9طab"));
  EXPECT_EQ((std::vector<std::string>{"abc\xCC\x81"
                                      "def"}),
            Terms("abc\xCC\x81"
                  "def"));
}

TEST(words_unicode_test, block_boundary_straddles) {
  for (const size_t k : {29u, 30u, 31u, 32u, 33u, 63u, 64u, 65u}) {
    const std::string glue = std::string(k, 'a') + "\xC3\xA9" + "b";
    EXPECT_EQ((std::vector<Span>{{0, static_cast<uint32_t>(k + 3)}}),
              Segments(glue))
      << "k=" << k;
    const std::string brk = std::string(k, 'a') + "\xE3\x80\x82";
    EXPECT_EQ((std::vector<Span>{
                {0, static_cast<uint32_t>(k)},
                {static_cast<uint32_t>(k), static_cast<uint32_t>(k + 3)}}),
              Segments(brk))
      << "k=" << k;
  }
}

TEST(words_unicode_test, regional_indicator_parity) {
  EXPECT_EQ((std::vector<Span>{{0, 4}}), Segments("\xF0\x9F\x87\xA6"));
  EXPECT_EQ((std::vector<Span>{{0, 8}}),
            Segments("\xF0\x9F\x87\xA6\xF0\x9F\x87\xA6"));
  EXPECT_EQ((std::vector<Span>{{0, 8}, {8, 12}}),
            Segments("\xF0\x9F\x87\xA6\xF0\x9F\x87\xA6\xF0\x9F\x87\xA6"));
  EXPECT_EQ(
    (std::vector<Span>{{0, 2}, {2, 10}, {10, 18}, {18, 22}, {22, 24}}),
    Segments("ab\xF0\x9F\x87\xA6\xF0\x9F\x87\xBA\xF0\x9F\x87\xA6\xF0\x9F\x87"
             "\xBA\xF0\x9F\x87\xA6"
             "cd"));
}

TEST(words_unicode_test, zwj_extended_pictographic) {
  EXPECT_EQ((std::vector<Span>{{0, 11}}),
            Segments("\xF0\x9F\x91\xA8\xE2\x80\x8D\xF0\x9F\x91\xA9"));
  EXPECT_EQ((std::vector<Span>{{0, 4}, {4, 8}}),
            Segments("\xF0\x9F\x91\xA8\xF0\x9F\x91\xA9"));
  EXPECT_EQ((std::vector<Span>{{0, 6}}), Segments("\xE2\x80\x8D\xE2\x93\x82"));
  EXPECT_EQ((std::vector<std::string>{"a\xE2\x80\x8D\xF0\x9F\x8D\x95", "x"}),
            Terms("a\xE2\x80\x8D\xF0\x9F\x8D\x95x"));
  EXPECT_EQ((std::vector<std::string>{"a", ":\xE2\x80\x8D\xF0\x9F\x8D\x95"}),
            Terms("a:\xE2\x80\x8D\xF0\x9F\x8D\x95"));
  EXPECT_EQ((std::vector<std::string>{" \xE2\x80\x8D\xF0\x9F\x8D\x95", " "}),
            Terms(" \xE2\x80\x8D\xF0\x9F\x8D\x95 "));
}

TEST(words_unicode_test, hebrew_quotes) {
  const std::string sq = "\xD7\x90'";
  EXPECT_EQ((std::vector<std::string>{sq}), Terms(sq));
  const std::string sq_word = "\xD7\x90'\xD7\x90";
  EXPECT_EQ((std::vector<std::string>{sq_word}), Terms(sq_word));
  const std::string sq_latin = "\xD7\x90'a";
  EXPECT_EQ((std::vector<std::string>{sq_latin}), Terms(sq_latin));
  const std::string dq_word = "\xD7\xA6\xD7\x94\xD7\xB4\xD7\x9C";
  EXPECT_EQ((std::vector<std::string>{dq_word}), Terms(dq_word));
  const std::string dq_trail = "\xD7\x90\xD7\xB4 a";
  EXPECT_EQ((std::vector<std::string>{"\xD7\x90", "\xD7\xB4", " ", "a"}),
            Terms(dq_trail));
}

TEST(words_unicode_test, mid_with_extend) {
  EXPECT_EQ((std::vector<std::string>{"a:\xCC\x88"
                                      "b"}),
            Terms("a:\xCC\x88"
                  "b"));
  EXPECT_EQ((std::vector<std::string>{"a", ":\xCC\x88"}), Terms("a:\xCC\x88"));
}

TEST(words_unicode_test, cjk_per_glyph) {
  const std::string_view text{"今天下午"};
  EXPECT_EQ((std::vector<Span>{{0, 3}, {3, 6}, {6, 9}, {9, 12}}),
            Segments(text));
}

TEST(words_unicode_test, segment_flags) {
  const auto segs = Scan("can' hi");
  ASSERT_EQ(4u, segs.size());
  EXPECT_TRUE(segs[0].has_ascii_alpha);
  EXPECT_TRUE(segs[0].ascii_only);
  EXPECT_FALSE(segs[1].has_ascii_alpha);
  EXPECT_FALSE(segs[2].has_ascii_alpha);
  EXPECT_TRUE(segs[3].has_ascii_alpha);

  const auto mixed = Scan(
    "\xD0\xB6"
    "1");
  ASSERT_EQ(1u, mixed.size());
  EXPECT_FALSE(mixed[0].ascii_only);
  EXPECT_FALSE(mixed[0].has_ascii_alpha);
  EXPECT_TRUE(mixed[0].has_ascii_digit);

  const auto cafe = Scan("caf\xC3\xA9");
  ASSERT_EQ(1u, cafe.size());
  EXPECT_FALSE(cafe[0].ascii_only);
  EXPECT_TRUE(cafe[0].has_ascii_alpha);

  const auto quote = Scan("a'\xCF\x89");
  ASSERT_EQ(1u, quote.size());
  EXPECT_FALSE(quote[0].ascii_only);
  EXPECT_TRUE(quote[0].has_ascii_alpha);
}

TEST(words_unicode_test, segments_tile_input) {
  std::mt19937_64 rng{42};
  const std::string_view pool =
    "aZ09_:;,.' \r\n\t\v\f#\"!x7\xC3\xA9\xD0\xB6\xE4\xBB\x8A\xF0\x9F\x8D\x95"
    "\xE2\x80\x8D\xCC\x81\xF0\x9F\x87\xA6\xFF\x80\xC3\xED\xA0\x80";
  for (size_t round = 0; round < 500; ++round) {
    std::string text;
    const size_t len = rng() % 96;
    for (size_t k = 0; k < len; ++k) {
      text += pool[rng() % pool.size()];
    }
    const auto segs = Segments(text);
    const auto again = Segments(text);
    ASSERT_EQ(segs, again);
    uint32_t prev = 0;
    for (const auto& [begin, end] : segs) {
      ASSERT_EQ(prev, begin) << "round " << round;
      ASSERT_LT(begin, end);
      prev = end;
    }
    ASSERT_EQ(text.size(), prev);
  }
}

std::string Cp(uint32_t cp) {
  irs::byte_type buf[irs::utf8_utils::kMaxCharSize];
  const auto len = irs::utf8_utils::FromChar32(cp, buf);
  return {reinterpret_cast<const char*>(buf), len};
}

void ExpectLengths(const std::string& text,
                   const std::vector<uint32_t>& lengths, size_t run) {
  std::vector<uint32_t> actual;
  for (const auto& [begin, end] : Segments(text)) {
    actual.push_back(end - begin);
  }
  EXPECT_EQ(lengths, actual) << "run=" << run << " text=" << text;
}

TEST(words_unicode_test, alyze_sanity) {
  const std::string alef = Cp(0x5D0);
  const std::string joiner = Cp(0x2060);
  const std::string sq_joined = alef + "'" + joiner + alef;
  EXPECT_EQ((std::vector<std::string>{sq_joined}), Terms(sq_joined));

  const std::string checkers =
    Cp(0x5D4) + Cp(0x5E6) + "'" + Cp(0x5E7) + Cp(0x5E8) + Cp(0x5D5) + Cp(0x5EA);
  EXPECT_EQ((std::vector<std::string>{checkers}), Terms(checkers));

  const std::string life = Cp(0x5DC) + Cp(0x5D9) + Cp(0x5D9) + Cp(0x5E3);
  const std::string energy =
    Cp(0x5D0) + Cp(0x5E0) + Cp(0x5E8) + Cp(0x5D2) + "'" + Cp(0x5D9);
  EXPECT_EQ((std::vector<std::string>{life, " ", energy}),
            Terms(life + " " + energy));

  const std::string gershayim = Cp(0x5F4);
  const std::string express =
    Cp(0x5D0) + Cp(0x5E7) + Cp(0x5E1) + Cp(0x5E4) + Cp(0x5E8) + Cp(0x5E1);
  const std::string today =
    Cp(0x5DE) + Cp(0x5D4) + Cp(0x5D9) + Cp(0x5D5) + Cp(0x5DD);
  EXPECT_EQ(
    (std::vector<std::string>{gershayim, express, gershayim, " ", today}),
    Terms(gershayim + express + gershayim + " " + today));

  const auto stop_sign = Cp(0x1F6D1);
  const auto segs = Scan("ab" + stop_sign);
  ASSERT_EQ(2u, segs.size());
  EXPECT_TRUE(segs[0].ascii_only);
  EXPECT_TRUE(segs[0].has_ascii_alpha);
  EXPECT_FALSE(segs[1].ascii_only);
}

TEST(words_unicode_test, sz_unit_goldens) {
  EXPECT_EQ((std::vector<std::string>{"don't"}), Terms("don't"));
  EXPECT_EQ((std::vector<std::string>{"3,14"}), Terms("3,14"));
  EXPECT_EQ((std::vector<std::string>{"3", ","}), Terms("3,"));
  EXPECT_EQ((std::vector<std::string>{"can't_stop"}), Terms("can't_stop"));
  EXPECT_EQ((std::vector<std::string>{"a", "\r\n", "b"}), Terms("a\r\nb"));
  EXPECT_EQ((std::vector<std::string>{Cp(0x4F60), Cp(0x597D)}),
            Terms(Cp(0x4F60) + Cp(0x597D)));
  EXPECT_EQ((std::vector<std::string>{"Hello", ",", " ", "world", "!"}),
            Terms("Hello, world!"));
}

TEST(words_unicode_test, sz_rule_motifs) {
  const std::string kata_a = Cp(0x30A2);
  const std::string alef = Cp(0x5D0);
  const std::string middot = Cp(0xB7);
  const std::string ri_us = Cp(0x1F1FA) + Cp(0x1F1F8);

  EXPECT_EQ((std::vector<std::string>{"l'avion"}), Terms("l'avion"));
  EXPECT_EQ((std::vector<std::string>{"1,2,3"}), Terms("1,2,3"));
  EXPECT_EQ((std::vector<std::string>{"word" + middot + "word"}),
            Terms("word" + middot + "word"));
  EXPECT_EQ((std::vector<std::string>{kata_a + kata_a}),
            Terms(kata_a + kata_a));
  EXPECT_EQ((std::vector<std::string>{kata_a, "z"}), Terms(kata_a + "z"));
  EXPECT_EQ((std::vector<std::string>{Cp(0xC548) + Cp(0xB155)}),
            Terms(Cp(0xC548) + Cp(0xB155)));
  const std::string arabic =
    Cp(0x645) + Cp(0x631) + Cp(0x62D) + Cp(0x628) + Cp(0x627);
  EXPECT_EQ((std::vector<std::string>{arabic + "ok"}), Terms(arabic + "ok"));
  EXPECT_EQ((std::vector<std::string>{ri_us}), Terms(ri_us));
  EXPECT_EQ((std::vector<std::string>{Cp(0x1F1FA), "a", Cp(0x1F1F8)}),
            Terms(Cp(0x1F1FA) + "a" + Cp(0x1F1F8)));

  EXPECT_EQ((std::vector<std::string>{"a", "'", "'", "b"}), Terms("a''b"));
  EXPECT_EQ((std::vector<std::string>{"a", "\"", "b"}), Terms("a\"b"));
  EXPECT_EQ((std::vector<std::string>{"1", "\"", "2"}), Terms("1\"2"));
  EXPECT_EQ((std::vector<std::string>{"1", ",", ",", "2"}), Terms("1,,2"));
}

TEST(words_unicode_test, sz_deferred_mid_goldens) {
  const std::string grave = Cp(0x300);
  const std::string he = Cp(0x5D4);
  const std::string emoji = Cp(0x1F600);
  const std::string zwj = Cp(0x200D);
  const std::string math_five = Cp(0x1D7D7);

  for (const size_t run : {3u, 8u, 15u, 31u, 32u, 33u, 100u}) {
    std::string marks;
    for (size_t k = 0; k != run; ++k) {
      marks += grave;
    }
    const auto m = static_cast<uint32_t>(marks.size());
    ExpectLengths("5," + marks + "6", {3 + m}, run);
    ExpectLengths("5," + marks + math_five, {6 + m}, run);
    ExpectLengths("a:" + marks + "b", {3 + m}, run);
    ExpectLengths(he + "\"" + marks + he, {5 + m}, run);
    ExpectLengths("5," + marks + "a", {1, 1 + m, 1}, run);
    ExpectLengths("a:" + marks + "5", {1, 1 + m, 1}, run);
    ExpectLengths("5," + marks, {1, 1 + m}, run);
    ExpectLengths("5," + marks + ",5", {1, 1 + m, 1, 1}, run);
    ExpectLengths(he + "'" + marks + "x", {4 + m}, run);
    ExpectLengths(he + "'" + marks + "5", {3 + m, 1}, run);
    ExpectLengths(he + "\"" + marks + "x", {2, 1 + m, 1}, run);
    ExpectLengths("a:" + marks + zwj + emoji, {1, 8 + m}, run);
  }
  ExpectLengths("5," + emoji, {1, 1, 4}, 0);
}

TEST(words_unicode_test, sz_seam_regressions) {
  const std::string_view seams[] = {
    "\xE3\x82\xAB\x2D\x0A\xF0\x9F\x87\xA6\x62\xC2\xAD\xF0\x9F\x87\xA6\xCC"
    "\x80\x0A\x5F\xF0\x9F\x87\xA6\xF0\x9F\x87\xA6\xC2\xAD\x0A\xCC\x88\xF0"
    "\x9F\x87\xBA\xF0\x9F\x8F\xBB\xCC\x88\xC2\xAD\xE2\x81\xA0\xCC\x88\xF0"
    "\x9F\x87\xA6\xF0\x9F\x87\xA6\xE4\xB8\xAD\xE2\x81\xA0\x5F",
    "\x5F\x27\xF0\x9F\x87\xBA\xE4\xB8\xAD\xCC\x81\xF0\x9F\x87\xA6\xF0\x9F"
    "\x87\xA6\x5F\xCC\x88\x5F\xD7\x90\x2D\xE2\x93\x82\xCC\x80\x62\xF0\x9F"
    "\x87\xA6\xE2\x81\xA0\xF0\x9F\x87\xBA\xCC\x80\xCC\x88\xF0\x9F\x8F\xBB"
    "\xF0\x9F\x87\xBA\xCC\x81\xF0\x9F\x87\xBA\x0A\xC2\xAD\xF0\x9F\x87\xBA"
    "\xCC\x88\xE3\x82\xAB\x5F\x27\x2E\xCC\x88\x0A\xE4\xB8\xAD",
    "\x5F\xE2\x81\xA0\x5F\x35\x2C\xF0\x9F\x98\x80\x27\xF0\x9F\x98\x80\x5F"
    "\xCC\x88\xCC\x81\xE4\xB8\xAD\xE2\x81\xA0\x5F\x62\xCC\x80\x61\xE4\xB8"
    "\xAD\xE3\x82\xAB\x35\xE2\x81\xA0\xCC\x88\xCC\x88\xF0\x9F\x8F\xBB\xCC"
    "\x88\xE2\x81\xA0\xCC\x80\xCC\x81\x27\x35\xCC\x80\xE4\xB8\xAD\xCC\x81"
    "\xE2\x80\x8D\xC3\xA9\xE3\x80\x80\x35\x27",
  };
  for (const auto seam : seams) {
    const auto segs = Segments(seam);
    ASSERT_EQ(segs, Segments(seam));
    uint32_t prev = 0;
    for (const auto& [begin, end] : segs) {
      ASSERT_EQ(prev, begin);
      ASSERT_LT(begin, end);
      prev = end;
    }
    ASSERT_EQ(seam.size(), prev);
  }
}

TEST(words_unicode_test, sz_dense_runs) {
  const std::string kata = Cp(0x30AB);
  const std::string acute = Cp(0x301);
  const std::string alef = Cp(0x5D0);
  const std::string bet = Cp(0x5D1);
  const std::string ri = Cp(0x1F1E6);
  for (const size_t count : {60u, 100u, 220u}) {
    std::string katakana;
    std::string midletter;
    std::string hebrew;
    std::string numeric;
    std::string flags;
    for (size_t k = 0; k != count; ++k) {
      katakana += kata;
      midletter += (k & 1u) != 0 ? "a" + Cp(0xB7) + "b" : "a'b";
      hebrew += alef + "'" + bet;
      numeric += "12" + acute + ",34 ";
      flags += ri;
    }
    EXPECT_EQ(1u, Segments(katakana).size()) << count;
    EXPECT_EQ(1u, Segments(midletter).size()) << count;
    EXPECT_EQ(1u, Segments(hebrew).size()) << count;
    EXPECT_EQ(2 * count, Segments(numeric).size()) << count;
    EXPECT_EQ((count + 1) / 2, Segments(flags).size()) << count;
    EXPECT_EQ(count / 2 + 1, Segments(flags + "a").size()) << count;
  }
}

TEST(utf8_case_tables_test, sorted_and_spot_values) {
  using irs::utf8_utils::kSimpleLowerTable;
  using irs::utf8_utils::kSimpleUpperTable;
  EXPECT_TRUE(
    std::is_sorted(kSimpleLowerTable.begin(), kSimpleLowerTable.end()));
  EXPECT_TRUE(
    std::is_sorted(kSimpleUpperTable.begin(), kSimpleUpperTable.end()));
  const auto lower = [](uint32_t cp) {
    const auto it =
      std::lower_bound(kSimpleLowerTable.begin(), kSimpleLowerTable.end(),
                       irs::utf8_utils::CaseMap{cp, 0});
    return it != kSimpleLowerTable.end() && it->cp == cp ? it->to : cp;
  };
  const auto upper = [](uint32_t cp) {
    const auto it =
      std::lower_bound(kSimpleUpperTable.begin(), kSimpleUpperTable.end(),
                       irs::utf8_utils::CaseMap{cp, 0});
    return it != kSimpleUpperTable.end() && it->cp == cp ? it->to : cp;
  };
  EXPECT_EQ(0x61u, lower(0x41));
  EXPECT_EQ(0x2C65u, lower(0x23A));
  EXPECT_EQ(0x69u, lower(0x130));
  EXPECT_EQ(0x3C3u, lower(0x3A3));
  EXPECT_EQ(0x430u, lower(0x410));
  EXPECT_EQ(0x39Cu, upper(0xB5));
  EXPECT_EQ(0xA77Du, upper(0x1D79));
  EXPECT_EQ(0xDFu, upper(0xDF));
  EXPECT_EQ(0x4ECAu, lower(0x4ECA));
  EXPECT_EQ(1u, irs::utf8_utils::kSimpleCaseMaxUtf8Growth);
}

}  // namespace
