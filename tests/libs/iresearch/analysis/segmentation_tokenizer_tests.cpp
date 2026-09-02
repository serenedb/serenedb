////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2021 ArangoDB GmbH, Cologne, Germany
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
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
///
/// @author Andrei Lobov
////////////////////////////////////////////////////////////////////////////////

#include <functional>
#include <vector>

#include "gtest/gtest.h"
#include "iresearch/analysis/icu_text_tokenizer.hpp"
#include "iresearch/analysis/segmentation_tokenizer.hpp"
#include "iresearch/analysis/text/words/ascii.hpp"
#include "iresearch/analysis/token_batch.hpp"
#include "iresearch/utils/utf8_character_tables.hpp"
#include "pg/sql_exception.h"
#include "tests_config.hpp"
#include "token_sink_asserts.hpp"
#include "token_sink_utils.hpp"

namespace {

struct AnalyzerToken {
  std::string_view value;
  size_t start;
  size_t end;
  uint32_t pos;
};

using AnalyzerTokens = std::vector<AnalyzerToken>;

// The pre-mask per-byte scan, kept verbatim as the differential reference.
void ReferenceScanAsciiWords(
  std::string_view value,
  const std::function<void(const irs::analysis::words::AsciiSegment&)>& emit) {
  using namespace irs::analysis::words;
  const auto* b = reinterpret_cast<const unsigned char*>(value.data());
  const size_t n = value.size();
  const auto word_class = [](uint8_t cls) {
    return cls == kAL || cls == kNU || cls == kEX;
  };
  size_t i = 0;
  while (i < n) {
    const size_t seg_begin = i;
    const uint8_t c0 = kWbClass[b[i]];
    bool has_alpha = false;
    bool has_digit = false;
    const auto advance = [&] {
      while (i < n) {
        const uint8_t cls = kWbClass[b[i]];
        if (!word_class(cls)) {
          return;
        }
        has_alpha |= cls == kAL;
        has_digit |= cls == kNU;
        ++i;
      }
    };
    switch (c0) {
      case kWS:
        do {
          ++i;
        } while (i < n && kWbClass[b[i]] == kWS);
        break;
      case kNL:
        i += (b[i] == '\r' && i + 1 < n && b[i + 1] == '\n') ? 2 : 1;
        break;
      case kAL:
      case kNU:
      case kEX: {
        advance();
        while (i + 1 < n) {
          const uint8_t c = kWbClass[b[i]];
          const uint8_t last = kWbClass[b[i - 1]];
          const uint8_t next = kWbClass[b[i + 1]];
          if (last == kAL && next == kAL &&
              (c == kML || c == kMNL || c == kSQ)) {
            ++i;
            advance();
            continue;
          }
          if (last == kNU && next == kNU &&
              (c == kMN || c == kMNL || c == kSQ)) {
            ++i;
            advance();
            continue;
          }
          break;
        }
        break;
      }
      default:
        ++i;
        break;
    }
    emit(irs::analysis::words::AsciiSegment{static_cast<uint32_t>(seg_begin),
                                            static_cast<uint32_t>(i), has_alpha,
                                            has_digit});
  }
}

}  // namespace

TEST(words_ascii_test, MaskScanMatchesReference) {
  using irs::analysis::words::AsciiSegment;
  const auto collect_ref = [](std::string_view v) {
    std::vector<AsciiSegment> out;
    ReferenceScanAsciiWords(v,
                            [&](const AsciiSegment& s) { out.push_back(s); });
    return out;
  };
  const auto check = [&](const std::string& v) {
    SCOPED_TRACE(testing::Message() << "n=" << v.size() << " v=" << v);
    const auto expect = collect_ref(v);

    std::vector<AsciiSegment> all;
    irs::analysis::words::ScanAscii(
      v, [&](const AsciiSegment& s) { all.push_back(s); });
    ASSERT_EQ(expect.size(), all.size());
    for (size_t k = 0; k < expect.size(); ++k) {
      ASSERT_EQ(expect[k].begin, all[k].begin) << k;
      ASSERT_EQ(expect[k].end, all[k].end) << k;
      ASSERT_EQ(expect[k].has_alpha, all[k].has_alpha) << k;
      ASSERT_EQ(expect[k].has_digit, all[k].has_digit) << k;
    }
  };

  check("");
  check("hello world");
  check("a");
  check(" ");
  check("...");
  for (const size_t len : {30, 31, 32, 33, 34, 63, 64, 65, 95, 97}) {
    check(std::string(len, 'a'));
    check(std::string(len, ' '));
    check(std::string(len, '#'));
    std::string mid(len, 'a');
    mid[len / 2] = ':';
    check(mid);
    std::string num(len, '7');
    num[len / 2] = '.';
    check(num);
  }
  for (const size_t at : {29, 30, 31, 32, 33}) {
    std::string v(64, 'a');
    v[at] = ':';
    check(v);
    v[at] = '.';
    check(v);
    v[at] = '\'';
    check(v);
    v[at] = ' ';
    check(v);
    v[at] = '\n';
    check(v);
    std::string d(64, '5');
    d[at] = '.';
    check(d);
    d[at] = ',';
    check(d);
  }
  for (const size_t at : {29, 30, 31, 32, 33, 62, 63}) {
    std::string v(64, 'a');
    v[at] = '\r';
    v[at + 1 < v.size() ? at + 1 : at] = '\n';
    check(v);
  }
  check("line1\r\nline2");
  check("\r\n\r\n\r");
  check("a\rb\nc\r\n");
  uint64_t rng = 0x243F6A8885A308D3ull;
  const char kAlphabet[] = "aZ09_:;,.' \r\n\t\v\f#\"!x7";
  for (int round = 0; round < 400; ++round) {
    rng = rng * 6364136223846793005ull + 1442695040888963407ull;
    const size_t len = 1 + ((rng >> 33) % 130);
    std::string v(len, ' ');
    for (auto& c : v) {
      rng = rng * 6364136223846793005ull + 1442695040888963407ull;
      c = kAlphabet[(rng >> 40) % (sizeof(kAlphabet) - 1)];
    }
    check(v);
  }
  std::string high(70, 'a');
  high[35] = static_cast<char>(0xC3);
  high[36] = static_cast<char>(0xA9);
  check(high);
}

TEST(words_ascii_test, WordRunsMatchFilteredScan) {
  using irs::analysis::words::AsciiSegment;
  const auto check = [](const std::string& v) {
    SCOPED_TRACE(testing::Message() << "n=" << v.size() << " v=" << v);
    std::vector<AsciiSegment> expect;
    irs::analysis::words::ScanAscii(v, [&](const AsciiSegment& s) {
      const auto cls =
        irs::analysis::words::kWbClass[static_cast<unsigned char>(v[s.begin])];
      if (irs::analysis::words::IsWordClass(cls)) {
        expect.push_back(s);
      }
    });
    std::vector<AsciiSegment> got;
    irs::analysis::words::ScanAsciiRuns(
      v, [&](const AsciiSegment& s) { got.push_back(s); });
    ASSERT_EQ(expect.size(), got.size());
    for (size_t k = 0; k < expect.size(); ++k) {
      ASSERT_EQ(expect[k].begin, got[k].begin) << k;
      ASSERT_EQ(expect[k].end, got[k].end) << k;
      ASSERT_EQ(expect[k].has_alpha, got[k].has_alpha) << k;
      ASSERT_EQ(expect[k].has_digit, got[k].has_digit) << k;
    }
  };
  check("");
  check("hello world");
  check("don't stop... 3.14, 1'000 _x M.I.T. line1\r\nline2");
  check("   leading and trailing   ");
  check("!!!");
  for (const size_t len : {30, 31, 32, 33, 34, 63, 64, 65, 95, 97}) {
    check(std::string(len, 'a'));
    check(std::string(len, ' '));
    check(std::string(len, '#') + "x");
  }
  uint64_t rng = 0x9E3779B97F4A7C15ull;
  const char kAlphabet[] = "aZ09_:;,.' \r\n\t\v\f#\"!x7";
  for (int round = 0; round < 400; ++round) {
    rng = rng * 6364136223846793005ull + 1442695040888963407ull;
    const size_t len = 1 + ((rng >> 33) % 130);
    std::string v(len, ' ');
    for (auto& c : v) {
      rng = rng * 6364136223846793005ull + 1442695040888963407ull;
      c = kAlphabet[(rng >> 40) % (sizeof(kAlphabet) - 1)];
    }
    check(v);
  }
}

void AssertStream(irs::analysis::Tokenizer* pipe, std::string_view data,
                  const AnalyzerTokens& expected_tokens) {
  SCOPED_TRACE(data);
  std::vector<irs::bstring> terms;
  std::vector<uint32_t> starts;
  std::vector<uint32_t> ends;
  const auto collect = [&](irs::TokenBatch& batch,
                           std::span<const irs::DocRun> runs) {
    ASSERT_FALSE(pipe->Traits().explicit_pos);
    ASSERT_TRUE(runs.empty());
    for (uint32_t i = 0; i < batch.count; ++i) {
      const auto& t = batch.terms[i];
      terms.emplace_back(reinterpret_cast<const irs::byte_type*>(t.GetData()),
                         t.GetSize());
      starts.push_back(batch.offs_start[i]);
      ends.push_back(batch.offs_end[i]);
    }
  };
  tests::FnTokenSink sink{irs::TokenLayout::TermsPosOffs, collect};
  ASSERT_TRUE(pipe->Fill(tests::ToStringT(data), sink.writer, {sink.layout}));
  sink.writer.Finish();

  ASSERT_EQ(expected_tokens.size(), terms.size());
  for (size_t i = 0; i < expected_tokens.size(); ++i) {
    const auto& e = expected_tokens[i];
    SCOPED_TRACE(testing::Message("Expected term:<") << e.value << ">");
    ASSERT_EQ(irs::ViewCast<irs::byte_type>(e.value), terms[i]);
    ASSERT_EQ(e.start, starts[i]);
    ASSERT_EQ(e.end, ends[i]);
    ASSERT_EQ(e.pos, i);
  }
}

using namespace irs::analysis;
using Options = SegmentationTokenizer::Options;

class SegmentationTokenizerTest : public testing::TestWithParam<bool> {};

TEST(SegmentationTokenizerTest, consts) {
  static_assert("segmentation" == irs::Type<SegmentationTokenizer>::name());
  EXPECT_TRUE(std::is_sorted(irs::utf8_utils::kSmallCategoryTable.begin(),
                             irs::utf8_utils::kSmallCategoryTable.end()));
  EXPECT_TRUE(std::is_sorted(irs::utf8_utils::kLargeCategoryTable.begin(),
                             irs::utf8_utils::kLargeCategoryTable.end()));
}

TEST_P(SegmentationTokenizerTest, alpha_no_case_test) {
  Options opt{
    .convert = Options::Convert::None,
  };
  auto stream = SegmentationTokenizer::Make(std::move(opt));
  constexpr std::string_view kData =
    "File:Constantinople(1878)-Turkish Goverment information brocure (1950s) "
    "- Istanbul coffee house.png";
  const AnalyzerTokens expected{{"File:Constantinople", 0, 19, 0},
                                {"1878", 20, 24, 1},
                                {"Turkish", 26, 33, 2},
                                {"Goverment", 34, 43, 3},
                                {"information", 44, 55, 4},
                                {"brocure", 56, 63, 5},
                                {"1950s", 65, 70, 6},
                                {"Istanbul", 74, 82, 7},
                                {"coffee", 83, 89, 8},
                                {"house.png", 90, 99, 9}};
  AssertStream(stream.get(), kData, expected);
}

TEST_P(SegmentationTokenizerTest, alpha_lower_case_test) {
  Options opt{};  // Lower is default
  auto stream = SegmentationTokenizer::Make(std::move(opt));
  constexpr std::string_view kData =
    "File:Constantinople(1878)-Turkish Goverment information brocure (1950s) "
    "- Istanbul coffee house.png";
  const AnalyzerTokens expected{{"file:constantinople", 0, 19, 0},
                                {"1878", 20, 24, 1},
                                {"turkish", 26, 33, 2},
                                {"goverment", 34, 43, 3},
                                {"information", 44, 55, 4},
                                {"brocure", 56, 63, 5},
                                {"1950s", 65, 70, 6},
                                {"istanbul", 74, 82, 7},
                                {"coffee", 83, 89, 8},
                                {"house.png", 90, 99, 9}};
  AssertStream(stream.get(), kData, expected);
}

TEST_P(SegmentationTokenizerTest, alpha_upper_case_test) {
  Options opt{
    .convert = Options::Convert::Upper,
  };
  auto stream = SegmentationTokenizer::Make(std::move(opt));

  constexpr std::string_view kData =
    "File:Constantinople(1878)-Turkish Goverment information brocure (1950s) "
    "- Istanbul coffee house.png";
  const AnalyzerTokens expected{{"FILE:CONSTANTINOPLE", 0, 19, 0},
                                {"1878", 20, 24, 1},
                                {"TURKISH", 26, 33, 2},
                                {"GOVERMENT", 34, 43, 3},
                                {"INFORMATION", 44, 55, 4},
                                {"BROCURE", 56, 63, 5},
                                {"1950S", 65, 70, 6},
                                {"ISTANBUL", 74, 82, 7},
                                {"COFFEE", 83, 89, 8},
                                {"HOUSE.PNG", 90, 99, 9}};
  AssertStream(stream.get(), kData, expected);
}

TEST_P(SegmentationTokenizerTest, graphic_upper_case_test) {
  Options opt{
    .accept = Options::Accept::Graphic,
    .convert = Options::Convert::Upper,
  };
  auto stream = SegmentationTokenizer::Make(std::move(opt));
  constexpr std::string_view kData =
    "File:Constantinople(1878)-Turkish Goverment information brocure (1950s) "
    "- Istanbul coffee house.png";
  const AnalyzerTokens expected{{"FILE:CONSTANTINOPLE", 0, 19, 0},
                                {"(", 19, 20, 1},
                                {"1878", 20, 24, 2},
                                {")", 24, 25, 3},
                                {"-", 25, 26, 4},
                                {"TURKISH", 26, 33, 5},
                                {"GOVERMENT", 34, 43, 6},
                                {"INFORMATION", 44, 55, 7},
                                {"BROCURE", 56, 63, 8},
                                {"(", 64, 65, 9},
                                {"1950S", 65, 70, 10},
                                {")", 70, 71, 11},
                                {"-", 72, 73, 12},
                                {"ISTANBUL", 74, 82, 13},
                                {"COFFEE", 83, 89, 14},
                                {"HOUSE.PNG", 90, 99, 15}};
  AssertStream(stream.get(), kData, expected);
}

TEST_P(SegmentationTokenizerTest, all_lower_case_test) {
  Options opt{
    .accept = Options::Accept::Any,
    .convert = Options::Convert::Lower,
  };
  auto stream = SegmentationTokenizer::Make(std::move(opt));
  constexpr std::string_view kData =
    "File:Constantinople(1878)-Turkish Goverment information brocure (1950s) "
    "- Istanbul coffee house.png";
  const AnalyzerTokens expected{{"file:constantinople", 0, 19, 0},
                                {"(", 19, 20, 1},
                                {"1878", 20, 24, 2},
                                {")", 24, 25, 3},
                                {"-", 25, 26, 4},
                                {"turkish", 26, 33, 5},
                                {" ", 33, 34, 6},
                                {"goverment", 34, 43, 7},
                                {" ", 43, 44, 8},
                                {"information", 44, 55, 9},
                                {" ", 55, 56, 10},
                                {"brocure", 56, 63, 11},
                                {" ", 63, 64, 12},
                                {"(", 64, 65, 13},
                                {"1950s", 65, 70, 14},
                                {")", 70, 71, 15},
                                {" ", 71, 72, 16},
                                {"-", 72, 73, 17},
                                {" ", 73, 74, 18},
                                {"istanbul", 74, 82, 19},
                                {" ", 82, 83, 20},
                                {"coffee", 83, 89, 21},
                                {" ", 89, 90, 22},
                                {"house.png", 90, 99, 23}};
  AssertStream(stream.get(), kData, expected);
}

TEST_P(SegmentationTokenizerTest, chinese_glyphs_test) {
  constexpr std::u8string_view kData =
    u8"\u4ECA\u5929\u4E0B\u5348\u7684\u592A\u9633\u5F88\u6E29\u6696\u3002";
  Options opt{};
  auto stream = SegmentationTokenizer::Make(std::move(opt));

  const auto glyph = [&](size_t i) {
    return std::string_view{reinterpret_cast<const char*>(kData.data()) + i * 3,
                            3};
  };
  AnalyzerTokens expected;
  for (uint32_t i = 0; i < 10; ++i) {
    expected.push_back({glyph(i), i * 3U, i * 3U + 3U, i});
  }
  AssertStream(stream.get(), irs::ViewCast<char>(kData), expected);
}

TEST_P(SegmentationTokenizerTest, crlf_merges_wb3) {
  Options opt{
    .accept = Options::Accept::Any,
    .convert = Options::Convert::None,
  };
  auto stream = SegmentationTokenizer::Make(std::move(opt));
  const AnalyzerTokens expected{
    {"line1", 0, 5, 0}, {"\r\n", 5, 7, 1}, {"line2", 7, 12, 2}};
  AssertStream(stream.get(), "line1\r\nline2", expected);
}

TEST_P(SegmentationTokenizerTest, simple_case_semantics) {
  {
    Options opt{};
    auto stream = SegmentationTokenizer::Make(std::move(opt));
    AssertStream(stream.get(), "\xC4\xB0", {{"i", 0, 2, 0}});
  }
  {
    Options opt{};
    auto stream = SegmentationTokenizer::Make(std::move(opt));
    AssertStream(stream.get(), "\xCE\x9F\xCE\x94\xCE\x9F\xCE\xA3",
                 {{"\xCE\xBF\xCE\xB4\xCE\xBF\xCF\x83", 0, 8, 0}});
  }
  {
    Options opt{
      .convert = Options::Convert::Upper,
    };
    auto stream = SegmentationTokenizer::Make(std::move(opt));
    AssertStream(stream.get(),
                 "stra\xC3\x9F"
                 "e",
                 {{"STRA\xC3\x9F"
                   "E",
                   0, 7, 0}});
  }
  {
    Options opt{};
    auto stream = SegmentationTokenizer::Make(std::move(opt));
    AssertStream(stream.get(), "\xC8\xBAx", {{"\xE2\xB1\xA5x", 0, 3, 0}});
  }
  {
    Options opt{};
    auto stream = SegmentationTokenizer::Make(std::move(opt));
    AssertStream(stream.get(), "\xE4\xBB\x8A\xE5\xA4\xA9",
                 {{"\xE4\xBB\x8A", 0, 3, 0}, {"\xE5\xA4\xA9", 3, 6, 1}});
  }
}

TEST_P(SegmentationTokenizerTest, mixed_value_run_switching) {
  Options opt{};
  auto stream = SegmentationTokenizer::Make(std::move(opt));
  const AnalyzerTokens expected{{"caf\xC3\xA9", 0, 5, 0},
                                {"men\xC3\xBA", 6, 11, 1},
                                {"society", 12, 19, 2},
                                {"123", 20, 23, 3}};
  AssertStream(stream.get(), "caf\xC3\xA9 MEN\xC3\x9A society 123", expected);
}

TEST_P(SegmentationTokenizerTest, non_ascii_alpha_accept) {
  Options opt{
    .accept = Options::Accept::Alpha,
    .convert = Options::Convert::None,
  };
  auto stream = SegmentationTokenizer::Make(std::move(opt));
  const AnalyzerTokens expected{
    {"\xE5\x8C\x97", 0, 3, 0}, {"\xE4\xBA\xAC", 3, 6, 1}, {"x", 7, 8, 2}};
  AssertStream(stream.get(), "\xE5\x8C\x97\xE4\xBA\xAC x", expected);
}

TEST(SegmentationTokenizerTest, make_empty_object) {
  auto stream = SegmentationTokenizer::Make(Options{});
  ASSERT_TRUE(stream);
  const AnalyzerTokens expected{{"test", 0, 4, 0}, {"retest", 7, 13, 1}};
  std::string data = "Test - ReTeSt";
  AssertStream(stream.get(), data, expected);
}

TEST(SegmentationTokenizerTest, make_lowercase) {
  auto stream =
    SegmentationTokenizer::Make(Options{.convert = Options::Convert::Lower});
  ASSERT_TRUE(stream);
  const AnalyzerTokens expected{{"test", 0, 4, 0}, {"retest", 7, 13, 1}};
  std::string data = "Test - ReTeSt";
  AssertStream(stream.get(), data, expected);
}

TEST(SegmentationTokenizerTest, make_nonecase) {
  auto stream =
    SegmentationTokenizer::Make(Options{.convert = Options::Convert::None});
  ASSERT_TRUE(stream);
  const AnalyzerTokens expected{{"Test", 0, 4, 0}, {"ReTeSt", 7, 13, 1}};
  std::string data = "Test - ReTeSt";
  AssertStream(stream.get(), data, expected);
}

TEST(SegmentationTokenizerTest, make_uppercase) {
  auto stream =
    SegmentationTokenizer::Make(Options{.convert = Options::Convert::Upper});
  ASSERT_TRUE(stream);
  const AnalyzerTokens expected{{"TEST", 0, 4, 0}, {"RETEST", 7, 13, 1}};
  std::string data = "Test - ReTeSt";
  AssertStream(stream.get(), data, expected);
}

TEST(SegmentationTokenizerTest, make_uppercase_alphabreak) {
  auto stream = SegmentationTokenizer::Make(Options{
    .accept = Options::Accept::Alpha,
    .convert = Options::Convert::Upper,
  });
  ASSERT_TRUE(stream);
  const AnalyzerTokens expected{{"TEST", 0, 4, 0}, {"RETEST", 7, 13, 1}};
  std::string data = "Test - ReTeSt";
  AssertStream(stream.get(), data, expected);
}

TEST(SegmentationTokenizerTest, make_uppercase_all_break) {
  auto stream = SegmentationTokenizer::Make(Options{
    .accept = Options::Accept::Any,
    .convert = Options::Convert::Upper,
  });
  ASSERT_TRUE(stream);
  const AnalyzerTokens expected{{"TEST", 0, 4, 0},
                                {" ", 4, 5, 1},
                                {"-", 5, 6, 2},
                                {" ", 6, 7, 3},
                                {"RETEST", 7, 13, 4}};
  std::string data = "Test - ReTeSt";
  AssertStream(stream.get(), data, expected);
}

TEST(SegmentationTokenizerTest, make_uppercase_graphic_break) {
  auto stream = SegmentationTokenizer::Make(Options{
    .accept = Options::Accept::Graphic,
    .convert = Options::Convert::Upper,
  });
  ASSERT_TRUE(stream);
  const AnalyzerTokens expected{
    {"TEST", 0, 4, 0}, {"-", 5, 6, 1}, {"RETEST", 7, 13, 2}};
  std::string data = "Test - ReTeSt";
  AssertStream(stream.get(), data, expected);
}

// The legacy tests `make_invalidcase`, `make_numbercase`,
// `make_uppercase_invalid_break`, `make_uppercase_invalid_number_break`,
// and `make_invalid_json` all exercised JSON-parser-level type / enum
// validation (rejecting e.g. `"case": 1`, `"break": "_INVALID_"`,
// non-object root values, etc.). The direct-Options API uses
// strongly-typed enums (`Options::Convert`, `Options::Accept`) so these
// assertions are now compile-time impossibilities and collapse to the
// happy-path enum-driven `make_*` cases above.
TEST(SegmentationTokenizerTest, make_default_smoke) {
  // Default-initialized Options must produce a usable analyzer.
  auto stream = SegmentationTokenizer::Make(Options{});
  ASSERT_NE(nullptr, stream);
}

INSTANTIATE_TEST_SUITE_P(SegmentationWithAsciiOptimization,
                         SegmentationTokenizerTest,
                         testing::Values(false, true));

namespace {

std::vector<tests::AnalyzerToken> PullSegmentation(
  irs::analysis::Tokenizer& stream, std::string_view data) {
  auto tokens = tests::Analyze(stream, data);
  EXPECT_TRUE(tokens.has_value());
  return tokens ? std::move(*tokens) : std::vector<tests::AnalyzerToken>{};
}

}  // namespace

TEST_P(SegmentationTokenizerTest, native_fills_match_pull) {
  std::string huge;
  for (size_t i = 0; i < 1500; ++i) {
    huge += "word" + std::to_string(i) + " ";
  }
  const std::vector<std::string> values = {
    "",
    "Test - ReTeSt",
    "quick  BROWN fox!! 123 --- end",
    "TwelveLtrsAB ThirteenLtrsX MiXeD",
    "\xe4\xbb\x8a\xe5\xa4\xa9\xe4\xb8\x8b\xe5\x8d\x88",
    "caf\xc3\xa9 MEN\xc3\x9a  123abc",
    "ab\xF0\x9F\x87\xA6\xF0\x9F\x87\xBA\xF0\x9F\x87\xA6"
    "cd",
    "a\xE2\x80\x8D\xF0\x9F\x8D\x95x",
    "abc\xCC\x81"
    "def line1\r\nline2",
    "\xCE\x9F\xCE\x94\xCE\x9F\xCE\xA3 123 \xD0\xB6",
    std::string(200, 'X'),
    huge};

  for (const auto accept :
       {Options::Accept::Any, Options::Accept::Graphic,
        Options::Accept::AlphaNumeric, Options::Accept::Alpha}) {
    for (const auto convert : {Options::Convert::None, Options::Convert::Lower,
                               Options::Convert::Upper}) {
      Options opts{.accept = accept, .convert = convert};
      auto pull_stream = SegmentationTokenizer::Make(Options{opts});
      auto fill_stream = SegmentationTokenizer::Make(Options{opts});
      for (const auto& v : values) {
        SCOPED_TRACE(testing::Message()
                     << "accept=" << int(accept) << " convert=" << int(convert)
                     << " value.size=" << v.size());
        const auto pulled = PullSegmentation(*pull_stream, v);

        std::vector<irs::bstring> terms;
        std::vector<uint32_t> starts;
        std::vector<uint32_t> ends;
        const auto collect = [&](irs::TokenBatch& batch,
                                 std::span<const irs::DocRun> /*runs*/) {
          ASSERT_FALSE(fill_stream->Traits().explicit_pos);
          for (uint32_t i = 0; i < batch.count; ++i) {
            const auto& t = batch.terms[i];
            terms.emplace_back(
              reinterpret_cast<const irs::byte_type*>(t.GetData()),
              t.GetSize());
            starts.push_back(batch.offs_start[i]);
            ends.push_back(batch.offs_end[i]);
          }
        };
        tests::FnTokenSink sink{irs::TokenLayout::TermsPosOffs, collect};
        ASSERT_TRUE(fill_stream->Fill(v, sink.writer, {sink.layout}));
        sink.writer.Finish();

        ASSERT_EQ(pulled.size(), terms.size());
        for (size_t i = 0; i < pulled.size(); ++i) {
          SCOPED_TRACE(testing::Message() << "token=" << i);
          ASSERT_EQ(
            irs::ViewCast<irs::byte_type>(std::string_view{pulled[i].term}),
            terms[i]);
          ASSERT_EQ(pulled[i].offs_start, starts[i]);
          ASSERT_EQ(pulled[i].offs_end, ends[i]);
        }
      }
    }
  }
}

TEST_P(SegmentationTokenizerTest, column_fill_runs) {
  auto stream = SegmentationTokenizer::Make(Options{});
  auto* analyzer = dynamic_cast<irs::analysis::Tokenizer*>(stream.get());
  ASSERT_NE(nullptr, analyzer);

  std::string big;
  for (size_t i = 0; i < 1200; ++i) {
    big += "tok" + std::to_string(i) + " ";
  }
  std::string big_mixed;
  for (size_t i = 0; i < 1200; ++i) {
    big_mixed += "caf\xC3\xA9" + std::to_string(i) + " ";
  }
  const std::vector<std::string> values = {"alpha beta", "", big, big_mixed,
                                           "tail"};

  std::vector<std::vector<irs::bstring>> expected(values.size());
  for (size_t v = 0; v < values.size(); ++v) {
    for (const auto& t : PullSegmentation(*analyzer, values[v])) {
      expected[v].emplace_back(
        irs::ViewCast<irs::byte_type>(std::string_view{t.term}));
    }
  }

  std::vector<duckdb::string_t> vals;
  for (size_t i = 0; i < values.size(); ++i) {
    vals.emplace_back(values[i].data(),
                      static_cast<uint32_t>(values[i].size()));
  }

  std::vector<std::vector<irs::bstring>> got(values.size());
  size_t flushes = 0;
  const auto collect = [&](irs::TokenBatch& batch,
                           std::span<const irs::DocRun> runs) {
    if (batch.count == irs::TokenBatch::kCapacity) {
      ++flushes;
    }
    uint32_t tok = 0;
    for (size_t r = 0; r < runs.size(); ++r) {
      const auto& run = runs[r];
      for (uint32_t j = 0; j < run.ntokens; ++j, ++tok) {
        const auto& t = batch.terms[tok];
        got[run.doc - 1].emplace_back(
          reinterpret_cast<const irs::byte_type*>(t.GetData()), t.GetSize());
      }
    }
    ASSERT_EQ(batch.count, tok);
  };
  tests::FnTokenSink sink{irs::TokenLayout::Terms, collect};
  tests::FillColumn(*analyzer, vals, 1, sink.writer, sink.layout);
  sink.writer.Finish();

  ASSERT_GT(flushes, 0);
  ASSERT_EQ(expected, got);
}

namespace {

void AssertAsciiMatchesUnicode(const Options& opts, std::string_view value) {
  auto stream = SegmentationTokenizer::Make(Options{opts});
  tests::AssertAsciiMatchesUnicode(*stream, value);
}

}  // namespace

TEST(SegmentationTokenizerAsciiFastPath, mid_rule_goldens) {
  const std::vector<std::string> values = {
    "don't",     "can't stop", "3.14",
    "1,234",     "1'000",      "foo_bar",
    "a:b",       "a.b",        "1:2",
    "a,b",       "1.a",        "a.5",
    "a_.b",      "a..b",       "1..2",
    "x\ty",      "a  b",       "line1\r\nline2",
    "a'",        "'a",         "\"quoted\"",
    "a.b.c d,e", "_lead",      "trail_",
    "M.I.T.",    "e.g. so",    "$5.00",
    "a-b",       "",           " ",
    ".",         "..",         "a",
    "Z9_z'x.q",
  };
  for (const auto accept :
       {Options::Accept::Any, Options::Accept::Graphic,
        Options::Accept::AlphaNumeric, Options::Accept::Alpha}) {
    for (const auto convert : {Options::Convert::None, Options::Convert::Lower,
                               Options::Convert::Upper}) {
      Options opts{.accept = accept, .convert = convert};
      for (const auto& v : values) {
        SCOPED_TRACE(testing::Message()
                     << "accept=" << int(accept) << " convert=" << int(convert)
                     << " value=\"" << v << "\"");
        AssertAsciiMatchesUnicode(opts, v);
      }
    }
  }
}

TEST(SegmentationTokenizerAsciiFastPath, property_oracle_random_ascii) {
  constexpr std::string_view kCharset =
    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
    "'.,:;_\"!?-()[] \t\r\n"
    "aaaeeiioo 0123  ..''";
  uint64_t seed = 0x5eed5eed5eedULL;
  const auto next = [&] {
    seed = seed * 6364136223846793005ULL + 1442695040888963407ULL;
    return static_cast<size_t>(seed >> 33);
  };
  for (const auto accept :
       {Options::Accept::Any, Options::Accept::Graphic,
        Options::Accept::AlphaNumeric, Options::Accept::Alpha}) {
    for (const auto convert : {Options::Convert::None, Options::Convert::Lower,
                               Options::Convert::Upper}) {
      Options opts{.accept = accept, .convert = convert};
      for (size_t iter = 0; iter < 300; ++iter) {
        std::string v;
        const size_t len = next() % 120;
        v.reserve(len);
        for (size_t i = 0; i < len; ++i) {
          v += kCharset[next() % kCharset.size()];
        }
        SCOPED_TRACE(testing::Message()
                     << "accept=" << int(accept) << " convert=" << int(convert)
                     << " iter=" << iter << " value=\"" << v << "\"");
        AssertAsciiMatchesUnicode(opts, v);
      }
    }
  }
}

TEST(SegmentationTokenizerAsciiFastPath, non_ascii_takes_unicode_path) {
  Options opts{};
  AssertAsciiMatchesUnicode(opts, "caf\xc3\xa9 society");
  AssertAsciiMatchesUnicode(
    opts, "\xd0\xbf\xd1\x80\xd0\xb8\xd0\xb2\xd0\xb5\xd1\x82 mixed ascii");
}

namespace {

irs::analysis::SegmentationTokenizer::Options ModeOpts(
  irs::analysis::SegmentationTokenizer::Options::Separate separate,
  irs::analysis::SegmentationTokenizer::Options::Convert convert =
    irs::analysis::SegmentationTokenizer::Options::Convert::None) {
  using Opts = irs::analysis::SegmentationTokenizer::Options;
  Opts opts;
  opts.separate = separate;
  opts.accept = Opts::Accept::Any;
  opts.convert = convert;
  return opts;
}

using ModeSeparate = irs::analysis::SegmentationTokenizer::Options::Separate;
using ModeConvert = irs::analysis::SegmentationTokenizer::Options::Convert;

}  // namespace

TEST(sentence_tokenizer_test, goldens) {
  auto stream = irs::analysis::SegmentationTokenizer::Make(
    ModeOpts(ModeSeparate::Sentence));
  ASSERT_TRUE(stream->Traits().offsets);
  {
    const auto tokens =
      tests::Analyze(*stream, "Hello world. Second sentence! Third?");
    ASSERT_TRUE(tokens.has_value());
    ASSERT_EQ(3, tokens->size());
    EXPECT_EQ((tests::AnalyzerToken{"Hello world.", 1, 0, 12}), (*tokens)[0]);
    EXPECT_EQ((tests::AnalyzerToken{"Second sentence!", 2, 13, 29}),
              (*tokens)[1]);
    EXPECT_EQ((tests::AnalyzerToken{"Third?", 3, 30, 36}), (*tokens)[2]);
  }
  {
    const auto tokens = tests::Analyze(*stream, "  spaced out.  \n");
    ASSERT_TRUE(tokens.has_value());
    ASSERT_EQ(1, tokens->size());
    EXPECT_EQ((tests::AnalyzerToken{"spaced out.", 1, 2, 13}), tokens->front());
  }
  {
    const auto tokens = tests::Analyze(*stream, "no terminator here");
    ASSERT_TRUE(tokens.has_value());
    ASSERT_EQ(1, tokens->size());
    EXPECT_EQ((tests::AnalyzerToken{"no terminator here", 1, 0, 18}),
              tokens->front());
  }
  {
    const auto tokens = tests::Analyze(*stream, "");
    ASSERT_TRUE(tokens.has_value());
    EXPECT_TRUE(tokens->empty());
  }
  {
    const auto tokens =
      tests::Analyze(*stream,
                     "\xD0\x9F\xD1\x80\xD0\xB8\xD0\xB2\xD0\xB5\xD1"
                     "\x82. \xE4\xBB\x8A\xE5\xA4\xA9!");
    ASSERT_TRUE(tokens.has_value());
    ASSERT_EQ(2, tokens->size());
    EXPECT_EQ("\xD0\x9F\xD1\x80\xD0\xB8\xD0\xB2\xD0\xB5\xD1\x82.",
              (*tokens)[0].term);
    EXPECT_EQ("\xE4\xBB\x8A\xE5\xA4\xA9!", (*tokens)[1].term);
  }
}

TEST(sentence_tokenizer_test, case_conversion) {
  auto stream = irs::analysis::SegmentationTokenizer::Make(
    ModeOpts(ModeSeparate::Sentence, ModeConvert::Lower));
  const auto tokens = tests::Analyze(*stream, "Hello World. CAF\xC3\x89 Time!");
  ASSERT_TRUE(tokens.has_value());
  ASSERT_EQ(2, tokens->size());
  EXPECT_EQ("hello world.", (*tokens)[0].term);
  EXPECT_EQ("caf\xC3\xA9 time!", (*tokens)[1].term);
}

TEST(line_tokenizer_test, goldens) {
  auto stream =
    irs::analysis::SegmentationTokenizer::Make(ModeOpts(ModeSeparate::Line));
  {
    const auto tokens =
      tests::Analyze(*stream, "first line\r\nsecond line\nthird");
    ASSERT_TRUE(tokens.has_value());
    ASSERT_EQ(3, tokens->size());
    EXPECT_EQ((tests::AnalyzerToken{"first line", 1, 0, 10}), (*tokens)[0]);
    EXPECT_EQ((tests::AnalyzerToken{"second line", 2, 12, 23}), (*tokens)[1]);
    EXPECT_EQ((tests::AnalyzerToken{"third", 3, 24, 29}), (*tokens)[2]);
  }
  {
    const auto tokens = tests::Analyze(*stream, "a\n\n\nb\n");
    ASSERT_TRUE(tokens.has_value());
    ASSERT_EQ(2, tokens->size());
    EXPECT_EQ("a", (*tokens)[0].term);
    EXPECT_EQ("b", (*tokens)[1].term);
  }
  {
    const auto tokens =
      tests::Analyze(*stream, "nel\xC2\x85ls\xE2\x80\xA8ps\xE2\x80\xA9tail");
    ASSERT_TRUE(tokens.has_value());
    ASSERT_EQ(4, tokens->size());
    EXPECT_EQ("nel", (*tokens)[0].term);
    EXPECT_EQ("ls", (*tokens)[1].term);
    EXPECT_EQ("ps", (*tokens)[2].term);
    EXPECT_EQ("tail", (*tokens)[3].term);
  }
}

TEST(paragraph_tokenizer_test, goldens) {
  auto stream = irs::analysis::SegmentationTokenizer::Make(
    ModeOpts(ModeSeparate::Paragraph));
  {
    const auto tokens = tests::Analyze(
      *stream, "para one line1\npara one line2\n\npara two\n\n\n\npara three");
    ASSERT_TRUE(tokens.has_value());
    ASSERT_EQ(3, tokens->size());
    EXPECT_EQ("para one line1\npara one line2", (*tokens)[0].term);
    EXPECT_EQ("para two", (*tokens)[1].term);
    EXPECT_EQ("para three", (*tokens)[2].term);
  }
  {
    const auto tokens =
      tests::Analyze(*stream, "one\r\n\r\ntwo\xE2\x80\xA9three");
    ASSERT_TRUE(tokens.has_value());
    ASSERT_EQ(3, tokens->size());
    EXPECT_EQ("one", (*tokens)[0].term);
    EXPECT_EQ("two", (*tokens)[1].term);
    EXPECT_EQ("three", (*tokens)[2].term);
  }
  {
    const auto tokens = tests::Analyze(*stream, "single\nnewlines\nglue");
    ASSERT_TRUE(tokens.has_value());
    ASSERT_EQ(1, tokens->size());
    EXPECT_EQ("single\nnewlines\nglue", tokens->front().term);
  }
  {
    const auto tokens = tests::Analyze(*stream, "\n\nlead and trail\n\n");
    ASSERT_TRUE(tokens.has_value());
    ASSERT_EQ(1, tokens->size());
    EXPECT_EQ("lead and trail", tokens->front().term);
  }
}

TEST(paragraph_tokenizer_test, spaced_blank_line_is_not_a_boundary) {
  auto stream = irs::analysis::SegmentationTokenizer::Make(
    ModeOpts(ModeSeparate::Paragraph));
  const auto tokens = tests::Analyze(*stream, "a\n \nb");
  ASSERT_TRUE(tokens.has_value());
  ASSERT_EQ(1, tokens->size());
  EXPECT_EQ("a\n \nb", tokens->front().term);
}

TEST(sentence_tokenizer_test, native_fills_match_pull) {
  const std::vector<std::string> values = {
    "Hello world. Second sentence! Third?", "no terminator",
    "One. Two. Three. Four. Five.",
    "line one\nline two\r\n\r\npara two\xE2\x80\xA9para three",
    std::string(200, 'x') + ". tail"};
  for (const auto separate :
       {ModeSeparate::Sentence, ModeSeparate::Line, ModeSeparate::Paragraph}) {
    SCOPED_TRACE(testing::Message()
                 << "separate=" << static_cast<int>(separate));
    auto stream =
      irs::analysis::SegmentationTokenizer::Make(ModeOpts(separate));
    for (const auto& value : values) {
      const auto pulled = tests::Analyze(*stream, value);
      ASSERT_TRUE(pulled.has_value());
      size_t flushes = 0;
      const auto check = [&](irs::TokenBatch& batch, irs::DocRuns /*runs*/) {
        ++flushes;
        ASSERT_EQ(pulled->size(), batch.count);
        for (uint32_t i = 0; i < batch.count; ++i) {
          const auto& t = batch.terms[i];
          ASSERT_EQ((*pulled)[i].term,
                    std::string_view(t.GetData(), t.GetSize()));
          ASSERT_EQ((*pulled)[i].offs_start, batch.offs_start[i]);
          ASSERT_EQ((*pulled)[i].offs_end, batch.offs_end[i]);
        }
      };
      tests::FnTokenSink sink{irs::TokenLayout::TermsPosOffs, check};
      ASSERT_TRUE(stream->Fill(tests::ToStringT(value), irs::doc_limits::min(),
                               sink.writer, {sink.layout}));
      sink.writer.Finish();
      ASSERT_EQ(1, flushes);
    }
  }
}

TEST(SegmentationTextAdoptedTest, number_grouping) {
  auto stream = SegmentationTokenizer::Make(Options{});
  const auto tokens = tests::Analyze(*stream, "1,24 prosenttia");
  ASSERT_TRUE(tokens.has_value());
  const std::vector<tests::AnalyzerToken> expected{{"1,24", 1, 0, 4},
                                                   {"prosenttia", 2, 5, 15}};
  ASSERT_EQ(expected, *tokens);
}

TEST(SegmentationTextAdoptedTest, whitespace_word_stream) {
  auto stream = SegmentationTokenizer::Make(Options{});
  const auto tokens = tests::Analyze(
    *stream,
    " A  hErd of   quIck brown  foXes ran    and Jumped over  a     "
    "runninG dog");
  ASSERT_TRUE(tokens.has_value());
  const std::vector<tests::AnalyzerToken> expected{
    {"a", 1, 1, 2},       {"herd", 2, 4, 8},    {"of", 3, 9, 11},
    {"quick", 4, 14, 19}, {"brown", 5, 20, 25}, {"foxes", 6, 27, 32},
    {"ran", 7, 33, 36},   {"and", 8, 40, 43},   {"jumped", 9, 44, 50},
    {"over", 10, 51, 55}, {"a", 11, 57, 58},    {"running", 12, 63, 70},
    {"dog", 13, 71, 74}};
  ASSERT_EQ(expected, *tokens);
}

TEST(SegmentationTextAdoptedTest, case_modes) {
  const std::string_view data = "A qUiCk brOwn FoX";
  {
    auto stream =
      SegmentationTokenizer::Make(Options{.convert = Options::Convert::Lower});
    const auto tokens = tests::AnalyzeTerms(*stream, data);
    ASSERT_TRUE(tokens.has_value());
    const std::vector<std::string> expected{"a", "quick", "brown", "fox"};
    ASSERT_EQ(expected, *tokens);
  }
  {
    auto stream =
      SegmentationTokenizer::Make(Options{.convert = Options::Convert::Upper});
    const auto tokens = tests::AnalyzeTerms(*stream, data);
    ASSERT_TRUE(tokens.has_value());
    const std::vector<std::string> expected{"A", "QUICK", "BROWN", "FOX"};
    ASSERT_EQ(expected, *tokens);
  }
  {
    auto stream =
      SegmentationTokenizer::Make(Options{.convert = Options::Convert::None});
    const auto tokens = tests::AnalyzeTerms(*stream, data);
    ASSERT_TRUE(tokens.has_value());
    const std::vector<std::string> expected{"A", "qUiCk", "brOwn", "FoX"};
    ASSERT_EQ(expected, *tokens);
  }
}

TEST(SegmentationTextAdoptedTest, russian_stream_both_engines) {
  const std::string_view data =
    "по вечерам ежик ходил к медвежонку считать звезды";
  const std::vector<tests::AnalyzerToken> expected{
    {"по", 1, 0, 4},        {"вечерам", 2, 5, 19}, {"ежик", 3, 20, 28},
    {"ходил", 4, 29, 39},   {"к", 5, 40, 42},      {"медвежонку", 6, 43, 63},
    {"считать", 7, 64, 78}, {"звезды", 8, 79, 91}};
  {
    auto stream = SegmentationTokenizer::Make(Options{});
    const auto tokens = tests::Analyze(*stream, data);
    ASSERT_TRUE(tokens.has_value());
    ASSERT_EQ(expected, *tokens);
  }
  {
    auto stream = IcuTextTokenizer::Make(IcuTextTokenizer::Options{
      .locale = icu::Locale::createFromName("ru_RU")});
    const auto tokens = tests::Analyze(*stream, data);
    ASSERT_TRUE(tokens.has_value());
    ASSERT_EQ(expected, *tokens);
  }
}

TEST(IcuTextTokenizerTest, cjk_dictionary_words) {
  auto stream = IcuTextTokenizer::Make(IcuTextTokenizer::Options{
    .locale = icu::Locale::createFromName("en_US.UTF-8")});
  {
    const auto tokens = tests::AnalyzeTerms(*stream, "中文测试");
    ASSERT_TRUE(tokens.has_value());
    const std::vector<std::string> expected{"中文", "测试"};
    ASSERT_EQ(expected, *tokens);
  }
  {
    const auto tokens = tests::AnalyzeTerms(*stream, "日本語のテキスト");
    ASSERT_TRUE(tokens.has_value());
    const std::vector<std::string> expected{"日本語", "の", "テキスト"};
    ASSERT_EQ(expected, *tokens);
  }
  {
    const auto tokens = tests::AnalyzeTerms(*stream, "ภาษาไทยทดสอบ");
    ASSERT_TRUE(tokens.has_value());
    const std::vector<std::string> expected{"ภาษา", "ไทย", "ทดสอบ"};
    ASSERT_EQ(expected, *tokens);
  }
}

TEST(IcuTextTokenizerTest, ascii_fast_path_matches_icu_for_every_accept) {
  using Accept = IcuTextTokenizer::Options::Accept;
  for (const auto accept :
       {Accept::Any, Accept::Graphic, Accept::AlphaNumeric, Accept::Alpha}) {
    SCOPED_TRACE(testing::Message() << "accept=" << static_cast<int>(accept));
    IcuTextTokenizer::Options opts;
    opts.locale = icu::Locale::createFromName("en_US.UTF-8");
    opts.accept = accept;
    auto stream = IcuTextTokenizer::Make(opts);
    for (const std::string_view value :
         {"Test - ReTeSt", "a, b; c!", "x-y_z 12.5 (q)"}) {
      tests::AssertAsciiMatchesUnicode(*stream, value);
    }
  }
}

TEST(IcuTextTokenizerTest, locale_required) {
  ASSERT_THROW(IcuTextTokenizer::Make({}), sdb::SqlException);
  ASSERT_THROW(IcuTextTokenizer::Make(IcuTextTokenizer::Options{
                 .separate = IcuTextTokenizer::Options::Separate::Sentence}),
               sdb::SqlException);
  ASSERT_NE(nullptr, IcuTextTokenizer::Make(IcuTextTokenizer::Options{
                       .locale = icu::Locale::createFromName("de_DE")}));
}
