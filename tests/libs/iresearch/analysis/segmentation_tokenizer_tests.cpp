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

#include <vector>

#include "gtest/gtest.h"
#include "iresearch/analysis/batch/token_batch.hpp"
#include "iresearch/analysis/segmentation_tokenizer.hpp"
#include "iresearch/utils/utf8_character_tables.hpp"
#include "tests_config.hpp"
#include "token_sink_utils.hpp"

namespace {

struct AnalyzerToken {
  std::string_view value;
  size_t start;
  size_t end;
  uint32_t pos;
};

using AnalyzerTokens = std::vector<AnalyzerToken>;

}  // namespace

void AssertStream(irs::analysis::Tokenizer* pipe, std::string_view data,
                  const AnalyzerTokens& expected_tokens) {
  SCOPED_TRACE(data);
  std::vector<irs::bstring> terms;
  std::vector<uint32_t> starts;
  std::vector<uint32_t> ends;
  const auto collect = [&](irs::TokenBatch& batch,
                           std::span<const irs::DocRun> runs) {
    ASSERT_TRUE(batch.dense_pos);
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
  ASSERT_TRUE(pipe->Fill(data, sink.writer, sink.layout));
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
    "\xe4\xbb\x8a\xe5\xa4\xa9\xe4\xb8\x8b\xe5\x8d\x88",
    "caf\xc3\xa9 MEN\xc3\x9a  123abc",
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
          ASSERT_TRUE(batch.dense_pos);
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
        ASSERT_TRUE(fill_stream->Fill(v, sink.writer, sink.layout));
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
  const std::vector<std::string> values = {"alpha beta", "", big, "tail"};

  std::vector<std::vector<irs::bstring>> expected(values.size());
  for (size_t v = 0; v < values.size(); ++v) {
    for (const auto& t : PullSegmentation(*analyzer, values[v])) {
      expected[v].emplace_back(
        irs::ViewCast<irs::byte_type>(std::string_view{t.term}));
    }
  }

  std::vector<duckdb::string_t> vals;
  std::vector<irs::doc_id_t> docs;
  for (size_t i = 0; i < values.size(); ++i) {
    vals.emplace_back(values[i].data(),
                      static_cast<uint32_t>(values[i].size()));
    docs.push_back(static_cast<irs::doc_id_t>(i + 1));
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
      if (run.doc == irs::DocRun::kOpenValue) {
        ASSERT_EQ(runs.size() - 1, r);
        continue;
      }
      for (uint32_t j = 0; j < run.ntokens; ++j, ++tok) {
        const auto& t = batch.terms[tok];
        got[run.doc - 1].emplace_back(
          reinterpret_cast<const irs::byte_type*>(t.GetData()), t.GetSize());
      }
    }
    ASSERT_EQ(batch.count, tok);
  };
  tests::FnTokenSink sink{irs::TokenLayout::Terms, collect};
  analyzer->Fill(vals, docs, sink.writer, sink.layout);
  sink.writer.Finish();

  ASSERT_GT(flushes, 0);
  ASSERT_EQ(expected, got);
}

namespace {

std::vector<tests::AnalyzerToken> AnalyzeWith(const Options& opts,
                                              std::string_view value,
                                              bool force_unicode) {
  auto stream = SegmentationTokenizer::Make(Options{opts});
  auto* seg = dynamic_cast<SegmentationTokenizer*>(stream.get());
  EXPECT_NE(nullptr, seg);
  seg->ForceUnicodePath(force_unicode);
  auto tokens = tests::Analyze(*stream, value);
  EXPECT_TRUE(tokens.has_value());
  return std::move(*tokens);
}

void AssertAsciiMatchesUnicode(const Options& opts, std::string_view value) {
  const auto slow = AnalyzeWith(opts, value, true);
  const auto fast = AnalyzeWith(opts, value, false);
  ASSERT_EQ(slow.size(), fast.size());
  for (size_t i = 0; i < slow.size(); ++i) {
    SCOPED_TRACE(testing::Message() << "token=" << i);
    ASSERT_EQ(slow[i].term, fast[i].term);
    ASSERT_EQ(slow[i].pos, fast[i].pos);
    ASSERT_EQ(slow[i].offs_start, fast[i].offs_start);
    ASSERT_EQ(slow[i].offs_end, fast[i].offs_end);
  }
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
