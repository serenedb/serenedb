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

#include <vpack/common.h>
#include <vpack/parser.h>

#include <vector>

#include "gtest/gtest.h"
#include "iresearch/analysis/segmentation_tokenizer.hpp"
#include "iresearch/utils/utf8_character_tables.hpp"
#include "tests_config.hpp"

namespace {

struct AnalyzerToken {
  std::string_view value;
  size_t start;
  size_t end;
  uint32_t pos;
};

using AnalyzerTokens = std::vector<AnalyzerToken>;

}  // namespace

void AssertStream(irs::analysis::Analyzer* pipe, std::string_view data,
                  const AnalyzerTokens& expected_tokens) {
  SCOPED_TRACE(data);
  auto* offset = irs::get<irs::OffsAttr>(*pipe);
  ASSERT_TRUE(offset);
  auto* term = irs::get<irs::TermAttr>(*pipe);
  ASSERT_TRUE(term);
  auto* inc = irs::get<irs::IncAttr>(*pipe);
  ASSERT_TRUE(inc);
  ASSERT_TRUE(pipe->reset(data));
  uint32_t pos{std::numeric_limits<uint32_t>::max()};
  auto expected_token = expected_tokens.begin();
  while (pipe->next()) {
    auto term_value =
      std::string(irs::ViewCast<char>(term->value).data(), term->value.size());
    SCOPED_TRACE(testing::Message("Term:<") << term_value << ">");
    SCOPED_TRACE(testing::Message("Expected term:<")
                 << expected_token->value << ">");
    pos += inc->value;
    ASSERT_NE(expected_token, expected_tokens.end());
    ASSERT_EQ(irs::ViewCast<irs::byte_type>(expected_token->value),
              term->value);
    ASSERT_EQ(expected_token->start, offset->start);
    ASSERT_EQ(expected_token->end, offset->end);
    ASSERT_EQ(expected_token->pos, pos);
    ++expected_token;
  }
  ASSERT_EQ(expected_token, expected_tokens.end());
  ASSERT_FALSE(pipe->next());
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
    .use_ascii_optimization = GetParam(),
  };
  auto stream = SegmentationTokenizer::make(std::move(opt));
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
  Options opt{.use_ascii_optimization = GetParam()};  // Lower is default
  auto stream = SegmentationTokenizer::make(std::move(opt));
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
    .use_ascii_optimization = GetParam(),
  };
  auto stream = SegmentationTokenizer::make(std::move(opt));

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
    .use_ascii_optimization = GetParam(),
  };
  auto stream = SegmentationTokenizer::make(std::move(opt));
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
    .use_ascii_optimization = GetParam(),
  };
  auto stream = SegmentationTokenizer::make(std::move(opt));
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
  Options opt{.use_ascii_optimization = GetParam()};
  auto stream = SegmentationTokenizer::make(std::move(opt));

  auto test_func = [](const std::string_view& data,
                      irs::analysis::Analyzer* p_stream) {
    ASSERT_TRUE(p_stream->reset(data));
    auto* p_offset = irs::get<irs::OffsAttr>(*p_stream);
    ASSERT_NE(nullptr, p_offset);
    auto* p_payload = irs::get<irs::PayAttr>(*p_stream);
    ASSERT_EQ(nullptr, p_payload);
    auto* p_value = irs::get<irs::TermAttr>(*p_stream);
    ASSERT_NE(nullptr, p_value);

    auto assert_offset = [p_offset](uint32_t start, uint32_t end) {
      ASSERT_EQ(start, p_offset->start);
      ASSERT_EQ(end, p_offset->end);
    };

    ASSERT_TRUE(p_stream->next());
    ASSERT_TRUE(u8"\u4ECA" == irs::ViewCast<char8_t>(p_value->value));
    assert_offset(0, 3);
    ASSERT_TRUE(p_stream->next());
    ASSERT_TRUE(u8"\u5929" == irs::ViewCast<char8_t>(p_value->value));
    assert_offset(3, 6);
    ASSERT_TRUE(p_stream->next());
    ASSERT_TRUE(u8"\u4E0B" == irs::ViewCast<char8_t>(p_value->value));
    assert_offset(6, 9);
    ASSERT_TRUE(p_stream->next());
    ASSERT_TRUE(u8"\u5348" == irs::ViewCast<char8_t>(p_value->value));
    assert_offset(9, 12);
    ASSERT_TRUE(p_stream->next());
    ASSERT_TRUE(u8"\u7684" == irs::ViewCast<char8_t>(p_value->value));
    assert_offset(12, 15);
    ASSERT_TRUE(p_stream->next());
    ASSERT_TRUE(u8"\u592A" == irs::ViewCast<char8_t>(p_value->value));
    assert_offset(15, 18);
    ASSERT_TRUE(p_stream->next());
    ASSERT_TRUE(u8"\u9633" == irs::ViewCast<char8_t>(p_value->value));
    assert_offset(18, 21);
    ASSERT_TRUE(p_stream->next());
    ASSERT_TRUE(u8"\u5F88" == irs::ViewCast<char8_t>(p_value->value));
    assert_offset(21, 24);
    ASSERT_TRUE(p_stream->next());
    ASSERT_TRUE(u8"\u6E29" == irs::ViewCast<char8_t>(p_value->value));
    assert_offset(24, 27);
    ASSERT_TRUE(p_stream->next());
    ASSERT_TRUE(u8"\u6696" == irs::ViewCast<char8_t>(p_value->value));
    assert_offset(27, 30);
    ASSERT_FALSE(p_stream->next());
  };

  test_func(irs::ViewCast<char>(kData), stream.get());
}

TEST(SegmentationTokenizerTest, make_empty_object) {
  auto stream = irs::analysis::analyzers::Get(
    "segmentation", irs::Type<irs::text_format::Json>::get(), "{}");
  ASSERT_TRUE(stream);
  const AnalyzerTokens expected{{"test", 0, 4, 0}, {"retest", 7, 13, 1}};
  std::string data = "Test - ReTeSt";
  AssertStream(stream.get(), data, expected);
}

TEST(SegmentationTokenizerTest, make_lowercase) {
  auto stream = irs::analysis::analyzers::Get(
    "segmentation", irs::Type<irs::text_format::Json>::get(),
    "{\"case\":\"lower\"}");
  ASSERT_TRUE(stream);
  const AnalyzerTokens expected{{"test", 0, 4, 0}, {"retest", 7, 13, 1}};
  std::string data = "Test - ReTeSt";
  AssertStream(stream.get(), data, expected);
}

TEST(SegmentationTokenizerTest, make_nonecase) {
  auto stream = irs::analysis::analyzers::Get(
    "segmentation", irs::Type<irs::text_format::Json>::get(),
    "{\"case\":\"none\"}");
  ASSERT_TRUE(stream);
  const AnalyzerTokens expected{{"Test", 0, 4, 0}, {"ReTeSt", 7, 13, 1}};
  std::string data = "Test - ReTeSt";
  AssertStream(stream.get(), data, expected);
}

TEST(SegmentationTokenizerTest, make_uppercase) {
  auto stream = irs::analysis::analyzers::Get(
    "segmentation", irs::Type<irs::text_format::Json>::get(),
    "{\"case\":\"upper\"}");
  ASSERT_TRUE(stream);
  const AnalyzerTokens expected{{"TEST", 0, 4, 0}, {"RETEST", 7, 13, 1}};
  std::string data = "Test - ReTeSt";
  AssertStream(stream.get(), data, expected);
}

TEST(SegmentationTokenizerTest, make_invalidcase) {
  auto stream = irs::analysis::analyzers::Get(
    "segmentation", irs::Type<irs::text_format::Json>::get(),
    "{\"case\":\"invalid\"}");
  ASSERT_FALSE(stream);
}

TEST(SegmentationTokenizerTest, make_numbercase) {
  auto stream = irs::analysis::analyzers::Get(
    "segmentation", irs::Type<irs::text_format::Json>::get(), "{\"case\":2}");
  ASSERT_FALSE(stream);
}

TEST(SegmentationTokenizerTest, make_uppercase_alphabreak) {
  auto stream = irs::analysis::analyzers::Get(
    "segmentation", irs::Type<irs::text_format::Json>::get(),
    "{\"case\":\"upper\", \"break\":\"alpha\"}");
  ASSERT_TRUE(stream);
  const AnalyzerTokens expected{{"TEST", 0, 4, 0}, {"RETEST", 7, 13, 1}};
  std::string data = "Test - ReTeSt";
  AssertStream(stream.get(), data, expected);
}

TEST(SegmentationTokenizerTest, make_uppercase_all_break) {
  auto stream = irs::analysis::analyzers::Get(
    "segmentation", irs::Type<irs::text_format::Json>::get(),
    "{\"case\":\"upper\", \"break\":\"all\"}");
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
  auto stream = irs::analysis::analyzers::Get(
    "segmentation", irs::Type<irs::text_format::Json>::get(),
    "{\"case\":\"upper\", \"break\":\"graphic\"}");
  ASSERT_TRUE(stream);
  const AnalyzerTokens expected{
    {"TEST", 0, 4, 0}, {"-", 5, 6, 1}, {"RETEST", 7, 13, 2}};
  std::string data = "Test - ReTeSt";
  AssertStream(stream.get(), data, expected);
}

TEST(SegmentationTokenizerTest, make_uppercase_invalid_break) {
  auto stream = irs::analysis::analyzers::Get(
    "segmentation", irs::Type<irs::text_format::Json>::get(),
    "{\"case\":\"upper\", \"break\":\"_INVALID_\"}");
  ASSERT_FALSE(stream);
}

TEST(SegmentationTokenizerTest, make_uppercase_invalid_number_break) {
  auto stream = irs::analysis::analyzers::Get(
    "segmentation", irs::Type<irs::text_format::Json>::get(),
    "{\"case\":\"upper\", \"break\":1}");
  ASSERT_FALSE(stream);
}

TEST(SegmentationTokenizerTest, make_invalid_json) {
  // load jSON invalid
  {
    ASSERT_EQ(nullptr,
              irs::analysis::analyzers::Get(
                "segmentation", irs::Type<irs::text_format::Json>::get(),
                std::string_view{}));
    ASSERT_EQ(nullptr,
              irs::analysis::analyzers::Get(
                "segmentation", irs::Type<irs::text_format::Json>::get(), "1"));
    ASSERT_EQ(nullptr, irs::analysis::analyzers::Get(
                         "segmentation",
                         irs::Type<irs::text_format::Json>::get(), "\"abc\""));
    ASSERT_EQ(nullptr,
              irs::analysis::analyzers::Get(
                "segmentation", irs::Type<irs::text_format::Json>::get(),
                "{\"case\":1}"));
    ASSERT_EQ(nullptr,
              irs::analysis::analyzers::Get(
                "segmentation", irs::Type<irs::text_format::Json>::get(),
                "{\"break\":1}"));
    ASSERT_EQ(nullptr,
              irs::analysis::analyzers::Get(
                "segmentation", irs::Type<irs::text_format::Json>::get(),
                "{\"case\":1, \"break\":\"all\"}"));
    ASSERT_EQ(nullptr,
              irs::analysis::analyzers::Get(
                "segmentation", irs::Type<irs::text_format::Json>::get(),
                "{\"case\":\"none\", \"break\":1}"));
  }
}

TEST(SegmentationTokenizerTest, normalize_empty_object) {
  std::string actual;
  ASSERT_TRUE(irs::analysis::analyzers::Normalize(
    actual, "segmentation", irs::Type<irs::text_format::Json>::get(), "{}"));
  ASSERT_EQ(actual, "{\n  \"break\" : \"alpha\",\n  \"case\" : \"lower\"\n}");
}

TEST(SegmentationTokenizerTest, normalize_all_none_values) {
  std::string actual;
  ASSERT_TRUE(irs::analysis::analyzers::Normalize(
    actual, "segmentation", irs::Type<irs::text_format::Json>::get(),
    "{\"break\":\"all\", \"case\":\"none\"}"));
  ASSERT_EQ(actual, "{\n  \"break\" : \"all\",\n  \"case\" : \"none\"\n}");
}

TEST(SegmentationTokenizerTest, normalize_graph_upper_values) {
  {
    std::string actual;
    ASSERT_TRUE(irs::analysis::analyzers::Normalize(
      actual, "segmentation", irs::Type<irs::text_format::Json>::get(),
      "{\"break\":\"graphic\", \"case\":\"upper\"}"));
    ASSERT_EQ(actual,
              "{\n  \"break\" : \"graphic\",\n  \"case\" : \"upper\"\n}");
  }

  // test vpack
  {
    std::string config = "{\"break\":\"graphic\", \"case\":\"upper\"}";
    auto in_vpack = vpack::Parser::fromJson(config.c_str(), config.size());
    std::string in_str;
    in_str.assign(in_vpack->slice().startAs<char>(),
                  in_vpack->slice().byteSize());
    std::string out_str;
    ASSERT_TRUE(irs::analysis::analyzers::Normalize(
      out_str, "segmentation", irs::Type<irs::text_format::VPack>::get(),
      in_str));
    vpack::Slice out_slice(reinterpret_cast<const uint8_t*>(out_str.c_str()));
    ASSERT_EQ("{\n  \"break\" : \"graphic\",\n  \"case\" : \"upper\"\n}",
              out_slice.toString());
  }
}

TEST(SegmentationTokenizerTest, normalize_invalid) {
  std::string actual;
  ASSERT_FALSE(irs::analysis::analyzers::Normalize(
    actual, "segmentation", irs::Type<irs::text_format::Json>::get(),
    std::string_view{}));
  ASSERT_FALSE(irs::analysis::analyzers::Normalize(
    actual, "segmentation", irs::Type<irs::text_format::Json>::get(), "1"));
  ASSERT_FALSE(irs::analysis::analyzers::Normalize(
    actual, "segmentation", irs::Type<irs::text_format::Json>::get(),
    "\"abc\""));
  ASSERT_FALSE(irs::analysis::analyzers::Normalize(
    actual, "segmentation", irs::Type<irs::text_format::Json>::get(),
    "{\"case\":1}"));
  ASSERT_FALSE(irs::analysis::analyzers::Normalize(
    actual, "segmentation", irs::Type<irs::text_format::Json>::get(),
    "{\"break\":1}"));
  ASSERT_FALSE(irs::analysis::analyzers::Normalize(
    actual, "segmentation", irs::Type<irs::text_format::Json>::get(),
    "{\"case\":1, \"break\":\"all\"}"));
  ASSERT_FALSE(irs::analysis::analyzers::Normalize(
    actual, "segmentation", irs::Type<irs::text_format::Json>::get(),
    "{\"case\":\"none\", \"break\":1}"));
}

INSTANTIATE_TEST_SUITE_P(SegmentationWithAsciiOptimization,
                         SegmentationTokenizerTest,
                         testing::Values(false, true));
