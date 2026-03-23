////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
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

#include <vpack/builder.h>
#include <vpack/common.h>
#include <vpack/parser.h>

#include <vector>

#include "gtest/gtest.h"
#include "iresearch/analysis/pattern_tokenizer.hpp"
#include "tests_config.hpp"

namespace {

class PatternTokenizerTests : public ::testing::Test {};

void AssertTokenStreamContents(
  irs::analysis::Analyzer* stream,
  const std::vector<std::string_view>& expected_tokens,
  const std::vector<size_t>& expected_start_offsets,
  const std::vector<size_t>& expected_end_offsets,
  const std::vector<int>& expected_pos_increments = {}) {
  ASSERT_NE(nullptr, stream);
  ASSERT_EQ(expected_tokens.size(), expected_start_offsets.size());
  ASSERT_EQ(expected_tokens.size(), expected_end_offsets.size());
  if (!expected_pos_increments.empty()) {
    ASSERT_EQ(expected_tokens.size(), expected_pos_increments.size());
  }

  auto* term = irs::get<irs::TermAttr>(*stream);
  auto* offset = irs::get<irs::OffsAttr>(*stream);
  auto* inc = irs::get<irs::IncAttr>(*stream);

  size_t token_idx = 0;
  while (stream->next()) {
    ASSERT_LT(token_idx, expected_tokens.size());
    ASSERT_EQ(expected_tokens[token_idx], irs::ViewCast<char>(term->value));
    ASSERT_EQ(expected_start_offsets[token_idx], offset->start);
    ASSERT_EQ(expected_end_offsets[token_idx], offset->end);
    if (!expected_pos_increments.empty()) {
      ASSERT_EQ(expected_pos_increments[token_idx], inc->value);
    }
    ++token_idx;
  }

  ASSERT_EQ(token_idx, expected_tokens.size());
  ASSERT_FALSE(stream->next());
}

}  // namespace

TEST_F(PatternTokenizerTests, consts) {
  static_assert("pattern" ==
                irs::Type<irs::analysis::PatternTokenizer>::name());
}

TEST_F(PatternTokenizerTests, test_split_mode) {
  std::string_view data("foo,bar,baz");
  irs::analysis::PatternTokenizer stream(",", -1);
  ASSERT_EQ(irs::Type<irs::analysis::PatternTokenizer>::id(), stream.type());

  ASSERT_TRUE(stream.reset(data));

  AssertTokenStreamContents(&stream, {"foo", "bar", "baz"}, {0, 4, 8},
                            {3, 7, 11}, {1, 1, 1});
}

TEST_F(PatternTokenizerTests, test_split_whitespace) {
  std::string_view data("hello world test");
  irs::analysis::PatternTokenizer stream("\\s+", -1);

  ASSERT_TRUE(stream.reset(data));

  AssertTokenStreamContents(&stream, {"hello", "world", "test"}, {0, 6, 12},
                            {5, 11, 16}, {1, 1, 1});
}

TEST_F(PatternTokenizerTests, test_group_extraction_0) {
  std::string_view data("aaa 'bbb' 'ccc'");
  irs::analysis::PatternTokenizer stream("'([^']+)'", 0);

  ASSERT_TRUE(stream.reset(data));

  AssertTokenStreamContents(&stream, {"'bbb'", "'ccc'"}, {4, 10}, {9, 15},
                            {1, 1});
}

TEST_F(PatternTokenizerTests, test_group_extraction_0_match) {
  std::string_view data("'aaa' bbb 'ccc' 'ddd'");
  irs::analysis::PatternTokenizer stream("'(?:\\w*)'", 0);

  ASSERT_TRUE(stream.reset(data));

  AssertTokenStreamContents(&stream, {"'aaa'", "'ccc'", "'ddd'"}, {0, 10, 16},
                            {5, 15, 21}, {1, 1, 1});
}

TEST_F(PatternTokenizerTests, test_group_extraction_1) {
  std::string_view data("aaa 'bbb' 'ccc'");
  irs::analysis::PatternTokenizer stream("'([^']+)'", 1);

  ASSERT_TRUE(stream.reset(data));

  AssertTokenStreamContents(&stream, {"bbb", "ccc"}, {5, 11}, {8, 14}, {1, 1});
}

TEST_F(PatternTokenizerTests, test_digits_extraction) {
  std::string_view data("foo123bar456baz789");
  irs::analysis::PatternTokenizer stream("([0-9]+)", 1);

  ASSERT_TRUE(stream.reset(data));

  AssertTokenStreamContents(&stream, {"123", "456", "789"}, {3, 9, 15},
                            {6, 12, 18}, {1, 1, 1});
}

TEST_F(PatternTokenizerTests, test_empty_input) {
  std::string_view data("");
  irs::analysis::PatternTokenizer stream(",", -1);

  ASSERT_TRUE(stream.reset(data));

  AssertTokenStreamContents(&stream, {}, {}, {});
}

TEST_F(PatternTokenizerTests, test_no_match) {
  std::string_view data("hello world");
  irs::analysis::PatternTokenizer stream("[0-9]+", 0);

  ASSERT_TRUE(stream.reset(data));

  AssertTokenStreamContents(&stream, {}, {}, {});
}

TEST_F(PatternTokenizerTests, test_bad_regex) {
  // Invalid regex should make analyzer construction fail
  ASSERT_EQ(nullptr, irs::analysis::PatternTokenizer::make("(", -1));
  ASSERT_EQ(nullptr, irs::analysis::PatternTokenizer::make("(", 1));

  ASSERT_EQ(nullptr, irs::analysis::analyzers::Get(
                       "pattern", irs::Type<irs::text_format::Json>::get(),
                       R"({"pattern": "(", "group": -1})"));
}

TEST_F(PatternTokenizerTests, test_reset) {
  irs::analysis::PatternTokenizer stream(",", -1);

  std::string_view data1("a,b");
  ASSERT_TRUE(stream.reset(data1));

  AssertTokenStreamContents(&stream, {"a", "b"}, {0, 2}, {1, 3}, {1, 1});

  std::string_view data2("x,y,z");
  ASSERT_TRUE(stream.reset(data2));

  AssertTokenStreamContents(&stream, {"x", "y", "z"}, {0, 2, 4}, {1, 3, 5},
                            {1, 1, 1});
}

TEST_F(PatternTokenizerTests, test_reset_reuse_different_inputs) {
  {
    irs::analysis::PatternTokenizer stream(",", -1);
    ASSERT_TRUE(stream.reset("a,b"));
    AssertTokenStreamContents(&stream, {"a", "b"}, {0, 2}, {1, 3}, {1, 1});

    ASSERT_TRUE(stream.reset("c,d,e"));
    AssertTokenStreamContents(&stream, {"c", "d", "e"}, {0, 2, 4}, {1, 3, 5},
                              {1, 1, 1});
  }

  {
    irs::analysis::PatternTokenizer stream("'([^']+)'", 1);
    ASSERT_TRUE(stream.reset("a 'foo'"));
    AssertTokenStreamContents(&stream, {"foo"}, {3}, {6}, {1});

    ASSERT_TRUE(stream.reset("b 'bar' c 'baz'"));
    AssertTokenStreamContents(&stream, {"bar", "baz"}, {3, 11}, {6, 14},
                              {1, 1});
  }
}

TEST_F(PatternTokenizerTests, test_splitting_double_dash) {
  std::string_view data("aaa--bbb--ccc");
  irs::analysis::PatternTokenizer stream("--", -1);

  ASSERT_TRUE(stream.reset(data));

  AssertTokenStreamContents(&stream, {"aaa", "bbb", "ccc"}, {0, 5, 10},
                            {3, 8, 13}, {1, 1, 1});
}

TEST_F(PatternTokenizerTests, test_splitting_colon) {
  std::string_view data("aaa:bbb:ccc");
  irs::analysis::PatternTokenizer stream(":", -1);

  ASSERT_TRUE(stream.reset(data));

  AssertTokenStreamContents(&stream, {"aaa", "bbb", "ccc"}, {0, 4, 8},
                            {3, 7, 11}, {1, 1, 1});
}

TEST_F(PatternTokenizerTests, test_splitting_multi_space_and_tabs) {
  std::string_view data("aaa   bbb \t\tccc  ");
  irs::analysis::PatternTokenizer stream("\\s+", -1);

  ASSERT_TRUE(stream.reset(data));

  AssertTokenStreamContents(&stream, {"aaa", "bbb", "ccc"}, {0, 6, 12},
                            {3, 9, 15}, {1, 1, 1});
}

TEST_F(PatternTokenizerTests, test_splitting_single_char) {
  std::string_view data("boo:and:foo");
  irs::analysis::PatternTokenizer stream("o", -1);

  ASSERT_TRUE(stream.reset(data));

  AssertTokenStreamContents(&stream, {"b", ":and:f"}, {0, 3}, {1, 9}, {1, 1});
}

TEST_F(PatternTokenizerTests, test_group_zero_matches) {
  std::string_view data("boo:and:foo");
  irs::analysis::PatternTokenizer stream(":", 0);

  ASSERT_TRUE(stream.reset(data));

  AssertTokenStreamContents(&stream, {":", ":"}, {3, 7}, {4, 8}, {1, 1});
}

TEST_F(PatternTokenizerTests, test_offset_with_complex_pattern) {
  std::string_view data("hello world test");
  irs::analysis::PatternTokenizer stream("[,;/\\s]+", -1);

  ASSERT_TRUE(stream.reset(data));

  AssertTokenStreamContents(&stream, {"hello", "world", "test"}, {0, 6, 12},
                            {5, 11, 16}, {1, 1, 1});
}

TEST_F(PatternTokenizerTests, test_consecutive_delimiters) {
  std::string_view data("a,,b");
  irs::analysis::PatternTokenizer stream(",", -1);

  ASSERT_TRUE(stream.reset(data));

  AssertTokenStreamContents(&stream, {"a", "b"}, {0, 3}, {1, 4}, {1, 1});
}

TEST_F(PatternTokenizerTests, test_delimiter_at_boundaries) {
  std::string_view data(",hello,world,");
  irs::analysis::PatternTokenizer stream(",", -1);

  ASSERT_TRUE(stream.reset(data));

  AssertTokenStreamContents(&stream, {"hello", "world"}, {1, 7}, {6, 12},
                            {1, 1});
}

TEST_F(PatternTokenizerTests, test_utf8_split_comma_cyrillic) {
  // "аба" = 6 UTF-8 bytes, comma, "цаба" = 8 bytes → total 15.
  std::string_view data("аба,цаба");
  irs::analysis::PatternTokenizer stream(",", -1);

  ASSERT_TRUE(stream.reset(data));

  AssertTokenStreamContents(&stream, {"аба", "цаба"}, {0, 7}, {6, 15}, {1, 1});
}

TEST_F(PatternTokenizerTests, test_utf8_split_whitespace_mixed_ascii_cjk) {
  // "a" (1) + space + "汉字" (3+3 bytes) + space + "b" (1) = 10 bytes
  std::string_view data("a 汉字 b");
  irs::analysis::PatternTokenizer stream("\\s+", -1);

  ASSERT_TRUE(stream.reset(data));

  AssertTokenStreamContents(&stream, {"a", "汉字", "b"}, {0, 2, 9}, {1, 8, 10},
                            {1, 1, 1});
}

TEST_F(PatternTokenizerTests, test_utf8_group_capture_inside_quotes) {
  std::string_view data("x '汉字' z");
  irs::analysis::PatternTokenizer stream("'([^']+)'", 1);

  ASSERT_TRUE(stream.reset(data));

  AssertTokenStreamContents(&stream, {"汉字"}, {3}, {9}, {1});
}

TEST_F(PatternTokenizerTests, test_utf8_split_comma_4byte_emoji) {
  // U+1F642 = 0xF0 0x9F 0x98 0x8A
  std::string_view emoji = "\xF0\x9F\x98\x8A";
  const std::string data(std::string("a,") + emoji);
  irs::analysis::PatternTokenizer stream(",", -1);

  ASSERT_TRUE(stream.reset(data));

  AssertTokenStreamContents(&stream, {"a", emoji}, {0, 2}, {1, 6}, {1, 1});
}

TEST_F(PatternTokenizerTests, test_vpack_options) {
  // Valid: pattern only, default group -1 (split mode)
  {
    auto stream = irs::analysis::analyzers::Get(
      "pattern", irs::Type<irs::text_format::Json>::get(),
      R"({"pattern": ","})");
    ASSERT_NE(nullptr, stream);

    std::string_view data("a,b,c");
    ASSERT_TRUE(stream->reset(data));

    AssertTokenStreamContents(stream.get(), {"a", "b", "c"}, {0, 2, 4},
                              {1, 3, 5}, {1, 1, 1});
  }

  // Valid: pattern + group 1 (extract first capturing group)
  {
    auto stream = irs::analysis::analyzers::Get(
      "pattern", irs::Type<irs::text_format::Json>::get(),
      R"({"pattern": "'([^']+)'", "group": 1})");
    ASSERT_NE(nullptr, stream);

    std::string_view data("a 'foo' b 'bar'");
    ASSERT_TRUE(stream->reset(data));

    AssertTokenStreamContents(stream.get(), {"foo", "bar"}, {3, 11}, {6, 14},
                              {1, 1});
  }

  // Valid: group omitted -> default -1
  {
    auto stream = irs::analysis::analyzers::Get(
      "pattern", irs::Type<irs::text_format::Json>::get(),
      R"({"pattern": ":"})");
    ASSERT_NE(nullptr, stream);

    std::string_view data("a:b:c");
    ASSERT_TRUE(stream->reset(data));

    AssertTokenStreamContents(stream.get(), {"a", "b", "c"}, {0, 2, 4},
                              {1, 3, 5}, {1, 1, 1});
  }

  // Invalid: empty object -> missing pattern
  ASSERT_EQ(nullptr,
            irs::analysis::analyzers::Get(
              "pattern", irs::Type<irs::text_format::Json>::get(), "{}"));

  // Invalid: missing pattern key
  ASSERT_EQ(nullptr, irs::analysis::analyzers::Get(
                       "pattern", irs::Type<irs::text_format::Json>::get(),
                       R"({"group": 0})"));

  // Invalid: pattern not a string
  ASSERT_EQ(nullptr, irs::analysis::analyzers::Get(
                       "pattern", irs::Type<irs::text_format::Json>::get(),
                       R"({"pattern": 123})"));

  // Invalid: empty pattern string
  ASSERT_EQ(nullptr, irs::analysis::analyzers::Get(
                       "pattern", irs::Type<irs::text_format::Json>::get(),
                       R"({"pattern": ""})"));

  // Invalid: not an object
  ASSERT_EQ(nullptr,
            irs::analysis::analyzers::Get(
              "pattern", irs::Type<irs::text_format::Json>::get(), "[]"));

  // Invalid: not an object
  ASSERT_EQ(nullptr,
            irs::analysis::analyzers::Get(
              "pattern", irs::Type<irs::text_format::Json>::get(), R"("x")"));

  // Invalid: group wrong type
  ASSERT_EQ(nullptr, irs::analysis::analyzers::Get(
                       "pattern", irs::Type<irs::text_format::Json>::get(),
                       R"({"pattern": ",", "group": "ignored"})"));

  // Invalid: group is non-integer number
  ASSERT_EQ(nullptr, irs::analysis::analyzers::Get(
                       "pattern", irs::Type<irs::text_format::Json>::get(),
                       R"({"pattern": ",", "group": 1.5})"));

  // Invalid: group out of int range
  ASSERT_EQ(nullptr, irs::analysis::analyzers::Get(
                       "pattern", irs::Type<irs::text_format::Json>::get(),
                       R"({"pattern": ",", "group": 9223372036854775807})"));

  // Invalid: unknown fields should be ignored
  {
    auto stream = irs::analysis::analyzers::Get(
      "pattern", irs::Type<irs::text_format::Json>::get(),
      R"({"pattern": ",", "unknown_field": 1, "group": -1})");
    ASSERT_NE(nullptr, stream);

    std::string_view data("a,b");
    ASSERT_TRUE(stream->reset(data));

    AssertTokenStreamContents(stream.get(), {"a", "b"}, {0, 2}, {1, 3}, {1, 1});
  }

  // Valid: config produces parseable definition
  {
    std::string definition;
    ASSERT_TRUE(irs::analysis::analyzers::Normalize(
      definition, "pattern", irs::Type<irs::text_format::Json>::get(),
      R"({"pattern": "\\s+", "group": -1})"));
    ASSERT_FALSE(definition.empty());

    auto stream = irs::analysis::analyzers::Get(
      "pattern", irs::Type<irs::text_format::Json>::get(), definition);
    ASSERT_NE(nullptr, stream);

    std::string_view data("a b c");
    ASSERT_TRUE(stream->reset(data));

    AssertTokenStreamContents(stream.get(), {"a", "b", "c"}, {0, 2, 4},
                              {1, 3, 5}, {1, 1, 1});
  }
}
