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

#include <vpack/common.h>
#include <vpack/parser.h>

#include <iresearch/analysis/analyzers.hpp>
#include <iresearch/analysis/path_hierarchy_tokenizer.hpp>

#include "gtest/gtest.h"

namespace irs::analysis {

class PathHierarchyTokenizerTests : public ::testing::Test {
 public:
  static void SetUpTestCase() { PathHierarchyTokenizer::init(); }
};

void AssertTokenStreamContents(
  analysis::Analyzer* stream,
  const std::vector<std::string_view>& expected_tokens,
  const std::vector<size_t>& expected_start_offsets,
  const std::vector<size_t>& expected_end_offsets,
  const std::vector<int>& expected_pos_increments = {}) {
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

TEST_F(PathHierarchyTokenizerTests, consts) {
  static_assert("path_hierarchy" == irs::Type<PathHierarchyTokenizer>::name());
}

using OptionsT = PathHierarchyTokenizer::OptionsT;

TEST_F(PathHierarchyTokenizerTests, test_forward_mode) {
  OptionsT options;
  options.reverse = false;

  std::string_view data = "/a/b/c";
  auto stream = PathHierarchyTokenizer::make(std::move(options));
  ASSERT_NE(nullptr, stream);
  ASSERT_EQ(irs::Type<PathHierarchyTokenizer>::id(), stream->type());
  ASSERT_TRUE(stream->reset(data));

  AssertTokenStreamContents(stream.get(), {"/a", "/a/b", "/a/b/c"}, {0, 0, 0},
                            {2, 4, 6}, {1, 1, 1});
}

TEST_F(PathHierarchyTokenizerTests, test_single_element_path) {
  OptionsT options;
  options.delimiter = '/';
  options.replacement = '/';
  options.reverse = false;

  std::string_view data = "a";
  auto stream = PathHierarchyTokenizer::make(std::move(options));
  ASSERT_NE(nullptr, stream);
  ASSERT_TRUE(stream->reset(data));

  AssertTokenStreamContents(stream.get(), {"a"}, {0}, {1}, {1});
}

TEST_F(PathHierarchyTokenizerTests, test_custom_delimiter) {
  OptionsT options;
  options.delimiter = '-';
  options.replacement = '-';
  options.reverse = false;

  std::string_view data = "a-b-c";
  auto stream = PathHierarchyTokenizer::make(std::move(options));
  ASSERT_NE(nullptr, stream);
  ASSERT_TRUE(stream->reset(data));

  AssertTokenStreamContents(stream.get(), {"a", "a-b", "a-b-c"}, {0, 0, 0},
                            {1, 3, 5}, {1, 1, 1});
}

TEST_F(PathHierarchyTokenizerTests, test_reset_multiple_times) {
  OptionsT options;
  options.delimiter = '/';
  auto stream = PathHierarchyTokenizer::make(std::move(options));
  ASSERT_NE(nullptr, stream);

  ASSERT_TRUE(stream->reset("/a/b"));
  AssertTokenStreamContents(stream.get(), {"/a", "/a/b"}, {0, 0}, {2, 4},
                            {1, 1});

  ASSERT_TRUE(stream->reset("/xx/y1/z"));
  AssertTokenStreamContents(stream.get(), {"/xx", "/xx/y1", "/xx/y1/z"},
                            {0, 0, 0}, {3, 6, 8}, {1, 1, 1});
}

TEST_F(PathHierarchyTokenizerTests, test_empty_path) {
  OptionsT options;
  auto stream = PathHierarchyTokenizer::make(std::move(options));
  ASSERT_NE(nullptr, stream);
  ASSERT_TRUE(stream->reset(""));

  AssertTokenStreamContents(stream.get(), {}, {}, {}, {});
}

TEST_F(PathHierarchyTokenizerTests, test_path_with_trailing_delimiter) {
  OptionsT options;
  auto stream = PathHierarchyTokenizer::make(std::move(options));
  ASSERT_NE(nullptr, stream);

  ASSERT_TRUE(stream->reset("/a/b/"));
  AssertTokenStreamContents(stream.get(), {"/a", "/a/b", "/a/b/"}, {0, 0, 0},
                            {2, 4, 5}, {1, 1, 1});
}

TEST_F(PathHierarchyTokenizerTests, test_skip_exceeds_tokens) {
  OptionsT options;
  options.delimiter = '/';
  options.skip = 5;
  options.reverse = false;

  std::string_view data = "/a/b";
  auto stream = PathHierarchyTokenizer::make(std::move(options));
  ASSERT_NE(nullptr, stream);
  ASSERT_TRUE(stream->reset(data));

  AssertTokenStreamContents(stream.get(), {}, {}, {}, {});
}

TEST_F(PathHierarchyTokenizerTests, test_standart_skip) {
  OptionsT options;
  options.delimiter = '/';
  options.skip = 2;
  options.reverse = false;

  std::string_view data = "/a/b/c/d/e";
  auto stream = PathHierarchyTokenizer::make(std::move(options));
  ASSERT_NE(nullptr, stream);

  ASSERT_TRUE(stream->reset(data));
  AssertTokenStreamContents(stream.get(), {"/c", "/c/d", "/c/d/e"}, {4, 4, 4},
                            {6, 8, 10}, {1, 1, 1});
}

TEST_F(PathHierarchyTokenizerTests, test_empty_delimiter) {
  OptionsT options;
  options.delimiter = '\0';
  options.reverse = false;

  std::string_view data = "abc";
  auto stream = PathHierarchyTokenizer::make(std::move(options));
  ASSERT_NE(nullptr, stream);

  ASSERT_TRUE(stream->reset(data));
  AssertTokenStreamContents(stream.get(), {"abc"}, {0}, {3}, {1});
}

TEST_F(PathHierarchyTokenizerTests, test_reset_without_next) {
  OptionsT options;
  options.delimiter = '/';

  auto stream = PathHierarchyTokenizer::make(std::move(options));
  ASSERT_NE(nullptr, stream);

  ASSERT_TRUE(stream->reset("/a/b"));
  ASSERT_TRUE(stream->reset("/x/y"));

  AssertTokenStreamContents(stream.get(), {"/x", "/x/y"}, {0, 0}, {2, 4},
                            {1, 1});
}

TEST_F(PathHierarchyTokenizerTests, test_utf8_characters) {
  OptionsT options;
  options.delimiter = '/';
  options.reverse = false;

  std::string_view data = "/café/café/café";
  auto stream = PathHierarchyTokenizer::make(std::move(options));
  ASSERT_NE(nullptr, stream);

  ASSERT_TRUE(stream->reset(data));
  AssertTokenStreamContents(stream.get(),
                            {"/café", "/café/café", "/café/café/café"},
                            {0, 0, 0}, {6, 12, 18}, {1, 1, 1});
}

TEST_F(PathHierarchyTokenizerTests, test_forward_with_different_replacement) {
  OptionsT options;
  options.delimiter = '/';
  options.replacement = '_';
  options.reverse = false;

  std::string_view data = "/a/b/c";
  auto stream = PathHierarchyTokenizer::make(std::move(options));
  ASSERT_NE(nullptr, stream);

  ASSERT_TRUE(stream->reset(data));
  AssertTokenStreamContents(stream.get(), {"_a", "_a_b", "_a_b_c"}, {0, 0, 0},
                            {2, 4, 6}, {1, 1, 1});
}

TEST_F(PathHierarchyTokenizerTests, test_consecutive_delimiters) {
  OptionsT options;
  options.delimiter = '/';
  options.reverse = false;

  std::string_view data = "//a///b//";
  auto stream = PathHierarchyTokenizer::make(std::move(options));
  ASSERT_NE(nullptr, stream);

  ASSERT_TRUE(stream->reset(data));
  AssertTokenStreamContents(
    stream.get(),
    {"/", "//a", "//a/", "//a//", "//a///b", "//a///b/", "//a///b//"},
    {0, 0, 0, 0, 0, 0, 0}, {1, 3, 4, 5, 7, 8, 9}, {1, 1, 1, 1, 1, 1, 1});
}

TEST_F(PathHierarchyTokenizerTests, test_basic_without_leading_delimiter) {
  OptionsT options;
  options.delimiter = '/';
  options.reverse = false;

  std::string_view data = "a/b/c";
  auto stream = PathHierarchyTokenizer::make(std::move(options));
  ASSERT_NE(nullptr, stream);

  ASSERT_TRUE(stream->reset(data));
  AssertTokenStreamContents(stream.get(), {"a", "a/b", "a/b/c"}, {0, 0, 0},
                            {1, 3, 5}, {1, 1, 1});
}

TEST_F(PathHierarchyTokenizerTests, test_only_delimiter) {
  OptionsT options;
  options.delimiter = '/';
  options.reverse = false;

  std::string_view data = "/";
  auto stream = PathHierarchyTokenizer::make(std::move(options));
  ASSERT_NE(nullptr, stream);

  ASSERT_TRUE(stream->reset(data));
  AssertTokenStreamContents(stream.get(), {"/"}, {0}, {1}, {1});
}

TEST_F(PathHierarchyTokenizerTests, test_only_delimiters) {
  OptionsT options;
  options.delimiter = '/';
  options.reverse = false;

  std::string_view data = "//";
  auto stream = PathHierarchyTokenizer::make(std::move(options));
  ASSERT_NE(nullptr, stream);

  ASSERT_TRUE(stream->reset(data));
  AssertTokenStreamContents(stream.get(), {"/", "//"}, {0, 0}, {1, 2}, {1, 1});
}

TEST_F(PathHierarchyTokenizerTests, test_forward_basic_skip) {
  OptionsT options;
  options.delimiter = '/';
  options.skip = 1;
  options.reverse = false;

  std::string_view data = "/a/b/c";
  auto stream = PathHierarchyTokenizer::make(std::move(options));
  ASSERT_NE(nullptr, stream);
  ASSERT_TRUE(stream->reset(data));

  AssertTokenStreamContents(stream.get(), {"/b", "/b/c"}, {2, 2}, {4, 6},
                            {1, 1});
}

TEST_F(PathHierarchyTokenizerTests, test_forward_end_of_delimiter_skip) {
  OptionsT options;
  options.delimiter = '/';
  options.skip = 1;
  options.reverse = false;

  std::string_view data = "/a/b/c/";
  auto stream = PathHierarchyTokenizer::make(std::move(options));
  ASSERT_NE(nullptr, stream);
  ASSERT_TRUE(stream->reset(data));

  AssertTokenStreamContents(stream.get(), {"/b", "/b/c", "/b/c/"}, {2, 2, 2},
                            {4, 6, 7}, {1, 1, 1});
}

TEST_F(PathHierarchyTokenizerTests, test_forward_start_of_char_skip) {
  OptionsT options;
  options.delimiter = '/';
  options.skip = 1;
  options.reverse = false;

  std::string_view data = "a/b/c";
  auto stream = PathHierarchyTokenizer::make(std::move(options));
  ASSERT_NE(nullptr, stream);
  ASSERT_TRUE(stream->reset(data));

  AssertTokenStreamContents(stream.get(), {"/b", "/b/c"}, {1, 1}, {3, 5},
                            {1, 1});
}

TEST_F(PathHierarchyTokenizerTests,
       test_forward_start_of_char_end_of_delimiter_skip) {
  OptionsT options;
  options.delimiter = '/';
  options.skip = 1;
  options.reverse = false;

  std::string_view data = "a/b/c/";
  auto stream = PathHierarchyTokenizer::make(std::move(options));
  ASSERT_NE(nullptr, stream);
  ASSERT_TRUE(stream->reset(data));

  AssertTokenStreamContents(stream.get(), {"/b", "/b/c", "/b/c/"}, {1, 1, 1},
                            {3, 5, 6}, {1, 1, 1});
}

TEST_F(PathHierarchyTokenizerTests, test_forward_only_delimiter_skip) {
  OptionsT options;
  options.delimiter = '/';
  options.skip = 1;
  options.reverse = false;

  std::string_view data = "/";
  auto stream = PathHierarchyTokenizer::make(std::move(options));
  ASSERT_NE(nullptr, stream);
  ASSERT_TRUE(stream->reset(data));

  AssertTokenStreamContents(stream.get(), {}, {}, {}, {});
}

TEST_F(PathHierarchyTokenizerTests, test_forward_only_delimiters_skip) {
  OptionsT options;
  options.delimiter = '/';
  options.skip = 1;
  options.reverse = false;

  std::string_view data = "//";
  auto stream = PathHierarchyTokenizer::make(std::move(options));
  ASSERT_NE(nullptr, stream);
  ASSERT_TRUE(stream->reset(data));

  AssertTokenStreamContents(stream.get(), {"/"}, {1}, {2}, {1});
}

TEST_F(PathHierarchyTokenizerTests, test_windows_path) {
  OptionsT options;
  options.delimiter = '\\';
  options.replacement = '\\';
  options.reverse = false;

  std::string_view data = "c:\\a\\b\\c";
  auto stream = PathHierarchyTokenizer::make(std::move(options));
  ASSERT_NE(nullptr, stream);
  ASSERT_TRUE(stream->reset(data));

  AssertTokenStreamContents(stream.get(),
                            {"c:", "c:\\a", "c:\\a\\b", "c:\\a\\b\\c"},
                            {0, 0, 0, 0}, {2, 4, 6, 8}, {1, 1, 1, 1});
}

TEST_F(PathHierarchyTokenizerTests, test_reverse_mode) {
  OptionsT options;
  options.delimiter = '.';
  options.replacement = '-';
  options.reverse = true;

  std::string_view data("www.example.com");
  auto stream = PathHierarchyTokenizer::make(std::move(options));
  ASSERT_NE(nullptr, stream);
  ASSERT_TRUE(stream->reset(data));

  AssertTokenStreamContents(stream.get(),
                            {"www-example-com", "example-com", "com"},
                            {0, 4, 12}, {15, 15, 15}, {1, 1, 1});
}

TEST_F(PathHierarchyTokenizerTests, test_reverse_domain_skip) {
  OptionsT options;
  options.delimiter = '.';
  options.replacement = '-';
  options.skip = 2;
  options.reverse = true;

  std::string_view data = "a.b.c.d.e";
  auto stream = PathHierarchyTokenizer::make(std::move(options));
  ASSERT_NE(nullptr, stream);
  ASSERT_TRUE(stream->reset(data));

  AssertTokenStreamContents(stream.get(), {"a-b-c-", "b-c-", "c-"}, {0, 2, 4},
                            {6, 6, 6}, {1, 1, 1});
}

TEST_F(PathHierarchyTokenizerTests, test_reverse_basic) {
  OptionsT options;
  options.delimiter = '/';
  options.replacement = '/';
  options.reverse = true;
  options.skip = 0;

  std::string_view data = "/a/b/c";
  auto stream = PathHierarchyTokenizer::make(std::move(options));
  ASSERT_NE(nullptr, stream);
  ASSERT_TRUE(stream->reset(data));

  AssertTokenStreamContents(stream.get(), {"/a/b/c", "a/b/c", "b/c", "c"},
                            {0, 1, 3, 5}, {6, 6, 6, 6}, {1, 1, 1, 1});
}

TEST_F(PathHierarchyTokenizerTests, test_reverse_end_of_delimiter) {
  OptionsT options;
  options.delimiter = '/';
  options.replacement = '/';
  options.reverse = true;
  options.skip = 0;

  std::string_view data = "/a/b/c/";
  auto stream = PathHierarchyTokenizer::make(std::move(options));
  ASSERT_NE(nullptr, stream);
  ASSERT_TRUE(stream->reset(data));

  AssertTokenStreamContents(stream.get(), {"/a/b/c/", "a/b/c/", "b/c/", "c/"},
                            {0, 1, 3, 5}, {7, 7, 7, 7}, {1, 1, 1, 1});
}

TEST_F(PathHierarchyTokenizerTests,
       test_reverse_start_of_char_end_of_delimiter) {
  OptionsT options;
  options.delimiter = '/';
  options.replacement = '/';
  options.reverse = true;
  options.skip = 0;

  std::string_view data = "a/b/c/";
  auto stream = PathHierarchyTokenizer::make(std::move(options));
  ASSERT_NE(nullptr, stream);
  ASSERT_TRUE(stream->reset(data));

  AssertTokenStreamContents(stream.get(), {"a/b/c/", "b/c/", "c/"}, {0, 2, 4},
                            {6, 6, 6}, {1, 1, 1});
}

TEST_F(PathHierarchyTokenizerTests, test_reverse_only_delimiter) {
  OptionsT options;
  options.delimiter = '/';
  options.replacement = '/';
  options.reverse = true;
  options.skip = 0;

  std::string_view data = "/";
  auto stream = PathHierarchyTokenizer::make(std::move(options));
  ASSERT_NE(nullptr, stream);
  ASSERT_TRUE(stream->reset(data));

  AssertTokenStreamContents(stream.get(), {"/"}, {0}, {1}, {1});
}

TEST_F(PathHierarchyTokenizerTests, test_reverse_only_delimiters) {
  OptionsT options;
  options.delimiter = '/';
  options.replacement = '/';
  options.reverse = true;
  options.skip = 0;

  std::string_view data = "//";
  auto stream = PathHierarchyTokenizer::make(std::move(options));
  ASSERT_NE(nullptr, stream);
  ASSERT_TRUE(stream->reset(data));

  AssertTokenStreamContents(stream.get(), {"//", "/"}, {0, 1}, {2, 2}, {1, 1});
}

TEST_F(PathHierarchyTokenizerTests, test_reverse_end_of_delimiter_skip) {
  OptionsT options;
  options.delimiter = '/';
  options.replacement = '/';
  options.skip = 1;
  options.reverse = true;

  std::string_view data = "/a/b/c/";
  auto stream = PathHierarchyTokenizer::make(std::move(options));
  ASSERT_NE(nullptr, stream);
  ASSERT_TRUE(stream->reset(data));

  AssertTokenStreamContents(stream.get(), {"/a/b/", "a/b/", "b/"}, {0, 1, 3},
                            {5, 5, 5}, {1, 1, 1});
}

TEST_F(PathHierarchyTokenizerTests, test_reverse_only_delimiter_skip) {
  OptionsT options;
  options.delimiter = '/';
  options.replacement = '/';
  options.skip = 1;
  options.reverse = true;

  std::string_view data = "/";
  auto stream = PathHierarchyTokenizer::make(std::move(options));
  ASSERT_NE(nullptr, stream);
  ASSERT_TRUE(stream->reset(data));

  AssertTokenStreamContents(stream.get(), {}, {}, {}, {});
}

TEST_F(PathHierarchyTokenizerTests, test_reverse_only_delimiters_skip) {
  OptionsT options;
  options.delimiter = '/';
  options.replacement = '/';
  options.skip = 1;
  options.reverse = true;

  std::string_view data = "//";
  auto stream = PathHierarchyTokenizer::make(std::move(options));
  ASSERT_NE(nullptr, stream);
  ASSERT_TRUE(stream->reset(data));

  AssertTokenStreamContents(stream.get(), {"/"}, {0}, {1}, {1});
}

TEST_F(PathHierarchyTokenizerTests, test_start_of_char_end_of_delimiter) {
  OptionsT options;
  options.delimiter = '/';
  options.reverse = false;

  std::string_view data = "a/b/c/";
  auto stream = PathHierarchyTokenizer::make(std::move(options));
  ASSERT_NE(nullptr, stream);
  ASSERT_TRUE(stream->reset(data));

  AssertTokenStreamContents(stream.get(), {"a", "a/b", "a/b/c", "a/b/c/"},
                            {0, 0, 0, 0}, {1, 3, 5, 6}, {1, 1, 1, 1});
}

TEST_F(PathHierarchyTokenizerTests, test_tokenizer_via_analyzer_forward) {
  auto test =
    [](std::string_view data, std::vector<std::string_view> expected_tokens,
       std::vector<size_t> expected_starts, std::vector<size_t> expected_ends) {
      auto stream = analyzers::Get(
        "path_hierarchy", irs::Type<irs::text_format::Json>::get(), "{}");
      ASSERT_NE(nullptr, stream);
      ASSERT_TRUE(stream->reset(data));

      std::vector<int> pos_increments(expected_tokens.size(), 1);
      AssertTokenStreamContents(stream.get(), expected_tokens, expected_starts,
                                expected_ends, pos_increments);
    };

  test("a/b/c", {"a", "a/b", "a/b/c"}, {0, 0, 0}, {1, 3, 5});
  test("a/b/c/", {"a", "a/b", "a/b/c", "a/b/c/"}, {0, 0, 0, 0}, {1, 3, 5, 6});
  test("/a/b/c", {"/a", "/a/b", "/a/b/c"}, {0, 0, 0}, {2, 4, 6});
  test("/a/b/c/", {"/a", "/a/b", "/a/b/c", "/a/b/c/"}, {0, 0, 0, 0},
       {2, 4, 6, 7});
}

TEST_F(PathHierarchyTokenizerTests, test_reverse_start_of_char_skip) {
  OptionsT options;
  options.delimiter = '/';
  options.replacement = '/';
  options.skip = 1;
  options.reverse = true;

  std::string_view data = "a/b/c";
  auto stream = PathHierarchyTokenizer::make(std::move(options));
  ASSERT_NE(nullptr, stream);
  ASSERT_TRUE(stream->reset(data));

  AssertTokenStreamContents(stream.get(), {"a/b/", "b/"}, {0, 2}, {4, 4},
                            {1, 1});
}

TEST_F(PathHierarchyTokenizerTests,
       test_reverse_start_of_char_end_of_delimiter_skip) {
  OptionsT options;
  options.delimiter = '/';
  options.replacement = '/';
  options.skip = 1;
  options.reverse = true;

  std::string_view data = "a/b/c/";
  auto stream = PathHierarchyTokenizer::make(std::move(options));
  ASSERT_NE(nullptr, stream);
  ASSERT_TRUE(stream->reset(data));

  AssertTokenStreamContents(stream.get(), {"a/b/", "b/"}, {0, 2}, {4, 4},
                            {1, 1});
}

TEST_F(PathHierarchyTokenizerTests, test_reverse_skip2) {
  OptionsT options;
  options.delimiter = '/';
  options.replacement = '/';
  options.skip = 2;
  options.reverse = true;

  std::string_view data = "/a/b/c/";
  auto stream = PathHierarchyTokenizer::make(std::move(options));
  ASSERT_NE(nullptr, stream);
  ASSERT_TRUE(stream->reset(data));

  AssertTokenStreamContents(stream.get(), {"/a/", "a/"}, {0, 1}, {3, 3},
                            {1, 1});
}

TEST_F(PathHierarchyTokenizerTests, test_tokenizer_via_analyzer_reverse) {
  auto test =
    [](std::string_view data, std::vector<std::string_view> expected_tokens,
       std::vector<size_t> expected_starts, std::vector<size_t> expected_ends) {
      auto stream = analyzers::Get("path_hierarchy",
                                   irs::Type<irs::text_format::Json>::get(),
                                   "{\"reverse\": true}");
      ASSERT_NE(nullptr, stream);
      ASSERT_TRUE(stream->reset(data));

      std::vector<int> pos_increments(expected_tokens.size(), 1);
      AssertTokenStreamContents(stream.get(), expected_tokens, expected_starts,
                                expected_ends, pos_increments);
    };

  test("a/b/c", {"a/b/c", "b/c", "c"}, {0, 2, 4}, {5, 5, 5});
  test("a/b/c/", {"a/b/c/", "b/c/", "c/"}, {0, 2, 4}, {6, 6, 6});
  test("/a/b/c", {"/a/b/c", "a/b/c", "b/c", "c"}, {0, 1, 3, 5}, {6, 6, 6, 6});
  test("/a/b/c/", {"/a/b/c/", "a/b/c/", "b/c/", "c/"}, {0, 1, 3, 5},
       {7, 7, 7, 7});
}

TEST_F(PathHierarchyTokenizerTests, test_make_config_json) {
  // with unknown parameter
  {
    std::string config =
      "{\"delimiter\":\"/\",\"replacement\":\"/\",\"invalid_parameter\":true,"
      "\"reverse\":false}";
    std::string actual;
    ASSERT_TRUE(analyzers::Normalize(actual, "path_hierarchy",
                                     irs::Type<irs::text_format::Json>::get(),
                                     config));
    ASSERT_EQ(vpack::Parser::fromJson(
                "{\"delimiter\":\"/\",\"replacement\":\"/\",\"buffer_size\":"
                "1024,\"reverse\":false,\"skip\":0}")
                ->toString(),
              actual);
  }

  // test normalization with custom values
  {
    std::string config =
      "{\"delimiter\":\".\",\"replacement\":\".\",\"reverse\":true}";
    std::string actual;
    ASSERT_TRUE(analyzers::Normalize(actual, "path_hierarchy",
                                     irs::Type<irs::text_format::Json>::get(),
                                     config));
    auto expected =
      vpack::Parser::fromJson(
        "{\"delimiter\":\".\",\"replacement\":\".\",\"buffer_size\":"
        "1024,\"reverse\":true,\"skip\":0}")
        ->toString();
    ASSERT_EQ(expected, actual);
  }

  // test VPack
  {
    std::string config =
      "{\"delimiter\":\":\",\"replacement\":\":\",\"invalid_parameter\":true}";
    auto in_vpack = vpack::Parser::fromJson(config);
    std::string in_str;
    in_str.assign(in_vpack->slice().startAs<char>(),
                  in_vpack->slice().byteSize());
    std::string out_str;
    ASSERT_TRUE(analyzers::Normalize(out_str, "path_hierarchy",
                                     irs::Type<irs::text_format::VPack>::get(),
                                     in_str));
    vpack::Slice out_slice(reinterpret_cast<const uint8_t*>(out_str.c_str()));
    ASSERT_EQ(
      vpack::Parser::fromJson(
        "{\"delimiter\":\":\",\"replacement\":\":\",\"buffer_size\":1024,"
        "\"reverse\":false,\"skip\":0}")
        ->toString(),
      out_slice.toString());
  }
}

TEST_F(PathHierarchyTokenizerTests, test_invalid_json) {
  // invalid JSON
  {
    ASSERT_EQ(nullptr,
              analyzers::Get("path_hierarchy",
                             irs::Type<irs::text_format::Json>::get(), ""));
    ASSERT_EQ(nullptr,
              analyzers::Get("path_hierarchy",
                             irs::Type<irs::text_format::Json>::get(), "1"));
    ASSERT_EQ(nullptr,
              analyzers::Get("path_hierarchy",
                             irs::Type<irs::text_format::Json>::get(), "[]"));
  }

  // invalid parameter types
  {
    ASSERT_EQ(nullptr, analyzers::Get("path_hierarchy",
                                      irs::Type<irs::text_format::Json>::get(),
                                      "{\"delimiter\": 1}"));
    ASSERT_EQ(nullptr, analyzers::Get("path_hierarchy",
                                      irs::Type<irs::text_format::Json>::get(),
                                      "{\"reverse\": 42}"));
    ASSERT_EQ(nullptr, analyzers::Get("path_hierarchy",
                                      irs::Type<irs::text_format::Json>::get(),
                                      "{\"skip\": \"invalid\"}"));
  }
}

TEST_F(PathHierarchyTokenizerTests, test_load_json) {
  // load JSON object with default options
  {
    std::string_view data = "/a/b/c";
    auto stream = analyzers::Get(
      "path_hierarchy", irs::Type<irs::text_format::Json>::get(), "{}");
    ASSERT_NE(nullptr, stream);
    ASSERT_TRUE(stream->reset(data));

    AssertTokenStreamContents(stream.get(), {"/a", "/a/b", "/a/b/c"}, {0, 0, 0},
                              {2, 4, 6}, {1, 1, 1});
  }

  // with custom delimiter
  {
    std::string_view data = "a.b.c";
    auto stream =
      analyzers::Get("path_hierarchy", irs::Type<irs::text_format::Json>::get(),
                     "{\"delimiter\":\".\" }");
    ASSERT_NE(nullptr, stream);
    ASSERT_TRUE(stream->reset(data));

    AssertTokenStreamContents(stream.get(), {"a", "a.b", "a.b.c"}, {0, 0, 0},
                              {1, 3, 5}, {1, 1, 1});
  }

  // with reverse mode
  {
    std::string_view data = "www.example.com";
    auto stream = analyzers::Get(
      "path_hierarchy", irs::Type<irs::text_format::Json>::get(),
      "{\"reverse\": true, \"delimiter\": \".\", \"replacement\": \".\"}");
    ASSERT_NE(nullptr, stream);
    ASSERT_TRUE(stream->reset(data));

    AssertTokenStreamContents(stream.get(),
                              {"www.example.com", "example.com", "com"},
                              {0, 4, 12}, {15, 15, 15}, {1, 1, 1});
  }
}

TEST_F(PathHierarchyTokenizerTests, test_load_vpack) {
  std::string_view data = "/a/b";
  auto builder = vpack::Parser::fromJson(R"({"delimiter":"/"})");
  std::string in_str;
  in_str.assign(builder->slice().startAs<char>(), builder->slice().byteSize());
  auto stream = analyzers::Get(
    "path_hierarchy", irs::Type<irs::text_format::VPack>::get(), in_str);
  ASSERT_NE(nullptr, stream);
  ASSERT_TRUE(stream->reset(data));

  AssertTokenStreamContents(stream.get(), {"/a", "/a/b"}, {0, 0}, {2, 4},
                            {1, 1});
}

}  // namespace irs::analysis
