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

#include <iresearch/analysis/path_hierarchy_tokenizer.hpp>
#include <iresearch/analysis/tokenizer.hpp>

#include "gtest/gtest.h"
#include "iresearch/analysis/batch/token_batch.hpp"
#include "token_sink_utils.hpp"

namespace irs::analysis {

class PathHierarchyTokenizerTests : public ::testing::Test {
 public:
  static void SetUpTestCase() {}
};

void AssertTokenStreamContents(
  analysis::Tokenizer* stream, std::string_view data,
  const std::vector<std::string_view>& expected_tokens,
  const std::vector<size_t>& expected_start_offsets,
  const std::vector<size_t>& expected_end_offsets,
  const std::vector<int>& expected_pos_increments = {}) {
  size_t token_idx = 0;
  const auto check = [&](irs::TokenBatch& batch,
                         std::span<const irs::DocRun> /*runs*/) {
    ASSERT_TRUE(stream->Traits().dense_pos);
    for (uint32_t i = 0; i < batch.count; ++i, ++token_idx) {
      SCOPED_TRACE(testing::Message() << "token=" << token_idx);
      ASSERT_LT(token_idx, expected_tokens.size());
      const auto& t = batch.terms[i];
      ASSERT_EQ(expected_tokens[token_idx],
                std::string_view(t.GetData(), t.GetSize()));
      ASSERT_EQ(expected_start_offsets[token_idx], batch.offs_start[i]);
      ASSERT_EQ(expected_end_offsets[token_idx], batch.offs_end[i]);
      if (!expected_pos_increments.empty()) {
        ASSERT_EQ(1, expected_pos_increments[token_idx]);
      }
    }
  };
  tests::FnTokenSink sink{irs::TokenLayout::TermsPosOffs, check};
  ASSERT_TRUE(stream->Fill(data, sink.writer, sink.layout));
  sink.writer.Finish();
  ASSERT_EQ(token_idx, expected_tokens.size());
}

TEST_F(PathHierarchyTokenizerTests, consts) {
  static_assert("path_hierarchy" == irs::Type<PathHierarchyTokenizer>::name());
}

using Options = PathHierarchyTokenizer::Options;

TEST_F(PathHierarchyTokenizerTests, test_forward_mode) {
  Options options;
  options.reverse = false;

  std::string_view data = "/a/b/c";
  auto stream = PathHierarchyTokenizer::Make(std::move(options));
  ASSERT_NE(nullptr, stream);
  ASSERT_EQ(irs::Type<PathHierarchyTokenizer>::id(), stream->type());

  AssertTokenStreamContents(stream.get(), data, {"/a", "/a/b", "/a/b/c"},
                            {0, 0, 0}, {2, 4, 6}, {1, 1, 1});
}

TEST_F(PathHierarchyTokenizerTests, test_single_element_path) {
  Options options;
  options.delimiter = "/";
  options.replacement = "/";
  options.reverse = false;

  std::string_view data = "a";
  auto stream = PathHierarchyTokenizer::Make(std::move(options));
  ASSERT_NE(nullptr, stream);

  AssertTokenStreamContents(stream.get(), data, {"a"}, {0}, {1}, {1});
}

TEST_F(PathHierarchyTokenizerTests, test_custom_delimiter) {
  Options options;
  options.delimiter = "-";
  options.replacement = "-";
  options.reverse = false;

  std::string_view data = "a-b-c";
  auto stream = PathHierarchyTokenizer::Make(std::move(options));
  ASSERT_NE(nullptr, stream);

  AssertTokenStreamContents(stream.get(), data, {"a", "a-b", "a-b-c"},
                            {0, 0, 0}, {1, 3, 5}, {1, 1, 1});
}

TEST_F(PathHierarchyTokenizerTests, test_reset_multiple_times) {
  Options options;
  options.delimiter = "/";
  auto stream = PathHierarchyTokenizer::Make(std::move(options));
  ASSERT_NE(nullptr, stream);

  AssertTokenStreamContents(stream.get(), "/a/b", {"/a", "/a/b"}, {0, 0},
                            {2, 4}, {1, 1});

  AssertTokenStreamContents(stream.get(), "/xx/y1/z",
                            {"/xx", "/xx/y1", "/xx/y1/z"}, {0, 0, 0}, {3, 6, 8},
                            {1, 1, 1});
}

TEST_F(PathHierarchyTokenizerTests, test_empty_path) {
  Options options;
  auto stream = PathHierarchyTokenizer::Make(std::move(options));
  ASSERT_NE(nullptr, stream);
  AssertTokenStreamContents(stream.get(), "", {}, {}, {}, {});
}

TEST_F(PathHierarchyTokenizerTests, test_path_with_trailing_delimiter) {
  Options options;
  auto stream = PathHierarchyTokenizer::Make(std::move(options));
  ASSERT_NE(nullptr, stream);

  AssertTokenStreamContents(stream.get(), "/a/b/", {"/a", "/a/b", "/a/b/"},
                            {0, 0, 0}, {2, 4, 5}, {1, 1, 1});
}

TEST_F(PathHierarchyTokenizerTests, test_skip_exceeds_tokens) {
  Options options;
  options.delimiter = "/";
  options.skip = 5;
  options.reverse = false;

  std::string_view data = "/a/b";
  auto stream = PathHierarchyTokenizer::Make(std::move(options));
  ASSERT_NE(nullptr, stream);

  AssertTokenStreamContents(stream.get(), data, {}, {}, {}, {});
}

TEST_F(PathHierarchyTokenizerTests, test_standart_skip) {
  Options options;
  options.delimiter = "/";
  options.skip = 2;
  options.reverse = false;

  std::string_view data = "/a/b/c/d/e";
  auto stream = PathHierarchyTokenizer::Make(std::move(options));
  ASSERT_NE(nullptr, stream);

  AssertTokenStreamContents(stream.get(), data, {"/c", "/c/d", "/c/d/e"},
                            {4, 4, 4}, {6, 8, 10}, {1, 1, 1});
}

TEST_F(PathHierarchyTokenizerTests, test_empty_delimiter) {
  Options options;
  options.delimiter = std::string(1, '\0');
  options.reverse = false;

  std::string_view data = "abc";
  auto stream = PathHierarchyTokenizer::Make(std::move(options));
  ASSERT_NE(nullptr, stream);

  AssertTokenStreamContents(stream.get(), data, {"abc"}, {0}, {3}, {1});
}

TEST_F(PathHierarchyTokenizerTests, test_reset_without_next) {
  Options options;
  options.delimiter = "/";

  auto stream = PathHierarchyTokenizer::Make(std::move(options));
  ASSERT_NE(nullptr, stream);

  ASSERT_TRUE(tests::Analyze(*stream, "/a/b").has_value());

  AssertTokenStreamContents(stream.get(), "/x/y", {"/x", "/x/y"}, {0, 0},
                            {2, 4}, {1, 1});
}

TEST_F(PathHierarchyTokenizerTests, test_utf8_characters) {
  Options options;
  options.delimiter = "/";
  options.reverse = false;

  std::string_view data = "/café/café/café";
  auto stream = PathHierarchyTokenizer::Make(std::move(options));
  ASSERT_NE(nullptr, stream);

  AssertTokenStreamContents(stream.get(), data,
                            {"/café", "/café/café", "/café/café/café"},
                            {0, 0, 0}, {6, 12, 18}, {1, 1, 1});
}

TEST_F(PathHierarchyTokenizerTests, test_forward_string_delimiter) {
  Options options;
  options.delimiter = "::";
  options.replacement = "::";
  options.reverse = false;

  std::string_view data = "a::b::c";
  auto stream = PathHierarchyTokenizer::Make(std::move(options));
  ASSERT_NE(nullptr, stream);

  AssertTokenStreamContents(stream.get(), data, {"a", "a::b", "a::b::c"},
                            {0, 0, 0}, {1, 4, 7}, {1, 1, 1});
}

TEST_F(PathHierarchyTokenizerTests, test_forward_string_delimiter_leading) {
  Options options;
  options.delimiter = "::";
  options.replacement = "::";
  options.reverse = false;

  std::string_view data = "::a::b";
  auto stream = PathHierarchyTokenizer::Make(std::move(options));
  ASSERT_NE(nullptr, stream);

  AssertTokenStreamContents(stream.get(), data, {"::a", "::a::b"}, {0, 0},
                            {3, 6}, {1, 1});
}

TEST_F(PathHierarchyTokenizerTests, test_forward_string_delimiter_replacement) {
  Options options;
  options.delimiter = "::";
  options.replacement = "|";
  options.reverse = false;

  std::string_view data = "a::b::c";
  auto stream = PathHierarchyTokenizer::Make(std::move(options));
  ASSERT_NE(nullptr, stream);

  AssertTokenStreamContents(stream.get(), data, {"a", "a|b", "a|b|c"},
                            {0, 0, 0}, {1, 4, 7}, {1, 1, 1});
}

TEST_F(PathHierarchyTokenizerTests, test_forward_utf8_delimiter_bytes) {
  const std::string utf1 = "\xF0\x9F\x98\x8A";
  const std::string utf2 = "\xF0\x9F\x98\x8B";
  Options options;
  options.delimiter = utf1;
  options.replacement = utf2;
  options.reverse = false;

  const std::string data = std::string("a") + utf1 + "b" + utf1 + "c";
  const std::string expect1 = "a";
  const std::string expect2 = "a" + utf2 + "b";
  const std::string expect3 = "a" + utf2 + "b" + utf2 + "c";

  auto stream = PathHierarchyTokenizer::Make(std::move(options));
  ASSERT_NE(nullptr, stream);

  AssertTokenStreamContents(
    stream.get(), data,
    {std::string_view(expect1), std::string_view(expect2),
     std::string_view(expect3)},
    {0, 0, 0}, {1, 6, 11}, {1, 1, 1});
}

TEST_F(PathHierarchyTokenizerTests, test_forward_with_different_replacement) {
  Options options;
  options.delimiter = "/";
  options.replacement = "_";
  options.reverse = false;

  std::string_view data = "/a/b/c";
  auto stream = PathHierarchyTokenizer::Make(std::move(options));
  ASSERT_NE(nullptr, stream);

  AssertTokenStreamContents(stream.get(), data, {"_a", "_a_b", "_a_b_c"},
                            {0, 0, 0}, {2, 4, 6}, {1, 1, 1});
}

TEST_F(PathHierarchyTokenizerTests, test_consecutive_delimiters) {
  Options options;
  options.delimiter = "/";
  options.reverse = false;

  std::string_view data = "//a///b//";
  auto stream = PathHierarchyTokenizer::Make(std::move(options));
  ASSERT_NE(nullptr, stream);

  AssertTokenStreamContents(
    stream.get(), data,
    {"/", "//a", "//a/", "//a//", "//a///b", "//a///b/", "//a///b//"},
    {0, 0, 0, 0, 0, 0, 0}, {1, 3, 4, 5, 7, 8, 9}, {1, 1, 1, 1, 1, 1, 1});
}

TEST_F(PathHierarchyTokenizerTests, test_basic_without_leading_delimiter) {
  Options options;
  options.delimiter = "/";
  options.reverse = false;

  std::string_view data = "a/b/c";
  auto stream = PathHierarchyTokenizer::Make(std::move(options));
  ASSERT_NE(nullptr, stream);

  AssertTokenStreamContents(stream.get(), data, {"a", "a/b", "a/b/c"},
                            {0, 0, 0}, {1, 3, 5}, {1, 1, 1});
}

TEST_F(PathHierarchyTokenizerTests, test_only_delimiter) {
  Options options;
  options.delimiter = "/";
  options.reverse = false;

  std::string_view data = "/";
  auto stream = PathHierarchyTokenizer::Make(std::move(options));
  ASSERT_NE(nullptr, stream);

  AssertTokenStreamContents(stream.get(), data, {"/"}, {0}, {1}, {1});
}

TEST_F(PathHierarchyTokenizerTests, test_only_delimiters) {
  Options options;
  options.delimiter = "/";
  options.reverse = false;

  std::string_view data = "//";
  auto stream = PathHierarchyTokenizer::Make(std::move(options));
  ASSERT_NE(nullptr, stream);

  AssertTokenStreamContents(stream.get(), data, {"/", "//"}, {0, 0}, {1, 2},
                            {1, 1});
}

TEST_F(PathHierarchyTokenizerTests, test_delimiter_in_replacement) {
  Options options;
  options.delimiter = "/";
  options.replacement = "//";
  options.reverse = false;

  std::string_view data = "/a/b/c";
  auto stream = PathHierarchyTokenizer::Make(std::move(options));
  ASSERT_NE(nullptr, stream);

  AssertTokenStreamContents(stream.get(), data, {"//a", "//a//b", "//a//b//c"},
                            {0, 0, 0}, {2, 4, 6}, {1, 1, 1});
}

TEST_F(PathHierarchyTokenizerTests, test_forward_basic_skip) {
  Options options;
  options.delimiter = "/";
  options.skip = 1;
  options.reverse = false;

  std::string_view data = "/a/b/c";
  auto stream = PathHierarchyTokenizer::Make(std::move(options));
  ASSERT_NE(nullptr, stream);

  AssertTokenStreamContents(stream.get(), data, {"/b", "/b/c"}, {2, 2}, {4, 6},
                            {1, 1});
}

TEST_F(PathHierarchyTokenizerTests, test_forward_end_of_delimiter_skip) {
  Options options;
  options.delimiter = "/";
  options.skip = 1;
  options.reverse = false;

  std::string_view data = "/a/b/c/";
  auto stream = PathHierarchyTokenizer::Make(std::move(options));
  ASSERT_NE(nullptr, stream);

  AssertTokenStreamContents(stream.get(), data, {"/b", "/b/c", "/b/c/"},
                            {2, 2, 2}, {4, 6, 7}, {1, 1, 1});
}

TEST_F(PathHierarchyTokenizerTests, test_forward_start_of_char_skip) {
  Options options;
  options.delimiter = "/";
  options.skip = 1;
  options.reverse = false;

  std::string_view data = "a/b/c";
  auto stream = PathHierarchyTokenizer::Make(std::move(options));
  ASSERT_NE(nullptr, stream);

  AssertTokenStreamContents(stream.get(), data, {"/b", "/b/c"}, {1, 1}, {3, 5},
                            {1, 1});
}

TEST_F(PathHierarchyTokenizerTests,
       test_forward_start_of_char_end_of_delimiter_skip) {
  Options options;
  options.delimiter = "/";
  options.skip = 1;
  options.reverse = false;

  std::string_view data = "a/b/c/";
  auto stream = PathHierarchyTokenizer::Make(std::move(options));
  ASSERT_NE(nullptr, stream);

  AssertTokenStreamContents(stream.get(), data, {"/b", "/b/c", "/b/c/"},
                            {1, 1, 1}, {3, 5, 6}, {1, 1, 1});
}

TEST_F(PathHierarchyTokenizerTests, test_forward_only_delimiter_skip) {
  Options options;
  options.delimiter = "/";
  options.skip = 1;
  options.reverse = false;

  std::string_view data = "/";
  auto stream = PathHierarchyTokenizer::Make(std::move(options));
  ASSERT_NE(nullptr, stream);

  AssertTokenStreamContents(stream.get(), data, {}, {}, {}, {});
}

TEST_F(PathHierarchyTokenizerTests, test_forward_only_delimiters_skip) {
  Options options;
  options.delimiter = "/";
  options.skip = 1;
  options.reverse = false;

  std::string_view data = "//";
  auto stream = PathHierarchyTokenizer::Make(std::move(options));
  ASSERT_NE(nullptr, stream);

  AssertTokenStreamContents(stream.get(), data, {"/"}, {1}, {2}, {1});
}

TEST_F(PathHierarchyTokenizerTests, test_windows_path) {
  Options options;
  options.delimiter = "\\";
  options.replacement = "\\";
  options.reverse = false;

  std::string_view data = "c:\\a\\b\\c";
  auto stream = PathHierarchyTokenizer::Make(std::move(options));
  ASSERT_NE(nullptr, stream);

  AssertTokenStreamContents(stream.get(), data,
                            {"c:", "c:\\a", "c:\\a\\b", "c:\\a\\b\\c"},
                            {0, 0, 0, 0}, {2, 4, 6, 8}, {1, 1, 1, 1});
}

TEST_F(PathHierarchyTokenizerTests, test_reverse_mode) {
  Options options;
  options.delimiter = ".";
  options.replacement = "-";
  options.reverse = true;

  std::string_view data("www.example.com");
  auto stream = PathHierarchyTokenizer::Make(std::move(options));
  ASSERT_NE(nullptr, stream);

  AssertTokenStreamContents(stream.get(), data,
                            {"www-example-com", "example-com", "com"},
                            {0, 4, 12}, {15, 15, 15}, {1, 1, 1});
}

TEST_F(PathHierarchyTokenizerTests, test_reverse_domain_skip) {
  Options options;
  options.delimiter = ".";
  options.replacement = "-";
  options.skip = 2;
  options.reverse = true;

  std::string_view data = "a.b.c.d.e";
  auto stream = PathHierarchyTokenizer::Make(std::move(options));
  ASSERT_NE(nullptr, stream);

  AssertTokenStreamContents(stream.get(), data, {"a-b-c-", "b-c-", "c-"},
                            {0, 2, 4}, {6, 6, 6}, {1, 1, 1});
}

TEST_F(PathHierarchyTokenizerTests, test_reverse_basic) {
  Options options;
  options.delimiter = "/";
  options.replacement = "/";
  options.reverse = true;
  options.skip = 0;

  std::string_view data = "/a/b/c";
  auto stream = PathHierarchyTokenizer::Make(std::move(options));
  ASSERT_NE(nullptr, stream);

  AssertTokenStreamContents(stream.get(), data, {"/a/b/c", "a/b/c", "b/c", "c"},
                            {0, 1, 3, 5}, {6, 6, 6, 6}, {1, 1, 1, 1});
}

TEST_F(PathHierarchyTokenizerTests, test_reverse_end_of_delimiter) {
  Options options;
  options.delimiter = "/";
  options.replacement = "/";
  options.reverse = true;
  options.skip = 0;

  std::string_view data = "/a/b/c/";
  auto stream = PathHierarchyTokenizer::Make(std::move(options));
  ASSERT_NE(nullptr, stream);

  AssertTokenStreamContents(stream.get(), data,
                            {"/a/b/c/", "a/b/c/", "b/c/", "c/"}, {0, 1, 3, 5},
                            {7, 7, 7, 7}, {1, 1, 1, 1});
}

TEST_F(PathHierarchyTokenizerTests,
       test_reverse_start_of_char_end_of_delimiter) {
  Options options;
  options.delimiter = "/";
  options.replacement = "/";
  options.reverse = true;
  options.skip = 0;

  std::string_view data = "a/b/c/";
  auto stream = PathHierarchyTokenizer::Make(std::move(options));
  ASSERT_NE(nullptr, stream);

  AssertTokenStreamContents(stream.get(), data, {"a/b/c/", "b/c/", "c/"},
                            {0, 2, 4}, {6, 6, 6}, {1, 1, 1});
}

TEST_F(PathHierarchyTokenizerTests, test_reverse_only_delimiter) {
  Options options;
  options.delimiter = "/";
  options.replacement = "/";
  options.reverse = true;
  options.skip = 0;

  std::string_view data = "/";
  auto stream = PathHierarchyTokenizer::Make(std::move(options));
  ASSERT_NE(nullptr, stream);

  AssertTokenStreamContents(stream.get(), data, {"/"}, {0}, {1}, {1});
}

TEST_F(PathHierarchyTokenizerTests, test_reverse_only_delimiters) {
  Options options;
  options.delimiter = "/";
  options.replacement = "/";
  options.reverse = true;
  options.skip = 0;

  std::string_view data = "//";
  auto stream = PathHierarchyTokenizer::Make(std::move(options));
  ASSERT_NE(nullptr, stream);

  AssertTokenStreamContents(stream.get(), data, {"//", "/"}, {0, 1}, {2, 2},
                            {1, 1});
}

TEST_F(PathHierarchyTokenizerTests, test_reverse_delimiter_in_replacement) {
  Options options;
  options.delimiter = "/";
  options.replacement = "//";
  options.reverse = true;

  std::string_view data = "/a/b/c";
  auto stream = PathHierarchyTokenizer::Make(std::move(options));
  ASSERT_NE(nullptr, stream);

  AssertTokenStreamContents(stream.get(), data,
                            {"//a//b//c", "a//b//c", "b//c", "c"}, {0, 1, 3, 5},
                            {6, 6, 6, 6}, {1, 1, 1, 1});
}

TEST_F(PathHierarchyTokenizerTests, test_reverse_end_of_delimiter_skip) {
  Options options;
  options.delimiter = "/";
  options.replacement = "/";
  options.skip = 1;
  options.reverse = true;

  std::string_view data = "/a/b/c/";
  auto stream = PathHierarchyTokenizer::Make(std::move(options));
  ASSERT_NE(nullptr, stream);

  AssertTokenStreamContents(stream.get(), data, {"/a/b/", "a/b/", "b/"},
                            {0, 1, 3}, {5, 5, 5}, {1, 1, 1});
}

TEST_F(PathHierarchyTokenizerTests, test_reverse_only_delimiter_skip) {
  Options options;
  options.delimiter = "/";
  options.replacement = "/";
  options.skip = 1;
  options.reverse = true;

  std::string_view data = "/";
  auto stream = PathHierarchyTokenizer::Make(std::move(options));
  ASSERT_NE(nullptr, stream);

  AssertTokenStreamContents(stream.get(), data, {}, {}, {}, {});
}

TEST_F(PathHierarchyTokenizerTests, test_reverse_only_delimiters_skip) {
  Options options;
  options.delimiter = "/";
  options.replacement = "/";
  options.skip = 1;
  options.reverse = true;

  std::string_view data = "//";
  auto stream = PathHierarchyTokenizer::Make(std::move(options));
  ASSERT_NE(nullptr, stream);

  AssertTokenStreamContents(stream.get(), data, {"/"}, {0}, {1}, {1});
}

TEST_F(PathHierarchyTokenizerTests, test_start_of_char_end_of_delimiter) {
  Options options;
  options.delimiter = "/";
  options.reverse = false;

  std::string_view data = "a/b/c/";
  auto stream = PathHierarchyTokenizer::Make(std::move(options));
  ASSERT_NE(nullptr, stream);

  AssertTokenStreamContents(stream.get(), data, {"a", "a/b", "a/b/c", "a/b/c/"},
                            {0, 0, 0, 0}, {1, 3, 5, 6}, {1, 1, 1, 1});
}

TEST_F(PathHierarchyTokenizerTests, test_tokenizer_via_analyzer_forward) {
  auto test =
    [](std::string_view data, std::vector<std::string_view> expected_tokens,
       std::vector<size_t> expected_starts, std::vector<size_t> expected_ends) {
      auto stream = PathHierarchyTokenizer::Make(Options{});
      ASSERT_NE(nullptr, stream);

      std::vector<int> pos_increments(expected_tokens.size(), 1);
      AssertTokenStreamContents(stream.get(), data, expected_tokens,
                                expected_starts, expected_ends, pos_increments);
    };

  test("a/b/c", {"a", "a/b", "a/b/c"}, {0, 0, 0}, {1, 3, 5});
  test("a/b/c/", {"a", "a/b", "a/b/c", "a/b/c/"}, {0, 0, 0, 0}, {1, 3, 5, 6});
  test("/a/b/c", {"/a", "/a/b", "/a/b/c"}, {0, 0, 0}, {2, 4, 6});
  test("/a/b/c/", {"/a", "/a/b", "/a/b/c", "/a/b/c/"}, {0, 0, 0, 0},
       {2, 4, 6, 7});
}

TEST_F(PathHierarchyTokenizerTests, test_reverse_start_of_char_skip) {
  Options options;
  options.delimiter = "/";
  options.replacement = "/";
  options.skip = 1;
  options.reverse = true;

  std::string_view data = "a/b/c";
  auto stream = PathHierarchyTokenizer::Make(std::move(options));
  ASSERT_NE(nullptr, stream);

  AssertTokenStreamContents(stream.get(), data, {"a/b/", "b/"}, {0, 2}, {4, 4},
                            {1, 1});
}

TEST_F(PathHierarchyTokenizerTests,
       test_reverse_start_of_char_end_of_delimiter_skip) {
  Options options;
  options.delimiter = "/";
  options.replacement = "/";
  options.skip = 1;
  options.reverse = true;

  std::string_view data = "a/b/c/";
  auto stream = PathHierarchyTokenizer::Make(std::move(options));
  ASSERT_NE(nullptr, stream);

  AssertTokenStreamContents(stream.get(), data, {"a/b/", "b/"}, {0, 2}, {4, 4},
                            {1, 1});
}

TEST_F(PathHierarchyTokenizerTests, test_reverse_skip2) {
  Options options;
  options.delimiter = "/";
  options.replacement = "/";
  options.skip = 2;
  options.reverse = true;

  std::string_view data = "/a/b/c/";
  auto stream = PathHierarchyTokenizer::Make(std::move(options));
  ASSERT_NE(nullptr, stream);

  AssertTokenStreamContents(stream.get(), data, {"/a/", "a/"}, {0, 1}, {3, 3},
                            {1, 1});
}

TEST_F(PathHierarchyTokenizerTests,
       test_reverse_with_replacement_char_collision) {
  PathHierarchyTokenizer::Options options;
  options.delimiter = "/";
  options.replacement = "_";
  options.reverse = true;

  std::string_view data = "foo_bar/baz";
  auto stream = PathHierarchyTokenizer::Make(std::move(options));
  ASSERT_NE(nullptr, stream);

  AssertTokenStreamContents(stream.get(), data, {"foo_bar_baz", "baz"}, {0, 8},
                            {11, 11}, {1, 1});
}

TEST_F(PathHierarchyTokenizerTests, test_forward_offset_with_long_replacement) {
  PathHierarchyTokenizer::Options options;
  options.delimiter = "/";
  options.replacement = "---";
  options.reverse = false;
  options.skip = 0;

  std::string_view data = "/a/b";
  auto stream = PathHierarchyTokenizer::Make(std::move(options));
  ASSERT_NE(nullptr, stream);

  AssertTokenStreamContents(stream.get(), data, {"---a", "---a---b"}, {0, 0},
                            {2, 4}, {1, 1});
}

TEST_F(PathHierarchyTokenizerTests, test_replacement_contains_delimiter) {
  Options options;
  options.delimiter = "/";
  options.replacement = "_/_";

  std::string_view data = "a/b";
  auto stream = PathHierarchyTokenizer::Make(std::move(options));

  AssertTokenStreamContents(stream.get(), data, {"a", "a_/_b"}, {0, 0}, {1, 3},
                            {1, 1});
}

TEST_F(PathHierarchyTokenizerTests, test_skip_exactly_max) {
  Options options;
  options.delimiter = "/";
  options.skip = 3;
  options.reverse = false;

  std::string_view data = "a/b/c";
  auto stream = PathHierarchyTokenizer::Make(std::move(options));

  AssertTokenStreamContents(stream.get(), data, {}, {}, {}, {});
}

TEST_F(PathHierarchyTokenizerTests, test_skip_more_than_max) {
  Options options;
  options.delimiter = "/";
  options.skip = 4;
  options.reverse = false;

  std::string_view data = "a/b/c";
  auto stream = PathHierarchyTokenizer::Make(std::move(options));

  AssertTokenStreamContents(stream.get(), data, {}, {}, {}, {});
}

TEST_F(PathHierarchyTokenizerTests, test_reverse_consecutive_delimiters) {
  Options options;
  options.delimiter = "/";
  options.reverse = true;

  std::string_view data = "a//b";
  auto stream = PathHierarchyTokenizer::Make(std::move(options));

  AssertTokenStreamContents(stream.get(), data, {"a//b", "/b", "b"}, {0, 2, 3},
                            {4, 4, 4}, {1, 1, 1});
}

TEST_F(PathHierarchyTokenizerTests, test_long_path_exceeding_buffer) {
  Options options;
  options.delimiter = "/";
  options.replacement = "/";

  std::string segment(5, 'a');
  std::string data;

  for (int i = 0; i < 500; ++i) {
    data += "/" + segment;
  }

  auto stream = PathHierarchyTokenizer::Make(std::move(options));
  ASSERT_NE(nullptr, stream);

  auto tokens = tests::Analyze(*stream, data);
  ASSERT_TRUE(tokens.has_value());
  ASSERT_EQ(500, tokens->size());
  const auto& last = tokens->back();
  ASSERT_EQ(data, last.term);
  ASSERT_EQ(0, last.offs_start);
  ASSERT_EQ(data.size(), last.offs_end);
}

TEST_F(PathHierarchyTokenizerTests, test_tokenizer_via_analyzer_reverse) {
  auto test =
    [](std::string_view data, std::vector<std::string_view> expected_tokens,
       std::vector<size_t> expected_starts, std::vector<size_t> expected_ends) {
      auto stream = PathHierarchyTokenizer::Make(Options{.reverse = true});
      ASSERT_NE(nullptr, stream);

      std::vector<int> pos_increments(expected_tokens.size(), 1);
      AssertTokenStreamContents(stream.get(), data, expected_tokens,
                                expected_starts, expected_ends, pos_increments);
    };

  test("a/b/c", {"a/b/c", "b/c", "c"}, {0, 2, 4}, {5, 5, 5});
  test("a/b/c/", {"a/b/c/", "b/c/", "c/"}, {0, 2, 4}, {6, 6, 6});
  test("/a/b/c", {"/a/b/c", "a/b/c", "b/c", "c"}, {0, 1, 3, 5}, {6, 6, 6, 6});
  test("/a/b/c/", {"/a/b/c/", "a/b/c/", "b/c/", "c/"}, {0, 1, 3, 5},
       {7, 7, 7, 7});
}

TEST_F(PathHierarchyTokenizerTests, test_load_json) {
  // load JSON object with default options
  {
    std::string_view data = "/a/b/c";
    auto stream = PathHierarchyTokenizer::Make(Options{});
    ASSERT_NE(nullptr, stream);

    AssertTokenStreamContents(stream.get(), data, {"/a", "/a/b", "/a/b/c"},
                              {0, 0, 0}, {2, 4, 6}, {1, 1, 1});
  }

  // with custom delimiter
  {
    std::string_view data = "a.b.c";
    auto stream = PathHierarchyTokenizer::Make(Options{
      .delimiter = ".",
      .replacement = ".",
    });
    ASSERT_NE(nullptr, stream);

    AssertTokenStreamContents(stream.get(), data, {"a", "a.b", "a.b.c"},
                              {0, 0, 0}, {1, 3, 5}, {1, 1, 1});
  }

  // with reverse mode
  {
    std::string_view data = "www.example.com";
    auto stream = PathHierarchyTokenizer::Make(Options{
      .delimiter = ".",
      .replacement = ".",
      .reverse = true,
    });
    ASSERT_NE(nullptr, stream);

    AssertTokenStreamContents(stream.get(), data,
                              {"www.example.com", "example.com", "com"},
                              {0, 4, 12}, {15, 15, 15}, {1, 1, 1});
  }
}

TEST_F(PathHierarchyTokenizerTests, test_invalid_input) {
  // The legacy `test_invalid_json` test exercised JSON-parser-level
  // rejection (empty input, non-object root, non-string `delimiter`,
  // non-bool `reverse`, non-integer `skip`, and empty-string `delimiter`).
  // The JSON parse path is gone; the direct-Options API is strongly
  // typed so the type-mismatch cases have no direct analogue. Keep the
  // happy-path default construction as a smoke test that the typed API
  // produces a valid analyzer.
  auto stream = PathHierarchyTokenizer::Make(Options{});
  ASSERT_NE(nullptr, stream);
  AssertTokenStreamContents(stream.get(), "/a/b", {"/a", "/a/b"}, {0, 0},
                            {2, 4}, {1, 1});
}

TEST_F(PathHierarchyTokenizerTests, test_load_options) {
  // Ported from the original `test_load_vpack` test, which round-tripped
  // a VPack-encoded `{"delimiter":"/"}` blob through the analyzer
  // registry. With the JSON/VPack parse paths removed, the equivalent
  // is direct construction with the same delimiter value -- the
  // round-trip itself is covered by VPack serializer tests elsewhere.
  std::string_view data = "/a/b";
  auto stream = PathHierarchyTokenizer::Make(Options{.delimiter = "/"});
  ASSERT_NE(nullptr, stream);

  AssertTokenStreamContents(stream.get(), data, {"/a", "/a/b"}, {0, 0}, {2, 4},
                            {1, 1});
}

TEST_F(PathHierarchyTokenizerTests, native_fills_match_pull) {
  using Options = PathHierarchyTokenizer::Options;

  std::string deep;
  for (size_t i = 0; i < 1200; ++i) {
    deep += "/d" + std::to_string(i);
  }
  const std::vector<std::string> values = {
    "",        "/a/b/c", "a/b/c/",
    "nodelim", "//",     "/a//b/c",
    deep,      "a.b.c",  "/\xc3\xa9/\xe2\x82\xac"};

  std::vector<Options> configs;
  configs.push_back(Options{});
  configs.push_back(Options{.reverse = true});
  configs.push_back(Options{.skip = 1});
  configs.push_back(Options{.skip = 1, .reverse = true});
  configs.push_back(Options{.delimiter = ".", .replacement = "-"});
  configs.push_back(
    Options{.delimiter = ".", .replacement = "-", .reverse = true});
  configs.push_back(Options{.delimiter = "::", .replacement = "->"});
  configs.push_back(Options{.delimiter = "//"});

  for (size_t c = 0; c < configs.size(); ++c) {
    auto pull_stream = PathHierarchyTokenizer::Make(Options{configs[c]});
    auto fill_stream = PathHierarchyTokenizer::Make(Options{configs[c]});
    for (const auto& v : values) {
      SCOPED_TRACE(testing::Message()
                   << "config=" << c << " value.size=" << v.size());
      auto pulled_tokens = tests::Analyze(*pull_stream, v);
      ASSERT_TRUE(pulled_tokens.has_value());
      std::vector<std::string> pulled;
      std::vector<uint32_t> pstarts;
      std::vector<uint32_t> pends;
      for (auto& t : *pulled_tokens) {
        pulled.emplace_back(std::move(t.term));
        pstarts.push_back(t.offs_start);
        pends.push_back(t.offs_end);
      }

      std::vector<std::string> filled;
      std::vector<uint32_t> fstarts;
      std::vector<uint32_t> fends;
      const auto collect = [&](irs::TokenBatch& batch,
                               std::span<const irs::DocRun> /*runs*/) {
        for (uint32_t i = 0; i < batch.count; ++i) {
          const auto& t = batch.terms[i];
          filled.emplace_back(t.GetData(), t.GetSize());
          fstarts.push_back(batch.offs_start[i]);
          fends.push_back(batch.offs_end[i]);
        }
      };
      tests::FnTokenSink sink{irs::TokenLayout::TermsPosOffs, collect};
      ASSERT_TRUE(fill_stream->Fill(v, sink.writer, sink.layout));
      sink.writer.Finish();

      ASSERT_EQ(pulled, filled);
      ASSERT_EQ(pstarts, fstarts);
      ASSERT_EQ(pends, fends);
    }
  }
}

}  // namespace irs::analysis
