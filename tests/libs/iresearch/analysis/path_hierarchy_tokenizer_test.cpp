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

TEST_F(PathHierarchyTokenizerTests, consts) {
  static_assert("path_hierarchy" == irs::Type<PathHierarchyTokenizer>::name());
}

TEST_F(PathHierarchyTokenizerTests, test_forward_mode) {
  using OptionsT = PathHierarchyTokenizer::OptionsT;

  // test default forward mode
  OptionsT options;
  options.reverse = false;

  std::string_view data = "/a/b/c";
  PathHierarchyTokenizer stream(options);
  ASSERT_EQ(irs::Type<PathHierarchyTokenizer>::id(), stream.type());

  auto* offset = irs::get<irs::OffsAttr>(stream);
  auto* payload = irs::get<irs::PayAttr>(stream);
  ASSERT_EQ(nullptr, payload);
  auto* term = irs::get<irs::TermAttr>(stream);
  auto* inc = irs::get<irs::IncAttr>(stream);

  ASSERT_TRUE(stream.reset(data));

  // First token: "/a" (up to first delimiter, excluding it)
  ASSERT_TRUE(stream.next());
  ASSERT_EQ(0, offset->start);
  ASSERT_EQ(2, offset->end);
  ASSERT_EQ("/a", irs::ViewCast<char>(term->value));
  ASSERT_EQ(1, inc->value);

  // Second token: "/a/b" (up to second delimiter, excluding it)
  ASSERT_TRUE(stream.next());
  ASSERT_EQ(0, offset->start);
  ASSERT_EQ(4, offset->end);
  ASSERT_EQ("/a/b", irs::ViewCast<char>(term->value));
  ASSERT_EQ(1, inc->value);

  // Third token: "/a/b/c" (entire string)
  ASSERT_TRUE(stream.next());
  ASSERT_EQ(0, offset->start);
  ASSERT_EQ(6, offset->end);
  ASSERT_EQ("/a/b/c", irs::ViewCast<char>(term->value));
  ASSERT_EQ(1, inc->value);

  ASSERT_FALSE(stream.next());
}

TEST_F(PathHierarchyTokenizerTests, test_reverse_mode) {
  using OptionsT = PathHierarchyTokenizer::OptionsT;

  // test reverse mode (domain-like)

  OptionsT options;
  options.delimiter = '.';
  options.replacement = '-';
  options.reverse = true;

  std::string_view data("www.example.com");
  PathHierarchyTokenizer stream(options);

  auto* offset = irs::get<irs::OffsAttr>(stream);
  auto* payload = irs::get<irs::PayAttr>(stream);
  ASSERT_EQ(nullptr, payload);
  auto* term = irs::get<irs::TermAttr>(stream);
  auto* inc = irs::get<irs::IncAttr>(stream);

  ASSERT_TRUE(stream.reset(data));

  // First token: "www.example.com"
  ASSERT_TRUE(stream.next());
  ASSERT_EQ(0, offset->start);
  ASSERT_EQ(15, offset->end);
  ASSERT_EQ("www-example-com", irs::ViewCast<char>(term->value));
  ASSERT_EQ(1, inc->value);

  // Second token: "example.com"
  ASSERT_TRUE(stream.next());
  ASSERT_EQ(4, offset->start);
  ASSERT_EQ(15, offset->end);
  ASSERT_EQ("example-com", irs::ViewCast<char>(term->value));
  ASSERT_EQ(1, inc->value);

  // Third token: "com"
  ASSERT_TRUE(stream.next());
  ASSERT_EQ(12, offset->start);
  ASSERT_EQ(15, offset->end);
  ASSERT_EQ("com", irs::ViewCast<char>(term->value));
  ASSERT_EQ(1, inc->value);

  ASSERT_FALSE(stream.next());
}

TEST_F(PathHierarchyTokenizerTests, test_single_element_path) {
  using OptionsT = PathHierarchyTokenizer::OptionsT;

  // test single element (no delimiter)

  OptionsT options;
  options.delimiter = '/';
  options.replacement = '/';
  options.reverse = false;

  std::string_view data = "a";
  PathHierarchyTokenizer stream(options);

  auto* offset = irs::get<irs::OffsAttr>(stream);
  auto* term = irs::get<irs::TermAttr>(stream);

  ASSERT_TRUE(stream.reset(data));

  ASSERT_TRUE(stream.next());
  ASSERT_EQ(0, offset->start);
  ASSERT_EQ(1, offset->end);
  ASSERT_EQ("a", irs::ViewCast<char>(term->value));

  ASSERT_FALSE(stream.next());
}

TEST_F(PathHierarchyTokenizerTests, test_custom_delimiter) {
  using OptionsT = PathHierarchyTokenizer::OptionsT;

  // test custom delimiter
  OptionsT options;
  options.delimiter = '-';
  options.replacement = '-';
  options.reverse = false;

  std::string_view data = "a-b-c";
  PathHierarchyTokenizer stream(options);

  auto* offset = irs::get<irs::OffsAttr>(stream);
  auto* term = irs::get<irs::TermAttr>(stream);

  // For "a-b-c" with delims at [1,3] in Lucene:
  // Token 0: "a" (0,1)
  ASSERT_TRUE(stream.reset(data));

  ASSERT_TRUE(stream.next());
  ASSERT_EQ(0, offset->start);
  ASSERT_EQ(1, offset->end);
  ASSERT_EQ("a", irs::ViewCast<char>(term->value));

  // For "a-b-c" with delims at [1,3]:
  // Token 0: up to delim[1] = 3 -> "a-b"
  ASSERT_TRUE(stream.next());
  ASSERT_EQ(0, offset->start);
  ASSERT_EQ(3, offset->end);
  ASSERT_EQ("a-b", irs::ViewCast<char>(term->value));

  // Token 1: full text -> "a-b-c"
  ASSERT_TRUE(stream.next());
  ASSERT_EQ(0, offset->start);
  ASSERT_EQ(5, offset->end);
  ASSERT_EQ("a-b-c", irs::ViewCast<char>(term->value));

  ASSERT_FALSE(stream.next());
}

TEST_F(PathHierarchyTokenizerTests, test_load_json) {
  // load JSON object with default options
  {
    std::string_view data = "/a/b/c";
    auto stream = analyzers::Get(
      "path_hierarchy", irs::Type<irs::text_format::Json>::get(), "{}");

    ASSERT_NE(nullptr, stream);
    ASSERT_TRUE(stream->reset(data));

    auto* offset = irs::get<irs::OffsAttr>(*stream);
    auto* payload = irs::get<irs::PayAttr>(*stream);
    ASSERT_EQ(nullptr, payload);
    auto* term = irs::get<irs::TermAttr>(*stream);

    ASSERT_TRUE(stream->next());
    ASSERT_EQ(0, offset->start);
    ASSERT_EQ(2, offset->end);
    ASSERT_EQ("/a", irs::ViewCast<char>(term->value));

    ASSERT_TRUE(stream->next());
    ASSERT_EQ(0, offset->start);
    ASSERT_EQ(4, offset->end);
    ASSERT_EQ("/a/b", irs::ViewCast<char>(term->value));

    ASSERT_TRUE(stream->next());
    ASSERT_EQ(0, offset->start);
    ASSERT_EQ(6, offset->end);
    ASSERT_EQ("/a/b/c", irs::ViewCast<char>(term->value));

    ASSERT_FALSE(stream->next());
  }

  // with custom delimiter
  {
    std::string_view data = "a.b.c";
    auto stream =
      analyzers::Get("path_hierarchy", irs::Type<irs::text_format::Json>::get(),
                     "{\"delimiter\":\".\" }");

    ASSERT_NE(nullptr, stream);
    ASSERT_TRUE(stream->reset(data));

    auto* offset = irs::get<irs::OffsAttr>(*stream);
    auto* term = irs::get<irs::TermAttr>(*stream);

    // For "a.b.c" with delims at [1,3]:
    // Token 0:  "a" (0,1)
    ASSERT_TRUE(stream->next());
    ASSERT_EQ(0, offset->start);
    ASSERT_EQ(1, offset->end);
    ASSERT_EQ("a", irs::ViewCast<char>(term->value));

    // Token 1: up to delim[1] = 3 -> "a.b"
    ASSERT_TRUE(stream->next());
    ASSERT_EQ(0, offset->start);
    ASSERT_EQ(3, offset->end);
    ASSERT_EQ("a.b", irs::ViewCast<char>(term->value));

    // Token 1: full text -> "a.b.c"
    ASSERT_TRUE(stream->next());
    ASSERT_EQ(0, offset->start);
    ASSERT_EQ(5, offset->end);
    ASSERT_EQ("a.b.c", irs::ViewCast<char>(term->value));

    ASSERT_FALSE(stream->next());
  }

  // with reverse mode
  {
    std::string_view data = "www.example.com";
    auto stream = analyzers::Get(
      "path_hierarchy", irs::Type<irs::text_format::Json>::get(),
      "{\"reverse\": true, \"delimiter\": \".\", \"replacement\": \".\"}");

    ASSERT_NE(nullptr, stream);
    ASSERT_TRUE(stream->reset(data));

    auto* offset = irs::get<irs::OffsAttr>(*stream);
    auto* term = irs::get<irs::TermAttr>(*stream);

    ASSERT_TRUE(stream->next());
    ASSERT_EQ(0, offset->start);
    ASSERT_EQ(15, offset->end);
    ASSERT_EQ("www.example.com", irs::ViewCast<char>(term->value));

    ASSERT_TRUE(stream->next());
    ASSERT_EQ(4, offset->start);
    ASSERT_EQ(15, offset->end);
    ASSERT_EQ("example.com", irs::ViewCast<char>(term->value));

    ASSERT_TRUE(stream->next());
    ASSERT_EQ(12, offset->start);
    ASSERT_EQ(15, offset->end);
    ASSERT_EQ("com", irs::ViewCast<char>(term->value));

    ASSERT_FALSE(stream->next());
  }
}

TEST_F(PathHierarchyTokenizerTests, test_load_vpack) {
  // load VPack object
  std::string_view data = "/a/b";
  auto builder = vpack::Parser::fromJson(R"({"delimiter":"/"})");
  std::string in_str;
  in_str.assign(builder->slice().startAs<char>(), builder->slice().byteSize());
  auto stream = analyzers::Get(
    "path_hierarchy", irs::Type<irs::text_format::VPack>::get(), in_str);

  ASSERT_NE(nullptr, stream);
  ASSERT_TRUE(stream->reset(data));

  auto* offset = irs::get<irs::OffsAttr>(*stream);
  auto* term = irs::get<irs::TermAttr>(*stream);

  ASSERT_TRUE(stream->next());
  ASSERT_EQ(0, offset->start);
  ASSERT_EQ(2, offset->end);
  ASSERT_EQ("/a", irs::ViewCast<char>(term->value));

  ASSERT_TRUE(stream->next());
  ASSERT_EQ(0, offset->start);
  ASSERT_EQ(4, offset->end);
  ASSERT_EQ("/a/b", irs::ViewCast<char>(term->value));

  ASSERT_FALSE(stream->next());
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

TEST_F(PathHierarchyTokenizerTests, test_reset_multiple_times) {
  using OptionsT = PathHierarchyTokenizer::OptionsT;

  OptionsT options;
  options.delimiter = '/';
  PathHierarchyTokenizer stream(options);

  auto* term = irs::get<irs::TermAttr>(stream);
  auto* offset = irs::get<irs::OffsAttr>(stream);

  // First reset
  ASSERT_TRUE(stream.reset("/a/b"));
  ASSERT_TRUE(stream.next());
  ASSERT_EQ("/a", irs::ViewCast<char>(term->value));
  ASSERT_EQ(0, offset->start);
  ASSERT_EQ(2, offset->end);
  ASSERT_TRUE(stream.next());
  ASSERT_EQ("/a/b", irs::ViewCast<char>(term->value));
  ASSERT_EQ(0, offset->start);
  ASSERT_EQ(4, offset->end);
  ASSERT_FALSE(stream.next());

  // Second reset
  ASSERT_TRUE(stream.reset("/xx/y1/z"));
  ASSERT_TRUE(stream.next());
  ASSERT_EQ("/xx", irs::ViewCast<char>(term->value));
  ASSERT_EQ(0, offset->start);
  ASSERT_EQ(3, offset->end);
  ASSERT_TRUE(stream.next());
  ASSERT_EQ("/xx/y1", irs::ViewCast<char>(term->value));
  ASSERT_EQ(0, offset->start);
  ASSERT_EQ(6, offset->end);
  ASSERT_TRUE(stream.next());
  ASSERT_EQ("/xx/y1/z", irs::ViewCast<char>(term->value));
  ASSERT_EQ(0, offset->start);
  ASSERT_EQ(8, offset->end);
  ASSERT_FALSE(stream.next());
}

TEST_F(PathHierarchyTokenizerTests, test_empty_path) {
  using OptionsT = PathHierarchyTokenizer::OptionsT;

  OptionsT options;
  PathHierarchyTokenizer stream(options);

  ASSERT_TRUE(stream.reset(""));
  ASSERT_FALSE(stream.next());
}

TEST_F(PathHierarchyTokenizerTests, test_path_with_trailing_delimiter) {
  using OptionsT = PathHierarchyTokenizer::OptionsT;

  OptionsT options;
  PathHierarchyTokenizer stream(options);

  auto* term = irs::get<irs::TermAttr>(stream);
  auto* offset = irs::get<irs::OffsAttr>(stream);

  ASSERT_TRUE(stream.reset("/a/b/"));

  ASSERT_TRUE(stream.next());
  ASSERT_EQ("/a", irs::ViewCast<char>(term->value));
  ASSERT_EQ(0, offset->start);
  ASSERT_EQ(2, offset->end);

  ASSERT_TRUE(stream.next());
  ASSERT_EQ("/a/b", irs::ViewCast<char>(term->value));
  ASSERT_EQ(0, offset->start);
  ASSERT_EQ(4, offset->end);

  ASSERT_TRUE(stream.next());
  ASSERT_EQ(0, offset->start);
  ASSERT_EQ(5, offset->end);
  ASSERT_EQ("/a/b/", irs::ViewCast<char>(term->value));

  ASSERT_FALSE(stream.next());
}

TEST_F(PathHierarchyTokenizerTests, test_skip_exceeds_tokens) {
  using OptionsT = PathHierarchyTokenizer::OptionsT;

  OptionsT options;
  options.delimiter = '/';
  options.skip = 5;
  options.reverse = false;

  std::string_view data = "/a/b";
  PathHierarchyTokenizer stream(options);

  ASSERT_TRUE(stream.reset(data));
  ASSERT_FALSE(stream.next());
}

TEST_F(PathHierarchyTokenizerTests, test_standart_skip) {
  using OptionsT = PathHierarchyTokenizer::OptionsT;

  OptionsT options;
  options.delimiter = '/';
  options.skip = 2;
  options.reverse = false;

  std::string_view data = "/a/b/c/d/e";
  PathHierarchyTokenizer stream(options);

  auto* term = irs::get<irs::TermAttr>(stream);
  auto* offset = irs::get<irs::OffsAttr>(stream);

  ASSERT_TRUE(stream.reset(data));

  // With skip=2 in Lucene: tokens start from the 3rd level
  // Delimiters at positions: [0, 2, 4, 6, 8]
  // Skip 2 means we skip "/a" and "/a/b"
  // First token: "/a/b/c" (from delimiter at pos 2 to delimiter at pos 4)
  ASSERT_TRUE(stream.next());
  ASSERT_EQ("/c", irs::ViewCast<char>(term->value));
  ASSERT_EQ(4, offset->start);
  ASSERT_EQ(6, offset->end);

  ASSERT_TRUE(stream.next());
  ASSERT_EQ("/c/d", irs::ViewCast<char>(term->value));
  ASSERT_EQ(4, offset->start);
  ASSERT_EQ(8, offset->end);

  ASSERT_TRUE(stream.next());
  ASSERT_EQ("/c/d/e", irs::ViewCast<char>(term->value));
  ASSERT_EQ(4, offset->start);
  ASSERT_EQ(10, offset->end);

  ASSERT_FALSE(stream.next());
}

TEST_F(PathHierarchyTokenizerTests, test_reverse_domain_skip) {
  using OptionsT = PathHierarchyTokenizer::OptionsT;
  OptionsT options;
  options.delimiter = '.';
  options.replacement = '-';
  options.skip = 2;
  options.reverse = true;

  std::string_view data = "a.b.c.d.e";
  PathHierarchyTokenizer stream(options);
  ASSERT_TRUE(stream.reset(data));

  auto* term = irs::get<irs::TermAttr>(stream);
  auto* offset = irs::get<irs::OffsAttr>(stream);

  ASSERT_TRUE(stream.next());
  ASSERT_EQ("a-b-c-", irs::ViewCast<char>(term->value));
  ASSERT_EQ(0, offset->start);
  ASSERT_EQ(6, offset->end);

  ASSERT_TRUE(stream.next());
  ASSERT_EQ("b-c-", irs::ViewCast<char>(term->value));
  ASSERT_EQ(2, offset->start);
  ASSERT_EQ(6, offset->end);

  ASSERT_TRUE(stream.next());
  ASSERT_EQ("c-", irs::ViewCast<char>(term->value));
  ASSERT_EQ(4, offset->start);
  ASSERT_EQ(6, offset->end);

  ASSERT_FALSE(stream.next());
}

TEST_F(PathHierarchyTokenizerTests, test_empty_delimiter) {
  using OptionsT = PathHierarchyTokenizer::OptionsT;

  OptionsT options;
  options.delimiter = '\0';
  options.reverse = false;

  std::string_view data = "abc";
  PathHierarchyTokenizer stream(options);

  auto* term = irs::get<irs::TermAttr>(stream);
  auto* offset = irs::get<irs::OffsAttr>(stream);

  ASSERT_TRUE(stream.reset(data));

  ASSERT_TRUE(stream.next());
  ASSERT_EQ("abc", irs::ViewCast<char>(term->value));
  ASSERT_EQ(0, offset->start);
  ASSERT_EQ(3, offset->end);

  ASSERT_FALSE(stream.next());
}

TEST_F(PathHierarchyTokenizerTests, test_reset_without_next) {
  using OptionsT = PathHierarchyTokenizer::OptionsT;

  OptionsT options;
  options.delimiter = '/';

  PathHierarchyTokenizer stream(options);

  ASSERT_TRUE(stream.reset("/a/b"));
  ASSERT_TRUE(stream.reset("/x/y"));

  auto* term = irs::get<irs::TermAttr>(stream);

  ASSERT_TRUE(stream.next());
  ASSERT_EQ("/x", irs::ViewCast<char>(term->value));

  ASSERT_TRUE(stream.next());
  ASSERT_EQ("/x/y", irs::ViewCast<char>(term->value));

  ASSERT_FALSE(stream.next());
}

TEST_F(PathHierarchyTokenizerTests, test_utf8_characters) {
  using OptionsT = PathHierarchyTokenizer::OptionsT;

  OptionsT options;
  options.delimiter = '/';
  options.reverse = false;

  std::string_view data = "/café/café/café";
  PathHierarchyTokenizer stream(options);

  auto* term = irs::get<irs::TermAttr>(stream);

  ASSERT_TRUE(stream.reset(data));

  ASSERT_TRUE(stream.next());
  ASSERT_EQ("/café", irs::ViewCast<char>(term->value));

  ASSERT_TRUE(stream.next());
  ASSERT_EQ("/café/café", irs::ViewCast<char>(term->value));

  ASSERT_TRUE(stream.next());
  ASSERT_EQ("/café/café/café", irs::ViewCast<char>(term->value));

  ASSERT_FALSE(stream.next());
}

TEST_F(PathHierarchyTokenizerTests, test_forward_with_different_replacement) {
  using OptionsT = PathHierarchyTokenizer::OptionsT;

  OptionsT options;
  options.delimiter = '/';
  options.replacement = '_';
  options.reverse = false;

  std::string_view data = "/a/b/c";
  PathHierarchyTokenizer stream(options);

  auto* term = irs::get<irs::TermAttr>(stream);

  ASSERT_TRUE(stream.reset(data));

  ASSERT_TRUE(stream.next());
  ASSERT_EQ("_a", irs::ViewCast<char>(term->value));

  ASSERT_TRUE(stream.next());
  ASSERT_EQ("_a_b", irs::ViewCast<char>(term->value));

  ASSERT_TRUE(stream.next());
  ASSERT_EQ("_a_b_c", irs::ViewCast<char>(term->value));

  ASSERT_FALSE(stream.next());
}

TEST_F(PathHierarchyTokenizerTests, test_consecutive_delimiters) {
  using OptionsT = PathHierarchyTokenizer::OptionsT;

  OptionsT options;
  options.delimiter = '/';
  options.reverse = false;

  std::string_view data = "//a///b//";
  PathHierarchyTokenizer stream(options);

  auto* term = irs::get<irs::TermAttr>(stream);
  auto* offset = irs::get<irs::OffsAttr>(stream);

  ASSERT_TRUE(stream.reset(data));

  ASSERT_TRUE(stream.next());
  ASSERT_EQ("/", irs::ViewCast<char>(term->value));
  ASSERT_EQ(0, offset->start);
  ASSERT_EQ(1, offset->end);

  ASSERT_TRUE(stream.next());
  ASSERT_EQ("//a", irs::ViewCast<char>(term->value));
  ASSERT_EQ(0, offset->start);
  ASSERT_EQ(3, offset->end);

  ASSERT_TRUE(stream.next());
  ASSERT_EQ("//a/", irs::ViewCast<char>(term->value));
  ASSERT_EQ(0, offset->start);
  ASSERT_EQ(4, offset->end);

  ASSERT_TRUE(stream.next());
  ASSERT_EQ("//a//", irs::ViewCast<char>(term->value));
  ASSERT_EQ(0, offset->start);
  ASSERT_EQ(5, offset->end);

  ASSERT_TRUE(stream.next());
  ASSERT_EQ("//a///b", irs::ViewCast<char>(term->value));
  ASSERT_EQ(0, offset->start);
  ASSERT_EQ(7, offset->end);

  ASSERT_TRUE(stream.next());
  ASSERT_EQ("//a///b/", irs::ViewCast<char>(term->value));
  ASSERT_EQ(0, offset->start);
  ASSERT_EQ(8, offset->end);

  ASSERT_TRUE(stream.next());
  ASSERT_EQ("//a///b//", irs::ViewCast<char>(term->value));
  ASSERT_EQ(0, offset->start);
  ASSERT_EQ(9, offset->end);

  ASSERT_FALSE(stream.next());
}

TEST_F(PathHierarchyTokenizerTests, test_basic_without_leading_delimiter) {
  using OptionsT = PathHierarchyTokenizer::OptionsT;

  OptionsT options;
  options.delimiter = '/';
  options.reverse = false;

  std::string_view data = "a/b/c";
  PathHierarchyTokenizer stream(options);

  auto* term = irs::get<irs::TermAttr>(stream);
  auto* offset = irs::get<irs::OffsAttr>(stream);

  ASSERT_TRUE(stream.reset(data));

  ASSERT_TRUE(stream.next());
  ASSERT_EQ("a", irs::ViewCast<char>(term->value));
  ASSERT_EQ(0, offset->start);
  ASSERT_EQ(1, offset->end);

  ASSERT_TRUE(stream.next());
  ASSERT_EQ("a/b", irs::ViewCast<char>(term->value));
  ASSERT_EQ(0, offset->start);
  ASSERT_EQ(3, offset->end);

  ASSERT_TRUE(stream.next());
  ASSERT_EQ("a/b/c", irs::ViewCast<char>(term->value));
  ASSERT_EQ(0, offset->start);
  ASSERT_EQ(5, offset->end);

  ASSERT_FALSE(stream.next());
}

TEST_F(PathHierarchyTokenizerTests, test_only_delimiter) {
  using OptionsT = PathHierarchyTokenizer::OptionsT;

  OptionsT options;
  options.delimiter = '/';
  options.reverse = false;

  std::string_view data = "/";
  PathHierarchyTokenizer stream(options);

  auto* term = irs::get<irs::TermAttr>(stream);
  auto* offset = irs::get<irs::OffsAttr>(stream);

  ASSERT_TRUE(stream.reset(data));

  ASSERT_TRUE(stream.next());
  ASSERT_EQ("/", irs::ViewCast<char>(term->value));
  ASSERT_EQ(0, offset->start);
  ASSERT_EQ(1, offset->end);

  ASSERT_FALSE(stream.next());
}

TEST_F(PathHierarchyTokenizerTests, test_only_delimiters) {
  using OptionsT = PathHierarchyTokenizer::OptionsT;

  OptionsT options;
  options.delimiter = '/';
  options.reverse = false;

  std::string_view data = "//";
  PathHierarchyTokenizer stream(options);

  auto* term = irs::get<irs::TermAttr>(stream);
  auto* offset = irs::get<irs::OffsAttr>(stream);

  ASSERT_TRUE(stream.reset(data));

  ASSERT_TRUE(stream.next());
  ASSERT_EQ("/", irs::ViewCast<char>(term->value));
  ASSERT_EQ(0, offset->start);
  ASSERT_EQ(1, offset->end);

  ASSERT_TRUE(stream.next());
  ASSERT_EQ("//", irs::ViewCast<char>(term->value));
  ASSERT_EQ(0, offset->start);
  ASSERT_EQ(2, offset->end);

  ASSERT_FALSE(stream.next());
}

TEST_F(PathHierarchyTokenizerTests, test_forward_basic_skip) {
  using OptionsT = PathHierarchyTokenizer::OptionsT;

  OptionsT options;
  options.delimiter = '/';
  options.skip = 1;
  options.reverse = false;

  std::string_view data = "/a/b/c";
  PathHierarchyTokenizer stream(options);
  ASSERT_TRUE(stream.reset(data));

  auto* term = irs::get<irs::TermAttr>(stream);
  auto* offset = irs::get<irs::OffsAttr>(stream);
  auto* inc = irs::get<irs::IncAttr>(stream);

  ASSERT_TRUE(stream.next());
  ASSERT_EQ("/b", irs::ViewCast<char>(term->value));
  ASSERT_EQ(2, offset->start);
  ASSERT_EQ(4, offset->end);
  ASSERT_EQ(1, inc->value);

  ASSERT_TRUE(stream.next());
  ASSERT_EQ("/b/c", irs::ViewCast<char>(term->value));
  ASSERT_EQ(2, offset->start);
  ASSERT_EQ(6, offset->end);
  ASSERT_EQ(1, inc->value);

  ASSERT_FALSE(stream.next());
}

TEST_F(PathHierarchyTokenizerTests, test_forward_end_of_delimiter_skip) {
  using OptionsT = PathHierarchyTokenizer::OptionsT;

  OptionsT options;
  options.delimiter = '/';
  options.skip = 1;
  options.reverse = false;

  std::string_view data = "/a/b/c/";
  PathHierarchyTokenizer stream(options);
  ASSERT_TRUE(stream.reset(data));

  auto* term = irs::get<irs::TermAttr>(stream);
  auto* offset = irs::get<irs::OffsAttr>(stream);

  ASSERT_TRUE(stream.next());
  ASSERT_EQ("/b", irs::ViewCast<char>(term->value));
  ASSERT_EQ(2, offset->start);
  ASSERT_EQ(4, offset->end);

  ASSERT_TRUE(stream.next());
  ASSERT_EQ("/b/c", irs::ViewCast<char>(term->value));
  ASSERT_EQ(2, offset->start);
  ASSERT_EQ(6, offset->end);

  ASSERT_TRUE(stream.next());
  ASSERT_EQ("/b/c/", irs::ViewCast<char>(term->value));
  ASSERT_EQ(2, offset->start);
  ASSERT_EQ(7, offset->end);

  ASSERT_FALSE(stream.next());
}

TEST_F(PathHierarchyTokenizerTests, test_forward_start_of_char_skip) {
  using OptionsT = PathHierarchyTokenizer::OptionsT;

  OptionsT options;
  options.delimiter = '/';
  options.skip = 1;
  options.reverse = false;

  std::string_view data = "a/b/c";
  PathHierarchyTokenizer stream(options);
  ASSERT_TRUE(stream.reset(data));

  auto* term = irs::get<irs::TermAttr>(stream);
  auto* offset = irs::get<irs::OffsAttr>(stream);

  ASSERT_TRUE(stream.next());
  ASSERT_EQ("/b", irs::ViewCast<char>(term->value));
  ASSERT_EQ(1, offset->start);
  ASSERT_EQ(3, offset->end);

  ASSERT_TRUE(stream.next());
  ASSERT_EQ("/b/c", irs::ViewCast<char>(term->value));
  ASSERT_EQ(1, offset->start);
  ASSERT_EQ(5, offset->end);

  ASSERT_FALSE(stream.next());
}

TEST_F(PathHierarchyTokenizerTests,
       test_forward_start_of_char_end_of_delimiter_skip) {
  using OptionsT = PathHierarchyTokenizer::OptionsT;

  OptionsT options;
  options.delimiter = '/';
  options.skip = 1;
  options.reverse = false;

  std::string_view data = "a/b/c/";
  PathHierarchyTokenizer stream(options);
  ASSERT_TRUE(stream.reset(data));

  auto* term = irs::get<irs::TermAttr>(stream);
  auto* offset = irs::get<irs::OffsAttr>(stream);

  ASSERT_TRUE(stream.next());
  ASSERT_EQ("/b", irs::ViewCast<char>(term->value));
  ASSERT_EQ(1, offset->start);
  ASSERT_EQ(3, offset->end);

  ASSERT_TRUE(stream.next());
  ASSERT_EQ("/b/c", irs::ViewCast<char>(term->value));
  ASSERT_EQ(1, offset->start);
  ASSERT_EQ(5, offset->end);

  ASSERT_TRUE(stream.next());
  ASSERT_EQ("/b/c/", irs::ViewCast<char>(term->value));
  ASSERT_EQ(1, offset->start);
  ASSERT_EQ(6, offset->end);

  ASSERT_FALSE(stream.next());
}

TEST_F(PathHierarchyTokenizerTests, test_forward_only_delimiter_skip) {
  using OptionsT = PathHierarchyTokenizer::OptionsT;

  OptionsT options;
  options.delimiter = '/';
  options.skip = 1;
  options.reverse = false;

  std::string_view data = "/";
  PathHierarchyTokenizer stream(options);
  ASSERT_TRUE(stream.reset(data));
  ASSERT_FALSE(stream.next());
}

TEST_F(PathHierarchyTokenizerTests, test_forward_only_delimiters_skip) {
  using OptionsT = PathHierarchyTokenizer::OptionsT;

  OptionsT options;
  options.delimiter = '/';
  options.skip = 1;
  options.reverse = false;

  std::string_view data = "//";
  PathHierarchyTokenizer stream(options);
  ASSERT_TRUE(stream.reset(data));

  auto* term = irs::get<irs::TermAttr>(stream);
  auto* offset = irs::get<irs::OffsAttr>(stream);

  ASSERT_TRUE(stream.next());
  ASSERT_EQ("/", irs::ViewCast<char>(term->value));
  ASSERT_EQ(1, offset->start);
  ASSERT_EQ(2, offset->end);

  ASSERT_FALSE(stream.next());
}

TEST_F(PathHierarchyTokenizerTests, test_windows_path) {
  using OptionsT = PathHierarchyTokenizer::OptionsT;

  OptionsT options;
  options.delimiter = '\\';
  options.replacement = '\\';
  options.reverse = false;

  std::string_view data = "c:\\a\\b\\c";
  PathHierarchyTokenizer stream(options);
  ASSERT_TRUE(stream.reset(data));

  auto* term = irs::get<irs::TermAttr>(stream);
  auto* offset = irs::get<irs::OffsAttr>(stream);

  ASSERT_TRUE(stream.next());
  ASSERT_EQ("c:", irs::ViewCast<char>(term->value));
  ASSERT_EQ(0, offset->start);
  ASSERT_EQ(2, offset->end);

  ASSERT_TRUE(stream.next());
  ASSERT_EQ("c:\\a", irs::ViewCast<char>(term->value));
  ASSERT_EQ(0, offset->start);
  ASSERT_EQ(4, offset->end);

  ASSERT_TRUE(stream.next());
  ASSERT_EQ("c:\\a\\b", irs::ViewCast<char>(term->value));
  ASSERT_EQ(0, offset->start);
  ASSERT_EQ(6, offset->end);

  ASSERT_TRUE(stream.next());
  ASSERT_EQ("c:\\a\\b\\c", irs::ViewCast<char>(term->value));
  ASSERT_EQ(0, offset->start);
  ASSERT_EQ(8, offset->end);

  ASSERT_FALSE(stream.next());
}

TEST_F(PathHierarchyTokenizerTests, test_reverse_basic) {
  using OptionsT = PathHierarchyTokenizer::OptionsT;

  OptionsT options;
  options.delimiter = '/';
  options.replacement = '/';
  options.reverse = true;
  options.skip = 0;

  std::string_view data = "/a/b/c";
  PathHierarchyTokenizer stream(options);
  ASSERT_TRUE(stream.reset(data));

  auto* term = irs::get<irs::TermAttr>(stream);
  auto* offset = irs::get<irs::OffsAttr>(stream);

  ASSERT_TRUE(stream.next());
  ASSERT_EQ("/a/b/c", irs::ViewCast<char>(term->value));
  ASSERT_EQ(0, offset->start);
  ASSERT_EQ(6, offset->end);

  ASSERT_TRUE(stream.next());
  ASSERT_EQ("a/b/c", irs::ViewCast<char>(term->value));
  ASSERT_EQ(1, offset->start);
  ASSERT_EQ(6, offset->end);

  ASSERT_TRUE(stream.next());
  ASSERT_EQ("b/c", irs::ViewCast<char>(term->value));
  ASSERT_EQ(3, offset->start);
  ASSERT_EQ(6, offset->end);

  ASSERT_TRUE(stream.next());
  ASSERT_EQ("c", irs::ViewCast<char>(term->value));
  ASSERT_EQ(5, offset->start);
  ASSERT_EQ(6, offset->end);

  ASSERT_FALSE(stream.next());
}

TEST_F(PathHierarchyTokenizerTests, test_reverse_end_of_delimiter) {
  using OptionsT = PathHierarchyTokenizer::OptionsT;

  OptionsT options;
  options.delimiter = '/';
  options.replacement = '/';
  options.reverse = true;
  options.skip = 0;

  std::string_view data = "/a/b/c/";
  PathHierarchyTokenizer stream(options);
  ASSERT_TRUE(stream.reset(data));

  auto* term = irs::get<irs::TermAttr>(stream);
  auto* offset = irs::get<irs::OffsAttr>(stream);

  ASSERT_TRUE(stream.next());
  ASSERT_EQ("/a/b/c/", irs::ViewCast<char>(term->value));
  ASSERT_EQ(0, offset->start);
  ASSERT_EQ(7, offset->end);

  ASSERT_TRUE(stream.next());
  ASSERT_EQ("a/b/c/", irs::ViewCast<char>(term->value));
  ASSERT_EQ(1, offset->start);
  ASSERT_EQ(7, offset->end);

  ASSERT_TRUE(stream.next());
  ASSERT_EQ("b/c/", irs::ViewCast<char>(term->value));
  ASSERT_EQ(3, offset->start);
  ASSERT_EQ(7, offset->end);

  ASSERT_TRUE(stream.next());
  ASSERT_EQ("c/", irs::ViewCast<char>(term->value));
  ASSERT_EQ(5, offset->start);
  ASSERT_EQ(7, offset->end);

  ASSERT_FALSE(stream.next());
}

TEST_F(PathHierarchyTokenizerTests,
       test_reverse_start_of_char_end_of_delimiter) {
  using OptionsT = PathHierarchyTokenizer::OptionsT;

  OptionsT options;
  options.delimiter = '/';
  options.replacement = '/';
  options.reverse = true;
  options.skip = 0;

  std::string_view data = "a/b/c/";
  PathHierarchyTokenizer stream(options);
  ASSERT_TRUE(stream.reset(data));

  auto* term = irs::get<irs::TermAttr>(stream);
  auto* offset = irs::get<irs::OffsAttr>(stream);

  ASSERT_TRUE(stream.next());
  ASSERT_EQ("a/b/c/", irs::ViewCast<char>(term->value));
  ASSERT_EQ(0, offset->start);
  ASSERT_EQ(6, offset->end);

  ASSERT_TRUE(stream.next());
  ASSERT_EQ("b/c/", irs::ViewCast<char>(term->value));
  ASSERT_EQ(2, offset->start);
  ASSERT_EQ(6, offset->end);

  ASSERT_TRUE(stream.next());
  ASSERT_EQ("c/", irs::ViewCast<char>(term->value));
  ASSERT_EQ(4, offset->start);
  ASSERT_EQ(6, offset->end);

  ASSERT_FALSE(stream.next());
}

TEST_F(PathHierarchyTokenizerTests, test_reverse_only_delimiter) {
  using OptionsT = PathHierarchyTokenizer::OptionsT;

  OptionsT options;
  options.delimiter = '/';
  options.replacement = '/';
  options.reverse = true;
  options.skip = 0;

  std::string_view data = "/";
  PathHierarchyTokenizer stream(options);
  ASSERT_TRUE(stream.reset(data));

  auto* term = irs::get<irs::TermAttr>(stream);
  auto* offset = irs::get<irs::OffsAttr>(stream);

  ASSERT_TRUE(stream.next());
  ASSERT_EQ("/", irs::ViewCast<char>(term->value));
  ASSERT_EQ(0, offset->start);
  ASSERT_EQ(1, offset->end);

  ASSERT_FALSE(stream.next());
}

TEST_F(PathHierarchyTokenizerTests, test_reverse_only_delimiters) {
  using OptionsT = PathHierarchyTokenizer::OptionsT;

  OptionsT options;
  options.delimiter = '/';
  options.replacement = '/';
  options.reverse = true;
  options.skip = 0;

  std::string_view data = "//";
  PathHierarchyTokenizer stream(options);
  ASSERT_TRUE(stream.reset(data));

  auto* term = irs::get<irs::TermAttr>(stream);
  auto* offset = irs::get<irs::OffsAttr>(stream);

  ASSERT_TRUE(stream.next());
  ASSERT_EQ("//", irs::ViewCast<char>(term->value));
  ASSERT_EQ(0, offset->start);
  ASSERT_EQ(2, offset->end);

  ASSERT_TRUE(stream.next());
  ASSERT_EQ("/", irs::ViewCast<char>(term->value));
  ASSERT_EQ(1, offset->start);
  ASSERT_EQ(2, offset->end);

  ASSERT_FALSE(stream.next());
}

TEST_F(PathHierarchyTokenizerTests, test_reverse_end_of_delimiter_skip) {
  using OptionsT = PathHierarchyTokenizer::OptionsT;

  OptionsT options;
  options.delimiter = '/';
  options.replacement = '/';
  options.skip = 1;
  options.reverse = true;

  std::string_view data = "/a/b/c/";
  PathHierarchyTokenizer stream(options);
  ASSERT_TRUE(stream.reset(data));

  auto* term = irs::get<irs::TermAttr>(stream);
  auto* offset = irs::get<irs::OffsAttr>(stream);

  ASSERT_TRUE(stream.next());
  ASSERT_EQ("/a/b/", irs::ViewCast<char>(term->value));
  ASSERT_EQ(0, offset->start);
  ASSERT_EQ(5, offset->end);

  ASSERT_TRUE(stream.next());
  ASSERT_EQ("a/b/", irs::ViewCast<char>(term->value));
  ASSERT_EQ(1, offset->start);
  ASSERT_EQ(5, offset->end);

  ASSERT_TRUE(stream.next());
  ASSERT_EQ("b/", irs::ViewCast<char>(term->value));
  ASSERT_EQ(3, offset->start);
  ASSERT_EQ(5, offset->end);

  ASSERT_FALSE(stream.next());
}
TEST_F(PathHierarchyTokenizerTests, test_reverse_only_delimiter_skip) {
  using OptionsT = PathHierarchyTokenizer::OptionsT;

  OptionsT options;
  options.delimiter = '/';
  options.replacement = '/';
  options.skip = 1;
  options.reverse = true;

  std::string_view data = "/";
  PathHierarchyTokenizer stream(options);
  ASSERT_TRUE(stream.reset(data));
  ASSERT_FALSE(stream.next());
}

TEST_F(PathHierarchyTokenizerTests, test_reverse_only_delimiters_skip) {
  using OptionsT = PathHierarchyTokenizer::OptionsT;

  OptionsT options;
  options.delimiter = '/';
  options.replacement = '/';
  options.skip = 1;
  options.reverse = true;

  std::string_view data = "//";
  PathHierarchyTokenizer stream(options);
  ASSERT_TRUE(stream.reset(data));

  auto* term = irs::get<irs::TermAttr>(stream);
  auto* offset = irs::get<irs::OffsAttr>(stream);

  ASSERT_TRUE(stream.next());
  ASSERT_EQ("/", irs::ViewCast<char>(term->value));
  ASSERT_EQ(0, offset->start);
  ASSERT_EQ(1, offset->end);

  ASSERT_FALSE(stream.next());
}

TEST_F(PathHierarchyTokenizerTests, test_start_of_char_end_of_delimiter) {
  using OptionsT = PathHierarchyTokenizer::OptionsT;
  OptionsT options;
  options.delimiter = '/';
  options.reverse = false;

  std::string_view data = "a/b/c/";
  PathHierarchyTokenizer stream(options);
  ASSERT_TRUE(stream.reset(data));

  auto* term = irs::get<irs::TermAttr>(stream);
  auto* offset = irs::get<irs::OffsAttr>(stream);

  ASSERT_TRUE(stream.next());
  ASSERT_EQ("a", irs::ViewCast<char>(term->value));
  ASSERT_EQ(0, offset->start);
  ASSERT_EQ(1, offset->end);

  ASSERT_TRUE(stream.next());
  ASSERT_EQ("a/b", irs::ViewCast<char>(term->value));
  ASSERT_EQ(0, offset->start);
  ASSERT_EQ(3, offset->end);

  ASSERT_TRUE(stream.next());
  ASSERT_EQ("a/b/c", irs::ViewCast<char>(term->value));
  ASSERT_EQ(0, offset->start);
  ASSERT_EQ(5, offset->end);

  ASSERT_TRUE(stream.next());
  ASSERT_EQ("a/b/c/", irs::ViewCast<char>(term->value));
  ASSERT_EQ(0, offset->start);
  ASSERT_EQ(6, offset->end);

  ASSERT_FALSE(stream.next());
}

TEST_F(PathHierarchyTokenizerTests, test_tokenizer_via_analyzer_forward) {
  auto test =
    [](std::string_view data, std::vector<std::string> expected_tokens,
       std::vector<size_t> expected_starts, std::vector<size_t> expected_ends) {
      auto stream = analyzers::Get(
        "path_hierarchy", irs::Type<irs::text_format::Json>::get(), "{}");
      ASSERT_NE(nullptr, stream);
      ASSERT_TRUE(stream->reset(data));

      auto* term = irs::get<irs::TermAttr>(*stream);
      auto* offset = irs::get<irs::OffsAttr>(*stream);

      for (size_t i = 0; i < expected_tokens.size(); ++i) {
        ASSERT_TRUE(stream->next());
        ASSERT_EQ(expected_tokens[i], irs::ViewCast<char>(term->value));
        ASSERT_EQ(expected_starts[i], offset->start);
        ASSERT_EQ(expected_ends[i], offset->end);
      }
      ASSERT_FALSE(stream->next());
    };

  test("a/b/c", {"a", "a/b", "a/b/c"}, {0, 0, 0}, {1, 3, 5});
  test("a/b/c/", {"a", "a/b", "a/b/c", "a/b/c/"}, {0, 0, 0, 0}, {1, 3, 5, 6});
  test("/a/b/c", {"/a", "/a/b", "/a/b/c"}, {0, 0, 0}, {2, 4, 6});
  test("/a/b/c/", {"/a", "/a/b", "/a/b/c", "/a/b/c/"}, {0, 0, 0, 0},
       {2, 4, 6, 7});
}

TEST_F(PathHierarchyTokenizerTests, test_reverse_start_of_char_skip) {
  using OptionsT = PathHierarchyTokenizer::OptionsT;
  OptionsT options;
  options.delimiter = '/';
  options.replacement = '/';
  options.skip = 1;
  options.reverse = true;

  std::string_view data = "a/b/c";
  PathHierarchyTokenizer stream(options);
  ASSERT_TRUE(stream.reset(data));

  auto* term = irs::get<irs::TermAttr>(stream);
  auto* offset = irs::get<irs::OffsAttr>(stream);

  ASSERT_TRUE(stream.next());
  ASSERT_EQ("a/b/", irs::ViewCast<char>(term->value));
  ASSERT_EQ(0, offset->start);
  ASSERT_EQ(4, offset->end);

  ASSERT_TRUE(stream.next());
  ASSERT_EQ("b/", irs::ViewCast<char>(term->value));
  ASSERT_EQ(2, offset->start);
  ASSERT_EQ(4, offset->end);

  ASSERT_FALSE(stream.next());
}

TEST_F(PathHierarchyTokenizerTests,
       test_reverse_start_of_char_end_of_delimiter_skip) {
  using OptionsT = PathHierarchyTokenizer::OptionsT;
  OptionsT options;
  options.delimiter = '/';
  options.replacement = '/';
  options.skip = 1;
  options.reverse = true;

  std::string_view data = "a/b/c/";
  PathHierarchyTokenizer stream(options);
  ASSERT_TRUE(stream.reset(data));

  auto* term = irs::get<irs::TermAttr>(stream);
  auto* offset = irs::get<irs::OffsAttr>(stream);

  ASSERT_TRUE(stream.next());
  ASSERT_EQ("a/b/", irs::ViewCast<char>(term->value));
  ASSERT_EQ(0, offset->start);
  ASSERT_EQ(4, offset->end);

  ASSERT_TRUE(stream.next());
  ASSERT_EQ("b/", irs::ViewCast<char>(term->value));
  ASSERT_EQ(2, offset->start);
  ASSERT_EQ(4, offset->end);

  ASSERT_FALSE(stream.next());
}

TEST_F(PathHierarchyTokenizerTests, test_reverse_skip2) {
  using OptionsT = PathHierarchyTokenizer::OptionsT;
  OptionsT options;
  options.delimiter = '/';
  options.replacement = '/';
  options.skip = 2;
  options.reverse = true;

  std::string_view data = "/a/b/c/";
  PathHierarchyTokenizer stream(options);
  ASSERT_TRUE(stream.reset(data));

  auto* term = irs::get<irs::TermAttr>(stream);
  auto* offset = irs::get<irs::OffsAttr>(stream);

  ASSERT_TRUE(stream.next());
  ASSERT_EQ("/a/", irs::ViewCast<char>(term->value));
  ASSERT_EQ(0, offset->start);
  ASSERT_EQ(3, offset->end);

  ASSERT_TRUE(stream.next());
  ASSERT_EQ("a/", irs::ViewCast<char>(term->value));
  ASSERT_EQ(1, offset->start);
  ASSERT_EQ(3, offset->end);

  ASSERT_FALSE(stream.next());
}

TEST_F(PathHierarchyTokenizerTests, test_random_strings_forward) {
  using OptionsT = PathHierarchyTokenizer::OptionsT;

  OptionsT options;
  options.delimiter = '/';
  options.reverse = false;

  PathHierarchyTokenizer stream(options);
  std::srand(static_cast<unsigned>(std::time(nullptr)));

  for (size_t i = 0; i < 100; ++i) {
    std::string s;
    size_t len = std::rand() % 50 + 1;
    for (size_t j = 0; j < len; ++j) {
      char c = static_cast<char>(std::rand() % 95 + 32);
      s.push_back(c);
    }
    ASSERT_TRUE(stream.reset(s));
    size_t token_count = 0;
    while (stream.next()) {
      ++token_count;
    }
    ASSERT_GE(token_count, 0);
  }
}

TEST_F(PathHierarchyTokenizerTests, test_random_strings_reverse) {
  using OptionsT = PathHierarchyTokenizer::OptionsT;

  OptionsT options;
  options.delimiter = '/';
  options.reverse = true;

  PathHierarchyTokenizer stream(options);
  std::srand(static_cast<unsigned>(std::time(nullptr)));

  for (size_t i = 0; i < 100; ++i) {
    std::string s;
    size_t len = std::rand() % 50 + 1;
    for (size_t j = 0; j < len; ++j) {
      char c = static_cast<char>(std::rand() % 95 + 32);
      s.push_back(c);
    }
    ASSERT_TRUE(stream.reset(s));
    size_t token_count = 0;
    while (stream.next()) {
      ++token_count;
    }
    ASSERT_GE(token_count, 0);
  }
}

}  // namespace irs::analysis
