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
#include <iresearch/analysis/edge_ngram_tokenizer.hpp>

#include "gtest/gtest.h"

namespace irs::analysis {

class EdgeNGramTokenizerTests : public ::testing::Test {};

namespace {

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

}  // namespace

TEST_F(EdgeNGramTokenizerTests, consts) {
  static_assert("edge_ngram" == irs::Type<EdgeNGramTokenizer>::name());
}

TEST_F(EdgeNGramTokenizerTests, test_ascii_prefixes_min_max) {
  auto stream =
    analyzers::Get("edge_ngram", irs::Type<irs::text_format::Json>::get(),
                   "{\"min\":1, \"max\":3, \"preserveOriginal\":false}");
  ASSERT_NE(nullptr, stream);
  ASSERT_TRUE(stream->reset("abcd"));

  AssertTokenStreamContents(stream.get(), {"a", "ab", "abc"}, {0, 0, 0},
                            {1, 2, 3}, {1, 0, 0});
}

TEST_F(EdgeNGramTokenizerTests, test_utf8_cyrillic_three_letters) {
  auto stream =
    analyzers::Get("edge_ngram", irs::Type<irs::text_format::Json>::get(),
                   "{\"min\":1, \"max\":10, \"preserveOriginal\":false}");
  ASSERT_NE(nullptr, stream);
  std::string_view data = "\xd0\xbf\xd1\x80\xd0\xb8";
  ASSERT_TRUE(stream->reset(data));

  AssertTokenStreamContents(stream.get(),
                            {std::string_view("\xd0\xbf", 2),
                             std::string_view("\xd0\xbf\xd1\x80", 4), data},
                            {0, 0, 0}, {2, 4, 6}, {1, 0, 0});
}

TEST_F(EdgeNGramTokenizerTests, test_utf8_mixed_ascii_and_cyrillic) {
  auto stream =
    analyzers::Get("edge_ngram", irs::Type<irs::text_format::Json>::get(),
                   "{\"min\":1, \"max\":2, \"preserveOriginal\":false}");
  ASSERT_NE(nullptr, stream);
  std::string data = "a\xd0\xb1";
  ASSERT_TRUE(stream->reset(data));

  AssertTokenStreamContents(stream.get(),
                            {std::string_view("a"), std::string_view(data)},
                            {0, 0}, {1, 3}, {1, 0});
}

TEST_F(EdgeNGramTokenizerTests, test_utf8_ascii_and_supplementary_emoji) {
  auto stream =
    analyzers::Get("edge_ngram", irs::Type<irs::text_format::Json>::get(),
                   "{\"min\":1, \"max\":2, \"preserveOriginal\":false}");
  ASSERT_NE(nullptr, stream);
  std::string data = "A\xf0\x9f\x98\x80";
  ASSERT_TRUE(stream->reset(data));

  AssertTokenStreamContents(stream.get(),
                            {std::string_view("A"), std::string_view(data)},
                            {0, 0}, {1, 5}, {1, 0});
}

TEST_F(EdgeNGramTokenizerTests, test_utf8_three_byte_cjk) {
  auto stream =
    analyzers::Get("edge_ngram", irs::Type<irs::text_format::Json>::get(),
                   "{\"min\":1, \"max\":2, \"preserveOriginal\":false}");
  ASSERT_NE(nullptr, stream);
  std::string data = "\xe6\x97\xa5\xe6\x9c\x88";
  ASSERT_TRUE(stream->reset(data));

  AssertTokenStreamContents(
    stream.get(), {std::string_view("\xe6\x97\xa5", 3), std::string_view(data)},
    {0, 0}, {3, 6}, {1, 0});
}

TEST_F(EdgeNGramTokenizerTests, test_utf8_latin_with_precomposed_e_acute) {
  auto stream =
    analyzers::Get("edge_ngram", irs::Type<irs::text_format::Json>::get(),
                   "{\"min\":1, \"max\":3, \"preserveOriginal\":false}");
  ASSERT_NE(nullptr, stream);
  std::string data = "caf\xc3\xa9";
  ASSERT_TRUE(stream->reset(data));

  AssertTokenStreamContents(stream.get(), {"c", "ca", "caf"}, {0, 0, 0},
                            {1, 2, 3}, {1, 0, 0});
}

TEST_F(EdgeNGramTokenizerTests, test_utf8_single_codepoint_min_two_no_tokens) {
  auto stream =
    analyzers::Get("edge_ngram", irs::Type<irs::text_format::Json>::get(),
                   "{\"min\":2, \"max\":4, \"preserveOriginal\":false}");
  ASSERT_NE(nullptr, stream);
  ASSERT_TRUE(stream->reset("\xd0\xaf"));
  AssertTokenStreamContents(stream.get(), {}, {}, {}, {});
}

TEST_F(EdgeNGramTokenizerTests, test_optional_max_unbounded) {
  auto stream =
    analyzers::Get("edge_ngram", irs::Type<irs::text_format::Json>::get(),
                   "{\"min\":2, \"preserveOriginal\":false}");
  ASSERT_NE(nullptr, stream);
  ASSERT_TRUE(stream->reset("hello"));

  AssertTokenStreamContents(stream.get(), {"he", "hel", "hell", "hello"},
                            {0, 0, 0, 0}, {2, 3, 4, 5}, {1, 0, 0, 0});
}

TEST_F(EdgeNGramTokenizerTests, test_preserve_original_after_max) {
  auto stream =
    analyzers::Get("edge_ngram", irs::Type<irs::text_format::Json>::get(),
                   "{\"min\":1, \"max\":2, \"preserveOriginal\":true}");
  ASSERT_NE(nullptr, stream);
  ASSERT_TRUE(stream->reset("abcd"));

  AssertTokenStreamContents(stream.get(), {"a", "ab", "abcd"}, {0, 0, 0},
                            {1, 2, 4}, {1, 0, 0});
}

TEST_F(EdgeNGramTokenizerTests, test_min_greater_than_input_no_preserve) {
  auto stream =
    analyzers::Get("edge_ngram", irs::Type<irs::text_format::Json>::get(),
                   "{\"min\":3, \"max\":5, \"preserveOriginal\":false}");
  ASSERT_NE(nullptr, stream);
  ASSERT_TRUE(stream->reset("ab"));

  AssertTokenStreamContents(stream.get(), {}, {}, {}, {});
}

TEST_F(EdgeNGramTokenizerTests, test_min_greater_than_input_preserve) {
  auto stream =
    analyzers::Get("edge_ngram", irs::Type<irs::text_format::Json>::get(),
                   "{\"min\":3, \"max\":5, \"preserveOriginal\":true}");
  ASSERT_NE(nullptr, stream);
  ASSERT_TRUE(stream->reset("ab"));

  AssertTokenStreamContents(stream.get(), {"ab"}, {0}, {2}, {1});
}

TEST_F(EdgeNGramTokenizerTests,
       test_preserve_original_no_duplicates_if_within_limits) {
  auto stream =
    analyzers::Get("edge_ngram", irs::Type<irs::text_format::Json>::get(),
                   "{\"min\":1, \"max\":3, \"preserveOriginal\":true}");
  ASSERT_NE(nullptr, stream);
  ASSERT_TRUE(stream->reset("ab"));

  AssertTokenStreamContents(stream.get(), {"a", "ab"}, {0, 0}, {1, 2}, {1, 0});
}

TEST_F(EdgeNGramTokenizerTests, test_empty_input) {
  auto stream =
    analyzers::Get("edge_ngram", irs::Type<irs::text_format::Json>::get(),
                   "{\"min\":1, \"max\":2, \"preserveOriginal\":false}");
  ASSERT_NE(nullptr, stream);
  ASSERT_TRUE(stream->reset(""));

  AssertTokenStreamContents(stream.get(), {}, {}, {}, {});
}

TEST_F(EdgeNGramTokenizerTests, test_invalid_json) {
  {
    ASSERT_EQ(nullptr,
              analyzers::Get("edge_ngram",
                             irs::Type<irs::text_format::Json>::get(), ""));
    ASSERT_EQ(nullptr,
              analyzers::Get("edge_ngram",
                             irs::Type<irs::text_format::Json>::get(), "1"));
    ASSERT_EQ(nullptr,
              analyzers::Get("edge_ngram",
                             irs::Type<irs::text_format::Json>::get(), "[]"));
  }

  {
    ASSERT_EQ(nullptr, analyzers::Get(
                         "edge_ngram", irs::Type<irs::text_format::Json>::get(),
                         "{\"min\":3, \"max\":2, \"preserveOriginal\":false}"));
  }

  {
    ASSERT_EQ(nullptr, analyzers::Get(
                         "edge_ngram", irs::Type<irs::text_format::Json>::get(),
                         "{\"min\":-1, \"preserveOriginal\":false}"));
  }

  {
    ASSERT_EQ(
      nullptr,
      analyzers::Get("edge_ngram", irs::Type<irs::text_format::Json>::get(),
                     "{\"min\":\"1\", \"max\":2, \"preserveOriginal\":false}"));
    ASSERT_EQ(
      nullptr,
      analyzers::Get("edge_ngram", irs::Type<irs::text_format::Json>::get(),
                     "{\"min\":1, \"max\":\"2\", \"preserveOriginal\":false}"));
    ASSERT_EQ(
      nullptr,
      analyzers::Get("edge_ngram", irs::Type<irs::text_format::Json>::get(),
                     "{\"min\":1, \"max\":2, \"preserveOriginal\":\"yes\"}"));
  }
}

TEST_F(EdgeNGramTokenizerTests, test_load_json_minimal) {
  auto stream = analyzers::Get("edge_ngram",
                               irs::Type<irs::text_format::Json>::get(), "{}");
  ASSERT_NE(nullptr, stream);
  ASSERT_TRUE(stream->reset("ab"));
  AssertTokenStreamContents(stream.get(), {"a", "ab"}, {0, 0}, {1, 2}, {1, 0});
}

TEST_F(EdgeNGramTokenizerTests, test_normalize_json) {
  {
    std::string config =
      "{\"min\":1,\"max\":3,\"unknown\":true,\"preserveOriginal\":false}";
    std::string actual;
    ASSERT_TRUE(analyzers::Normalize(
      actual, "edge_ngram", irs::Type<irs::text_format::Json>::get(), config));
    ASSERT_EQ(vpack::Parser::fromJson(
                "{\"min\":1,\"max\":3,\"preserveOriginal\":false}")
                ->toString(),
              actual);
  }

  {
    std::string config = "{\"min\":0,\"max\":2,\"preserveOriginal\":false}";
    std::string actual;
    ASSERT_TRUE(analyzers::Normalize(
      actual, "edge_ngram", irs::Type<irs::text_format::Json>::get(), config));
    ASSERT_EQ(vpack::Parser::fromJson(
                "{\"min\":1,\"max\":2,\"preserveOriginal\":false}")
                ->toString(),
              actual);
  }

  {
    std::string config = "{\"min\":2,\"preserveOriginal\":true}";
    std::string actual;
    ASSERT_TRUE(analyzers::Normalize(
      actual, "edge_ngram", irs::Type<irs::text_format::Json>::get(), config));
    ASSERT_EQ(vpack::Parser::fromJson("{\"min\":2,\"preserveOriginal\":true}")
                ->toString(),
              actual);
  }
}

TEST_F(EdgeNGramTokenizerTests, test_normalize_vpack) {
  auto in = vpack::Parser::fromJson(
    "{\"min\":1,\"max\":4,\"extra\":null,\"preserveOriginal\":false}");
  std::string in_str;
  in_str.assign(in->slice().startAs<char>(), in->slice().byteSize());
  std::string out_str;
  ASSERT_TRUE(analyzers::Normalize(
    out_str, "edge_ngram", irs::Type<irs::text_format::VPack>::get(), in_str));
  vpack::Slice out_slice(reinterpret_cast<const uint8_t*>(out_str.c_str()));
  ASSERT_EQ(
    vpack::Parser::fromJson("{\"min\":1,\"max\":4,\"preserveOriginal\":false}")
      ->toString(),
    out_slice.toString());
}

TEST_F(EdgeNGramTokenizerTests, test_normalize_json_invalid) {
  std::string out;
  ASSERT_FALSE(analyzers::Normalize(
    out, "edge_ngram", irs::Type<irs::text_format::Json>::get(), ""));
  ASSERT_FALSE(analyzers::Normalize(
    out, "edge_ngram", irs::Type<irs::text_format::Json>::get(), "[]"));
}

}  // namespace irs::analysis
