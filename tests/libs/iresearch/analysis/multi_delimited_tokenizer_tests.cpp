////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2023 ArangoDB GmbH, Cologne, Germany
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
////////////////////////////////////////////////////////////////////////////////

#include "gtest/gtest.h"
#include "iresearch/analysis/batch/token_batch.hpp"
#include "iresearch/analysis/multi_delimited_tokenizer.hpp"
#include "tests_config.hpp"
#include "token_sink_utils.hpp"

using namespace irs::analysis;

namespace {

irs::bstring operator""_b(const char* ptr, size_t size) {
  return irs::bstring{reinterpret_cast<const irs::byte_type*>(ptr), size};
}

class MultiDelimitedTokenizerTests : public ::testing::Test {
 public:
  static void SetUpTestCase() {}

  void SetUp() final {
    // Code here will be called immediately after the constructor (right before
    // each test).
  }

  void TearDown() final {
    // Code here will be called immediately after each test (right before the
    // destructor).
  }
};

}  // namespace
namespace {

struct BlockTok {
  std::string_view term;
  uint32_t start;
  uint32_t end;
};

void AssertBlockTokens(irs::analysis::Tokenizer& stream, std::string_view data,
                       const std::vector<BlockTok>& expected) {
  size_t tok = 0;
  const auto check = [&](irs::TokenBatch& batch,
                         std::span<const irs::DocRun> /*runs*/) {
    ASSERT_TRUE(stream.Traits().dense_pos);
    for (uint32_t i = 0; i < batch.count; ++i, ++tok) {
      SCOPED_TRACE(testing::Message() << "token=" << tok);
      ASSERT_LT(tok, expected.size());
      const auto& t = batch.terms[i];
      ASSERT_EQ(expected[tok].term, std::string_view(t.GetData(), t.GetSize()));
      ASSERT_EQ(expected[tok].start, batch.offs_start[i]);
      ASSERT_EQ(expected[tok].end, batch.offs_end[i]);
    }
  };
  tests::FnTokenSink sink{irs::TokenLayout::TermsPosOffs, check};
  ASSERT_TRUE(stream.Fill(data, sink.writer, sink.layout));
  sink.writer.Finish();
  ASSERT_EQ(expected.size(), tok);
}

}  // namespace

TEST_F(MultiDelimitedTokenizerTests, consts) {
  static_assert("multi_delimiter" ==
                irs::Type<MultiDelimitedTokenizer>::name());
}

TEST_F(MultiDelimitedTokenizerTests, test_delimiter) {
  auto stream = MultiDelimitedTokenizer::Make({.delimiters = {"a"_b}});
  ASSERT_EQ(irs::Type<MultiDelimitedTokenizer>::id(), stream->type());
  AssertBlockTokens(*stream, "baccaad",
                    {{"b", 0, 1}, {"cc", 2, 4}, {"d", 6, 7}});
}

TEST_F(MultiDelimitedTokenizerTests, test_delimiter_empty_match) {
  auto stream = MultiDelimitedTokenizer::Make({.delimiters = {"."_b}});
  ASSERT_EQ(irs::Type<MultiDelimitedTokenizer>::id(), stream->type());
  AssertBlockTokens(*stream, "..", {});
}

TEST_F(MultiDelimitedTokenizerTests, test_delimiter_3) {
  auto stream =
    MultiDelimitedTokenizer::Make({.delimiters = {";"_b, ","_b, "|"_b}});
  ASSERT_EQ(irs::Type<MultiDelimitedTokenizer>::id(), stream->type());
  AssertBlockTokens(
    *stream, "a;b||c|d,ff",
    {{"a", 0, 1}, {"b", 2, 3}, {"c", 5, 6}, {"d", 7, 8}, {"ff", 9, 11}});
}

TEST_F(MultiDelimitedTokenizerTests, test_delimiter_5) {
  auto stream = MultiDelimitedTokenizer::Make(
    {.delimiters = {";"_b, ","_b, "|"_b, "."_b, ":"_b}});
  ASSERT_EQ(irs::Type<MultiDelimitedTokenizer>::id(), stream->type());
  AssertBlockTokens(
    *stream, "a:b||c.d,ff.",
    {{"a", 0, 1}, {"b", 2, 3}, {"c", 5, 6}, {"d", 7, 8}, {"ff", 9, 11}});
}

TEST_F(MultiDelimitedTokenizerTests, test_delimiter_single_long) {
  auto stream = MultiDelimitedTokenizer::Make({.delimiters = {"foo"_b}});
  ASSERT_EQ(irs::Type<MultiDelimitedTokenizer>::id(), stream->type());
  AssertBlockTokens(*stream, "foobarfoobazbarfoobar",
                    {{"bar", 3, 6}, {"bazbar", 9, 15}, {"bar", 18, 21}});
}

TEST_F(MultiDelimitedTokenizerTests, no_delimiter) {
  auto stream = MultiDelimitedTokenizer::Make({.delimiters = {}});
  ASSERT_EQ(irs::Type<MultiDelimitedTokenizer>::id(), stream->type());
  AssertBlockTokens(*stream, "foobar", {{"foobar", 0, 6}});
}

TEST_F(MultiDelimitedTokenizerTests, multi_words) {
  auto stream =
    MultiDelimitedTokenizer::Make({.delimiters = {"foo"_b, "bar"_b, "baz"_b}});
  ASSERT_EQ(irs::Type<MultiDelimitedTokenizer>::id(), stream->type());
  AssertBlockTokens(*stream, "fooxyzbarbazz", {{"xyz", 3, 6}, {"z", 12, 13}});
}

TEST_F(MultiDelimitedTokenizerTests, multi_words_2) {
  auto stream =
    MultiDelimitedTokenizer::Make({.delimiters = {"foo"_b, "bar"_b, "baz"_b}});
  ASSERT_EQ(irs::Type<MultiDelimitedTokenizer>::id(), stream->type());
  AssertBlockTokens(*stream, "foobarbaz", {});
}

TEST_F(MultiDelimitedTokenizerTests, trick_matching_1) {
  auto stream =
    MultiDelimitedTokenizer::Make({.delimiters = {"foo"_b, "ffa"_b}});
  ASSERT_EQ(irs::Type<MultiDelimitedTokenizer>::id(), stream->type());
  AssertBlockTokens(*stream, "abcffoobar", {{"abcf", 0, 4}, {"bar", 7, 10}});
}

TEST_F(MultiDelimitedTokenizerTests, construct) {
  // happy path -- two distinct delimiters.
  {
    auto stream = MultiDelimitedTokenizer::Make({.delimiters = {"a"_b, "b"_b}});
    ASSERT_NE(nullptr, stream);
    AssertBlockTokens(*stream, "aib", {{"i", 1, 2}});
  }

  // .........................................................................
  // The old JSON suite covered "wrong name" (registry asked for a non-existent
  // type) and "wrong type" (`"delimiters": 1`). Both are parser-/registry-
  // level failures with no direct-API analogue. The remaining valid construct
  // case is the default-Options happy path -- an empty delimiters list still
  // produces a valid (degenerate) analyzer.
  // .........................................................................
  {
    auto stream =
      MultiDelimitedTokenizer::Make(MultiDelimitedTokenizer::Options{});
    ASSERT_NE(nullptr, stream);
  }
}

TEST_F(MultiDelimitedTokenizerTests, native_fills_match_pull) {
  const std::vector<std::vector<irs::bstring>> delim_sets = {
    {},
    {"a"_b},
    {";"_b, ","_b},
    {";"_b, ","_b, "|"_b, "."_b, ":"_b},
    {"0"_b, "1"_b, "2"_b, "3"_b, "4"_b, "5"_b, "6"_b, "7"_b, "8"_b},
    {"foo"_b},
    {"foo"_b, "bar"_b, "baz"_b}};

  std::string huge;
  for (size_t i = 0; i < 1500; ++i) {
    huge += "w" + std::to_string(i) + ";";
  }
  const std::vector<std::string> values = {
    "",
    ";;;",
    "a;b,c|d.e:f",
    "plain",
    ";lead",
    "trail;",
    huge,
    "foobarfoobazbar",
    std::string(100, 'q') + ";" + std::string(50, 'r')};

  for (const auto& delims : delim_sets) {
    auto pull_stream = MultiDelimitedTokenizer::Make({.delimiters = {delims}});
    auto fill_stream = MultiDelimitedTokenizer::Make({.delimiters = {delims}});
    for (const auto& v : values) {
      SCOPED_TRACE(testing::Message()
                   << "delims=" << delims.size() << " value.size=" << v.size());
      auto pulled = tests::Analyze(*pull_stream, v);
      ASSERT_TRUE(pulled.has_value());
      std::vector<BlockTok> expected;
      std::vector<std::string> storage;
      storage.reserve(pulled->size());
      for (auto& t : *pulled) {
        storage.emplace_back(std::move(t.term));
        expected.push_back({storage.back(), t.offs_start, t.offs_end});
      }
      AssertBlockTokens(*fill_stream, v, expected);
    }
  }
}
