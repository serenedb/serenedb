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
#include "iresearch/analysis/multi_delimited_tokenizer.hpp"
#include "iresearch/analysis/token_batch.hpp"
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
    ASSERT_FALSE(stream.Traits().explicit_pos);
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
  ASSERT_TRUE(stream.Fill(tests::ToStringT(data), sink.writer, {sink.layout}));
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
  ASSERT_TRUE(stream->Traits().stable);
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

TEST_F(MultiDelimitedTokenizerTests, single_long_needle_over_bm_threshold) {
  auto stream =
    MultiDelimitedTokenizer::Make({.delimiters = {"|====SPLIT====|"_b}});
  ASSERT_TRUE(stream->Traits().stable);
  AssertBlockTokens(*stream, "aa|====SPLIT====|bb",
                    {{"aa", 0, 2}, {"bb", 17, 19}});
  AssertBlockTokens(*stream, "|====SPLIT====||====SPLIT====|", {});
  AssertBlockTokens(*stream, "|====SPLIT====", {{"|====SPLIT====", 0, 14}});
}

TEST_F(MultiDelimitedTokenizerTests, match_completes_across_failed_prefix) {
  auto stream =
    MultiDelimitedTokenizer::Make({.delimiters = {"abd"_b, "bc"_b}});
  AssertBlockTokens(*stream, "1abc2", {{"1a", 0, 2}, {"2", 4, 5}});
}

TEST_F(MultiDelimitedTokenizerTests, match_completes_at_value_end) {
  auto stream = MultiDelimitedTokenizer::Make({.delimiters = {"aXb"_b, "X"_b}});
  AssertBlockTokens(*stream, "aX", {{"a", 0, 1}});
  AssertBlockTokens(*stream, "aXb", {});
  AssertBlockTokens(*stream, "zXz", {{"z", 0, 1}, {"z", 2, 3}});
}

TEST_F(MultiDelimitedTokenizerTests, high_byte_single_char_delimiters) {
  auto few = MultiDelimitedTokenizer::Make({.delimiters = {"\xFF"_b, ";"_b}});
  AssertBlockTokens(*few,
                    "a\xFF"
                    "b;c",
                    {{"a", 0, 1}, {"b", 2, 3}, {"c", 4, 5}});
  const std::string xs(40, 'x');
  const std::string ys(40, 'y');
  AssertBlockTokens(*few, xs + "\xFF" + ys, {{xs, 0, 40}, {ys, 41, 81}});

  auto many = MultiDelimitedTokenizer::Make(
    {.delimiters = {"\xFF"_b, "\x80"_b, "0"_b, "1"_b, "2"_b, "3"_b, "4"_b,
                    "5"_b, "6"_b, "7"_b, "8"_b}});
  AssertBlockTokens(*many,
                    "a\x80"
                    "b\xFF"
                    "c7d",
                    {{"a", 0, 1}, {"b", 2, 3}, {"c", 4, 5}, {"d", 6, 7}});
}

TEST_F(MultiDelimitedTokenizerTests, multi_string_matches_one_string) {
  auto one = MultiDelimitedTokenizer::Make({.delimiters = {", "_b}});
  auto multi =
    MultiDelimitedTokenizer::Make({.delimiters = {", "_b, "\x01\x01"_b}});
  constexpr std::string_view kData = "foo, bar,baz, , qux, ";
  const auto expected = tests::AnalyzeTerms(*one, kData);
  const auto actual = tests::AnalyzeTerms(*multi, kData);
  ASSERT_TRUE(expected.has_value());
  ASSERT_TRUE(actual.has_value());
  ASSERT_EQ(*expected, *actual);
  ASSERT_EQ((std::vector<std::string>{"foo", "bar,baz", "qux"}), *actual);
}

TEST_F(MultiDelimitedTokenizerTests,
       multi_string_candidates_beyond_first_block) {
  {
    auto stream =
      MultiDelimitedTokenizer::Make({.delimiters = {", "_b, "; "_b}});
    AssertBlockTokens(*stream, "aaaaaaaaaa, bbbbbbbbbb; cc,ccdd, eeee; f",
                      {{"aaaaaaaaaa", 0, 10},
                       {"bbbbbbbbbb", 12, 22},
                       {"cc,ccdd", 24, 31},
                       {"eeee", 33, 37},
                       {"f", 39, 40}});
  }
  {
    auto stream = MultiDelimitedTokenizer::Make(
      {.delimiters = {", "_b, "; "_b, ": "_b, " - "_b, " | "_b, "\t"_b, "--"_b,
                      "\r\n"_b}});
    const std::string xs(32, 'x');
    const std::string data = xs + ", a, b; c: d - e | f\tg--h\r\ni";
    AssertBlockTokens(*stream, data,
                      {{xs, 0, 32},
                       {"a", 34, 35},
                       {"b", 37, 38},
                       {"c", 40, 41},
                       {"d", 43, 44},
                       {"e", 47, 48},
                       {"f", 51, 52},
                       {"g", 53, 54},
                       {"h", 56, 57},
                       {"i", 59, 60}});
  }
  {
    auto stream = MultiDelimitedTokenizer::Make(
      {.delimiters = {",,"_b, ";;"_b, "::"_b, "!!"_b, "??"_b, ".."_b, "||"_b,
                      "&&"_b, "##"_b}});
    AssertBlockTokens(*stream, "x,,y;;z::w!!v??u..t||s&&r##q",
                      {{"x", 0, 1},
                       {"y", 3, 4},
                       {"z", 6, 7},
                       {"w", 9, 10},
                       {"v", 12, 13},
                       {"u", 15, 16},
                       {"t", 18, 19},
                       {"s", 21, 22},
                       {"r", 24, 25},
                       {"q", 27, 28}});
  }
}

TEST_F(MultiDelimitedTokenizerTests, delimiter_inside_another_path) {
  auto stream =
    MultiDelimitedTokenizer::Make({.delimiters = {"ab"_b, "xabc"_b}});

  AssertBlockTokens(*stream, "1xab2", {{"1x", 0, 2}, {"2", 4, 5}});
}

TEST_F(MultiDelimitedTokenizerTests, delimiter_inside_another_path_deep) {
  auto stream =
    MultiDelimitedTokenizer::Make({.delimiters = {"cd"_b, "abcde"_b}});

  AssertBlockTokens(*stream, "xabcdy", {{"xab", 0, 3}, {"y", 5, 6}});
}

// Two delimiters ending on the same byte: the one that started earlier wins, so
// the shorter suffix must not pre-empt it.
TEST_F(MultiDelimitedTokenizerTests, delimiters_ending_together_leftmost_wins) {
  auto stream =
    MultiDelimitedTokenizer::Make({.delimiters = {"bc"_b, "abc"_b}});

  AssertBlockTokens(*stream, "1abc2", {{"1", 0, 1}, {"2", 4, 5}});
}

// A chain where each delimiter is a suffix of the next; the longest, which
// started first, is the match.
TEST_F(MultiDelimitedTokenizerTests, delimiter_chain_of_suffixes) {
  auto stream =
    MultiDelimitedTokenizer::Make({.delimiters = {"c"_b, "bc"_b, "abc"_b}});

  AssertBlockTokens(*stream, "zabcz", {{"z", 0, 1}, {"z", 4, 5}});
}

TEST_F(MultiDelimitedTokenizerTests,
       multi_string_scan_continues_inside_and_across_blocks) {
  auto stream =
    MultiDelimitedTokenizer::Make({.delimiters = {"<br>"_b, "</p>"_b}});
  const std::string x28(28, 'x');
  const std::string x29(29, 'x');
  const std::string x30(30, 'x');
  const std::string x31(31, 'x');
  const std::string c26 = "c" + std::string(25, 'x');
  const std::string x30b = x30 + "<b";
  const std::string x31i = x31 + "<i>";

  AssertBlockTokens(*stream, x29 + "<br>ab</p>cd",
                    {{x29, 0, 29}, {"ab", 33, 35}, {"cd", 39, 41}});
  AssertBlockTokens(*stream, "a<br>b</p>" + c26,
                    {{"a", 0, 1}, {"b", 5, 6}, {c26, 10, 36}});
  AssertBlockTokens(*stream, x28 + "<br>abc", {{x28, 0, 28}, {"abc", 32, 35}});
  AssertBlockTokens(*stream, x30b, {{x30b, 0, 32}});
  AssertBlockTokens(*stream, x30 + "<br>y</p>", {{x30, 0, 30}, {"y", 34, 35}});
  AssertBlockTokens(*stream, x31 + "<br>z", {{x31, 0, 31}, {"z", 35, 36}});
  AssertBlockTokens(*stream, x31i + "</p>q", {{x31i, 0, 34}, {"q", 38, 39}});
}

TEST_F(MultiDelimitedTokenizerTests, multi_string_prefix_tiers_verify_exactly) {
  auto stream = MultiDelimitedTokenizer::Make(
    {.delimiters = {"</sect01>"_b, "</sect02>"_b, "</long05>"_b, "<meta02>"_b,
                    "</section>"_b, "</sectio>"_b}});
  AssertBlockTokens(
    *stream, "a</sect01xb</sect02>c<meta02>d</sectio>e</section>f</long05>g",
    {{"a</sect01xb", 0, 11},
     {"c", 20, 21},
     {"d", 29, 30},
     {"e", 39, 40},
     {"f", 50, 51},
     {"g", 60, 61}});
  AssertBlockTokens(*stream, "</sect0", {{"</sect0", 0, 7}});
  AssertBlockTokens(*stream, "<meta02>", {});

  auto tags =
    MultiDelimitedTokenizer::Make({.delimiters = {"</b>"_b, "</i>"_b}});
  AssertBlockTokens(*tags, "xy</b>", {{"xy", 0, 2}});
  AssertBlockTokens(*tags, "ab</", {{"ab</", 0, 4}});
  const std::string x30(30, 'x');
  AssertBlockTokens(*tags, x30 + "</b>q", {{x30, 0, 30}, {"q", 34, 35}});
  AssertBlockTokens(*tags, x30 + "</i", {{x30 + "</i", 0, 33}});
}

TEST_F(MultiDelimitedTokenizerTests,
       many_chars_delimiters_in_the_overlapping_tail_block) {
  auto stream =
    MultiDelimitedTokenizer::Make({.delimiters = {","_b, ";"_b, "|"_b}});
  const std::string x31(31, 'x');
  const std::string x32(32, 'x');
  AssertBlockTokens(*stream, x31 + ";yy|z",
                    {{x31, 0, 31}, {"yy", 32, 34}, {"z", 35, 36}});
  AssertBlockTokens(*stream, x32 + ",y", {{x32, 0, 32}, {"y", 33, 34}});
  AssertBlockTokens(*stream, x32 + ",,", {{x32, 0, 32}});
  auto one = MultiDelimitedTokenizer::Make({.delimiters = {","_b}});
  AssertBlockTokens(*one, x31 + ",yy,z",
                    {{x31, 0, 31}, {"yy", 32, 34}, {"z", 35, 36}});
}

TEST_F(MultiDelimitedTokenizerTests, construct) {
  // happy path -- two distinct delimiters.
  {
    auto stream = MultiDelimitedTokenizer::Make({.delimiters = {"a"_b, "b"_b}});
    ASSERT_NE(nullptr, stream);
    AssertBlockTokens(*stream, "aib", {{"i", 1, 2}});
  }

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
    {"\xFF"_b, ";"_b},
    {"\xFF"_b, "\x80"_b, "0"_b, "1"_b, "2"_b, "3"_b, "4"_b, "5"_b, "6"_b},
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
    "a\xFF"
    "b\x80"
    "c;d",
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

TEST_F(MultiDelimitedTokenizerTests, column_fill_matches_per_value) {
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

  struct Tok {
    std::string term;
    uint32_t start;
    uint32_t end;
    bool operator==(const Tok&) const = default;
  };

  for (const auto& delims : delim_sets) {
    auto column_stream =
      MultiDelimitedTokenizer::Make({.delimiters = {delims}});
    auto per_value = MultiDelimitedTokenizer::Make({.delimiters = {delims}});

    std::vector<std::vector<Tok>> expected(values.size());
    for (size_t v = 0; v < values.size(); ++v) {
      auto tokens = tests::Analyze(*per_value, values[v]);
      ASSERT_TRUE(tokens.has_value());
      for (auto& t : *tokens) {
        expected[v].push_back({std::move(t.term), t.offs_start, t.offs_end});
      }
    }

    std::vector<duckdb::string_t> vals;
    for (size_t i = 0; i < values.size(); ++i) {
      vals.emplace_back(values[i].data(),
                        static_cast<uint32_t>(values[i].size()));
    }
    std::vector<std::vector<Tok>> got(values.size());
    const auto collect = [&](irs::TokenBatch& batch,
                             std::span<const irs::DocRun> runs) {
      uint32_t tok = 0;
      for (const auto& run : runs) {
        for (uint32_t j = 0; j < run.ntokens; ++j, ++tok) {
          const auto& t = batch.terms[tok];
          got[run.doc - 1].push_back({std::string{t.GetData(), t.GetSize()},
                                      batch.offs_start[tok],
                                      batch.offs_end[tok]});
        }
      }
    };
    tests::FnTokenSink sink{irs::TokenLayout::TermsPosOffs, collect};
    tests::FillColumn(*column_stream, vals, 1, sink.writer, sink.layout);
    sink.writer.Finish();

    for (size_t v = 0; v < values.size(); ++v) {
      SCOPED_TRACE(testing::Message()
                   << "delims=" << delims.size() << " doc=" << v + 1);
      ASSERT_EQ(expected[v], got[v]);
    }
  }
}
