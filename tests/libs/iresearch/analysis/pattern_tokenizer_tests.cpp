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

#include <vector>

#include "gtest/gtest.h"
#include "iresearch/analysis/batch/token_batch.hpp"
#include "iresearch/analysis/pattern_tokenizer.hpp"
#include "tests_config.hpp"
#include "token_sink_utils.hpp"

namespace {

class PatternTokenizerTests : public ::testing::Test {};

void AssertTokenStreamContents(
  irs::analysis::Tokenizer* stream, std::string_view data,
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

  size_t token_idx = 0;
  const auto check = [&](irs::TokenBatch& batch,
                         std::span<const irs::DocRun> /*runs*/) {
    ASSERT_TRUE(batch.dense_pos);
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

}  // namespace

TEST_F(PatternTokenizerTests, consts) {
  static_assert("pattern" ==
                irs::Type<irs::analysis::PatternTokenizer>::name());
}

TEST_F(PatternTokenizerTests, test_split_mode) {
  std::string_view data("foo,bar,baz");
  irs::analysis::PatternTokenizer stream(",", -1);
  ASSERT_EQ(irs::Type<irs::analysis::PatternTokenizer>::id(), stream.type());

  AssertTokenStreamContents(&stream, data, {"foo", "bar", "baz"}, {0, 4, 8},
                            {3, 7, 11}, {1, 1, 1});
}

TEST_F(PatternTokenizerTests, test_split_whitespace) {
  std::string_view data("hello world test");
  irs::analysis::PatternTokenizer stream("\\s+", -1);

  AssertTokenStreamContents(&stream, data, {"hello", "world", "test"},
                            {0, 6, 12}, {5, 11, 16}, {1, 1, 1});
}

TEST_F(PatternTokenizerTests, test_group_extraction_0) {
  std::string_view data("aaa 'bbb' 'ccc'");
  irs::analysis::PatternTokenizer stream("'([^']+)'", 0);

  AssertTokenStreamContents(&stream, data, {"'bbb'", "'ccc'"}, {4, 10}, {9, 15},
                            {1, 1});
}

TEST_F(PatternTokenizerTests, test_group_extraction_0_match) {
  std::string_view data("'aaa' bbb 'ccc' 'ddd'");
  irs::analysis::PatternTokenizer stream("'(?:\\w*)'", 0);

  AssertTokenStreamContents(&stream, data, {"'aaa'", "'ccc'", "'ddd'"},
                            {0, 10, 16}, {5, 15, 21}, {1, 1, 1});
}

TEST_F(PatternTokenizerTests, test_group_extraction_1) {
  std::string_view data("aaa 'bbb' 'ccc'");
  irs::analysis::PatternTokenizer stream("'([^']+)'", 1);

  AssertTokenStreamContents(&stream, data, {"bbb", "ccc"}, {5, 11}, {8, 14},
                            {1, 1});
}

TEST_F(PatternTokenizerTests, test_digits_extraction) {
  std::string_view data("foo123bar456baz789");
  irs::analysis::PatternTokenizer stream("([0-9]+)", 1);

  AssertTokenStreamContents(&stream, data, {"123", "456", "789"}, {3, 9, 15},
                            {6, 12, 18}, {1, 1, 1});
}

TEST_F(PatternTokenizerTests, test_empty_input) {
  std::string_view data("");
  irs::analysis::PatternTokenizer stream(",", -1);

  AssertTokenStreamContents(&stream, data, {}, {}, {});
}

TEST_F(PatternTokenizerTests, test_no_match) {
  std::string_view data("hello world");
  irs::analysis::PatternTokenizer stream("[0-9]+", 0);

  AssertTokenStreamContents(&stream, data, {}, {}, {});
}

TEST_F(PatternTokenizerTests, test_bad_regex) {
  // Invalid regex should make analyzer construction fail
  ASSERT_ANY_THROW(irs::analysis::PatternTokenizer::Make(
    irs::analysis::PatternTokenizer::Options{.pattern = "(", .group = -1}));
  ASSERT_ANY_THROW(irs::analysis::PatternTokenizer::Make(
    irs::analysis::PatternTokenizer::Options{.pattern = "(", .group = 1}));
}

TEST_F(PatternTokenizerTests, test_reset) {
  irs::analysis::PatternTokenizer stream(",", -1);

  std::string_view data1("a,b");
  AssertTokenStreamContents(&stream, data1, {"a", "b"}, {0, 2}, {1, 3}, {1, 1});

  std::string_view data2("x,y,z");
  AssertTokenStreamContents(&stream, data2, {"x", "y", "z"}, {0, 2, 4},
                            {1, 3, 5}, {1, 1, 1});
}

TEST_F(PatternTokenizerTests, test_reset_reuse_different_inputs) {
  {
    irs::analysis::PatternTokenizer stream(",", -1);
    AssertTokenStreamContents(&stream, "a,b", {"a", "b"}, {0, 2}, {1, 3},
                              {1, 1});

    AssertTokenStreamContents(&stream, "c,d,e", {"c", "d", "e"}, {0, 2, 4},
                              {1, 3, 5}, {1, 1, 1});
  }

  {
    irs::analysis::PatternTokenizer stream("'([^']+)'", 1);
    AssertTokenStreamContents(&stream, "a 'foo'", {"foo"}, {3}, {6}, {1});

    AssertTokenStreamContents(&stream, "b 'bar' c 'baz'", {"bar", "baz"},
                              {3, 11}, {6, 14}, {1, 1});
  }
}

TEST_F(PatternTokenizerTests, test_splitting_double_dash) {
  std::string_view data("aaa--bbb--ccc");
  irs::analysis::PatternTokenizer stream("--", -1);

  AssertTokenStreamContents(&stream, data, {"aaa", "bbb", "ccc"}, {0, 5, 10},
                            {3, 8, 13}, {1, 1, 1});
}

TEST_F(PatternTokenizerTests, test_splitting_colon) {
  std::string_view data("aaa:bbb:ccc");
  irs::analysis::PatternTokenizer stream(":", -1);

  AssertTokenStreamContents(&stream, data, {"aaa", "bbb", "ccc"}, {0, 4, 8},
                            {3, 7, 11}, {1, 1, 1});
}

TEST_F(PatternTokenizerTests, test_splitting_multi_space_and_tabs) {
  std::string_view data("aaa   bbb \t\tccc  ");
  irs::analysis::PatternTokenizer stream("\\s+", -1);

  AssertTokenStreamContents(&stream, data, {"aaa", "bbb", "ccc"}, {0, 6, 12},
                            {3, 9, 15}, {1, 1, 1});
}

TEST_F(PatternTokenizerTests, test_splitting_single_char) {
  std::string_view data("boo:and:foo");
  irs::analysis::PatternTokenizer stream("o", -1);

  AssertTokenStreamContents(&stream, data, {"b", ":and:f"}, {0, 3}, {1, 9},
                            {1, 1});
}

TEST_F(PatternTokenizerTests, test_group_zero_matches) {
  std::string_view data("boo:and:foo");
  irs::analysis::PatternTokenizer stream(":", 0);

  AssertTokenStreamContents(&stream, data, {":", ":"}, {3, 7}, {4, 8}, {1, 1});
}

TEST_F(PatternTokenizerTests, test_offset_with_complex_pattern) {
  std::string_view data("hello world test");
  irs::analysis::PatternTokenizer stream("[,;/\\s]+", -1);

  AssertTokenStreamContents(&stream, data, {"hello", "world", "test"},
                            {0, 6, 12}, {5, 11, 16}, {1, 1, 1});
}

TEST_F(PatternTokenizerTests, test_consecutive_delimiters) {
  std::string_view data("a,,b");
  irs::analysis::PatternTokenizer stream(",", -1);

  AssertTokenStreamContents(&stream, data, {"a", "b"}, {0, 3}, {1, 4}, {1, 1});
}

TEST_F(PatternTokenizerTests, test_delimiter_at_boundaries) {
  std::string_view data(",hello,world,");
  irs::analysis::PatternTokenizer stream(",", -1);

  AssertTokenStreamContents(&stream, data, {"hello", "world"}, {1, 7}, {6, 12},
                            {1, 1});
}

TEST_F(PatternTokenizerTests, test_utf8_split_comma_cyrillic) {
  // "аба" = 6 UTF-8 bytes, comma, "цаба" = 8 bytes -> total 15.
  std::string_view data("аба,цаба");
  irs::analysis::PatternTokenizer stream(",", -1);

  AssertTokenStreamContents(&stream, data, {"аба", "цаба"}, {0, 7}, {6, 15},
                            {1, 1});
}

TEST_F(PatternTokenizerTests, test_utf8_split_whitespace_mixed_ascii_cjk) {
  // "a" (1) + space + "汉字" (3+3 bytes) + space + "b" (1) = 10 bytes
  std::string_view data("a 汉字 b");
  irs::analysis::PatternTokenizer stream("\\s+", -1);

  AssertTokenStreamContents(&stream, data, {"a", "汉字", "b"}, {0, 2, 9},
                            {1, 8, 10}, {1, 1, 1});
}

TEST_F(PatternTokenizerTests, test_utf8_group_capture_inside_quotes) {
  std::string_view data("x '汉字' z");
  irs::analysis::PatternTokenizer stream("'([^']+)'", 1);

  AssertTokenStreamContents(&stream, data, {"汉字"}, {3}, {9}, {1});
}

TEST_F(PatternTokenizerTests, test_utf8_split_comma_4byte_emoji) {
  // U+1F642 = 0xF0 0x9F 0x98 0x8A
  std::string_view emoji = "\xF0\x9F\x98\x8A";
  const std::string data(std::string("a,") + emoji);
  irs::analysis::PatternTokenizer stream(",", -1);

  AssertTokenStreamContents(&stream, data, {"a", emoji}, {0, 2}, {1, 6},
                            {1, 1});
}

TEST_F(PatternTokenizerTests, test_make_options) {
  // Valid: pattern only, default group -1 (split mode)
  {
    auto stream = irs::analysis::PatternTokenizer::Make(
      irs::analysis::PatternTokenizer::Options{.pattern = ",", .group = -1});
    ASSERT_NE(nullptr, stream);

    std::string_view data("a,b,c");
    AssertTokenStreamContents(stream.get(), data, {"a", "b", "c"}, {0, 2, 4},
                              {1, 3, 5}, {1, 1, 1});
  }

  // Valid: pattern + group 1 (extract first capturing group)
  {
    auto stream = irs::analysis::PatternTokenizer::Make(
      irs::analysis::PatternTokenizer::Options{.pattern = "'([^']+)'",
                                               .group = 1});
    ASSERT_NE(nullptr, stream);

    std::string_view data("a 'foo' b 'bar'");
    AssertTokenStreamContents(stream.get(), data, {"foo", "bar"}, {3, 11},
                              {6, 14}, {1, 1});
  }

  // Valid: group omitted -> default -1
  {
    auto stream = irs::analysis::PatternTokenizer::Make(
      irs::analysis::PatternTokenizer::Options{.pattern = ":"});
    ASSERT_NE(nullptr, stream);

    std::string_view data("a:b:c");
    AssertTokenStreamContents(stream.get(), data, {"a", "b", "c"}, {0, 2, 4},
                              {1, 3, 5}, {1, 1, 1});
  }

  // Invalid: empty pattern -- ported from the legacy "empty pattern
  // string" / "missing pattern key" / "empty object" JSON-parse
  // rejections. With the direct-Options API, the absence of a pattern
  // collapses to a default-initialized empty `pattern` field.
  ASSERT_ANY_THROW(irs::analysis::PatternTokenizer::Make(
    irs::analysis::PatternTokenizer::Options{}));
  ASSERT_ANY_THROW(irs::analysis::PatternTokenizer::Make(
    irs::analysis::PatternTokenizer::Options{.pattern = ""}));
  ASSERT_ANY_THROW(irs::analysis::PatternTokenizer::Make(
    irs::analysis::PatternTokenizer::Options{.group = 0}));

  // Legacy JSON-parser cases with no direct-API analogue: the typed
  // Options struct makes "pattern not a string", "not an object",
  // "group wrong type", "group is non-integer number", "group out of
  // int range", and "unknown fields ignored" all compile-time
  // impossibilities. Documenting here so future readers see the
  // assertions intentionally collapsed.
}

TEST_F(PatternTokenizerTests, native_fills_match_pull) {
  std::string huge;
  for (size_t i = 0; i < 1500; ++i) {
    huge += "w" + std::to_string(i) + ",";
  }
  const std::vector<std::pair<std::string, int>> configs = {
    {",", -1},  {"\\s+", -1}, {"(\\d+)", 0},       {"(\\d+)", 1},
    {"a*", -1}, {"x?", -1},   {"(\\w+)@(\\w+)", 2}};
  const std::vector<std::string> values = {"",
                                           "foo,bar,baz",
                                           "no delims here at all whatsoever",
                                           "a1 b22 c333",
                                           "user@host other@domain",
                                           "aaaa",
                                           ",,,",
                                           huge};

  for (const auto& [pattern, group] : configs) {
    irs::analysis::PatternTokenizer pull_stream(pattern, group);
    irs::analysis::PatternTokenizer fill_stream(pattern, group);
    for (const auto& v : values) {
      SCOPED_TRACE(testing::Message() << "pattern=" << pattern << " group="
                                      << group << " value.size=" << v.size());
      auto pulled_tokens = tests::Analyze(pull_stream, v);
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
      ASSERT_TRUE(fill_stream.Fill(v, sink.writer, sink.layout));
      sink.writer.Finish();

      ASSERT_EQ(pulled, filled);
      ASSERT_EQ(pstarts, fstarts);
      ASSERT_EQ(pends, fends);
    }
  }
}

namespace {

std::vector<tests::AnalyzerToken> PatternAnalyzeWith(std::string_view pattern,
                                                     std::string_view value,
                                                     bool force_regex) {
  auto stream = irs::analysis::PatternTokenizer::Make(
    irs::analysis::PatternTokenizer::Options{.pattern = std::string{pattern}});
  auto* pat = dynamic_cast<irs::analysis::PatternTokenizer*>(stream.get());
  EXPECT_NE(nullptr, pat);
  pat->ForceRegexPath(force_regex);
  auto tokens = tests::Analyze(*stream, value);
  EXPECT_TRUE(tokens.has_value());
  return std::move(*tokens);
}

void AssertPatternFastMatchesRegex(std::string_view pattern,
                                   std::string_view value) {
  const auto slow = PatternAnalyzeWith(pattern, value, true);
  const auto fast = PatternAnalyzeWith(pattern, value, false);
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

TEST(PatternTokenizerFastSplit, eligibility) {
  const auto eligible = [](std::string_view pattern) {
    auto stream = irs::analysis::PatternTokenizer::Make(
      irs::analysis::PatternTokenizer::Options{.pattern =
                                                 std::string{pattern}});
    return dynamic_cast<irs::analysis::PatternTokenizer*>(stream.get())
      ->FastSplitEligible();
  };
  ASSERT_TRUE(eligible(","));
  ASSERT_TRUE(eligible("\\s+"));
  ASSERT_TRUE(eligible("[,;]+"));
  ASSERT_TRUE(eligible("[a-c]"));
  ASSERT_TRUE(eligible(" +"));
  ASSERT_TRUE(eligible("::"));
  ASSERT_TRUE(eligible(", "));
  ASSERT_TRUE(eligible("--"));
  ASSERT_FALSE(eligible("(?i)::"));
  ASSERT_FALSE(
    eligible("a\xc2\xa7"
             "b"));
  ASSERT_TRUE(eligible("ab"));
  ASSERT_FALSE(eligible(",*"));
  ASSERT_FALSE(eligible("(,)"));
  ASSERT_FALSE(eligible(",|;;"));
  ASSERT_FALSE(eligible("\\p{L}"));
  ASSERT_FALSE(eligible("x?"));
}

TEST(PatternTokenizerFastSplit, property_oracle) {
  const std::vector<std::string> patterns = {
    ",", "\\s+", "[,;]+", "[a-c]", " +", ";", "::", ", ", "aa", "--"};
  uint64_t seed = 0xfa57;
  const auto next = [&] {
    seed = seed * 6364136223846793005ULL + 1442695040888963407ULL;
    return static_cast<size_t>(seed >> 33);
  };
  constexpr std::string_view kCharset = "abcdefgh ,;\t\nxyz0123  ,,;;";
  for (const auto& pattern : patterns) {
    for (const std::string_view v :
         {std::string_view{""}, std::string_view{","}, std::string_view{",,"},
          std::string_view{"x"}, std::string_view{",x,"},
          std::string_view{"a b,c;d"}}) {
      SCOPED_TRACE(testing::Message()
                   << "pattern=" << pattern << " value=\"" << v << "\"");
      AssertPatternFastMatchesRegex(pattern, v);
    }
    for (size_t iter = 0; iter < 200; ++iter) {
      std::string v;
      const size_t len = next() % 80;
      for (size_t i = 0; i < len; ++i) {
        v += kCharset[next() % kCharset.size()];
      }
      SCOPED_TRACE(testing::Message() << "pattern=" << pattern << " iter="
                                      << iter << " value=\"" << v << "\"");
      AssertPatternFastMatchesRegex(pattern, v);
    }
  }
}
