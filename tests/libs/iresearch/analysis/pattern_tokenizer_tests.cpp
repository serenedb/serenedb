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

#include <array>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

#include "gtest/gtest.h"
#include "iresearch/analysis/pattern_tokenizer.hpp"
#include "iresearch/analysis/token_batch.hpp"
#include "tests_config.hpp"
#include "token_sink_utils.hpp"

namespace delim = irs::analysis::delim;

namespace {

class PatternTokenizerTests : public ::testing::Test {};

irs::analysis::Tokenizer::ptr MakePattern(std::string_view pattern,
                                          int group = -1) {
  return irs::analysis::PatternTokenizer::Make(
    {.pattern = std::string{pattern}, .group = group});
}

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
    ASSERT_FALSE(stream->Traits().explicit_pos);
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
  ASSERT_TRUE(stream->Fill(tests::ToStringT(data), sink.writer, {sink.layout}));
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
  auto stream = MakePattern(",");
  ASSERT_EQ(irs::Type<irs::analysis::PatternTokenizer>::id(), stream->type());

  AssertTokenStreamContents(stream.get(), data, {"foo", "bar", "baz"},
                            {0, 4, 8}, {3, 7, 11}, {1, 1, 1});
}

TEST_F(PatternTokenizerTests, test_split_whitespace) {
  std::string_view data("hello world test");
  auto stream = MakePattern("\\s+");

  AssertTokenStreamContents(stream.get(), data, {"hello", "world", "test"},
                            {0, 6, 12}, {5, 11, 16}, {1, 1, 1});
}

TEST_F(PatternTokenizerTests, test_group_extraction_0) {
  std::string_view data("aaa 'bbb' 'ccc'");
  auto stream = MakePattern("'([^']+)'", 0);
  ASSERT_EQ(irs::Type<irs::analysis::PatternTokenizer>::id(), stream->type());

  AssertTokenStreamContents(stream.get(), data, {"'bbb'", "'ccc'"}, {4, 10},
                            {9, 15}, {1, 1});
}

TEST_F(PatternTokenizerTests, test_group_extraction_0_match) {
  std::string_view data("'aaa' bbb 'ccc' 'ddd'");
  auto stream = MakePattern("'(?:\\w*)'", 0);

  AssertTokenStreamContents(stream.get(), data, {"'aaa'", "'ccc'", "'ddd'"},
                            {0, 10, 16}, {5, 15, 21}, {1, 1, 1});
}

TEST_F(PatternTokenizerTests, test_group_extraction_1) {
  std::string_view data("aaa 'bbb' 'ccc'");
  auto stream = MakePattern("'([^']+)'", 1);

  AssertTokenStreamContents(stream.get(), data, {"bbb", "ccc"}, {5, 11},
                            {8, 14}, {1, 1});
}

TEST_F(PatternTokenizerTests, test_digits_extraction) {
  std::string_view data("foo123bar456baz789");
  auto stream = MakePattern("([0-9]+)", 1);

  AssertTokenStreamContents(stream.get(), data, {"123", "456", "789"},
                            {3, 9, 15}, {6, 12, 18}, {1, 1, 1});
}

TEST_F(PatternTokenizerTests, test_capture_group_zero_uses_whole_match) {
  std::string_view data("a12b3");
  auto stream = MakePattern("(\\d+)", 0);

  AssertTokenStreamContents(stream.get(), data, {"12", "3"}, {1, 4}, {3, 5},
                            {1, 1});
}

TEST_F(PatternTokenizerTests, test_empty_input) {
  std::string_view data("");
  auto stream = MakePattern(",");

  AssertTokenStreamContents(stream.get(), data, {}, {}, {});
}

TEST_F(PatternTokenizerTests, test_no_match) {
  std::string_view data("hello world");
  auto stream = MakePattern("[0-9]+", 0);

  AssertTokenStreamContents(stream.get(), data, {}, {}, {});
}

TEST_F(PatternTokenizerTests, test_bad_regex) {
  // Invalid regex should make analyzer construction fail
  ASSERT_ANY_THROW(irs::analysis::PatternTokenizer::Make(
    irs::analysis::PatternTokenizer::Options{.pattern = "(", .group = -1}));
  ASSERT_ANY_THROW(irs::analysis::PatternTokenizer::Make(
    irs::analysis::PatternTokenizer::Options{.pattern = "(", .group = 1}));
}

TEST_F(PatternTokenizerTests, test_group_out_of_range) {
  ASSERT_ANY_THROW(irs::analysis::PatternTokenizer::Make(
    irs::analysis::PatternTokenizer::Options{.pattern = "(a)", .group = 2}));
  ASSERT_ANY_THROW(irs::analysis::PatternTokenizer::Make(
    irs::analysis::PatternTokenizer::Options{.pattern = ",", .group = 1}));
  ASSERT_ANY_THROW(irs::analysis::PatternTokenizer::Make(
    irs::analysis::PatternTokenizer::Options{.pattern = ",", .group = -2}));
}

TEST_F(PatternTokenizerTests, test_split_empty_matches_keep_bytes) {
  auto stream = MakePattern("x*");

  AssertTokenStreamContents(stream.get(), "ab", {"a", "b"}, {0, 1}, {1, 2},
                            {1, 1});
  AssertTokenStreamContents(stream.get(), "xaxb", {"a", "b"}, {1, 3}, {2, 4},
                            {1, 1});
  AssertTokenStreamContents(stream.get(), "xx", {}, {}, {});
}

TEST_F(PatternTokenizerTests, test_split_empty_matches_step_code_points) {
  auto stream = MakePattern("x*");

  AssertTokenStreamContents(stream.get(), "\xC3\xA9", {"\xC3\xA9"}, {0}, {2},
                            {1});
  AssertTokenStreamContents(stream.get(),
                            "a\xC3\xA9"
                            "b",
                            {"a", "\xC3\xA9", "b"}, {0, 1, 3}, {1, 3, 4},
                            {1, 1, 1});
  AssertTokenStreamContents(stream.get(),
                            "x\xE4\xB8\xAD"
                            "x",
                            {"\xE4\xB8\xAD"}, {1}, {4}, {1});
}

TEST_F(PatternTokenizerTests, test_anchor_matches_text_start_only) {
  auto stream = MakePattern("^x", 0);

  AssertTokenStreamContents(stream.get(), "xxaxx", {"x"}, {0}, {1}, {1});
}

TEST_F(PatternTokenizerTests, test_reset) {
  auto stream = MakePattern(",");

  std::string_view data1("a,b");
  AssertTokenStreamContents(stream.get(), data1, {"a", "b"}, {0, 2}, {1, 3},
                            {1, 1});

  std::string_view data2("x,y,z");
  AssertTokenStreamContents(stream.get(), data2, {"x", "y", "z"}, {0, 2, 4},
                            {1, 3, 5}, {1, 1, 1});
}

TEST_F(PatternTokenizerTests, test_reset_reuse_different_inputs) {
  {
    auto stream = MakePattern(",");
    AssertTokenStreamContents(stream.get(), "a,b", {"a", "b"}, {0, 2}, {1, 3},
                              {1, 1});

    AssertTokenStreamContents(stream.get(), "c,d,e", {"c", "d", "e"}, {0, 2, 4},
                              {1, 3, 5}, {1, 1, 1});
  }

  {
    auto stream = MakePattern("'([^']+)'", 1);
    AssertTokenStreamContents(stream.get(), "a 'foo'", {"foo"}, {3}, {6}, {1});

    AssertTokenStreamContents(stream.get(), "b 'bar' c 'baz'", {"bar", "baz"},
                              {3, 11}, {6, 14}, {1, 1});
  }
}

TEST_F(PatternTokenizerTests, test_splitting_double_dash) {
  std::string_view data("aaa--bbb--ccc");
  auto stream = MakePattern("--");

  AssertTokenStreamContents(stream.get(), data, {"aaa", "bbb", "ccc"},
                            {0, 5, 10}, {3, 8, 13}, {1, 1, 1});
}

TEST_F(PatternTokenizerTests, test_splitting_colon) {
  std::string_view data("aaa:bbb:ccc");
  auto stream = MakePattern(":");

  AssertTokenStreamContents(stream.get(), data, {"aaa", "bbb", "ccc"},
                            {0, 4, 8}, {3, 7, 11}, {1, 1, 1});
}

TEST_F(PatternTokenizerTests, test_splitting_multi_space_and_tabs) {
  std::string_view data("aaa   bbb \t\tccc  ");
  auto stream = MakePattern("\\s+");

  AssertTokenStreamContents(stream.get(), data, {"aaa", "bbb", "ccc"},
                            {0, 6, 12}, {3, 9, 15}, {1, 1, 1});
}

TEST_F(PatternTokenizerTests, test_splitting_single_char) {
  std::string_view data("boo:and:foo");
  auto stream = MakePattern("o");

  AssertTokenStreamContents(stream.get(), data, {"b", ":and:f"}, {0, 3}, {1, 9},
                            {1, 1});
}

TEST_F(PatternTokenizerTests, test_group_zero_matches) {
  std::string_view data("boo:and:foo");
  auto stream = MakePattern(":", 0);

  AssertTokenStreamContents(stream.get(), data, {":", ":"}, {3, 7}, {4, 8},
                            {1, 1});
}

TEST_F(PatternTokenizerTests, test_group_zero_runs) {
  std::string_view data(" a  bb\tccc ");
  auto stream = MakePattern("\\S+", 0);

  AssertTokenStreamContents(stream.get(), data, {"a", "bb", "ccc"}, {1, 4, 7},
                            {2, 6, 10}, {1, 1, 1});
}

TEST_F(PatternTokenizerTests, test_group_zero_lines) {
  std::string_view data("ab\n\ncd");
  auto stream = MakePattern("[^\\n]+", 0);

  AssertTokenStreamContents(stream.get(), data, {"ab", "cd"}, {0, 4}, {2, 6},
                            {1, 1});
}

TEST_F(PatternTokenizerTests, test_group_zero_without_plus_emits_chars) {
  std::string_view data("ab c");
  auto stream = MakePattern("[a-z]", 0);

  AssertTokenStreamContents(stream.get(), data, {"a", "b", "c"}, {0, 1, 3},
                            {1, 2, 4}, {1, 1, 1});
}

TEST_F(PatternTokenizerTests, test_group_zero_non_greedy_emits_chars) {
  std::string_view data("abc");
  auto stream = MakePattern("[a-z]+?", 0);

  AssertTokenStreamContents(stream.get(), data, {"a", "b", "c"}, {0, 1, 2},
                            {1, 2, 3}, {1, 1, 1});
}

TEST_F(PatternTokenizerTests, test_offset_with_complex_pattern) {
  std::string_view data("hello world test");
  auto stream = MakePattern("[,;/\\s]+");

  AssertTokenStreamContents(stream.get(), data, {"hello", "world", "test"},
                            {0, 6, 12}, {5, 11, 16}, {1, 1, 1});
}

TEST_F(PatternTokenizerTests, test_split_non_word) {
  std::string_view data("hello, world! αβγ done");
  auto stream = MakePattern("\\W+");

  AssertTokenStreamContents(stream.get(), data, {"hello", "world", "done"},
                            {0, 7, 21}, {5, 12, 25}, {1, 1, 1});
}

TEST_F(PatternTokenizerTests, test_split_negated_class_treats_utf8_as_delim) {
  std::string_view data("aé b");
  auto stream = MakePattern("[^a-z]+");

  AssertTokenStreamContents(stream.get(), data, {"a", "b"}, {0, 4}, {1, 5},
                            {1, 1});
}

TEST_F(PatternTokenizerTests, test_consecutive_delimiters) {
  std::string_view data("a,,b");
  auto stream = MakePattern(",");

  AssertTokenStreamContents(stream.get(), data, {"a", "b"}, {0, 3}, {1, 4},
                            {1, 1});
}

TEST_F(PatternTokenizerTests, test_delimiter_at_boundaries) {
  std::string_view data(",hello,world,");
  auto stream = MakePattern(",");

  AssertTokenStreamContents(stream.get(), data, {"hello", "world"}, {1, 7},
                            {6, 12}, {1, 1});
}

TEST_F(PatternTokenizerTests, test_utf8_split_comma_cyrillic) {
  // "аба" = 6 UTF-8 bytes, comma, "цаба" = 8 bytes -> total 15.
  std::string_view data("аба,цаба");
  auto stream = MakePattern(",");

  AssertTokenStreamContents(stream.get(), data, {"аба", "цаба"}, {0, 7},
                            {6, 15}, {1, 1});
}

TEST_F(PatternTokenizerTests, test_utf8_split_whitespace_mixed_ascii_cjk) {
  // "a" (1) + space + "汉字" (3+3 bytes) + space + "b" (1) = 10 bytes
  std::string_view data("a 汉字 b");
  auto stream = MakePattern("\\s+");

  AssertTokenStreamContents(stream.get(), data, {"a", "汉字", "b"}, {0, 2, 9},
                            {1, 8, 10}, {1, 1, 1});
}

TEST_F(PatternTokenizerTests, test_utf8_group_capture_inside_quotes) {
  std::string_view data("x '汉字' z");
  auto stream = MakePattern("'([^']+)'", 1);

  AssertTokenStreamContents(stream.get(), data, {"汉字"}, {3}, {9}, {1});
}

TEST_F(PatternTokenizerTests, test_utf8_split_comma_4byte_emoji) {
  // U+1F642 = 0xF0 0x9F 0x98 0x8A
  std::string_view emoji = "\xF0\x9F\x98\x8A";
  const std::string data(std::string("a,") + emoji);
  auto stream = MakePattern(",");

  AssertTokenStreamContents(stream.get(), data, {"a", emoji}, {0, 2}, {1, 6},
                            {1, 1});
}

TEST_F(PatternTokenizerTests, test_split_utf8_literal_delimiter) {
  std::string_view data("аба→цаба→x");
  auto stream = MakePattern("→");

  AssertTokenStreamContents(stream.get(), data, {"аба", "цаба", "x"},
                            {0, 9, 20}, {6, 17, 21}, {1, 1, 1});
}

TEST_F(PatternTokenizerTests, test_split_long_literal_delimiter) {
  std::string_view data("a<<<<<<<<<<b<<<<<<<<<<c");
  auto stream = MakePattern("<<<<<<<<<<");

  AssertTokenStreamContents(stream.get(), data, {"a", "b", "c"}, {0, 11, 22},
                            {1, 12, 23}, {1, 1, 1});
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
    {",", -1},    {"\\s+", -1}, {"(\\d+)", 0},        {"(\\d+)", 1},
    {"a*", -1},   {"x?", -1},   {"(\\w+)@(\\w+)", 2}, {"\\S+", 0},
    {"\\W+", -1}, {"[^,]+", 0}};
  const std::vector<std::string> values = {"",
                                           "foo,bar,baz",
                                           "no delims here at all whatsoever",
                                           "a1 b22 c333",
                                           "user@host other@domain",
                                           "aaaa",
                                           ",,,",
                                           huge};

  for (const auto& [pattern, group] : configs) {
    auto pull_stream = MakePattern(pattern, group);
    auto fill_stream = MakePattern(pattern, group);
    for (const auto& v : values) {
      SCOPED_TRACE(testing::Message() << "pattern=" << pattern << " group="
                                      << group << " value.size=" << v.size());
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
      ASSERT_TRUE(fill_stream->Fill(v, sink.writer, {sink.layout}));
      sink.writer.Finish();

      ASSERT_EQ(pulled, filled);
      ASSERT_EQ(pstarts, fstarts);
      ASSERT_EQ(pends, fends);
    }
  }
}

TEST_F(PatternTokenizerTests, column_fill_matches_per_value) {
  std::string longv;
  for (size_t i = 0; i < 40; ++i) {
    longv += "w" + std::to_string(i) + ",";
  }
  ASSERT_GT(longv.size(), 64);
  const std::vector<std::string> values = {"",         ",,,",         longv,
                                           "аба,цаба", "foo,bar,baz", "a,,b"};

  struct Tok {
    std::string term;
    uint32_t start;
    uint32_t end;
    bool operator==(const Tok&) const = default;
  };
  for (const std::string_view pattern : {",", "\\W+", "[^,]+"}) {
    SCOPED_TRACE(testing::Message() << "pattern=" << pattern);
    auto stream = MakePattern(pattern);
    auto per_value = MakePattern(pattern);

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
    tests::FillColumn(*stream, vals, 1, sink.writer, sink.layout);
    sink.writer.Finish();

    for (size_t v = 0; v < values.size(); ++v) {
      SCOPED_TRACE(testing::Message() << "doc=" << v + 1);
      ASSERT_EQ(expected[v], got[v]);
    }
  }
}

namespace {

using Regex = std::monostate;

template<typename Finder>
bool Detects(std::string_view pattern, int group = -1) {
  return std::holds_alternative<Finder>(
    irs::analysis::PatternTokenizer::Detect(pattern, group));
}

std::vector<tests::AnalyzerToken> PatternAnalyzeWith(std::string_view pattern,
                                                     int group,
                                                     std::string_view value,
                                                     bool regex_path) {
  const std::string effective =
    regex_path ? "(?:" + std::string{pattern} + "){1}" : std::string{pattern};
  EXPECT_EQ(regex_path, Detects<Regex>(effective, group));
  auto stream = MakePattern(effective, group);
  auto tokens = tests::Analyze(*stream, value);
  EXPECT_TRUE(tokens.has_value());
  return std::move(*tokens);
}

void AssertPatternFastMatchesRegex(std::string_view pattern, int group,
                                   std::string_view value) {
  const auto slow = PatternAnalyzeWith(pattern, group, value, true);
  const auto fast = PatternAnalyzeWith(pattern, group, value, false);
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
  ASSERT_TRUE(Detects<delim::OneCharFinder>(","));
  ASSERT_TRUE(Detects<delim::ManyCharsFinder>("\\s+"));
  ASSERT_TRUE(Detects<delim::ManyCharsFinder>("[,;]+"));
  ASSERT_TRUE(Detects<delim::ManyCharsFinder>("[a-c]"));
  ASSERT_TRUE(Detects<delim::OneCharFinder>(" +"));
  ASSERT_TRUE(Detects<delim::OneCharFinder>("(?:,+)+"));
  ASSERT_TRUE(Detects<delim::OneStringFinder>("(?:(?:ab)+)+"));
  ASSERT_TRUE(Detects<delim::ManyCharsFinder>(",|;"));
  ASSERT_TRUE(Detects<delim::ManyCharsFinder>("(?i)[a-c]"));
  ASSERT_TRUE(Detects<delim::OneCharFinder>("(?i),"));
  ASSERT_TRUE(Detects<delim::OneStringFinder>("(?i)::"));
  ASSERT_TRUE(Detects<Regex>("(?i)a"));
  ASSERT_TRUE(Detects<Regex>("(?i)ab"));
  ASSERT_TRUE(Detects<delim::OneStringFinder>("::"));
  ASSERT_TRUE(Detects<delim::OneStringFinder>(", "));
  ASSERT_TRUE(Detects<delim::OneStringFinder>("--"));
  ASSERT_TRUE(Detects<delim::OneStringFinder>("ab"));
  ASSERT_TRUE(Detects<delim::OneStringFinder>("a§b"));
  ASSERT_TRUE(Detects<delim::OneStringFinder>("§"));
  ASSERT_TRUE(Detects<delim::OneStringFinder>("§+"));
  ASSERT_TRUE(Detects<delim::OneLongStringFinder>("<<<<<<<<<<"));
  ASSERT_TRUE(Detects<Regex>("(?i)§"));
  ASSERT_TRUE(Detects<Regex>(",*"));
  ASSERT_TRUE(Detects<delim::OneCharFinder>("(,)"));
  ASSERT_TRUE(Detects<Regex>("(,)", 1));
  ASSERT_TRUE(Detects<Regex>(":{2}"));
  ASSERT_TRUE(Detects<Regex>(",|;;"));
  ASSERT_TRUE(Detects<Regex>("\\p{L}"));
  ASSERT_TRUE(Detects<Regex>("x?"));

  ASSERT_TRUE(Detects<delim::ByteRangesFinder>("\\W+"));
  ASSERT_TRUE(Detects<delim::ByteRangesFinder>("[^A-Za-z0-9]+"));
  ASSERT_TRUE(Detects<delim::ByteRangesFinder>("[^,]"));
  ASSERT_TRUE(Detects<delim::ByteRangesFinder>("."));
  ASSERT_TRUE(Detects<Regex>("[^\\p{L}]+"));
  ASSERT_TRUE(Detects<Regex>("[a-zé]"));

  ASSERT_TRUE(Detects<delim::OneCharFinder>("[^,]+", 0));
  ASSERT_TRUE(Detects<delim::OneCharFinder>("[^\\n]+", 0));
  ASSERT_TRUE(Detects<delim::OneCharFinder>(".+", 0));
  ASSERT_TRUE(Detects<delim::ManyCharsFinder>("\\S+", 0));
  ASSERT_TRUE(Detects<delim::ByteRangesFinder>("\\w+", 0));
  ASSERT_TRUE(Detects<delim::ByteRangesFinder>("[0-9]+", 0));
  ASSERT_TRUE(Detects<delim::ByteRangesFinder>("(\\d+)", 0));
  ASSERT_TRUE(Detects<Regex>("[a-z]", 0));
  ASSERT_TRUE(Detects<Regex>("[a-z]+?", 0));
  ASSERT_TRUE(Detects<Regex>("x+", 0));
  ASSERT_TRUE(Detects<Regex>("\\p{L}+", 0));
  ASSERT_TRUE(Detects<Regex>("(\\d+)", 1));
}

TEST(PatternTokenizerFastSplit, property_oracle) {
  struct Case {
    std::string_view pattern;
    int group;
  };
  const std::vector<Case> cases = {{",", -1},          {"\\s+", -1},
                                   {"[,;]+", -1},      {"[a-c]", -1},
                                   {" +", -1},         {";", -1},
                                   {"::", -1},         {", ", -1},
                                   {"aa", -1},         {"--", -1},
                                   {",|;", -1},        {"(?i)::", -1},
                                   {"(?:,+)+", -1},    {"(?:ab)+", -1},
                                   {"§", -1},          {"§+", -1},
                                   {"a§b", -1},        {"→", -1},
                                   {"<<<<<<<<<<", -1}, {"\\W+", -1},
                                   {"[^,]+", -1},      {"[^A-Za-z0-9]+", -1},
                                   {"[^aeiou]+", -1},  {"[^\\n]", -1},
                                   {".", -1},          {"(,)", -1},
                                   {"\\S+", 0},        {"\\w+", 0},
                                   {"[0-9]+", 0},      {"[^,]+", 0},
                                   {"[a-z]+", 0},      {"\\d+", 0},
                                   {"[^\\n]+", 0},     {".+", 0},
                                   {"(\\d+)", 0},      {"[,;]+", 0}};
  constexpr std::array<std::string_view, 32> kAlphabet = {"a",
                                                          "b",
                                                          "c",
                                                          "d",
                                                          "e",
                                                          "f",
                                                          "g",
                                                          "h",
                                                          " ",
                                                          ",",
                                                          ";",
                                                          "\t",
                                                          "\n",
                                                          "x",
                                                          "y",
                                                          "z",
                                                          "0",
                                                          "1",
                                                          "2",
                                                          "3",
                                                          " ",
                                                          " ",
                                                          ",",
                                                          ",",
                                                          ";",
                                                          ";",
                                                          "§",
                                                          "→",
                                                          "汉",
                                                          "é",
                                                          "\xF0\x9F\x98\x8A",
                                                          "-"};
  uint64_t seed = 0xfa57;
  const auto next = [&] {
    seed = seed * 6364136223846793005ULL + 1442695040888963407ULL;
    return static_cast<size_t>(seed >> 33);
  };
  for (const auto& [pattern, group] : cases) {
    for (const std::string_view v :
         {std::string_view{""}, std::string_view{","}, std::string_view{",,"},
          std::string_view{"x"}, std::string_view{",x,"},
          std::string_view{"a b,c;d"}, std::string_view{"аба→цаба→→x"},
          std::string_view{"→→"}, std::string_view{"§x§§"},
          std::string_view{"a§b§§c"}, std::string_view{"a1\nb22\n\nc333"},
          std::string_view{"x<<<<<<<<<<y<<<<<<<<<<<<z"}}) {
      SCOPED_TRACE(testing::Message() << "pattern=" << pattern << " group="
                                      << group << " value=\"" << v << "\"");
      AssertPatternFastMatchesRegex(pattern, group, v);
    }
    for (size_t iter = 0; iter < 200; ++iter) {
      std::string v;
      const size_t len = next() % 80;
      for (size_t i = 0; i < len; ++i) {
        v += kAlphabet[next() % kAlphabet.size()];
      }
      SCOPED_TRACE(testing::Message()
                   << "pattern=" << pattern << " group=" << group
                   << " iter=" << iter << " value=\"" << v << "\"");
      AssertPatternFastMatchesRegex(pattern, group, v);
    }
  }
}
