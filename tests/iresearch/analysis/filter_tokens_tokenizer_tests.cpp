////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2026 SereneDB GmbH, Berlin, Germany
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

#include <duckdb.hpp>
#include <iresearch/analysis/delimited_tokenizer.hpp>
#include <iresearch/analysis/filter_tokens_tokenizer.hpp>
#include <iresearch/analysis/pipeline_tokenizer.hpp>
#include <iresearch/utils/duckdb_engine.hpp>
#include <iresearch/utils/pg/sql_exception.hpp>
#include <random>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "token_sink_utils.hpp"

namespace {

using irs::analysis::FilterTokensTokenizer;
using Options = FilterTokensTokenizer::Options;

duckdb::ClientContext& TestContext() {
  static auto* conn =
    new duckdb::Connection{irs::DuckDBEngine::Instance().instance()};
  return *conn->context;
}

bool Passes(Options opts, std::string_view value) {
  FilterTokensTokenizer stream{opts};
  stream.Bind(TestContext());
  const auto tokens = tests::Analyze(stream, value);
  EXPECT_TRUE(tokens.has_value());
  EXPECT_LE(tokens->size(), 1);
  if (tokens->empty()) {
    return false;
  }
  const auto& t = tokens->front();
  EXPECT_EQ(value, t.term);
  EXPECT_EQ(1, t.pos);
  EXPECT_EQ(0, t.offs_start);
  EXPECT_EQ(value.size(), t.offs_end);
  return true;
}

size_t RefChars(std::string_view value) {
  size_t n = 0;
  for (const char c : value) {
    n += (static_cast<uint8_t>(c) & 0xC0) != 0x80;
  }
  return n;
}

irs::analysis::PipelineTokenizer SplitThenFilter(Options opts) {
  std::vector<irs::analysis::Tokenizer::ptr> stages;
  stages.emplace_back(irs::analysis::DelimitedTokenizer::Make(
    irs::analysis::DelimitedTokenizer::Options{.delimiter = " "}));
  stages.emplace_back(FilterTokensTokenizer::Make(std::move(opts)));
  return irs::analysis::PipelineTokenizer{std::move(stages)};
}

}  // namespace

TEST(filter_tokens_tokenizer_test, consts) {
  static_assert("filter_tokens" == irs::Type<FilterTokensTokenizer>::name());
}

TEST(filter_tokens_tokenizer_test, traits) {
  auto stream = FilterTokensTokenizer::Make({});
  ASSERT_NE(nullptr, stream);
  ASSERT_EQ(irs::Type<FilterTokensTokenizer>::id(), stream->type());
  ASSERT_TRUE(stream->Traits().unique);
  ASSERT_TRUE(stream->Traits().offsets);
}

TEST(filter_tokens_tokenizer_test, defaults_pass_everything) {
  EXPECT_TRUE(Passes({}, "a"));
  EXPECT_TRUE(Passes({}, std::string(10000, 'x')));
  EXPECT_TRUE(Passes({}, "\xF0\x9F\x98\x80"));
}

TEST(filter_tokens_tokenizer_test, length_bounds_are_inclusive) {
  const Options opts{.min_length = 2, .max_length = 4};
  EXPECT_FALSE(Passes(opts, "a"));
  EXPECT_TRUE(Passes(opts, "ab"));
  EXPECT_TRUE(Passes(opts, "abcd"));
  EXPECT_FALSE(Passes(opts, "abcde"));
  EXPECT_FALSE(Passes({.min_length = 1}, ""));
  EXPECT_TRUE(Passes({.min_length = 5}, std::string(5000, 'q')));
  EXPECT_FALSE(Passes({.max_length = 3}, std::string(5000, 'q')));
}

TEST(filter_tokens_tokenizer_test, length_counts_characters_not_bytes) {
  const std::string cjk = "\xE6\x97\xA5\xE6\x9C\xAC\xE8\xAA\x9E";
  EXPECT_TRUE(Passes({.min_length = 3, .max_length = 3}, cjk));
  EXPECT_FALSE(Passes({.min_length = 4}, cjk));
  EXPECT_FALSE(Passes({.max_length = 2}, cjk));
  const std::string strasse =
    "stra\xC3\x9F"
    "e";
  EXPECT_TRUE(Passes({.max_length = 6}, strasse));
  EXPECT_FALSE(Passes({.max_length = 5}, strasse));
  EXPECT_FALSE(Passes({.min_length = 7}, strasse));
  const std::string emoji = "\xF0\x9F\x98\x80\xF0\x9F\x98\x80";
  EXPECT_TRUE(Passes({.min_length = 2, .max_length = 2}, emoji));
  EXPECT_FALSE(Passes({.min_length = 3}, emoji));
  EXPECT_FALSE(Passes({.max_length = 1}, emoji));
}

TEST(filter_tokens_tokenizer_test, length_oracle_random_utf8) {
  const std::vector<std::string> units = {
    "a", "Z", " ", "\xC3\xA9", "\xE6\x97\xA5", "\xF0\x9F\x98\x80"};
  std::mt19937_64 rng{53};
  for (size_t iter = 0; iter < 2000; ++iter) {
    std::string value;
    const size_t n = rng() % 24;
    for (size_t i = 0; i < n; ++i) {
      value += units[rng() % units.size()];
    }
    const size_t min = rng() % 12;
    const size_t max = rng() % 3 == 0 ? 0 : min + rng() % 12;
    const size_t chars = RefChars(value);
    const bool expected = chars >= min && (max == 0 || chars <= max);
    SCOPED_TRACE(testing::Message()
                 << "value=" << value << " min=" << min << " max=" << max);
    ASSERT_EQ(expected, Passes({.min_length = min, .max_length = max}, value));
  }
}

TEST(filter_tokens_tokenizer_test, predicate_keeps_true_only) {
  const Options opts{.predicate = "input NOT LIKE 'http%'"};
  EXPECT_TRUE(Passes(opts, "abc"));
  EXPECT_FALSE(Passes(opts, "https://example.com"));
  const Options nulls{.predicate = "CASE WHEN input = 'keep' THEN true END"};
  EXPECT_TRUE(Passes(nulls, "keep"));
  EXPECT_FALSE(Passes(nulls, "drop"));
}

TEST(filter_tokens_tokenizer_test, predicate_and_length_combine) {
  const Options opts{
    .min_length = 2, .max_length = 8, .predicate = "input <> 'the'"};
  EXPECT_FALSE(Passes(opts, "a"));
  EXPECT_FALSE(Passes(opts, "the"));
  EXPECT_FALSE(Passes(opts, "overlylongword"));
  EXPECT_TRUE(Passes(opts, "fox"));
}

TEST(filter_tokens_tokenizer_test, predicate_must_be_boolean) {
  FilterTokensTokenizer stream{{.predicate = "length(input)"}};
  ASSERT_THROW(stream.Bind(TestContext()), irs::SqlException);
}

TEST(filter_tokens_tokenizer_test, predicate_rejects_subqueries) {
  ASSERT_THROW(FilterTokensTokenizer({.predicate = "input IN (SELECT 'a')"}),
               irs::SqlException);
}

TEST(filter_tokens_tokenizer_test, pipeline_drops_without_position_gaps) {
  auto pipe = SplitThenFilter({.min_length = 2, .max_length = 3});
  ASSERT_TRUE(pipe.Traits().offsets);
  const std::string data = "a bb ccc dddd \xC3\xA9\xC3\xA9 x";
  const auto tokens = tests::Analyze(pipe, data);
  ASSERT_TRUE(tokens.has_value());
  const std::vector<tests::AnalyzerToken> expected{
    {"bb", 1, 2, 4}, {"ccc", 2, 5, 8}, {"\xC3\xA9\xC3\xA9", 3, 14, 18}};
  ASSERT_EQ(expected, *tokens);
}

TEST(filter_tokens_tokenizer_test, pipeline_predicate_keeps_offsets) {
  auto pipe = SplitThenFilter(
    {.min_length = 2, .predicate = "input NOT IN ('the', 'over')"});
  pipe.Bind(TestContext());
  const std::string data = "the quick fox a over the lazy dog";
  const auto tokens = tests::Analyze(pipe, data);
  ASSERT_TRUE(tokens.has_value());
  const std::vector<tests::AnalyzerToken> expected{{"quick", 1, 4, 9},
                                                   {"fox", 2, 10, 13},
                                                   {"lazy", 3, 25, 29},
                                                   {"dog", 4, 30, 33}};
  ASSERT_EQ(expected, *tokens);
}

TEST(filter_tokens_tokenizer_test, pipeline_predicate_spans_batches) {
  auto pipe = SplitThenFilter({.predicate = "length(input) % 3 = 0"});
  pipe.Bind(TestContext());
  std::string data;
  std::vector<std::string> expected;
  for (size_t i = 0; i < 5000; ++i) {
    auto word = "w" + std::to_string(i);
    if (word.size() % 3 == 0) {
      expected.push_back(word);
    }
    data += word;
    data += ' ';
  }
  data.pop_back();
  const auto terms = tests::AnalyzeTerms(pipe, data);
  ASSERT_TRUE(terms.has_value());
  ASSERT_EQ(expected, *terms);
}

TEST(filter_tokens_tokenizer_test, column_fill_matches_per_value) {
  const std::vector<std::string> base = {"",
                                         "a",
                                         "ab",
                                         "abc",
                                         "abcd",
                                         "\xC3\xA9\xC3\xA9",
                                         "\xE6\x97\xA5",
                                         std::string(40, 'w'),
                                         "http://x"};
  std::vector<std::string> values;
  for (size_t i = 0; i < 3000; ++i) {
    values.push_back(base[i % base.size()]);
  }
  const Options opts{
    .min_length = 2, .max_length = 40, .predicate = "input NOT LIKE 'http%'"};
  FilterTokensTokenizer stream{opts};
  stream.Bind(TestContext());
  std::vector<duckdb::string_t> vals;
  for (const auto& v : values) {
    vals.emplace_back(v.data(), static_cast<uint32_t>(v.size()));
  }
  std::vector<std::vector<std::string>> got(values.size());
  const auto collect = [&](irs::TokenBatch& batch,
                           std::span<const irs::DocRun> runs) {
    uint32_t tok = 0;
    for (const auto& run : runs) {
      for (uint32_t j = 0; j < run.ntokens; ++j, ++tok) {
        const auto& t = batch.terms[tok];
        got[run.doc - 1].emplace_back(t.GetData(), t.GetSize());
      }
    }
  };
  tests::FnTokenSink sink{irs::TokenLayout::Terms, collect};
  tests::FillColumn(stream, vals, 1, sink.writer, sink.layout);
  sink.writer.Finish();
  std::vector<bool> kept;
  for (const auto& v : base) {
    kept.push_back(Passes(opts, v));
  }
  for (size_t v = 0; v < values.size(); ++v) {
    SCOPED_TRACE(testing::Message() << "doc=" << v + 1);
    const std::vector<std::string> expected =
      kept[v % base.size()] ? std::vector<std::string>{values[v]}
                            : std::vector<std::string>{};
    ASSERT_EQ(expected, got[v]);
  }
}
