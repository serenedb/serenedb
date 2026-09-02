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
#include <iresearch/analysis/sql_tokenizer.hpp>

#include "basics/duckdb_engine.h"
#include "gtest/gtest.h"
#include "pg/sql_exception.h"
#include "token_sink_utils.hpp"

namespace {

using irs::analysis::SqlTokenizer;

duckdb::ClientContext& TestContext() {
  static auto* conn =
    new duckdb::Connection{sdb::DuckDBEngine::Instance().instance()};
  return *conn->context;
}

irs::analysis::Tokenizer::ptr MakeBound(std::string expression) {
  auto a = SqlTokenizer::Make({.expression = std::move(expression)});
  a->Bind(TestContext());
  return a;
}

std::vector<std::vector<std::string>> FillColumn(
  irs::analysis::Tokenizer& a, const std::vector<std::string>& values,
  irs::TokenLayout layout = irs::TokenLayout::TermsPos) {
  std::vector<duckdb::string_t> vals;
  vals.reserve(values.size());
  for (size_t i = 0; i < values.size(); ++i) {
    vals.emplace_back(values[i].data(),
                      static_cast<uint32_t>(values[i].size()));
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
  tests::FnTokenSink sink{layout, collect};
  tests::FillColumn(a, vals, 1, sink.writer, layout);
  sink.writer.Finish();
  return got;
}

void AssertColumnMatchesPerValue(irs::analysis::Tokenizer& a,
                                 const std::vector<std::string>& values) {
  std::vector<std::vector<std::string>> expected;
  for (const auto& v : values) {
    auto terms = tests::AnalyzeTerms(a, v);
    expected.push_back(terms.value_or(std::vector<std::string>{}));
  }
  for (const auto layout :
       {irs::TokenLayout::Terms, irs::TokenLayout::TermsPos}) {
    ASSERT_EQ(expected, FillColumn(a, values, layout));
  }
}

TEST(SqlTokenizerTest, scalarMode) {
  auto a = MakeBound("upper(input)");
  auto terms = tests::AnalyzeTerms(*a, "hello");
  ASSERT_TRUE(terms.has_value());
  ASSERT_EQ(*terms, (std::vector<std::string>{"HELLO"}));
}

TEST(SqlTokenizerTest, listMode) {
  auto a = MakeBound("string_split(lower(input), ' ')");
  auto terms = tests::AnalyzeTerms(*a, "Foo BAR baz");
  ASSERT_TRUE(terms.has_value());
  ASSERT_EQ(*terms, (std::vector<std::string>{"foo", "bar", "baz"}));
}

TEST(SqlTokenizerTest, regexSplit) {
  auto a = MakeBound(R"(regexp_split_to_array(lower(input), '\W+'))");
  auto terms = tests::AnalyzeTerms(*a, "Hello, World! FOO bar");
  ASSERT_TRUE(terms.has_value());
  ASSERT_EQ(*terms, (std::vector<std::string>{"hello", "world", "foo", "bar"}));
}

TEST(SqlTokenizerTest, nullResultRejectsValue) {
  auto a = MakeBound("nullif(input, 'skip')");
  ASSERT_FALSE(tests::AnalyzeTerms(*a, "skip").has_value());
  auto terms = tests::AnalyzeTerms(*a, "keep");
  ASSERT_TRUE(terms.has_value());
  ASSERT_EQ(*terms, (std::vector<std::string>{"keep"}));
}

TEST(SqlTokenizerTest, nullElementDropsToken) {
  auto a = MakeBound("list_value(upper(input), NULL, lower(input))");
  auto terms = tests::AnalyzeTerms(*a, "Ab");
  ASSERT_TRUE(terms.has_value());
  ASSERT_EQ(*terms, (std::vector<std::string>{"AB", "ab"}));
}

TEST(SqlTokenizerTest, emptyListAcceptsValueWithoutTokens) {
  auto a = MakeBound(
    "CASE WHEN len(input) > 8 THEN string_split(input, ' ') "
    "ELSE CAST([] AS VARCHAR[]) END");
  auto terms = tests::AnalyzeTerms(*a, "a b c");
  ASSERT_TRUE(terms.has_value());
  ASSERT_TRUE(terms->empty());
}

TEST(SqlTokenizerTest, columnFillMatchesPerValue) {
  auto a = MakeBound(R"(regexp_split_to_array(lower(input), '\W+'))");
  AssertColumnMatchesPerValue(
    *a, {"Hello, World!", "", "one", "skip TWO three", "a-b-c d"});
}

TEST(SqlTokenizerTest, columnFillLongValueCrossesBatches) {
  auto a = MakeBound("string_split(input, ' ')");
  std::string long_value;
  std::vector<std::string> expected_tokens;
  for (size_t i = 0; i < 3000; ++i) {
    auto word = "wordwordwordword" + std::to_string(i);
    expected_tokens.push_back(word);
    long_value += word;
    if (i + 1 < 3000) {
      long_value += ' ';
    }
  }
  const std::vector<std::string> values{"before value", long_value, "after"};
  auto got = FillColumn(*a, values);
  ASSERT_EQ(3u, got.size());
  ASSERT_EQ((std::vector<std::string>{"before", "value"}), got[0]);
  ASSERT_EQ(expected_tokens, got[1]);
  ASSERT_EQ((std::vector<std::string>{"after"}), got[2]);
}

TEST(SqlTokenizerTest, columnFillManyValues) {
  auto a = MakeBound("string_split(input, '|')");
  std::vector<std::string> values;
  for (size_t i = 0; i < 5000; ++i) {
    values.push_back("a" + std::to_string(i) + "|b|c");
  }
  AssertColumnMatchesPerValue(*a, values);
}

TEST(SqlTokenizerTest, matchesDelimitedTokenizer) {
  auto a = MakeBound("string_split(input, '|')");
  irs::analysis::DelimitedTokenizer reference{"|"};
  for (const std::string_view value :
       {"one|two|three", "single", "trail|", "|lead"}) {
    auto got = tests::AnalyzeTerms(*a, value);
    auto expected = tests::AnalyzeTerms(reference, value);
    ASSERT_TRUE(got.has_value());
    ASSERT_TRUE(expected.has_value());
    ASSERT_EQ(*expected, *got) << value;
  }
}

TEST(SqlTokenizerTest, rebindAcrossPoolCycles) {
  auto a = SqlTokenizer::Make({.expression = "upper(input)"});
  a->Bind(TestContext());
  ASSERT_EQ(tests::AnalyzeTerms(*a, "x"), (std::vector<std::string>{"X"}));
  a->Unbind();
  a->Bind(TestContext());
  ASSERT_EQ(tests::AnalyzeTerms(*a, "y"), (std::vector<std::string>{"Y"}));
}

TEST(SqlTokenizerTest, parseErrors) {
  ASSERT_THROW(SqlTokenizer::Make({.expression = "lower(("}),
               sdb::SqlException);
  ASSERT_THROW(SqlTokenizer::Make({.expression = "input, input"}),
               sdb::SqlException);
  ASSERT_THROW(SqlTokenizer::Make({.expression = "(SELECT 'x')"}),
               sdb::SqlException);
  ASSERT_THROW(SqlTokenizer::Make({.expression = "upper($1)"}),
               sdb::SqlException);
  ASSERT_THROW(SqlTokenizer::Make({.expression = "memory.main.upper(input)"}),
               sdb::SqlException);
  // Text-search functions would re-enter the tokenizer machinery (recursion).
  ASSERT_THROW(SqlTokenizer::Make({.expression = "ts_lexize('d', input)"}),
               sdb::SqlException);
  ASSERT_THROW(SqlTokenizer::Make({.expression = "ts_tokenize(input, 'd')"}),
               sdb::SqlException);
  ASSERT_THROW(
    SqlTokenizer::Make({.expression = "lower(ts_lexize('d', input)[1])"}),
    sdb::SqlException);
}

TEST(SqlTokenizerTest, bindErrors) {
  const auto expect_bind_error = [&](std::string expression) {
    auto a = SqlTokenizer::Make({.expression = std::move(expression)});
    ASSERT_THROW(a->Bind(TestContext()), sdb::SqlException);
  };
  expect_bind_error("no_such_function_xyz(input)");
  expect_bind_error("upper(no_such_column)");
  expect_bind_error("length(input)");
  expect_bind_error("random()::VARCHAR");
}

TEST(SqlTokenizerTest, userMacroRejected) {
  duckdb::Connection conn{sdb::DuckDBEngine::Instance().instance()};
  auto res = conn.Query("ATTACH ':memory:' AS sql_tok_userdb");
  ASSERT_FALSE(res->HasError()) << res->GetError();
  res = conn.Query(
    "CREATE OR REPLACE MACRO sql_tok_userdb.main.sql_tok_test_up(x) AS "
    "upper(x)");
  ASSERT_FALSE(res->HasError()) << res->GetError();

  ASSERT_THROW(
    SqlTokenizer::Make({.expression = "sql_tok_userdb.main.sql_tok_test_up("
                                      "input)"}),
    sdb::SqlException);

  auto a = SqlTokenizer::Make({.expression = "sql_tok_test_up(input)"});
  ASSERT_THROW(a->Bind(*conn.context), sdb::SqlException);
}

TEST(SqlTokenizerTest, traits) {
  SqlTokenizer a{{.expression = "upper(input)"}};
  const auto traits = a.Traits();
  ASSERT_FALSE(traits.explicit_pos);
  ASSERT_FALSE(traits.offsets);
  ASSERT_FALSE(traits.keyword);
  ASSERT_FALSE(traits.unique);
}

TEST(SqlTokenizerTest, traits_unique_by_mode) {
  {
    SqlTokenizer a{{.expression = "upper(input)"}};
    ASSERT_FALSE(a.Traits().unique);
    a.Bind(TestContext());
    ASSERT_TRUE(a.Traits().unique);
    a.Unbind();
    ASSERT_TRUE(a.Traits().unique);
  }
  {
    SqlTokenizer a{{.expression = "string_split(input, ',')"}};
    a.Bind(TestContext());
    ASSERT_FALSE(a.Traits().unique);
  }
}

}  // namespace
