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
#include <iresearch/analysis/ngram_tokenizer.hpp>
#include <iresearch/analysis/pipeline_tokenizer.hpp>
#include <iresearch/analysis/sql_tokenizer.hpp>
#include <iresearch/utils/duckdb_engine.hpp>
#include <iresearch/utils/pg/sql_exception.hpp>

#include "bound_config.hpp"
#include "gtest/gtest.h"
#include "test_resources.hpp"
#include "token_sink_utils.hpp"

namespace {

using irs::analysis::SqlTokenizer;

irs::analysis::TokenizerConfig Sql(std::string expression) {
  return irs::analysis::TokenizerConfig{
    SqlTokenizer::Options{.expression = std::move(expression)}};
}

irs::analysis::Tokenizer::ptr MakeBound(
  std::string expression,
  const duckdb::LogicalType& input = duckdb::LogicalType::VARCHAR) {
  auto a = tests::MakeBound(Sql(std::move(expression)), input);
  a->Bind(tests::EngineContext());
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

TEST(SqlTokenizerTest, pipelineNullDropKeepsStackedPositions) {
  std::vector<irs::analysis::Tokenizer::ptr> stages;
  stages.emplace_back(irs::analysis::DelimitedTokenizer::Make(
    irs::analysis::DelimitedTokenizer::Options{.delimiter = " "}));
  stages.emplace_back(irs::analysis::NGramTokenizer::Make(
    {.min_gram = 2,
     .max_gram = 3,
     .preserve_original = false,
     .stream_bytes_type = irs::analysis::NGramTokenizer::InputType::UTF8}));
  stages.emplace_back(
    tests::MakeBound(Sql("CASE WHEN length(input) >= 3 THEN input END")));
  irs::analysis::PipelineTokenizer pipe{std::move(stages)};
  pipe.Bind(tests::EngineContext());
  const auto tokens =
    tests::Analyze(pipe, "search engine", irs::TokenLayout::TermsPos);
  ASSERT_TRUE(tokens.has_value());
  const std::vector<std::pair<std::string, uint32_t>> expected{
    {"sea", 1}, {"ear", 2}, {"arc", 3}, {"rch", 4},
    {"eng", 5}, {"ngi", 6}, {"gin", 7}, {"ine", 8}};
  ASSERT_EQ(expected.size(), tokens->size());
  for (size_t i = 0; i < expected.size(); ++i) {
    EXPECT_EQ(expected[i].first, (*tokens)[i].term);
    EXPECT_EQ(expected[i].second, (*tokens)[i].pos);
  }
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
  auto a = MakeBound("upper(input)");
  ASSERT_EQ(tests::AnalyzeTerms(*a, "x"), (std::vector<std::string>{"X"}));
  a->Unbind();
  a->Bind(tests::EngineContext());
  ASSERT_EQ(tests::AnalyzeTerms(*a, "y"), (std::vector<std::string>{"Y"}));
}

TEST(SqlTokenizerTest, storedExpressionThatDoesNotReadNamesTheStage) {
  auto bound = tests::Bound(Sql("upper(input)"));
  auto& opts = std::get<SqlTokenizer::Options>(bound.config);
  opts.bound.resize(opts.bound.size() / 2);
  auto a = irs::analysis::CreateTokenizer(std::move(bound), tests::Cache());
  try {
    a->Bind(tests::EngineContext());
    FAIL() << "a truncated expression bound";
  } catch (const irs::SqlException& e) {
    EXPECT_TRUE(std::string_view{e.what()}.starts_with("sql: ")) << e.what();
  }
}

TEST(SqlTokenizerTest, parseErrors) {
  for (const auto* expression :
       {"lower((", "input, input", "(SELECT 'x')", "upper($1)",
        "memory.main.upper(input)", "ts_lexize('d', input)",
        "ts_tokenize(input, 'd')", "lower(ts_lexize('d', input)[1])"}) {
    SCOPED_TRACE(expression);
    ASSERT_THROW(tests::Bound(Sql(expression)), irs::SqlException);
  }
}

TEST(SqlTokenizerTest, bindErrors) {
  const auto expect_bind_error = [&](std::string expression) {
    ASSERT_THROW(tests::Bound(Sql(std::move(expression))), irs::SqlException);
  };
  expect_bind_error("no_such_function_xyz(input)");
  expect_bind_error("upper(no_such_column)");
  expect_bind_error("length(input)");
  expect_bind_error("[length(input)]");
  expect_bind_error("random()::VARCHAR");
}

TEST(SqlTokenizerTest, userMacroRejected) {
  duckdb::Connection conn{irs::DuckDBEngine::Instance().instance()};
  auto res = conn.Query("ATTACH ':memory:' AS sql_tok_userdb");
  ASSERT_FALSE(res->HasError()) << res->GetError();
  res = conn.Query(
    "CREATE OR REPLACE MACRO sql_tok_userdb.main.sql_tok_test_up(x) AS "
    "upper(x)");
  ASSERT_FALSE(res->HasError()) << res->GetError();

  ASSERT_THROW(tests::Bound(Sql("sql_tok_userdb.main.sql_tok_test_up(input)")),
               irs::SqlException);

  irs::analysis::BindContext ctx{
    .client = *conn.context,
    .cache = tests::Cache(),
    .row_alone = tests::AnyExpression,
  };
  ASSERT_THROW(irs::analysis::BindInput(Sql("sql_tok_test_up(input)"),
                                        duckdb::LogicalType::VARCHAR, ctx),
               irs::SqlException);
}

TEST(SqlTokenizerTest, traits) {
  const auto traits = tests::MakeBound(Sql("upper(input)"))->Traits();
  ASSERT_FALSE(traits.explicit_pos);
  ASSERT_FALSE(traits.offsets);
  ASSERT_FALSE(traits.keyword);
  ASSERT_TRUE(traits.unique);
}

TEST(SqlTokenizerTest, traits_unique_by_mode) {
  {
    auto a = tests::MakeBound(Sql("upper(input)"));
    ASSERT_TRUE(a->Traits().unique);
    a->Bind(tests::EngineContext());
    ASSERT_TRUE(a->Traits().unique);
    a->Unbind();
    ASSERT_TRUE(a->Traits().unique);
  }
  {
    auto a = tests::MakeBound(Sql("string_split(input, ',')"));
    ASSERT_FALSE(a->Traits().unique);
  }
}

TEST(SqlTokenizerTest, blobResultsBindAsBlobTerms) {
  ASSERT_EQ(duckdb::LogicalTypeId::BLOB,
            tests::Bound(Sql("[input::BLOB]")).terms);
  auto list = MakeBound("[input::BLOB]");
  ASSERT_FALSE(list->Traits().unique);
  ASSERT_EQ(tests::AnalyzeTerms(*list, "ab"), (std::vector<std::string>{"ab"}));
  AssertColumnMatchesPerValue(*list, {"ab", "", "xyz"});

  ASSERT_EQ(duckdb::LogicalTypeId::BLOB,
            tests::Bound(Sql("input::BLOB")).terms);
  auto scalar = MakeBound("input::BLOB");
  ASSERT_TRUE(scalar->Traits().unique);
  ASSERT_EQ(tests::AnalyzeTerms(*scalar, "ab"),
            (std::vector<std::string>{"ab"}));
  AssertColumnMatchesPerValue(*scalar, {"ab", "", "xyz"});

  ASSERT_EQ(duckdb::LogicalTypeId::VARCHAR,
            tests::Bound(Sql("upper(input)")).terms);
}

TEST(SqlTokenizerTest, bytesInputIsBoundAsBlob) {
  auto a = MakeBound("hex(input)", duckdb::LogicalType::BLOB);
  ASSERT_EQ(tests::AnalyzeTerms(*a, "\\x41"),
            (std::vector<std::string>{"5C783431"}));
  ASSERT_EQ(tests::AnalyzeTerms(*a, "A"), (std::vector<std::string>{"41"}));
}

TEST(SqlTokenizerTest, inputReferencedTwiceWithConstants) {
  auto a = MakeBound("concat(input, '-', input)");
  ASSERT_EQ(tests::AnalyzeTerms(*a, "ab"), (std::vector<std::string>{"ab-ab"}));
  AssertColumnMatchesPerValue(*a, {"x", "yy", "", "zzz"});
}

TEST(SqlTokenizerTest, nestedCallsMatchPerValue) {
  auto a = MakeBound("string_split(upper(trim(input)), ',')");
  ASSERT_EQ(tests::AnalyzeTerms(*a, "  a,b ,c  "),
            (std::vector<std::string>{"A", "B ", "C"}));
  AssertColumnMatchesPerValue(*a, {"x,y", " q ", "", "a,,b", "  z  "});

  auto b = MakeBound("concat(upper(input), '-', lower(input))");
  ASSERT_EQ(tests::AnalyzeTerms(*b, "Ab"), (std::vector<std::string>{"AB-ab"}));
  AssertColumnMatchesPerValue(*b, {"Ab", "", "cD"});
}

TEST(SqlTokenizerTest, nullConstantArgumentYieldsNull) {
  auto a = MakeBound("replace(input, NULL, 'x')");
  ASSERT_FALSE(tests::AnalyzeTerms(*a, "abc").has_value());
  AssertColumnMatchesPerValue(*a, {"abc", "d"});
}

TEST(SqlTokenizerTest, constantArgumentsSurviveRepeatedFills) {
  auto a = MakeBound("string_split(input, ',')");
  for (int i = 0; i < 3; ++i) {
    ASSERT_EQ(tests::AnalyzeTerms(*a, "a,b,c"),
              (std::vector<std::string>{"a", "b", "c"}));
    std::vector<std::string> values;
    for (int j = 0; j < 2500; ++j) {
      values.push_back(std::to_string(j) + ",x");
    }
    AssertColumnMatchesPerValue(*a, values);
  }
}

TEST(SqlTokenizerTest, memoryUsageStaysBoundedAcrossFills) {
  auto a = MakeBound("upper(input)");
  const std::string value(64, 'a');
  ASSERT_TRUE(tests::AnalyzeTerms(*a, value).has_value());
  const auto after_first = a->MemoryUsage();
  ASSERT_GT(after_first, 0u);
  for (int i = 0; i < 1000; ++i) {
    ASSERT_TRUE(tests::AnalyzeTerms(*a, value).has_value());
  }
  ASSERT_EQ(after_first, a->MemoryUsage());

  const std::vector<std::string> column(3000, value);
  FillColumn(*a, column);
  const auto after_column = a->MemoryUsage();
  for (int i = 0; i < 10; ++i) {
    FillColumn(*a, column);
  }
  ASSERT_EQ(after_column, a->MemoryUsage());
}

TEST(SqlTokenizerTest, nullRowsInColumnAreSkipped) {
  auto a = MakeBound("upper(input)");
  const std::vector<std::string> values{"a", "b", "c", "d"};
  std::vector<duckdb::string_t> vals;
  for (const auto& v : values) {
    vals.emplace_back(v.data(), static_cast<uint32_t>(v.size()));
  }
  duckdb::ValidityMask validity{static_cast<duckdb::idx_t>(vals.size())};
  validity.SetInvalid(1);
  validity.SetInvalid(3);
  duckdb::UnifiedVectorFormat fmt;
  fmt.sel = duckdb::FlatVector::IncrementalSelectionVector();
  fmt.data = reinterpret_cast<duckdb::const_data_ptr_t>(vals.data());
  fmt.validity = validity;
  fmt.physical_type = duckdb::PhysicalType::VARCHAR;

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
  a->Fill(fmt, static_cast<uint32_t>(vals.size()), 1, sink.writer,
          {irs::TokenLayout::Terms});
  sink.writer.Finish();
  ASSERT_EQ((std::vector<std::vector<std::string>>{{"A"}, {}, {"C"}, {}}), got);
}

}  // namespace
