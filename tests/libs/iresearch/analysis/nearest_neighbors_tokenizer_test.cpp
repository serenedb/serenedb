////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2021 ArangoDB GmbH, Cologne, Germany
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
///
/// @author Alex Geenen
/// @author Andrey Abramov
////////////////////////////////////////////////////////////////////////////////

#include "iresearch/analysis/nearest_neighbors_tokenizer.hpp"
#include "iresearch/analysis/token_batch.hpp"
#include "test_resources.hpp"
#include "tests_shared.hpp"
#include "token_sink_utils.hpp"

namespace {

std::string ModelLocation() {
#ifdef WIN32
  return test_base::resource("model_cooking.bin").generic_string();
#else
  return TestBase::resource("model_cooking.bin").string();
#endif
}

}  // namespace

TEST(nearest_neighbors_tokenizer_test, consts) {
  static_assert("nearest_neighbors" ==
                irs::Type<irs::analysis::NearestNeighborsTokenizer>::name());
}

TEST(nearest_neighbors_tokenizer_test, model_is_shared_between_instances) {
  duckdb::DuckDB db{nullptr};
  auto& cache = db.instance->GetSharedObjectCache();
  const auto make = [&] {
    return irs::analysis::NearestNeighborsTokenizer::Make(
      irs::analysis::NearestNeighborsTokenizer::Options{
        .model_location = ModelLocation(), .top_k = 2},
      cache);
  };
  ASSERT_EQ(0u, cache.GetEntryCount());
  const auto a = make();
  const auto b = make();
  ASSERT_TRUE(a);
  ASSERT_TRUE(b);
  EXPECT_EQ(1u, cache.GetEntryCount());
}

TEST(nearest_neighbors_tokenizer_test, test_load) {
  // load json string
  {
    std::string_view data{"salt"};
    auto stream = irs::analysis::NearestNeighborsTokenizer::Make(
      irs::analysis::NearestNeighborsTokenizer::Options{
        .model_location = ModelLocation(),
      },
      tests::Cache());

    ASSERT_NE(nullptr, stream);

    const auto tokens = tests::Analyze(*stream, data);
    ASSERT_TRUE(tokens.has_value());
    ASSERT_EQ(1, tokens->size());
    ASSERT_EQ((tests::AnalyzerToken{"homogenized", 1, 0, 4}), (*tokens)[0]);
  }

  {
    auto stream = irs::analysis::NearestNeighborsTokenizer::Make(
      irs::analysis::NearestNeighborsTokenizer::Options{
        .model_location = ModelLocation(),
        .top_k = 2,
      },
      tests::Cache());

    ASSERT_NE(nullptr, stream);

    {
      const auto tokens = tests::Analyze(*stream, "salt");
      ASSERT_TRUE(tokens.has_value());
      ASSERT_EQ(2, tokens->size());
      ASSERT_EQ((tests::AnalyzerToken{"homogenized", 1, 0, 4}), (*tokens)[0]);
      ASSERT_EQ((tests::AnalyzerToken{"teach", 1, 0, 4}), (*tokens)[1]);
    }

    {
      const auto tokens = tests::Analyze(*stream, "pizza");
      ASSERT_TRUE(tokens.has_value());
      ASSERT_EQ(2, tokens->size());
      ASSERT_EQ((tests::AnalyzerToken{"\"prepared\"", 1, 0, 5}), (*tokens)[0]);
      ASSERT_EQ((tests::AnalyzerToken{"tinfoil", 1, 0, 5}), (*tokens)[1]);
    }
  }

  // test longer string
  {
    std::string_view data{"salt oil"};
    auto stream = irs::analysis::NearestNeighborsTokenizer::Make(
      irs::analysis::NearestNeighborsTokenizer::Options{
        .model_location = ModelLocation(),
        .top_k = 2,
      },
      tests::Cache());

    ASSERT_NE(nullptr, stream);

    const auto tokens = tests::Analyze(*stream, data);
    ASSERT_TRUE(tokens.has_value());
    ASSERT_EQ(4, tokens->size());
    ASSERT_EQ(1, (*tokens)[0].pos);
    ASSERT_EQ(0, (*tokens)[0].offs_start);
    ASSERT_EQ(8, (*tokens)[0].offs_end);
    ASSERT_EQ("homogenized", (*tokens)[0].term);
    ASSERT_EQ(1, (*tokens)[1].pos);
    ASSERT_EQ("teach", (*tokens)[1].term);
    ASSERT_EQ(2, (*tokens)[2].pos);
    ASSERT_EQ("tube\"", (*tokens)[2].term);
    ASSERT_EQ(2, (*tokens)[3].pos);
    ASSERT_EQ("\"breather", (*tokens)[3].term);
  }

  // invalid model location
  ASSERT_ANY_THROW(irs::analysis::NearestNeighborsTokenizer::Make(
    irs::analysis::NearestNeighborsTokenizer::Options{
      .model_location = "invalid_localtion",
    },
    tests::Cache()));

  // .........................................................................
  // additional invalid-Options cases ported from the old JSON failing-case
  // suite. JSON-parser-internal rejections (e.g. `"model_location": 42`,
  // `"top_k": false`) have no direct-API analogue and are dropped.
  // .........................................................................

  // missing required model_location.
  ASSERT_ANY_THROW(irs::analysis::NearestNeighborsTokenizer::Make(
    irs::analysis::NearestNeighborsTokenizer::Options{}, tests::Cache()));

  // top_k must be > 0.
  ASSERT_ANY_THROW(irs::analysis::NearestNeighborsTokenizer::Make(
    irs::analysis::NearestNeighborsTokenizer::Options{
      .model_location = ModelLocation(),
      .top_k = 0,
    },
    tests::Cache()));
  ASSERT_ANY_THROW(irs::analysis::NearestNeighborsTokenizer::Make(
    irs::analysis::NearestNeighborsTokenizer::Options{
      .model_location = ModelLocation(),
      .top_k = -1,
    },
    tests::Cache()));
}

TEST(nearest_neighbors_tokenizer_test, native_fills_match_pull) {
  auto make = [] {
    return irs::analysis::NearestNeighborsTokenizer::Make(
      irs::analysis::NearestNeighborsTokenizer::Options{
        .model_location = ModelLocation(), .top_k = 2},
      tests::Cache());
  };
  auto value_stream = make();
  auto column_stream = make();

  const std::vector<std::string> values = {"salt", "salt pepper", ""};

  std::vector<std::vector<tests::AnalyzerToken>> expected;
  for (const auto& v : values) {
    SCOPED_TRACE(v);
    auto tokens = tests::Analyze(*value_stream, v);
    ASSERT_TRUE(tokens.has_value());
    expected.push_back(std::move(*tokens));
  }

  std::vector<duckdb::string_t> vals;
  for (size_t i = 0; i < values.size(); ++i) {
    vals.emplace_back(values[i].data(),
                      static_cast<uint32_t>(values[i].size()));
  }

  size_t flushes = 0;
  const auto check = [&](irs::TokenBatch& batch, irs::DocRuns runs) {
    ++flushes;
    ASSERT_EQ(values.size(), runs.size());
    uint32_t token_idx = 0;
    for (size_t v = 0; v < values.size(); ++v) {
      SCOPED_TRACE(values[v]);
      ASSERT_EQ(v + 1, runs[v].doc);
      ASSERT_EQ(expected[v].size(), runs[v].ntokens);
      for (const auto& e : expected[v]) {
        const auto& t = batch.terms[token_idx];
        ASSERT_EQ(e.term, (std::string{t.GetData(), t.GetSize()}));
        ASSERT_EQ(e.pos, batch.pos[token_idx]);
        ++token_idx;
      }
    }
    ASSERT_EQ(batch.count, token_idx);
  };
  tests::FnTokenSink sink{irs::TokenLayout::TermsPos, check};
  tests::FillColumn(*column_stream, vals, 1, sink.writer, sink.layout);
  sink.writer.Finish();
  ASSERT_EQ(1, flushes);
}
