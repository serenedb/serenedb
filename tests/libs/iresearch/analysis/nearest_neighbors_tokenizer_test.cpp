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

#include "iresearch/analysis/batch/token_batch.hpp"
#include "iresearch/analysis/nearest_neighbors_tokenizer.hpp"
#include "tests_shared.hpp"
#include "token_sink_utils.hpp"

namespace {

std::string_view gExpectedModel;

irs::analysis::NearestNeighborsTokenizer::model_ptr NullProvider(
  std::string_view model) {
  EXPECT_EQ(gExpectedModel, model);
  return nullptr;
}

irs::analysis::NearestNeighborsTokenizer::model_ptr ThrowingProvider(
  std::string_view model) {
  EXPECT_EQ(gExpectedModel, model);
  throw std::exception();
}

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

TEST(nearest_neighbors_tokenizer_test, test_custom_provider) {
  const auto model_loc = ModelLocation();
  gExpectedModel = model_loc;

  ASSERT_EQ(nullptr,
            irs::analysis::NearestNeighborsTokenizer::set_model_provider(
              &::NullProvider));
  ASSERT_ANY_THROW(irs::analysis::NearestNeighborsTokenizer::Make(
    irs::analysis::NearestNeighborsTokenizer::Options{
      .model_location = model_loc,
      .top_k = 2,
    }));

  ASSERT_EQ(&::NullProvider,
            irs::analysis::NearestNeighborsTokenizer::set_model_provider(
              &::ThrowingProvider));
  ASSERT_ANY_THROW(irs::analysis::NearestNeighborsTokenizer::Make(
    irs::analysis::NearestNeighborsTokenizer::Options{
      .model_location = model_loc,
      .top_k = 2,
    }));

  ASSERT_EQ(
    &::ThrowingProvider,
    irs::analysis::NearestNeighborsTokenizer::set_model_provider(nullptr));
}

TEST(nearest_neighbors_tokenizer_test, test_load) {
  // load json string
  {
    std::string_view data{"salt"};
    auto stream = irs::analysis::NearestNeighborsTokenizer::Make(
      irs::analysis::NearestNeighborsTokenizer::Options{
        .model_location = ModelLocation(),
      });

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
      });

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
      });

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
    }));

  // .........................................................................
  // additional invalid-Options cases ported from the old JSON failing-case
  // suite. JSON-parser-internal rejections (e.g. `"model_location": 42`,
  // `"top_k": false`) have no direct-API analogue and are dropped.
  // .........................................................................

  // missing required model_location.
  ASSERT_ANY_THROW(irs::analysis::NearestNeighborsTokenizer::Make(
    irs::analysis::NearestNeighborsTokenizer::Options{}));

  // top_k must be > 0.
  ASSERT_ANY_THROW(irs::analysis::NearestNeighborsTokenizer::Make(
    irs::analysis::NearestNeighborsTokenizer::Options{
      .model_location = ModelLocation(),
      .top_k = 0,
    }));
  ASSERT_ANY_THROW(irs::analysis::NearestNeighborsTokenizer::Make(
    irs::analysis::NearestNeighborsTokenizer::Options{
      .model_location = ModelLocation(),
      .top_k = -1,
    }));
}

TEST(nearest_neighbors_tokenizer_test, native_fills_match_pull) {
  auto make = [] {
    return irs::analysis::NearestNeighborsTokenizer::Make(
      irs::analysis::NearestNeighborsTokenizer::Options{
        .model_location = ModelLocation(), .top_k = 2});
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
  std::vector<irs::doc_id_t> docs;
  for (size_t i = 0; i < values.size(); ++i) {
    vals.emplace_back(values[i].data(),
                      static_cast<uint32_t>(values[i].size()));
    docs.push_back(static_cast<irs::doc_id_t>(i + 1));
  }

  tests::OneBatchSink sink{irs::TokenLayout::TermsPos};
  column_stream->Fill(vals, docs, sink.writer, sink.layout);
  ASSERT_FALSE(sink.flushed());
  auto& batch = sink.writer.buf;
  const auto runs = sink.writer.Runs();

  ASSERT_EQ(values.size(), runs.size());
  uint32_t token_idx = 0;
  for (size_t v = 0; v < values.size(); ++v) {
    SCOPED_TRACE(values[v]);
    ASSERT_EQ(docs[v], runs[v].doc);
    ASSERT_EQ(expected[v].size(), runs[v].ntokens);
    for (const auto& e : expected[v]) {
      const auto& t = batch.terms[token_idx];
      ASSERT_EQ(e.term, (std::string{t.GetData(), t.GetSize()}));
      ASSERT_EQ(e.pos, batch.pos[token_idx]);
      ++token_idx;
    }
  }
  ASSERT_EQ(batch.count, token_idx);
}
