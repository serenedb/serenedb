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

#include <array>

#include "connector/functions/search.h"
#include "connector/functions/ts_query_codec.h"
#include "gtest/gtest.h"
#include "iresearch/search/unscored.hpp"

namespace {

using namespace sdb::connector;

TEST(ts_query_codec_test, plain_value_round_trips_as_bare_text) {
  const auto value = MakeTSQueryValue(MakeTSQueryType(), "quick brown fox");
  const auto parts = TryGetTSQueryParts(value);
  ASSERT_TRUE(parts.has_value());
  EXPECT_EQ("quick brown fox", parts->text);
  EXPECT_TRUE(parts->tokenizer.empty());
  EXPECT_TRUE(parts->scorer.empty());
  EXPECT_EQ(0, parts->slop);
  EXPECT_FLOAT_EQ(1.0f, parts->boost);
  EXPECT_EQ(TSQueryMerge::Default, parts->merge);
}

TEST(ts_query_codec_test, parts_for_a_bare_type_are_defaults) {
  const auto parts = TSQueryPartsForType(MakeTSQueryType(), "fox");
  EXPECT_EQ("fox", parts.text);
  EXPECT_TRUE(parts.scorer.empty());
  EXPECT_EQ(TSQueryMerge::Default, parts.merge);
}

TEST(ts_query_codec_test, unmodified_value_renders_bare_text) {
  EXPECT_EQ("fox", RenderTSQueryValueText({.text = "fox"}));
}

TEST(ts_query_codec_test, modifiers_render_into_the_sql_form) {
  const auto sql = RenderTSQueryValueText({.text = "fox",
                                           .tokenizer = "en",
                                           .scorer = "bm25(1.2, 0.75)",
                                           .slop = 3,
                                           .boost = 2.5f,
                                           .merge = TSQueryMerge::Max});
  EXPECT_NE(std::string::npos, sql.find("fox"));
  EXPECT_NE(std::string::npos, sql.find("en"));
  EXPECT_NE(std::string::npos, sql.find("bm25(1.2, 0.75)"));
  EXPECT_NE(std::string::npos, sql.find("3"));
  EXPECT_NE(std::string::npos, sql.find("max"));
}

TEST(ts_query_codec_test, unscored_renders_as_score_null) {
  EXPECT_EQ("fox", RenderTSQueryValueText({.text = "fox"}));
  EXPECT_EQ(
    "'fox'::score(NULL)",
    RenderTSQueryValueText(
      {.text = "fox", .scorer = std::string{irs::Unscored::type_name()}}));
}

TEST(ts_query_codec_test, named_scorer_renders_with_its_parameters) {
  EXPECT_EQ(
    "'fox'::score('bm25(1.2, 0.75)')",
    RenderTSQueryValueText({.text = "fox", .scorer = "bm25(1.2, 0.75)"}));
}

duckdb::LogicalType TruncatedTSQueryType(size_t n) {
  duckdb::child_list_t<duckdb::LogicalType> ch;
  const std::array<std::pair<const char*, duckdb::LogicalType>, 6> all{{
    {"text", duckdb::LogicalType::VARCHAR},
    {"tokenizer", duckdb::LogicalType::VARCHAR},
    {"boost", duckdb::LogicalType::FLOAT},
    {"slop", duckdb::LogicalType::BIGINT},
    {"scorer", duckdb::LogicalType::VARCHAR},
    {"merge", duckdb::LogicalType::UTINYINT},
  }};
  for (size_t i = 0; i < n; ++i) {
    ch.emplace_back(all[i].first, all[i].second);
  }
  auto type = duckdb::LogicalType::STRUCT(ch);
  type.SetAlias(std::string{kTSQueryTypeName});
  return type;
}

duckdb::Value FullStruct(const duckdb::Value& scorer) {
  duckdb::vector<duckdb::Value> children;
  children.emplace_back("fox");
  children.emplace_back("en");
  children.emplace_back(duckdb::Value::FLOAT(2.5f));
  children.emplace_back(duckdb::Value::BIGINT(3));
  children.emplace_back(scorer);
  children.emplace_back(
    duckdb::Value::UTINYINT(static_cast<uint8_t>(TSQueryMerge::Max)));
  return duckdb::Value::STRUCT(MakeTSQueryType(), std::move(children));
}

TEST(ts_query_codec_test, all_six_children_decode) {
  const auto parts = TryGetTSQueryParts(FullStruct(duckdb::Value("bm25(1.2)")));
  ASSERT_TRUE(parts.has_value());
  EXPECT_EQ("fox", parts->text);
  EXPECT_EQ("en", parts->tokenizer);
  EXPECT_FLOAT_EQ(2.5f, parts->boost);
  EXPECT_EQ(3, parts->slop);
  EXPECT_EQ("bm25(1.2)", parts->scorer);
  EXPECT_EQ(TSQueryMerge::Max, parts->merge);
}

TEST(ts_query_codec_test, null_and_empty_scorer_both_decode_to_empty) {
  const auto null_scorer =
    TryGetTSQueryParts(FullStruct(duckdb::Value{duckdb::LogicalType::VARCHAR}));
  const auto empty_scorer = TryGetTSQueryParts(FullStruct(duckdb::Value("")));
  ASSERT_TRUE(null_scorer.has_value());
  ASSERT_TRUE(empty_scorer.has_value());
  EXPECT_TRUE(null_scorer->scorer.empty());
  EXPECT_TRUE(empty_scorer->scorer.empty());
}

TEST(ts_query_codec_test, a_struct_missing_trailing_children_keeps_defaults) {
  duckdb::vector<duckdb::Value> children;
  children.emplace_back("fox");
  children.emplace_back("en");
  children.emplace_back(duckdb::Value::FLOAT(2.0f));
  const auto parts = TryGetTSQueryParts(
    duckdb::Value::STRUCT(TruncatedTSQueryType(3), std::move(children)));
  ASSERT_TRUE(parts.has_value());
  EXPECT_EQ("fox", parts->text);
  EXPECT_FLOAT_EQ(2.0f, parts->boost);
  EXPECT_EQ(0, parts->slop);
  EXPECT_TRUE(parts->scorer.empty());
  EXPECT_EQ(TSQueryMerge::Default, parts->merge);
}

TEST(ts_query_codec_test, null_text_is_not_a_tsquery) {
  duckdb::vector<duckdb::Value> children;
  children.emplace_back(duckdb::Value{duckdb::LogicalType::VARCHAR});
  children.emplace_back(duckdb::Value{duckdb::LogicalType::VARCHAR});
  children.emplace_back(duckdb::Value::FLOAT(1.0f));
  EXPECT_FALSE(TryGetTSQueryParts(duckdb::Value::STRUCT(TruncatedTSQueryType(3),
                                                        std::move(children)))
                 .has_value());
}

TEST(ts_query_codec_test, a_plain_varchar_is_not_a_tsquery) {
  EXPECT_FALSE(TryGetTSQueryParts(duckdb::Value("fox")).has_value());
}

}  // namespace
