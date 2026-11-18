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
#include <cmath>
#include <iresearch/search/explain/explanation.hpp>
#include <limits>
#include <optional>
#include <string_view>

#include "connector/score_emit.h"
#include "connector/vector_explain.h"
#include "gtest/gtest.h"

namespace sdb::connector {
namespace {

using enum irs::VectorMetric;
using enum ScoreEmit;

VectorScorerOptions Options(irs::VectorMetric metric, ScoreEmit emit) {
  return {
    .field_id = 1,
    .query_vector = {1.f, 0.f},
    .dims = 2,
    .metric = metric,
    .score_emit = emit,
    .natural_order = duckdb::OrderType::ASCENDING,
    .nprobe = 0,
  };
}

irs::score_t ValueOf(const irs::explain::Node& node) {
  return std::get<irs::score_t>(node.value);
}

const irs::explain::Node* Detail(const irs::explain::Node& node,
                                 std::string_view key) {
  for (const auto& child : node.details) {
    if (child.key == key) {
      return &child;
    }
  }
  return nullptr;
}

TEST(vector_explain, names_the_function_the_query_ranks_by) {
  struct Case {
    irs::VectorMetric metric;
    ScoreEmit emit;
    std::string_view name;
  };
  for (const auto& [metric, emit, name] : std::array{
         Case{L2Sqr, SqrtNeg, "l2_distance"},
         Case{L2Sqr, ZeroMinus, "l2_sqr_distance"},
         Case{L1, ZeroMinus, "l1_distance"},
         Case{Cosine, OneMinus, "cosine_distance"},
         Case{Cosine, Identity, "cosine_similarity"},
         Case{InnerProduct, Identity, "inner_product"},
         Case{InnerProduct, Negate, "negative_inner_product"},
       }) {
    SCOPED_TRACE(name);
    auto vs = Options(metric, emit);
    vs.exact = true;
    const auto node = ExplainDistance(vs, "emb", -1.f, -1.f);
    EXPECT_EQ("distance", node.key);
    EXPECT_EQ(name, node.scorer);
    EXPECT_EQ(1u, node.subject.field);
    EXPECT_EQ(ApplyScoreEmit(emit, -1.f), ValueOf(node));
  }
}

TEST(vector_explain, a_norm_names_no_query_vector) {
  auto l2 = Options(L2Sqr, SqrtNeg);
  l2.query_vector = {0.f, 0.f};
  l2.exact = true;
  EXPECT_EQ("l2_distance", ExplainDistance(l2, "emb", -4.f, -4.f).scorer);
  l2.norm = true;
  const auto node = ExplainDistance(l2, "emb", -4.f, -4.f);
  EXPECT_EQ("l2_norm", node.scorer);
  EXPECT_EQ("sqrt(sum(emb[i]^2))", node.expression);
  auto l1 = Options(L1, ZeroMinus);
  l1.query_vector = {0.f, 0.f};
  l1.exact = true;
  l1.norm = true;
  EXPECT_EQ("l1_norm", ExplainDistance(l1, "emb", -3.f, -3.f).scorer);
}

TEST(vector_explain, an_exact_distance_lists_only_the_dimensions) {
  auto vs = Options(L2Sqr, SqrtNeg);
  vs.exact = true;
  const auto node = ExplainDistance(vs, "emb", -0.25f, -0.25f);
  EXPECT_EQ(0.5f, ValueOf(node));
  EXPECT_EQ("sqrt(sum((emb[i] - query[i])^2))", node.expression);
  EXPECT_EQ("computed from the row's stored vector", node.note);
  ASSERT_EQ(1u, node.details.size());
  EXPECT_EQ("dims", node.details.front().key);
  EXPECT_EQ(2u, std::get<uint64_t>(node.details.front().value));
}

TEST(vector_explain, a_search_shows_the_exact_distance_and_its_parameters) {
  auto vs = Options(L2Sqr, SqrtNeg);
  vs.quant = irs::VectorQuantization::SQ8;
  vs.top_k = 3;
  vs.rescore_width = 2.f;
  auto node = ExplainDistance(vs, "emb", -0.36f, -0.25f);
  EXPECT_EQ("found by an ivf search over sq8 codes", node.note);
  EXPECT_EQ(std::sqrt(0.36f), ValueOf(node));
  ASSERT_NE(nullptr, Detail(node, "exact"));
  EXPECT_EQ(0.5f, ValueOf(*Detail(node, "exact")));
  ASSERT_NE(nullptr, Detail(node, "k"));
  EXPECT_EQ(3u, std::get<uint64_t>(Detail(node, "k")->value));
  ASSERT_NE(nullptr, Detail(node, "rescore_width"));
  EXPECT_EQ(2.f, ValueOf(*Detail(node, "rescore_width")));
  EXPECT_EQ(nullptr, Detail(node, "nprobe"));

  vs.nprobe = 8;
  node = ExplainDistance(vs, "emb", -0.36f, -0.25f);
  ASSERT_NE(nullptr, Detail(node, "nprobe"));
  EXPECT_EQ(8u, std::get<uint64_t>(Detail(node, "nprobe")->value));

  vs.rescore_all = true;
  EXPECT_EQ(nullptr, Detail(ExplainDistance(vs, "emb", -0.36f, -0.25f),
                            "rescore_width"));

  vs.kind = irs::AnnKind::Hnsw;
  vs.quant = irs::VectorQuantization::None;
  vs.ef_search = 40;
  node = ExplainDistance(vs, "emb", -0.25f, -0.25f);
  EXPECT_EQ("found by an hnsw search", node.note);
  ASSERT_NE(nullptr, Detail(node, "ef_search"));
  EXPECT_EQ(40u, std::get<uint64_t>(Detail(node, "ef_search")->value));
  EXPECT_EQ(nullptr, Detail(node, "nprobe"));
  EXPECT_EQ(nullptr, Detail(node, "rescore_width"));
}

TEST(vector_explain, a_range_search_shows_its_radius) {
  auto vs = Options(L2Sqr, SqrtNeg);
  vs.radius = 0.9f;
  vs.nprobe = 8;
  auto node = ExplainDistance(vs, "emb", std::nullopt, -0.25f);
  EXPECT_EQ(0.5f, ValueOf(node));
  ASSERT_NE(nullptr, Detail(node, "radius"));
  EXPECT_EQ(0.9f, ValueOf(*Detail(node, "radius")));
  EXPECT_EQ("the search returns distances below it",
            Detail(node, "radius")->note);
  EXPECT_EQ(nullptr, Detail(node, "nprobe"));
  EXPECT_EQ(nullptr, Detail(node, "k"));

  vs.radius_inclusive = true;
  node = ExplainDistance(vs, "emb", std::nullopt, -0.25f);
  EXPECT_EQ("the largest distance the search returns",
            Detail(node, "radius")->note);
}

TEST(vector_explain, a_row_without_a_vector_has_no_distance) {
  auto vs = Options(Cosine, OneMinus);
  vs.exact = true;
  const auto node = ExplainDistance(
    vs, "emb", std::numeric_limits<irs::score_t>::quiet_NaN(), std::nullopt);
  EXPECT_TRUE(std::isnan(ValueOf(node)));
  EXPECT_EQ("cosine_distance", node.scorer);
  EXPECT_EQ("emb is NULL in the row", node.note);
  EXPECT_TRUE(node.expression.empty());
  EXPECT_TRUE(node.details.empty());
}

}  // namespace
}  // namespace sdb::connector
