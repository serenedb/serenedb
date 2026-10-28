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

#include <duckdb/planner/expression/bound_between_expression.hpp>
#include <duckdb/planner/expression/bound_comparison_expression.hpp>
#include <duckdb/planner/expression/bound_conjunction_expression.hpp>
#include <duckdb/planner/expression/bound_constant_expression.hpp>
#include <duckdb/planner/expression/bound_operator_expression.hpp>
#include <duckdb/planner/expression/bound_reference_expression.hpp>
#include <duckdb/storage/statistics/numeric_stats.hpp>
#include <duckdb/storage/statistics/string_stats.hpp>
#include <vector>

#include "connector/scan/col_filter_share.h"
#include "gtest/gtest.h"

namespace sdb::connector {
namespace {

using Expr = duckdb::unique_ptr<duckdb::Expression>;
using enum duckdb::ExpressionType;

constexpr double kEps = 1e-12;

duckdb::BaseStatistics Numbers(const duckdb::Value& min,
                               const duckdb::Value& max) {
  auto stats = duckdb::NumericStats::CreateEmpty(min.type());
  duckdb::NumericStats::SetMin(stats, min);
  duckdb::NumericStats::SetMax(stats, max);
  stats.SetHasNoNull();
  return stats;
}

duckdb::BaseStatistics Ints() {
  return Numbers(duckdb::Value::INTEGER(0), duckdb::Value::INTEGER(99));
}

Expr Col(const duckdb::BaseStatistics& stats) {
  return duckdb::make_uniq<duckdb::BoundReferenceExpression>(stats.GetType(),
                                                             0);
}

Expr Const(duckdb::Value value) {
  return duckdb::make_uniq<duckdb::BoundConstantExpression>(std::move(value));
}

Expr Num(int32_t value) { return Const(duckdb::Value::INTEGER(value)); }

Expr Cmp(duckdb::ExpressionType type, Expr left, Expr right) {
  return duckdb::BoundComparisonExpression::Create(type, std::move(left),
                                                   std::move(right));
}

Expr Conj(duckdb::ExpressionType type, Expr left, Expr right) {
  return duckdb::make_uniq<duckdb::BoundConjunctionExpression>(
    type, std::move(left), std::move(right));
}

Expr Op(duckdb::ExpressionType type, std::vector<Expr> children) {
  auto op = duckdb::make_uniq<duckdb::BoundOperatorExpression>(
    type, duckdb::LogicalType::BOOLEAN);
  for (auto& child : children) {
    op->GetChildrenMutable().push_back(std::move(child));
  }
  return op;
}

Expr Op(duckdb::ExpressionType type, Expr child) {
  std::vector<Expr> children;
  children.push_back(std::move(child));
  return Op(type, std::move(children));
}

double Share(const Expr& expr, const duckdb::BaseStatistics& stats) {
  return EstimateShare(*expr, stats);
}

TEST(col_filter_share_test, ranges_interpolate_between_min_and_max) {
  const auto stats = Ints();
  EXPECT_NEAR(0.01, Share(Cmp(COMPARE_EQUAL, Col(stats), Num(5)), stats), kEps);
  EXPECT_NEAR(0.5, Share(Cmp(COMPARE_LESSTHAN, Col(stats), Num(50)), stats),
              kEps);
  EXPECT_NEAR(0.05, Share(Cmp(COMPARE_GREATERTHAN, Num(5), Col(stats)), stats),
              kEps);
  EXPECT_NEAR(0.0, Share(Cmp(COMPARE_GREATERTHAN, Col(stats), Num(200)), stats),
              kEps);
  EXPECT_NEAR(0.1,
              Share(Conj(CONJUNCTION_AND,
                         Cmp(COMPARE_GREATERTHANOREQUALTO, Col(stats), Num(10)),
                         Cmp(COMPARE_LESSTHANOREQUALTO, Col(stats), Num(19))),
                    stats),
              kEps);
  EXPECT_NEAR(0.2,
              Share(duckdb::BoundBetweenExpression::Create(Col(stats), Num(10),
                                                           Num(29), true, true),
                    stats),
              kEps);
}

TEST(col_filter_share_test, sets_and_negations_combine_value_shares) {
  const auto stats = Ints();
  std::vector<Expr> in;
  in.push_back(Col(stats));
  in.push_back(Num(1));
  in.push_back(Num(2));
  in.push_back(Num(300));
  EXPECT_NEAR(0.02, Share(Op(COMPARE_IN, std::move(in)), stats), kEps);
  EXPECT_NEAR(0.99, Share(Cmp(COMPARE_NOTEQUAL, Col(stats), Num(5)), stats),
              kEps);
  EXPECT_NEAR(0.0199,
              Share(Conj(CONJUNCTION_OR, Cmp(COMPARE_EQUAL, Col(stats), Num(5)),
                         Cmp(COMPARE_EQUAL, Col(stats), Num(6))),
                    stats),
              kEps);
  EXPECT_NEAR(
    0.75,
    Share(Op(OPERATOR_NOT, Cmp(COMPARE_LESSTHAN, Col(stats), Num(25))), stats),
    kEps);
}

TEST(col_filter_share_test, floats_are_continuous) {
  const auto stats =
    Numbers(duckdb::Value::DOUBLE(0.0), duckdb::Value::DOUBLE(1.0));
  EXPECT_NEAR(
    0.25,
    Share(Cmp(COMPARE_LESSTHAN, Col(stats), Const(duckdb::Value::DOUBLE(0.25))),
          stats),
    kEps);
}

TEST(col_filter_share_test, null_checks_follow_null_flags) {
  const auto stats = Ints();
  EXPECT_NEAR(0.0, Share(Op(OPERATOR_IS_NULL, Col(stats)), stats), kEps);
  EXPECT_NEAR(1.0, Share(Op(OPERATOR_IS_NOT_NULL, Col(stats)), stats), kEps);
}

TEST(col_filter_share_test, unknown_predicates_keep_every_row) {
  auto text = duckdb::StringStats::CreateEmpty(duckdb::LogicalType::VARCHAR);
  text.SetHasNoNull();
  EXPECT_NEAR(
    1.0,
    Share(Cmp(COMPARE_EQUAL, Col(text), Const(duckdb::Value{"abc"})), text),
    kEps);
  auto nullable = Ints();
  nullable.SetHasNull();
  EXPECT_NEAR(
    0.5,
    Share(Conj(CONJUNCTION_AND, Cmp(COMPARE_LESSTHAN, Col(nullable), Num(50)),
               Op(OPERATOR_IS_NOT_NULL, Col(nullable))),
          nullable),
    kEps);
}

}  // namespace
}  // namespace sdb::connector
