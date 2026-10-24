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

#include <gtest/gtest.h>

#include "catalog/entry/inverted_index.h"

namespace sdb::catalog {
namespace {

TEST(InvertedIndexConfigTest, StoresExpressionNeedsAStoredField) {
  const auto keys = [] {
    return std::vector<InvertedIndexKey>{
      {{.field_id = 7,
        .type = duckdb::LogicalType::VARCHAR,
        .normalized_expression = "lower(title)"},
       "lower(title)"}};
  };
  const InvertedIndexConfig unstored{keys(), {}};
  EXPECT_FALSE(unstored.StoresExpression(7));
  EXPECT_FALSE(unstored.StoresExpressions());
  EXPECT_TRUE(unstored
                .ExpressionsWithRootSignature(
                  RootSignatureSeed(duckdb::LogicalTypeId::VARCHAR))
                .empty());
  EXPECT_TRUE(unstored.HasExpressionKeyOfType(duckdb::LogicalTypeId::VARCHAR));
  InvertedIndexFields fields;
  fields.emplace(7, InvertedIndexField{{.store_values = true}});
  const InvertedIndexConfig stored{keys(), std::move(fields)};
  EXPECT_TRUE(stored.StoresExpression(7));
  EXPECT_TRUE(stored.StoresExpressions());
  EXPECT_TRUE(stored
                .ExpressionsWithRootSignature(
                  RootSignatureSeed(duckdb::LogicalTypeId::VARCHAR))
                .empty());
  EXPECT_TRUE(stored
                .ExpressionsWithRootSignature(
                  RootSignatureSeed(duckdb::LogicalTypeId::BOOLEAN))
                .empty());
  EXPECT_FALSE(stored.HasExpressionKeyOfType(duckdb::LogicalTypeId::BOOLEAN));
}

TEST(InvertedIndexConfigTest, LookupsFollowKeyOrder) {
  constexpr irs::field_id kColumn = 1;
  constexpr irs::field_id kNull = 5;
  constexpr irs::field_id kTerms = 20;
  constexpr irs::field_id kStored = 21;
  constexpr irs::field_id kBoth = 22;
  const auto key = [](irs::field_id field, irs::field_id column,
                      std::string expression) {
    return InvertedIndexKey{{.field_id = field,
                             .column_id = column,
                             .type = duckdb::LogicalType::VARCHAR,
                             .normalized_expression = expression},
                            expression};
  };
  InvertedIndexFields fields;
  fields.emplace(kColumn, InvertedIndexField{{.null_field_id = kNull,
                                              .indexed_term_dict = true}});
  fields.emplace(kTerms, InvertedIndexField{{.indexed_term_dict = true}});
  fields.emplace(kStored, InvertedIndexField{{.store_values = true}});
  fields.emplace(kBoth, InvertedIndexField{
                          {.store_values = true, .indexed_term_dict = true}});
  const InvertedIndexConfig config{
    {key(kColumn, kColumn, ""), key(kColumn, kColumn, ""),
     key(kTerms, irs::field_limits::invalid(), "e"),
     key(kStored, irs::field_limits::invalid(), "e"),
     key(kBoth, irs::field_limits::invalid(), "e")},
    std::move(fields)};

  EXPECT_EQ(&config.keys[0], config.FindKey(kColumn));
  EXPECT_TRUE(config.WritesField(config.keys[0]));
  EXPECT_FALSE(config.WritesField(config.keys[1]));
  EXPECT_TRUE(config.WritesField(config.keys[2]));
  EXPECT_EQ(&config.keys[2], config.FindKey(kTerms));
  EXPECT_EQ(nullptr, config.FindKey(99));
  EXPECT_EQ(kColumn, config.TermField(kColumn));
  EXPECT_EQ(irs::field_id{42}, config.TermField(42));
  EXPECT_EQ(kColumn, config.ColumnOf(kColumn));
  EXPECT_EQ(kTerms, config.ColumnOf(kTerms));
  EXPECT_EQ(kTerms, config.FindFieldIdByExpression("e"));
  const auto expression = config.FindExpressionKeys("e");
  EXPECT_EQ(&config.keys[2], expression.first);
  EXPECT_EQ(&config.keys[4], expression.term);
  EXPECT_EQ(&config.keys[3], expression.stored);
  EXPECT_EQ(nullptr, config.FindExpressionKeys("f").term);
  EXPECT_TRUE(config.StoresExpression(kStored));
  EXPECT_FALSE(config.StoresExpression(kTerms));
  EXPECT_TRUE(config
                .ExpressionsWithRootSignature(
                  RootSignatureSeed(duckdb::LogicalTypeId::VARCHAR))
                .empty());
  EXPECT_TRUE(config.StoresExpressions());
  const auto owner = config.LookupField(kNull);
  EXPECT_EQ(kColumn, owner.entry_field_id);
  EXPECT_EQ(config.FindEntry(kColumn), owner.entry);
  EXPECT_EQ(nullptr, config.LookupField(77).entry);
}

TEST(InvertedIndexConfigTest, KeysAreFoundByRootSignature) {
  constexpr uint8_t kClass = 1;
  constexpr uint8_t kType = 2;
  const auto key = [](irs::field_id field, std::string function) {
    return InvertedIndexKey{{.field_id = field,
                             .type = duckdb::LogicalType::VARCHAR,
                             .normalized_expression = function,
                             .root_chain = {{.expression_class = kClass,
                                             .expression_type = kType,
                                             .function = function}},
                             .columns = {5}},
                            function};
  };
  const auto signature = [](std::string_view function) {
    return RootSignatureStep(RootSignatureSeed(duckdb::LogicalTypeId::VARCHAR),
                             kClass, kType, function);
  };
  InvertedIndexFields fields;
  fields.emplace(20, InvertedIndexField{{.indexed_term_dict = true}});
  fields.emplace(21, InvertedIndexField{{.store_values = true}});
  fields.emplace(22, InvertedIndexField{{.store_values = true}});
  const InvertedIndexConfig config{
    {key(20, "lower"), key(21, "upper"), key(22, "lower")}, std::move(fields)};

  const auto lower = config.ExpressionsWithRootSignature(signature("lower"));
  ASSERT_EQ(1, lower.size());
  EXPECT_EQ(&config.keys[0], lower[0].first);
  EXPECT_EQ(&config.keys[0], lower[0].term);
  EXPECT_EQ(&config.keys[2], lower[0].stored);
  const auto upper = config.ExpressionsWithRootSignature(signature("upper"));
  ASSERT_EQ(1, upper.size());
  EXPECT_EQ(&config.keys[1], upper[0].first);
  EXPECT_EQ(nullptr, upper[0].term);
  EXPECT_EQ(&config.keys[1], upper[0].stored);
  EXPECT_TRUE(config.ExpressionsWithRootSignature(signature("trim")).empty());
  EXPECT_TRUE(config.StoresExpressions());
  EXPECT_TRUE(config
                .ExpressionsWithRootSignature(
                  RootSignatureSeed(duckdb::LogicalTypeId::VARCHAR))
                .empty());
  EXPECT_TRUE(config.HasExpressionKeyOfType(duckdb::LogicalTypeId::VARCHAR));
}

}  // namespace
}  // namespace sdb::catalog
