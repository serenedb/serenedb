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
#include <gtest/gtest.h>
#include <velox/core/ITypedExpr.h>

#include <iresearch/analysis/tokenizers.hpp>
#include <iresearch/search/boolean_filter.hpp>
#include <iresearch/search/granular_range_filter.hpp>
#include <iresearch/search/range_filter.hpp>
#include <iresearch/search/term_filter.hpp>

#include "app/options/program_options.h"
#include "axiom/optimizer/Optimization.h"
#include "catalog/mangling.h"
#include "catalog/table.h"
#include "connector/search_filter_builder.hpp"
#include "connector/serenedb_connector.hpp"
#include "general_server/state.h"
#include "pg/sql_parser.h"
#include "pg/sql_utils.h"
#include "search_filter_printer.hpp"
#include "utils/query_string.h"
#include "velox/expression/Expr.h"

LIBPG_QUERY_INCLUDES_BEGIN
#include "postgres.h"

#include "nodes/pg_list.h"
LIBPG_QUERY_INCLUDES_END

#include "connector_mock.hpp"
#include "pg/pg_functions_registration.hpp"

namespace {

using namespace sdb;
using namespace sdb::connector::test;

class SearchFilterBuilderTest : public ::testing::Test {
 public:
  static void SetUpTestCase() {
    velox::memory::MemoryManager::testingSetInstance({});
    gServerState.SetGTest(true);
    gServerState.SetRole(ServerState::Role::Single);
    sdb::pg::RegisterVeloxFunctionsAndTypes();
  }

  static void TearDownTestCase() { gServerState.Reset(); }

  void SetUp() final { _pg_memory_ctx = pg::CreateMemoryContext(); }

  void TearDown() final { pg::ResetMemoryContext(*_pg_memory_ctx); }

  void AssertFilter(
    const irs::And& expected, std::string_view query_string,
    std::vector<std::unique_ptr<const axiom::connector::Column>>&& columns,
    bool must_succeed) {
    SCOPED_TRACE(testing::Message("Parsing: <") << query_string << ">");
    irs::And root;
    MockConnector connector("mock", nullptr);
    pg::Params params;
    pg::Objects objects;
    QueryString qs(query_string);
    auto root_list = pg::Parse(*_pg_memory_ctx, qs);
    auto* raw_stmt = castNode(RawStmt, list_nth(root_list, 0));
    pg::ParamIndex max_bind_param_idx = 0;
    pg::Collect("testdb", *raw_stmt, objects, max_bind_param_idx);
    ASSERT_EQ(objects.getRelations().size(), 1)
      << "Code is designed to work with just one table in query";
    // Fake resolving - just make mock for table needed
    auto& rel = *objects.getRelations().begin();
    catalog::CreateTableOptions opts;
    opts.name = rel.first.relation;
    for (auto& col : columns) {
      auto& serene_column = basics::downCast<connector::SereneDBColumn>(*col);
      opts.columns.emplace_back(
        sdb::catalog::Column{.id = serene_column.Id(),
                             .type = serene_column.type(),
                             .name = serene_column.name()});
    }
    rel.second.object =
      std::make_shared<catalog::Table>(std::move(opts), ObjectId{1});
    auto table = std::make_shared<TableMock>(
      connector, std::string{rel.first.relation}, std::move(columns),
      folly::F14FastMap<std::string, velox::Variant>{});
    rel.second.table = table;

    auto transaction = std::make_shared<query::Transaction>();
    auto query_ctx =
      std::make_shared<query::QueryContext>(transaction, objects);
    pg::UniqueIdGenerator id_generator;
    auto query_desc = pg::AnalyzeVelox(*raw_stmt, qs, objects, id_generator,
                                       *query_ctx, params, nullptr, nullptr);

    auto plan = axiom::optimizer::Optimization::toVeloxPlan(
      *query_desc.root, *query_ctx->query_memory_pool, {},
      {.numWorkers = 1, .numDrivers = 1});
    auto velox_query_ctx = velox::core::QueryCtx::create();
    velox::exec::SimpleExpressionEvaluator evaluator(
      velox_query_ctx.get(), query_ctx->query_memory_pool.get());
    for (auto& conjunct :
         connector._table_handles[rel.first.relation]->AcceptedFilters()) {
      auto res = connector::search::ExprToFilter(root, evaluator, conjunct,
                                                 table->columnMap());
      ASSERT_EQ(res.ok(), must_succeed);
    }
    if (must_succeed) {
      ASSERT_EQ(root, expected);
    }
  }

  template<typename T>
  std::string MakeFieldName(catalog::Column::Id column_id) {
    std::string field_name;
    basics::StrResize(field_name, sizeof(column_id));
    absl::big_endian::Store(field_name.data(), column_id);
    if constexpr (std::is_same_v<T, bool>) {
      sdb::search::mangling::MangleBool(field_name);
    } else if constexpr (std::is_same_v<T, velox::StringView>) {
      sdb::search::mangling::MangleString(field_name);
    } else if constexpr (std::is_floating_point_v<T> || std::is_integral_v<T>) {
      sdb::search::mangling::MangleNumeric(field_name);
    } else {
      EXPECT_FALSE(true) << "Unexpected conversion";
    }
    return field_name;
  }

  template<typename Filter, typename Source>
  auto& AddFilter(Source& parent) {
    if constexpr (std::is_same_v<irs::Not, Source>) {
      return parent.template filter<Filter>();
    } else {
      return parent.template add<Filter>();
    }
  }

  template<typename T, typename Filter>
  void AddTermFilter(Filter& root, catalog::Column::Id column, const T& value) {
    auto& term = AddFilter<irs::ByTerm>(root);
    *term.mutable_field() = MakeFieldName<T>(column);
    if constexpr (std::is_same_v<T, bool>) {
      term.mutable_options()->term.assign(
        irs::ViewCast<irs::byte_type>(irs::BooleanTokenizer::value(value)));
    } else if constexpr (std::is_same_v<T, velox::StringView>) {
      irs::StringTokenizer stream;
      const irs::TermAttr* token = irs::get<irs::TermAttr>(stream);
      stream.reset(value);
      stream.next();
      term.mutable_options()->term.assign(token->value);
    } else if constexpr (std::is_floating_point_v<T> || std::is_integral_v<T>) {
      irs::NumericTokenizer stream;
      const irs::TermAttr* token = irs::get<irs::TermAttr>(stream);
      stream.reset(value);
      stream.next();
      term.mutable_options()->term.assign(token->value);
    } else {
      ASSERT_FALSE(true) << "Unexpected term type";
    }
  }

  template<typename T, typename Filter>
  void AddRangeFilter(Filter& root, catalog::Column::Id column,
                      const std::optional<T>& min_value, bool min_inclusive,
                      const std::optional<T>& max_value, bool max_inclusive) {
    if constexpr (std::is_same_v<T, velox::StringView>) {
      // Use ByRange for strings
      auto& range = AddFilter<irs::ByRange>(root);
      *range.mutable_field() = MakeFieldName<T>(column);
      auto& options = range.mutable_options()->range;
      irs::StringTokenizer stream;
      const irs::TermAttr* token = irs::get<irs::TermAttr>(stream);
      if (min_value.has_value()) {
        stream.reset(*min_value);
        stream.next();
        options.min.assign(token->value);
        options.min_type =
          min_inclusive ? irs::BoundType::Inclusive : irs::BoundType::Exclusive;
      } else {
        options.min_type = irs::BoundType::Unbounded;
      }

      if (max_value.has_value()) {
        stream.reset(*max_value);
        stream.next();
        options.max.assign(token->value);
        options.max_type =
          max_inclusive ? irs::BoundType::Inclusive : irs::BoundType::Exclusive;
      } else {
        options.max_type = irs::BoundType::Unbounded;
      }
    } else if constexpr (std::is_floating_point_v<T> || std::is_integral_v<T>) {
      // Use ByGranularRange for numerics
      auto& range = AddFilter<irs::ByGranularRange>(root);
      *range.mutable_field() = MakeFieldName<T>(column);
      auto& options = range.mutable_options()->range;
      irs::NumericTokenizer stream;
      if (min_value.has_value()) {
        stream.reset(*min_value);
        irs::SetGranularTerm(options.min, stream);
        options.min_type =
          min_inclusive ? irs::BoundType::Inclusive : irs::BoundType::Exclusive;
      } else {
        options.min_type = irs::BoundType::Unbounded;
      }

      if (max_value.has_value()) {
        stream.reset(*max_value);
        irs::SetGranularTerm(options.max, stream);
        options.max_type =
          max_inclusive ? irs::BoundType::Inclusive : irs::BoundType::Exclusive;
      } else {
        options.max_type = irs::BoundType::Unbounded;
      }
    } else if constexpr (std::is_same_v<T, bool>) {
      // For bool, use term filter as there's no meaningful range
      ASSERT_FALSE(true) << "Range queries on bool type not supported";
    } else {
      ASSERT_FALSE(true) << "Unexpected range type";
    }
  }

 protected:
  pg::MemoryContextPtr _pg_memory_ctx;
  static ServerState gServerState;
};

ServerState SearchFilterBuilderTest::gServerState;

// ============================================================================
// Basic OR Tests
// ============================================================================

TEST_F(SearchFilterBuilderTest, test_SimpleDisjunction) {
  std::vector<std::unique_ptr<const axiom::connector::Column>> columns;
  irs::And expected;
  auto& or_filter = expected.add<irs::Or>();
  AddTermFilter<int32_t>(or_filter, 1, 10);
  AddTermFilter<int32_t>(or_filter, 1, 11);
  columns.emplace_back(
    std::make_unique<connector::SereneDBColumn>("b", velox::INTEGER(), 1));
  AssertFilter(expected, "SELECT * FROM foo WHERE b = 10 OR b = 11",
               std::move(columns), true);
}

TEST_F(SearchFilterBuilderTest, test_SimpleDisjunctionDifferentFields) {
  std::vector<std::unique_ptr<const axiom::connector::Column>> columns;
  irs::And expected;
  auto& or_filter = expected.add<irs::Or>();
  AddTermFilter<int32_t>(or_filter, 300, 10);
  AddTermFilter<velox::StringView>(or_filter, 512, velox::StringView{"foobar"});
  columns.emplace_back(
    std::make_unique<connector::SereneDBColumn>("a", velox::INTEGER(), 300));
  columns.emplace_back(
    std::make_unique<connector::SereneDBColumn>("b", velox::VARCHAR(), 512));
  AssertFilter(expected, "SELECT * FROM foo WHERE a = '10' OR b = 'foobar'",
               std::move(columns), true);
}

TEST_F(SearchFilterBuilderTest, test_MultipleOr) {
  std::vector<std::unique_ptr<const axiom::connector::Column>> columns;
  irs::And expected;
  auto& or_filter = expected.add<irs::Or>();
  AddTermFilter<int32_t>(or_filter, 1, 5);
  AddTermFilter<int32_t>(or_filter, 1, 10);
  AddTermFilter<int32_t>(or_filter, 1, 15);
  columns.emplace_back(
    std::make_unique<connector::SereneDBColumn>("a", velox::INTEGER(), 1));
  AssertFilter(expected, "SELECT * FROM foo WHERE a = 5 OR a = 10 OR a = 15",
               std::move(columns), true);
}

// ============================================================================
// Basic AND Tests
// ============================================================================

TEST_F(SearchFilterBuilderTest, test_SimpleConjunction) {
  std::vector<std::unique_ptr<const axiom::connector::Column>> columns;
  irs::And expected;
  AddTermFilter<int32_t>(expected, 1, 10);
  AddTermFilter<int32_t>(expected, 2, 20);
  columns.emplace_back(
    std::make_unique<connector::SereneDBColumn>("a", velox::INTEGER(), 1));
  columns.emplace_back(
    std::make_unique<connector::SereneDBColumn>("b", velox::INTEGER(), 2));
  AssertFilter(expected, "SELECT * FROM foo WHERE a = 10 AND b = 20",
               std::move(columns), true);
}

TEST_F(SearchFilterBuilderTest, test_MultipleAnd) {
  std::vector<std::unique_ptr<const axiom::connector::Column>> columns;
  irs::And expected;
  AddTermFilter<int32_t>(expected, 1000, 10);
  AddTermFilter<velox::StringView>(expected, 2000, velox::StringView{"test"});
  AddTermFilter<bool>(expected, 3000, true);
  columns.emplace_back(
    std::make_unique<connector::SereneDBColumn>("a", velox::INTEGER(), 1000));
  columns.emplace_back(
    std::make_unique<connector::SereneDBColumn>("b", velox::VARCHAR(), 2000));
  columns.emplace_back(
    std::make_unique<connector::SereneDBColumn>("c", velox::BOOLEAN(), 3000));
  AssertFilter(expected,
               "SELECT * FROM foo WHERE a = 10 AND b = 'test' AND c = true",
               std::move(columns), true);
}

// ============================================================================
// NOT Tests
// ============================================================================

TEST_F(SearchFilterBuilderTest, test_NotTerm) {
  std::vector<std::unique_ptr<const axiom::connector::Column>> columns;
  irs::And expected;
  auto& not_filter = expected.add<irs::Not>();
  AddTermFilter<int32_t>(not_filter, 1, 10);
  columns.emplace_back(
    std::make_unique<connector::SereneDBColumn>("a", velox::INTEGER(), 1));
  AssertFilter(expected, "SELECT * FROM foo WHERE NOT (a = '10')",
               std::move(columns), true);
}

TEST_F(SearchFilterBuilderTest, test_NotOr) {
  std::vector<std::unique_ptr<const axiom::connector::Column>> columns;
  irs::And expected;
  auto& not_filter = expected.add<irs::Not>();
  auto& or_filter = AddFilter<irs::Or>(not_filter);
  AddTermFilter<int32_t>(or_filter, 1, 10);
  AddTermFilter<int32_t>(or_filter, 1, 20);
  columns.emplace_back(
    std::make_unique<connector::SereneDBColumn>("a", velox::INTEGER(), 1));
  AssertFilter(expected, "SELECT * FROM foo WHERE NOT (a = 10 OR a = 20)",
               std::move(columns), true);
}

TEST_F(SearchFilterBuilderTest, test_NotAnd) {
  std::vector<std::unique_ptr<const axiom::connector::Column>> columns;
  irs::And expected;
  auto& not_filter = expected.add<irs::Not>();
  auto& and_filter = AddFilter<irs::And>(not_filter);
  AddTermFilter<int32_t>(and_filter, 1, 10);
  AddTermFilter<int32_t>(and_filter, 2, 20);
  columns.emplace_back(
    std::make_unique<connector::SereneDBColumn>("a", velox::INTEGER(), 1));
  columns.emplace_back(
    std::make_unique<connector::SereneDBColumn>("b", velox::INTEGER(), 2));
  AssertFilter(expected, "SELECT * FROM foo WHERE NOT (a = 10 AND b = 20)",
               std::move(columns), true);
}

// ============================================================================
// Comparison Operator Tests - Less Than
// ============================================================================

TEST_F(SearchFilterBuilderTest, test_LessThanInteger) {
  std::vector<std::unique_ptr<const axiom::connector::Column>> columns;
  irs::And expected;
  AddRangeFilter<int32_t>(expected, 1, std::nullopt, false, 100, false);
  columns.emplace_back(
    std::make_unique<connector::SereneDBColumn>("a", velox::INTEGER(), 1));
  AssertFilter(expected, "SELECT * FROM foo WHERE a < 100", std::move(columns),
               true);
}

TEST_F(SearchFilterBuilderTest, test_LessThanString) {
  std::vector<std::unique_ptr<const axiom::connector::Column>> columns;
  irs::And expected;
  AddRangeFilter<velox::StringView>(expected, 1, std::nullopt, false,
                                    velox::StringView{"xyz"}, false);
  columns.emplace_back(
    std::make_unique<connector::SereneDBColumn>("a", velox::VARCHAR(), 1));
  AssertFilter(expected, "SELECT * FROM foo WHERE a < 'xyz'",
               std::move(columns), true);
}

// ============================================================================
// Comparison Operator Tests - Less Than or Equal
// ============================================================================

TEST_F(SearchFilterBuilderTest, test_LessThanOrEqualInteger) {
  std::vector<std::unique_ptr<const axiom::connector::Column>> columns;
  irs::And expected;
  AddRangeFilter<int32_t>(expected, 1, std::nullopt, false, 100, true);
  columns.emplace_back(
    std::make_unique<connector::SereneDBColumn>("a", velox::INTEGER(), 1));
  AssertFilter(expected, "SELECT * FROM foo WHERE a <= 100", std::move(columns),
               true);
}

TEST_F(SearchFilterBuilderTest, test_LessThanOrEqualString) {
  std::vector<std::unique_ptr<const axiom::connector::Column>> columns;
  irs::And expected;
  AddRangeFilter<velox::StringView>(expected, 1, std::nullopt, false,
                                    velox::StringView{"test"}, true);
  columns.emplace_back(
    std::make_unique<connector::SereneDBColumn>("a", velox::VARCHAR(), 1));
  AssertFilter(expected, "SELECT * FROM foo WHERE a <= 'test'",
               std::move(columns), true);
}

// ============================================================================
// Comparison Operator Tests - Greater Than
// ============================================================================

TEST_F(SearchFilterBuilderTest, test_GreaterThanInteger) {
  std::vector<std::unique_ptr<const axiom::connector::Column>> columns;
  irs::And expected;
  AddRangeFilter<int32_t>(expected, 1, 50, false, std::nullopt, false);
  columns.emplace_back(
    std::make_unique<connector::SereneDBColumn>("a", velox::INTEGER(), 1));
  AssertFilter(expected, "SELECT * FROM foo WHERE a > 50", std::move(columns),
               true);
}

TEST_F(SearchFilterBuilderTest, test_GreaterThanString) {
  std::vector<std::unique_ptr<const axiom::connector::Column>> columns;
  irs::And expected;
  AddRangeFilter<velox::StringView>(expected, 1, velox::StringView{"abc"},
                                    false, std::nullopt, false);
  columns.emplace_back(
    std::make_unique<connector::SereneDBColumn>("a", velox::VARCHAR(), 1));
  AssertFilter(expected, "SELECT * FROM foo WHERE a > 'abc'",
               std::move(columns), true);
}

// ============================================================================
// Comparison Operator Tests - Greater Than or Equal
// ============================================================================

TEST_F(SearchFilterBuilderTest, test_GreaterThanOrEqualInteger) {
  std::vector<std::unique_ptr<const axiom::connector::Column>> columns;
  irs::And expected;
  AddRangeFilter<int32_t>(expected, 1, 50, true, std::nullopt, false);
  columns.emplace_back(
    std::make_unique<connector::SereneDBColumn>("a", velox::INTEGER(), 1));
  AssertFilter(expected, "SELECT * FROM foo WHERE a >= 50", std::move(columns),
               true);
}

TEST_F(SearchFilterBuilderTest, test_GreaterThanOrEqualString) {
  std::vector<std::unique_ptr<const axiom::connector::Column>> columns;
  irs::And expected;
  AddRangeFilter<velox::StringView>(expected, 1, velox::StringView{"start"},
                                    true, std::nullopt, false);
  columns.emplace_back(
    std::make_unique<connector::SereneDBColumn>("a", velox::VARCHAR(), 1));
  AssertFilter(expected, "SELECT * FROM foo WHERE a >= 'start'",
               std::move(columns), true);
}

// ============================================================================
// BETWEEN Tests
// ============================================================================

TEST_F(SearchFilterBuilderTest, test_BetweenInteger) {
  std::vector<std::unique_ptr<const axiom::connector::Column>> columns;
  irs::And expected;
  AddRangeFilter<int32_t>(expected, 500, 10, true, std::nullopt, false);
  AddRangeFilter<int32_t>(expected, 500, std::nullopt, false, 100, true);
  columns.emplace_back(
    std::make_unique<connector::SereneDBColumn>("a", velox::INTEGER(), 500));
  AssertFilter(expected, "SELECT * FROM foo WHERE a BETWEEN 10 AND 100",
               std::move(columns), true);
}

TEST_F(SearchFilterBuilderTest, test_BetweenSymmetricInteger) {
  std::vector<std::unique_ptr<const axiom::connector::Column>> columns;
  irs::And expected;
  AddRangeFilter<int32_t>(expected, 500, 10, true, std::nullopt, false);
  AddRangeFilter<int32_t>(expected, 500, std::nullopt, false, 100, true);
  columns.emplace_back(
    std::make_unique<connector::SereneDBColumn>("a", velox::INTEGER(), 500));
  AssertFilter(expected, "SELECT * FROM foo WHERE a BETWEEN SYMMETRIC 100 AND 10",
               std::move(columns), true);
}

TEST_F(SearchFilterBuilderTest, test_BetweenString) {
  std::vector<std::unique_ptr<const axiom::connector::Column>> columns;
  irs::And expected;
  AddRangeFilter<velox::StringView>(expected, 1, velox::StringView{"apple"},
                                    true, std::nullopt, false);
  AddRangeFilter<velox::StringView>(expected, 1, std::nullopt, false,
                                    velox::StringView{"orange"}, true);
  columns.emplace_back(
    std::make_unique<connector::SereneDBColumn>("a", velox::VARCHAR(), 1));
  AssertFilter(expected,
               "SELECT * FROM foo WHERE a BETWEEN 'apple' AND 'orange'",
               std::move(columns), true);
}

TEST_F(SearchFilterBuilderTest, test_NotBetween) {
  std::vector<std::unique_ptr<const axiom::connector::Column>> columns;
  irs::And expected;
  // NOT BETWEEN 10 AND 50 is equivalent to (a < 10 OR a > 50)
  auto& or_filter = expected.add<irs::Or>();
  AddRangeFilter<int32_t>(or_filter, 1, std::nullopt, false, 10, false);
  AddRangeFilter<int32_t>(or_filter, 1, 50, false, std::nullopt, false);
  columns.emplace_back(
    std::make_unique<connector::SereneDBColumn>("a", velox::INTEGER(), 1));
  AssertFilter(expected, "SELECT * FROM foo WHERE a NOT BETWEEN 10 AND 50",
               std::move(columns), true);
}

// ============================================================================
// Combined AND/OR Tests
// ============================================================================

TEST_F(SearchFilterBuilderTest, test_AndWithOr) {
  std::vector<std::unique_ptr<const axiom::connector::Column>> columns;
  irs::And expected;
  AddTermFilter<int32_t>(expected, 400, 10);
  auto& or_filter = expected.add<irs::Or>();
  AddTermFilter<velox::StringView>(or_filter, 800, velox::StringView{"foo"});
  AddTermFilter<velox::StringView>(or_filter, 800, velox::StringView{"bar"});
  columns.emplace_back(
    std::make_unique<connector::SereneDBColumn>("a", velox::INTEGER(), 400));
  columns.emplace_back(
    std::make_unique<connector::SereneDBColumn>("b", velox::VARCHAR(), 800));
  AssertFilter(expected,
               "SELECT * FROM foo WHERE a = 10 AND (b = 'foo' OR b = 'bar')",
               std::move(columns), true);
}

TEST_F(SearchFilterBuilderTest, test_OrWithAnd) {
  std::vector<std::unique_ptr<const axiom::connector::Column>> columns;
  irs::And expected;
  auto& or_filter = expected.add<irs::Or>();
  auto& and1 = or_filter.add<irs::And>();
  AddTermFilter<int32_t>(and1, 1, 10);
  AddTermFilter<velox::StringView>(and1, 2, velox::StringView{"test"});
  auto& and2 = or_filter.add<irs::And>();
  AddTermFilter<int32_t>(and2, 1, 20);
  AddTermFilter<velox::StringView>(and2, 2, velox::StringView{"demo"});
  columns.emplace_back(
    std::make_unique<connector::SereneDBColumn>("a", velox::INTEGER(), 1));
  columns.emplace_back(
    std::make_unique<connector::SereneDBColumn>("b", velox::VARCHAR(), 2));
  AssertFilter(expected,
               "SELECT * FROM foo WHERE (a = 10 AND b = 'test') OR (a = 20 AND "
               "b = 'demo')",
               std::move(columns), true);
}

// ============================================================================
// Combined with Comparison Operators
// ============================================================================

TEST_F(SearchFilterBuilderTest, test_AndWithComparison) {
  std::vector<std::unique_ptr<const axiom::connector::Column>> columns;
  irs::And expected;
  AddRangeFilter<int32_t>(expected, 1, 10, true, std::nullopt, false);
  AddRangeFilter<int32_t>(expected, 1, std::nullopt, false, 100, true);
  columns.emplace_back(
    std::make_unique<connector::SereneDBColumn>("a", velox::INTEGER(), 1));
  AssertFilter(expected, "SELECT * FROM foo WHERE a >= 10 AND a <= 100",
               std::move(columns), true);
}

TEST_F(SearchFilterBuilderTest, test_OrWithComparison) {
  std::vector<std::unique_ptr<const axiom::connector::Column>> columns;
  irs::And expected;
  auto& or_filter = expected.add<irs::Or>();
  AddRangeFilter<int32_t>(or_filter, 1, std::nullopt, false, 10, false);
  AddRangeFilter<int32_t>(or_filter, 1, 100, false, std::nullopt, false);
  columns.emplace_back(
    std::make_unique<connector::SereneDBColumn>("a", velox::INTEGER(), 1));
  AssertFilter(expected, "SELECT * FROM foo WHERE a < 10 OR a > 100",
               std::move(columns), true);
}

TEST_F(SearchFilterBuilderTest, test_MixedEqualsAndComparison) {
  std::vector<std::unique_ptr<const axiom::connector::Column>> columns;
  irs::And expected;
  AddTermFilter<velox::StringView>(expected, 1, velox::StringView{"active"});
  AddRangeFilter<int32_t>(expected, 2, 18, true, std::nullopt, false);
  columns.emplace_back(
    std::make_unique<connector::SereneDBColumn>("status", velox::VARCHAR(), 1));
  columns.emplace_back(
    std::make_unique<connector::SereneDBColumn>("age", velox::INTEGER(), 2));
  AssertFilter(expected,
               "SELECT * FROM foo WHERE status = 'active' AND age >= 18",
               std::move(columns), true);
}

// ============================================================================
// Combined with NOT
// ============================================================================

TEST_F(SearchFilterBuilderTest, test_NotWithComparison) {
  std::vector<std::unique_ptr<const axiom::connector::Column>> columns;
  irs::And expected;
  // NOT (a > 50) is equivalent to a <= 50
  AddRangeFilter<int32_t>(expected, 1, std::nullopt, false, 50, true);
  columns.emplace_back(
    std::make_unique<connector::SereneDBColumn>("a", velox::INTEGER(), 1));
  AssertFilter(expected, "SELECT * FROM foo WHERE NOT (a > 50)",
               std::move(columns), true);
}

TEST_F(SearchFilterBuilderTest, test_NotLessThan) {
  std::vector<std::unique_ptr<const axiom::connector::Column>> columns;
  irs::And expected;
  // NOT (a < 100) is equivalent to a >= 100
  AddRangeFilter<int32_t>(expected, 1, 100, true, std::nullopt, false);
  columns.emplace_back(
    std::make_unique<connector::SereneDBColumn>("a", velox::INTEGER(), 1));
  AssertFilter(expected, "SELECT * FROM foo WHERE NOT (a < 100)",
               std::move(columns), true);
}

TEST_F(SearchFilterBuilderTest, test_NotGreaterThanOrEqual) {
  std::vector<std::unique_ptr<const axiom::connector::Column>> columns;
  irs::And expected;
  // NOT (a >= 50) is equivalent to a < 50
  AddRangeFilter<int32_t>(expected, 1, std::nullopt, false, 50, false);
  columns.emplace_back(
    std::make_unique<connector::SereneDBColumn>("a", velox::INTEGER(), 1));
  AssertFilter(expected, "SELECT * FROM foo WHERE NOT (a >= 50)",
               std::move(columns), true);
}

TEST_F(SearchFilterBuilderTest, test_NotLessThanOrEqual) {
  std::vector<std::unique_ptr<const axiom::connector::Column>> columns;
  irs::And expected;
  // NOT (a <= 25) is equivalent to a > 25
  AddRangeFilter<int32_t>(expected, 1, 25, false, std::nullopt, false);
  columns.emplace_back(
    std::make_unique<connector::SereneDBColumn>("a", velox::INTEGER(), 1));
  AssertFilter(expected, "SELECT * FROM foo WHERE NOT (a <= 25)",
               std::move(columns), true);
}

TEST_F(SearchFilterBuilderTest, test_AndWithNotOr) {
  std::vector<std::unique_ptr<const axiom::connector::Column>> columns;
  irs::And expected;
  AddTermFilter<bool>(expected, 1, true);
  auto& not_filter = expected.add<irs::Not>();
  auto& or_filter = AddFilter<irs::Or>(not_filter);
  AddTermFilter<int32_t>(or_filter, 2, 10);
  AddTermFilter<int32_t>(or_filter, 2, 20);
  columns.emplace_back(
    std::make_unique<connector::SereneDBColumn>("active", velox::BOOLEAN(), 1));
  columns.emplace_back(
    std::make_unique<connector::SereneDBColumn>("value", velox::INTEGER(), 2));
  AssertFilter(
    expected,
    "SELECT * FROM foo WHERE active = true AND NOT (value = 10 OR value = 20)",
    std::move(columns), true);
}

TEST_F(SearchFilterBuilderTest, test_OrWithNot) {
  std::vector<std::unique_ptr<const axiom::connector::Column>> columns;
  irs::And expected;
  auto& or_filter = expected.add<irs::Or>();
  AddTermFilter<int32_t>(or_filter, 1, 5);
  auto& not_filter = or_filter.add<irs::And>().add<irs::Not>();
  AddTermFilter<velox::StringView>(not_filter, 2, velox::StringView{"test"});
  columns.emplace_back(
    std::make_unique<connector::SereneDBColumn>("a", velox::INTEGER(), 1));
  columns.emplace_back(
    std::make_unique<connector::SereneDBColumn>("b", velox::VARCHAR(), 2));
  AssertFilter(expected, "SELECT * FROM foo WHERE a = 5 OR NOT (b = 'test')",
               std::move(columns), true);
}

// ============================================================================
// Complex Nested Tests
// ============================================================================

TEST_F(SearchFilterBuilderTest, test_ComplexNested1) {
  std::vector<std::unique_ptr<const axiom::connector::Column>> columns;
  irs::And expected;
  auto& or_filter = expected.add<irs::Or>();

  auto& and1 = or_filter.add<irs::And>();
  auto& and_b = and1.add<irs::And>();
  AddRangeFilter<int32_t>(and_b, 1, 18, true, std::nullopt, false);
  AddRangeFilter<int32_t>(and_b, 1, std::nullopt, false, 65, true);
  AddTermFilter<velox::StringView>(and1, 2, velox::StringView{"active"});

  auto& and2 = or_filter.add<irs::And>();
  AddRangeFilter<int32_t>(and2, 1, 65, false, std::nullopt, false);
  AddTermFilter<velox::StringView>(and2, 2, velox::StringView{"retired"});

  columns.emplace_back(
    std::make_unique<connector::SereneDBColumn>("age", velox::INTEGER(), 1));
  columns.emplace_back(
    std::make_unique<connector::SereneDBColumn>("status", velox::VARCHAR(), 2));
  AssertFilter(expected,
               "SELECT * FROM foo WHERE (age BETWEEN 18 AND 65 AND status = "
               "'active') OR (age > 65 AND status = 'retired')",
               std::move(columns), true);
}

TEST_F(SearchFilterBuilderTest, test_ComplexNested2) {
  std::vector<std::unique_ptr<const axiom::connector::Column>> columns;
  irs::And expected;

  AddRangeFilter<int32_t>(expected, 1024, 100, true, std::nullopt, false);

  auto& or_filter = expected.add<irs::Or>();
  AddTermFilter<velox::StringView>(or_filter, 2048,
                                   velox::StringView{"premium"});
  AddTermFilter<velox::StringView>(or_filter, 2048, velox::StringView{"gold"});

  auto& not_filter = expected.add<irs::Not>();
  AddTermFilter<bool>(not_filter, 4096, false);

  columns.emplace_back(std::make_unique<connector::SereneDBColumn>(
    "price", velox::INTEGER(), 1024));
  columns.emplace_back(std::make_unique<connector::SereneDBColumn>(
    "tier", velox::VARCHAR(), 2048));
  columns.emplace_back(std::make_unique<connector::SereneDBColumn>(
    "enabled", velox::BOOLEAN(), 4096));
  AssertFilter(expected,
               "SELECT * FROM foo WHERE price >= 100 AND (tier = 'premium' OR "
               "tier = 'gold') AND NOT (enabled = false)",
               std::move(columns), true);
}

TEST_F(SearchFilterBuilderTest, test_ComplexNested3) {
  std::vector<std::unique_ptr<const axiom::connector::Column>> columns;
  irs::And expected;

  auto& or_outer = expected.add<irs::Or>();

  auto& and1 = or_outer.add<irs::And>();
  AddTermFilter<velox::StringView>(and1, 1, velox::StringView{"USA"});
  AddRangeFilter<int32_t>(and1, 2, std::nullopt, false, 1000, false);

  auto& and2 = or_outer.add<irs::And>();
  AddTermFilter<velox::StringView>(and2, 1, velox::StringView{"UK"});
  AddRangeFilter<int32_t>(and2, 2, std::nullopt, false, 800, false);

  auto& and3 = or_outer.add<irs::And>();
  auto& not_filter = and3.add<irs::Not>();
  auto& or_inner = AddFilter<irs::Or>(not_filter);
  AddTermFilter<velox::StringView>(or_inner, 1, velox::StringView{"USA"});
  AddTermFilter<velox::StringView>(or_inner, 1, velox::StringView{"UK"});
  AddRangeFilter<int32_t>(and3, 2, std::nullopt, false, 500, false);

  columns.emplace_back(std::make_unique<connector::SereneDBColumn>(
    "country", velox::VARCHAR(), 1));
  columns.emplace_back(
    std::make_unique<connector::SereneDBColumn>("price", velox::INTEGER(), 2));
  AssertFilter(expected,
               "SELECT * FROM foo WHERE (country = 'USA' AND price < 1000) OR "
               "(country = 'UK' AND price < 800) OR (NOT (country = 'USA' OR "
               "country = 'UK') AND price < 500)",
               std::move(columns), true);
}

// ============================================================================
// Implicit Cast Tests
// ============================================================================

TEST_F(SearchFilterBuilderTest, test_ImplicitCastIntegerToString) {
  std::vector<std::unique_ptr<const axiom::connector::Column>> columns;
  irs::And expected;
  AddTermFilter<int32_t>(expected, 1, 42);
  columns.emplace_back(
    std::make_unique<connector::SereneDBColumn>("a", velox::INTEGER(), 1));
  AssertFilter(expected, "SELECT * FROM foo WHERE a = '42'", std::move(columns),
               true);
}

TEST_F(SearchFilterBuilderTest, test_ImplicitCastInComparison) {
  std::vector<std::unique_ptr<const axiom::connector::Column>> columns;
  irs::And expected;
  AddRangeFilter<int32_t>(expected, 1, 10, true, std::nullopt, false);
  columns.emplace_back(
    std::make_unique<connector::SereneDBColumn>("a", velox::INTEGER(), 1));
  AssertFilter(expected, "SELECT * FROM foo WHERE a >= '10'",
               std::move(columns), true);
}

TEST_F(SearchFilterBuilderTest, test_ImplicitCastInBetween) {
  std::vector<std::unique_ptr<const axiom::connector::Column>> columns;
  irs::And expected;
  AddRangeFilter<int32_t>(expected, 65535, 5, true, std::nullopt, false);
  AddRangeFilter<int32_t>(expected, 65535, std::nullopt, false, 15, true);
  columns.emplace_back(
    std::make_unique<connector::SereneDBColumn>("a", velox::INTEGER(), 65535));
  AssertFilter(expected, "SELECT * FROM foo WHERE a BETWEEN '5' AND '15'",
               std::move(columns), true);
}

// ============================================================================
// Multiple Comparisons on Same Field
// ============================================================================

TEST_F(SearchFilterBuilderTest, test_MultipleComparisonsOnSameField) {
  std::vector<std::unique_ptr<const axiom::connector::Column>> columns;
  irs::And expected;
  AddRangeFilter<int32_t>(expected, 1, 10, false, std::nullopt, false);
  AddRangeFilter<int32_t>(expected, 1, std::nullopt, false, 100, false);
  columns.emplace_back(
    std::make_unique<connector::SereneDBColumn>("a", velox::INTEGER(), 1));
  AssertFilter(expected, "SELECT * FROM foo WHERE a > 10 AND a < 100",
               std::move(columns), true);
}

TEST_F(SearchFilterBuilderTest, test_MixedOperatorsOnSameField) {
  std::vector<std::unique_ptr<const axiom::connector::Column>> columns;
  irs::And expected;
  auto& or_filter = expected.add<irs::Or>();
  AddTermFilter<int32_t>(or_filter, 1, 0);
  AddRangeFilter<int32_t>(or_filter, 1, 100, true, std::nullopt, false);
  columns.emplace_back(
    std::make_unique<connector::SereneDBColumn>("value", velox::INTEGER(), 1));
  AssertFilter(expected, "SELECT * FROM foo WHERE value = 0 OR value >= 100",
               std::move(columns), true);
}

}  // namespace
