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
#include <iresearch/search/terms_filter.hpp>
#include <iresearch/search/wildcard_filter.hpp>

#include "app/options/program_options.h"
#include "axiom/common/Session.h"
#include "axiom/optimizer/Optimization.h"
#include "axiom/optimizer/QueryGraphContext.h"
#include "axiom/optimizer/VeloxHistory.h"
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
#include "pg/system_catalog.h"

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

    // resolve system functions that could be used in queries.
    // copy of logic from Resolver but only for system functions
    auto functions = std::move(objects.getFunctions());
    for (auto& [name, data] : functions) {
      if (const auto func = pg::GetFunction(name.relation)) {
        auto& new_data = objects.ensureFunction(name.schema, name.relation);
        new_data = data;
        new_data.object = std::move(func);
      }
    }

    MockConnector connector("mock", nullptr);
    auto table = std::make_shared<TableMock>(
      connector, std::string{rel.first.relation}, std::move(columns),
      folly::F14FastMap<std::string, velox::Variant>{});
    rel.second.table = table;
    auto transaction = std::make_shared<query::Transaction>();
    auto query_ctx =
      std::make_shared<query::QueryContext>(transaction, objects);

    {
      // Table handles gather espressions for filter. Expressions could hold
      // memory from pool so we must make sure all expressions are deleted
      // before leaf pool to avoid trigger leak detection. So scope exits and
      // local block are important here.
      SCOPE_EXIT {
        connector._table_handles.clear();
        table.reset();
      };
      irs::And root;
      pg::UniqueIdGenerator id_generator;
      auto query_desc = pg::AnalyzeVelox(*raw_stmt, qs, objects, id_generator,
                                         *query_ctx, params, nullptr, nullptr);

      velox::exec::SimpleExpressionEvaluator evaluator(
        query_ctx->velox_query_ctx.get(), query_ctx->query_memory_pool.get());

      auto allocator = std::make_unique<velox::HashStringAllocator>(
        query_ctx->query_memory_pool.get());
      auto context =
        std::make_unique<axiom::optimizer::QueryGraphContext>(*allocator);
      axiom::optimizer::queryCtx() = context.get();
      SCOPE_EXIT { axiom::optimizer::queryCtx() = nullptr; };

      axiom::optimizer::VeloxHistory history;

      auto session =
        std::make_shared<axiom::Session>(query_ctx->velox_query_ctx->queryId());

      axiom::optimizer::Optimization opt{session,
                                         *query_desc.root,
                                         history,
                                         query_ctx->velox_query_ctx,
                                         evaluator,
                                         {},
                                         {.numWorkers = 1, .numDrivers = 1}};

      auto plan = opt.toVeloxPlan(opt.bestPlan()->op);

      for (auto& conjunct :
           connector._table_handles[rel.first.relation]->AcceptedFilters()) {
        auto res = connector::search::ExprToFilter(root, evaluator, conjunct,
                                                   table->columnMap());
        ASSERT_EQ(res.ok(), must_succeed) << res.errorMessage();
      }
      if (must_succeed) {
        ASSERT_EQ(root, expected);
      }
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

  template<typename T, typename Filter>
  void AddTermsFilter(Filter& root, catalog::Column::Id column,
                      const std::vector<T>& values) {
    auto& terms = AddFilter<irs::ByTerms>(root);
    *terms.mutable_field() = MakeFieldName<T>(column);

    for (const auto& value : values) {
      if constexpr (std::is_same_v<T, bool>) {
        terms.mutable_options()->terms.emplace(
          irs::ViewCast<irs::byte_type>(irs::BooleanTokenizer::value(value)));
      } else if constexpr (std::is_same_v<T, velox::StringView>) {
        irs::StringTokenizer stream;
        const irs::TermAttr* token = irs::get<irs::TermAttr>(stream);
        stream.reset(value);
        stream.next();
        terms.mutable_options()->terms.emplace(token->value);
      } else if constexpr (std::is_floating_point_v<T> ||
                           std::is_integral_v<T>) {
        irs::NumericTokenizer stream;
        const irs::TermAttr* token = irs::get<irs::TermAttr>(stream);
        stream.reset(value);
        stream.next();
        terms.mutable_options()->terms.emplace(token->value);
      } else {
        ASSERT_FALSE(true) << "Unexpected term type";
      }
    }
  }

  template<typename Filter>
  void AddNullFilter(Filter& root, catalog::Column::Id column) {
    auto& term = AddFilter<irs::ByTerm>(root);
    std::string field_name;
    basics::StrResize(field_name, sizeof(column));
    absl::big_endian::Store(field_name.data(), column);
    sdb::search::mangling::MangleNull(field_name);
    *term.mutable_field() = field_name;
    term.mutable_options()->term.assign(
      irs::ViewCast<irs::byte_type>(irs::NullTokenizer::value_null()));
  }

  template<typename Filter>
  void AddLikeFilter(Filter& root, catalog::Column::Id column,
                     std::string_view value) {
    auto& wc = AddFilter<irs::ByWildcard>(root);
    *wc.mutable_field() = MakeFieldName<velox::StringView>(column);
    wc.mutable_options()->term.assign(irs::ViewCast<irs::byte_type>(value));
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
  AssertFilter(expected,
               "SELECT * FROM foo WHERE a BETWEEN SYMMETRIC 100 AND 10",
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

TEST_F(SearchFilterBuilderTest, test_ComparisonNotConst) {
  std::vector<std::unique_ptr<const axiom::connector::Column>> columns;
  irs::And expected;
  columns.emplace_back(
    std::make_unique<connector::SereneDBColumn>("status", velox::INTEGER(), 1));
  columns.emplace_back(
    std::make_unique<connector::SereneDBColumn>("age", velox::INTEGER(), 2));
  AssertFilter(expected, "SELECT * FROM foo WHERE status <= age",
               std::move(columns), false);
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

TEST_F(SearchFilterBuilderTest, test_DoubleNegation) {
  std::vector<std::unique_ptr<const axiom::connector::Column>> columns;
  irs::And expected;

  // NOT (NOT (a = 10)) should simplify to a = 10
  AddTermFilter<int32_t>(expected, 1, 10);

  columns.emplace_back(
    std::make_unique<connector::SereneDBColumn>("a", velox::INTEGER(), 1));
  AssertFilter(expected, "SELECT * FROM foo WHERE NOT (NOT (a = 10))",
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

TEST_F(SearchFilterBuilderTest, test_NestedNotWithComparisons) {
  std::vector<std::unique_ptr<const axiom::connector::Column>> columns;
  irs::And expected;

  // NOT (a > 50 AND b < 100) is equivalent to (a <= 50 OR b >= 100)
  auto& or_filter = expected.add<irs::Or>();
  AddRangeFilter<int32_t>(or_filter, 1, std::nullopt, false, 50, true);
  AddRangeFilter<int32_t>(or_filter, 2, 100, true, std::nullopt, false);

  columns.emplace_back(
    std::make_unique<connector::SereneDBColumn>("a", velox::INTEGER(), 1));
  columns.emplace_back(
    std::make_unique<connector::SereneDBColumn>("b", velox::INTEGER(), 2));
  AssertFilter(expected, "SELECT * FROM foo WHERE NOT (a > 50 AND b < 100)",
               std::move(columns), true);
}

TEST_F(SearchFilterBuilderTest, test_NestedNotWithOr) {
  std::vector<std::unique_ptr<const axiom::connector::Column>> columns;
  irs::And root;
  auto& expected = root.add<irs::And>();

  // NOT (a < 10 OR a > 100) is equivalent to (a >= 10 AND a <= 100)

  AddRangeFilter<int32_t>(expected, 1, 10, true, std::nullopt, false);
  AddRangeFilter<int32_t>(expected, 1, std::nullopt, false, 100, true);

  columns.emplace_back(
    std::make_unique<connector::SereneDBColumn>("a", velox::INTEGER(), 1));
  AssertFilter(root, "SELECT * FROM foo WHERE NOT (a < 10 OR a > 100)",
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

// ============================================================================
// IN Operator Tests
// ============================================================================

TEST_F(SearchFilterBuilderTest, test_InOperatorIntegers) {
  std::vector<std::unique_ptr<const axiom::connector::Column>> columns;
  irs::And expected;

  AddTermsFilter<int32_t>(expected, 1, {10, 20, 30, 40});

  columns.emplace_back(
    std::make_unique<connector::SereneDBColumn>("a", velox::INTEGER(), 1));
  AssertFilter(expected, "SELECT * FROM foo WHERE a IN (10, 20, 30, 40)",
               std::move(columns), true);
}

TEST_F(SearchFilterBuilderTest, test_InOperatorStrings) {
  std::vector<std::unique_ptr<const axiom::connector::Column>> columns;
  irs::And expected;

  AddTermsFilter<velox::StringView>(
    expected, 1,
    {velox::StringView{"active"}, velox::StringView{"pending"},
     velox::StringView{"completed"}});

  columns.emplace_back(
    std::make_unique<connector::SereneDBColumn>("status", velox::VARCHAR(), 1));
  AssertFilter(
    expected,
    "SELECT * FROM foo WHERE status IN ('active', 'pending', 'completed')",
    std::move(columns), true);
}

TEST_F(SearchFilterBuilderTest, test_InOperatorLongStrings) {
  std::vector<std::unique_ptr<const axiom::connector::Column>> columns;
  irs::And expected;

  AddTermsFilter<velox::StringView>(
    expected, 1,
    {velox::StringView{
       "ACTIVE LONG STRING THAT WILL NOT FIT INTO INLINE STRINGVIEW"},
     velox::StringView{
       "pending super string that will not fit into inline string view"},
     velox::StringView{
       "COMPLETED FULL STRING THAT WILL NOT FIT INTO INLINE STRING VIEW"}});

  columns.emplace_back(
    std::make_unique<connector::SereneDBColumn>("status", velox::VARCHAR(), 1));
  AssertFilter(
    expected,
    "SELECT * FROM foo WHERE status IN (UPPER('active long string that will "
    "not fit into inline stringView'), LOWER('pending super strinG that will "
    "not fit into inline string view'), UPPER('completed full string that will "
    "not fit into inline string view'))",
    std::move(columns), true);
}

TEST_F(SearchFilterBuilderTest, test_InOperatorWithImplicitCast) {
  std::vector<std::unique_ptr<const axiom::connector::Column>> columns;
  irs::And expected;

  AddTermsFilter<int32_t>(expected, 1, {1, 2, 3, 5, 8, 13});

  columns.emplace_back(std::make_unique<connector::SereneDBColumn>(
    "fibonacci", velox::INTEGER(), 1));
  AssertFilter(
    expected,
    "SELECT * FROM foo WHERE fibonacci IN ('1', '2', '3', '5', '8', '13')",
    std::move(columns), true);
}

TEST_F(SearchFilterBuilderTest, test_InOperatorWithAnd) {
  std::vector<std::unique_ptr<const axiom::connector::Column>> columns;
  irs::And expected;

  AddTermsFilter<int32_t>(expected, 1, {10, 20, 30});
  AddRangeFilter<int32_t>(expected, 2, 100, true, std::nullopt, false);

  columns.emplace_back(
    std::make_unique<connector::SereneDBColumn>("type", velox::INTEGER(), 1));
  columns.emplace_back(
    std::make_unique<connector::SereneDBColumn>("value", velox::INTEGER(), 2));
  AssertFilter(expected,
               "SELECT * FROM foo WHERE type IN (10, 20, 30) AND value >= 100",
               std::move(columns), true);
}

TEST_F(SearchFilterBuilderTest, test_InOperatorWithOr) {
  std::vector<std::unique_ptr<const axiom::connector::Column>> columns;
  irs::And expected;

  auto& or_filter = expected.add<irs::Or>();
  AddTermsFilter<int32_t>(or_filter, 1, {1, 2, 3});
  AddTermFilter<velox::StringView>(or_filter, 2, velox::StringView{"special"});

  columns.emplace_back(std::make_unique<connector::SereneDBColumn>(
    "category", velox::INTEGER(), 1));
  columns.emplace_back(
    std::make_unique<connector::SereneDBColumn>("name", velox::VARCHAR(), 2));
  AssertFilter(
    expected,
    "SELECT * FROM foo WHERE category IN (1, 2, 3) OR name = 'special'",
    std::move(columns), true);
}

TEST_F(SearchFilterBuilderTest, test_NotIn) {
  std::vector<std::unique_ptr<const axiom::connector::Column>> columns;
  irs::And expected;

  auto& not_filter = expected.add<irs::Not>();
  AddTermsFilter<int32_t>(not_filter, 1, {100, 200, 300});

  columns.emplace_back(std::make_unique<connector::SereneDBColumn>(
    "excluded", velox::INTEGER(), 1));
  AssertFilter(expected,
               "SELECT * FROM foo WHERE excluded NOT IN (100, 200, 300)",
               std::move(columns), true);
}

TEST_F(SearchFilterBuilderTest, test_InWithSingleValue) {
  std::vector<std::unique_ptr<const axiom::connector::Column>> columns;
  irs::And expected;

  AddTermFilter<int32_t>(expected, 1, 42);

  columns.emplace_back(
    std::make_unique<connector::SereneDBColumn>("answer", velox::INTEGER(), 1));
  AssertFilter(expected, "SELECT * FROM foo WHERE answer IN (42)",
               std::move(columns), true);
}

TEST_F(SearchFilterBuilderTest, test_InOperatorLargeColumnId) {
  std::vector<std::unique_ptr<const axiom::connector::Column>> columns;
  irs::And expected;

  AddTermsFilter<int32_t>(expected, 8192, {10, 20, 30, 40, 50});

  columns.emplace_back(std::make_unique<connector::SereneDBColumn>(
    "code", velox::INTEGER(), 8192));
  AssertFilter(expected, "SELECT * FROM foo WHERE code IN (10, 20, 30, 40, 50)",
               std::move(columns), true);
}

TEST_F(SearchFilterBuilderTest, test_InNotConst) {
  std::vector<std::unique_ptr<const axiom::connector::Column>> columns;
  irs::And expected;

  columns.emplace_back(
    std::make_unique<connector::SereneDBColumn>("a", velox::INTEGER(), 1));
  columns.emplace_back(
    std::make_unique<connector::SereneDBColumn>("b", velox::INTEGER(), 1));
  AssertFilter(expected, "SELECT * FROM foo WHERE a IN (10, b, 30, 40)",
               std::move(columns), false);
}

TEST_F(SearchFilterBuilderTest, test_InNotConst2) {
  std::vector<std::unique_ptr<const axiom::connector::Column>> columns;
  irs::And expected;

  columns.emplace_back(
    std::make_unique<connector::SereneDBColumn>("a", velox::INTEGER(), 1));
  columns.emplace_back(
    std::make_unique<connector::SereneDBColumn>("b", velox::INTEGER(), 1));
  AssertFilter(expected, "SELECT * FROM foo WHERE a IN (b)", std::move(columns),
               false);
}

TEST_F(SearchFilterBuilderTest, test_InOperatorIntegersNulls) {
  std::vector<std::unique_ptr<const axiom::connector::Column>> columns;
  irs::And expected;

  AddTermsFilter<int32_t>(expected, 1, {10, 20, 30, 40});

  columns.emplace_back(
    std::make_unique<connector::SereneDBColumn>("a", velox::INTEGER(), 1));
  AssertFilter(expected, "SELECT * FROM foo WHERE a IN (10, 20, NULL, 30, 40)",
               std::move(columns), true);
}

TEST_F(SearchFilterBuilderTest, test_InOperatorOnlyNulls) {
  std::vector<std::unique_ptr<const axiom::connector::Column>> columns;
  irs::And expected;
  AddTermsFilter<int32_t>(expected, 1, {});
  columns.emplace_back(
    std::make_unique<connector::SereneDBColumn>("a", velox::INTEGER(), 1));
  AssertFilter(expected, "SELECT * FROM foo WHERE a IN (NULL, NULL)",
               std::move(columns), true);
}

// ============================================================================
// IS NULL Tests
// ============================================================================

TEST_F(SearchFilterBuilderTest, test_IsNull) {
  std::vector<std::unique_ptr<const axiom::connector::Column>> columns;
  irs::And expected;

  AddNullFilter(expected, 1);

  columns.emplace_back(std::make_unique<connector::SereneDBColumn>(
    "optional_field", velox::INTEGER(), 1));
  AssertFilter(expected, "SELECT * FROM foo WHERE optional_field IS NULL",
               std::move(columns), true);
}

TEST_F(SearchFilterBuilderTest, test_IsNullString) {
  std::vector<std::unique_ptr<const axiom::connector::Column>> columns;
  irs::And expected;

  AddNullFilter(expected, 1);

  columns.emplace_back(std::make_unique<connector::SereneDBColumn>(
    "description", velox::VARCHAR(), 1));
  AssertFilter(expected, "SELECT * FROM foo WHERE description IS NULL",
               std::move(columns), true);
}

TEST_F(SearchFilterBuilderTest, test_IsNotNull) {
  std::vector<std::unique_ptr<const axiom::connector::Column>> columns;
  irs::And expected;

  auto& not_filter = expected.add<irs::Not>();
  AddNullFilter(not_filter, 1);

  columns.emplace_back(std::make_unique<connector::SereneDBColumn>(
    "required_field", velox::INTEGER(), 1));
  AssertFilter(expected, "SELECT * FROM foo WHERE required_field IS NOT NULL",
               std::move(columns), true);
}

TEST_F(SearchFilterBuilderTest, test_IsNotNotNull) {
  std::vector<std::unique_ptr<const axiom::connector::Column>> columns;
  irs::And expected;

  AddNullFilter(expected, 1);

  columns.emplace_back(std::make_unique<connector::SereneDBColumn>(
    "required_field", velox::INTEGER(), 1));
  AssertFilter(expected,
               "SELECT * FROM foo WHERE NOT(required_field IS NOT NULL)",
               std::move(columns), true);
}

TEST_F(SearchFilterBuilderTest, test_NotIsNull) {
  std::vector<std::unique_ptr<const axiom::connector::Column>> columns;
  irs::And expected;

  auto& not_filter = expected.add<irs::Not>();
  AddNullFilter(not_filter, 1);

  columns.emplace_back(std::make_unique<connector::SereneDBColumn>(
    "required_field", velox::INTEGER(), 1));
  AssertFilter(expected, "SELECT * FROM foo WHERE NOT(required_field IS NULL)",
               std::move(columns), true);
}

TEST_F(SearchFilterBuilderTest, test_IsNullWithAnd) {
  std::vector<std::unique_ptr<const axiom::connector::Column>> columns;
  irs::And expected;

  AddNullFilter(expected, 1);
  AddTermFilter<velox::StringView>(expected, 2, velox::StringView{"active"});

  columns.emplace_back(std::make_unique<connector::SereneDBColumn>(
    "deleted_at", velox::INTEGER(), 1));
  columns.emplace_back(
    std::make_unique<connector::SereneDBColumn>("status", velox::VARCHAR(), 2));
  AssertFilter(
    expected,
    "SELECT * FROM foo WHERE deleted_at IS NULL AND status = 'active'",
    std::move(columns), true);
}

TEST_F(SearchFilterBuilderTest, test_IsNullWithOr) {
  std::vector<std::unique_ptr<const axiom::connector::Column>> columns;
  irs::And expected;

  auto& or_filter = expected.add<irs::Or>();
  AddNullFilter(or_filter, 1);
  AddTermFilter<int32_t>(or_filter, 1, 0);

  columns.emplace_back(
    std::make_unique<connector::SereneDBColumn>("count", velox::INTEGER(), 1));
  AssertFilter(expected, "SELECT * FROM foo WHERE count IS NULL OR count = 0",
               std::move(columns), true);
}

TEST_F(SearchFilterBuilderTest, test_IsNullLargeColumnId) {
  std::vector<std::unique_ptr<const axiom::connector::Column>> columns;
  irs::And expected;

  AddNullFilter(expected, 16384);

  columns.emplace_back(std::make_unique<connector::SereneDBColumn>(
    "extra_data", velox::VARCHAR(), 16384));
  AssertFilter(expected, "SELECT * FROM foo WHERE extra_data IS NULL",
               std::move(columns), true);
}

TEST_F(SearchFilterBuilderTest, test_IsNullOrNotInside) {
  std::vector<std::unique_ptr<const axiom::connector::Column>> columns;
  irs::And expected;

  auto& or_filter = expected.add<irs::Or>();
  AddNullFilter(or_filter, 1);
  // NOT inside OR requires intermediate AND
  auto& and_filter = or_filter.add<irs::And>();
  auto& not_filter = and_filter.add<irs::Not>();
  AddTermFilter<velox::StringView>(not_filter, 2, velox::StringView{"invalid"});

  columns.emplace_back(
    std::make_unique<connector::SereneDBColumn>("field1", velox::INTEGER(), 1));
  columns.emplace_back(
    std::make_unique<connector::SereneDBColumn>("field2", velox::VARCHAR(), 2));
  AssertFilter(
    expected,
    "SELECT * FROM foo WHERE field1 IS NULL OR NOT (field2 = 'invalid')",
    std::move(columns), true);
}

TEST_F(SearchFilterBuilderTest, test_ComplexWithInAndNull) {
  std::vector<std::unique_ptr<const axiom::connector::Column>> columns;
  irs::And expected;

  AddTermsFilter<int32_t>(expected, 1, {1, 2, 3});
  auto& or_filter = expected.add<irs::Or>();
  AddNullFilter(or_filter, 2);
  AddRangeFilter<int32_t>(or_filter, 2, 100, true, std::nullopt, false);

  columns.emplace_back(std::make_unique<connector::SereneDBColumn>(
    "category", velox::INTEGER(), 1));
  columns.emplace_back(std::make_unique<connector::SereneDBColumn>(
    "priority", velox::INTEGER(), 2));
  AssertFilter(expected,
               "SELECT * FROM foo WHERE category IN (1, 2, 3) AND (priority IS "
               "NULL OR priority >= 100)",
               std::move(columns), true);
}

// ============================================================================
// LIKE Tests
// ============================================================================

TEST_F(SearchFilterBuilderTest, test_Like) {
  std::vector<std::unique_ptr<const axiom::connector::Column>> columns;
  irs::And expected;

  AddLikeFilter(expected, 1, "%foo_");

  columns.emplace_back(std::make_unique<connector::SereneDBColumn>(
    "required_field", velox::VARCHAR(), 1));
  AssertFilter(expected, "SELECT * FROM foo WHERE required_field LIKE '%foo_'",
               std::move(columns), true);
}

TEST_F(SearchFilterBuilderTest, test_LikeOp) {
  std::vector<std::unique_ptr<const axiom::connector::Column>> columns;
  irs::And expected;

  AddLikeFilter(expected, 1, "%foo_");

  columns.emplace_back(std::make_unique<connector::SereneDBColumn>(
    "required_field", velox::VARCHAR(), 1));
  AssertFilter(expected, "SELECT * FROM foo WHERE required_field ~~ '%foo_'",
               std::move(columns), true);
}

TEST_F(SearchFilterBuilderTest, test_LikeCustomEscape) {
  std::vector<std::unique_ptr<const axiom::connector::Column>> columns;
  irs::And expected;

  AddLikeFilter(expected, 1, "\\%!foo_");

  columns.emplace_back(std::make_unique<connector::SereneDBColumn>(
    "required_field", velox::VARCHAR(), 1));
  AssertFilter(
    expected,
    "SELECT * FROM foo WHERE required_field LIKE '!%!!foo_' ESCAPE '!'",
    std::move(columns), true);
}

TEST_F(SearchFilterBuilderTest, test_NotLike) {
  std::vector<std::unique_ptr<const axiom::connector::Column>> columns;
  irs::And expected;

  auto& not_filter = expected.add<irs::Not>();
  AddLikeFilter(not_filter, 1, "%bar_");

  columns.emplace_back(std::make_unique<connector::SereneDBColumn>(
    "required_field", velox::VARCHAR(), 1));
  AssertFilter(expected,
               "SELECT * FROM foo WHERE NOT(required_field LIKE '%bar_')",
               std::move(columns), true);
}

TEST_F(SearchFilterBuilderTest, test_LikeWithFunc) {
  std::vector<std::unique_ptr<const axiom::connector::Column>> columns;
  irs::And expected;

  AddLikeFilter(expected, 1, "!!!%FOO_");

  columns.emplace_back(std::make_unique<connector::SereneDBColumn>(
    "required_field", velox::VARCHAR(), 1));
  AssertFilter(expected,
               "SELECT * FROM foo WHERE required_field LIKE UPPER('!!!%foo_')",
               std::move(columns), true);
}

TEST_F(SearchFilterBuilderTest, test_LikeNotConst) {
  std::vector<std::unique_ptr<const axiom::connector::Column>> columns;
  irs::And expected;
  columns.emplace_back(std::make_unique<connector::SereneDBColumn>(
    "required_field", velox::VARCHAR(), 1));
  columns.emplace_back(std::make_unique<connector::SereneDBColumn>(
    "value_field", velox::VARCHAR(), 2));
  AssertFilter(expected,
               "SELECT * FROM foo WHERE required_field LIKE UPPER(value_field)",
               std::move(columns), false);
}

TEST_F(SearchFilterBuilderTest, test_FieldCastError) {
  irs::And expected;
  {
    std::vector<std::unique_ptr<const axiom::connector::Column>> columns;
    columns.emplace_back(
      std::make_unique<connector::SereneDBColumn>("b", velox::TINYINT(), 1));
    AssertFilter(expected, "SELECT * FROM foo WHERE b = 999999999999 ",
                 std::move(columns), false);
  }
  {
    std::vector<std::unique_ptr<const axiom::connector::Column>> columns;
    columns.emplace_back(
      std::make_unique<connector::SereneDBColumn>("b", velox::TINYINT(), 1));
    AssertFilter(expected, "SELECT * FROM foo WHERE b <= 999999999999 ",
                 std::move(columns), false);
  }
  {
    std::vector<std::unique_ptr<const axiom::connector::Column>> columns;
    columns.emplace_back(
      std::make_unique<connector::SereneDBColumn>("b", velox::TINYINT(), 1));
    AssertFilter(expected, "SELECT * FROM foo WHERE b IN (1.24, 3.0, 4.5) ",
                 std::move(columns), false);
  }
}

}  // namespace
