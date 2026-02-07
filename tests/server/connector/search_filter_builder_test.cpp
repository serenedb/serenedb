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
    auto res = connector::search::ExprToFilter(
      root, evaluator,
      connector._table_handles[rel.first.relation]->AcceptedFilters().front(),
      table->columnMap());
    ASSERT_EQ(res.ok(), must_succeed);
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
  void AddTermFilter(Filter& root, catalog::Column::Id column,
                     const T& value) {
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

 protected:
  pg::MemoryContextPtr _pg_memory_ctx;
  static ServerState gServerState;
};

ServerState SearchFilterBuilderTest::gServerState;

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
  AddTermFilter<int32_t>(or_filter, 1, 10);
  AddTermFilter<velox::StringView>(or_filter, 2, velox::StringView{"foobar"});
  columns.emplace_back(
    std::make_unique<connector::SereneDBColumn>("a", velox::INTEGER(), 1));
  columns.emplace_back(
    std::make_unique<connector::SereneDBColumn>("b", velox::VARCHAR(), 2));
  AssertFilter(expected, "SELECT * FROM foo WHERE a = '10' OR b = 'foobar'",
               std::move(columns), true);
}

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

}  // namespace
