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

#include <duckdb/main/connection.hpp>
#include <iresearch/utils/duckdb_engine.hpp>

#include "network/pg/protocol_state.h"
#include "network/pg/simple_query.h"
#include "pg/connection_context.h"

using namespace sdb::network::pg;

namespace {

struct Script {
  duckdb::Connection connection{
    sdb::CreateClientContext(irs::DuckDBEngine::Instance().instance())};
  ImplicitTxnState txn{connection.context->transaction};
  SimpleQuery query{*connection.context,
                    sdb::ConnectionContext::Get(*connection.context), txn};
};

}  // namespace

TEST(NetworkPgSimpleQuery, ASyntaxErrorNamesTheStatementItStoppedAt) {
  Script first;
  EXPECT_ANY_THROW(first.query.Parse("SELEC 1; SELECT 2"));
  EXPECT_EQ(first.query.Statement(), size_t{1});

  Script third;
  EXPECT_ANY_THROW(third.query.Parse("SELECT 1; SELECT 2; SELEC 3"));
  EXPECT_EQ(third.query.Statement(), size_t{3});
}

TEST(NetworkPgSimpleQuery, NoStatementIsNamedBeforeOneRuns) {
  Script empty;
  empty.query.Parse("");
  EXPECT_EQ(empty.query.Statement(), size_t{0});

  Script parsed;
  parsed.query.Parse("SELECT 1; SELECT 2");
  EXPECT_EQ(parsed.query.Statement(), size_t{0});
}

TEST(NetworkPgSimpleQuery, EachStatementIsNamedWhileItRuns) {
  Script script;
  script.query.Parse("SELECT 1; SELECT 2");
  SimpleQuery::Step step;
  ASSERT_TRUE(script.query.Next(step));
  EXPECT_EQ(script.query.Statement(), size_t{1});
  ASSERT_TRUE(script.query.Next(step));
  EXPECT_EQ(script.query.Statement(), size_t{2});
  EXPECT_TRUE(step.last);
  EXPECT_FALSE(script.query.Next(step));
}
